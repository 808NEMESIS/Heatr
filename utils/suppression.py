"""
utils/suppression.py — platformbrede (cross-workspace) suppressielijst.

Fase 2 (PR 7, audit v2 P0-2): suppressie leefde als mutabele velden op de
lead-rij, per workspace — dezelfde persoon in workspace B bleef mailbaar na
een unsubscribe in A, en een re-import kon de vlag overschrijven. Deze
module is de tweede, absolute verdedigingslinie bovenop de lead-level
compliance_check:

  - WRITE: webhook (unsubscribe/hard bounce), reply-classifier en
    gdpr-forget registreren het adres hier — append-only, idempotent
    (de partial-UNIQUE uit migratie 024 maakt de tweede insert een no-op).
  - READ: de outbound-dispatcher raadpleegt check_suppressed() vóór élke
    prospect-send, FAIL-CLOSED (Besluit 3: suppressie-infra onbeschikbaar
    = niet versturen).

PRIVACY-CONTRACT: de gate geeft callers alleen door DAT een adres
gesuppressed is ("globally_suppressed"), nooit door wie/welke tenant of
waarom in detail. source_workspace_id blijft intern.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

SUPPRESSION_TYPES = (
    "unsubscribe", "hard_bounce", "complaint", "forgotten",
    "manual_global", "domain_block",
)


def normalize_email(email: str | None) -> str | None:
    """Canonieke vorm voor suppressie-matching: lower + trim.

    Bewust GEEN plus-adressering of punt-normalisatie — dat zou adressen
    suppressen die de ontvanger niet heeft uitgeschreven (over-suppressie
    is hier de fout-richting: legitieme leads blokkeren op andermans keuze).
    """
    if not email or "@" not in email:
        return None
    return email.strip().lower()


def normalize_domain(value: str | None) -> str | None:
    """Domein-sleutel voor org-brede suppressie (AVG-05): het deel na @ bij een
    e-mailadres, of de kale host bij een domein/URL. lower + trim, zonder schema/pad.
    """
    if not value:
        return None
    import re
    v = value.strip().lower()
    if "@" in v:
        v = v.split("@", 1)[1]
    v = re.sub(r"^https?://", "", v).split("/")[0].strip().strip(".")
    return v or None


def add_suppression(
    supabase_client: Any,
    *,
    email: str | None,
    suppression_type: str,
    source: str,
    source_workspace_id: str | None = None,
    lead_id: str | None = None,
    campaign_id: str | None = None,
    event_id: str | None = None,
    reason: str | None = None,
    created_by: str | None = None,
    domain: str | None = None,
    kvk_number: str | None = None,
) -> dict:
    """Registreer een actieve suppressie. Idempotent en fail-soft.

    - Bestaat er al een actieve suppressie voor dit adres (unique-conflict),
      dan is dat een no-op: {"ok": True, "already": True}.
    - Elke andere fout wordt LUID gelogd en als {"ok": False, "error": ...}
      teruggegeven — de aanroepende webhook/forget-flow beslist zelf of dat
      de operatie laat falen. De write is bewust niet-raising omdat de
      lead-level status (compliance_check) als eerste linie al gezet is.
    """
    if suppression_type not in SUPPRESSION_TYPES:
        return {"ok": False, "error": f"onbekend suppression_type: {suppression_type!r}"}
    normalized = normalize_email(email)
    if not normalized:
        return {"ok": False, "error": "geen bruikbaar e-mailadres"}
    # AVG-05: leg óók het org-niveau vast (domein uit de expliciete param of het
    # e-mailadres, + KvK) zodat een andere collega bij dezelfde praktijk niet
    # opnieuw benaderd wordt na een afmelding/forget.
    norm_domain = normalize_domain(domain) or normalize_domain(email)
    row = {
        "normalized_email": normalized,
        "normalized_domain": norm_domain,
        "kvk_number": (kvk_number or "").strip() or None,
        "suppression_type": suppression_type,
        "source": source,
        "source_workspace_id": source_workspace_id,
        "lead_id": lead_id,
        "campaign_id": campaign_id,
        "event_id": event_id,
        "reason": (reason or "")[:500] or None,
        "created_by": created_by,
    }
    def _is_dup(err) -> bool:
        t = str(err)
        return getattr(err, "code", None) == "23505" or "23505" in t or "duplicate key" in t.lower()

    try:
        supabase_client.table("suppressions").insert(row).execute()
        logger.info(
            "suppression: %s geregistreerd (type=%s source=%s lead=%s)",
            normalized, suppression_type, source, lead_id or "-",
        )
        return {"ok": True, "already": False}
    except Exception as e:
        text = str(e)
        if _is_dup(e):
            # Al actief gesuppressed — precies wat we willen (idempotent).
            return {"ok": True, "already": True}
        # Migratie 042 (normalized_domain/kvk_number) nog niet gedraaid? Val terug
        # op de e-mail-only insert zodat de bestaande suppressie BLIJFT werken —
        # anders zou een unsubscribe-write pre-042 stilletjes falen (compliance-gat).
        if "normalized_domain" in text or "kvk_number" in text or "PGRST204" in text \
                or ("column" in text.lower() and "does not exist" in text.lower()):
            base = {k: v for k, v in row.items() if k not in ("normalized_domain", "kvk_number")}
            try:
                supabase_client.table("suppressions").insert(base).execute()
                logger.warning(
                    "suppression: org-kolommen (migratie 042) ontbreken nog → %s alleen "
                    "op e-mail gesuppressed (domein/KvK volgt na 042).", normalized,
                )
                return {"ok": True, "already": False, "org_columns_missing": True}
            except Exception as e2:
                if _is_dup(e2):
                    return {"ok": True, "already": True}
                text = str(e2)
        logger.error(
            "suppression: REGISTRATIE MISLUKT voor type=%s source=%s lead=%s: %s — "
            "is migratie 024 gedraaid? Lead-level status is de enige actieve blokkade.",
            suppression_type, source, lead_id or "-", text,
        )
        return {"ok": False, "error": text}


def check_suppressed(
    supabase_client: Any,
    emails: list[str | None],
) -> dict[str, str]:
    """Batch-check: welke van deze adressen zijn actief gesuppressed?

    Returns:
        {normalized_email: suppression_type} — leeg dict = niets gesuppressed.

    Raises:
        Exception bij een DB-fout — de dispatcher behandelt dat FAIL-CLOSED
        (prospect-send geblokkeerd). Bewust geen except hier: een geslikte
        leesfout zou de suppressielijst stilletjes uitschakelen, precies de
        faalvorm die audit v2 overal aantrof.
    """
    normalized = sorted({n for n in (normalize_email(e) for e in emails) if n})
    if not normalized:
        return {}
    res = (
        supabase_client.table("suppressions")
        .select("normalized_email, suppression_type")
        .in_("normalized_email", normalized)
        .is_("revoked_at", "null")
        .execute()
    )
    return {
        r["normalized_email"]: r.get("suppression_type") or "unknown"
        for r in (res.data or [])
    }


def suppression_match_keys(lead: dict) -> dict[str, str]:
    """Org-brede suppressie-sleutels voor een lead: e-mail + domein + KvK (AVG-05).
    PUUR — testbaar zonder DB."""
    keys: dict[str, str] = {}
    email = normalize_email(lead.get("email"))
    if email:
        keys["email"] = email
    domain = normalize_domain(lead.get("email") or lead.get("domain") or lead.get("website"))
    if domain:
        keys["domain"] = domain
    kvk = (lead.get("kvk_number") or "").strip()
    if kvk:
        keys["kvk"] = kvk
    return keys


def check_suppressed_lead(supabase_client: Any, lead: dict) -> str | None:
    """Org-brede suppressie-check voor één lead: is het e-mailadres, het domein óf
    het KvK-nummer actief gesuppressed? Returned de suppression_type of None.

    FAIL-CLOSED: raist bij een DB-fout — de send-gate blokkeert dan (Besluit 3),
    net als check_suppressed. Bewust geen except."""
    keys = suppression_match_keys(lead)
    if not keys:
        return None
    ors = []
    if keys.get("email"):
        ors.append(f"normalized_email.eq.{keys['email']}")
    if keys.get("domain"):
        ors.append(f"normalized_domain.eq.{keys['domain']}")
    if keys.get("kvk"):
        ors.append(f"kvk_number.eq.{keys['kvk']}")
    res = (
        supabase_client.table("suppressions")
        .select("suppression_type")
        .or_(",".join(ors))
        .is_("revoked_at", "null")
        .execute()
    )
    rows = res.data or []
    return (rows[0].get("suppression_type") or "unknown") if rows else None
