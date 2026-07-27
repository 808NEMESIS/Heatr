"""utils/compliance_flags.py — persistente, zichtbare compliance-vlaggen + drip-pre-gate.

Sami 2026-07-27: een post-send-vondst (bv. een missende Warmr-afmeldlink) mag geen
logregel blijven. Hij wordt een rij in heatr_compliance_flags (migratie 044) met een
acknowledged-state, en de drip-pre-gate (assert_no_open_flags) laat een volgende
batch NIET doorgaan zolang er een open vlag is.

Ontwerpkeuzes:
  - raise_flag is idempotent op de open-vlag (partial-UNIQUE 044): een herhaalde
    sweep spamt geen dubbele vlaggen.
  - assert_no_open_flags is FAIL-CLOSED: kan de tabel niet gelezen worden, dan HALT
    (we kunnen "geen open vlag" niet bevestigen → niet versturen). Een compliance-
    hold hoort strenger te falen dan een gewone gate.
  - Werkt met de heatr_-wrapper-client (.table("compliance_flags") → heatr_compliance_flags).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

FLAG_MISSING_UNSUBSCRIBE = "missing_unsubscribe"


class ComplianceHold(Exception):
    """De drip-pre-gate blokkeert: er is een open compliance-vlag (of de vlag-tabel
    is onleesbaar → fail-closed). Verstuur niets tot de vlag is afgehandeld."""


def raise_flag(
    sb: Any, *, workspace_id: str, flag_type: str, lead_id: str | None,
    campaign_id: str | None = None, detail: str | None = None,
) -> str | None:
    """Zet een compliance-vlag (idempotent op de open-vlag). Returnt de rij-id, of
    None als de vlag al open stond (unique-conflict) of de write faalde.

    Een write-fout wordt LUID gelogd — een verloren compliance-vlag is ernstig — maar
    raist niet: de sweep moet doorgaan met de overige leads."""
    row = {
        "workspace_id": workspace_id,
        "flag_type": flag_type,
        "lead_id": str(lead_id) if lead_id is not None else None,
        "campaign_id": str(campaign_id) if campaign_id is not None else None,
        "detail": (detail or "")[:2000] or None,
    }
    try:
        res = sb.table("compliance_flags").insert(row).execute()
        fid = (res.data or [{}])[0].get("id")
        logger.warning(
            "compliance_flag GEZET type=%s workspace=%s lead=%s campaign=%s — %s",
            flag_type, workspace_id, lead_id, campaign_id, detail,
        )
        return fid
    except Exception as e:
        text = str(e)
        if "23505" in text or "duplicate key" in text.lower():
            return None                       # vlag stond al open — prima
        logger.error(
            "compliance_flag WRITE MISLUKT type=%s lead=%s: %s — is migratie 044 gedraaid?",
            flag_type, lead_id, e,
        )
        return None


def open_flags(sb: Any, workspace_id: str, flag_type: str | None = None) -> list[dict]:
    """Alle open (niet-acknowledged) vlaggen. Raist bij een leesfout — callers die
    fail-closed willen (assert_no_open_flags) vangen dat af."""
    q = (sb.table("compliance_flags")
         .select("id, flag_type, lead_id, campaign_id, detail, created_at")
         .eq("workspace_id", workspace_id)
         .is_("acknowledged_at", "null"))
    if flag_type:
        q = q.eq("flag_type", flag_type)
    return q.execute().data or []


def assert_no_open_flags(sb: Any, workspace_id: str, flag_type: str | None = None) -> None:
    """Drip-pre-gate. Raist ComplianceHold als er een open vlag is — of als de tabel
    onleesbaar is (FAIL-CLOSED). Roep dit aan vóór een drip-batch/receptie-send."""
    try:
        flags = open_flags(sb, workspace_id, flag_type)
    except Exception as e:
        raise ComplianceHold(
            f"compliance-vlaggen niet leesbaar (fail-closed, geen send): {str(e)[:120]}"
        ) from e
    if flags:
        kinds = ", ".join(sorted({f.get("flag_type") or "?" for f in flags}))
        raise ComplianceHold(
            f"{len(flags)} open compliance-vlag(gen) [{kinds}] — drip geblokkeerd tot "
            f"afgehandeld (utils/compliance_flags.acknowledge_flags)."
        )


def acknowledge_flags(sb: Any, flag_ids: list[str], *, by: str) -> int:
    """Markeer vlaggen als bekeken/afgehandeld (deblokkeert de drip). Returnt het
    aantal bijgewerkte rijen."""
    if not flag_ids:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    res = (sb.table("compliance_flags")
           .update({"acknowledged_at": now, "acknowledged_by": by})
           .in_("id", [str(i) for i in flag_ids])
           .is_("acknowledged_at", "null")
           .execute())
    return len(res.data or [])
