"""
campaigns/sequence_engine.py — Email sequence engine for Heatr.

Manages multi-step outreach sequences. Integrates with Warmr for actual
sending and SendingGuard for safety checks before every send.

n8n workflow 01-sequence-due-sends.json calls GET /sequences/due-sends
every 15 minutes to process pending sends.
"""

from __future__ import annotations

import logging
import os
import re
import random
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---- Constants ---------------------------------------------------------------
MAX_SEQUENCE_STEPS      = 4       # including initial email
MIN_WAIT_DAYS           = 2       # minimum delay between steps
SPAM_WORDS = {
    "free", "gratis", "gegarandeerd", "klik hier", "nu kopen",
    "100%", "risico vrij", "risicovrij", "win", "winnaar",
    "geld verdienen", "snel rijk",
}
RECONTACT_COOLDOWN_DAYS = int(os.getenv("RECONTACT_COOLDOWN_DAYS", "90"))

# Banned opener-patterns — Claude-generated openers met deze frasen vallen door
# naar de statische block-fallback. Houd Claude weg van clichés.
_OPENER_BAN_PATTERNS = [
    "als ondernemer",
    "ik begrijp dat",
    "in deze drukke tijd",
    "snelle vraag",
    "korte vraag",
    "ik zal het kort houden",
    "verspil je tijd niet",
    "ik weet dat je het druk hebt",
]


def clean_claude_opener(opener: str | None) -> str:
    """Strip Claude-output artifacts vóór quality-check.

    Bewezen problemen uit live data:
      - Markdown-header prefix ('# Voorstel openingszin:' of '## Opener:')
      - Markdown-header in midden (zelden, maar geheel weghalen)
      - Triple-fence code blocks die soms wrapped worden
      - Leading/trailing whitespace
    """
    if not opener:
        return ""
    text = opener.strip()
    # Strip markdown code-fences ```...```
    if text.startswith("```"):
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    # Strip leading markdown-headers (#, ##, ### lines vóór echte body)
    while text.startswith("#"):
        first_break = text.find("\n")
        if first_break < 0:
            break
        text = text[first_break + 1:].lstrip()
    # Strip "Opener:" / "Openingszin:" / "Voorstel:" prefix-labels
    text = re.sub(
        r"^(opener|openingszin|voorstel(\s+openingszin)?|optie\s*\d+)\s*[:\-—]\s*",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    return text


def is_quality_opener(opener: str | None) -> tuple[bool, str]:
    """Return (is_quality, reason) — beslist of een Claude-gegenereerde opener
    bruikbaar is voor Mail 1.

    Criteria:
      - Minstens 25 woorden (anders: te dun voor Mail 1)
      - Maximaal 90 woorden (cap per v1.0 spec)
      - Geen banned cliché-frasen (case-insensitive)
      - Geen leeg / None
      - Geen onafgemaakte placeholder ({{...}})
      - Eindigt op terminal punctuation (`.`, `!`, `?`, `…`) — anders mid-zin
        afgekapt door max_tokens (live bug live ontdekt: opener stopte op
        "...over de kwaliteit van uw werk en de waar" — afgekapt-tekst
        triggert direct unsubscribe).
      - Geen markdown headers (# / ##) of code-fences (```)
    """
    if not opener or not opener.strip():
        return False, "empty"
    # Clean eerst — als clean dingen verwijdert, valideren we de schoongemaakte versie
    cleaned = clean_claude_opener(opener)
    if cleaned != opener.strip():
        # Caller moet `cleaned` gebruiken, niet origineel — signaleer dit
        # door een speciale reason zodat caller weet er was opschoning nodig
        pass
    text = cleaned
    if not text:
        return False, "empty_after_cleanup"
    if "{{" in text and "}}" in text:
        return False, "unresolved_placeholder"
    if "#" == text[:1] or "##" in text:
        return False, "contains_markdown_header"
    word_count = len(re.findall(r"\w+", text))
    if word_count < 25:
        return False, f"too_short:{word_count}_words"
    if word_count > 90:
        return False, f"too_long:{word_count}_words"
    # Truncation-check: laatste niet-whitespace karakter moet terminal punctuation zijn
    last_char = text.rstrip()[-1:] if text.rstrip() else ""
    if last_char not in ".!?…\"'»":
        return False, f"truncated:no_terminal_punct (last_char={last_char!r})"
    text_lower = text.lower()
    for pattern in _OPENER_BAN_PATTERNS:
        if pattern in text_lower:
            return False, f"banned_pattern:{pattern}"
    return True, "ok"


# ==============================================================================
# Validation
# ==============================================================================

def validate_sequence_config(steps: list[dict]) -> tuple[bool, list[str]]:
    """
    Validate a sequence configuration before creating a campaign.

    Args:
        steps: List of sequence step dicts with keys:
               subject, body, delay_days (0 for first step)

    Returns:
        (is_valid: bool, errors: list[str])
        errors is empty when is_valid is True.
    """
    errors: list[str] = []

    if not steps:
        errors.append("Sequence heeft minimaal 1 stap nodig.")
        return False, errors

    if len(steps) > MAX_SEQUENCE_STEPS:
        errors.append(
            f"Sequence heeft maximaal {MAX_SEQUENCE_STEPS} stappen "
            f"(inclusief eerste email). Je hebt {len(steps)} stappen."
        )

    for i, step in enumerate(steps):
        label = f"Stap {i + 1}"

        # Subject required
        subject = (step.get("subject") or "").strip()
        if not subject:
            errors.append(f"{label}: onderwerp is verplicht.")
        else:
            # Spam word check in subject
            subject_lower = subject.lower()
            found_spam = [w for w in SPAM_WORDS if w in subject_lower]
            if found_spam:
                errors.append(
                    f"{label}: onderwerp bevat spam-gevoelige woorden: {', '.join(found_spam)}."
                )

        # Body required + minimum length
        body = (step.get("body") or "").strip()
        if not body:
            errors.append(f"{label}: berichttekst is verplicht.")
        else:
            # Steps die {{opener}} bevatten krijgen hun woorden server-side
            # ingespoten door Warmr (per-lead observation paragraph). Skip de
            # woordcount voor die steps — anders is een uniforme frame altijd te kort.
            if "{{opener}}" in body:
                pass
            else:
                word_count = len(re.findall(r"\w+", body))
                if word_count < 50:
                    errors.append(
                        f"{label}: berichttekst heeft minimaal 50 woorden "
                        f"(nu {word_count} woorden)."
                    )

        # Wait days — first step exempt, follow-ups minimum MIN_WAIT_DAYS
        if i > 0:
            wait = int(step.get("delay_days") or step.get("wait_days") or 0)
            if wait < MIN_WAIT_DAYS:
                errors.append(
                    f"{label}: wachttijd moet minimaal {MIN_WAIT_DAYS} dagen zijn "
                    f"(nu {wait} dag(en)). Automatisch verhoogd bij uitvoering."
                )
                # Not a hard block, just a warning-as-error per spec
                # Caller can decide to auto-fix

    is_valid = len(errors) == 0
    return is_valid, errors


def auto_fix_sequence_config(steps: list[dict]) -> list[dict]:
    """
    Auto-fix correctable issues in a sequence config.
    Currently fixes: delay_days < MIN_WAIT_DAYS → set to MIN_WAIT_DAYS.
    Returns a new list — does not mutate input.
    """
    fixed = []
    for i, step in enumerate(steps):
        s = dict(step)
        if i > 0:
            wait = int(s.get("delay_days") or s.get("wait_days") or 0)
            if wait < MIN_WAIT_DAYS:
                s["delay_days"] = MIN_WAIT_DAYS
                logger.info("Sequence: auto-fixed step %d delay %d→%d days", i + 1, wait, MIN_WAIT_DAYS)
        fixed.append(s)
    return fixed


# ==============================================================================
# Variable injection + spintax
# ==============================================================================

def resolve_spintax(text: str, rng: random.Random | None = None) -> str:
    """
    Resolve {option1|option2|option3} spintax in text.
    Picks one option per group.

    Args:
        rng: optionele deterministische generator. Zonder rng valt het terug
            op de module-`random` (niet-deterministisch). Sends leveren ALTIJD
            een geseede rng (zie render_step) zodat één logische send (lead +
            stap) reproduceerbaar dezelfde body oplevert — de kern van
            invariant I8. Cross-lead blijft de tekst variëren (andere seed per
            lead) voor deliverability.
    """
    chooser = rng or random

    def pick(match: re.Match) -> str:
        options = match.group(1).split("|")
        return chooser.choice(options).strip()

    return re.sub(r"\{([^{}]+)\}", pick, text)


def inject_variables(text: str, lead: dict) -> str:
    """
    Replace {{variable}} placeholders with lead data.

    v1.0 tokens (legacy):
      {{first_name}}  {{company}}  {{city}}  {{opener}}  {{sector}}
      {{website}}     {{score}}

    v3.1 tokens (brug-based templates, 2026-05-07):
      {{bedrijfsnaam}}              — alias voor {{company}}, NL-friendly
      {{stad}}                       — alias voor {{city}}
      {{primaire_dienstverlening}}   — treatment_focus / industry / sector fallback
      {{signaal_blok}}               — korte signaal-zin uit pick_observation_block
      {{sector_noemer}}              — 'cliniek-ondernemers' / 'praktijken' / etc.
      {{LOOM_LINK}}, {{VIDEO_LINK}}  — TODO leeg in v1, handmatig pre-send

    `first_name` gaat door utils.lead_naming.display_first_name zodat email-
    inference-artefacten ('Ceciledebooij' uit gmail-local-part) niet in mail
    bodies belanden. Fallback: 'daar' (informeel-neutraal).
    """
    from utils.lead_naming import display_first_name
    from utils.signal_picker import pick_signaal_blok
    from utils.sector_impact import pick_sector_impact_frame
    from config.sequence_templates import (
        primaire_dienstverlening_for_lead,
        sector_noemer_for_lead,
    )

    safe_first = display_first_name(lead, fallback="daar")
    company = lead.get("company_name") or lead.get("domain") or ""

    # Spec 2 (2026-07-20) — voornaam-fallback zonder aanhef-gat. Met naam
    # "Hoi Jan,"; zonder naam "Hoi," (de opener draagt dan de personalisatie).
    # 29,4% van de leads mist contact_first_name → geen zwak "Hoi daar,".
    _real_first = display_first_name(lead, fallback="")
    begroeting = f"Hoi {_real_first}," if _real_first else "Hoi,"

    # v3.2 {{stad_of_sector}}: city-naam tenzij leeg/generiek, anders "jullie sector".
    # "Generiek" = niet bruikbaar in opener-zin "ondernemers in [city] met lokale impact".
    raw_city = (lead.get("city") or "").strip()
    _GENERIC_CITY_VALUES = {"", "nederland", "nl", "onbekend", "unknown"}
    stad_of_sector = raw_city if raw_city.lower() not in _GENERIC_CITY_VALUES else "jullie sector"

    # Resolve signaal_blok eerst, want zinbegin-pattern (Brug 3 frame:
    # ". {{signaal_blok}}.") vereist capitalize-first voor grammaticale
    # correctheid (Tier 5+6 starten met kleine letter, mid-zin natuurlijk).
    # Mid-zin {{signaal_blok}} occurrences (Brug 1+2) blijven kleine letter.
    signaal = pick_signaal_blok(lead)
    if signaal and ". {{signaal_blok}}" in text:
        capitalized = signaal[0].upper() + signaal[1:]
        text = text.replace(". {{signaal_blok}}", f". {capitalized}", 1)

    # Spec 1 (2026-07-20) — {{opener}} rendert de QA-gate Haiku-observatie; valt
    # terug op {{signaal_blok}} als er geen opener is. Vangnet: een reeds-
    # opgeslagen opener mét em-/en-dash (86/954 stale) mag nooit uitgaan → ook
    # dan fallback, zodat de mail nooit een em-dash bevat vóór de regenerate-run.
    _opener_raw = (lead.get("personalized_opener") or "").strip()
    if _opener_raw and "—" not in _opener_raw and "–" not in _opener_raw:
        opener_val = _opener_raw
    elif signaal:
        # signaal_blok is een mid-zin-fragment → als standalone opener-zin
        # kapitaliseren + afsluiten met een punt.
        opener_val = signaal[0].upper() + signaal[1:]
        if opener_val[-1] not in ".!?":
            opener_val += "."
    else:
        opener_val = ""

    replacements = {
        # v1.0 — legacy
        "{{first_name}}":              safe_first,
        "{{begroeting}}":              begroeting,
        "{{company}}":                 company,
        "{{city}}":                    lead.get("city") or "",
        "{{opener}}":                  opener_val,
        "{{sector}}":                  lead.get("sector") or "",
        "{{website}}":                 f"https://{lead.get('domain')}" if lead.get("domain") else "",
        "{{score}}":                   str(lead.get("website_score") or ""),
        # v3.1 — brug-based
        "{{bedrijfsnaam}}":            company,
        "{{stad}}":                    lead.get("city") or "",
        "{{primaire_dienstverlening}}": primaire_dienstverlening_for_lead(lead),
        "{{signaal_blok}}":            signaal,
        "{{sector_noemer}}":           sector_noemer_for_lead(lead),
        # v3.2 — sector-impact frame + stad-of-sector fallback
        "{{stad_of_sector}}":          stad_of_sector,
        "{{sector_impact_frame}}":     pick_sector_impact_frame(lead.get("sector")),
        # Fase A (2026-07-20) — degradatie-tokens (spec 4/5, nu meestal leeg →
        # engine kiest de degradation-variant) + live plekken-teller (spec 3).
        "{{detail_2}}":                lead.get("detail_2") or "",
        "{{concurrent_signaal}}":      lead.get("concurrent_signaal") or "",
        "{{vrije_plekken}}":           str(lead.get("vrije_plekken")) if lead.get("vrije_plekken") is not None else "",
        # LOOM/VIDEO — geen kolom, geen invulmechanisme (outreach-reparatie
        # 2026-07-18, Sami-keuze: Loom NIET in de eerste campagne). Het blok
        # wordt hieronder CONDITIONEEL weggelaten als de link leeg is, zodat er
        # geen kale regel in de mail belandt. Wel een link (kolom + mechanisme,
        # aparte klus) -> rendert gewoon.
        "{{LOOM_LINK}}":               lead.get("loom_link") or "",
        "{{VIDEO_LINK}}":              lead.get("video_link") or "",
    }
    # Conditionele blokken: een placeholder die op een eigen regel staat en leeg
    # rendert wordt mét zijn witregel verwijderd i.p.v. een lege regel achter te
    # laten ("{{LOOM_LINK}}\n\n" in de v3.1-bodies).
    _conditional = ("{{LOOM_LINK}}", "{{VIDEO_LINK}}")
    for placeholder in _conditional:
        if not replacements.get(placeholder):
            text = text.replace(placeholder + "\n\n", "").replace(placeholder + "\n", "")
    for placeholder, value in replacements.items():
        text = text.replace(placeholder, value or "")
    return text


def render_step(step: dict, lead: dict, *, seed: str | None = None) -> dict:
    """
    Render a single sequence step for a specific lead.

    Dit is de ENIGE plek waar sequence-content wordt gerenderd (invariant I8,
    Sprint 3). Volgorde: variable-injection EERST (`{{name}}` → value), spintax
    DAARNA (`{a|b}` → keuze). Andersom zou de regex `{[^{}]+}` van spintax de
    inhoud van `{{opener}}` opsnoepen voordat injection erbij kan.

    Args:
        seed: deterministische spintax-seed. Sends geven `{lead_id}:{step_index}`
            mee (zie process_due_send) zodat dezelfde logische send altijd exact
            dezelfde body oplevert — nodig om de body één keer te bevriezen in
            het outbound-ledger (I7) en om "één send = één body" te bewijzen.
            Zonder seed (previews/tests-zonder-eis) blijft het niet-deterministisch.

    Returns:
        { "subject": str, "body": str, "delay_days": int }
    """
    rng = random.Random(seed) if seed is not None else None
    subject = resolve_spintax(inject_variables(step.get("subject") or "", lead), rng)
    body    = resolve_spintax(inject_variables(step.get("body")    or "", lead), rng)
    return {
        "subject":    subject,
        "body":       body,
        "delay_days": int(step.get("delay_days") or 0),
    }


async def free_founding_five_slots(
    niche: str, supabase_client, workspace_id: str, total: int | None = None
) -> int:
    """Vrije Founding-Five-plekken voor een niche (= lead.sector), spec 3.

    Drempel = getekende deal: elke rij in heatr_founding_five_slots is een
    vergeven plek. vrije = max(0, TOTAAL - getekend). Per niche geteld, want
    Founding Five geldt per niche (cosmetisch/chiro apart).

    Fail-safe: kan de teller niet gelezen worden, ga dan uit van 0 vergeven
    (= alle plekken vrij) — dat is de begin-realiteit (niets getekend) en houdt
    de mail leesbaar; nooit een verzonnen tekort forceren.
    """
    from config.sequence_templates import FOUNDING_FIVE_TOTAL
    cap = FOUNDING_FIVE_TOTAL if total is None else total
    try:
        res = (supabase_client.table("founding_five_slots")
               .select("id", count="exact")
               .eq("workspace_id", workspace_id).eq("niche", niche).execute())
        taken = res.count or 0
    except Exception as e:
        logger.warning("free_founding_five_slots: teller onleesbaar (%s) — 0 vergeven aangenomen", e)
        taken = 0
    return max(0, cap - taken)


# ==============================================================================
# Due-send processing
# ==============================================================================

async def get_due_sends(workspace_id: str, supabase_client, limit: int = 50) -> list[dict]:
    """
    Return lead_campaign_history rows where next send is due.
    Called by n8n every 15 minutes.

    Returns list of pending send records with full lead + sequence data.
    """
    now = datetime.now(timezone.utc).isoformat()
    try:
        res = (
            supabase_client.table("lead_campaign_history")
            .select("*, leads(id, company_name, city, sector, email, status, gdpr_safe, "
                    "contact_first_name, domain, personalized_opener, snoozed_until, "
                    "next_contact_after, crm_stage)")
            .eq("workspace_id", workspace_id)
            # ADR-001 (fase 3): HARDE grens tegen dubbele drip. Warmr-owned
            # tracking-rijen (send_owner='warmr', geschreven door launch)
            # mogen NOOIT door dit pad verstuurd worden — Warmr dript zelf.
            # Alleen expliciete heatr-owned rijen zijn dispatch-kandidaat.
            .eq("send_owner", "heatr")
            .eq("status", "pending")
            .eq("is_active", True)
            .lte("next_send_at", now)
            .order("next_send_at", desc=False)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.error("get_due_sends failed: %s", e)
        return []


async def process_due_send(
    send_record: dict,
    supabase_client,
    warmr_client=None,
) -> dict[str, Any]:
    """
    Process a single pending send from lead_campaign_history.

    1. Run SendingGuard checks
    2. Render sequence step for this lead
    3. Push to Warmr (or skip if dry_run)
    4. Update lead_campaign_history: status, step_index, next_send_at
    5. Log to lead_timeline

    Returns:
        { "sent": bool, "reason": str, "lead_id": str }
    """
    from utils.sending_guard import SendingGuard
    from integrations.warmr_client import WarmrClient

    lead = send_record.get("leads") or {}
    lead_id     = lead.get("id") or send_record.get("lead_id")
    inbox_id    = send_record.get("inbox_id") or send_record.get("preferred_inbox_id")
    workspace_id = send_record.get("workspace_id")
    record_id   = send_record.get("id")

    # Safety check
    guard = SendingGuard()
    can_send, block_reason = await guard.check_can_send(
        lead_id=lead_id,
        inbox_id=inbox_id or "",
        workspace_id=workspace_id,
        supabase_client=supabase_client,
    )

    if not can_send:
        logger.info("Send blocked for lead %s: %s", lead_id, block_reason)
        _mark_send_blocked(record_id, block_reason, supabase_client)
        return {"sent": False, "reason": block_reason, "lead_id": lead_id}

    # Load sequence steps from campaign config
    sequence_steps = send_record.get("sequence_steps") or []
    step_index = int(send_record.get("step_index") or 0)

    if step_index >= len(sequence_steps):
        # Sequence complete
        await _complete_sequence(record_id, lead_id, workspace_id, supabase_client)
        return {"sent": False, "reason": "sequence_complete", "lead_id": lead_id}

    # Deterministische seed per (lead, stap): dezelfde logische send rendert
    # altijd dezelfde body — voorwaarde om 'm één keer te bevriezen in het
    # ledger (I7) en om invariant I8 te bewijzen. restart_epoch zit BEWUST niet
    # in de seed: een restart moet dezelfde content herzenden, niet nieuwe.
    step = render_step(
        sequence_steps[step_index], lead,
        seed=f"{lead_id}:{step_index}",
    )

    # Push to Warmr — via de dispatcher (I3/I6/I7). Dit pad draait autonoom
    # (n8n elke 15 min); de idempotency-key record:step:epoch garandeert dat
    # dezelfde stap nooit twee keer verstuurd wordt (dubbele n8n-tick,
    # overlappende workers), terwijl een bewuste operator-restart de
    # restart_epoch bumpt en daarmee een nieuwe key krijgt.
    from utils.outbound_dispatcher import DispatchBlocked, dispatch_outbound
    try:
        wc = warmr_client or WarmrClient()
        campaign_id = send_record.get("campaign_id")
        restart_epoch = int(send_record.get("restart_epoch") or 0)
        disp = await dispatch_outbound(
            kind="warmr_push",
            idempotency_key=f"seq-send:{record_id}:step:{step_index}:epoch:{restart_epoch}",
            actor="scheduler:sequence-dispatch",
            lead=lead,
            send=lambda: wc.push_lead(
                lead,
                campaign_id=campaign_id,
                preferred_inbox_id=inbox_id,
                custom_subject=step["subject"],
                custom_body=step["body"],
            ),
            supabase_client=supabase_client,
            workspace_id=workspace_id,
            # De gerenderde subject+body worden bevroren in heatr_outbound_log:
            # wat de deur uitging is achteraf exact reproduceerbaar (I7), zonder
            # afhankelijk te zijn van een re-render tegen mogelijk-gewijzigde
            # templates. Dit is Heatr's render als bron van waarheid (I8).
            metadata={
                "record_id": record_id, "step_index": step_index,
                "rendered": {"subject": step["subject"], "body": step["body"]},
                "render_owner": "heatr",
            },
        )
        if disp.skipped_duplicate:
            # Stap al verstuurd (bv. dubbele n8n-tick): NIET nog eens pushen,
            # wél doorschuiven zodat de sequence niet blijft hangen op een
            # al-verzonden stap.
            logger.warning(
                "Sequence-send geskipt als duplicate (record=%s step=%d) — "
                "stap was al verstuurd op %s", record_id, step_index,
                (disp.previous or {}).get("created_at"),
            )
    except DispatchBlocked as e:
        logger.error("Sequence-send compliance-blocked voor lead %s: %s", lead_id, e)
        _mark_send_blocked(record_id, str(e), supabase_client)
        return {"sent": False, "reason": f"compliance_blocked: {e}", "lead_id": lead_id}
    except Exception as e:
        logger.error("Warmr push failed for lead %s: %s", lead_id, e)
        _mark_send_error(record_id, str(e), supabase_client)
        return {"sent": False, "reason": f"warmr_error: {e}", "lead_id": lead_id}

    # Advance sequence
    next_step_idx = step_index + 1
    is_last = next_step_idx >= len(sequence_steps)

    if is_last:
        next_send_at = None
        status = "sequence_complete"
    else:
        wait_days = int(sequence_steps[next_step_idx].get("delay_days") or MIN_WAIT_DAYS)
        wait_days = max(wait_days, MIN_WAIT_DAYS)
        next_send_at = (datetime.now(timezone.utc) + timedelta(days=wait_days)).isoformat()
        status = "pending"

    try:
        supabase_client.table("lead_campaign_history").update({
            "status": status,
            "step_index": next_step_idx,
            "next_send_at": next_send_at,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", record_id).execute()
    except Exception as e:
        logger.error("Failed to advance sequence record %s: %s", record_id, e)

    # Increment contact attempt count
    try:
        supabase_client.table("leads").update({
            "contact_attempt_count": (lead.get("contact_attempt_count") or 0) + 1,
        }).eq("id", lead_id).execute()
    except Exception:
        pass

    # Timeline entry
    _log_timeline_event(
        supabase_client, workspace_id, lead_id,
        "email_sent",
        f"Email verzonden: {step['subject']} (stap {step_index + 1})",
        metadata={"step_index": step_index, "campaign_id": send_record.get("campaign_id")},
    )

    return {"sent": True, "reason": "ok", "lead_id": lead_id}


async def _complete_sequence(
    record_id: str,
    lead_id: str,
    workspace_id: str,
    db,
) -> None:
    """Mark sequence as complete, set lead to no_response + cooldown."""
    recontact_after = (
        datetime.now(timezone.utc) + timedelta(days=RECONTACT_COOLDOWN_DAYS)
    ).isoformat()

    try:
        db.table("lead_campaign_history").update({
            "status": "sequence_complete",
            "is_active": False,
        }).eq("id", record_id).execute()
    except Exception as e:
        logger.warning("_complete_sequence: history update failed: %s", e)

    try:
        db.table("leads").update({
            "status": "no_response",
            "next_contact_after": recontact_after,
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.warning("_complete_sequence: leads update failed: %s", e)

    _log_timeline_event(
        db, workspace_id, lead_id, "sequence_completed",
        f"Sequence afgerond — heractivatie mogelijk na {RECONTACT_COOLDOWN_DAYS} dagen",
        metadata={"recontact_after": recontact_after},
    )


async def stop_all_sequences_for_lead(lead_id: str, workspace_id: str, db) -> int:
    """
    Cancel all active sequences for a lead (used on unsubscribe/forget).
    Returns count of stopped sequences.
    """
    try:
        res = db.table("lead_campaign_history").update({
            "status": "unsubscribed",
            "is_active": False,
        }).eq("lead_id", lead_id).eq("workspace_id", workspace_id).eq("is_active", True).execute()
        stopped = len(res.data or [])
        logger.info("stop_all_sequences: stopped %d sequences for lead %s", stopped, lead_id)
        return stopped
    except Exception as e:
        logger.error("stop_all_sequences failed for lead %s: %s", lead_id, e)
        return 0


def _mark_send_blocked(record_id: str, reason: str, db) -> None:
    try:
        db.table("lead_campaign_history").update({
            "status": "blocked",
            "block_reason": reason,
        }).eq("id", record_id).execute()
    except Exception:
        pass


def _mark_send_error(record_id: str, error: str, db) -> None:
    try:
        db.table("lead_campaign_history").update({
            "status": "error",
            "block_reason": error,
        }).eq("id", record_id).execute()
    except Exception:
        pass


def _log_timeline_event(db, workspace_id, lead_id, event_type, title, metadata=None) -> None:
    try:
        db.table("lead_timeline").insert({
            "workspace_id": workspace_id,
            "lead_id": lead_id,
            "event_type": event_type,
            "title": title,
            "metadata": metadata or {},
            "created_by": "sequence_engine",
        }).execute()
    except Exception as e:
        logger.debug("Timeline log failed: %s", e)


# ==============================================================================
# Snooze wake-up
# ==============================================================================

async def wake_snoozed_leads(workspace_id: str, supabase_client) -> int:
    """
    Move leads from 'later' stage back to previous stage when snooze expires.
    Called by n8n workflow 02-snooze-wakeup every 15 minutes.

    Returns count of woken leads.
    """
    now = datetime.now(timezone.utc).isoformat()
    try:
        res = (
            supabase_client.table("leads")
            .update({"crm_stage": "ontdekt", "snoozed_until": None})
            .eq("workspace_id", workspace_id)
            .eq("crm_stage", "later")
            .lte("snoozed_until", now)
            .execute()
        )
        woken = len(res.data or [])
        if woken:
            logger.info("wake_snoozed_leads: %d leads woken in workspace %s", woken, workspace_id)
        return woken
    except Exception as e:
        logger.error("wake_snoozed_leads failed: %s", e)
        return 0


async def reactivate_snoozed_tasks(workspace_id: str, supabase_client) -> int:
    """
    Reactivate snoozed tasks whose snooze_until has passed.
    Called by n8n workflow 02-snooze-wakeup every 15 minutes.
    """
    now = datetime.now(timezone.utc).isoformat()
    try:
        res = (
            supabase_client.table("crm_tasks")
            .update({"status": "open", "snoozed_until": None})
            .eq("workspace_id", workspace_id)
            .eq("status", "snoozed")
            .lte("snoozed_until", now)
            .execute()
        )
        count = len(res.data or [])
        return count
    except Exception as e:
        logger.error("reactivate_snoozed_tasks failed: %s", e)
        return 0
