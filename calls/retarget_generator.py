"""
calls/retarget_generator.py — de retarget-mail die een afgesloten gesprek
opnieuw opent (de reden om terug te komen).

Twee varianten, gekozen met een HARDE if op report_status (niet met een
prompt-instructie: een model dat zelf mag kiezen of het naar het rapport verwijst
is precies het gat dat hard rule 1 verbiedt):

  with_report  (report_status == 'sent')  -> mag naar de check-up verwijzen,
      want die is aantoonbaar verstuurd. Verwijst naar één punt eruit.
  no_report    (anders, bv. 'skipped')    -> mag het rapport NIET noemen; puur
      een korte terugkomst op wat de prospect zelf zei.

`validate_retarget_sendable` is de fail-closed QA-gate: geen streepjes, geen
superlatief, maximaal 60 woorden, precies één vraag, en — hard rule 1 — geen
rapport/check-up-verwijzing tenzij report_status == 'sent'.

Model: Haiku (kort, herhaalbaar). Never-raise.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)

_HAIKU_MODEL = "claude-haiku-4-5-20251001"
from config.pricing import get_legacy_per_m_eur
_PRICING = get_legacy_per_m_eur(_HAIKU_MODEL)
_COST_PER_M_INPUT = _PRICING["input_per_m_eur"]
_COST_PER_M_OUTPUT = _PRICING["output_per_m_eur"]

_MAX_WORDS = 60
_DASH_RE = re.compile(r"[—–]")
# Verwijzingen naar het rapport/de check-up — verboden in de no_report-variant en
# altijd verboden zolang report_status != 'sent' (hard rule 1).
_REPORT_RE = re.compile(r"\b(check.?up|rapport|analyse|op\s+een\s+pagina|doorgerekend)\b", re.I)
_PITCH_RE = re.compile(
    r"\b(aanbieding|korting|gratis|offerte|pakket|abonnement|nu\s+bestellen|"
    r"schrijf\s+je\s+in|boek\s+een\s+demo|ontdek|maak\s+kennis)\b", re.I)

_OUTCOME_CONTEXT = {
    "timing":   "De prospect zei dat het nu niet uitkwam maar later mogelijk wel.",
    "no_value": "De prospect zag de waarde nog niet.",
    "stalled":  "Het gesprek liep dood zonder duidelijke nee.",
}


def choose_variant(report_status: str | None) -> str:
    """Harde variant-keuze: alleen 'with_report' als het rapport aantoonbaar uit is."""
    return "with_report" if report_status == "sent" else "no_report"


def validate_retarget_sendable(body: str, variant: str, report_status: str | None) -> tuple[bool, str]:
    """Fail-closed QA-gate voor de retarget-mail.

    Returns (ok, reason). reason in
    {ok, empty, dash, pitch, too_long, no_question, multi_question, unverifiable_report_ref}.
    """
    text = (body or "").strip()
    if not text:
        return False, "empty"
    if _DASH_RE.search(text):
        return False, "dash"
    if _PITCH_RE.search(text):
        return False, "pitch"
    if len(text.split()) > _MAX_WORDS:
        return False, "too_long"
    q = text.count("?")
    if q == 0:
        return False, "no_question"
    if q > 1:
        return False, "multi_question"
    # Hard rule 1: rapport alleen noemen als het echt verstuurd is, en nooit in de
    # no_report-variant.
    if _REPORT_RE.search(text) and (variant != "with_report" or report_status != "sent"):
        return False, "unverifiable_report_ref"
    return True, "ok"


async def generate_retarget_mail(
    call: dict,
    lead: dict,
    anthropic_client: Any | None = None,
    supabase_client: Any | None = None,
) -> dict:
    """Genereer de retarget-mail. Never-raise -> altijd een dict, 'error' bij falen.

    Args:
        call: heatr_call_records-dict (outcome, report_status, report_findings,
            timing_target_date).
        lead: heatr_leads-dict (company_name, contact_first_name, city).
        anthropic_client: optioneel geïnjecteerde AsyncAnthropic.
        supabase_client: optioneel, voor api_cost_log.

    Returns:
        {"subject", "body", "variant", "tokens_used", "cost_eur"} of {..., "error"}.
    """
    empty = {"subject": None, "body": None, "variant": None, "tokens_used": 0, "cost_eur": 0.0}
    try:
        variant = choose_variant(call.get("report_status"))
        company = (lead.get("company_name") or "de praktijk").strip()
        first = (lead.get("contact_first_name") or "").strip()
        outcome = call.get("outcome") or ""
        context = _OUTCOME_CONTEXT.get(outcome, "Een eerder gesprek zonder afronding.")

        if variant == "with_report":
            findings = call.get("report_findings") or []
            points = "; ".join(str(f.get("title") or "") for f in findings if isinstance(f, dict))[:300]
            task = (
                f"Je stuurde deze praktijk eerder een check-up met deze punten: {points}. "
                f"Schrijf een korte opvolgmail die naar EEN van die punten terugverwijst en "
                f"vraagt of het inmiddels het bekijken waard is. Je mag naar de check-up "
                f"verwijzen (die is echt verstuurd)."
            )
        else:
            task = (
                f"Schrijf een korte opvolgmail die terugkomt op wat de prospect zelf zei. "
                f"Context: {context} "
                f"Noem GEEN rapport, check-up of analyse (dat is er niet). Stel een lichte, "
                f"open vraag of het nu beter uitkomt."
            )

        prompt = (
            f"Je schrijft namens Aerys aan {first or 'de eigenaar'} van {company}.\n\n"
            f"{task}\n\n"
            f"Regels (streng):\n"
            f"- Maximaal {_MAX_WORDS} woorden\n"
            f"- Precies EEN vraag\n"
            f"- Geen superlatieven, geen verkooppraatje, geen call to action\n"
            f"- Gebruik geen liggende streepjes of gedachtestreepjes, in geen enkele vorm\n"
            f"- Nederlands, droog, zakelijk. Onderdrijf liever\n"
            f"- Begin niet met 'Ik'\n\n"
            f"Return ALLEEN JSON: {{\"subject\": \"...\", \"body\": \"...\"}}"
        )

        if anthropic_client is None:
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                return {**empty, "variant": variant, "error": "no_anthropic_key"}
            import anthropic
            anthropic_client = anthropic.AsyncAnthropic(api_key=api_key)

        try:
            response = await anthropic_client.messages.create(
                model=_HAIKU_MODEL,
                max_tokens=400,
                temperature=0.5,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("retarget: Haiku-call faalde voor %s: %s", company, e)
            return {**empty, "variant": variant, "error": f"claude_failed: {str(e)[:100]}"}

        text = response.content[0].text.strip() if response.content else ""
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        import json
        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning("retarget: JSON-parse faalde voor %s: %s", company, e)
            return {**empty, "variant": variant, "error": f"parse_failed: {str(e)[:100]}"}

        subject = (parsed.get("subject") or "").strip()
        body = (parsed.get("body") or "").strip()

        input_tokens = response.usage.input_tokens or 0
        output_tokens = response.usage.output_tokens or 0
        cost_eur = round((input_tokens * _COST_PER_M_INPUT + output_tokens * _COST_PER_M_OUTPUT) / 1_000_000, 6)

        if supabase_client is not None:
            try:
                supabase_client.table("api_cost_log").insert({
                    "workspace_id": lead.get("workspace_id") or "aerys",
                    "model": _HAIKU_MODEL, "prompt_tokens": input_tokens,
                    "response_tokens": output_tokens, "cost_eur": cost_eur,
                    "context": "retarget_mail", "lead_id": lead.get("id"),
                }).execute()
            except Exception as e:  # noqa: BLE001
                logger.debug("retarget cost log faalde: %s", e)

        return {"subject": subject, "body": body, "variant": variant,
                "tokens_used": input_tokens + output_tokens, "cost_eur": cost_eur}
    except Exception as e:  # noqa: BLE001 - never-raise
        logger.error("retarget generator onverwacht gefaald: %s", e)
        return {**empty, "error": f"unexpected: {str(e)[:100]}"}
