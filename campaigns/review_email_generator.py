"""
campaigns/review_email_generator.py — AI-gegenereerde review emails voor outreach.

Genereert een korte, persoonlijke Nederlandse email (max 90 woorden) die
verwijst naar één specifiek probleem op de website van de lead. Deze email
is de "Trojan Horse" van de Aerys outreach strategie: een gratis website
review als eerste gesprek.

CLAUDE.md regel 268-292 spec:
  - Van: {sender_name} van Aerys
  - Aan: {contact_name} van {company_name} in {city}
  - Website score, grootste probleem, marktpositie, specifieke observatie
  - Regels:
    * Begin NIET met 'Ik'
    * Stel ÉÉN concrete vraag
    * Geen verkooppraatje
    * Verwijs naar één specifiek probleem
    * Eindig open

Cost: 1 Claude Haiku call per email (~€0.00020)
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)


# Claude Haiku model — latest as of 2026-04
_HAIKU_MODEL = "claude-haiku-4-5-20251001"

# Pricing centraal in config/pricing.py (single source of truth).
from config.pricing import get_legacy_per_m_eur
_PRICING = get_legacy_per_m_eur(_HAIKU_MODEL)
_COST_PER_M_INPUT = _PRICING["input_per_m_eur"]
_COST_PER_M_OUTPUT = _PRICING["output_per_m_eur"]


# =============================================================================
# Top issue derivation (rule-based, no Claude)
# =============================================================================

def _derive_top_issue(
    website_intelligence: dict,
    *,
    allow_automatisering: bool = True,
) -> str:
    """Derive the single most impactful website issue from WI data.

    Priority order (highest impact first):
      1. Technical score < 10 → 'verouderde techniek'
      2. Conversion score < 10 → 'ontbrekende conversie-elementen'
      3. Sector score < 7 → sector-specifiek (eerste falende check)
      4. Default → 'website uit de tijd'
    """
    if not website_intelligence:
        return "website uit de tijd"

    tech_score = website_intelligence.get("technical_score") or 0
    conv_score = website_intelligence.get("conversion_score") or 0
    sector_score = website_intelligence.get("sector_score") or 0

    if tech_score < 10:
        return "verouderde techniek"

    if conv_score < 10:
        conv_details = website_intelligence.get("conversion_details") or {}
        if not conv_details.get("has_online_booking"):
            return "geen online boekingsmogelijkheid"
        if not conv_details.get("has_whatsapp"):
            return "geen WhatsApp voor klanten"
        # De chat/AI-angle is automatisering (compliance-gevoelig op medische
        # praktijksites) → alleen als de sector 'm toestaat.
        if allow_automatisering and not conv_details.get("has_chatbot"):
            return "geen chat voor snelle vragen"
        return "ontbrekende conversie-elementen"

    if sector_score < 7:
        sector_details = website_intelligence.get("sector_details") or {}
        checks = sector_details.get("checks") or []
        failing = [c for c in checks if not c.get("passed") and c.get("points", 0) > 0]
        if failing:
            return failing[0].get("label") or "sector-specifieke verbeterpunten"
        return "sector-specifieke verbeterpunten"

    return "website uit de tijd"


def _compose_specific_observation(
    website_intelligence: dict,
    top_issue: str,
) -> str:
    """Return a one-liner with concrete evidence for the top issue."""
    conv = website_intelligence.get("conversion_details") or {}
    tech = website_intelligence.get("technical_details") or {}

    if "techniek" in top_issue.lower():
        cms = tech.get("cms") or "onbekend CMS"
        ssl = tech.get("has_ssl")
        if not ssl:
            return "de site heeft geen SSL-certificaat"
        return f"de site draait nog op {cms}"

    if "boeking" in top_issue.lower():
        return "klanten moeten bellen of mailen om een afspraak te maken"

    if "whatsapp" in top_issue.lower():
        return "bezoekers kunnen alleen via telefoon of formulier contact opnemen"

    if "chat" in top_issue.lower():
        return "er is geen directe manier om een vraag te stellen"

    if conv.get("form_field_count", 0) > 7:
        return f"het contactformulier vraagt {conv['form_field_count']} velden"

    return "enkele concrete verbeterpunten voor conversie"


# =============================================================================
# Main generator
# =============================================================================

async def generate_review_email(
    lead: dict,
    website_intelligence: dict,
    anthropic_client: Any | None = None,
    supabase_client: Any | None = None,
) -> dict:
    """Generate a short, personal Dutch review email for outreach.

    Follows CLAUDE.md regel 273-290 prompt spec. Never raises — always
    returns a dict, with 'error' key on failure.

    Args:
        lead: Full lead dict from heatr_leads. Must contain company_name,
              city. Optional: contact_first_name, sector.
        website_intelligence: Full WI dict from heatr_website_intelligence.
              Uses total_score, technical/conversion/sector details,
              competitor_data.score_vs_market.
        anthropic_client: Optional injected Anthropic client. Defaults to
              creating one from ANTHROPIC_API_KEY env.
        supabase_client: Optional injected Supabase client (for cost log).

    Returns:
        {
            "subject": str,
            "body": str,
            "top_issue": str,
            "specific_observation": str,
            "tokens_used": int,
            "cost_eur": float,
        }
        On failure, adds "error" key with reason.
    """
    try:
        company_name = lead.get("company_name") or ""
        city = lead.get("city") or ""
        contact_name = lead.get("contact_first_name") or "daar"
        sector = lead.get("sector") or ""

        if not company_name:
            return {
                "subject": "",
                "body": "",
                "top_issue": "",
                "specific_observation": "",
                "tokens_used": 0,
                "cost_eur": 0.0,
                "error": "incomplete_lead_data: missing company_name",
            }

        total_score = website_intelligence.get("total_score") or 0
        comp_data = website_intelligence.get("competitor_data") or {}
        score_vs_market = comp_data.get("score_vs_market") or 0

        # Sector-poort: alt-zorg/chiro krijgen nooit een chat/AI-angle.
        from config.sectors import get_allowed_offers
        _allow_auto = "automatisering" in get_allowed_offers(sector)
        top_issue = _derive_top_issue(website_intelligence, allow_automatisering=_allow_auto)
        specific_observation = _compose_specific_observation(
            website_intelligence, top_issue,
        )

        # Init Anthropic client if not injected
        if anthropic_client is None:
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                return {
                    "subject": "",
                    "body": "",
                    "top_issue": top_issue,
                    "specific_observation": specific_observation,
                    "tokens_used": 0,
                    "cost_eur": 0.0,
                    "error": "ANTHROPIC_API_KEY missing",
                }
            import anthropic
            anthropic_client = anthropic.AsyncAnthropic(api_key=api_key)

        sender_name = os.environ.get("SENDER_NAME", "Sami")

        # Build prompt exactly per CLAUDE.md regel 273-290
        market_phrase = (
            f"{abs(score_vs_market)} punten onder concurrenten in {city}"
            if score_vs_market < 0
            else f"vergelijkbaar met concurrenten in {city}"
        )

        prompt = (
            f"Schrijf een email (max 90 woorden) in het Nederlands.\n"
            f"Van: {sender_name} van Aerys\n"
            f"Aan: {contact_name} van {company_name} in {city}\n"
            f"\n"
            f"Website score: {total_score}/100\n"
            f"Grootste probleem: {top_issue}\n"
            f"Marktpositie: {market_phrase}\n"
            f"Specifieke observatie: {specific_observation}\n"
            f"\n"
            f"Regels:\n"
            f"- Begin NIET met 'Ik'\n"
            f"- Stel ÉÉN concrete vraag (eindig met een '?')\n"
            f"- Geen verkooppraatje\n"
            f"- Verwijs naar één specifiek probleem\n"
            f"- Eindig open\n"
            f"\n"
            f"Return ALLEEN JSON (geen andere tekst):\n"
            f'{{"subject": "max 60 tekens, niet generic", "body": "max 90 woorden"}}'
        )

        # Call Claude — use AsyncAnthropic directly (no cache needed,
        # every email is unique per lead)
        try:
            response = await anthropic_client.messages.create(
                model=_HAIKU_MODEL,
                max_tokens=350,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            logger.warning(
                "review_email_generator: Claude call failed for %s: %s",
                company_name, e,
            )
            return {
                "subject": "",
                "body": "",
                "top_issue": top_issue,
                "specific_observation": specific_observation,
                "tokens_used": 0,
                "cost_eur": 0.0,
                "error": f"claude_failed: {str(e)[:100]}",
            }

        text = response.content[0].text.strip() if response.content else ""
        # Strip markdown code blocks
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)

        try:
            parsed = json.loads(text)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(
                "review_email_generator: JSON parse failed for %s: %s",
                company_name, e,
            )
            return {
                "subject": "",
                "body": "",
                "top_issue": top_issue,
                "specific_observation": specific_observation,
                "tokens_used": 0,
                "cost_eur": 0.0,
                "error": f"parse_failed: {str(e)[:100]}",
            }

        subject = (parsed.get("subject") or "").strip()
        body = (parsed.get("body") or "").strip()

        # Post-validation
        if body and body[:2].lower().startswith("ik"):
            logger.info(
                "review_email_generator: body started with 'Ik' — prepending neutral greeting",
            )
            # Lightweight correction — rewrite first sentence to remove 'Ik'
            # rather than re-calling Claude. Preserves the body content.
            body = "Je " + body[2:].lstrip(" ,") if body[:2].lower() == "ik" else body

        # Calculate cost
        input_tokens = response.usage.input_tokens or 0
        output_tokens = response.usage.output_tokens or 0
        tokens_used = input_tokens + output_tokens
        cost_eur = round(
            (input_tokens * _COST_PER_M_INPUT + output_tokens * _COST_PER_M_OUTPUT)
            / 1_000_000,
            6,
        )

        # Log cost (if supabase client available)
        if supabase_client is not None:
            try:
                supabase_client.table("api_cost_log").insert({
                    "workspace_id": lead.get("workspace_id") or "aerys",
                    "model": _HAIKU_MODEL,
                    "prompt_tokens": input_tokens,
                    "response_tokens": output_tokens,
                    "cost_eur": cost_eur,
                    "context": "review_email",
                    "lead_id": lead.get("id"),
                }).execute()
            except Exception as e:
                logger.debug("review_email cost log failed: %s", e)

        logger.info(
            "review_email_generator: %s — issue='%s' cost=EUR %.6f",
            company_name, top_issue, cost_eur,
        )

        return {
            "subject": subject,
            "body": body,
            "top_issue": top_issue,
            "specific_observation": specific_observation,
            "tokens_used": tokens_used,
            "cost_eur": cost_eur,
        }

    except Exception as e:
        # Final safety net — never crash the caller
        logger.error(
            "review_email_generator: unexpected error for %s: %s",
            lead.get("company_name") if isinstance(lead, dict) else "?", e,
        )
        return {
            "subject": "",
            "body": "",
            "top_issue": "",
            "specific_observation": "",
            "tokens_used": 0,
            "cost_eur": 0.0,
            "error": f"unexpected: {str(e)[:100]}",
        }
