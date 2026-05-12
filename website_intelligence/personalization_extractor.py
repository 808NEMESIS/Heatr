"""
website_intelligence/personalization_extractor.py — Extract outreach hooks from websites.

Uses Claude Haiku to analyze homepage + about text and extract:
- Company positioning
- Personalization hooks for outreach
- Website gaps / growth opportunities
- Recent signals (news, blog, events)
"""
from __future__ import annotations

import logging
import re
from typing import Any

from utils.cost_guard import LeadCostAccumulator, guarded_call

logger = logging.getLogger(__name__)

# Conservatieve schatting voor cost_guard pre-check (avg €0.00038/call uit cost-audit).
_ESTIMATED_COST_EUR = 0.0005


async def extract_personalization(
    domain: str,
    page_html: str,
    sector: str,
    anthropic_client: Any,
    supabase_client: Any = None,
    *,
    lead_id: str,
    accumulator: LeadCostAccumulator | None = None,
) -> dict[str, Any]:
    """
    Extract personalization context from a website for outreach.

    Returns dict with:
        positioning (str), hooks (list[str]), observations (list[str]),
        gaps (list[str]), recent_signals (list[str])
    """
    result: dict[str, Any] = {
        "positioning": "",
        "hooks": [],
        "observations": [],
        "gaps": [],
        "recent_signals": [],
    }

    if not page_html:
        return result

    # Strip HTML to text, cap at 3000 chars for token budget
    page_text = _strip_html(page_html)[:3000]

    if len(page_text) < 100:
        return result

    # Soft-warning bij ontbrekende accumulator — per-lead cap kan niet worden
    # gehandhaafd zonder. Eenmalig per call (geen flooding bij batch-runs).
    if accumulator is None:
        logger.warning(
            "personalization_extractor called without accumulator — per-lead cap not enforced. "
            "Pass LeadCostAccumulator instance from job-orchestrator for cap-handhaving."
        )

    # Pre-check via cost_guard (skipt API-call als per-lead/daily/monthly cap raakt).
    workspace_id = (accumulator.workspace_id if accumulator else "aerys")
    allowed, reason = await guarded_call(
        workspace_id=workspace_id,
        lead_id=lead_id,
        context="personalization",
        estimated_cost_eur=_ESTIMATED_COST_EUR,
        supabase_client=supabase_client,
        accumulator=accumulator,
    )
    if not allowed:
        logger.info("personalization: skipped — %s", reason)
        return result

    from utils.claude_cache import cached_claude_call

    prompt = (
        f"Analyseer deze website-tekst van een bedrijf in de sector '{sector}'.\n\n"
        "Geef je antwoord als JSON met deze velden:\n"
        "- positioning: hoe het bedrijf zichzelf positioneert (1 zin)\n"
        "- hooks: 3 concrete observaties bruikbaar als opener in een outreach email (array van strings)\n"
        "- observations: 3-5 relevante observaties over het bedrijf (array)\n"
        "- gaps: website-zwakheden of groei-kansen die je opvallen (array)\n"
        "- recent_signals: recente activiteit of nieuws als je dat ziet (array, mag leeg)\n\n"
        "Return ALLEEN valid JSON, geen andere tekst.\n\n"
        f"Website tekst:\n{page_text}"
    )

    try:
        response_text = await cached_claude_call(
            prompt=prompt,
            cache_key_suffix=f"personalization:{domain}",
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system="Je bent een outbound sales expert. Analyseer websites en vind hooks voor gepersonaliseerde outreach. Antwoord alleen in valid JSON.",
            supabase_client=supabase_client,
            lead_id=lead_id,
        )

        # Charge accumulator met estimate (cached_claude_call returnt alleen text,
        # niet cost). Estimate is conservatief — actuele cost wordt al gelogd
        # in api_cost_log door cached_claude_call zelf. Accumulator tracked
        # per-lead-totaal voor cap-handhaving; lichte overshatting is veiliger
        # dan onderschatting.
        if accumulator is not None:
            accumulator.charge(_ESTIMATED_COST_EUR, "personalization")

        import json
        text = response_text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)

        parsed = json.loads(text)
        result["positioning"] = parsed.get("positioning") or ""
        result["hooks"] = (parsed.get("hooks") or [])[:5]
        result["observations"] = (parsed.get("observations") or [])[:5]
        result["gaps"] = (parsed.get("gaps") or [])[:5]
        result["recent_signals"] = (parsed.get("recent_signals") or [])[:3]

    except Exception as e:
        logger.warning("Personalization extraction failed for %s: %s", domain, e)

    return result


def _strip_html(html: str) -> str:
    """Strip HTML tags, scripts, styles and normalize whitespace."""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
