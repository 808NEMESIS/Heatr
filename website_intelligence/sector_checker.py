"""
website_intelligence/sector_checker.py — Layer 4: Sector-specific website scoring.

Sectors.py v2 (2026-04-21) bevat geen per-criterium punten meer, alleen een lijst
positieve/negatieve `website_signals`. Deze checker vraagt Claude Haiku om de
website-tekst te classificeren op een 4-tier schaal (A/B/C/D) en converteert
dat naar 0-15 punten, zoals voorzien in Laag 4 van de website intelligence pijp.

Waarom Haiku en niet keyword-matching:
- De signalen in sectors.py zijn hints ("RBCZ", "BIG nummer", etc.) — niet
  harde flags. Een site kan RBCZ in de footer hebben zonder effectief lid te
  zijn, of het kan andersom.
- Haiku leest de tekst en beoordeelt de daadwerkelijke kwaliteit: zijn de
  signalen aanwezig *en geloofwaardig*? Geeft ook een korte uitleg die we
  teruggeven in `rationale`.
- Cost: ~€0.00015 per lead (< 1% van de per-lead budget).

Output contract blijft:
  {
    "sector_score": int (0-15),
    "checks": [{"key", "label", "passed", "points"}, ...],
    "tier": "A" | "B" | "C" | "D",
    "rationale": str,
    "tokens_used": int,
    "cost_eur": float,
  }

`checks` wordt afgeleid uit Haiku's per-signal oordeel (voor UI transparantie).
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

from config.pricing import get_legacy_per_m_eur
from config.sectors import get_sector

logger = logging.getLogger(__name__)

_HAIKU_MODEL = "claude-haiku-4-5-20251001"

# Pricing centraal in config/pricing.py.
_PRICING = get_legacy_per_m_eur(_HAIKU_MODEL)
_COST_PER_M_INPUT = _PRICING["input_per_m_eur"]
_COST_PER_M_OUTPUT = _PRICING["output_per_m_eur"]

_TIER_TO_POINTS: dict[str, int] = {"A": 15, "B": 10, "C": 5, "D": 0}


async def check_sector_specific(
    domain: str,
    page_html: str,
    sector_key: str,
    anthropic_client: Any | None = None,
    supabase_client: Any | None = None,
) -> dict[str, Any]:
    """Classify a lead's website against sector-specific website_signals.

    Uses Claude Haiku to judge tier A-D based on positive/negative signals
    from config/sectors.py. Maps tier to 0-15 points.

    Never raises — returns partial dict on any error so the caller (analyzer)
    can continue to later layers.
    """
    result: dict[str, Any] = {
        "sector_score": 0,
        "checks": [],
        "tier": "D",
        "rationale": "",
        "tokens_used": 0,
        "cost_eur": 0.0,
    }

    try:
        sector = get_sector(sector_key)
    except ValueError:
        logger.info("sector_checker: unknown sector %s — skipping", sector_key)
        return result

    signals = sector.get("website_signals") or {}
    positive_signals: list[str] = signals.get("positive") or []
    negative_signals: list[str] = signals.get("negative") or []

    if not positive_signals:
        logger.debug("sector_checker: no website_signals for %s — scoring 0", sector_key)
        return result

    text = _html_to_text(page_html)
    if not text.strip():
        return result

    # Init Anthropic client if not injected
    if anthropic_client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            logger.warning("sector_checker: ANTHROPIC_API_KEY missing — returning 0")
            return result
        try:
            import anthropic
            anthropic_client = anthropic.AsyncAnthropic(api_key=api_key)
        except Exception as e:
            logger.warning("sector_checker: anthropic init failed: %s", e)
            return result

    # Build prompt — cap text to keep cost bounded
    text_capped = text[:6000]
    prompt = (
        f"Je beoordeelt een website uit de sector '{sector.get('label') or sector_key}'.\n"
        f"\n"
        f"Positieve signalen (kenmerken van hoogwaardige praktijken):\n"
        f"{', '.join(positive_signals)}\n"
        f"\n"
        f"Negatieve signalen (rode vlaggen):\n"
        f"{', '.join(negative_signals) if negative_signals else '(geen)'}\n"
        f"\n"
        f"Website tekst (eerste ~6000 tekens):\n"
        f'"""\n{text_capped}\n"""\n'
        f"\n"
        f"Classificeer deze website op één van vier tiers:\n"
        f"- A: veel positieve signalen zichtbaar, geloofwaardig gepresenteerd, geen rode vlaggen\n"
        f"- B: meerdere positieve signalen, maar niet compleet of half zichtbaar\n"
        f"- C: enkele signalen, veel ontbreekt\n"
        f"- D: bijna geen signalen of duidelijke rode vlaggen\n"
        f"\n"
        f"Lijst ook per positief signaal of je hem daadwerkelijk zag ('yes'/'no').\n"
        f"Rationale in 1 zin (NL), concreet met voorbeeld uit de tekst.\n"
        f"\n"
        f"Return ALLEEN JSON:\n"
        f'{{"tier": "A|B|C|D", "rationale": "...", "signals_found": {{"RBCZ": "yes|no", ...}}}}'
    )

    try:
        response = await anthropic_client.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=500,
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        logger.warning("sector_checker: Claude call failed for %s: %s", domain, e)
        return result

    text_out = response.content[0].text.strip() if response.content else ""
    if text_out.startswith("```"):
        text_out = re.sub(r"^```[a-z]*\n?", "", text_out)
        text_out = re.sub(r"\n?```$", "", text_out)

    try:
        parsed = json.loads(text_out)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("sector_checker: JSON parse failed for %s: %s", domain, e)
        return result

    tier = (parsed.get("tier") or "D").strip().upper()
    if tier not in _TIER_TO_POINTS:
        tier = "D"
    rationale = (parsed.get("rationale") or "").strip()
    signals_found = parsed.get("signals_found") or {}

    # Build checks list for UI transparency
    checks: list[dict] = []
    for sig in positive_signals:
        raw = signals_found.get(sig)
        passed = isinstance(raw, str) and raw.strip().lower() == "yes"
        checks.append({
            "key": _slugify(sig),
            "label": sig,
            "passed": passed,
            "points": 0,  # individual points have no meaning under the tier system
        })

    # Cost calculation
    input_tokens = getattr(response.usage, "input_tokens", 0) or 0
    output_tokens = getattr(response.usage, "output_tokens", 0) or 0
    tokens_used = input_tokens + output_tokens
    cost_eur = round(
        (input_tokens * _COST_PER_M_INPUT + output_tokens * _COST_PER_M_OUTPUT) / 1_000_000,
        6,
    )

    if supabase_client is not None:
        try:
            supabase_client.table("api_cost_log").insert({
                "model": _HAIKU_MODEL,
                "prompt_tokens": input_tokens,
                "response_tokens": output_tokens,
                "cost_eur": cost_eur,
                "context": "sector_checker",
            }).execute()
        except Exception as e:
            logger.debug("sector_checker: cost log failed: %s", e)

    result.update({
        "sector_score": _TIER_TO_POINTS[tier],
        "checks": checks,
        "tier": tier,
        "rationale": rationale,
        "tokens_used": tokens_used,
        "cost_eur": cost_eur,
    })
    logger.info(
        "sector_checker: %s → tier=%s score=%d cost=EUR %.6f",
        domain, tier, result["sector_score"], cost_eur,
    )
    return result


def _html_to_text(html: str) -> str:
    """Strip HTML tags and collapse whitespace for Claude input."""
    if not html:
        return ""
    # Remove script/style contents
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _slugify(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    return s or "signal"
