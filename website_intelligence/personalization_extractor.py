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

logger = logging.getLogger(__name__)


async def extract_personalization(
    domain: str,
    page_html: str,
    sector: str,
    anthropic_client: Any,
    supabase_client: Any = None,
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
        )

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
