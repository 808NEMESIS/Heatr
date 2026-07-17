"""
website_intelligence/visual_analyzer.py — Layer 2: Visual website analysis via Claude Sonnet Vision.

Takes a Playwright screenshot, uploads to Supabase Storage, sends to Claude Sonnet
for visual quality assessment. Max 25 points.

Cost-control layers (PR2.5):
  1. SCREENSHOT_ENABLED env kill-switch (bestaand)
  2. Conditional skip: technical_score >= VISION_SKIP_TECHNICAL_THRESHOLD (default 20)
     → site is technisch al te goed voor Aerys-websitebouw-verkoop, Vision voegt
     weinig toe aan de opener/review-email. Skip + visual_score=None.
  3. Screenshot-hash cache via utils/vision_cache — zelfde PNG-bytes →
     dezelfde response, hergebruikt over leads en re-enrichments.

Deze laag is de duurste (~€0.014/lead, 87% van totaalkost). Conditional + cache
brengt dat structureel terug naar ~€0.005 gemiddeld bij normale flow en naar
~€0 bij re-enrichment.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from utils.vision_cache import get_cached_vision, store_vision_result

logger = logging.getLogger(__name__)

SCREENSHOT_ENABLED = os.getenv("SCREENSHOT_ENABLED", "true").lower() == "true"
STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "screenshots")

# Boven deze technical_score gaan we Vision overslaan — site is al te gezond
# voor de Aerys-websitebouw-pitch. Override via env.
_VISION_SKIP_TECH_THRESHOLD = int(os.getenv("VISION_SKIP_TECHNICAL_THRESHOLD", "20"))


async def analyze_visual(
    domain: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
    sector: str = "",
    technical_score: int = 0,
    screenshot_b64: str | None = None,
    screenshot_media_type: str = "image/png",
) -> dict[str, Any]:
    """
    Take screenshot + run Claude Sonnet Vision analysis.

    Returns dict with 8 dimension scores (1-10), overall_score,
    top_strengths, top_improvements, visual_score (0-25).

    Skips Vision when:
      - SCREENSHOT_ENABLED=false
      - technical_score >= _VISION_SKIP_TECH_THRESHOLD (site te goed)
      - Screenshot grab faalt
    On cache hit (same PNG bytes as before): return cached result, no API call.
    """
    result: dict[str, Any] = {
        "overall_score": None,
        "visual_score": 0,
        "dimensions": {},
        "top_strengths": [],
        "top_improvements": [],
        "skipped_reason": None,
        "cache_hit": False,
    }

    if not SCREENSHOT_ENABLED:
        logger.info("Visual analysis skipped — SCREENSHOT_ENABLED=false")
        result["skipped_reason"] = "disabled"
        return result

    # Conditional gate: skip Vision if technical already strong
    if technical_score >= _VISION_SKIP_TECH_THRESHOLD:
        logger.info(
            "visual_analyzer: skipping Vision for %s — technical_score=%d >= %d (no Aerys-pitch opportunity)",
            domain, technical_score, _VISION_SKIP_TECH_THRESHOLD,
        )
        result["skipped_reason"] = f"technical_score_above_threshold:{technical_score}"
        return result

    # Screenshot komt uit capture_site (het enige capture-pad) — Vision captured
    # en uploadt niet meer zelf. Geen screenshot -> geen Vision.
    if not screenshot_b64:
        result["skipped_reason"] = "no_screenshot"
        return result

    # Cache check (SHA-256 van de screenshot-bytes)
    try:
        cached = await get_cached_vision(screenshot_b64, supabase_client)
    except Exception as e:
        logger.debug("visual_analyzer: cache lookup errored: %s", e)
        cached = None

    if cached:
        cached["cache_hit"] = True
        return cached

    # Claude Sonnet Vision analysis
    sector_context = {
        "alternatieve_geneeskunde": (
            "alternatieve / complementaire zorg praktijken in Nederland "
            "(acupuncturisten, osteopaten, natuurgeneeskundigen, haptotherapeuten)"
        ),
        "cosmetische_behandelaars": (
            "cosmetische klinieken en behandelaars in Nederland "
            "(botox, fillers, laser, huidtherapie, schoonheidssalons)"
        ),
    }.get(sector, "Nederlands MKB")

    prompt = (
        f"Je bent een senior webdesigner gespecialiseerd in {sector_context}.\n"
        "Analyseer deze website screenshot.\n\n"
        "Geef per onderdeel score 1-10 + één concrete zin:\n"
        "1. ALGEMENE INDRUK — modern en professioneel in 2024?\n"
        "2. TYPOGRAFIE — leesbaar, modern, consistente hiërarchie?\n"
        "3. KLEURGEBRUIK — past bij de sector? Coherent?\n"
        "4. WITRUIMTE — genoeg ademruimte? Gebalanceerd?\n"
        "5. AFBEELDINGEN — professioneel? Echte foto's of stock?\n"
        "6. VERTROUWENSSIGNALEN — reviews, certificaten, team zichtbaar?\n"
        "7. MOBIELE INDRUK — ziet het responsive-vriendelijk uit?\n"
        "8. SECTOR AUTHENTICITEIT — past dit bij de sector?\n\n"
        "Daarna:\n"
        "- TOP 3 STERKSTE PUNTEN (bullet list)\n"
        "- TOP 3 VERBETERPUNTEN (concreet en actionable)\n"
        "- OVERALL SCORE: gewogen gemiddelde 1-10\n\n"
        "Antwoord in het Nederlands. Wees direct en eerlijk."
    )

    try:
        message = await anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": screenshot_media_type, "data": screenshot_b64}},
                    {"type": "text", "text": prompt},
                ],
            }],
        )

        response_text = message.content[0].text
        parsed = _parse_vision_response(response_text)
        parsed["visual_score"] = _calculate_visual_score(parsed.get("overall_score") or 5)
        parsed["skipped_reason"] = None
        parsed["cache_hit"] = False
        result = parsed

        # Store in cache for future re-enrichments / sibling leads on same site
        try:
            usage = getattr(message, "usage", None)
            in_tok = getattr(usage, "input_tokens", 0) or 0
            out_tok = getattr(usage, "output_tokens", 0) or 0
            await store_vision_result(
                screenshot_b64, domain, result, in_tok, out_tok, supabase_client,
            )
        except Exception as e:
            logger.debug("visual_analyzer: cache store failed for %s: %s", domain, e)

    except Exception as e:
        logger.error("Claude Vision analysis failed for %s: %s", domain, e)

    return result


def _calculate_visual_score(overall_1_to_10: int) -> int:
    """Convert 1-10 overall score to 0-25 point scale."""
    # Linear mapping: 1→0, 5→12, 10→25
    return min(25, max(0, int((overall_1_to_10 / 10) * 25)))


def _parse_vision_response(text: str) -> dict[str, Any]:
    """Parse Claude's vision response into structured data."""
    import re

    result: dict[str, Any] = {
        "dimensions": {},
        "top_strengths": [],
        "top_improvements": [],
        "overall_score": 5,
        "raw_analysis": text,
    }

    # Extract scores (pattern: "N. LABEL — ... score: X/10" or just "X/10")
    dimension_names = [
        "algemene indruk", "typografie", "kleurgebruik", "witruimte",
        "afbeeldingen", "vertrouwenssignalen", "mobiele indruk", "sector authenticiteit",
    ]

    for dim in dimension_names:
        pattern = rf"{dim}.*?(\d+)\s*/\s*10"
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            result["dimensions"][dim] = int(match.group(1))

    # Extract overall score
    overall_match = re.search(r"overall\s*(?:score)?[:\s]*(\d+(?:\.\d+)?)\s*/\s*10", text, re.IGNORECASE)
    if overall_match:
        result["overall_score"] = int(float(overall_match.group(1)))

    # Extract strengths and improvements (bullet points after headers)
    strengths_match = re.search(r"(?:sterkste punten|strengths)[:\s]*\n((?:[-•*]\s*.+\n?){1,5})", text, re.IGNORECASE)
    if strengths_match:
        result["top_strengths"] = [
            line.lstrip("-•* ").strip()
            for line in strengths_match.group(1).strip().split("\n")
            if line.strip()
        ][:3]

    improvements_match = re.search(r"(?:verbeterpunten|improvements)[:\s]*\n((?:[-•*]\s*.+\n?){1,5})", text, re.IGNORECASE)
    if improvements_match:
        result["top_improvements"] = [
            line.lstrip("-•* ").strip()
            for line in improvements_match.group(1).strip().split("\n")
            if line.strip()
        ][:3]

    return result
