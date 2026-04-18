"""
website_intelligence/visual_analyzer.py — Layer 2: Visual website analysis via Claude Sonnet Vision.

Takes a Playwright screenshot, uploads to Supabase Storage, sends to Claude Sonnet
for visual quality assessment. Max 25 points.

This is the most expensive layer (~$0.01-0.02 per analysis) and should only run
for leads that pass earlier quality gates (valid email, verified website).
"""
from __future__ import annotations

import base64
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

SCREENSHOT_ENABLED = os.getenv("SCREENSHOT_ENABLED", "true").lower() == "true"
STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "screenshots")


async def analyze_visual(
    domain: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
    sector: str = "",
) -> dict[str, Any]:
    """
    Take screenshot + run Claude Sonnet Vision analysis.

    Returns dict with 8 dimension scores (1-10), overall_score,
    top_strengths, top_improvements, visual_score (0-25).
    """
    result: dict[str, Any] = {
        "overall_score": None,
        "visual_score": 0,
        "dimensions": {},
        "top_strengths": [],
        "top_improvements": [],
    }

    if not SCREENSHOT_ENABLED:
        logger.info("Visual analysis skipped — SCREENSHOT_ENABLED=false")
        return result

    # Take screenshot
    screenshot_b64 = await _take_screenshot(domain)
    if not screenshot_b64:
        return result

    # Upload to Supabase Storage
    try:
        path = f"{domain}.png"
        file_bytes = base64.b64decode(screenshot_b64)
        supabase_client.storage.from_(STORAGE_BUCKET).upload(
            path, file_bytes,
            file_options={"content-type": "image/png", "upsert": "true"},
        )
    except Exception as e:
        logger.debug("Screenshot upload failed for %s (may already exist): %s", domain, e)

    # Claude Sonnet Vision analysis
    sector_context = {
        "makelaars": "makelaardij / vastgoed in Nederland",
        "behandelaren": "coaching, therapie of behandelpraktijk in Nederland",
        "bouwbedrijven": "bouw, renovatie of aannemerij in Nederland",
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
        message = anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": screenshot_b64}},
                    {"type": "text", "text": prompt},
                ],
            }],
        )

        response_text = message.content[0].text
        result = _parse_vision_response(response_text)
        result["visual_score"] = _calculate_visual_score(result.get("overall_score") or 5)

    except Exception as e:
        logger.error("Claude Vision analysis failed for %s: %s", domain, e)

    return result


async def _take_screenshot(domain: str) -> str | None:
    """Take a full-page screenshot via Playwright. Returns base64 string or None."""
    try:
        from utils.playwright_helpers import new_browser_context

        async with new_browser_context() as (browser, context):
            page = await context.new_page()
            await page.set_viewport_size({"width": 1280, "height": 720})
            await page.goto(f"https://{domain}", wait_until="networkidle", timeout=20_000)

            import asyncio
            await asyncio.sleep(2)

            screenshot = await page.screenshot(full_page=True, type="png")
            return base64.b64encode(screenshot).decode("utf-8")
    except Exception as e:
        logger.warning("Screenshot failed for %s: %s", domain, e)
        return None


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
