"""
scripts/backfill_vision_coverage.py — koop Vision-dekking op de skippers (herijking-
keuze 3, 2026-07-21).

67% van de leads skipte Vision (technical_score >= 20). Deze run draait Vision
alsnog op de reeds-opgeslagen screenshots (geen re-capture) en her-normaliseert de
total_score met de EXACT dezelfde formule als analyzer.py:
    achieved = technical + conversion + sector [+ visual]
    denom    = 25 + 30 + 15 [+ 25]        (70 zonder visual, 95 met)
    total    = round(achieved / denom * 100)

Resume-veilig (visual_included=True → skip). Alleen leads mét screenshot (593);
de 43 dode sites zonder screenshot blijven onvermijdelijk zonder visual.
"""
from __future__ import annotations

import asyncio
import base64
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import httpx
import anthropic
from config.database import get_heatr_supabase
from website_intelligence.visual_analyzer import analyze_visual

WORKSPACE = "aerys"


async def _fetch_b64(client: httpx.AsyncClient, url: str | None) -> str | None:
    if not url:
        return None
    try:
        r = await client.get(url, timeout=30)
        r.raise_for_status()
        return base64.b64encode(r.content).decode()
    except Exception:
        return None


def _renorm(tech: int, conv: int, sec: int, visual: int) -> tuple[int, int]:
    """EXACT de analyzer-formule met visual erbij (denom 95)."""
    achieved = tech + conv + sec + visual
    denom = 25 + 30 + 15 + 25
    return round(achieved / denom * 100), denom


async def main() -> int:
    dry = "--dry-run" in sys.argv
    sb = get_heatr_supabase()
    ac = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    rows = (sb.table("website_intelligence")
            .select("lead_id, domain, technical_score, conversion_score, sector_score, "
                    "total_score, screenshot_desktop_url, screenshot_mobile_url")
            .eq("workspace_id", WORKSPACE).eq("visual_included", False)
            .not_.is_("screenshot_desktop_url", "null").execute()).data or []
    print(f"visual=False mét screenshot: {len(rows)} te dekken{' (DRY-RUN)' if dry else ''}.")

    ok = fail = 0
    shifts = []
    async with httpx.AsyncClient() as http:
        for i, w in enumerate(rows, 1):
            dom = w.get("domain") or "?"
            d_b64 = await _fetch_b64(http, w.get("screenshot_desktop_url"))
            m_b64 = await _fetch_b64(http, w.get("screenshot_mobile_url"))
            if not d_b64:
                fail += 1
                continue
            # technical_score=0 → override de skip-gate; Vision draait gegarandeerd.
            res = await analyze_visual(dom, WORKSPACE, sb, ac, technical_score=0,
                                       screenshot_desktop_b64=d_b64, screenshot_mobile_b64=m_b64)
            v = res.get("visual_score")
            if v is None:
                fail += 1
                print(f"  [{i}/{len(rows)}] {dom:32} Vision faalde/parse ({res.get('skipped_reason')})")
                continue
            new_total, denom = _renorm(w["technical_score"] or 0, w["conversion_score"] or 0,
                                       w["sector_score"] or 0, v)
            shifts.append(new_total - (w["total_score"] or 0))
            if not dry:
                sb.table("website_intelligence").update({
                    "visual_score": v, "visual_included": True,
                    "total_score": new_total, "score_denominator": denom,
                }).eq("lead_id", w["lead_id"]).eq("workspace_id", WORKSPACE).execute()
            ok += 1
            if i % 25 == 0:
                print(f"  [{i}/{len(rows)}] {dom:28} visual={v}/25  {w['total_score']}→{new_total}")

    avg_shift = sum(shifts) / len(shifts) if shifts else 0
    print(f"\nKlaar: {ok} gedekt, {fail} gefaald. Gem. total_score-verschuiving: {avg_shift:+.1f} punten.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.get_event_loop().run_until_complete(main()))
