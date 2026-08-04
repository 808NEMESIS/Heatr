"""
website_intelligence/analyzer.py — Website intelligence orchestrator.

Coordinates all analysis layers and stores results in the website_intelligence table.
Called as step 5 in the enrichment pipeline.

Layers:
  1. Technical (25 pts) — SSL, PageSpeed, CMS, schema, sitemap
  2. Visual (25 pts) — Claude Sonnet Vision screenshot analysis (optional)
  3. Conversion (30 pts) — CTA, booking, chat, WhatsApp, forms
  4. Sector-specific (15 pts + bonus) — sector expectations from config
  5. Personalization — hooks, observations, gaps for outreach context
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from utils.cost_guard import LeadCostAccumulator
from website_intelligence.technical_checker import check_technical
from website_intelligence.conversion_checker import check_conversion
from website_intelligence.sector_checker import check_sector_specific
from website_intelligence.contact_extractor import extract_contacts_from_website
from website_intelligence.personalization_extractor import extract_personalization
from website_intelligence.opportunity_classifier import classify_opportunities

logger = logging.getLogger(__name__)


async def analyze_website(
    lead_id: str,
    domain: str,
    sector: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
    *,
    accumulator: LeadCostAccumulator | None = None,
) -> dict[str, Any]:
    """
    Run full website intelligence analysis for a lead.

    Executes all layers, computes total score, classifies opportunities,
    and stores everything in the website_intelligence table.

    Args:
        lead_id: Lead UUID.
        domain: Website domain (without https://).
        sector: Sector key from config/sectors.py.
        workspace_id: Workspace slug.
        supabase_client: Supabase client.
        anthropic_client: Anthropic client for Claude calls.

    Returns:
        Complete website intelligence dict.
    """
    logger.info("analyze_website: starting for %s (sector=%s)", domain, sector)

    result: dict[str, Any] = {
        "lead_id": lead_id,
        "workspace_id": workspace_id,
        "domain": domain,
        "sector": sector,
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }

    # Fetch homepage HTML once — reused by multiple layers
    page_html = ""
    try:
        async with httpx.AsyncClient(
            timeout=15.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"},
        ) as client:
            r = await client.get(f"https://{domain}")
            if r.status_code == 200:
                page_html = r.text
    except Exception as e:
        logger.warning("analyze_website: failed to fetch %s: %s", domain, e)

    if not page_html:
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                r = await client.get(f"http://{domain}")
                if r.status_code == 200:
                    page_html = r.text
        except Exception:
            pass

    # --- Crawl-instrumentatie (ONGATED, los van enable_vision) ---
    # Het ENIGE capture-pad: screenshots (desktop+mobiel), pre/post-consent
    # netwerkgedrag en content-hashes. Draait altijd, ook met enable_vision=False.
    # Fail-soft en score-neutraal (aparte kolommen + aparte writes verderop). Staat
    # vóór Laag 2 zodat Vision (indien ooit aan) de screenshot hergebruikt i.p.v.
    # zelf te capturen.
    capture: dict[str, Any] = {}
    try:
        from website_intelligence.site_capture import capture_site
        capture = await capture_site(domain, lead_id, workspace_id, supabase_client)
        if capture.get("error"):
            logger.warning("analyze_website: capture voor %s met fout: %s", domain, capture["error"])
    except Exception as e:
        logger.warning("analyze_website: capture_site faalde voor %s: %s", domain, e)
    result["capture"] = capture

    # --- Layer 1: Technical (max 25 pts) ---
    technical = await check_technical(domain, supabase_client)
    result["technical"] = technical
    result["technical_score"] = technical["technical_score"]

    # --- Layer 2: Visual (max 25 pts) via Claude Sonnet Vision ---
    # Draait op de desktop+mobiel screenshots uit capture_site (één capture-pad).
    # visual_analyzer skipt zelf op SCREENSHOT_ENABLED=false of een technisch al
    # sterke site (kostencontrole) -> dan blijft visual_score None en sluit de
    # normalisatie de laag uit (geen stille deflatie van de totaalscore).
    visual_overall = None
    try:
        from website_intelligence.visual_analyzer import analyze_visual
        visual = await analyze_visual(
            domain, workspace_id, supabase_client, anthropic_client,
            sector=sector,
            technical_score=result["technical_score"],
            screenshot_desktop_b64=capture.get("screenshot_desktop_b64"),
            screenshot_mobile_b64=capture.get("screenshot_mobile_b64"),
        )
        result["visual"] = visual
        result["visual_score"] = visual.get("visual_score")   # 0-25 punten (of None)
        visual_overall = visual.get("overall_score")           # 0-10, voor opportunity
    except Exception as e:
        logger.warning("Visual analysis failed for %s: %s", domain, e)
        result["visual_score"] = None

    # --- Layer 3: Conversion (max 30 pts) ---
    conversion = await check_conversion(domain, page_html, sector, supabase_client)
    result["conversion"] = conversion
    result["conversion_score"] = conversion["conversion_score"]

    # --- Layer 4: Sector-specific (max 15, Claude Haiku tier-classifier) ---
    sector_result = await check_sector_specific(
        domain, page_html, sector,
        anthropic_client=anthropic_client,
        supabase_client=supabase_client,
        lead_id=lead_id,
        accumulator=accumulator,
    )
    result["sector_specific"] = sector_result
    result["sector_score"] = sector_result["sector_score"]

    # --- Total score — genormaliseerd over de BEHAALDE noemer ---
    # Optellen alsof alle lagen meedoen deflateert de score zodra er één ontbreekt
    # (Vision skipt/faalt). Daarom: normaliseer over de max-punten van de lagen die
    # daadwerkelijk bijdroegen. Zonder visual -> denom 70; mét visual -> 95.
    _layers = [
        ("technical", result["technical_score"], 25),
        ("conversion", result["conversion_score"], 30),
        ("sector", result["sector_score"], 15),
    ]
    _visual_included = result.get("visual_score") is not None
    if _visual_included:
        _layers.append(("visual", result["visual_score"], 25))
    _achieved = sum(s for _, s, _ in _layers)
    _denom = sum(m for _, _, m in _layers)
    result["total_score"] = round(_achieved / _denom * 100) if _denom else 0
    result["score_denominator"] = _denom
    result["visual_included"] = _visual_included

    # --- Layer 5: Personalization extraction ---
    personalization = await extract_personalization(
        domain, page_html, sector, anthropic_client, supabase_client,
        lead_id=lead_id,
        accumulator=accumulator,
    )
    result["personalization"] = personalization

    # --- Contact extraction from team pages ---
    contacts = await extract_contacts_from_website(
        domain, supabase_client, anthropic_client,
        lead_id=lead_id, accumulator=accumulator,
    )
    result["team_contacts"] = contacts

    # --- Opportunity classification ---
    opportunities = classify_opportunities(
        total_score=result["total_score"],
        technical_result=technical,
        conversion_result=conversion,
        sector_result=sector_result,
        visual_score=visual_overall,   # 0-10 overall — de <4-check verwacht die schaal
        sector=sector,                 # sector-poort: nooit een offer buiten allowed_offers
    )
    result["opportunities"] = opportunities

    # --- Store in website_intelligence table ---
    try:
        supabase_client.table("website_intelligence").upsert({
            "lead_id": lead_id,
            "workspace_id": workspace_id,
            "domain": domain,
            "total_score": result["total_score"],
            "technical_score": result["technical_score"],
            "visual_score": result.get("visual_score"),
            "conversion_score": result["conversion_score"],
            "sector_score": result["sector_score"],
            "opportunity_types": opportunities["opportunity_types"],
            "priority": opportunities["priority"],
            "technical_details": technical,
            "conversion_details": conversion,
            "sector_details": sector_result,
            "personalization": personalization,
            "team_contacts": contacts,
            "opportunity_reasons": opportunities["reasons"],
            "analyzed_at": result["analyzed_at"],
        }, on_conflict="lead_id").execute()
    except Exception as e:
        logger.error("Failed to store website_intelligence for %s: %s", lead_id, e)

    # --- Persisteer de normalisatie-noemer APART (migratie-veilig, 034) ---
    # De genormaliseerde total_score staat al in de upsert (bestaande kolom).
    # score_denominator + visual_included zijn nieuw: aparte update zodat een
    # ontbrekende migratie 034 alleen dit blok raakt, niet de score-write.
    try:
        supabase_client.table("website_intelligence").update({
            "score_denominator": result.get("score_denominator"),
            "visual_included": result.get("visual_included"),
        }).eq("lead_id", lead_id).eq("workspace_id", workspace_id).execute()
    except Exception as e:
        logger.warning("analyze_website: score-noemer opslaan faalde voor %s: %s", lead_id, e)

    # --- Persisteer visual_observations APART (migratie-veilig, 046) ---
    # De concrete Vision-observaties (verbeter-/sterke punten) voeden de
    # benchmark-pitchzin; alleen visual_score staat al in de upsert hierboven.
    # Zonder deze write moest run_vision_refresh dezelfde lead nóg eens door
    # Vision halen (dubbele kosten). Zelfde vorm als run_vision_refresh; aparte
    # update zodat een nog-niet-gedraaide migratie 046 alleen dit blok raakt.
    _visual = result.get("visual") or {}
    if result.get("visual_score") is not None:
        try:
            supabase_client.table("website_intelligence").update({
                "visual_observations": {
                    "improvements": _visual.get("top_improvements") or [],
                    "strengths": _visual.get("top_strengths") or [],
                    "overall": _visual.get("overall_score"),
                    "at": result["analyzed_at"],
                },
            }).eq("lead_id", lead_id).eq("workspace_id", workspace_id).execute()
        except Exception as e:
            logger.info("analyze_website: visual_observations niet opgeslagen "
                        "(migratie 046 nog niet gedraaid?) voor %s: %s", lead_id, str(e)[:80])

    # --- Persisteer capture-velden APART (migratie-veilig) ---
    # Aparte update i.p.v. in de score-upsert hierboven: als migratie 033 nog niet
    # gedraaid is, faalt alleen dit blok (fail-soft) en blijft de score-write heel.
    if capture and not capture.get("error"):
        try:
            supabase_client.table("website_intelligence").update({
                "screenshot_desktop_url": capture.get("screenshot_desktop_url"),
                "screenshot_mobile_url": capture.get("screenshot_mobile_url"),
                "screenshot_desktop_hash": capture.get("screenshot_desktop_hash"),
                "screenshot_mobile_hash": capture.get("screenshot_mobile_hash"),
                "dom_text_hash": capture.get("dom_text_hash"),
                "capture_timings": capture.get("timings"),
            }).eq("lead_id", lead_id).eq("workspace_id", workspace_id).execute()
        except Exception as e:
            logger.warning("analyze_website: capture-velden opslaan faalde voor %s: %s", lead_id, e)

        # Netwerk-log is APPEND-ONLY (before/after op de tracking-check).
        if capture.get("network"):
            try:
                supabase_client.table("website_network_log").insert({
                    "workspace_id": workspace_id, "lead_id": lead_id, "domain": domain,
                    "dom_text_hash": capture.get("dom_text_hash"),
                    "requests": capture["network"], "summary": capture.get("network_summary"),
                }).execute()
            except Exception as e:
                logger.warning("analyze_website: network-log opslaan faalde voor %s: %s", lead_id, e)

    # --- Update lead with website score + personalization data ---
    try:
        lead_update: dict[str, Any] = {
            "website_score": result["total_score"],
        }
        if personalization.get("positioning"):
            lead_update["company_positioning"] = personalization["positioning"]
        if personalization.get("hooks"):
            lead_update["personalization_hooks"] = personalization["hooks"]
        if personalization.get("observations"):
            lead_update["personalization_observations"] = personalization["observations"]

        # Warmr Sequence v1.0 datapoint — booking_system enum
        if conversion.get("booking_system"):
            lead_update["booking_system"] = conversion["booking_system"]

        supabase_client.table("leads").update(lead_update).eq("id", lead_id).execute()
    except Exception as e:
        logger.error("Failed to update lead with website score: %s", e)

    logger.info(
        "analyze_website: %s done — total=%d tech=%d conv=%d sector=%d opp=%s",
        domain, result["total_score"], result["technical_score"],
        result["conversion_score"], result["sector_score"],
        opportunities["opportunity_types"],
    )

    return result
