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
    enable_vision: bool = False,
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
        enable_vision: Whether to run Sonnet Vision analysis (expensive).

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

    # --- Layer 1: Technical (max 25 pts) ---
    technical = await check_technical(domain, supabase_client)
    result["technical"] = technical
    result["technical_score"] = technical["technical_score"]

    # --- Layer 2: Visual (max 25 pts) — optional, expensive ---
    visual_score = None
    if enable_vision and page_html:
        try:
            from website_intelligence.visual_analyzer import analyze_visual
            visual = await analyze_visual(domain, workspace_id, supabase_client, anthropic_client)
            result["visual"] = visual
            visual_score = visual.get("overall_score")
            result["visual_score"] = visual_score
        except Exception as e:
            logger.warning("Visual analysis failed for %s: %s", domain, e)
            result["visual_score"] = None
    else:
        result["visual_score"] = None

    # --- Layer 3: Conversion (max 30 pts) ---
    conversion = await check_conversion(domain, page_html, sector, supabase_client)
    result["conversion"] = conversion
    result["conversion_score"] = conversion["conversion_score"]

    # --- Layer 4: Sector-specific (max 15 + bonus) ---
    sector_result = await check_sector_specific(
        domain, page_html, sector, conversion, technical,
    )
    result["sector_specific"] = sector_result
    result["sector_score"] = sector_result["sector_score"]

    # --- Total score ---
    total = (
        result["technical_score"]
        + (result["visual_score"] or 0)
        + result["conversion_score"]
        + result["sector_score"]
    )
    result["total_score"] = min(total, 100)

    # --- Layer 5: Personalization extraction ---
    personalization = await extract_personalization(
        domain, page_html, sector, anthropic_client, supabase_client,
    )
    result["personalization"] = personalization

    # --- Contact extraction from team pages ---
    contacts = await extract_contacts_from_website(domain, supabase_client, anthropic_client)
    result["team_contacts"] = contacts

    # --- Opportunity classification ---
    opportunities = classify_opportunities(
        total_score=result["total_score"],
        technical_result=technical,
        conversion_result=conversion,
        sector_result=sector_result,
        visual_score=visual_score,
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
