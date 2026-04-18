"""
enrichment/lead_qualifier.py — Pre-enrichment qualification gate.

Decides whether a companies_raw record should become a lead at all.
Runs BEFORE the enrichment pipeline to prevent wasting Claude credits
on companies that will never be usable leads.

A company must pass ALL hard gates to become a lead.
Soft gates reduce priority but don't block.
"""
from __future__ import annotations

import logging
import re
from typing import Any

from config.sectors import get_sector

logger = logging.getLogger(__name__)

# Domains that indicate parked/placeholder/aggregator sites
_JUNK_DOMAINS = {
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "youtube.com", "google.com", "yelp.com", "tripadvisor.com",
    "trustpilot.com", "klantenvertellen.nl", "thuispagina.nl",
    "wix.com", "wordpress.com", "blogspot.com", "sites.google.com",
    "squarespace.com", "weebly.com",
}

# Patterns that indicate a business is closed or inactive
_CLOSED_PATTERNS = [
    r"permanent\s*gesloten",
    r"permanently\s*closed",
    r"tijdelijk\s*gesloten",
    r"niet\s*meer\s*actief",
    r"gestopt",
    r"opgeheven",
]


async def qualify_raw_company(
    raw: dict,
    sector_key: str,
    workspace_id: str,
    supabase_client: Any,
) -> tuple[bool, str, int]:
    """
    Decide if a companies_raw record should become a lead.

    Hard gates (instant reject):
      - No company name
      - No website/domain (enrichment requires a website)
      - Domain is a social platform / aggregator
      - Company name matches exclude_keywords for this sector
      - Google Maps shows "permanently closed"

    Soft signals (lower priority, not blocking):
      - No phone number → priority -1
      - Very low rating (<3.0) → priority -1
      - 0 reviews → priority -1

    Args:
        raw: companies_raw dict.
        sector_key: Sector for exclude_keyword checking.
        workspace_id: Workspace slug.
        supabase_client: Supabase client (for dedup check).

    Returns:
        (qualified: bool, reason: str, priority: int)
        priority: 1 (highest) to 10 (lowest). Default 5.
    """
    name = (raw.get("company_name") or "").strip()
    domain = (raw.get("domain") or "").strip().lower()
    phone = raw.get("phone") or ""
    rating = raw.get("google_rating") or raw.get("rating") or 0
    review_count = raw.get("google_review_count") or raw.get("review_count") or 0
    category = (raw.get("google_category") or raw.get("category") or "").lower()
    maps_status = (raw.get("business_status") or "").lower()

    priority = 5

    # ── Hard gates ────────────────────────────────────────────────────────

    if not name:
        return False, "no_company_name", 10

    if not domain:
        return False, "no_website", 10

    # Junk domain check
    domain_clean = domain.replace("www.", "")
    if domain_clean in _JUNK_DOMAINS or any(domain_clean.endswith(f".{junk}") for junk in _JUNK_DOMAINS):
        return False, f"junk_domain:{domain_clean}", 10

    # Closed business check
    if maps_status in ("closed_permanently", "permanently_closed"):
        return False, "permanently_closed", 10

    for pattern in _CLOSED_PATTERNS:
        if re.search(pattern, name, re.IGNORECASE):
            return False, "closed_pattern_in_name", 10

    # Exclude keywords from sector config
    try:
        sector_config = get_sector(sector_key)
        exclude_keywords = sector_config.get("exclude_keywords", [])
        name_lower = name.lower()
        category_lower = category.lower()
        for kw in exclude_keywords:
            if kw.lower() in name_lower or kw.lower() in category_lower:
                return False, f"exclude_keyword:{kw}", 10
    except ValueError:
        pass

    # Dedup: already exists as a lead? (check leads table, NOT companies_raw)
    try:
        existing = supabase_client.table("leads").select("id").eq(
            "workspace_id", workspace_id,
        ).eq("domain", domain_clean).limit(1).execute()
        if existing.data:
            return False, "duplicate_domain", 10
    except Exception:
        pass

    # ── Soft signals ──────────────────────────────────────────────────────

    if not phone:
        priority += 1  # No phone = slightly less reachable

    if isinstance(rating, (int, float)) and rating > 0 and rating < 3.0:
        priority += 1  # Low rating = potential problem business

    if isinstance(review_count, (int, float)) and review_count == 0:
        priority += 1  # No reviews = possibly new or inactive

    # Positive signals → boost priority
    if isinstance(rating, (int, float)) and rating >= 4.5:
        priority -= 1

    if isinstance(review_count, (int, float)) and review_count >= 10:
        priority -= 1

    # ICP keyword match in name or category → boost priority
    try:
        icp_keywords = sector_config.get("icp_keywords", [])
        combined = f"{name_lower} {category_lower}"
        matches = sum(1 for kw in icp_keywords if kw.lower() in combined)
        if matches >= 2:
            priority -= 1
    except (NameError, UnboundLocalError):
        pass

    priority = max(1, min(priority, 10))

    return True, "qualified", priority


async def qualify_and_create_lead(
    raw: dict,
    sector_key: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict | None:
    """
    Qualify a raw company and create a lead record if it passes.

    Returns the created lead dict, or None if disqualified.
    """
    qualified, reason, priority = await qualify_raw_company(
        raw, sector_key, workspace_id, supabase_client,
    )

    if not qualified:
        logger.info(
            "Disqualified: %s — reason=%s",
            raw.get("company_name", "?"), reason,
        )
        # Log disqualification
        try:
            supabase_client.table("companies_raw").update({
                "qualification_status": "disqualified",
                "disqualification_reason": reason,
            }).eq("id", raw.get("id")).execute()
        except Exception:
            pass
        return None

    # Create lead from raw company
    domain = (raw.get("domain") or "").strip().lower().replace("www.", "")
    lead = {
        "workspace_id": workspace_id,
        "company_name": raw.get("company_name"),
        "domain": domain,
        "city": raw.get("city") or raw.get("address_city") or "",
        "sector": sector_key,
        "phone": raw.get("phone") or "",
        "google_rating": raw.get("google_rating") or raw.get("rating"),
        "google_review_count": raw.get("google_review_count") or raw.get("review_count"),
        "google_category": raw.get("google_category") or raw.get("category") or "",
        "google_maps_url": raw.get("google_maps_url") or raw.get("maps_url") or "",
        "source": raw.get("source") or "google_maps",
        "status": "discovered",
        "enrichment_version": 0,
        "gdpr_safe": True,  # Starts as safe — may be revoked during enrichment
    }

    try:
        res = supabase_client.table("leads").insert(lead).execute()
        if res.data:
            lead_id = res.data[0]["id"]
            logger.info("Created lead: %s → %s (priority=%d)", raw.get("company_name"), lead_id, priority)

            # Queue for enrichment
            from job_queue.enrichment_queue import queue_lead_for_enrichment
            await queue_lead_for_enrichment(
                lead_id=lead_id,
                workspace_id=workspace_id,
                supabase_client=supabase_client,
                priority=priority,
            )

            return res.data[0]
    except Exception as e:
        logger.error("Failed to create lead from raw %s: %s", raw.get("company_name"), e)

    return None
