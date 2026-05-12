"""
scoring/icp_matcher.py — ICP (Ideal Customer Profile) matching.

Checks a lead against the sector's ICP definition: keywords, disqualifiers,
Google rating, KvK SBI match.

Returns a 0.0-1.0 ICP match score stored in leads.icp_match.
"""
from __future__ import annotations

import logging
from typing import Any

from config.sectors import get_sector

logger = logging.getLogger(__name__)

# Per sectors.py (2026-04-21 herziening): typical_company_size is niet meer
# gedefinieerd per sector. We werken met één conservatieve range die alle
# behandelaars-sectoren dekt: 1-persoons solo praktijken tot 15-person klinieken.
_FALLBACK_COMPANY_SIZE_RANGE = (1, 15)


def _collect_icp_signals(sector_config: dict) -> list[str]:
    """Flatten alle subcategory icp_signals + global lead_keywords tot één lijst."""
    signals: list[str] = list(sector_config.get("lead_keywords") or [])
    for sub in (sector_config.get("subcategories") or {}).values():
        signals.extend(sub.get("icp_signals") or [])
    return signals


async def match_icp(
    lead_id: str,
    sector: str,
    workspace_id: str,
    supabase_client: Any,
) -> float:
    """
    Compute ICP match score for a lead.

    Checks:
      - icp_keywords found on website / company description
      - exclude_keywords (disqualify if found)
      - Company size in target range
      - Google rating threshold
      - KvK SBI code match

    Returns:
        ICP match score 0.0-1.0. Also writes to leads.icp_match.
    """
    try:
        sector_config = get_sector(sector)
    except ValueError:
        logger.warning("icp_matcher: unknown sector %s", sector)
        return 0.0

    # Load lead
    lead_res = supabase_client.table("leads").select("*").eq(
        "id", lead_id,
    ).eq("workspace_id", workspace_id).maybe_single().execute()

    if not lead_res.data:
        return 0.0

    lead = lead_res.data
    signals: list[str] = []
    score = 0.0
    max_score = 0.0

    # --- Disqualifiers check (instant disqualify) ---
    # Combineer globale + subcategory disqualifiers
    disqualifiers: list[str] = list(sector_config.get("disqualifiers") or [])
    for sub in (sector_config.get("subcategories") or {}).values():
        disqualifiers.extend(sub.get("disqualifiers") or [])

    company_text = _build_company_text(lead).lower()

    for kw in disqualifiers:
        if kw.lower() in company_text:
            logger.info("icp_matcher: lead %s disqualified by keyword '%s'", lead_id, kw)
            _store_icp_match(supabase_client, lead_id, 0.0)
            return 0.0

    # --- ICP keywords (max 0.35) ---
    icp_keywords = _collect_icp_signals(sector_config)
    max_score += 0.35
    if icp_keywords:
        matches = sum(1 for kw in icp_keywords if kw.lower() in company_text)
        keyword_ratio = min(matches / max(len(icp_keywords) * 0.3, 1), 1.0)
        score += keyword_ratio * 0.35

    # --- KvK SBI match (0.20) ---
    # Nieuwe 5-cijferige SBI 2025 codes. Prefix-match blijft werken (e.g. lead sbi
    # "86919" matcht entry "86919"; oude 4-cijferige lead sbi "8691" matcht ook
    # via prefix omdat "86919" startswith("8691") — maar leads zouden inmiddels
    # naar 5-cijferig gemigreerd moeten zijn).
    max_score += 0.20
    sbi_codes = sector_config.get("sbi_codes") or []
    lead_sbi = (lead.get("sbi_code") or "").replace(".", "")
    if lead_sbi and sbi_codes:
        if any(code.startswith(lead_sbi) or lead_sbi.startswith(code) for code in sbi_codes):
            score += 0.20
            signals.append("sbi_match")

    # --- Company size fit (0.15) ---
    # Sectors.py v2 (2026-04-21) bevat geen typical_company_size meer per sector.
    # We gebruiken een vaste behandelaars-range (1-15) tot feedback data meer
    # nuance rechtvaardigt.
    max_score += 0.15
    lo, hi = _FALLBACK_COMPANY_SIZE_RANGE
    employee_count = lead.get("employee_count") or lead.get("estimated_size") or 0
    if employee_count:
        if lo <= employee_count <= hi:
            score += 0.15
            signals.append("size_fit")
    else:
        score += 0.08  # Unknown size — give partial credit

    # --- Google rating (0.15) ---
    max_score += 0.15
    rating = lead.get("google_rating") or 0
    if rating >= 4.5:
        score += 0.15
        signals.append("high_rating")
    elif rating >= 4.0:
        score += 0.10
    elif rating >= 3.5:
        score += 0.05

    # --- Has website (0.10) ---
    max_score += 0.10
    if lead.get("domain"):
        score += 0.10
        signals.append("has_website")

    # --- Has valid email (0.05) ---
    max_score += 0.05
    if lead.get("email_status") in ("valid", "risky"):
        score += 0.05
        signals.append("has_email")

    icp_match = round(min(score, 1.0), 3)
    _store_icp_match(supabase_client, lead_id, icp_match)

    logger.info("icp_matcher: lead=%s sector=%s match=%.2f signals=%s", lead_id, sector, icp_match, signals)
    return icp_match


def _build_company_text(lead: dict) -> str:
    """Build searchable text from all lead fields for keyword matching."""
    parts = [
        lead.get("company_name") or "",
        lead.get("company_summary") or "",
        lead.get("industry") or "",
        lead.get("google_category") or "",
        lead.get("company_positioning") or "",
    ]
    return " ".join(p for p in parts if p)


def _store_icp_match(supabase_client: Any, lead_id: str, icp_match: float) -> None:
    """Write ICP match score back to leads table."""
    try:
        supabase_client.table("leads").update({
            "icp_match": icp_match,
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.debug("Failed to store icp_match: %s", e)
