"""
scoring/lead_scoring.py — Multi-dimensional lead scoring for Heatr.

Scores leads across 4 dimensions:
  1. fit_score (0-40) — ICP match + sector alignment
  2. data_quality_score (0-20) — verification confidence
  3. reachability_score (0-25) — email + contact + phone
  4. personalization_potential (0-15) — hooks + context available

Total score: 0-100, stored in leads.score.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from scoring.icp_matcher import match_icp
from scoring.website_scorer import score_website

logger = logging.getLogger(__name__)

MIN_SCORE_FOR_WARMR = int(os.getenv("MIN_SCORE_FOR_WARMR", "65"))
MIN_ICP_MATCH_FOR_WARMR = float(os.getenv("MIN_ICP_MATCH_FOR_WARMR", "0.6"))


async def score_lead(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict[str, Any]:
    """
    Compute multi-dimensional lead score.

    Runs ICP matching, reads data quality from verification, checks
    reachability signals, and assesses personalization potential.

    Returns:
        {
            "score": int (0-100),
            "fit_score": int (0-40),
            "data_quality_score_num": float (0-20),
            "reachability_score": int (0-25),
            "personalization_potential": int (0-15),
            "push_eligible": bool,
            "push_block_reasons": list[str],
        }
    """
    result: dict[str, Any] = {
        "score": 0,
        "fit_score": 0,
        "data_quality_score_num": 0.0,
        "reachability_score": 0,
        "personalization_potential": 0,
        "push_eligible": False,
        "push_block_reasons": [],
    }

    # Load lead
    lead_res = supabase_client.table("leads").select("*").eq(
        "id", lead_id,
    ).eq("workspace_id", workspace_id).maybe_single().execute()

    if not lead_res.data:
        logger.warning("score_lead: lead %s not found", lead_id)
        return result

    lead = lead_res.data
    sector = lead.get("sector") or ""

    # -----------------------------------------------------------------------
    # Dimension 1: FIT SCORE (0-40)
    # -----------------------------------------------------------------------
    icp_match = await match_icp(lead_id, sector, workspace_id, supabase_client)
    fit_score = int(icp_match * 40)

    # Sector-specific boosts from config
    try:
        from config.sectors import get_sector
        sector_config = get_sector(sector)
        boosts = sector_config.get("scoring_boosts", {})
        boost_total = 0
        for boost_key, boost_pts in boosts.items():
            if _check_boost(boost_key, lead):
                boost_total += boost_pts
        fit_score = min(fit_score + boost_total, 40)
    except ValueError:
        pass

    result["fit_score"] = fit_score

    # -----------------------------------------------------------------------
    # Dimension 2: DATA QUALITY (0-20)
    # -----------------------------------------------------------------------
    confidence = lead.get("confidence_scores") or {}
    if confidence:
        dq = lead.get("data_quality_score") or 0
        result["data_quality_score_num"] = round(float(dq) * 20, 1)
    else:
        # No verification run yet — assign baseline from available data
        baseline = 0.0
        if lead.get("domain"):
            baseline += 0.3
        if lead.get("email"):
            baseline += 0.2
        if lead.get("company_name"):
            baseline += 0.2
        result["data_quality_score_num"] = round(baseline * 20, 1)

    # -----------------------------------------------------------------------
    # Dimension 3: REACHABILITY (0-25)
    # -----------------------------------------------------------------------
    reach = 0

    # Email quality (0-10)
    email_status = lead.get("email_status") or ""
    email_scores = {"valid": 10, "risky": 6, "catch_all": 3, "catchall_risky": 2}
    reach += email_scores.get(email_status, 0)

    # Contact person found (0-5)
    if lead.get("contact_first_name") and lead.get("contact_source"):
        contact_source_scores = {
            "website_team_page": 5,
            "kvk": 5,
            "linkedin_google_search": 4,
            "email_inference": 2,
        }
        reach += contact_source_scores.get(lead.get("contact_source", ""), 2)
    elif lead.get("contact_first_name"):
        reach += 2

    # Phone available (0-3)
    if lead.get("phone"):
        reach += 3

    # GDPR safe (0-3)
    if lead.get("gdpr_safe"):
        reach += 3

    # LinkedIn URL (0-2)
    if lead.get("contact_linkedin_url"):
        reach += 2

    # Domain email (0-2) — email on company domain stronger than external
    email = lead.get("email") or ""
    domain = lead.get("domain") or ""
    if email and domain and email.split("@")[-1].lower() == domain.lower():
        reach += 2

    result["reachability_score"] = min(reach, 25)

    # -----------------------------------------------------------------------
    # Dimension 4: PERSONALIZATION POTENTIAL (0-15)
    # -----------------------------------------------------------------------
    pers = 0

    hooks = lead.get("personalization_hooks") or []
    observations = lead.get("personalization_observations") or []
    positioning = lead.get("company_positioning") or ""

    if hooks:
        pers += min(len(hooks) * 2, 6)   # Up to 6 pts for hooks
    if observations:
        pers += min(len(observations), 4)  # Up to 4 pts for observations
    if positioning:
        pers += 3                          # Has clear positioning
    if lead.get("personalized_opener"):
        pers += 2                          # Claude opener generated

    result["personalization_potential"] = min(pers, 15)

    # -----------------------------------------------------------------------
    # Total score
    # -----------------------------------------------------------------------
    total = (
        result["fit_score"]
        + int(result["data_quality_score_num"])
        + result["reachability_score"]
        + result["personalization_potential"]
    )
    result["score"] = min(total, 100)

    # -----------------------------------------------------------------------
    # Push eligibility
    # -----------------------------------------------------------------------
    block_reasons: list[str] = []

    if result["score"] < MIN_SCORE_FOR_WARMR:
        block_reasons.append(f"score {result['score']} < {MIN_SCORE_FOR_WARMR}")
    if icp_match < MIN_ICP_MATCH_FOR_WARMR:
        block_reasons.append(f"icp_match {icp_match:.2f} < {MIN_ICP_MATCH_FOR_WARMR}")
    if not lead.get("email") or email_status in ("not_found", "invalid"):
        block_reasons.append("no valid email")
    if not lead.get("gdpr_safe"):
        block_reasons.append("not gdpr_safe")
    if lead.get("status") in ("forgotten", "unsubscribed", "disqualified"):
        block_reasons.append(f"status={lead.get('status')}")

    result["push_eligible"] = len(block_reasons) == 0
    result["push_block_reasons"] = block_reasons

    # -----------------------------------------------------------------------
    # Write to DB
    # -----------------------------------------------------------------------
    try:
        supabase_client.table("leads").update({
            "score": result["score"],
            "fit_score": result["fit_score"],
            "data_quality_score": result["data_quality_score_num"] / 20,  # Store as 0-1
            "reachability_score": result["reachability_score"],
            "personalization_potential": result["personalization_potential"],
            "scored_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.error("Failed to store lead score for %s: %s", lead_id, e)

    logger.info(
        "score_lead: lead=%s total=%d fit=%d dq=%.1f reach=%d pers=%d eligible=%s",
        lead_id, result["score"], result["fit_score"],
        result["data_quality_score_num"], result["reachability_score"],
        result["personalization_potential"], result["push_eligible"],
    )

    return result


def _check_boost(key: str, lead: dict) -> bool:
    """Check if a sector-specific scoring boost applies to this lead."""
    checks = {
        "has_funda_listing": lambda: "funda" in (lead.get("company_summary") or "").lower(),
        "has_nvm_certification": lambda: "nvm" in (lead.get("company_summary") or "").lower(),
        "has_virtual_tour": lambda: "360" in (lead.get("company_summary") or "").lower() or "rondleiding" in (lead.get("company_summary") or "").lower(),
        "has_online_booking": lambda: bool(lead.get("has_booking")),
        "has_instagram": lambda: bool(lead.get("instagram_url")),
        "has_gratis_kennismaking_cta": lambda: "kennismaking" in (lead.get("company_summary") or "").lower(),
        "has_personal_photo": lambda: bool(lead.get("contact_first_name")),
        "email_starts_with_name": lambda: _email_starts_with_name(lead),
        "kvk_sbi_match": lambda: bool(lead.get("kvk_number")),
        "google_rating_above_4_5": lambda: (lead.get("google_rating") or 0) >= 4.5,
        "has_project_portfolio": lambda: "project" in (lead.get("company_summary") or "").lower(),
        "has_werkspot_profile": lambda: "werkspot" in (lead.get("source") or "").lower(),
        "has_bouwend_nl_lid": lambda: "bouwend nederland" in (lead.get("company_summary") or "").lower(),
        "has_client_reviews": lambda: (lead.get("google_review_count") or 0) >= 5,
        "has_before_after_gallery": lambda: "voor en na" in (lead.get("company_summary") or "").lower(),
    }

    checker = checks.get(key)
    if checker:
        try:
            return checker()
        except Exception:
            return False
    return False


def _email_starts_with_name(lead: dict) -> bool:
    """Check if email local part matches contact first name."""
    email = (lead.get("email") or "").lower()
    name = (lead.get("contact_first_name") or "").lower()
    if email and name and len(name) >= 2:
        return email.split("@")[0].startswith(name)
    return False
