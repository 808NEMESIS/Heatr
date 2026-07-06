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

    # Sectors.py v2 (2026-04-21) heeft geen scoring_boosts meer per sector.
    # Fit-score komt volledig uit icp_match + review_count signalen hieronder.
    # Zie CHANGES_PR2.md voor rationale.

    # Review count signal — high review count = active, established business
    review_count = lead.get("google_review_count") or 0
    if review_count >= 100:
        fit_score = min(fit_score + 4, 40)
    elif review_count >= 50:
        fit_score = min(fit_score + 3, 40)
    elif review_count >= 20:
        fit_score = min(fit_score + 2, 40)
    elif review_count >= 5:
        fit_score = min(fit_score + 1, 40)
    # 0 reviews = no boost (not penalized, but no signal of activity)

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
    # Compliance (gdpr_safe + status) via de CENTRALE gate — geen eigen
    # duplicaat-oordeel meer (Sprint 1-audit §2: dit was de drift-plek).
    from utils.enrichment_check import compliance_check
    compliant, compliance_reason = compliance_check(lead)
    if not compliant:
        block_reasons.append(compliance_reason or "compliance-block")

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


