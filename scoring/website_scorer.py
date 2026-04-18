"""
scoring/website_scorer.py — Website quality scoring.

Reads from the website_intelligence table and applies the 5-layer weights
from config/scoring_weights.py. Pure calculation — actual analysis done
by website_intelligence modules.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def score_website(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
) -> int:
    """
    Compute final website score from website_intelligence data.

    Returns 0-100 score. Also updates leads.website_score.
    """
    try:
        res = supabase_client.table("website_intelligence").select(
            "total_score, technical_score, visual_score, conversion_score, sector_score",
        ).eq("lead_id", lead_id).maybe_single().execute()

        if not res.data:
            return 0

        score = res.data.get("total_score") or 0
        return min(int(score), 100)
    except Exception as e:
        logger.debug("website_scorer: failed to read for lead %s: %s", lead_id, e)
        return 0
