"""
utils/pipeline_metrics.py — Pipeline health monitoring for Heatr.

Tracks lead conversion through every stage of the pipeline.
Shows exactly where leads drop off and why.

Called daily by the metrics collection workflow, or on-demand via
GET /analytics/pipeline-health.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


async def collect_pipeline_health(
    workspace_id: str,
    supabase_client: Any,
    days: int = 1,
) -> dict[str, Any]:
    """
    Compute pipeline conversion metrics for the last N days.

    Tracks how many leads pass through each stage and where they drop off.

    Returns:
        {
            "period_days": 1,
            "funnel": {
                "raw_companies": 142,
                "qualified": 98,
                "email_found": 71,
                "email_verified": 54,
                "website_analyzed": 48,
                "contact_found": 38,
                "data_verified": 35,
                "scored_above_65": 29,
                "pushed_to_warmr": 24,
            },
            "drop_off": {
                "qualification": {"pct": 31, "top_reasons": [...]},
                "email_discovery": {"pct": 28, "top_reasons": [...]},
                ...
            },
            "costs": {
                "total_claude_eur": 0.45,
                "per_qualified_lead": 0.0046,
                "per_pushed_lead": 0.019,
            },
        }
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    funnel: dict[str, int] = {}
    drop_off: dict[str, dict] = {}

    # --- Raw companies discovered ---
    try:
        res = supabase_client.table("companies_raw").select("id", count="exact").eq(
            "workspace_id", workspace_id,
        ).gte("created_at", cutoff).execute()
        funnel["raw_companies"] = res.count or 0
    except Exception:
        funnel["raw_companies"] = 0

    # --- Qualified (became leads) ---
    try:
        res = supabase_client.table("leads").select("id", count="exact").eq(
            "workspace_id", workspace_id,
        ).gte("created_at", cutoff).execute()
        funnel["qualified"] = res.count or 0
    except Exception:
        funnel["qualified"] = 0

    # --- Disqualified reasons ---
    try:
        disq_res = supabase_client.table("companies_raw").select(
            "disqualification_reason",
        ).eq("workspace_id", workspace_id).eq(
            "qualification_status", "disqualified",
        ).gte("created_at", cutoff).execute()

        reasons: dict[str, int] = {}
        for row in (disq_res.data or []):
            reason = row.get("disqualification_reason") or "unknown"
            reasons[reason] = reasons.get(reason, 0) + 1

        sorted_reasons = sorted(reasons.items(), key=lambda x: x[1], reverse=True)[:5]
        drop_off["qualification"] = {
            "dropped": funnel["raw_companies"] - funnel["qualified"],
            "pct": _pct(funnel["raw_companies"] - funnel["qualified"], funnel["raw_companies"]),
            "top_reasons": [{"reason": r, "count": c} for r, c in sorted_reasons],
        }
    except Exception:
        drop_off["qualification"] = {"dropped": 0, "pct": 0, "top_reasons": []}

    # --- Email found ---
    try:
        res = supabase_client.table("leads").select("id", count="exact").eq(
            "workspace_id", workspace_id,
        ).gte("created_at", cutoff).neq("email_status", "not_found").not_.is_("email", "null").execute()
        funnel["email_found"] = res.count or 0
    except Exception:
        funnel["email_found"] = 0

    drop_off["email_discovery"] = {
        "dropped": funnel["qualified"] - funnel["email_found"],
        "pct": _pct(funnel["qualified"] - funnel["email_found"], funnel["qualified"]),
    }

    # --- Email verified (valid or risky) ---
    try:
        res = supabase_client.table("leads").select("id", count="exact").eq(
            "workspace_id", workspace_id,
        ).gte("created_at", cutoff).in_("email_status", ["valid", "risky"]).execute()
        funnel["email_verified"] = res.count or 0
    except Exception:
        funnel["email_verified"] = 0

    drop_off["email_verification"] = {
        "dropped": funnel["email_found"] - funnel["email_verified"],
        "pct": _pct(funnel["email_found"] - funnel["email_verified"], funnel["email_found"]),
    }

    # --- Website analyzed ---
    try:
        res = supabase_client.table("website_intelligence").select("id", count="exact").eq(
            "workspace_id", workspace_id,
        ).gte("analyzed_at", cutoff).execute()
        funnel["website_analyzed"] = res.count or 0
    except Exception:
        funnel["website_analyzed"] = 0

    # --- Contact found ---
    try:
        res = supabase_client.table("leads").select("id", count="exact").eq(
            "workspace_id", workspace_id,
        ).gte("created_at", cutoff).not_.is_("contact_first_name", "null").execute()
        funnel["contact_found"] = res.count or 0
    except Exception:
        funnel["contact_found"] = 0

    drop_off["contact_discovery"] = {
        "dropped": funnel["email_verified"] - funnel["contact_found"],
        "pct": _pct(funnel["email_verified"] - funnel["contact_found"], funnel["email_verified"]),
    }

    # --- Data verified (quality > 0.5) ---
    try:
        res = supabase_client.table("leads").select("id", count="exact").eq(
            "workspace_id", workspace_id,
        ).gte("created_at", cutoff).gte("data_quality_score", 0.5).execute()
        funnel["data_verified"] = res.count or 0
    except Exception:
        funnel["data_verified"] = 0

    # --- Scored above 65 ---
    try:
        res = supabase_client.table("leads").select("id", count="exact").eq(
            "workspace_id", workspace_id,
        ).gte("created_at", cutoff).gte("score", 65).execute()
        funnel["scored_above_65"] = res.count or 0
    except Exception:
        funnel["scored_above_65"] = 0

    drop_off["scoring"] = {
        "dropped": funnel["contact_found"] - funnel["scored_above_65"],
        "pct": _pct(funnel["contact_found"] - funnel["scored_above_65"], funnel["contact_found"]),
    }

    # --- Pushed to Warmr ---
    try:
        res = supabase_client.table("lead_campaign_history").select("id", count="exact").eq(
            "workspace_id", workspace_id,
        ).gte("created_at", cutoff).execute()
        funnel["pushed_to_warmr"] = res.count or 0
    except Exception:
        funnel["pushed_to_warmr"] = 0

    # --- Costs ---
    costs: dict[str, float] = {}
    try:
        cost_res = supabase_client.table("api_cost_log").select("cost_eur").eq(
            "workspace_id", workspace_id,
        ).gte("date", cutoff[:10]).execute()
        total_cost = sum(r.get("cost_eur") or 0 for r in (cost_res.data or []))
        costs["total_claude_eur"] = round(total_cost, 4)
        costs["per_qualified_lead"] = round(total_cost / max(funnel["qualified"], 1), 4)
        costs["per_pushed_lead"] = round(total_cost / max(funnel["pushed_to_warmr"], 1), 4)
    except Exception:
        costs = {"total_claude_eur": 0, "per_qualified_lead": 0, "per_pushed_lead": 0}

    # --- Overall conversion rate ---
    overall_rate = _pct(funnel.get("pushed_to_warmr", 0), funnel.get("raw_companies", 0))

    return {
        "period_days": days,
        "funnel": funnel,
        "drop_off": drop_off,
        "costs": costs,
        "overall_conversion_pct": overall_rate,
    }


def _pct(part: int, whole: int) -> int:
    """Calculate percentage, returning 0 if whole is 0."""
    if whole <= 0:
        return 0
    return round((part / whole) * 100)
