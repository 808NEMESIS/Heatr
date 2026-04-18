"""
utils/metrics_collector.py — Daily metrics collection for Heatr.

Called daily at 23:55 by n8n workflow 05-daily-metrics.
Calculates all KPIs and stores in daily_metrics table.
After storing, checks metric alerts.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


async def collect_daily_metrics(
    workspace_id: str,
    supabase_client,
    target_date: str | None = None,
) -> dict[str, Any]:
    """
    Calculate and store all daily metrics for workspace.

    Args:
        workspace_id: Workspace to collect metrics for.
        supabase_client: Supabase client.
        target_date: ISO date string (YYYY-MM-DD). Defaults to today.

    Returns:
        The stored metrics dict.
    """
    today = target_date or datetime.now(timezone.utc).date().isoformat()
    today_start = f"{today}T00:00:00+00:00"
    today_end   = f"{today}T23:59:59+00:00"

    logger.info("Collecting daily metrics for workspace=%s date=%s", workspace_id, today)
    metrics: dict[str, Any] = {"workspace_id": workspace_id, "date": today}

    # -------------------------------------------------------------------------
    # Scraping / Discovery
    # -------------------------------------------------------------------------
    try:
        disc_res = supabase_client.table("leads").select("id", count="exact").eq(
            "workspace_id", workspace_id).gte("created_at", today_start).lte("created_at", today_end).execute()
        metrics["companies_discovered"] = disc_res.count or 0
    except Exception as e:
        logger.warning("metrics: companies_discovered failed: %s", e)
        metrics["companies_discovered"] = 0

    try:
        enr_res = supabase_client.table("leads").select("id", count="exact").eq(
            "workspace_id", workspace_id).gte("updated_at", today_start).gt("score", 0).execute()
        metrics["leads_enriched"] = enr_res.count or 0
    except Exception:
        metrics["leads_enriched"] = 0

    try:
        qual_res = supabase_client.table("leads").select("id", count="exact").eq(
            "workspace_id", workspace_id).gte("updated_at", today_start).gte("score", 65).execute()
        metrics["leads_qualified"] = qual_res.count or 0
    except Exception:
        metrics["leads_qualified"] = 0

    try:
        total_leads = supabase_client.table("leads").select("id, email_status", count="exact").eq(
            "workspace_id", workspace_id).execute()
        total = total_leads.count or 0
        covered = sum(1 for l in (total_leads.data or []) if l.get("email_status") in ("verified", "catch_all"))
        metrics["email_coverage_rate"] = round(covered / total, 4) if total else 0.0
    except Exception:
        metrics["email_coverage_rate"] = 0.0

    # -------------------------------------------------------------------------
    # Sending
    # -------------------------------------------------------------------------
    try:
        sent_res = supabase_client.table("lead_campaign_history").select("id", count="exact").eq(
            "workspace_id", workspace_id).gte("sent_at", today_start).execute()
        metrics["emails_sent"] = sent_res.count or 0
    except Exception:
        metrics["emails_sent"] = 0

    try:
        blocked_res = supabase_client.table("blocked_sends").select("id", count="exact").eq(
            "workspace_id", workspace_id).gte("blocked_at", today_start).execute()
        metrics["emails_blocked"] = blocked_res.count or 0
    except Exception:
        metrics["emails_blocked"] = 0

    try:
        inbox_res = supabase_client.table("reply_inbox").select("event_type").eq(
            "workspace_id", workspace_id).gte("received_at", today_start).execute()
        inbox_events = inbox_res.data or []
        bounced   = sum(1 for e in inbox_events if e.get("event_type") == "bounced")
        spam      = sum(1 for e in inbox_events if e.get("event_type") == "spam")
        unsub     = sum(1 for e in inbox_events if e.get("event_type") == "unsubscribed")
        replies   = sum(1 for e in inbox_events if e.get("event_type") in ("replied", "interested"))
        interested = sum(1 for e in inbox_events if e.get("event_type") == "interested")
        opens     = sum(1 for e in inbox_events if e.get("event_type") == "opened")

        sent = metrics["emails_sent"] or 1  # avoid zero-div
        metrics["bounce_count"]          = bounced
        metrics["bounce_rate"]           = round(bounced / sent, 4)
        metrics["spam_complaint_count"]  = spam
        metrics["unsubscribe_count"]     = unsub
        metrics["unsubscribe_rate"]      = round(unsub / sent, 4)
        metrics["open_count"]            = opens
        metrics["open_rate"]             = round(opens / sent, 4)
        metrics["reply_count"]           = replies
        metrics["reply_rate"]            = round(replies / sent, 4)
        metrics["interested_count"]      = interested
        metrics["meeting_rate"]          = round(interested / sent, 4)
    except Exception as e:
        logger.warning("metrics: inbox events failed: %s", e)
        for k in ("bounce_count","bounce_rate","spam_complaint_count","unsubscribe_count",
                  "unsubscribe_rate","open_count","open_rate","reply_count","reply_rate",
                  "interested_count","meeting_rate"):
            metrics.setdefault(k, 0)

    # -------------------------------------------------------------------------
    # Website intelligence
    # -------------------------------------------------------------------------
    try:
        wi_res = supabase_client.table("website_intelligence").select(
            "total_score, opportunity_types, priority"
        ).eq("workspace_id", workspace_id).gte("created_at", today_start).execute()
        wi_rows = wi_res.data or []
        metrics["websites_analysed"] = len(wi_rows)
        scores = [r.get("total_score") or 0 for r in wi_rows]
        metrics["avg_website_score"] = round(sum(scores) / len(scores), 1) if scores else None
        metrics["urgent_opportunities"] = sum(1 for r in wi_rows if r.get("priority") == "urgent")
    except Exception:
        metrics.setdefault("websites_analysed", 0)
        metrics.setdefault("avg_website_score", None)
        metrics.setdefault("urgent_opportunities", 0)

    try:
        rev_res = supabase_client.table("lead_timeline").select("id", count="exact").eq(
            "workspace_id", workspace_id).eq("event_type", "review_email_sent").gte("created_at", today_start).execute()
        metrics["review_emails_sent"] = rev_res.count or 0
    except Exception:
        metrics["review_emails_sent"] = 0

    # -------------------------------------------------------------------------
    # CRM
    # -------------------------------------------------------------------------
    try:
        deals_res = supabase_client.table("crm_deals").select("value").eq(
            "workspace_id", workspace_id).gte("created_at", today_start).execute()
        deals = deals_res.data or []
        metrics["deals_won"]    = len(deals)
        metrics["revenue_won"]  = round(sum(d.get("value") or 0 for d in deals), 2)
    except Exception:
        metrics["deals_won"]   = 0
        metrics["revenue_won"] = 0.0

    try:
        tasks_done = supabase_client.table("crm_tasks").select("id", count="exact").eq(
            "workspace_id", workspace_id).gte("completed_at", today_start).execute()
        tasks_new = supabase_client.table("crm_tasks").select("id", count="exact").eq(
            "workspace_id", workspace_id).gte("created_at", today_start).execute()
        metrics["tasks_completed"] = tasks_done.count or 0
        metrics["tasks_created"]   = tasks_new.count or 0
    except Exception:
        metrics["tasks_completed"] = 0
        metrics["tasks_created"]   = 0

    # -------------------------------------------------------------------------
    # Costs
    # -------------------------------------------------------------------------
    try:
        cost_res = supabase_client.table("api_cost_log").select("cost_eur").eq(
            "workspace_id", workspace_id).eq("date", today).execute()
        metrics["estimated_cost_eur"] = round(sum(r.get("cost_eur") or 0 for r in (cost_res.data or [])), 4)
    except Exception:
        metrics["estimated_cost_eur"] = 0.0

    # -------------------------------------------------------------------------
    # Upsert to daily_metrics
    # -------------------------------------------------------------------------
    try:
        supabase_client.table("daily_metrics").upsert(
            metrics, on_conflict="workspace_id,date"
        ).execute()
        logger.info("Daily metrics stored for %s / %s", workspace_id, today)
    except Exception as e:
        logger.error("Failed to store daily metrics: %s", e)

    # -------------------------------------------------------------------------
    # Metric alerts
    # -------------------------------------------------------------------------
    from utils.alert_manager import check_metric_alerts
    try:
        await check_metric_alerts(metrics, workspace_id, supabase_client)
    except Exception as e:
        logger.warning("Metric alert check failed: %s", e)

    return metrics


async def get_metrics_range(
    workspace_id: str,
    days: int,
    supabase_client,
) -> list[dict]:
    """
    Return daily_metrics rows for the last N days.
    Used by GET /analytics/metrics.
    """
    from datetime import date
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
    try:
        res = (
            supabase_client.table("daily_metrics")
            .select("*")
            .eq("workspace_id", workspace_id)
            .gte("date", cutoff)
            .order("date", desc=True)
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.error("get_metrics_range failed: %s", e)
        return []
