"""
scrapers/discovery_scheduler.py — Automatic recurring lead discovery.

Defines sector + city combinations that should be scraped on a recurring
schedule. Each time a schedule is due, a delta-scrape runs (skip bestaande
bedrijven, alleen nieuwe binnenhalen).

Called daily by n8n / cron. No manual triggers needed for configured schedules.

Schedule table structure:
  id              uuid
  workspace_id    text
  sector          text       — sector key
  city            text       — city name
  country         text       — 'NL' or 'BE'
  frequency_days  int        — run every N days
  target_new_leads int       — stop if already N new leads found this run
  max_results     int        — cap per run
  last_run_at     timestamptz
  next_run_at     timestamptz
  active          boolean
  created_at      timestamptz
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


async def get_due_schedules(
    workspace_id: str,
    supabase_client: Any,
) -> list[dict]:
    """
    Return all active schedules whose next_run_at has passed.

    Called by the orchestrator to decide which scrapes to trigger.
    """
    now = datetime.now(timezone.utc).isoformat()
    try:
        res = (
            supabase_client.table("lead_discovery_schedules")
            .select("*")
            .eq("workspace_id", workspace_id)
            .eq("active", True)
            .lte("next_run_at", now)
            .order("next_run_at", desc=False)
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.error("get_due_schedules failed: %s", e)
        return []


async def create_schedule(
    workspace_id: str,
    sector: str,
    city: str,
    frequency_days: int,
    supabase_client: Any,
    country: str = "NL",
    target_new_leads: int = 20,
    max_results: int = 40,
) -> str | None:
    """Create a new recurring schedule. Returns schedule_id or None."""
    now = datetime.now(timezone.utc)
    try:
        res = (
            supabase_client.table("lead_discovery_schedules")
            .insert({
                "workspace_id": workspace_id,
                "sector": sector,
                "city": city,
                "country": country,
                "frequency_days": frequency_days,
                "target_new_leads": target_new_leads,
                "max_results": max_results,
                "next_run_at": now.isoformat(),  # Eerste run meteen
                "active": True,
            })
            .execute()
        )
        if res.data:
            return res.data[0]["id"]
    except Exception as e:
        logger.error("create_schedule failed: %s", e)
    return None


async def run_due_schedules(
    workspace_id: str,
    supabase_client: Any,
) -> dict[str, Any]:
    """
    Execute all due schedules. Returns summary stats.

    For each due schedule:
      1. Trigger a scraping_job via the normal queue
      2. Update last_run_at + next_run_at
      3. Log result

    This is the main entry point called by cron/n8n every morning.
    """
    from job_queue.scraping_queue import create_scraping_job
    from config.sectors import get_sector

    due = await get_due_schedules(workspace_id, supabase_client)
    results = {
        "schedules_processed": 0,
        "scrape_jobs_created": 0,
        "errors": [],
    }

    for schedule in due:
        sched_id = schedule["id"]
        sector = schedule["sector"]
        city = schedule["city"]
        country = schedule.get("country") or "NL"
        freq = schedule.get("frequency_days") or 14
        max_results = schedule.get("max_results") or 40

        logger.info(
            "run_due_schedules: triggering %s in %s (every %d days)",
            sector, city, freq,
        )

        try:
            # Use first query from sector config as the scraping query
            sector_config = get_sector(sector)
            query = sector_config["search_queries"][0].replace("{city}", city)

            # Enqueue the scrape (dedup is handled by scraping_queue with delta_mode)
            job_id = await create_scraping_job(
                job_type="google_maps",
                sector_key=sector,
                query=query,
                location=city,
                country=country,
                workspace_id=workspace_id,
                supabase_client=supabase_client,
                delta_mode=True,  # 1-day dedup window instead of 7 days
            )
            if job_id:
                results["scrape_jobs_created"] += 1

            # Schedule next run
            now = datetime.now(timezone.utc)
            next_run = now + timedelta(days=freq)
            supabase_client.table("lead_discovery_schedules").update({
                "last_run_at": now.isoformat(),
                "next_run_at": next_run.isoformat(),
            }).eq("id", sched_id).execute()

            results["schedules_processed"] += 1

        except Exception as e:
            logger.error("Schedule %s failed: %s", sched_id, e)
            results["errors"].append({"schedule_id": sched_id, "error": str(e)})

    return results


async def list_schedules(
    workspace_id: str,
    supabase_client: Any,
    active_only: bool = False,
) -> list[dict]:
    """Return all configured schedules."""
    try:
        q = supabase_client.table("lead_discovery_schedules").select("*").eq(
            "workspace_id", workspace_id,
        )
        if active_only:
            q = q.eq("active", True)
        res = q.order("next_run_at", desc=False).execute()
        return res.data or []
    except Exception as e:
        logger.error("list_schedules failed: %s", e)
        return []


async def pause_schedule(schedule_id: str, supabase_client: Any) -> bool:
    try:
        supabase_client.table("lead_discovery_schedules").update(
            {"active": False}
        ).eq("id", schedule_id).execute()
        return True
    except Exception:
        return False


async def delete_schedule(schedule_id: str, supabase_client: Any) -> bool:
    try:
        supabase_client.table("lead_discovery_schedules").delete().eq(
            "id", schedule_id,
        ).execute()
        return True
    except Exception:
        return False
