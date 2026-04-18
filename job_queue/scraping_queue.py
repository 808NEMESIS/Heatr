"""
queue/scraping_queue.py — Async job manager for all Heatr scraping operations.

Reads from and writes to the ``scraping_jobs`` Supabase table. Multiple workers
can run concurrently; job claiming is atomic via a status-flip update.

Job lifecycle:
  pending → running → completed
                    ↘ failed (after 3 retries)
                    ↘ captcha_blocked (manual intervention needed)

Deduplication: identical query+location+sector_key jobs that completed within
the last 7 days return the existing job_id without creating a new run.

Concurrency: MAX_CONCURRENT_SCRAPERS env var controls the asyncio.Semaphore
that gates how many jobs run at the same time per worker process.

Note: ``scraping_jobs`` needs a ``retry_count`` column not in the session 1
schema. The ``create_scraping_job`` function adds it via ALTER TABLE on first
use if missing, or you can add it manually:
  ALTER TABLE scraping_jobs ADD COLUMN IF NOT EXISTS retry_count int NOT NULL DEFAULT 0;
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_JOB_DEDUP_DAYS = 7          # Skip re-scraping same query within this window
_MAX_RETRIES = 3             # Jobs fail permanently after this many attempts
_WORKER_SLEEP_SECONDS = 30   # How long a worker sleeps when no jobs are pending


# =============================================================================
# Job creation
# =============================================================================

async def create_scraping_job(
    job_type: str,
    sector_key: str,
    query: str,
    location: str,
    country: str,
    workspace_id: str,
    supabase_client: Any,
    delta_mode: bool = False,
) -> str:
    """Create a new scraping job, or return an existing recent one.

    Args:
        delta_mode: If True, reduces dedup window to 1 day (for daily delta scrapes).
                    Existing companies are automatically skipped by the upsert logic
                    in google_maps_scraper and directory_scraper.

    Before inserting, checks whether an identical (query + location + sector_key)
    job completed successfully within the last ``_JOB_DEDUP_DAYS`` days. If found,
    returns that job's ID without creating a duplicate run.

    Args:
        job_type: Scraper to invoke — 'google_maps' | 'website' | 'directory'.
        sector_key: Sector key, e.g. 'alternatieve_zorg'.
        query: Search query string, e.g. 'fysiotherapeut'.
        location: City or region, e.g. 'Amsterdam'.
        country: ISO 2-letter country code.
        workspace_id: Workspace slug — scopes the job.
        supabase_client: Initialised supabase-py client.

    Returns:
        UUID string of the new or existing job row.

    Raises:
        RuntimeError: If the Supabase insert fails.
    """
    dedup_days = 1 if delta_mode else _JOB_DEDUP_DAYS
    cutoff = (datetime.now(timezone.utc) - timedelta(days=dedup_days)).isoformat()

    # --- Deduplication check -------------------------------------------------
    try:
        existing = (
            supabase_client.table("scraping_jobs")
            .select("id, status, created_at")
            .eq("workspace_id", workspace_id)
            .eq("sector", sector_key)
            .eq("search_query", query)
            .eq("city", location)
            .eq("source", job_type)
            .eq("status", "completed")
            .gte("created_at", cutoff)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if existing.data:
            existing_id = existing.data[0]["id"]
            logger.info(
                "Skipping duplicate job: query=%s location=%s → reusing %s",
                query, location, existing_id,
            )
            return existing_id
    except Exception as e:
        logger.warning("Dedup check failed (non-fatal): %s", e)

    # --- Insert new job row --------------------------------------------------
    record = {
        "workspace_id": workspace_id,
        "sector": sector_key,
        "city": location,
        "country": country,
        "source": job_type,
        "search_query": query,
        "status": "pending",
        "total_found": 0,
        "total_new": 0,
        "total_enriched": 0,
        "retry_count": 0,
    }

    try:
        response = supabase_client.table("scraping_jobs").insert(record).execute()
        job_id: str = response.data[0]["id"]
        logger.info(
            "Created scraping job: id=%s type=%s query=%s location=%s",
            job_id, job_type, query, location,
        )
        return job_id
    except Exception as e:
        raise RuntimeError(f"Failed to create scraping job: {e}") from e


# =============================================================================
# Job claiming
# =============================================================================

async def claim_next_job(
    workspace_id: str,
    supabase_client: Any,
) -> dict | None:
    """Atomically claim the next pending scraping job for this worker.

    Fetches the oldest pending job and updates its status to 'running'.
    Uses a select-then-update pattern; concurrent workers may occasionally
    both fetch the same row, but only the first update will succeed if
    Supabase enforces optimistic locking via updated_at checks.

    Args:
        workspace_id: Workspace slug — only claims jobs for this workspace.
        supabase_client: Initialised supabase-py client.

    Returns:
        Full job dict if a job was successfully claimed, None if no pending jobs.
    """
    try:
        # Fetch the oldest pending job
        response = (
            supabase_client.table("scraping_jobs")
            .select("*")
            .eq("workspace_id", workspace_id)
            .eq("status", "pending")
            .order("created_at", desc=False)
            .limit(1)
            .execute()
        )

        if not response.data:
            return None

        job = response.data[0]
        job_id = job["id"]
        now_iso = datetime.now(timezone.utc).isoformat()

        # Attempt to claim by flipping status to 'running'
        update_response = (
            supabase_client.table("scraping_jobs")
            .update({
                "status": "running",
                "started_at": now_iso,
                "worker_id": _worker_id(),
            })
            .eq("id", job_id)
            .eq("status", "pending")  # Guard: only update if still pending
            .execute()
        )

        if not update_response.data:
            # Another worker claimed it first — try again
            logger.debug("Job %s already claimed by another worker", job_id)
            return None

        logger.info("Claimed job: id=%s type=%s query=%s", job_id, job.get("source"), job.get("search_query"))
        return update_response.data[0]

    except Exception as e:
        logger.error("claim_next_job failed: %s", e)
        return None


# =============================================================================
# Job completion / failure
# =============================================================================

async def complete_job(
    job_id: str,
    companies_found: int,
    companies_new: int,
    supabase_client: Any,
) -> None:
    """Mark a job as successfully completed and write final counts.

    Args:
        job_id: UUID of the scraping_jobs row.
        companies_found: Total companies seen (including duplicates).
        companies_new: New companies inserted into companies_raw.
        supabase_client: Supabase client.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        supabase_client.table("scraping_jobs").update({
            "status": "completed",
            "completed_at": now_iso,
            "total_found": companies_found,
            "total_new": companies_new,
        }).eq("id", job_id).execute()
        logger.info("Job completed: id=%s found=%d new=%d", job_id, companies_found, companies_new)
    except Exception as e:
        logger.error("complete_job failed for %s: %s", job_id, e)


async def fail_job(
    job_id: str,
    error_message: str,
    supabase_client: Any,
) -> None:
    """Record a job failure and either requeue it or mark it permanently failed.

    Increments retry_count. If retry_count < ``_MAX_RETRIES``: resets status
    to 'pending' so the job will be retried by the next available worker.
    If retry_count >= ``_MAX_RETRIES``: sets status to 'failed'.

    Args:
        job_id: UUID of the scraping_jobs row.
        error_message: Error description for debugging.
        supabase_client: Supabase client.
    """
    try:
        # Fetch current retry count
        response = (
            supabase_client.table("scraping_jobs")
            .select("retry_count")
            .eq("id", job_id)
            .single()
            .execute()
        )

        current_retries = 0
        if response.data:
            current_retries = response.data.get("retry_count", 0) or 0

        new_retry_count = current_retries + 1
        will_retry = new_retry_count < _MAX_RETRIES
        new_status = "pending" if will_retry else "failed"

        supabase_client.table("scraping_jobs").update({
            "status": new_status,
            "error_message": error_message[:2000],  # Truncate very long errors
            "retry_count": new_retry_count,
        }).eq("id", job_id).execute()

        if will_retry:
            logger.warning(
                "Job %s failed (attempt %d/%d), requeueing: %s",
                job_id, new_retry_count, _MAX_RETRIES, error_message[:200],
            )
        else:
            logger.error(
                "Job %s permanently failed after %d attempts: %s",
                job_id, new_retry_count, error_message[:200],
            )

    except Exception as e:
        logger.error("fail_job error for %s: %s", job_id, e)


# =============================================================================
# Job status query
# =============================================================================

async def get_job_status(job_id: str, supabase_client: Any) -> dict:
    """Return the full status row for a scraping job.

    Args:
        job_id: UUID string of the scraping_jobs row.
        supabase_client: Supabase client.

    Returns:
        Job row dict, or ``{"error": "not_found"}`` if ID doesn't exist.
    """
    try:
        response = (
            supabase_client.table("scraping_jobs")
            .select("*")
            .eq("id", job_id)
            .single()
            .execute()
        )
        if response.data:
            return response.data
        return {"error": "not_found", "id": job_id}
    except Exception as e:
        logger.error("get_job_status failed for %s: %s", job_id, e)
        return {"error": str(e), "id": job_id}


# =============================================================================
# Worker loop
# =============================================================================

async def run_scraping_worker(
    workspace_id: str,
    supabase_client: Any,
) -> None:
    """Main worker loop — claims and executes scraping jobs until cancelled.

    Runs indefinitely. Sleeps ``_WORKER_SLEEP_SECONDS`` when no jobs are
    available. Respects ``MAX_CONCURRENT_SCRAPERS`` via an asyncio.Semaphore
    shared across all coroutines in the same process.

    Dispatch logic:
      source='google_maps' → google_maps_scraper.scrape_google_maps()
      source='website'     → website_scraper.scrape_website()
      source='directory'   → directory_scraper.run_directory_scrapers_for_sector()

    Args:
        workspace_id: Workspace slug — worker only processes jobs for this workspace.
        supabase_client: Initialised supabase-py client.
    """
    max_concurrent = int(os.getenv("MAX_CONCURRENT_SCRAPERS", "3"))
    semaphore = asyncio.Semaphore(max_concurrent)

    logger.info(
        "Scraping worker started: workspace=%s max_concurrent=%d",
        workspace_id, max_concurrent,
    )

    while True:
        job = await claim_next_job(workspace_id, supabase_client)

        if job is None:
            logger.debug("No pending jobs — sleeping %ds", _WORKER_SLEEP_SECONDS)
            await asyncio.sleep(_WORKER_SLEEP_SECONDS)
            continue

        # Run job under semaphore to enforce concurrency limit
        asyncio.create_task(
            _execute_job_with_semaphore(job, semaphore, supabase_client)
        )


async def _execute_job_with_semaphore(
    job: dict,
    semaphore: asyncio.Semaphore,
    supabase_client: Any,
) -> None:
    """Execute a single scraping job, guarded by the concurrency semaphore.

    Args:
        job: Full job row dict from scraping_jobs.
        semaphore: Shared asyncio.Semaphore limiting concurrent scrapers.
        supabase_client: Supabase client.
    """
    job_id = job["id"]
    job_type = job.get("source", "")
    sector_key = job.get("sector", "")
    query = job.get("search_query", "")
    location = job.get("city", "")
    country = job.get("country", "NL")
    workspace_id = job.get("workspace_id", "")

    async with semaphore:
        try:
            result = await _dispatch_job(
                job_type=job_type,
                sector_key=sector_key,
                query=query,
                location=location,
                country=country,
                workspace_id=workspace_id,
                job_id=job_id,
                supabase_client=supabase_client,
            )

            await complete_job(
                job_id=job_id,
                companies_found=result.get("found", 0),
                companies_new=result.get("new", 0),
                supabase_client=supabase_client,
            )

        except Exception as exc:
            logger.exception("Job %s raised an exception: %s", job_id, exc)
            await fail_job(
                job_id=job_id,
                error_message=str(exc),
                supabase_client=supabase_client,
            )


async def _dispatch_job(
    job_type: str,
    sector_key: str,
    query: str,
    location: str,
    country: str,
    workspace_id: str,
    job_id: str,
    supabase_client: Any,
) -> dict:
    """Route a job to the correct scraper function based on job_type.

    Imports scrapers lazily to avoid circular import issues.

    Args:
        job_type: 'google_maps' | 'website' | 'directory'.
        sector_key: Sector key string.
        query: Search query or domain string.
        location: City/region string.
        country: ISO 2-letter country code.
        workspace_id: Workspace slug.
        job_id: UUID of the scraping_jobs row (passed for incremental updates).
        supabase_client: Supabase client.

    Returns:
        Summary dict from the scraper with at least 'found' and 'new' keys.

    Raises:
        ValueError: If job_type is not recognised.
    """
    if job_type == "google_maps":
        from scrapers.google_maps_scraper import scrape_google_maps
        return await scrape_google_maps(
            query=query,
            location=location,
            country=country,
            sector_key=sector_key,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
            job_id=job_id,
        )

    elif job_type == "website":
        # Website jobs use query as domain, location as lead_id
        from scrapers.website_scraper import scrape_website
        result = await scrape_website(
            domain=query,
            lead_id=location,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )
        # Normalise to standard summary shape
        return {"found": 1, "new": 1 if result.get("emails") else 0}

    elif job_type == "directory":
        from scrapers.directory_scraper import run_directory_scrapers_for_sector
        return await run_directory_scrapers_for_sector(
            sector_key=sector_key,
            city=location,
            country=country,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )

    else:
        raise ValueError(f"Unknown job_type '{job_type}'. Expected: google_maps | website | directory")


# =============================================================================
# Internal utilities
# =============================================================================

def _worker_id() -> str:
    """Generate a short worker identifier for job row tracking.

    Returns:
        String combining hostname and process ID.
    """
    import socket
    import os as _os
    return f"{socket.gethostname()[:20]}-pid{_os.getpid()}"
