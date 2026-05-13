"""
scripts/run_worker.py — Start the Heatr scraping worker.

Picks up pending scraping_jobs from the queue and executes them
(Playwright scrapes, directory scrapes, etc.).

Run continuously:
    python scripts/run_worker.py

Or via cron (if worker daemon not running):
    */5 * * * * cd /Users/nemesis/Heatr && python3 scripts/run_worker.py --once
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
if not os.getenv("SUPABASE_URL"):
    load_dotenv("/Users/nemesis/warmr/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("worker")


async def run_once(workspace_id: str, supabase_client) -> bool:
    """Run exactly one job (or return False if none pending)."""
    from job_queue.scraping_queue import claim_next_job, _execute_job_with_semaphore

    job = await claim_next_job(workspace_id, supabase_client)
    if job is None:
        logger.info("No pending jobs")
        return False

    logger.info("Executing job: %s %s in %s", job.get("source"), job.get("sector"), job.get("city"))
    semaphore = asyncio.Semaphore(1)
    await _execute_job_with_semaphore(job, semaphore, supabase_client)
    return True


async def run_continuous(workspace_id: str, supabase_client) -> None:
    """Run the worker loop indefinitely."""
    from job_queue.scraping_queue import run_scraping_worker
    await run_scraping_worker(workspace_id, supabase_client)


async def main():
    from config.database import get_heatr_supabase
    sb = get_heatr_supabase()
    workspace_id = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")

    once = "--once" in sys.argv

    if once:
        # Process one at a time, exit when queue is empty
        processed = 0
        while await run_once(workspace_id, sb):
            processed += 1
        logger.info("Worker finished — processed %d jobs", processed)
    else:
        logger.info("Starting continuous worker (Ctrl+C to stop)")
        await run_continuous(workspace_id, sb)


if __name__ == "__main__":
    asyncio.run(main())
