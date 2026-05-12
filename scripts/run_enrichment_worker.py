"""
scripts/run_enrichment_worker.py — Continuous enrichment-worker loop.

Calls process_next_enrichment() every ~15 seconds. Cost-guard and rate-limiter
hold back automatically; this loop just keeps feeding jobs.

Run:
    python3 scripts/run_enrichment_worker.py
    python3 scripts/run_enrichment_worker.py --once
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
logger = logging.getLogger("enrichment_worker")


async def main():
    from config.database import get_heatr_supabase
    from job_queue.enrichment_queue import process_next_enrichment

    db = get_heatr_supabase()
    workspace_id = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")
    once = "--once" in sys.argv

    iteration = 0
    empty_in_row = 0
    while True:
        iteration += 1
        try:
            result = await process_next_enrichment(workspace_id, db)
        except Exception as e:
            logger.exception("process_next_enrichment raised: %s", e)
            result = None

        if result is None:
            empty_in_row += 1
            if empty_in_row >= 3:
                logger.info("No jobs for 3 iterations — exiting gracefully")
                break
            logger.info("Queue empty; sleeping 30s before retry")
            await asyncio.sleep(30)
            continue

        empty_in_row = 0
        if result.get("processed"):
            logger.info(
                "[%d] processed lead=%s job=%s",
                iteration, result.get("lead_id"), result.get("job_id"),
            )
        else:
            logger.warning(
                "[%d] NOT processed — lead=%s error=%s",
                iteration, result.get("lead_id"), result.get("error"),
            )

        if once:
            break

        # Small delay — natural rate-limiters (RDAP/Meta/Claude) throttle further
        await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(main())
