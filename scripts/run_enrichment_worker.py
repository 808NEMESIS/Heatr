"""
scripts/run_enrichment_worker.py — Continuous enrichment-worker loop.

Calls process_next_enrichment() every ~15 seconds. Cost-guard and rate-limiter
hold back automatically; this loop just keeps feeding jobs.

Run:
    python3 scripts/run_enrichment_worker.py            # exit na 3 idle polls
    python3 scripts/run_enrichment_worker.py --once
    python3 scripts/run_enrichment_worker.py --daemon   # blijf draaien (persistent)
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


_IDLE_EXIT_THRESHOLD = 3


def should_exit_when_idle(empty_in_row: int, daemon: bool) -> bool:
    """Beslis of de worker stopt na een reeks lege polls. In daemon-modus nooit;
    anders na `_IDLE_EXIT_THRESHOLD` opeenvolgende lege polls (one-shot/cron)."""
    return (not daemon) and empty_in_row >= _IDLE_EXIT_THRESHOLD

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
    # Daemon-modus: blijf draaien ook als de queue leeg is (voor een persistente
    # worker naast de scraping-worker). Default = exit na 3 idle polls, zodat
    # bestaande cron-runs die 't als one-shot gebruiken niet veranderen.
    daemon = "--daemon" in sys.argv or os.getenv("HEATR_ENRICHMENT_DAEMON", "").lower() == "true"

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
            if should_exit_when_idle(empty_in_row, daemon):
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
