"""
scripts/run_website_worker.py — dedicated worker voor website-intelligence-analyse.

De zware website-analyse (Playwright screenshot + Claude Vision + PageSpeed +
meerdere Claude-calls) is 2026-07-15 uit de inline enrichment gehaald omdat hij
de single-threaded enrichment-worker minutenlang kon blokkeren. Hij draait nu
GEÏSOLEERD hier: een stall raakt alleen de website-analyse, niet de kern-
pijplijn (email → score → launchbaar).

Roept process_next_website_analysis() aan (self-selecting op eligible leads).
Elke analyse heeft een eigen harde timeout (WEBSITE_ANALYSIS_TIMEOUT).

Run:
    python3 scripts/run_website_worker.py            # exit na 3 idle polls
    python3 scripts/run_website_worker.py --daemon   # blijf draaien (persistent)
    python3 scripts/run_website_worker.py --once
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("website_worker")

_IDLE_EXIT_THRESHOLD = 3


def should_exit_when_idle(empty_in_row: int, daemon: bool) -> bool:
    """Stop na een reeks lege polls, tenzij daemon-modus (dan nooit)."""
    return (not daemon) and empty_in_row >= _IDLE_EXIT_THRESHOLD


async def main():
    from config.database import get_heatr_supabase
    from job_queue.website_analysis_queue import process_next_website_analysis

    db = get_heatr_supabase()
    workspace_id = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")
    once = "--once" in sys.argv
    daemon = "--daemon" in sys.argv or os.getenv("HEATR_WEBSITE_DAEMON", "").lower() == "true"

    iteration = 0
    empty_in_row = 0
    while True:
        iteration += 1
        try:
            result = await process_next_website_analysis(workspace_id, db)
        except Exception as e:
            logger.exception("process_next_website_analysis raised: %s", e)
            result = None

        if result is None:
            empty_in_row += 1
            if should_exit_when_idle(empty_in_row, daemon):
                logger.info("No eligible leads for 3 iterations — exiting gracefully")
                break
            logger.info("Geen eligible leads; 30s slapen")
            await asyncio.sleep(30)
            continue

        empty_in_row = 0
        if result.get("processed"):
            logger.info("[%d] analyzed lead=%s domain=%s score=%s",
                        iteration, result.get("lead_id"), result.get("domain"),
                        result.get("total_score"))
        else:
            logger.info("[%d] overgeslagen lead=%s domain=%s reden=%s",
                        iteration, result.get("lead_id"), result.get("domain"),
                        result.get("error") or result.get("reason"))

        if once:
            break
        await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())
