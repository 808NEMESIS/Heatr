"""
scripts/run_scraping_worker.py — Recovery Patch 3: gecommitte scraping-runner.

De scraping-queue had geen productie-runner (het oude scripts/run_worker.py was
een orphan, nooit gecommit) → /search-jobs bleven eeuwig 'pending'. Dit is de
gesuperviseerde entrypoint: hij draait de continue run_scraping_worker-loop, die
pending scraping_jobs claimt en uitvoert (Google Maps / website / directory).

Run los:      python3 scripts/run_scraping_worker.py
Gesuperviseerd: launchd/nl.aerys.heatr.scraping-worker.plist (laadt .env via
                config, KeepAlive). Zie docs/deploy_operator_shell.md-stijl.
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
logger = logging.getLogger("scraping_worker")


async def main() -> None:
    from config.database import get_heatr_supabase
    from job_queue.scraping_queue import run_scraping_worker

    db = get_heatr_supabase()
    workspace_id = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")
    logger.info("Scraping-worker entrypoint: workspace=%s", workspace_id)
    # run_scraping_worker draait indefinitely (while True); claim + dispatch.
    await run_scraping_worker(workspace_id, db)


if __name__ == "__main__":
    asyncio.run(main())
