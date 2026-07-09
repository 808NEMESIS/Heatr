"""
utils/queue_reaper.py — Recovery Patch 3: stuck-job recovery + worker-liveness.

Beide queues (scraping_jobs, enrichment_jobs) claimen een job door status naar
'running' te flippen met een started_at. Crasht de verwerking, dan blijft de rij
EEUWIG 'running' — geen heartbeat, geen visibility-timeout, geen reaper — en
verdwijnt permanent uit de pipeline. Deze reaper requeue't zulke rijen
(running ouder dan N minuten → pending) tot een retry-plafond; daarna
dead-letter (→ failed). Vondst van stuck jobs is tevens een liveness-signaal:
een gezonde worker laat niets lang op 'running' staan.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

REAPER_TABLES: tuple[str, ...] = ("scraping_jobs", "enrichment_jobs")
DEFAULT_STUCK_MINUTES = 30
DEFAULT_MAX_RETRIES = 3


async def requeue_stuck_jobs(
    supabase_client: Any,
    *,
    stuck_minutes: int = DEFAULT_STUCK_MINUTES,
    max_retries: int = DEFAULT_MAX_RETRIES,
    tables: tuple[str, ...] = REAPER_TABLES,
) -> dict:
    """Requeue of dead-letter jobs die te lang op 'running' staan.

    Returns een samenvatting per tabel: {table: {requeued, dead_lettered, stuck}}.
    Faalt per-tabel zacht (één ontbrekende/afwijkende tabel stopt de rest niet),
    maar logt luid — geen stille except rond deze pipeline-recovery.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=stuck_minutes)).isoformat()
    summary: dict[str, dict] = {}

    for table in tables:
        requeued = dead = 0
        try:
            stuck = (
                supabase_client.table(table)
                .select("id, retry_count, started_at")
                .eq("status", "running")
                .lt("started_at", cutoff)
                .limit(500)
                .execute()
            ).data or []
        except Exception as e:
            logger.error("reaper: kon stuck jobs niet ophalen uit %s: %s", table, e)
            summary[table] = {"requeued": 0, "dead_lettered": 0, "stuck": 0, "error": str(e)}
            continue

        for row in stuck:
            retries = int(row.get("retry_count") or 0)
            # NB: heatr_scraping_jobs/heatr_enrichment_jobs hebben GEEN updated_at
            # (geverifieerd tegen supabase_schema_prefixed.sql) — alleen status /
            # retry_count / error_message aanraken.
            try:
                if retries < max_retries:
                    supabase_client.table(table).update({
                        "status": "pending",
                        "retry_count": retries + 1,
                        "error_message": f"reaper: requeued na >{stuck_minutes}m op running",
                    }).eq("id", row["id"]).eq("status", "running").execute()
                    requeued += 1
                else:
                    supabase_client.table(table).update({
                        "status": "failed",
                        "error_message": f"reaper: dead-letter na {retries} retries (stuck >{stuck_minutes}m)",
                    }).eq("id", row["id"]).eq("status", "running").execute()
                    dead += 1
            except Exception as e:
                logger.error("reaper: kon job %s in %s niet herstellen: %s", row.get("id"), table, e)

        if stuck:
            logger.warning(
                "reaper: %s had %d stuck job(s) → %d requeued, %d dead-lettered "
                "(worker mogelijk gecrasht/offline)",
                table, len(stuck), requeued, dead,
            )
        summary[table] = {"requeued": requeued, "dead_lettered": dead, "stuck": len(stuck)}

    return summary
