"""
utils/pipeline_ops.py — Operationele pijplijn-gezondheid (de "draait de machine?"-laag).

Complementair aan pipeline_metrics.collect_pipeline_health (dat de CONVERSIE-funnel
over de laatste N dagen berekent). Deze module geeft de HUIDIGE operationele stand:
queue-diepte, vastgelopen jobs, worker-heartbeat en — het punt — een expliciete
STALL-detectie, zodat stilstand zichtbaar wordt i.p.v. per toeval ontdekt.

Splitsing: `gather_ops_snapshot` doet de DB-reads (fail-soft); `evaluate_ops_health`
is PUUR (snapshot + now → status + alerts) en dus los testbaar. `ops_health`
combineert beide.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Een enrichment-job die langer dan dit op 'running' staat is vermoedelijk
# vastgelopen (worker gecrasht/gekilld midden in een lead). De reaper-cron
# requeuet ze; wij flaggen ze.
_STUCK_RUNNING_MINUTES = int(os.getenv("OPS_STUCK_RUNNING_MINUTES", "30"))
# Pending werk maar geen enkele completion binnen dit venster = stall.
_STALL_MINUTES = int(os.getenv("OPS_STALL_MINUTES", "20"))
# Zoveel discovered-leads zonder enrichment-job = de promotie/enqueue-brug hapert.
_ORPHAN_DISCOVERED_ALERT = int(os.getenv("OPS_ORPHAN_DISCOVERED_ALERT", "25"))


def _min_score() -> int:
    return int(os.getenv("MIN_SCORE_FOR_WARMR", "55"))


def _min_icp() -> float:
    return float(os.getenv("MIN_ICP_MATCH_FOR_WARMR", "0.50"))


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        s = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def evaluate_ops_health(snapshot: dict, now: datetime) -> dict:
    """Puur: beoordeel een ops-snapshot → {status, alerts}.

    status: 'healthy' | 'degraded' | 'stalled'. alerts: lijst {severity, message}.
    Geen DB, geen tijd-afhankelijkheid buiten `now` → volledig deterministisch testbaar.
    """
    alerts: list[dict] = []

    enr = snapshot.get("enrichment", {})
    pending = enr.get("pending", 0)
    running = enr.get("running", 0)
    last_completed = _parse_ts(enr.get("last_completed_at"))
    stuck = enr.get("stuck_running", 0)

    # --- STALL: pending werk maar geen recente completion ---
    stalled = False
    if pending > 0:
        if last_completed is None or (now - last_completed) > timedelta(minutes=_STALL_MINUTES):
            stalled = True
            mins = "onbekend" if last_completed is None else f"{int((now - last_completed).total_seconds() // 60)}"
            alerts.append({
                "severity": "critical",
                "message": (f"Enrichment gestald: {pending} pending, laatste completion "
                            f"{mins} min geleden (>{_STALL_MINUTES}m). Worker down of vastgelopen?"),
            })

    # --- Vastgelopen 'running'-jobs ---
    if stuck > 0:
        alerts.append({
            "severity": "warning",
            "message": f"{stuck} enrichment-job(s) >{_STUCK_RUNNING_MINUTES}m op 'running' — reaper zou moeten requeuen.",
        })

    # --- Promotie/enqueue-brug hapert: veel discovered zonder job ---
    orphan = snapshot.get("orphan_discovered", 0)
    if orphan >= _ORPHAN_DISCOVERED_ALERT and pending == 0:
        alerts.append({
            "severity": "warning",
            "message": (f"{orphan} leads op 'discovered' maar 0 enrichment-jobs — "
                        f"promotie/enqueue draait niet."),
        })

    # --- Scraping-queue vastgelopen ---
    scr = snapshot.get("scraping", {})
    if scr.get("stuck_running", 0) > 0:
        alerts.append({
            "severity": "warning",
            "message": f"{scr['stuck_running']} scraping-job(s) >{_STUCK_RUNNING_MINUTES}m op 'running'.",
        })

    if stalled:
        status = "stalled"
    elif alerts:
        status = "degraded"
    else:
        status = "healthy"
    return {"status": status, "alerts": alerts}


async def gather_ops_snapshot(workspace_id: str, supabase_client: Any, now: datetime) -> dict:
    """Lees de huidige operationele stand uit de DB (fail-soft per query)."""
    stuck_cutoff = (now - timedelta(minutes=_STUCK_RUNNING_MINUTES)).isoformat()

    def _count(table: str, **filters) -> int:
        try:
            q = supabase_client.table(table).select("id", count="exact").eq("workspace_id", workspace_id)
            for k, v in filters.items():
                q = q.eq(k, v)
            return q.execute().count or 0
        except Exception as e:
            logger.debug("ops snapshot count %s%s faalde: %s", table, filters, e)
            return 0

    def _stuck(table: str) -> int:
        try:
            return (supabase_client.table(table).select("id", count="exact")
                    .eq("workspace_id", workspace_id).eq("status", "running")
                    .lt("started_at", stuck_cutoff).execute().count or 0)
        except Exception:
            return 0

    def _last_completed(table: str) -> str | None:
        try:
            r = (supabase_client.table(table).select("completed_at")
                 .eq("workspace_id", workspace_id).eq("status", "completed")
                 .order("completed_at", desc=True).limit(1).execute().data or [])
            return r[0]["completed_at"] if r else None
        except Exception:
            return None

    # Lead-stadia (huidige snapshot, niet tijd-gevensterd)
    stages = {
        "discovered": _count("leads", status="discovered"),
        "enriched": _count("leads", status="enriched"),
    }
    # Verzendbaar-benadering: valid email + boven de gate (kerncriteria; volledige
    # readiness zit in scripts/batch_readiness_report.py).
    launchable = 0
    try:
        rows = (supabase_client.table("leads")
                .select("score,icp_match,email_status")
                .eq("workspace_id", workspace_id).eq("email_status", "valid")
                .limit(5000).execute().data or [])
        ms, mi = _min_score(), _min_icp()
        launchable = sum(1 for r in rows
                         if (r.get("score") or 0) >= ms and (r.get("icp_match") or 0) >= mi)
    except Exception:
        pass

    # Orphan: discovered-leads zonder enige enrichment-job
    orphan = stages["discovered"]  # benadering: alle discovered horen een job te hebben

    return {
        "enrichment": {
            "pending": _count("enrichment_jobs", status="pending"),
            "running": _count("enrichment_jobs", status="running"),
            "stuck_running": _stuck("enrichment_jobs"),
            "last_completed_at": _last_completed("enrichment_jobs"),
        },
        "scraping": {
            "pending": _count("scraping_jobs", status="pending"),
            "running": _count("scraping_jobs", status="running"),
            "stuck_running": _stuck("scraping_jobs"),
        },
        "stages": stages,
        "launchable": launchable,
        "orphan_discovered": orphan,
    }


async def ops_health(workspace_id: str, supabase_client: Any, now: datetime | None = None) -> dict:
    """Volledige ops-health: snapshot + oordeel + alerts."""
    now = now or datetime.now(timezone.utc)
    snapshot = await gather_ops_snapshot(workspace_id, supabase_client, now)
    verdict = evaluate_ops_health(snapshot, now)
    return {"checked_at": now.isoformat(), **verdict, "snapshot": snapshot}
