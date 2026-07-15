"""
tests/test_pipeline_ops.py — ops-health stall-detectie (2026-07-15).

De pure evaluate_ops_health beoordeelt een snapshot → status + alerts. Dit is de
laag die stilstand expliciet maakt (elke bug deze sessie was een stille naad).
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.pipeline_ops import evaluate_ops_health

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)


def _snap(**enr):
    base = {"pending": 0, "running": 0, "stuck_running": 0, "last_completed_at": None}
    base.update(enr)
    return {"enrichment": base, "scraping": {"stuck_running": 0}, "orphan_discovered": 0}


class TestStall:
    def test_healthy_recent_completion(self):
        s = _snap(pending=10, last_completed_at=(NOW - timedelta(minutes=2)).isoformat())
        r = evaluate_ops_health(s, NOW)
        assert r["status"] == "healthy"
        assert r["alerts"] == []

    def test_stalled_old_completion(self):
        s = _snap(pending=10, last_completed_at=(NOW - timedelta(minutes=45)).isoformat())
        r = evaluate_ops_health(s, NOW)
        assert r["status"] == "stalled"
        assert any(a["severity"] == "critical" for a in r["alerts"])

    def test_stalled_no_completion_ever(self):
        s = _snap(pending=5, last_completed_at=None)
        r = evaluate_ops_health(s, NOW)
        assert r["status"] == "stalled"

    def test_no_pending_is_healthy_even_if_idle_long(self):
        """Lege queue = niks te doen = gezond, ook al is de laatste completion oud."""
        s = _snap(pending=0, last_completed_at=(NOW - timedelta(hours=5)).isoformat())
        assert evaluate_ops_health(s, NOW)["status"] == "healthy"


class TestDegraded:
    def test_stuck_running_is_degraded(self):
        s = _snap(pending=0, stuck_running=3)
        r = evaluate_ops_health(s, NOW)
        assert r["status"] == "degraded"
        assert any("running" in a["message"] for a in r["alerts"])

    def test_orphan_discovered_warns(self):
        s = _snap(pending=0)
        s["orphan_discovered"] = 40
        r = evaluate_ops_health(s, NOW)
        assert r["status"] == "degraded"
        assert any("discovered" in a["message"] for a in r["alerts"])

    def test_orphan_ignored_when_jobs_pending(self):
        """Als er jobs pending zijn, is 'discovered' geen weesprobleem."""
        s = _snap(pending=20, last_completed_at=(NOW - timedelta(minutes=1)).isoformat())
        s["orphan_discovered"] = 40
        r = evaluate_ops_health(s, NOW)
        assert not any("discovered" in a["message"] for a in r["alerts"])

    def test_scraping_stuck_warns(self):
        s = _snap()
        s["scraping"]["stuck_running"] = 2
        r = evaluate_ops_health(s, NOW)
        assert r["status"] == "degraded"


class TestStallWinsOverDegraded:
    def test_stall_status_when_both(self):
        s = _snap(pending=10, last_completed_at=(NOW - timedelta(minutes=45)).isoformat(), stuck_running=2)
        assert evaluate_ops_health(s, NOW)["status"] == "stalled"
