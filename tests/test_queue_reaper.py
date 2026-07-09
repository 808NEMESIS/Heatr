"""
tests/test_queue_reaper.py — Recovery Patch 3: stuck-job recovery.

Bewijst dat jobs die te lang op 'running' staan worden hersteld (requeue) of
na het retry-plafond dead-lettered, i.p.v. eeuwig te blijven hangen.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.queue_reaper import requeue_stuck_jobs


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class _Chain:
    def __init__(self, db, table):
        self._db, self._t = db, table
        self._payload = None

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def lt(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def update(self, payload):
        self._payload = payload
        return self

    def execute(self):
        class _R:
            pass
        res = _R()
        if self._payload is not None:
            self._db.updates.append((self._t, self._payload))
            res.data = [{"id": "x"}]
        else:
            res.data = list(self._db.stuck.get(self._t, []))
        return res


class FakeDB:
    def __init__(self, stuck):
        self.stuck = stuck          # {table: [rows]}
        self.updates: list = []      # [(table, payload)]

    def table(self, name):
        return _Chain(self, name)


def test_stuck_job_requeued_and_retry_incremented():
    db = FakeDB({"scraping_jobs": [{"id": "J1", "retry_count": 0, "started_at": "old"}],
                 "enrichment_jobs": []})
    summary = _run(requeue_stuck_jobs(db))
    assert summary["scraping_jobs"]["requeued"] == 1
    assert summary["scraping_jobs"]["dead_lettered"] == 0
    (tbl, payload), = [u for u in db.updates if u[0] == "scraping_jobs"]
    assert payload["status"] == "pending"
    assert payload["retry_count"] == 1


def test_stuck_job_at_max_retries_dead_lettered():
    db = FakeDB({"scraping_jobs": [], "enrichment_jobs": [
        {"id": "J2", "retry_count": 3, "started_at": "old"}]})
    summary = _run(requeue_stuck_jobs(db, max_retries=3))
    assert summary["enrichment_jobs"]["dead_lettered"] == 1
    assert summary["enrichment_jobs"]["requeued"] == 0
    (_, payload), = [u for u in db.updates if u[0] == "enrichment_jobs"]
    assert payload["status"] == "failed"


def test_no_stuck_jobs_is_noop():
    db = FakeDB({"scraping_jobs": [], "enrichment_jobs": []})
    summary = _run(requeue_stuck_jobs(db))
    assert db.updates == []
    assert all(v["stuck"] == 0 for v in summary.values())


def test_mixed_requeue_and_deadletter_across_tables():
    db = FakeDB({
        "scraping_jobs": [{"id": "A", "retry_count": 1, "started_at": "old"}],
        "enrichment_jobs": [{"id": "B", "retry_count": 5, "started_at": "old"}],
    })
    summary = _run(requeue_stuck_jobs(db, max_retries=3))
    assert summary["scraping_jobs"]["requeued"] == 1
    assert summary["enrichment_jobs"]["dead_lettered"] == 1
    assert summary["scraping_jobs"]["stuck"] == 1
    assert summary["enrichment_jobs"]["stuck"] == 1


def test_per_table_error_does_not_stop_others():
    class Boom(FakeDB):
        def table(self, name):
            if name == "scraping_jobs":
                raise RuntimeError("PGRST205: table missing")
            return super().table(name)
    db = Boom({"scraping_jobs": [], "enrichment_jobs": [
        {"id": "C", "retry_count": 0, "started_at": "old"}]})
    summary = _run(requeue_stuck_jobs(db))
    assert "error" in summary["scraping_jobs"]
    assert summary["enrichment_jobs"]["requeued"] == 1
