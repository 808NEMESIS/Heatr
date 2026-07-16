"""
tests/test_calls_endpoints.py — check-up endpoints (fase 3).

Test routing/auth/gate-logica met een gemockte DB (heatr_call_records bestaat pas
na migratie 032). Patroon: TestClient + dependency_overrides, chainbare fake-DB.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class _Chain:
    def __init__(self, table, db):
        self._table, self._db = table, db
        self._op = None
        self._payload = None
        self._eq: dict = {}

    def insert(self, payload):
        self._op, self._payload = "insert", payload
        return self

    def update(self, payload):
        self._op, self._payload = "update", payload
        self._db.updates.append((self._table, payload))
        return self

    def select(self, *a, **k):
        return self

    def eq(self, col, val):
        self._eq[col] = val
        return self

    def is_(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def maybe_single(self):
        self._single = True
        return self

    def execute(self):
        class _R:
            pass
        r = _R()
        if self._op == "insert":
            r.data = [{**self._payload, "id": "call-1"}]
            return r
        if self._op == "update":
            r.data = [{**self._db.call_row, **self._payload}]
            return r
        # select
        if self._table == "call_records":
            r.data = self._db.call_row
        elif self._table == "leads":
            r.data = self._db.lead_row
        elif self._table == "website_intelligence":
            r.data = self._db.wi_row
        else:
            r.data = None
        if not getattr(self, "_single", False):
            r.data = [r.data] if r.data else []
        return r


class _FakeDB:
    def __init__(self, *, call_row=None, lead_row=None, wi_row=None):
        self.call_row = call_row or {"id": "call-1", "workspace_id": "aerys",
                                     "lead_id": "L1", "transcript": "t"}
        self.lead_row = lead_row
        self.wi_row = wi_row
        self.updates: list = []

    def table(self, name):
        return _Chain(name, self)


def _client(db):
    from fastapi.testclient import TestClient
    import api.main as m
    m.app.dependency_overrides[m.get_supabase] = lambda: db
    m.app.dependency_overrides[m.get_workspace] = lambda: "aerys"
    return TestClient(m.app), m


def _clear(m):
    m.app.dependency_overrides.clear()


def test_create_call():
    db = _FakeDB()
    client, m = _client(db)
    resp = client.post("/calls", json={"transcript": "gesprek", "call_date": "2026-07-16T10:00:00Z"})
    _clear(m)
    assert resp.status_code == 200
    assert resp.json()["match_status"] == "unmatched"      # geen lead_id → fail-closed


def test_create_call_with_lead_matches():
    db = _FakeDB()
    client, m = _client(db)
    resp = client.post("/calls", json={"transcript": "g", "call_date": "2026-07-16T10:00:00Z", "lead_id": "L1"})
    _clear(m)
    assert resp.json()["match_status"] == "manually_matched"


def test_unmatched_route_not_shadowed_by_id():
    """GET /calls/unmatched mag niet als /calls/{id} matchen."""
    db = _FakeDB(call_row={"id": "call-1", "workspace_id": "aerys", "match_status": "unmatched"})
    client, m = _client(db)
    resp = client.get("/calls/unmatched")
    _clear(m)
    assert resp.status_code == 200
    assert "calls" in resp.json()


def test_outcome_invalid_rejected():
    db = _FakeDB()
    client, m = _client(db)
    resp = client.patch("/calls/call-1/outcome", json={"outcome": "misschien"})
    _clear(m)
    assert resp.status_code == 422


def test_outcome_valid_won():
    db = _FakeDB()
    client, m = _client(db)
    resp = client.patch("/calls/call-1/outcome", json={"outcome": "won"})
    _clear(m)
    assert resp.status_code == 200
    # lead moet crm_stage=gewonnen krijgen
    lead_updates = [p for t, p in db.updates if t == "leads"]
    assert any(u.get("crm_stage") == "gewonnen" for u in lead_updates)


def test_generate_report_skipped_without_checkup_data():
    # lead zonder checkup_data → report_status='skipped' (regel 4)
    db = _FakeDB(lead_row={"id": "L1", "workspace_id": "aerys", "checkup_data": None})
    client, m = _client(db)
    resp = client.post("/calls/call-1/generate-report")
    _clear(m)
    assert resp.status_code == 200
    assert resp.json()["report_status"] == "skipped"


def test_generate_report_needs_matched_lead():
    db = _FakeDB(call_row={"id": "call-1", "workspace_id": "aerys", "lead_id": None, "transcript": "t"})
    client, m = _client(db)
    resp = client.post("/calls/call-1/generate-report")
    _clear(m)
    assert resp.status_code == 400


def test_report_discard_resets_to_pending():
    db = _FakeDB()
    client, m = _client(db)
    resp = client.patch("/calls/call-1/report", json={"action": "discard"})
    _clear(m)
    assert resp.status_code == 200
    assert resp.json()["report_status"] == "pending"
