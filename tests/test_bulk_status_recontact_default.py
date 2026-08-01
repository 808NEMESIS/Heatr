"""tests/test_bulk_status_recontact_default.py — recontact_later krijgt altijd een datum.

UI-audit Fase 3: leads die via bulk/drag naar 'recontact_later' gaan kregen geen
recontact_after → de recontact-logica (filtert op recontact_after <= now) zag ze nooit.
De bulk-status-endpoint zet nu een default van +90 dagen als er geen datum meekomt,
en respecteert een expliciete datum.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class _Chain:
    def __init__(self, sink, table):
        self.sink = sink
        self.table_name = table
        self._op = None
        self._payload = None

    def update(self, payload):
        self._op, self._payload = "update", payload
        return self

    def insert(self, payload):
        self._op, self._payload = "insert", payload
        return self

    def in_(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def execute(self):
        self.sink.append((self.table_name, self._op, self._payload))
        # de bulk-update moet rijen teruggeven zodat de timeline-loop draait
        if self.table_name == "leads" and self._op == "update":
            return type("R", (), {"data": [{"id": "L1"}]})()
        return type("R", (), {"data": [{}]})()


class _FakeDB:
    def __init__(self):
        self.calls: list = []

    def table(self, name):
        return _Chain(self.calls, name)


def _post(db, body):
    from fastapi.testclient import TestClient
    import api.main as m

    m.app.dependency_overrides[m.get_supabase] = lambda: db
    m.app.dependency_overrides[m.get_workspace] = lambda: "aerys"
    resp = TestClient(m.app).post("/leads/bulk-status", json=body)
    m.app.dependency_overrides.clear()
    return resp


def _patch(db):
    return next(p for (t, op, p) in db.calls if t == "leads" and op == "update")


def test_recontact_later_defaults_to_90d():
    db = _FakeDB()
    resp = _post(db, {"lead_ids": ["L1"], "status": "recontact_later"})
    assert resp.status_code == 200, resp.text
    ra = _patch(db).get("recontact_after")
    assert ra, "recontact_after is niet gezet"
    days = (datetime.fromisoformat(ra) - datetime.now(timezone.utc)).days
    assert 88 <= days <= 91, f"verwacht ~90 dagen, kreeg {days}"


def test_recontact_later_respects_explicit_date():
    db = _FakeDB()
    resp = _post(db, {"lead_ids": ["L1"], "status": "recontact_later", "recontact_after": "2030-01-01"})
    assert resp.status_code == 200, resp.text
    assert _patch(db)["recontact_after"] == "2030-01-01"
