"""tests/test_discovery_resume.py — POST /discovery-schedules/{id}/resume (Fase 4, Item 1).

Er was wel een /pause (active=false) maar geen resume → een actief/gepauzeerd-toggle in de UI
kon niet werken. Deze test pint de nieuwe resume-endpoint (spiegel van pause, active=true).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class _Chain:
    def __init__(self, sink):
        self.sink = sink
        self._payload = None

    def update(self, payload):
        self._payload = payload
        return self

    def eq(self, *a, **k):
        return self

    def execute(self):
        self.sink.append(("update", self._payload))
        return type("R", (), {"data": [{}]})()


class _FakeDB:
    def __init__(self):
        self.calls: list = []

    def table(self, name):
        assert name == "lead_discovery_schedules"
        return _Chain(self.calls)


def _post(db, path):
    from fastapi.testclient import TestClient
    import api.main as m
    m.app.dependency_overrides[m.get_supabase] = lambda: db
    m.app.dependency_overrides[m.get_workspace] = lambda: "aerys"
    resp = TestClient(m.app).post(path)
    m.app.dependency_overrides.clear()
    return resp


def test_resume_sets_active_true():
    db = _FakeDB()
    resp = _post(db, "/discovery-schedules/S1/resume")
    assert resp.status_code == 200 and resp.json() == {"ok": True}
    assert ("update", {"active": True}) in db.calls
