"""tests/test_compliance_flags_endpoints.py — GET/POST compliance-vlaggen (Fase 1, Item 1).

De fail-closed drip-gate (assert_no_open_flags) blokkeert de hele batch bij één open vlag,
nu alleen via SQL te deblokkeren. Twee endpoints maken 'm zichtbaar/afhandelbaar:
  - GET  /compliance/flags            → open (niet-acknowledged) vlaggen
  - POST /compliance/flags/acknowledge→ handel af (workspace-veilig: alleen eigen open vlaggen)
Patroon: TestClient + dependency_overrides (zie tests/test_reply_inbox_endpoint.py).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class _Chain:
    def __init__(self, db):
        self.db = db
        self._op = "select"
        self._in_ids = None

    def select(self, *a, **k):
        self._op = "select"; return self

    def update(self, payload):
        self._op = "update"; return self

    def insert(self, payload):
        self._op = "insert"; return self

    def eq(self, *a, **k):
        return self

    def is_(self, *a, **k):
        return self

    def in_(self, col, vals):
        self._in_ids = list(vals); return self

    def order(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def execute(self):
        class _R:
            pass
        r = _R()
        if self._op == "select":
            r.data = list(self.db.open_rows)
        elif self._op == "update":
            acked = [row for row in self.db.open_rows if row["id"] in (self._in_ids or [])]
            self.db.acked.extend(row["id"] for row in acked)
            r.data = acked
        else:
            r.data = []
        return r


class _FakeDB:
    def __init__(self, open_rows):
        self.open_rows = open_rows
        self.acked: list[str] = []

    def table(self, name):
        assert name == "compliance_flags"
        return _Chain(self)


_FLAGS = [
    {"id": "F1", "flag_type": "missing_unsubscribe", "lead_id": "L1", "campaign_id": "C1",
     "detail": "geen afmeldlink", "created_at": "2026-07-29T10:00:00+00:00"},
    {"id": "F2", "flag_type": "missing_unsubscribe", "lead_id": "L2", "campaign_id": "C1",
     "detail": "geen afmeldlink", "created_at": "2026-07-29T10:01:00+00:00"},
]


def _client(db):
    from fastapi.testclient import TestClient
    import api.main as m
    m.app.dependency_overrides[m.get_supabase] = lambda: db
    m.app.dependency_overrides[m.get_workspace] = lambda: "aerys"
    return TestClient(m.app), m


def _get(db, path):
    client, m = _client(db)
    resp = client.get(path)
    m.app.dependency_overrides.clear()
    return resp


def _post(db, path, payload):
    client, m = _client(db)
    resp = client.post(path, json=payload)
    m.app.dependency_overrides.clear()
    return resp


def test_list_open_flags_shape():
    resp = _get(_FakeDB(_FLAGS), "/compliance/flags")
    assert resp.status_code == 200
    flags = resp.json()["flags"]
    assert len(flags) == 2 and flags[0]["flag_type"] == "missing_unsubscribe"


def test_list_empty():
    assert _get(_FakeDB([]), "/compliance/flags").json() == {"flags": []}


def test_acknowledge_own_flag():
    db = _FakeDB(_FLAGS)
    resp = _post(db, "/compliance/flags/acknowledge", {"flag_ids": ["F1"]})
    assert resp.status_code == 200 and resp.json()["acknowledged"] == 1
    assert db.acked == ["F1"]


def test_acknowledge_foreign_id_is_noop():
    # id dat niet in de eigen open-vlaggen zit → workspace-veilig genegeerd
    db = _FakeDB(_FLAGS)
    resp = _post(db, "/compliance/flags/acknowledge", {"flag_ids": ["FOREIGN"]})
    assert resp.json()["acknowledged"] == 0 and db.acked == []


def test_acknowledge_empty_list():
    assert _post(_FakeDB(_FLAGS), "/compliance/flags/acknowledge", {"flag_ids": []}).json()["acknowledged"] == 0
