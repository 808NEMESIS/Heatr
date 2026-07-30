"""tests/test_audit_read_endpoint.py — GET /leads/{lead_id}/audit (Fase 2, Item 4).

De audit-scorer persisteert append-only naar heatr_audit_reports (962 rijen live), maar er
was geen GET-leespad — een UI kon alleen POST'en (re-run + versie-bloat, en de POST laat
categories/score_total weg). Dit read-endpoint geeft het laatste rapport (hoogste versie),
workspace-scoped, met de volledige rij. Patroon: TestClient + dependency_overrides.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class _Chain:
    def __init__(self, rows):
        self._rows, self._tier = rows, None

    def select(self, *a, **k):
        return self

    def eq(self, col, val):
        if col == "tier":
            self._tier = val
        return self

    def order(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def execute(self):
        rows = [r for r in self._rows if self._tier is None or r.get("tier") == self._tier]
        rows = sorted(rows, key=lambda r: r.get("version", 0), reverse=True)[:1]
        return type("R", (), {"data": rows})()


class _FakeDB:
    def __init__(self, rows):
        self.rows = rows

    def table(self, name):
        assert name == "audit_reports"
        return _Chain(self.rows)


_REPORTS = [
    {"id": "A1", "lead_id": "L1", "version": 1, "tier": 1, "score_normalized": 40,
     "categories": {"technical": {"behaald": 5, "max": 10, "label": "Werkt uw site"}}, "findings": []},
    {"id": "A3", "lead_id": "L1", "version": 3, "tier": 1, "score_normalized": 55,
     "categories": {"technical": {"behaald": 7, "max": 10, "label": "Werkt uw site"}}, "findings": []},
    {"id": "A2", "lead_id": "L1", "version": 2, "tier": 2, "score_normalized": 62,
     "categories": {}, "findings": [], "benchmark": {"city": "Utrecht", "n": 8}},
]


def _get(db, path):
    from fastapi.testclient import TestClient
    import api.main as m
    m.app.dependency_overrides[m.get_supabase] = lambda: db
    m.app.dependency_overrides[m.get_workspace] = lambda: "aerys"
    resp = TestClient(m.app).get(path)
    m.app.dependency_overrides.clear()
    return resp


def test_returns_latest_version():
    resp = _get(_FakeDB(_REPORTS), "/leads/L1/audit")
    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == 3 and body["score_normalized"] == 55
    assert "categories" in body                       # de POST laat dit weg; de GET niet


def test_tier_filter_returns_latest_of_tier():
    body = _get(_FakeDB(_REPORTS), "/leads/L1/audit?tier=2").json()
    assert body["version"] == 2 and body["tier"] == 2 and "benchmark" in body


def test_no_report_returns_empty():
    assert _get(_FakeDB([]), "/leads/L1/audit").json() == {}
