"""
tests/test_suppression.py — platformbrede suppressielijst (fase 2 PR 7).

Valideert: normalisatie, idempotente registratie (unique-conflict = ok),
fail-soft write met eerlijke error, batch-check die alleen actieve
(niet-gerevokete) suppressies matcht, en raise-bij-leesfout (de dispatcher
behandelt dat fail-closed).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.suppression import add_suppression, check_suppressed, normalize_email


class _Query:
    def __init__(self, db):
        self.db = db
        self.op = "select"
        self.payload = None
        self.in_values: list | None = None
        self.is_null_col: str | None = None

    def insert(self, row):
        self.op, self.payload = "insert", dict(row)
        return self

    def select(self, *_):
        return self

    def in_(self, _col, vals):
        self.in_values = list(vals)
        return self

    def is_(self, col, _null):
        self.is_null_col = col
        return self

    def execute(self):
        if self.db.raises:
            raise RuntimeError(self.db.raises)
        res = type("Res", (), {})()
        if self.op == "insert":
            row = self.payload
            active = [r for r in self.db.rows
                      if r["normalized_email"] == row["normalized_email"]
                      and r.get("revoked_at") is None]
            if active:
                raise RuntimeError(
                    'duplicate key value violates unique constraint '
                    '"uq_suppressions_active_email" (23505)')
            self.db.rows.append({**row, "revoked_at": None})
            res.data = [row]
            return res
        rows = [r for r in self.db.rows if r["normalized_email"] in (self.in_values or [])]
        if self.is_null_col:
            rows = [r for r in rows if r.get(self.is_null_col) is None]
        res.data = [dict(r) for r in rows]
        return res


class FakeSuppressionDB:
    def __init__(self, raises: str | None = None):
        self.rows: list[dict] = []
        self.raises = raises

    def table(self, name):
        assert name == "suppressions", f"onverwachte tabel: {name}"
        return _Query(self)


def test_normalize_email():
    assert normalize_email("  Piet@Praktijk.NL ") == "piet@praktijk.nl"
    assert normalize_email(None) is None
    assert normalize_email("geen-email") is None
    # Bewust GEEN plus-normalisatie: exact adres, geen over-suppressie
    assert normalize_email("a+tag@b.nl") == "a+tag@b.nl"


def test_add_suppression_writes_row():
    db = FakeSuppressionDB()
    out = add_suppression(db, email="Piet@Praktijk.nl", suppression_type="unsubscribe",
                          source="test", source_workspace_id="aerys", lead_id="l1")
    assert out == {"ok": True, "already": False}
    assert db.rows[0]["normalized_email"] == "piet@praktijk.nl"
    assert db.rows[0]["suppression_type"] == "unsubscribe"


def test_add_suppression_idempotent_on_conflict():
    """Tweede registratie van hetzelfde adres = no-op, geen fout."""
    db = FakeSuppressionDB()
    add_suppression(db, email="a@b.nl", suppression_type="unsubscribe", source="t")
    out = add_suppression(db, email="A@B.NL", suppression_type="hard_bounce", source="t2")
    assert out == {"ok": True, "already": True}
    assert len(db.rows) == 1


def test_add_suppression_fail_soft_with_honest_error():
    db = FakeSuppressionDB(raises="PGRST205: table not found")
    out = add_suppression(db, email="a@b.nl", suppression_type="unsubscribe", source="t")
    assert out["ok"] is False
    assert "PGRST205" in out["error"]


def test_add_suppression_rejects_unknown_type_and_bad_email():
    db = FakeSuppressionDB()
    assert add_suppression(db, email="a@b.nl", suppression_type="wraak", source="t")["ok"] is False
    assert add_suppression(db, email=None, suppression_type="unsubscribe", source="t")["ok"] is False
    assert db.rows == []


def test_check_suppressed_batch_and_normalization():
    db = FakeSuppressionDB()
    add_suppression(db, email="a@b.nl", suppression_type="unsubscribe", source="t")
    hits = check_suppressed(db, ["A@B.NL", "onbekend@x.nl", None, "geen-email"])
    assert hits == {"a@b.nl": "unsubscribe"}


def test_check_suppressed_ignores_revoked():
    db = FakeSuppressionDB()
    add_suppression(db, email="a@b.nl", suppression_type="unsubscribe", source="t")
    db.rows[0]["revoked_at"] = "2026-07-01T00:00:00Z"  # operator-revoke
    assert check_suppressed(db, ["a@b.nl"]) == {}


def test_check_suppressed_raises_on_db_error():
    """Bewust GEEN except in de check — de dispatcher moet fail-closed kunnen
    reageren; een geslikte leesfout zou de lijst stil uitschakelen."""
    db = FakeSuppressionDB(raises="db down")
    with pytest.raises(RuntimeError):
        check_suppressed(db, ["a@b.nl"])


def test_check_suppressed_empty_input_no_query():
    db = FakeSuppressionDB(raises="zou niet bevraagd mogen worden")
    assert check_suppressed(db, [None, "geen-email"]) == {}
