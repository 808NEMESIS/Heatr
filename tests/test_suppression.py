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


# ── AVG-05: org-brede suppressie op domein + KvK (2026-07-24) ────────────────
from utils.suppression import (  # noqa: E402
    check_suppressed_lead,
    normalize_domain,
    suppression_match_keys,
)


def test_normalize_domain():
    assert normalize_domain("info@Kliniek.NL") == "kliniek.nl"
    assert normalize_domain("https://www.x.nl/pad") == "www.x.nl"
    assert normalize_domain("x.nl") == "x.nl"
    assert normalize_domain(None) is None and normalize_domain("") is None


def test_suppression_match_keys():
    k = suppression_match_keys({"email": "info@kliniek.nl", "kvk_number": "12345678"})
    assert k == {"email": "info@kliniek.nl", "domain": "kliniek.nl", "kvk": "12345678"}
    # domein valt terug op het domain-veld als er geen e-mail is
    assert suppression_match_keys({"domain": "praktijk.nl"}) == {"domain": "praktijk.nl"}


def test_add_suppression_stores_domain_and_kvk():
    db = FakeSuppressionDB()
    add_suppression(db, email="a@kliniek.nl", suppression_type="unsubscribe",
                    source="webhook", kvk_number="12345678")
    assert db.rows[0]["normalized_domain"] == "kliniek.nl"
    assert db.rows[0]["kvk_number"] == "12345678"


class _OrFake:
    """Minimale fake voor check_suppressed_lead: legt de .or_-filter vast."""
    def __init__(self, rows=None, raises=None):
        self.rows = rows or []
        self.raises = raises
        self.or_arg = None

    def table(self, _):
        return self

    def select(self, *_):
        return self

    def or_(self, expr):
        self.or_arg = expr
        return self

    def is_(self, *_):
        return self

    def execute(self):
        if self.raises:
            raise RuntimeError(self.raises)
        return type("R", (), {"data": self.rows})()


def test_check_suppressed_lead_matches_on_domain():
    fake = _OrFake(rows=[{"suppression_type": "forgotten"}])
    out = check_suppressed_lead(fake, {"email": "nieuw@kliniek.nl", "kvk_number": "999"})
    assert out == "forgotten"
    assert "normalized_email.eq.nieuw@kliniek.nl" in fake.or_arg
    assert "normalized_domain.eq.kliniek.nl" in fake.or_arg
    assert "kvk_number.eq.999" in fake.or_arg


def test_check_suppressed_lead_none_when_no_match():
    assert check_suppressed_lead(_OrFake(rows=[]), {"email": "x@y.nl"}) is None


def test_check_suppressed_lead_fail_closed_raises():
    import pytest as _pt
    with _pt.raises(RuntimeError):
        check_suppressed_lead(_OrFake(raises="db down"), {"email": "x@y.nl"})


def test_add_suppression_falls_back_to_email_only_before_migration_042():
    # pre-042: insert mét org-kolommen faalt (kolom bestaat niet) → retry e-mail-only
    # zodat de bestaande unsubscribe-suppressie BLIJFT werken.
    class _PreMigrationDB:
        def __init__(self): self.inserts = []
        def table(self, _): return self
        def insert(self, row):
            self._row = dict(row); return self
        def execute(self):
            self.inserts.append(self._row)
            if "normalized_domain" in self._row:
                raise RuntimeError("column \"normalized_domain\" does not exist")
            class _R: data = [{}]
            return _R()
    db = _PreMigrationDB()
    out = add_suppression(db, email="a@kliniek.nl", suppression_type="unsubscribe", source="webhook")
    assert out["ok"] is True and out.get("org_columns_missing") is True
    # tweede insert had de org-kolommen NIET meer
    assert "normalized_domain" not in db.inserts[-1]
    assert db.inserts[-1]["normalized_email"] == "a@kliniek.nl"
