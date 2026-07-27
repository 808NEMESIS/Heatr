"""tests/test_compliance_flags.py — persistente compliance-vlaggen + drip-pre-gate.

Kern (Sami 2026-07-27): een missende Warmr-afmeldlink wordt een ZICHTBARE vlag, geen
logregel, en een open vlag BLOKKEERT de volgende drip (assert_no_open_flags). Fail-
closed: onleesbare vlag-tabel → ook blokkeren.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.compliance_flags import (
    ComplianceHold, FLAG_MISSING_UNSUBSCRIBE, acknowledge_flags,
    assert_no_open_flags, open_flags, raise_flag,
)
from utils.warmr_unsubscribe import verify_and_flag


# ── Capabele fake supabase-client (insert/select/update + eq/is_/in_/order/limit) ──
class _Res:
    def __init__(self, data): self.data = data


class _Tbl:
    def __init__(self, store, mode):
        self._store, self._mode = store, mode
        self._op = None
        self._ins = self._upd = None
        self._eq = {}
        self._isnull = []
        self._in = None

    def insert(self, row): self._op = "insert"; self._ins = row; return self
    def update(self, row): self._op = "update"; self._upd = row; return self
    def select(self, *a, **k): self._op = "select"; return self
    def eq(self, k, v): self._eq[k] = v; return self
    def is_(self, k, v):
        if v == "null":
            self._isnull.append(k)
        return self
    def in_(self, k, vals): self._in = (k, list(vals)); return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self

    def _match(self, r):
        return (all(r.get(k) == v for k, v in self._eq.items())
                and all(r.get(k) is None for k in self._isnull))

    def execute(self):
        if self._op == "insert":
            if self._mode == "conflict":
                raise RuntimeError("duplicate key value violates unique constraint (23505)")
            if self._mode == "write_error":
                raise RuntimeError("REST 500")
            row = dict(self._ins); row.setdefault("id", f"flag-{len(self._store)+1}")
            self._store.append(row)
            return _Res([row])
        if self._op == "select":
            if self._mode == "read_error":
                raise RuntimeError("REST down")
            return _Res([r for r in self._store if self._match(r)])
        if self._op == "update":
            hit = []
            k, vals = self._in or (None, [])
            for r in self._store:
                if r.get(k) in vals and all(r.get(kk) is None for kk in self._isnull):
                    r.update(self._upd); hit.append(r)
            return _Res(hit)
        return _Res([])


class _SB:
    def __init__(self, store=None, mode="ok"):
        self._store = store if store is not None else []
        self._mode = mode
    def table(self, _name): return _Tbl(self._store, self._mode)


# ── raise_flag ────────────────────────────────────────────────────────────────

def test_raise_flag_inserts_visible_row():
    store: list = []
    sb = _SB(store)
    fid = raise_flag(sb, workspace_id="aerys", flag_type=FLAG_MISSING_UNSUBSCRIBE,
                     lead_id="L1", campaign_id="C1", detail="geen rij")
    assert fid and len(store) == 1
    assert store[0]["flag_type"] == FLAG_MISSING_UNSUBSCRIBE and store[0]["lead_id"] == "L1"


def test_raise_flag_conflict_is_graceful():
    # Vlag stond al open (partial-UNIQUE) → geen crash, geen dubbel.
    fid = raise_flag(_SB(mode="conflict"), workspace_id="aerys",
                     flag_type=FLAG_MISSING_UNSUBSCRIBE, lead_id="L1", campaign_id="C1")
    assert fid is None


def test_raise_flag_write_error_returns_none_not_raises():
    fid = raise_flag(_SB(mode="write_error"), workspace_id="aerys",
                     flag_type=FLAG_MISSING_UNSUBSCRIBE, lead_id="L1")
    assert fid is None


# ── drip-pre-gate ───────────────────────────────────────────────────────────────

def test_assert_passes_when_no_open_flags():
    assert_no_open_flags(_SB([]), "aerys", FLAG_MISSING_UNSUBSCRIBE)  # geen raise


def test_assert_blocks_on_open_flag():
    store = [{"id": "f1", "workspace_id": "aerys", "flag_type": FLAG_MISSING_UNSUBSCRIBE,
              "lead_id": "L1", "acknowledged_at": None}]
    with pytest.raises(ComplianceHold, match="open compliance-vlag"):
        assert_no_open_flags(_SB(store), "aerys", FLAG_MISSING_UNSUBSCRIBE)


def test_assert_ignores_acknowledged_flag():
    store = [{"id": "f1", "workspace_id": "aerys", "flag_type": FLAG_MISSING_UNSUBSCRIBE,
              "lead_id": "L1", "acknowledged_at": "2026-07-27T10:00:00Z"}]
    assert_no_open_flags(_SB(store), "aerys", FLAG_MISSING_UNSUBSCRIBE)  # afgehandeld → geen block


def test_assert_fail_closed_on_read_error():
    with pytest.raises(ComplianceHold, match="niet leesbaar"):
        assert_no_open_flags(_SB(mode="read_error"), "aerys", FLAG_MISSING_UNSUBSCRIBE)


def test_acknowledge_clears_the_block():
    store = [{"id": "f1", "workspace_id": "aerys", "flag_type": FLAG_MISSING_UNSUBSCRIBE,
              "lead_id": "L1", "acknowledged_at": None}]
    sb = _SB(store)
    with pytest.raises(ComplianceHold):
        assert_no_open_flags(sb, "aerys", FLAG_MISSING_UNSUBSCRIBE)
    n = acknowledge_flags(sb, ["f1"], by="sami")
    assert n == 1
    assert_no_open_flags(sb, "aerys", FLAG_MISSING_UNSUBSCRIBE)  # deblokkeerd


# ── verify_and_flag: missende afmeldlink → vlag ─────────────────────────────────

def test_verify_and_flag_raises_flag_when_token_missing():
    raw = _SB([])                    # geen unsubscribe_tokens-rij
    flag_store: list = []
    heatr = _SB(flag_store)
    ok, _ = verify_and_flag(raw, heatr, workspace_id="aerys", lead_id="L1", campaign_id="C1")
    assert ok is False
    assert len(flag_store) == 1 and flag_store[0]["flag_type"] == FLAG_MISSING_UNSUBSCRIBE


def test_verify_and_flag_no_flag_when_token_present():
    raw = _SB([{"id": "t1", "token": "abc123xyz", "used": False,
                "lead_id": "L1", "campaign_id": "C1"}])
    flag_store: list = []
    ok, _ = verify_and_flag(raw, _SB(flag_store), workspace_id="aerys", lead_id="L1", campaign_id="C1")
    assert ok is True and flag_store == []
