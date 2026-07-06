"""
tests/test_outbound_dispatcher.py — de verplichte outbound-doorgang (I3/I6/I7).

Valideert: compliance-vangnet, idempotency-skip, append-only ledger op elke
uitkomst (completed/failed/blocked/skipped), record-only-modus voor alerts,
fail-open-gedrag als de ledger-tabel ontbreekt.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.outbound_dispatcher import DispatchBlocked, dispatch_outbound, ids_hash


class FakeLedgerDB:
    """Mock-Supabase voor outbound_log: verzamelt inserts, configureerbare
    bestaande completed-records, optionele table-missing-simulatie."""

    def __init__(self, existing_completed: dict | None = None, table_missing: bool = False):
        self.inserted: list[dict] = []
        self.existing = existing_completed
        self.table_missing = table_missing

    def table(self, name):
        assert name == "outbound_log", f"onverwachte tabel: {name}"
        db = self
        chain = MagicMock()

        def _insert(row):
            if db.table_missing:
                raise RuntimeError("PGRST205: table not found")
            db.inserted.append(row)
            res = MagicMock()
            res.data = [{"id": f"rec-{len(db.inserted)}"}]
            inner = MagicMock()
            inner.execute.return_value = res
            return inner

        def _execute():
            if db.table_missing:
                raise RuntimeError("PGRST205: table not found")
            res = MagicMock()
            res.data = [db.existing] if db.existing else []
            return res

        chain.insert.side_effect = _insert
        chain.select.return_value = chain
        chain.eq.return_value = chain
        chain.limit.return_value = chain
        chain.execute.side_effect = _execute
        return chain


COMPLIANT = {"id": "l1", "gdpr_safe": True, "status": "enriched"}
BLOCKED = {"id": "l2", "gdpr_safe": True, "status": "unsubscribed"}


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _dispatch(db, *, lead=COMPLIANT, send=None, **kw):
    calls = {"n": 0}

    async def default_send():
        calls["n"] += 1
        return {"pushed": 1}

    result = _run(dispatch_outbound(
        kind=kw.pop("kind", "warmr_push"),
        idempotency_key=kw.pop("idempotency_key", "test-key-1"),
        actor="test",
        send=send or default_send,
        supabase_client=db,
        workspace_id="aerys",
        lead=lead,
        **kw,
    ))
    return result, calls


def test_success_writes_completed_record():
    db = FakeLedgerDB()
    result, calls = _dispatch(db)
    assert result.executed is True
    assert calls["n"] == 1
    assert len(db.inserted) == 1
    assert db.inserted[0]["status"] == "completed"
    assert db.inserted[0]["idempotency_key"] == "test-key-1"
    assert db.inserted[0]["actor"] == "test"


def test_compliance_block_prevents_send_and_records():
    db = FakeLedgerDB()
    with pytest.raises(DispatchBlocked):
        _dispatch(db, lead=BLOCKED)
    assert len(db.inserted) == 1
    assert db.inserted[0]["status"] == "blocked_compliance"
    assert "unsubscribed" in db.inserted[0]["error"]


def test_bulk_compliance_blocks_on_any_noncompliant():
    db = FakeLedgerDB()
    with pytest.raises(DispatchBlocked):
        _dispatch(db, lead=None, leads=[COMPLIANT, BLOCKED], kind="warmr_bulk_push")
    assert db.inserted[0]["status"] == "blocked_compliance"


def test_duplicate_skipped_send_not_called():
    db = FakeLedgerDB(existing_completed={
        "id": "prev-1", "status": "completed", "created_at": "2026-07-01T10:00:00Z", "actor": "eerder",
    })
    result, calls = _dispatch(db)
    assert result.executed is False
    assert result.skipped_duplicate is True
    assert result.previous["id"] == "prev-1"
    assert calls["n"] == 0  # side-effect NIET uitgevoerd
    assert db.inserted[0]["status"] == "skipped_duplicate"


def test_enforce_idempotency_false_executes_despite_previous():
    """Alerts: record-only — nooit onderdrukken."""
    db = FakeLedgerDB(existing_completed={"id": "prev-1", "status": "completed"})
    result, calls = _dispatch(db, kind="operator_email", lead=None, enforce_idempotency=False)
    assert result.executed is True
    assert calls["n"] == 1
    assert db.inserted[0]["status"] == "completed"


def test_send_failure_writes_failed_and_reraises():
    db = FakeLedgerDB()

    async def failing_send():
        raise ConnectionError("warmr down")

    with pytest.raises(ConnectionError):
        _dispatch(db, send=failing_send)
    assert db.inserted[0]["status"] == "failed"
    assert "warmr down" in db.inserted[0]["error"]


def test_table_missing_fails_open_send_still_executes():
    """Migratie 020 niet gedraaid → luide log, maar sends bricken niet."""
    db = FakeLedgerDB(table_missing=True)
    result, calls = _dispatch(db)
    assert result.executed is True
    assert calls["n"] == 1


def test_prospect_kind_without_lead_raises():
    db = FakeLedgerDB()
    with pytest.raises(ValueError):
        _dispatch(db, lead=None)


def test_unknown_kind_raises():
    db = FakeLedgerDB()
    with pytest.raises(ValueError):
        _dispatch(db, kind="carrier_pigeon")


def test_ids_hash_deterministic_and_order_independent():
    assert ids_hash(["b", "a"]) == ids_hash(["a", "b"])
    assert ids_hash(["a", "b"]) != ids_hash(["a", "c"])
