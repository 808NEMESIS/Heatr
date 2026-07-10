"""
tests/test_outbound_dispatcher.py — de verplichte outbound-doorgang (I3/I6/I7,
aangescherpt in Werkpakket A).

Valideert het reserveringsmodel: atomische in_flight-INSERT als
eigenaarstoewijzing (gedekt door de partial-UNIQUE uit migratie 022),
in-place finalisatie naar completed/failed_retryable/failed_terminal,
stale-takeover, concurrency (twee dispatches, één key → één send),
fail-CLOSED bij ledger-uitval voor prospect-kinds (Besluit 3, gewijzigd
t.o.v. het oude fail-open), fail-soft voor operator_email, en het
compliance-vangnet.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.outbound_dispatcher import (
    DispatchBlocked,
    DispatchLedgerUnavailable,
    dispatch_outbound,
    ids_hash,
)

_ACTIVE = ("in_flight", "completed")


class _Query:
    """Minimale supabase-py querychain over een in-memory rijenlijst."""

    def __init__(self, db: "FakeLedgerDB"):
        self.db = db
        self.op: str | None = None
        self.payload: dict | None = None
        self.filters: list[tuple[str, str, object]] = []
        self._limit: int | None = None

    # chain-methodes
    def insert(self, row):
        self.op, self.payload = "insert", dict(row)
        return self

    def update(self, patch):
        self.op, self.payload = "update", dict(patch)
        return self

    def select(self, *_):
        self.op = self.op or "select"
        return self

    def eq(self, col, val):
        self.filters.append(("eq", col, val))
        return self

    def in_(self, col, vals):
        self.filters.append(("in", col, list(vals)))
        return self

    def order(self, *_, **__):
        return self

    def limit(self, n):
        self._limit = n
        return self

    def _match(self, row) -> bool:
        for kind, col, val in self.filters:
            if kind == "eq" and row.get(col) != val:
                return False
            if kind == "in" and row.get(col) not in val:
                return False
        return True

    def execute(self):
        if self.db.table_missing:
            raise RuntimeError("PGRST205: table outbound_log not found")
        res = type("Res", (), {})()
        if self.op == "insert":
            row = self.payload
            # Emuleer de partial-UNIQUE uit migratie 022.
            if row.get("status") in _ACTIVE:
                clash = [r for r in self.db.rows
                         if r["workspace_id"] == row["workspace_id"]
                         and r["idempotency_key"] == row["idempotency_key"]
                         and r["status"] in _ACTIVE]
                if clash:
                    raise RuntimeError(
                        'duplicate key value violates unique constraint '
                        '"uq_outbound_log_active_key" (23505)'
                    )
            row = {**row, "id": f"rec-{len(self.db.rows) + 1}",
                   "created_at": row.get("created_at")
                   or datetime.now(timezone.utc).isoformat()}
            self.db.rows.append(row)
            res.data = [{"id": row["id"]}]
            return res
        if self.op == "update":
            updated = []
            for row in self.db.rows:
                if self._match(row):
                    row.update(self.payload)
                    updated.append(dict(row))
            res.data = updated
            return res
        # select
        rows = [dict(r) for r in self.db.rows if self._match(r)]
        if self._limit:
            rows = rows[: self._limit]
        res.data = rows
        return res


class FakeLedgerDB:
    """In-memory heatr_outbound_log met UNIQUE-emulatie en CAS-updates."""

    def __init__(self, table_missing: bool = False):
        self.rows: list[dict] = []
        self.table_missing = table_missing

    def table(self, name):
        assert name == "outbound_log", f"onverwachte tabel: {name}"
        return _Query(self)

    def seed(self, *, status: str, key: str = "test-key-1", ws: str = "aerys",
             age_minutes: int = 0, result=None) -> dict:
        row = {
            "id": f"seed-{len(self.rows) + 1}", "workspace_id": ws,
            "idempotency_key": key, "kind": "warmr_push", "status": status,
            "actor": "eerder", "lead_id": None, "lead_ids": None,
            "result": result, "error": None, "metadata": {},
            "created_at": (datetime.now(timezone.utc)
                           - timedelta(minutes=age_minutes)).isoformat(),
        }
        self.rows.append(row)
        return row

    def by_status(self, status: str) -> list[dict]:
        return [r for r in self.rows if r["status"] == status]


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


# ── Reserveringsmodel: happy path ───────────────────────────────────────────

def test_success_reserves_then_finalizes_completed():
    db = FakeLedgerDB()
    result, calls = _dispatch(db)
    assert result.executed is True
    assert calls["n"] == 1
    # Eén rij: de reservering, in-place gefinaliseerd naar completed.
    assert len(db.rows) == 1
    assert db.rows[0]["status"] == "completed"
    assert db.rows[0]["idempotency_key"] == "test-key-1"
    assert db.rows[0]["result"] == {"pushed": 1}
    assert result.record_ids == [db.rows[0]["id"]]


# ── Idempotency / concurrency ───────────────────────────────────────────────

def test_completed_duplicate_skipped_send_not_called():
    db = FakeLedgerDB()
    db.seed(status="completed", result="camp-123")
    result, calls = _dispatch(db)
    assert result.executed is False
    assert result.skipped_duplicate is True
    assert calls["n"] == 0
    # previous bevat het eerdere record incl. result (launch herstelt camp-id).
    assert result.previous["result"] == "camp-123"
    assert db.by_status("skipped_duplicate")


def test_concurrent_inflight_blocks_second_dispatch():
    """Scenario 2 (audit v2): twee workers, één key → tweede verstuurt NIET."""
    db = FakeLedgerDB()
    db.seed(status="in_flight", age_minutes=1)  # verse reservering van worker 1
    result, calls = _dispatch(db)
    assert result.executed is False
    assert result.skipped_duplicate is True
    assert calls["n"] == 0
    assert result.previous["status"] == "in_flight"


def test_true_race_second_insert_conflicts():
    """Race op de INSERT zelf: beide passeren de lookup, UNIQUE wint."""
    db = FakeLedgerDB()
    order: list[str] = []

    async def send_one():
        order.append("send-1")
        return {"pushed": 1}

    async def send_two():
        order.append("send-2")
        return {"pushed": 1}

    async def both():
        r1, r2 = await asyncio.gather(
            dispatch_outbound(
                kind="warmr_push", idempotency_key="race-key", actor="w1",
                send=send_one, supabase_client=db, workspace_id="aerys",
                lead=COMPLIANT),
            dispatch_outbound(
                kind="warmr_push", idempotency_key="race-key", actor="w2",
                send=send_two, supabase_client=db, workspace_id="aerys",
                lead=COMPLIANT),
        )
        return r1, r2

    r1, r2 = _run(both())
    executed = [r for r in (r1, r2) if r.executed]
    skipped = [r for r in (r1, r2) if r.skipped_duplicate]
    assert len(executed) == 1 and len(skipped) == 1
    assert len(order) == 1  # precies één send


def test_stale_inflight_taken_over_and_resent():
    """Worker-crash: oude in_flight (> TTL) wordt overgenomen en herverstuurd."""
    db = FakeLedgerDB()
    stale = db.seed(status="in_flight", age_minutes=60)
    result, calls = _dispatch(db)
    assert result.executed is True
    assert calls["n"] == 1
    # De gecrashte reservering is vrijgegeven, de nieuwe is completed.
    assert stale["status"] == "failed_retryable"
    assert db.by_status("completed")


def test_retry_after_failed_retryable_sends_again():
    """failed_retryable valt uit het UNIQUE-predicaat → bewuste retry mag."""
    db = FakeLedgerDB()
    db.seed(status="failed_retryable")
    result, calls = _dispatch(db)
    assert result.executed is True
    assert calls["n"] == 1


# ── Failure-classificatie ───────────────────────────────────────────────────

def test_network_failure_finalizes_retryable_and_reraises():
    db = FakeLedgerDB()

    async def failing_send():
        raise ConnectionError("warmr down")

    with pytest.raises(ConnectionError):
        _dispatch(db, send=failing_send)
    assert db.rows[0]["status"] == "failed_retryable"
    assert "warmr down" in db.rows[0]["error"]
    # Timeout/netwerk: externe staat onbekend → gemarkeerd voor de runbook.
    assert "external_state_unknown" in db.rows[0]["error"]


def test_4xx_failure_finalizes_terminal():
    db = FakeLedgerDB()

    class ApiError(Exception):
        status_code = 422

    async def failing_send():
        raise ApiError("unprocessable")

    with pytest.raises(ApiError):
        _dispatch(db, send=failing_send)
    assert db.rows[0]["status"] == "failed_terminal"


# ── Fail-closed / fail-soft bij ledger-uitval (Besluit 3) ───────────────────

def test_ledger_missing_prospect_send_fails_closed():
    """GEWIJZIGD gedrag (WP-A): geen reservering = geen prospect-send."""
    db = FakeLedgerDB(table_missing=True)
    with pytest.raises(DispatchLedgerUnavailable):
        _dispatch(db)


def test_ledger_missing_operator_email_fails_soft():
    """Interne meldingen: suppressie is gevaarlijker dan een duplicaat."""
    db = FakeLedgerDB(table_missing=True)
    result, calls = _dispatch(db, kind="operator_email", lead=None)
    assert result.executed is True
    assert calls["n"] == 1


def test_ledger_unavailable_is_catchable_as_dispatch_blocked():
    """Bestaande callers vangen DispatchBlocked — de subclass moet daaronder vallen."""
    db = FakeLedgerDB(table_missing=True)
    with pytest.raises(DispatchBlocked):
        _dispatch(db)


# ── Compliance-vangnet (ongewijzigd gedrag) ─────────────────────────────────

def test_compliance_block_prevents_send_and_records():
    db = FakeLedgerDB()
    with pytest.raises(DispatchBlocked):
        _dispatch(db, lead=BLOCKED)
    assert db.rows[0]["status"] == "blocked_compliance"
    assert "unsubscribed" in db.rows[0]["error"]


def test_bulk_compliance_blocks_on_any_noncompliant():
    db = FakeLedgerDB()
    with pytest.raises(DispatchBlocked):
        _dispatch(db, lead=None, leads=[COMPLIANT, BLOCKED], kind="warmr_bulk_push")
    assert db.rows[0]["status"] == "blocked_compliance"


# ── Record-only (alerts) ────────────────────────────────────────────────────

def test_enforce_idempotency_false_executes_despite_previous():
    db = FakeLedgerDB()
    db.seed(status="completed")
    result, calls = _dispatch(db, kind="operator_email", lead=None,
                              enforce_idempotency=False,
                              idempotency_key="alert:record-only")
    assert result.executed is True
    assert calls["n"] == 1
    assert db.by_status("completed")


# ── Input-validatie ─────────────────────────────────────────────────────────

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
