"""
tests/test_warmr_enrollment.py — tracking-only enrollments (ADR-001, fase 3 PR 9).

Valideert: registratie met send_owner='warmr'/status='active' (nooit
'pending'), idempotentie via de uq_lch_enrollment-emulatie, de
terminal-guard + campaign-scoping + sent_at-backfill van de webhook-closure,
en de ADR-grens: get_due_sends negeert warmr-owned rijen.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from campaigns.enrollment import close_campaign_enrollments, record_warmr_enrollments


class _Query:
    def __init__(self, db, table):
        self.db = db
        self.table_name = table
        self.op = "select"
        self.payload = None
        self.filters: list[tuple[str, str, object]] = []

    def insert(self, row):
        self.op, self.payload = "insert", dict(row)
        return self

    def update(self, patch):
        self.op, self.payload = "update", dict(patch)
        return self

    def select(self, *_):
        return self

    def eq(self, col, val):
        self.filters.append(("eq", col, val))
        return self

    def in_(self, col, vals):
        self.filters.append(("in", col, list(vals)))
        return self

    def is_(self, col, _null):
        self.filters.append(("is_null", col, None))
        return self

    def lte(self, col, val):
        self.filters.append(("lte", col, val))
        return self

    def order(self, *_, **__):
        return self

    def limit(self, _n):
        return self

    def _match(self, row):
        for kind, col, val in self.filters:
            if kind == "eq" and row.get(col) != val:
                return False
            if kind == "in" and row.get(col) not in val:
                return False
            if kind == "is_null" and row.get(col) is not None:
                return False
            if kind == "lte" and not (row.get(col) or "") <= val:
                return False
        return True

    def execute(self):
        rows = self.db.rows
        res = type("Res", (), {})()
        if self.op == "insert":
            row = self.payload
            # uq_lch_enrollment-emulatie (migratie 025)
            for r in rows:
                if (r["workspace_id"], r["lead_id"], r["campaign_id"]) == \
                   (row["workspace_id"], row["lead_id"], row["campaign_id"]):
                    raise RuntimeError(
                        'duplicate key value violates unique constraint '
                        '"uq_lch_enrollment" (23505)')
            rows.append(dict(row))
            res.data = [row]
            return res
        if self.op == "update":
            matched = [r for r in rows if self._match(r)]
            for r in matched:
                r.update(self.payload)
            res.data = [dict(r) for r in matched]
            return res
        res.data = [dict(r) for r in rows if self._match(r)]
        return res


class FakeLchDB:
    def __init__(self):
        self.rows: list[dict] = []

    def table(self, name):
        assert name == "lead_campaign_history", f"onverwachte tabel: {name}"
        return _Query(self, name)


def _run(coro):
    # asyncio.run: eigen verse loop per call — de suite bevat TestClient-tests
    # die de default loop sluiten.
    return asyncio.run(coro)


LEADS = [{"id": "l1", "email": "a@b.nl"}, {"id": "l2", "email": "c@d.nl"}]


def test_record_creates_warmr_owned_active_rows():
    db = FakeLchDB()
    out = _run(record_warmr_enrollments(
        db, workspace_id="aerys", leads=LEADS, campaign_id="camp-1",
        template_id="v3_1_website", service_type="website",
        sequence_steps=[{"step": 1}],
    ))
    assert out == {"created": 2, "already_enrolled": 0, "failed": 0}
    for row in db.rows:
        # ADR-001: NOOIT 'pending' (Model B-vocabulaire) op een warmr-rij
        assert row["send_owner"] == "warmr"
        assert row["status"] == "active"
        assert row["is_active"] is True
        assert row["enrolled_at"]


def test_record_is_idempotent_per_lead_campaign():
    db = FakeLchDB()
    _run(record_warmr_enrollments(db, workspace_id="aerys", leads=LEADS, campaign_id="camp-1"))
    out = _run(record_warmr_enrollments(db, workspace_id="aerys", leads=LEADS, campaign_id="camp-1"))
    assert out == {"created": 0, "already_enrolled": 2, "failed": 0}
    assert len(db.rows) == 2
    # Nieuwe campagne = expliciete nieuwe enrollment (her-benadering)
    out2 = _run(record_warmr_enrollments(db, workspace_id="aerys", leads=LEADS, campaign_id="camp-2"))
    assert out2["created"] == 2 and len(db.rows) == 4


def test_close_sets_terminal_status_inactive_and_sent_at():
    """F7-fix: completion sluit de enrollment én zet het cooldown-anker."""
    db = FakeLchDB()
    _run(record_warmr_enrollments(db, workspace_id="aerys", leads=LEADS[:1], campaign_id="camp-1"))
    closed = close_campaign_enrollments(
        db, workspace_id="aerys", lead_id="l1",
        mapped_status="no_response", campaign_id="camp-1", completed=True,
    )
    assert closed == 1
    row = db.rows[0]
    assert row["status"] == "no_response"
    assert row["is_active"] is False
    assert row["completed_at"] and row["sent_at"]  # cooldown-anker gezet


def test_close_terminal_guard_never_downgrades_replied():
    """Scenario 4 (audit v2): campaign.completed ná replied mag de terminale
    replied-status niet overschrijven naar no_response."""
    db = FakeLchDB()
    _run(record_warmr_enrollments(db, workspace_id="aerys", leads=LEADS[:1], campaign_id="camp-1"))
    close_campaign_enrollments(db, workspace_id="aerys", lead_id="l1",
                               mapped_status="replied", campaign_id="camp-1")
    closed = close_campaign_enrollments(db, workspace_id="aerys", lead_id="l1",
                                        mapped_status="no_response",
                                        campaign_id="camp-1", completed=True)
    assert closed == 0
    assert db.rows[0]["status"] == "replied"


def test_close_scopes_on_campaign_id():
    """Multi-campagne per lead: een no_response van campagne X mag de actieve
    enrollment in campagne Y niet raken (audit v2 §C2)."""
    db = FakeLchDB()
    _run(record_warmr_enrollments(db, workspace_id="aerys", leads=LEADS[:1], campaign_id="camp-X"))
    _run(record_warmr_enrollments(db, workspace_id="aerys", leads=LEADS[:1], campaign_id="camp-Y"))
    closed = close_campaign_enrollments(db, workspace_id="aerys", lead_id="l1",
                                        mapped_status="no_response",
                                        campaign_id="camp-X", completed=True)
    assert closed == 1
    by_camp = {r["campaign_id"]: r for r in db.rows}
    assert by_camp["camp-X"]["status"] == "no_response"
    assert by_camp["camp-Y"]["status"] == "active"
    assert by_camp["camp-Y"]["is_active"] is True


def test_get_due_sends_ignores_warmr_owned_rows():
    """DE ADR-001-grens: een warmr-tracking-rij mag nooit als due send uit
    de poller komen — anders dript Heatr bovenop Warmr (dubbele mail)."""
    from campaigns.sequence_engine import get_due_sends

    class _DueQuery(_Query):
        def select(self, *_):
            return self

    class DueDB(FakeLchDB):
        def table(self, name):
            assert name == "lead_campaign_history"
            return _DueQuery(self, name)

    db = DueDB()
    # Warmr-rij die er qua timing/status "due" uit zou zien als de
    # send_owner-filter zou ontbreken:
    db.rows.append({
        "workspace_id": "aerys", "lead_id": "l1", "campaign_id": "camp-1",
        "send_owner": "warmr", "status": "pending", "is_active": True,
        "next_send_at": "2020-01-01T00:00:00+00:00",
    })
    # Heatr-rij (Model B) die wél opgepikt moet worden:
    db.rows.append({
        "workspace_id": "aerys", "lead_id": "l2", "campaign_id": "camp-2",
        "send_owner": "heatr", "status": "pending", "is_active": True,
        "next_send_at": "2020-01-01T00:00:00+00:00",
    })
    due = _run(get_due_sends("aerys", db))
    assert [r["lead_id"] for r in due] == ["l2"]
