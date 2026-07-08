"""
tests/test_dedup_cooldown.py — Recovery Patch 1: 90-dagen-cooldown +
active-campaign-dedup na de status-vocabulaire-fix.

Bewijst dat dubbele outreach binnen 90 dagen wordt geblokkeerd. Dekt:
  - het exacte auditdefect (sequence_complete viel buiten de cooldown);
  - het gewenste gedrag (terminale enrollment >90d → weer toegestaan);
  - active-campaign-block via is_active (i.p.v. de nooit-geschreven status
    "active");
  - het foutpad (onparseerbare/lege sent_at maskeert geen oudere in-window rij).
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.deduplicator import should_allow_warmr_push, is_lead_in_active_campaign


def _iso(days_ago: float | None) -> str | None:
    if days_ago is None:
        return None
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()


class _Query:
    def __init__(self, store, table):
        self._store = store
        self._table = table
        self._eq: dict = {}
        self._single = False

    def select(self, *_a, **_k):
        return self

    def eq(self, col, val):
        self._eq[col] = val
        return self

    def in_(self, col, vals):
        self._eq[("in", col)] = vals
        return self

    def order(self, *_a, **_k):
        return self

    def limit(self, *_a, **_k):
        return self

    def single(self):
        self._single = True
        return self

    def maybe_single(self):
        self._single = True
        return self

    def execute(self):
        rows = self._store.get(self._table, [])
        # pas eq-filters toe
        out = []
        for r in rows:
            ok = True
            for k, v in self._eq.items():
                if isinstance(k, tuple):  # in_-filter
                    if r.get(k[1]) not in v:
                        ok = False
                elif r.get(k) != v:
                    ok = False
            if ok:
                out.append(r)

        class _R:
            pass

        res = _R()
        res.data = (out[0] if out else None) if self._single else out
        return res


class FakeDB:
    def __init__(self, store):
        self._store = store

    def table(self, name):
        return _Query(self._store, name)


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


_SENDABLE_LEAD = {
    "id": "L1", "email": "info@kliniek.nl", "email_status": "valid",
    "gdpr_safe": True, "status": "enriched", "workspace_id": "aerys",
}


# --- Het exacte auditdefect: sequence_complete moet cooldown triggeren --------

def test_completed_sequence_within_90d_blocks_reoutreach():
    """DE recovery-assertie: een lead die een sequence AFMAAKTE
    (status=sequence_complete, is_active=False) binnen 90 dagen mag NIET
    opnieuw gepusht worden. Pre-fix matchte de cooldown alleen 'completed' →
    'sequence_complete' viel erbuiten → dubbele outreach."""
    db = FakeDB({
        "leads": [_SENDABLE_LEAD],
        "lead_campaign_history": [
            {"id": "H1", "lead_id": "L1", "is_active": False,
             "status": "sequence_complete", "sent_at": _iso(30)},
        ],
    })
    allowed, reason = _run(should_allow_warmr_push("L1", db))
    assert allowed is False
    assert reason.startswith("cooldown_"), reason


def test_terminal_older_than_90d_allows_reoutreach():
    db = FakeDB({
        "leads": [_SENDABLE_LEAD],
        "lead_campaign_history": [
            {"id": "H1", "lead_id": "L1", "is_active": False,
             "status": "sequence_complete", "sent_at": _iso(120)},
        ],
    })
    allowed, reason = _run(should_allow_warmr_push("L1", db))
    assert allowed is True and reason == "ok"


def test_stopped_and_bounced_still_block():
    for st in ("stopped", "bounced", "unsubscribed", "no_response"):
        db = FakeDB({
            "leads": [_SENDABLE_LEAD],
            "lead_campaign_history": [
                {"id": "H", "lead_id": "L1", "is_active": False,
                 "status": st, "sent_at": _iso(10)},
            ],
        })
        allowed, reason = _run(should_allow_warmr_push("L1", db))
        assert allowed is False, f"{st} zou moeten blokkeren"
        assert reason.startswith("cooldown_")


# --- Active-campaign-dedup via is_active (niet status='active') ---------------

def test_active_campaign_blocks_via_is_active():
    db = FakeDB({"lead_campaign_history": [
        {"id": "H1", "lead_id": "L1", "is_active": True, "status": "pending"},
    ]})
    assert _run(is_lead_in_active_campaign("L1", db)) is True


def test_pending_active_lead_blocked_by_full_gate():
    db = FakeDB({
        "leads": [_SENDABLE_LEAD],
        "lead_campaign_history": [
            {"id": "H1", "lead_id": "L1", "is_active": True, "status": "pending",
             "sent_at": _iso(2)},
        ],
    })
    allowed, reason = _run(should_allow_warmr_push("L1", db))
    assert allowed is False and reason == "already_in_active_campaign"


# --- Foutpad: lege/onparseerbare sent_at maskeert geen oudere in-window rij ---

def test_null_recent_sent_at_does_not_mask_older_in_window_row():
    db = FakeDB({
        "leads": [_SENDABLE_LEAD],
        "lead_campaign_history": [
            # meest recente terminale rij zonder sent_at (bv. vóór eerste send geblokkeerd)
            {"id": "H2", "lead_id": "L1", "is_active": False, "status": "blocked", "sent_at": None},
            # oudere, WEL verzonden en binnen window → moet blokkeren
            {"id": "H1", "lead_id": "L1", "is_active": False, "status": "sequence_complete", "sent_at": _iso(20)},
        ],
    })
    allowed, reason = _run(should_allow_warmr_push("L1", db))
    assert allowed is False and reason.startswith("cooldown_")


def test_no_history_allows():
    db = FakeDB({"leads": [_SENDABLE_LEAD], "lead_campaign_history": []})
    allowed, reason = _run(should_allow_warmr_push("L1", db))
    assert allowed is True and reason == "ok"


# --- De bestaande gates blijven werken ---------------------------------------

def test_disqualified_blocks():
    lead = {**_SENDABLE_LEAD, "status": "disqualified"}
    db = FakeDB({"leads": [lead], "lead_campaign_history": []})
    allowed, reason = _run(should_allow_warmr_push("L1", db))
    assert allowed is False and reason == "lead_disqualified"


def test_bad_email_status_blocks():
    lead = {**_SENDABLE_LEAD, "email_status": "not_found"}
    db = FakeDB({"leads": [lead], "lead_campaign_history": []})
    allowed, reason = _run(should_allow_warmr_push("L1", db))
    assert allowed is False and reason.startswith("email_status_")


# --- SendingGuard (dispatch-pad) dwingt de cooldown nu ook af -----------------

class _GuardQuery:
    def __init__(self, store, table):
        self._s, self._t, self._eq = store, table, {}
        self._single = False

    def select(self, *a, **k):
        return self

    def eq(self, c, v):
        self._eq[c] = v
        return self

    def gte(self, c, v):
        self._eq[("gte", c)] = v
        return self

    def order(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def maybe_single(self):
        self._single = True
        return self

    def insert(self, *a, **k):
        return self

    def execute(self):
        rows = [r for r in self._s.get(self._t, [])
                if all(r.get(c) == v for c, v in self._eq.items() if not isinstance(c, tuple))]

        class _R:
            pass
        res = _R()
        res.data = (rows[0] if rows else None) if self._single else rows
        res.count = 0  # geen dag-limiet-hits in deze test
        return res


class _GuardDB:
    def __init__(self, store):
        self._s = store

    def table(self, name):
        return _GuardQuery(self._s, name)


def test_sending_guard_blocks_on_90d_cooldown():
    """Dispatch-pad: een compliant lead die 30 dagen geleden een sequence
    afmaakte wordt door SendingGuard geblokkeerd met reden cooldown_90d —
    voorheen ontbrak deze check volledig in het live pad."""
    from utils.sending_guard import SendingGuard
    store = {
        "leads": [{"id": "L1", "workspace_id": "aerys", "gdpr_safe": True,
                   "status": "qualified", "next_contact_after": None, "crm_stage": "ontdekt"}],
        "lead_campaign_history": [
            {"id": "H1", "lead_id": "L1", "is_active": False,
             "status": "sequence_complete", "sent_at": _iso(30)},
        ],
    }
    guard = SendingGuard()
    ok, reason = _run(guard.check_can_send(
        lead_id="L1", inbox_id="i1", workspace_id="aerys", supabase_client=_GuardDB(store)))
    assert ok is False
    assert "90-dagen-cooldown" in reason and "cooldown_" in reason, reason


def test_sending_guard_allows_when_no_recent_campaign():
    from utils.sending_guard import SendingGuard
    store = {
        "leads": [{"id": "L1", "workspace_id": "aerys", "gdpr_safe": True,
                   "status": "qualified", "next_contact_after": None, "crm_stage": "ontdekt"}],
        "lead_campaign_history": [],
    }
    guard = SendingGuard()
    ok, reason = _run(guard.check_can_send(
        lead_id="L1", inbox_id="i1", workspace_id="aerys", supabase_client=_GuardDB(store)))
    assert ok is True, reason
