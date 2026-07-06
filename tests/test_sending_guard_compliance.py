"""
tests/test_sending_guard_compliance.py — Sprint 1 fix-verificatie.

Het dispatch-pad (process_due_send → SendingGuard) velde een eigen
compliance-oordeel dat "disqualified" miste: een lead die ná launch
gediskwalificeerd werd, kreeg mail 2/3 gewoon door. Na de fix loopt
SendingGuard via de centrale compliance_check — deze tests bewijzen dat
alle BLOCKED_STATUSES + gdpr_safe nu ook op dispatch-tijd blokkeren.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.sending_guard import SendingGuard


def _db_with_lead(lead: dict | None):
    """MagicMock-db: leads-select levert `lead`; alle overige queries leeg."""
    db = MagicMock()

    def table(name):
        chain = MagicMock()
        chain.select.return_value = chain
        chain.eq.return_value = chain
        chain.gte.return_value = chain
        chain.limit.return_value = chain
        chain.maybe_single.return_value = chain
        chain.insert.return_value = chain
        result = MagicMock()
        if name == "leads":
            result.data = lead
        else:
            result.data = []
        result.count = 0
        chain.execute.return_value = result
        return chain

    db.table.side_effect = table
    return db


def _check(lead):
    guard = SendingGuard()
    db = _db_with_lead(lead)
    return asyncio.get_event_loop().run_until_complete(
        guard.check_can_send(lead_id="l1", inbox_id="i1", workspace_id="aerys", supabase_client=db)
    )


def _lead(**overrides):
    base = {
        "id": "l1",
        "gdpr_safe": True,
        "status": "qualified",
        "next_contact_after": None,
        "crm_stage": "ontdekt",
    }
    base.update(overrides)
    return base


def test_compliant_lead_passes_compliance():
    ok, reason = _check(_lead())
    assert ok is True, f"compliant lead geblokkeerd: {reason}"


def test_disqualified_blocks_on_dispatch():
    """DE Sprint 1-vondst: disqualified werd vóór de fix NIET geblokkeerd."""
    ok, reason = _check(_lead(status="disqualified"))
    assert ok is False
    assert "disqualified" in reason


def test_unsubscribed_blocks_on_dispatch():
    ok, reason = _check(_lead(status="unsubscribed"))
    assert ok is False
    assert "unsubscribed" in reason


def test_forgotten_blocks_on_dispatch():
    ok, reason = _check(_lead(status="forgotten"))
    assert ok is False
    assert "forgotten" in reason


def test_gdpr_unsafe_blocks_on_dispatch():
    ok, reason = _check(_lead(gdpr_safe=False))
    assert ok is False
    assert "gdpr_safe" in reason


def test_missing_lead_blocks():
    ok, reason = _check(None)
    assert ok is False
    assert "niet gevonden" in reason.lower()
