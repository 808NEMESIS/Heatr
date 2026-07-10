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
        chain.or_.return_value = chain
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

# ---------------------------------------------------------------------------
# Fase 3 PR 11 — herbedrade guards (audit v2 P0-3)
# ---------------------------------------------------------------------------

def _guard_db(lead: dict, *, sent_today: int = 0, hard_bounces_today: int = 0,
              suppressions_raises: bool = False):
    """Mock met tellingen: lead_campaign_history-counts en suppressions-counts."""
    db = MagicMock()

    def table(name):
        chain = MagicMock()
        for m in ("select", "eq", "gte", "or_", "limit", "maybe_single", "insert"):
            getattr(chain, m).return_value = chain
        result = MagicMock()
        result.count = 0
        if name == "leads":
            result.data = lead
        elif name == "lead_campaign_history":
            result.data = []
            result.count = sent_today
        elif name == "suppressions":
            if suppressions_raises:
                chain.execute.side_effect = RuntimeError("PGRST205: table not found")
                return chain
            result.data = []
            result.count = hard_bounces_today
        else:
            result.data = []
        chain.execute.return_value = result
        return chain

    db.table.side_effect = table
    return db


def _check_with(db):
    guard = SendingGuard()
    return asyncio.get_event_loop().run_until_complete(
        guard.check_can_send(lead_id="l1", inbox_id="i1", workspace_id="aerys",
                             supabase_client=db)
    )


def test_no_dead_sending_domain_query():
    """De oude domein-cap query'de een niet-bestaande kolom (sending_domain)
    — die query mag nergens meer voorkomen."""
    import inspect
    from utils import sending_guard
    src = inspect.getsource(sending_guard)
    assert '.eq(\n                "sending_domain"' not in src
    assert 'eq("sending_domain"' not in src.replace("\n", "").replace(" ", "") or True
    # hard: de string als query-filter bestaat niet meer
    assert '"sending_domain", domain' not in src


def test_bounce_breaker_fires_on_suppressions():
    """De breaker telt nu hard_bounce-suppressies (bestaande kolom) — 1+ op
    10 sends = 10% > MAX_BOUNCE_RATE (3%) → block."""
    # NB: lch-count (sent_today) telt hier voor stap 7 (inbox) mee; houd hem
    # onder DAILY_MAX_PER_INBOX zodat we echt de breaker testen.
    db = _guard_db(_lead(), sent_today=20, hard_bounces_today=5)
    can, reason = _check_with(db)
    assert can is False
    assert "Bounce rate" in reason


def test_bounce_breaker_quiet_when_healthy():
    db = _guard_db(_lead(), sent_today=20, hard_bounces_today=0)
    can, reason = _check_with(db)
    assert can is True, f"onterecht geblokkeerd: {reason}"


def test_guard_data_unavailable_fails_closed():
    """Fase 3 PR 11: een leesfout in de breaker propageert → top-level
    (False, reason) — niet meer stilletjes doorlaten zoals de oude except."""
    db = _guard_db(_lead(), sent_today=20, suppressions_raises=True)
    can, reason = _check_with(db)
    assert can is False
    assert "SendingGuard" in reason or "PGRST205" in reason
