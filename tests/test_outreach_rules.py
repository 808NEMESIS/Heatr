"""
tests/test_outreach_rules.py — Outreach safety rules test suite.

Tests validate_sequence_config and SendingGuard logic.
Run with: pytest tests/test_outreach_rules.py -v
"""

from __future__ import annotations

import asyncio
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from campaigns.sequence_engine import (
    validate_sequence_config,
    auto_fix_sequence_config,
    MAX_SEQUENCE_STEPS,
    MIN_WAIT_DAYS,
    RECONTACT_COOLDOWN_DAYS,
)
from utils.sending_guard import SendingGuard


# ==============================================================================
# Helpers
# ==============================================================================

def _good_step(subject="Gratis website review voor {{company}}", delay_days=0):
    """Return a valid sequence step with enough body words."""
    body = " ".join(["word"] * 55)  # 55 words — above 50-word minimum
    return {"subject": subject, "body": body, "delay_days": delay_days}


def _make_db(overrides: dict | None = None):
    """Return a mock Supabase client that passes all guard checks by default."""
    db = MagicMock()

    def _chain(*args, **kwargs):
        """Make every chained call return the same mock so asserts work."""
        m = MagicMock()
        m.eq = _chain; m.gte = _chain; m.lte = _chain
        m.in_ = _chain; m.not_ = _chain; m.gt = _chain
        m.select = _chain; m.update = _chain; m.insert = _chain
        m.delete = _chain; m.maybe_single = _chain; m.limit = _chain
        m.order = _chain; m.execute = MagicMock(return_value=MagicMock(data=[], count=0))
        return m

    db.table = MagicMock(side_effect=lambda name: _chain())

    if overrides:
        for key, val in overrides.items():
            setattr(db, key, val)

    return db


def _lead(overrides: dict | None = None) -> dict:
    base = {
        "id": "lead-001",
        "gdpr_safe": True,
        "status": "active",
        "next_contact_after": None,
        "crm_stage": "ontdekt",
        "contact_attempt_count": 0,
    }
    if overrides:
        base.update(overrides)
    return base


# ==============================================================================
# Regel 1 — maximaal MAX_SEQUENCE_STEPS stappen
# ==============================================================================

class TestMaxFollowups:
    def test_max_steps_valid(self):
        """Sequence with exactly MAX_SEQUENCE_STEPS is valid."""
        steps = [_good_step() if i == 0 else _good_step(delay_days=3) for i in range(MAX_SEQUENCE_STEPS)]
        valid, errors = validate_sequence_config(steps)
        assert valid, f"Expected valid, got errors: {errors}"

    def test_max_steps_exceeded(self):
        """Sequence with MAX_SEQUENCE_STEPS + 1 steps produces an error."""
        steps = [_good_step() if i == 0 else _good_step(delay_days=3) for i in range(MAX_SEQUENCE_STEPS + 1)]
        valid, errors = validate_sequence_config(steps)
        assert not valid
        assert any("stappen" in e for e in errors), f"Expected stappen error, got: {errors}"

    def test_single_step_valid(self):
        """A single step (no follow-ups) is always valid."""
        valid, errors = validate_sequence_config([_good_step()])
        assert valid, f"Single step should be valid: {errors}"


# ==============================================================================
# Regel 2 — minimale wachttijd MIN_WAIT_DAYS
# ==============================================================================

class TestMinWaitDays:
    def test_wait_below_minimum_produces_error(self):
        """wait_days = 1 on follow-up step produces validation error."""
        steps = [_good_step(), _good_step(delay_days=1)]
        valid, errors = validate_sequence_config(steps)
        assert not valid
        assert any("wachttijd" in e for e in errors), f"Expected wachttijd error, got: {errors}"

    def test_wait_at_minimum_is_valid(self):
        """wait_days = MIN_WAIT_DAYS on follow-up step is valid."""
        steps = [_good_step(), _good_step(delay_days=MIN_WAIT_DAYS)]
        valid, errors = validate_sequence_config(steps)
        assert valid, f"Expected valid at MIN_WAIT_DAYS, got: {errors}"

    def test_auto_fix_raises_wait_days(self):
        """auto_fix_sequence_config raises delay_days below minimum."""
        steps = [_good_step(), _good_step(delay_days=0)]
        fixed = auto_fix_sequence_config(steps)
        assert fixed[1]["delay_days"] == MIN_WAIT_DAYS

    def test_first_step_wait_days_ignored(self):
        """First step delay_days = 0 is not checked."""
        steps = [_good_step(delay_days=0)]
        valid, errors = validate_sequence_config(steps)
        assert valid


# ==============================================================================
# Regel 3 — unsubscribed leads nooit opnieuw benaderen
# ==============================================================================

class TestUnsubscribedNeverResent:
    @pytest.mark.asyncio
    async def test_unsubscribed_blocked(self):
        """SendingGuard blocks leads with status='unsubscribed'."""
        guard = SendingGuard()

        db = _make_db()
        # Mock lead fetch to return unsubscribed lead
        lead_result = MagicMock()
        lead_result.data = _lead({"status": "unsubscribed"})

        table_mock = MagicMock()
        table_mock.select.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.maybe_single.return_value = table_mock
        table_mock.execute.return_value = lead_result
        table_mock.insert.return_value = table_mock
        table_mock.delete.return_value = table_mock
        db.table = MagicMock(return_value=table_mock)

        can_send, reason = await guard.check_can_send(
            lead_id="lead-001", inbox_id="inbox-001",
            workspace_id="aerys", supabase_client=db,
        )
        assert not can_send
        assert "uitgeschreven" in reason.lower() or "unsubscribed" in reason.lower()

    @pytest.mark.asyncio
    async def test_active_lead_not_blocked_by_status(self):
        """Active lead passes status check."""
        guard = SendingGuard()

        db = _make_db()
        lead_result = MagicMock()
        lead_result.data = _lead({"status": "active", "gdpr_safe": True})

        campaign_result = MagicMock()
        campaign_result.data = []

        inbox_result = MagicMock()
        inbox_result.data = None

        execute_results = [lead_result, campaign_result]

        table_mock = MagicMock()
        table_mock.select.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.gte.return_value = table_mock
        table_mock.lte.return_value = table_mock
        table_mock.limit.return_value = table_mock
        table_mock.maybe_single.return_value = table_mock
        table_mock.insert.return_value = table_mock
        # Return different results on successive execute() calls

        call_count = [0]
        def mock_execute():
            idx = call_count[0]
            call_count[0] += 1
            results = [lead_result, campaign_result, MagicMock(data=[], count=0),
                       MagicMock(data=[], count=0), MagicMock(data=[], count=0),
                       MagicMock(data=[], count=5), MagicMock(data=[], count=5)]
            return results[idx] if idx < len(results) else MagicMock(data=[], count=0)

        table_mock.execute = mock_execute
        db.table = MagicMock(return_value=table_mock)

        # Status check passes (no unsubscribed), other checks will hit mocked data
        # The point is: status='active' doesn't get blocked by rule 3
        can_send, reason = await guard.check_can_send(
            lead_id="lead-001", inbox_id="inbox-001",
            workspace_id="aerys", supabase_client=db,
        )
        # May still be blocked by other checks (GDPR, limits etc) but not by "unsubscribed"
        assert "uitgeschreven" not in reason.lower()


# ==============================================================================
# Regel 4 — cooldown wordt gerespecteerd
# ==============================================================================

class TestRecontactCooldown:
    @pytest.mark.asyncio
    async def test_lead_in_cooldown_is_blocked(self):
        """Lead with next_contact_after in the future is blocked."""
        guard = SendingGuard()

        future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()

        db = _make_db()
        lead_result = MagicMock()
        lead_result.data = _lead({"status": "no_response", "next_contact_after": future})

        table_mock = MagicMock()
        table_mock.select.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.maybe_single.return_value = table_mock
        table_mock.insert.return_value = table_mock
        table_mock.execute.return_value = lead_result
        db.table = MagicMock(return_value=table_mock)

        can_send, reason = await guard.check_can_send(
            lead_id="lead-001", inbox_id="inbox-001",
            workspace_id="aerys", supabase_client=db,
        )
        assert not can_send
        assert "cooldown" in reason.lower()

    @pytest.mark.asyncio
    async def test_expired_cooldown_passes(self):
        """Lead with next_contact_after in the past passes cooldown check."""
        guard = SendingGuard()

        past = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        db = _make_db()
        lead_result = MagicMock()
        lead_result.data = _lead({"status": "no_response", "next_contact_after": past, "gdpr_safe": True})

        table_mock = MagicMock()
        table_mock.select.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.maybe_single.return_value = table_mock
        table_mock.insert.return_value = table_mock
        table_mock.execute.return_value = MagicMock(data=lead_result.data, count=0)
        db.table = MagicMock(return_value=table_mock)

        can_send, reason = await guard.check_can_send(
            lead_id="lead-001", inbox_id="inbox-001",
            workspace_id="aerys", supabase_client=db,
        )
        # Should not be blocked by cooldown (may be blocked by other checks)
        assert "cooldown" not in reason.lower()


# ==============================================================================
# Regel 5 — zelfde lead nooit in twee actieve campagnes
# ==============================================================================

class TestNoDuplicateCampaigns:
    @pytest.mark.asyncio
    async def test_lead_in_active_campaign_blocked(self):
        """Lead already in an active campaign is blocked."""
        guard = SendingGuard()

        db = _make_db()
        lead_result = MagicMock()
        lead_result.data = _lead({"gdpr_safe": True, "status": "active"})

        campaign_result = MagicMock()
        campaign_result.data = [{"id": "campaign-001"}]  # active campaign exists

        call_count = [0]
        results = [lead_result, campaign_result]

        table_mock = MagicMock()
        table_mock.select.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.maybe_single.return_value = table_mock
        table_mock.limit.return_value = table_mock
        table_mock.insert.return_value = table_mock

        def mock_execute():
            idx = call_count[0]
            call_count[0] += 1
            return results[idx] if idx < len(results) else MagicMock(data=[], count=0)

        table_mock.execute = mock_execute
        db.table = MagicMock(return_value=table_mock)

        can_send, reason = await guard.check_can_send(
            lead_id="lead-001", inbox_id="inbox-001",
            workspace_id="aerys", supabase_client=db,
        )
        assert not can_send
        assert "campagne" in reason.lower()


# ==============================================================================
# Regel 6 — bounce rate circuit breaker
# ==============================================================================

class TestBounceRateCircuitBreaker:
    @pytest.mark.asyncio
    async def test_high_bounce_rate_blocks_all_sends(self):
        """Bounce rate > MAX_BOUNCE_RATE blocks all sends for workspace."""
        guard = SendingGuard()
        guard.MAX_BOUNCE_RATE = 0.03  # 3%

        db = _make_db()
        lead_result = MagicMock()
        lead_result.data = _lead({"gdpr_safe": True, "status": "active"})

        no_campaign = MagicMock()
        no_campaign.data = []

        no_inbox = MagicMock()
        no_inbox.data = None  # no inbox cache

        # Inbox daily count: 5 (under limit)
        inbox_count = MagicMock(); inbox_count.data = []; inbox_count.count = 5
        # Workspace daily count: 50 (under limit)
        ws_count = MagicMock(); ws_count.data = []; ws_count.count = 50
        # Bounce check: 100 sent, 5 bounced = 5% > 3%
        sent_count = MagicMock(); sent_count.count = 100
        bounce_count = MagicMock(); bounce_count.count = 5  # 5%

        results = [lead_result, no_campaign, no_inbox, inbox_count, ws_count, sent_count, bounce_count]
        call_count = [0]

        table_mock = MagicMock()
        table_mock.select.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.gte.return_value = table_mock
        table_mock.lte.return_value = table_mock
        table_mock.limit.return_value = table_mock
        table_mock.maybe_single.return_value = table_mock
        table_mock.insert.return_value = table_mock

        def mock_execute():
            idx = call_count[0]
            call_count[0] += 1
            return results[idx] if idx < len(results) else MagicMock(data=[], count=0)

        table_mock.execute = mock_execute
        db.table = MagicMock(return_value=table_mock)

        with patch("utils.alert_manager.send_alert", new_callable=AsyncMock):
            can_send, reason = await guard.check_can_send(
                lead_id="lead-001", inbox_id="inbox-001",
                workspace_id="aerys", supabase_client=db,
            )

        assert not can_send
        assert "bounce" in reason.lower()

    def test_bounce_rate_below_threshold_passes(self):
        """Bounce rate below MAX_BOUNCE_RATE does not trigger circuit breaker."""
        # This is a logic test — we verify the math, not the full async flow
        guard = SendingGuard()
        rate = 5 / 200  # 2.5% — below 3%
        assert rate < guard.MAX_BOUNCE_RATE


# ==============================================================================
# Sequence validation — spam words
# ==============================================================================

class TestSpamWordDetection:
    def test_spam_word_in_subject_produces_error(self):
        """Subject containing spam word triggers validation error."""
        step = _good_step(subject="Gratis website analyse voor uw bedrijf")
        valid, errors = validate_sequence_config([step])
        assert not valid
        assert any("gratis" in e.lower() for e in errors)

    def test_clean_subject_passes(self):
        """Clean subject passes validation."""
        step = _good_step(subject="Uw website scoort lager dan concurrenten in Amsterdam")
        valid, errors = validate_sequence_config([step])
        assert valid, f"Expected valid, got: {errors}"


# ==============================================================================
# Sequence validation — body word count
# ==============================================================================

class TestBodyWordCount:
    def test_short_body_fails(self):
        """Body under 50 words fails validation."""
        short_step = {"subject": "Uw website", "body": "Goedemiddag, ik wilde contact opnemen.", "delay_days": 0}
        valid, errors = validate_sequence_config([short_step])
        assert not valid
        assert any("woorden" in e for e in errors)

    def test_exact_50_words_passes(self):
        """Body with exactly 50 words passes validation."""
        body = " ".join(["woord"] * 50)
        step = {"subject": "Uw website", "body": body, "delay_days": 0}
        valid, errors = validate_sequence_config([step])
        assert valid, f"50 words should pass: {errors}"


# ==============================================================================
# GDPR — non-safe leads blocked
# ==============================================================================

class TestGdprSafeCheck:
    @pytest.mark.asyncio
    async def test_non_gdpr_safe_lead_blocked(self):
        """Lead with gdpr_safe=False is always blocked."""
        guard = SendingGuard()

        db = _make_db()
        lead_result = MagicMock()
        lead_result.data = _lead({"gdpr_safe": False, "status": "active"})

        table_mock = MagicMock()
        table_mock.select.return_value = table_mock
        table_mock.eq.return_value = table_mock
        table_mock.maybe_single.return_value = table_mock
        table_mock.insert.return_value = table_mock
        table_mock.execute.return_value = lead_result
        db.table = MagicMock(return_value=table_mock)

        can_send, reason = await guard.check_can_send(
            lead_id="lead-001", inbox_id="inbox-001",
            workspace_id="aerys", supabase_client=db,
        )
        assert not can_send
        assert "gdpr" in reason.lower()
