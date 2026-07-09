"""
tests/test_cost_guard.py — Cost-guard hard ceilings.

Verifies LeadCostAccumulator en guarded_call-gate gedrag zonder echte DB/Claude.
Run: pytest tests/test_cost_guard.py -v
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import MagicMock

from utils.cost_guard import LeadCostAccumulator, check_daily_budget, guarded_call


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    monkeypatch.delenv("COST_GUARD_DISABLED", raising=False)
    monkeypatch.delenv("ENRICHMENT_DAILY_BUDGET_EUR", raising=False)
    monkeypatch.delenv("MAX_COST_PER_LEAD_EUR", raising=False)


def _db_with_today_spend(total_eur: float):
    """Mock Supabase client whose api_cost_log select().execute().data
    returns rows summing to `total_eur`."""
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.gte.return_value = chain
    chain.insert.return_value = chain
    chain.execute.return_value = MagicMock(data=[{"cost_eur": total_eur}] if total_eur else [])
    db = MagicMock()
    db.table.return_value = chain
    return db


class TestLeadCostAccumulator:
    def test_allowed_when_below_ceiling(self):
        acc = LeadCostAccumulator(lead_id="x", workspace_id="aerys", ceiling_eur=0.05)
        ok, _ = acc.check_allowed(0.01)
        assert ok

    def test_blocked_when_charge_exceeds(self):
        acc = LeadCostAccumulator(lead_id="x", workspace_id="aerys", ceiling_eur=0.005)
        ok, _ = acc.check_allowed(0.001)
        assert ok
        acc.charge(0.002, "step1")
        acc.charge(0.004, "step2")
        assert acc.blocked
        ok, reason = acc.check_allowed(0.001)
        assert not ok
        assert "ceiling_exceeded" in reason

    def test_disabled_env_bypasses(self, monkeypatch):
        monkeypatch.setenv("COST_GUARD_DISABLED", "true")
        acc = LeadCostAccumulator(lead_id="x", workspace_id="aerys", ceiling_eur=0.0)
        ok, _ = acc.check_allowed(99.0)
        assert ok


class TestDailyBudget:
    @pytest.mark.asyncio
    async def test_allowed_below_cap(self, monkeypatch):
        monkeypatch.setenv("ENRICHMENT_DAILY_BUDGET_EUR", "0.50")
        db = _db_with_today_spend(0.10)
        ok, spent, cap = await check_daily_budget("aerys", db)
        assert ok and cap == 0.50 and spent == 0.10

    @pytest.mark.asyncio
    async def test_blocked_above_cap(self, monkeypatch):
        monkeypatch.setenv("ENRICHMENT_DAILY_BUDGET_EUR", "0.50")
        db = _db_with_today_spend(0.60)
        ok, spent, cap = await check_daily_budget("aerys", db)
        assert not ok and spent == 0.60

    @pytest.mark.asyncio
    async def test_db_error_fails_closed(self):
        # Recovery-fix (Patch 5): een leesfout mag het plafond NIET uitschakelen.
        # Voorheen fail-open (assert ok); nu fail-closed — blokkeren.
        db = MagicMock()
        db.table.side_effect = RuntimeError("down")
        ok, spent, cap = await check_daily_budget("aerys", db)
        assert not ok and spent == cap


class TestGuardedCall:
    @pytest.mark.asyncio
    async def test_records_block_event_when_per_lead_exceeded(self, monkeypatch):
        monkeypatch.setenv("ENRICHMENT_DAILY_BUDGET_EUR", "10.0")
        acc = LeadCostAccumulator(lead_id="x", workspace_id="aerys", ceiling_eur=0.001)
        acc.charge(0.001, "pre")
        db = _db_with_today_spend(0.01)
        ok, reason = await guarded_call(
            workspace_id="aerys", lead_id="x",
            context="treatment_classifier", estimated_cost_eur=0.0005,
            supabase_client=db, accumulator=acc,
        )
        assert not ok
        assert "ceiling_exceeded" in reason
