"""
tests/test_cost_guard_failclosed.py — Recovery Patch 5.

Bewijst dat de kostenplafonds FAIL-CLOSED zijn: kan de cost-log niet gelezen
worden (DB-fout), dan wordt geblokkeerd i.p.v. blind doorgespend. Voorheen
faalden beide checks OPEN → een DB-hik schakelde het plafond uit.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.cost_guard import check_daily_budget, check_monthly_budget


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class _BoomDB:
    """Elke read faalt — simuleert een Supabase-leesfout."""
    def rpc(self, *a, **k):
        return self

    def table(self, *a, **k):
        return self

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def gte(self, *a, **k):
        return self

    def execute(self):
        raise RuntimeError("PGRST000: connection lost")


def test_daily_budget_fails_closed_on_db_error():
    allowed, spent, cap = _run(check_daily_budget("aerys", _BoomDB()))
    assert allowed is False, "daily budget moet BLOKKEREN bij een leesfout"
    assert spent == cap  # herkenbaar fail-closed-signaal


def test_monthly_budget_fails_closed_on_db_error():
    result = _run(check_monthly_budget("aerys", _BoomDB()))
    assert result["allowed"] is False
    assert result["block_reason"] == "cost_guard_db_error"
    assert result["tier_100_hit"] is True
