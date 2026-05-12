"""
tests/test_website_analysis_queue.py — Tests for process_next_website_analysis.

Verifies eligibility filtering, workspace isolation, error handling.
Run with: pytest tests/test_website_analysis_queue.py -v
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from job_queue import website_analysis_queue as waq


def _lead(overrides: dict | None = None) -> dict:
    base = {
        "id": "lead-001",
        "company_name": "Praktijk Test",
        "domain": "test.nl",
        "sector": "alternatieve_geneeskunde",
        "email_status": "valid",
        "status": "enriched",
        "score": 70,
    }
    if overrides:
        base.update(overrides)
    return base


def _mk_db(leads: list, wi_rows: list | None = None):
    """Build a Supabase client mock where the first table(leads) query returns `leads`
    and the subsequent table(website_intelligence) query returns `wi_rows`.

    Supports fluent chain including `.not_.is_(...)` (namespace access).
    """
    wi_rows = wi_rows or []
    table_mock = MagicMock()

    # Fluent methods returning self
    for m in ("select", "eq", "in_", "order", "limit", "update"):
        setattr(table_mock, m, MagicMock(return_value=table_mock))

    # `not_` is a namespace/property — its `.is_(...)` returns the chain
    not_namespace = MagicMock()
    not_namespace.is_ = MagicMock(return_value=table_mock)
    table_mock.not_ = not_namespace

    results = [MagicMock(data=leads), MagicMock(data=wi_rows)]
    idx = {"n": 0}

    def exec_fn():
        n = idx["n"]
        idx["n"] += 1
        return results[n] if n < len(results) else MagicMock(data=[])
    table_mock.execute = exec_fn

    db = MagicMock()
    db.table = MagicMock(return_value=table_mock)
    return db


# ==============================================================================
# Empty queue
# ==============================================================================

class TestEmptyQueue:
    @pytest.mark.asyncio
    async def test_no_candidates_returns_none(self):
        db = _mk_db(leads=[])
        result = await waq.process_next_website_analysis("aerys", db)
        assert result is None

    @pytest.mark.asyncio
    async def test_only_terminal_candidates_returns_none(self):
        db = _mk_db(leads=[_lead({"status": "disqualified"})])
        result = await waq.process_next_website_analysis("aerys", db)
        assert result is None


# ==============================================================================
# Dedup against existing WI
# ==============================================================================

class TestDedupAgainstExistingWi:
    @pytest.mark.asyncio
    async def test_lead_with_recent_wi_row_is_skipped(self):
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        db = _mk_db(
            leads=[_lead()],
            wi_rows=[{"lead_id": "lead-001", "analyzed_at": recent}],
        )
        result = await waq.process_next_website_analysis("aerys", db)
        # single candidate is deduped → nothing eligible → None
        assert result is None


# ==============================================================================
# Prescreen fail path
# ==============================================================================

class TestPrescreenFail:
    @pytest.mark.asyncio
    async def test_prescreen_fail_marks_failure_and_returns_dict(self):
        db = _mk_db(leads=[_lead()])

        with patch(
            "enrichment.website_prescreener.is_real_website",
            new=AsyncMock(return_value=(False, "parked_domain")),
        ):
            result = await waq.process_next_website_analysis("aerys", db)

        assert result is not None
        assert result["processed"] is False
        assert "prescreen_fail" in result["reason"]
        assert result["lead_id"] == "lead-001"


# ==============================================================================
# Happy path — analyze_website is invoked
# ==============================================================================

class TestHappyPath:
    @pytest.mark.asyncio
    async def test_eligible_lead_runs_analyze_website(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
        db = _mk_db(leads=[_lead()])

        fake_analyze = AsyncMock(return_value={"total_score": 42})

        with patch(
            "enrichment.website_prescreener.is_real_website",
            new=AsyncMock(return_value=(True, "ok")),
        ), patch("website_intelligence.analyzer.analyze_website", new=fake_analyze):
            result = await waq.process_next_website_analysis("aerys", db)

        assert result["processed"] is True
        assert result["lead_id"] == "lead-001"
        assert result["total_score"] == 42
        assert fake_analyze.await_count == 1
        kwargs = fake_analyze.await_args.kwargs
        assert kwargs["workspace_id"] == "aerys"
        assert kwargs["lead_id"] == "lead-001"
        assert kwargs["domain"] == "test.nl"


# ==============================================================================
# analyze_website raises → never propagates
# ==============================================================================

class TestAnalyzeError:
    @pytest.mark.asyncio
    async def test_analyze_raises_returns_error_dict(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
        db = _mk_db(leads=[_lead()])

        with patch(
            "enrichment.website_prescreener.is_real_website",
            new=AsyncMock(return_value=(True, "ok")),
        ), patch(
            "website_intelligence.analyzer.analyze_website",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            result = await waq.process_next_website_analysis("aerys", db)

        assert result["processed"] is False
        assert "analyze_failed" in result["error"]


# ==============================================================================
# Workspace isolation
# ==============================================================================

class TestWorkspaceIsolation:
    @pytest.mark.asyncio
    async def test_query_filters_on_workspace_id(self):
        db = _mk_db(leads=[])
        await waq.process_next_website_analysis("aerys", db)

        # Verify leads table was queried with eq("workspace_id", "aerys")
        called_tables = [c.args[0] for c in db.table.call_args_list]
        assert "leads" in called_tables
        eq_calls = db.table.return_value.eq.call_args_list
        assert any(c.args == ("workspace_id", "aerys") for c in eq_calls)
