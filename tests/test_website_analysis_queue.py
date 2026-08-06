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


def _make_chain(execute_fn):
    """One fluent Supabase-table mock. `.range(lo,hi)` records the slice bounds;
    `.execute()` calls execute_fn(state) so leads-paging can be simulated
    faithfully. Supports `.not_.is_(...)` (namespace access)."""
    m = MagicMock()
    for name in ("select", "eq", "in_", "order", "limit", "update"):
        setattr(m, name, MagicMock(return_value=m))
    not_ns = MagicMock()
    not_ns.is_ = MagicMock(return_value=m)
    m.not_ = not_ns
    state = {"lo": 0, "hi": None}

    def _range(lo, hi):
        state["lo"], state["hi"] = lo, hi
        return m
    m.range = MagicMock(side_effect=_range)
    m.execute = lambda: execute_fn(state)
    return m


def _mk_db(leads: list, wi_rows: list | None = None):
    """Query-aware Supabase mock. `table('leads')` pages through `leads` via
    `.range()` (score-ordered as the DB would return them); `table(...)` for
    website_intelligence returns `wi_rows`. Faithful to the paginated selector,
    so a starved lead beyond the first page is reachable in tests.
    """
    wi_rows = wi_rows or []

    def _leads_exec(state):
        lo = state["lo"]
        hi = state["hi"]
        page = leads[lo:(hi + 1)] if hi is not None else leads[lo:]
        return MagicMock(data=page)

    leads_chain = _make_chain(_leads_exec)
    wi_chain = _make_chain(lambda _state: MagicMock(data=wi_rows))

    def _table(name):
        return leads_chain if name == "leads" else wi_chain

    db = MagicMock()
    db.table = MagicMock(side_effect=_table)
    db._leads_chain = leads_chain          # test-introspectie (workspace-filter)
    db._wi_chain = wi_chain
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
        eq_calls = db._leads_chain.eq.call_args_list
        assert any(c.args == ("workspace_id", "aerys") for c in eq_calls)


# ==============================================================================
# Regressie: verse (lager scorende) leads mogen niet verhongeren achter een
# venster vol al-geanalyseerde high-score leads (de top-20-bug).
# ==============================================================================

class TestNoStarvationBeyondFirstPage:
    @pytest.mark.asyncio
    async def test_unanalyzed_lead_beyond_page_is_selected(self, monkeypatch):
        # Kleine pagegrootte forceert meerdere pagina's zoals in productie.
        monkeypatch.setattr(waq, "_SELECT_PAGE", 2)
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        # Score-aflopend zoals de DB teruggeeft: 2 hoge-score AL geanalyseerd,
        # daarna een verse lager scorende lead zonder WI-rij.
        leads = [
            _lead({"id": "old-A", "score": 90}),
            _lead({"id": "old-B", "score": 85}),
            _lead({"id": "fresh-C", "score": 40, "domain": "breda.nl"}),
        ]
        wi_rows = [
            {"lead_id": "old-A", "analyzed_at": recent},
            {"lead_id": "old-B", "analyzed_at": recent},
        ]
        db = _mk_db(leads=leads, wi_rows=wi_rows)
        picked = await waq._find_next_eligible_lead("aerys", db)
        assert picked is not None and picked["id"] == "fresh-C"

    @pytest.mark.asyncio
    async def test_highest_score_unanalyzed_wins_within_page(self):
        # Prioriteit blijft: bij meerdere niet-geanalyseerde leads → hoogste score.
        leads = [_lead({"id": "hi", "score": 90}), _lead({"id": "lo", "score": 50})]
        db = _mk_db(leads=leads, wi_rows=[])
        picked = await waq._find_next_eligible_lead("aerys", db)
        assert picked["id"] == "hi"

    @pytest.mark.asyncio
    async def test_all_analyzed_returns_none(self, monkeypatch):
        monkeypatch.setattr(waq, "_SELECT_PAGE", 2)
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        leads = [_lead({"id": "a", "score": 90}), _lead({"id": "b", "score": 80})]
        wi_rows = [{"lead_id": "a", "analyzed_at": recent},
                   {"lead_id": "b", "analyzed_at": recent}]
        db = _mk_db(leads=leads, wi_rows=wi_rows)
        assert await waq._find_next_eligible_lead("aerys", db) is None

    @pytest.mark.asyncio
    async def test_stale_wi_row_is_reanalyzed(self):
        # WI-rij ouder dan het reanalyse-venster → lead telt als niet-vers → opnieuw.
        stale = (datetime.now(timezone.utc) - timedelta(days=999)).isoformat()
        leads = [_lead({"id": "stale-one", "score": 70})]
        db = _mk_db(leads=leads, wi_rows=[{"lead_id": "stale-one", "analyzed_at": stale}])
        picked = await waq._find_next_eligible_lead("aerys", db)
        assert picked is not None and picked["id"] == "stale-one"

    @pytest.mark.asyncio
    async def test_recently_failed_lead_is_skipped(self):
        # Doomed hoogste-score lead die net faalde (redirect-loop/timeout) → skip;
        # de worker gaat door naar de volgende. Voorkomt queue-monopolie.
        recent_fail = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        leads = [
            _lead({"id": "doomed", "score": 90, "website_analysis_failed_at": recent_fail}),
            _lead({"id": "good", "score": 50}),
        ]
        db = _mk_db(leads=leads, wi_rows=[])
        picked = await waq._find_next_eligible_lead("aerys", db)
        assert picked["id"] == "good"

    @pytest.mark.asyncio
    async def test_old_failure_is_retried_after_cooldown(self):
        # Failure ouder dan de cooldown → opnieuw proberen (site kan hersteld zijn).
        old_fail = (datetime.now(timezone.utc) - timedelta(days=999)).isoformat()
        leads = [_lead({"id": "retry", "score": 90, "website_analysis_failed_at": old_fail})]
        db = _mk_db(leads=leads, wi_rows=[])
        picked = await waq._find_next_eligible_lead("aerys", db)
        assert picked is not None and picked["id"] == "retry"
