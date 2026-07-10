"""
tests/test_process_next_enrichment.py — Smoketest for process_next_enrichment.

Verifies wrapper dispatches correctly to claim + run helpers and never raises.
Run with: pytest tests/test_process_next_enrichment.py -v
"""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from job_queue import enrichment_queue as eq


# ==============================================================================
# Empty queue
# ==============================================================================

class TestEmptyQueue:
    @pytest.mark.asyncio
    async def test_empty_queue_returns_none(self):
        with patch.object(eq, "claim_next_enrichment_job", new=AsyncMock(return_value=None)):
            result = await eq.process_next_enrichment("aerys", MagicMock())
        assert result is None

    @pytest.mark.asyncio
    async def test_claim_raises_returns_error_dict(self):
        with patch.object(
            eq, "claim_next_enrichment_job",
            new=AsyncMock(side_effect=RuntimeError("db gone")),
        ):
            result = await eq.process_next_enrichment("aerys", MagicMock())
        assert result == {"processed": False, "error": "claim_failed: db gone"}


# ==============================================================================
# Workspace safety
# ==============================================================================

class TestWorkspaceMismatch:
    @pytest.mark.asyncio
    async def test_job_from_different_workspace_rejected(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
        monkeypatch.setenv("WARMR_API_URL", "https://example.test")
        monkeypatch.setenv("WARMR_API_KEY", "key")

        job = {"id": "job-1", "lead_id": "lead-1", "workspace_id": "other-ws"}
        db = MagicMock()
        with patch.object(eq, "claim_next_enrichment_job", new=AsyncMock(return_value=job)):
            result = await eq.process_next_enrichment("aerys", db)
        assert result["processed"] is False
        assert result["error"] == "workspace_mismatch"
        # Fase 4 PR 13: het vangnet laat de job niet meer stranden in
        # 'running' — hij wordt teruggezet naar pending.
        db.table.assert_any_call("enrichment_jobs")
        update_calls = [c for c in db.table.return_value.update.call_args_list
                        if c.args and c.args[0].get("status") == "pending"]
        assert update_calls, "job niet teruggezet naar pending (strand-fix)"

    @pytest.mark.asyncio
    async def test_claim_is_workspace_scoped(self, monkeypatch):
        """Fase 4 PR 13 (audit v2 scenario 10): de claim filtert op de eigen
        workspace — cross-tenant job-steal is structureel onmogelijk."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
        monkeypatch.setenv("WARMR_API_URL", "https://example.test")
        monkeypatch.setenv("WARMR_API_KEY", "key")
        claim = AsyncMock(return_value=None)
        with patch.object(eq, "claim_next_enrichment_job", new=claim):
            await eq.process_next_enrichment("aerys", MagicMock())
        claim.assert_awaited_once()
        assert claim.await_args.kwargs.get("workspace_id") == "aerys"


# ==============================================================================
# Happy path — run_enrichment_for_lead dispatched
# ==============================================================================

class TestHappyPath:
    @pytest.mark.asyncio
    async def test_claimed_job_triggers_run(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

        job = {"id": "job-42", "lead_id": "lead-42", "workspace_id": "aerys"}
        fake_run = AsyncMock(return_value=None)

        with patch.object(eq, "claim_next_enrichment_job", new=AsyncMock(return_value=job)), \
             patch.object(eq, "run_enrichment_for_lead", new=fake_run), \
             patch.object(eq, "_get_warmr_client_for_worker", return_value=MagicMock()):
            result = await eq.process_next_enrichment("aerys", MagicMock())

        assert result == {"processed": True, "lead_id": "lead-42", "job_id": "job-42"}
        assert fake_run.await_count == 1


# ==============================================================================
# Error during run — captured, never raised
# ==============================================================================

class TestRunError:
    @pytest.mark.asyncio
    async def test_run_raises_returns_error_dict(self, monkeypatch):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

        job = {"id": "job-9", "lead_id": "lead-9", "workspace_id": "aerys"}
        with patch.object(eq, "claim_next_enrichment_job", new=AsyncMock(return_value=job)), \
             patch.object(
                eq, "run_enrichment_for_lead",
                new=AsyncMock(side_effect=RuntimeError("boom")),
             ), patch.object(eq, "_get_warmr_client_for_worker", return_value=MagicMock()):
            result = await eq.process_next_enrichment("aerys", MagicMock())

        assert result["processed"] is False
        assert result["lead_id"] == "lead-9"
        assert "run_failed" in result["error"]
