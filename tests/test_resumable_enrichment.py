"""
tests/test_resumable_enrichment.py — fase 4 PR 17 (audit v2 P1-3/scenario 9).

Valideert: eerder voltooide stappen worden geskipt bij een retry, de
voortgang wordt na elke stap gepersisteerd, en het per-lead-kostenplafond
overleeft een restart via de spent_eur-carryover.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from job_queue import enrichment_queue as eq


def _job(**overrides):
    base = {
        "id": "job-1", "lead_id": "lead-1", "workspace_id": "aerys",
        "enrichment_types": ["website", "owner_extract", "scoring"],
        "steps_completed": [], "spent_eur": 0,
    }
    base.update(overrides)
    return base


def _db():
    chain = MagicMock()
    for m in ("select", "eq", "gte", "update", "insert", "limit", "order", "maybe_single"):
        getattr(chain, m).return_value = chain
    chain.execute.return_value = MagicMock(data=[])
    db = MagicMock()
    db.table.return_value = chain
    return db


def _patches(run_step: AsyncMock):
    return (
        patch.object(eq, "_run_step", new=run_step),
        patch.object(eq, "_get_lead_field", new=AsyncMock(return_value="NL")),
        patch.object(eq, "_boost_job_priority", new=AsyncMock()),
    )


@pytest.mark.asyncio
async def test_completed_steps_skipped_on_retry(monkeypatch):
    """Crash op stap 3 → retry draait stap 1-2 NIET opnieuw (geen dubbele
    Claude-kosten)."""
    monkeypatch.delenv("COST_GUARD_DISABLED", raising=False)
    run_step = AsyncMock()
    p1, p2, p3 = _patches(run_step)
    with p1, p2, p3:
        await eq.run_enrichment_for_lead(
            job=_job(steps_completed=["website", "owner_extract"]),
            supabase_client=_db(), anthropic_client=MagicMock(),
            warmr_client=MagicMock(),
        )
    ran = [c.kwargs["step_name"] for c in run_step.await_args_list]
    assert ran == ["scoring"]  # alleen de nog-niet-voltooide stap


@pytest.mark.asyncio
async def test_progress_persisted_after_each_step(monkeypatch):
    monkeypatch.delenv("COST_GUARD_DISABLED", raising=False)
    db = _db()
    run_step = AsyncMock()
    p1, p2, p3 = _patches(run_step)
    with p1, p2, p3:
        await eq.run_enrichment_for_lead(
            job=_job(), supabase_client=db,
            anthropic_client=MagicMock(), warmr_client=MagicMock(),
        )
    updates = [c.args[0] for c in db.table.return_value.update.call_args_list
               if c.args and "steps_completed" in c.args[0]]
    assert len(updates) == 3  # één persist per voltooide stap
    assert updates[-1]["steps_completed"] == ["website", "owner_extract", "scoring"]
    assert "spent_eur" in updates[-1]


@pytest.mark.asyncio
async def test_failed_step_not_marked_completed(monkeypatch):
    """Een gefaalde stap wordt NIET als voltooid geregistreerd — de retry
    pakt precies die stap opnieuw op."""
    monkeypatch.delenv("COST_GUARD_DISABLED", raising=False)
    db = _db()

    async def boom(**kwargs):
        if kwargs["step_name"] == "owner_extract":
            raise RuntimeError("claude down")

    p1, p2, p3 = _patches(AsyncMock(side_effect=boom))
    with p1, p2, p3:
        await eq.run_enrichment_for_lead(
            job=_job(), supabase_client=db,
            anthropic_client=MagicMock(), warmr_client=MagicMock(),
        )
    updates = [c.args[0] for c in db.table.return_value.update.call_args_list
               if c.args and "steps_completed" in c.args[0]]
    assert updates[-1]["steps_completed"] == ["website", "scoring"]
    assert "owner_extract" not in updates[-1]["steps_completed"]


@pytest.mark.asyncio
async def test_hanging_step_times_out_and_pipeline_continues(monkeypatch):
    """Een stap die blijft hangen (trage website) wordt na de per-stap timeout
    afgekapt; de lead gaat door met de volgende stap i.p.v. de worker te
    blokkeren. De getimede stap komt NIET in steps_completed."""
    import asyncio as _asyncio
    monkeypatch.delenv("COST_GUARD_DISABLED", raising=False)
    monkeypatch.setattr(eq, "_STEP_TIMEOUT_SECONDS", 0.2)  # bij import gelezen → patch attribuut
    db = _db()

    async def slow(**kwargs):
        if kwargs["step_name"] == "owner_extract":
            await _asyncio.sleep(5)  # langer dan de timeout → moet afgekapt worden

    p1, p2, p3 = _patches(AsyncMock(side_effect=slow))
    with p1, p2, p3:
        # mag niet blokkeren — moet ruim binnen de sleep(5) klaar zijn
        await _asyncio.wait_for(
            eq.run_enrichment_for_lead(
                job=_job(), supabase_client=db,
                anthropic_client=MagicMock(), warmr_client=MagicMock(),
            ),
            timeout=3,
        )
    updates = [c.args[0] for c in db.table.return_value.update.call_args_list
               if c.args and "steps_completed" in c.args[0]]
    assert updates[-1]["steps_completed"] == ["website", "scoring"]
    assert "owner_extract" not in updates[-1]["steps_completed"]


@pytest.mark.asyncio
async def test_spent_carryover_blocks_over_budget_retry(monkeypatch):
    """Het per-lead-plafond overleeft de restart: een job die vóór de crash
    het budget al opbrandde, krijgt bij retry GEEN vers budget."""
    monkeypatch.delenv("COST_GUARD_DISABLED", raising=False)
    monkeypatch.setenv("MAX_COST_PER_LEAD_EUR", "0.05")
    run_step = AsyncMock()
    p1, p2, p3 = _patches(run_step)
    with p1, p2, p3:
        await eq.run_enrichment_for_lead(
            job=_job(spent_eur=0.20),  # 4x het plafond al besteed
            supabase_client=_db(), anthropic_client=MagicMock(),
            warmr_client=MagicMock(),
        )
    run_step.assert_not_awaited()  # alles geblokkeerd door carryover
