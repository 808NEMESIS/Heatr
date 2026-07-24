"""
tests/test_website_analysis_receptie_wiring.py — bewijst dat de website-analyse-
worker de receptie-detectie ÉCHT aanroept in de productie-flow (audit-bevinding:
er was niets dat hook_* schreef). Drijft process_next_website_analysis met mocks.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

def _run(coro):
    """Private event-loop: raakt de gedeelde pytest-asyncio-loop niet aan."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


import job_queue.website_analysis_queue as wq


def test_worker_calls_run_receptie_for_lead(monkeypatch):
    calls = {}

    async def _fake_find(workspace_id, supabase):
        return {"id": 7, "domain": "kliniek.nl", "sector": "cosmetische_behandelaars",
                "company_name": "Kliniek", "workspace_id": workspace_id}

    async def _fake_is_real(domain):
        return True, "ok"

    async def _fake_analyze(**kw):
        return {"total_score": 55}

    async def _fake_receptie(supabase, lead, **kw):
        calls["lead"] = lead
        return {"hook_code": "Q4", "hook_ladder": ["Q4", "Q7"], "second_hook": "Q7"}

    monkeypatch.setattr(wq, "_find_next_eligible_lead", _fake_find)
    monkeypatch.setattr(wq, "_get_anthropic_client", lambda: object())
    import enrichment.website_prescreener as pre
    monkeypatch.setattr(pre, "is_real_website", _fake_is_real)
    import website_intelligence.analyzer as an
    monkeypatch.setattr(an, "analyze_website", _fake_analyze)
    import website_intelligence.hook_writer as hw
    monkeypatch.setattr(hw, "run_receptie_for_lead", _fake_receptie)

    out = _run(
        wq.process_next_website_analysis("aerys", supabase_client=object()))

    assert out["processed"] is True
    # de worker heeft de receptie-detectie voor DEZE lead aangeroepen
    assert calls.get("lead", {}).get("id") == 7
    assert calls["lead"]["domain"] == "kliniek.nl"
    assert calls["lead"]["workspace_id"] == "aerys"


def test_worker_survives_receptie_failure(monkeypatch):
    # receptie-detectie die crasht mag de analyse-uitkomst niet kapotmaken.
    async def _fake_find(workspace_id, supabase):
        return {"id": 8, "domain": "x.nl", "sector": "cosmetische_behandelaars",
                "company_name": "X", "workspace_id": workspace_id}

    async def _fake_is_real(domain):
        return True, "ok"

    async def _fake_analyze(**kw):
        return {"total_score": 40}

    async def _boom(*a, **kw):
        raise RuntimeError("detect kapot")

    monkeypatch.setattr(wq, "_find_next_eligible_lead", _fake_find)
    monkeypatch.setattr(wq, "_get_anthropic_client", lambda: object())
    import enrichment.website_prescreener as pre
    monkeypatch.setattr(pre, "is_real_website", _fake_is_real)
    import website_intelligence.analyzer as an
    monkeypatch.setattr(an, "analyze_website", _fake_analyze)
    import website_intelligence.hook_writer as hw
    monkeypatch.setattr(hw, "run_receptie_for_lead", _boom)

    out = _run(
        wq.process_next_website_analysis("aerys", supabase_client=object()))

    assert out["processed"] is True and out["total_score"] == 40
