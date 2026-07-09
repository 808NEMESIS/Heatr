"""
tests/test_company_enrichment_cost.py — Recovery Patch 5b.

Bewijst dat company_enrichment's Claude-calls (industry/summary/opener) hun
kosten registreren (log_api_cost + accumulator.charge). Vóór de fix omzeilden
deze 3 calls ALLE kostentracking → onzichtbaar voor de caps en /analytics.
"""
from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import utils.claude_cache as claude_cache
from utils.cost_guard import LeadCostAccumulator
import enrichment.company_enrichment as ce


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class _FakeAnthropic:
    """messages.create geeft een response met content + usage."""
    def __init__(self, text="Kliniek X levert huidtherapie in Utrecht."):
        self._text = text
        self.messages = self

    async def create(self, **kwargs):
        return types.SimpleNamespace(
            content=[types.SimpleNamespace(text=self._text)],
            usage=types.SimpleNamespace(input_tokens=400, output_tokens=60),
        )


def _cost_ctx(acc):
    return {"workspace_id": "aerys", "lead_id": "L1", "supabase_client": object(), "accumulator": acc}


def _patch_log(monkeypatch):
    calls = []

    async def _fake_log(**kw):
        calls.append(kw)

    monkeypatch.setattr(claude_cache, "log_api_cost", _fake_log)
    return calls


def test_summary_registers_cost(monkeypatch):
    calls = _patch_log(monkeypatch)
    acc = LeadCostAccumulator(lead_id="L1", workspace_id="aerys", ceiling_eur=1.0)
    out = _run(ce.generate_company_summary(
        company_name="Kliniek X", industry="Huidtherapiepraktijk", city="Utrecht",
        website_text="", anthropic_client=_FakeAnthropic(), cost_ctx=_cost_ctx(acc)))
    assert out  # kreeg tekst
    assert acc.spent_eur > 0, "accumulator niet gecharged"
    assert len(calls) == 1 and calls[0]["cost_eur"] > 0
    assert calls[0]["context"] == "company_enrichment:summary"


def test_opener_registers_cost(monkeypatch):
    calls = _patch_log(monkeypatch)
    acc = LeadCostAccumulator(lead_id="L1", workspace_id="aerys", ceiling_eur=1.0)
    _run(ce.generate_personalized_opener(
        company_name="Kliniek X", city="Utrecht", industry="Huidtherapie",
        contact_name=None, summary="", has_instagram=True, google_rating=4.8,
        google_review_count=40, sector_key="cosmetische_behandelaars",
        language="nl", anthropic_client=_FakeAnthropic(), cost_ctx=_cost_ctx(acc)))
    assert acc.spent_eur > 0
    assert calls and calls[0]["context"] == "company_enrichment:opener"


def test_industry_registers_cost(monkeypatch):
    calls = _patch_log(monkeypatch)
    acc = LeadCostAccumulator(lead_id="L1", workspace_id="aerys", ceiling_eur=1.0)
    _run(ce.infer_industry_claude(
        website_text="huidtherapie utrecht", google_category="Skin care clinic",
        sector_key="cosmetische_behandelaars", anthropic_client=_FakeAnthropic("Huidtherapiepraktijk"),
        cost_ctx=_cost_ctx(acc)))
    assert acc.spent_eur > 0
    assert calls and calls[0]["context"] == "company_enrichment:industry"


def test_no_cost_ctx_is_noop(monkeypatch):
    """Zonder cost_ctx (losse aanroep) geen crash, geen log."""
    calls = _patch_log(monkeypatch)
    out = _run(ce.generate_company_summary(
        company_name="Kliniek X", industry="Huidtherapie", city="Utrecht",
        website_text="", anthropic_client=_FakeAnthropic(), cost_ctx=None))
    assert out and calls == []
