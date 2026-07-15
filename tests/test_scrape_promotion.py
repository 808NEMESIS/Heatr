"""
tests/test_scrape_promotion.py — discovery hands-off (2026-07-14).

Dekt de drie wijzigingen die scraping → leads automatisch maken:
  1. _promote_scraped_companies roept de bestaande qualifier aan over
     onverwerkte companies_raw-rijen (fail-soft, workspace/sector/stad-scoped);
  2. should_exit_when_idle: daemon-modus stopt niet bij lege queue;
  3. _store_enrichment_result schrijft geen niet-bestaande kolommen meer
     (email_verified/mx_records) naar heatr_enrichment_data.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ==============================================================================
# Chainbare fake-supabase
# ==============================================================================

class _Chain:
    def __init__(self, table, sink, rows):
        self._table, self._sink, self._rows = table, sink, rows
        self._op = None
        self._payload = None
        self._filters = {}

    def select(self, *a, **k):
        return self

    def insert(self, payload):
        self._op, self._payload = "insert", payload
        return self

    def eq(self, col, val):
        self._filters[col] = val
        return self

    def is_(self, col, val):
        self._filters[f"is:{col}"] = val
        return self

    def limit(self, *a, **k):
        return self

    def execute(self):
        if self._op == "insert":
            self._sink.setdefault(self._table, []).append(self._payload)

        class _R:
            pass
        r = _R()
        r.data = self._rows.get(self._table, []) if self._op is None else []
        return r


class _FakeDB:
    def __init__(self, rows=None):
        self.sink: dict = {}
        self._rows = rows or {}
        self.chains: list = []

    def table(self, name):
        c = _Chain(name, self.sink, self._rows)
        self.chains.append(c)
        return c


# ==============================================================================
# 1. _promote_scraped_companies
# ==============================================================================

_RAW = [
    {"id": "c1", "company_name": "Kliniek A", "domain": "a.nl", "sector": "cosmetische_behandelaars", "city": "Amsterdam"},
    {"id": "c2", "company_name": "Kliniek B", "domain": "b.nl", "sector": "cosmetische_behandelaars", "city": "Amsterdam"},
]


class TestPromoteScrapedCompanies:
    @pytest.mark.asyncio
    async def test_calls_qualifier_per_row_and_filters_null(self, monkeypatch):
        from job_queue import scraping_queue as sq
        import enrichment.lead_qualifier as lq

        calls = []

        async def fake_qcl(raw, sector, ws, sb):
            calls.append(raw["id"])
            return {"id": f"lead-{raw['id']}"}  # promoted

        monkeypatch.setattr(lq, "qualify_and_create_lead", fake_qcl)
        db = _FakeDB(rows={"companies_raw": _RAW})
        await sq._promote_scraped_companies("cosmetische_behandelaars", "Amsterdam", "aerys", db)

        assert calls == ["c1", "c2"]
        # de fetch filtert op qualification_status IS NULL + workspace + sector + stad
        fetch = db.chains[0]
        assert fetch._filters.get("is:qualification_status") == "null"
        assert fetch._filters.get("workspace_id") == "aerys"
        assert fetch._filters.get("sector") == "cosmetische_behandelaars"
        assert fetch._filters.get("city") == "Amsterdam"

    @pytest.mark.asyncio
    async def test_failsoft_per_row(self, monkeypatch):
        """Eén rij die faalt stopt de rest niet."""
        from job_queue import scraping_queue as sq
        import enrichment.lead_qualifier as lq

        seen = []

        async def flaky(raw, sector, ws, sb):
            seen.append(raw["id"])
            if raw["id"] == "c1":
                raise RuntimeError("boom")
            return {"id": "lead-c2"}

        monkeypatch.setattr(lq, "qualify_and_create_lead", flaky)
        db = _FakeDB(rows={"companies_raw": _RAW})
        # mag niet raisen
        await sq._promote_scraped_companies("cosmetische_behandelaars", "Amsterdam", "aerys", db)
        assert seen == ["c1", "c2"]

    @pytest.mark.asyncio
    async def test_failsoft_on_fetch_error(self, monkeypatch):
        """Een DB-fout op de fetch mag niet doorslaan naar de scrape-job."""
        from job_queue import scraping_queue as sq

        class _BoomDB:
            def table(self, name):
                raise RuntimeError("db weg")

        # mag niet raisen
        await sq._promote_scraped_companies("s", "c", "aerys", _BoomDB())


# ==============================================================================
# 2. should_exit_when_idle (daemon-vlag)
# ==============================================================================

class TestDaemonExit:
    def test_non_daemon_exits_after_threshold(self):
        from scripts.run_enrichment_worker import should_exit_when_idle
        assert should_exit_when_idle(3, daemon=False) is True
        assert should_exit_when_idle(2, daemon=False) is False

    def test_daemon_never_exits(self):
        from scripts.run_enrichment_worker import should_exit_when_idle
        assert should_exit_when_idle(3, daemon=True) is False
        assert should_exit_when_idle(999, daemon=True) is False

    def test_website_worker_daemon_flag(self):
        from scripts.run_website_worker import should_exit_when_idle
        assert should_exit_when_idle(3, daemon=False) is True
        assert should_exit_when_idle(3, daemon=True) is False


# ==============================================================================
# website_intelligence ontkoppeld van inline enrichment (2026-07-15)
# ==============================================================================

class TestWebsiteIntelligenceDecoupled:
    @pytest.mark.asyncio
    async def test_default_enrichment_types_excludes_website_intelligence(self):
        """De zware Vision-analyse mag niet meer in het inline kern-pad zitten
        (blokkeerde de worker) — draait nu in de website_analysis_queue."""
        from job_queue import enrichment_queue as eq
        db = _FakeDB(rows={})
        # vang het gequeuede record
        captured = {}

        class _Cap:
            def insert(self, rec):
                captured["rec"] = rec
                return self

            def execute(self):
                class _R:
                    data = [{"id": "job-x"}]
                return _R()

            def table(self, *a):
                return self

        class _CapDB:
            def table(self, name):
                return _Cap()

        await eq.queue_lead_for_enrichment(
            lead_id="L1", workspace_id="aerys", supabase_client=_CapDB(),
        )
        types = captured["rec"]["enrichment_types"]
        assert "website_intelligence" not in types
        # de kern-stappen blijven wél inline
        for core in ("website", "email_waterfall", "scoring", "inbox_selection"):
            assert core in types


# ==============================================================================
# 3. _store_enrichment_result — geen niet-bestaande kolommen
# ==============================================================================

class TestEnrichmentDataRecord:
    @pytest.mark.asyncio
    async def test_record_omits_nonexistent_columns(self):
        from scrapers.website_scraper import _store_enrichment_result
        db = _FakeDB()
        await _store_enrichment_result(
            lead_id="L1", workspace_id="aerys", step=1, source="website",
            emails_found=["info@a.nl"], raw_result={"x": 1}, succeeded=True,
            supabase_client=db,
        )
        rec = db.sink["enrichment_data"][0]
        assert "email_verified" not in rec
        assert "mx_records" not in rec
        # de geldige velden staan er nog
        for f in ("workspace_id", "lead_id", "step", "source", "email_candidate",
                  "email_status", "catch_all", "raw_result", "succeeded"):
            assert f in rec
        assert rec["email_candidate"] == "info@a.nl"
