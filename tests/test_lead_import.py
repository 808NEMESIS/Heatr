"""
tests/test_lead_import.py — CSV import + dedup-pipeline.

Run: pytest tests/test_lead_import.py -v
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from utils.lead_import import (
    _normalize_domain,
    _normalize_email,
    _normalize_company_city,
    _row_dedup_match,
    import_leads,
)


def _mock_db(existing_leads: list[dict], inserted: list | None = None):
    """Mock supabase client met bestaande leads + insert tracking."""
    inserted = inserted if inserted is not None else []
    db = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    select_result = MagicMock()
    select_result.data = existing_leads
    chain.execute.return_value = select_result

    def insert_side_effect(payload):
        inserted.append(payload)
        ins = MagicMock()
        ins.execute.return_value = MagicMock(data=[{**payload, "id": f"new-{len(inserted)}"}])
        return ins

    chain.insert.side_effect = insert_side_effect
    db.table.return_value = chain
    return db, inserted


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def test_normalize_domain_strips_protocol_and_www():
    assert _normalize_domain("https://www.example.nl/") == "example.nl"
    assert _normalize_domain("HTTP://Example.NL") == "example.nl"
    assert _normalize_domain("example.nl") == "example.nl"
    assert _normalize_domain(None) is None
    assert _normalize_domain("") is None


def test_normalize_email_lowercases_and_validates():
    assert _normalize_email("Sami@Aerys.NL") == "sami@aerys.nl"
    assert _normalize_email("notanemail") is None
    assert _normalize_email("") is None


def test_normalize_company_strips_legal_suffix_and_lowercases():
    assert _normalize_company_city("Praktijk Mark BV", "Utrecht") == "praktijk mark|utrecht"
    assert _normalize_company_city("Skin Studio B.V.", "Amsterdam") == "skin studio|amsterdam"
    assert _normalize_company_city("Skin Studio", None) == "skin studio"
    assert _normalize_company_city(None, "Utrecht") is None


# ---------------------------------------------------------------------------
# Dedup matching
# ---------------------------------------------------------------------------

def test_dedup_email_match_wins_first():
    existing = [
        {"id": "a", "email": "info@kliniek.nl", "domain": "kliniek.nl", "company_name": "Kliniek X", "city": "Utrecht"},
    ]
    matched, mtype = _row_dedup_match({"email": "INFO@kliniek.nl"}, existing)
    assert matched["id"] == "a"
    assert mtype == "email"


def test_dedup_domain_match_when_no_email():
    existing = [{"id": "a", "domain": "kliniek.nl"}]
    matched, mtype = _row_dedup_match({"domain": "https://www.kliniek.nl/"}, existing)
    assert matched["id"] == "a"
    assert mtype == "domain"


def test_dedup_kvk_match():
    existing = [{"id": "a", "kvk_number": "12345678"}]
    matched, mtype = _row_dedup_match({"kvk_number": "12345678"}, existing)
    assert matched["id"] == "a"
    assert mtype == "kvk"


def test_dedup_fuzzy_company_city():
    existing = [{"id": "a", "company_name": "Praktijk Mark", "city": "Utrecht"}]
    matched, mtype = _row_dedup_match(
        {"company_name": "Praktijk Mark BV", "city": "Utrecht"}, existing,
    )
    assert matched["id"] == "a"
    assert mtype == "fuzzy_company_city"


def test_dedup_no_match_for_unrelated_company():
    existing = [{"id": "a", "company_name": "Praktijk Mark", "city": "Utrecht"}]
    matched, _ = _row_dedup_match(
        {"company_name": "Salon Lisa", "city": "Amsterdam"}, existing,
    )
    assert matched is None


def test_dedup_returns_none_for_empty_row():
    matched, _ = _row_dedup_match({}, [{"id": "a", "company_name": "X"}])
    assert matched is None


# ---------------------------------------------------------------------------
# Full import flow
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_import_dry_run_does_not_insert():
    db, inserted = _mock_db([])
    rows = [{"company_name": "Nieuw Bedrijf", "domain": "nieuw.nl", "email": "info@nieuw.nl"}]
    result = await import_leads(rows, "aerys", db, dry_run=True, auto_enrich=False)
    assert len(result["imported"]) == 1
    assert result["imported"][0]["would_insert"] is True
    assert inserted == []  # geen inserts


@pytest.mark.asyncio
async def test_import_separates_imported_duplicates_errors():
    existing = [{"id": "ex1", "email": "info@bestaand.nl", "domain": "bestaand.nl", "company_name": "Bestaand"}]
    db, _ = _mock_db(existing)
    rows = [
        {"company_name": "Nieuw", "domain": "nieuw.nl", "email": "info@nieuw.nl"},  # imported
        {"company_name": "Bestaand", "email": "info@bestaand.nl"},                   # duplicate (email)
        {"city": "Utrecht"},                                                          # error (geen match-velden)
    ]
    result = await import_leads(rows, "aerys", db, auto_enrich=False)
    assert len(result["imported"]) == 1
    assert len(result["duplicates"]) == 1
    assert result["duplicates"][0]["match_type"] == "email"
    assert len(result["errors"]) == 1
    assert "mist alle dedup-velden" in result["errors"][0]["error"]


@pytest.mark.asyncio
async def test_import_caps_at_500_rows():
    db, _ = _mock_db([])
    rows = [{"email": f"x{i}@a.nl"} for i in range(501)]
    result = await import_leads(rows, "aerys", db, auto_enrich=False)
    assert result["fatal"] is not None
    assert "500" in result["fatal"]


@pytest.mark.asyncio
async def test_import_dedup_within_same_batch():
    """2x dezelfde email in 1 batch → 1 import, 1 duplicate."""
    db, _ = _mock_db([])
    rows = [
        {"company_name": "X", "email": "same@a.nl", "domain": "x.nl"},
        {"company_name": "X dupe", "email": "same@a.nl", "domain": "x.nl"},
    ]
    result = await import_leads(rows, "aerys", db, auto_enrich=False)
    assert len(result["imported"]) == 1
    assert len(result["duplicates"]) == 1
    assert result["duplicates"][0]["match_type"] == "email"


@pytest.mark.asyncio
async def test_import_auto_enrich_calls_queue_for_each_imported():
    """Imported leads moeten op de enrichment-queue belanden (niet duplicaten/errors)."""
    db, _ = _mock_db([])
    rows = [
        {"company_name": "A", "email": "a@a.nl", "domain": "a.nl"},
        {"company_name": "B", "email": "b@b.nl", "domain": "b.nl"},
    ]
    # Patch queue_lead_for_enrichment om calls te tracken
    from unittest.mock import AsyncMock, patch
    with patch("job_queue.enrichment_queue.queue_lead_for_enrichment", new_callable=AsyncMock) as mock_queue:
        mock_queue.side_effect = lambda **kw: f"job-{kw['lead_id']}"
        result = await import_leads(rows, "aerys", db, auto_enrich=True)
    assert mock_queue.call_count == 2
    assert all(e["enrichment_queued"] for e in result["imported"])
    assert result["summary"]["enrichment_queued_count"] == 2


@pytest.mark.asyncio
async def test_import_auto_enrich_disabled_skips_queue():
    db, _ = _mock_db([])
    rows = [{"company_name": "X", "email": "x@x.nl", "domain": "x.nl"}]
    from unittest.mock import AsyncMock, patch
    with patch("job_queue.enrichment_queue.queue_lead_for_enrichment", new_callable=AsyncMock) as mock_queue:
        result = await import_leads(rows, "aerys", db, auto_enrich=False)
    mock_queue.assert_not_called()
    assert result["summary"]["enrichment_queued_count"] == 0
    assert result["imported"][0]["enrichment_queued"] is False


@pytest.mark.asyncio
async def test_import_dry_run_skips_queue_even_when_auto_enrich_true():
    """Dry-run mag NOOIT iets schrijven, ook geen queue-jobs."""
    db, _ = _mock_db([])
    rows = [{"company_name": "X", "email": "x@x.nl", "domain": "x.nl"}]
    from unittest.mock import AsyncMock, patch
    with patch("job_queue.enrichment_queue.queue_lead_for_enrichment", new_callable=AsyncMock) as mock_queue:
        result = await import_leads(rows, "aerys", db, dry_run=True, auto_enrich=True)
    mock_queue.assert_not_called()
    assert result["summary"]["enrichment_queued_count"] == 0


# ---------------------------------------------------------------------------
# Cost-preview
# ---------------------------------------------------------------------------

def test_estimate_import_cost_scales_linearly():
    from utils.lead_import import ESTIMATED_ENRICHMENT_COST_EUR, estimate_import_cost
    est = estimate_import_cost(100)
    assert est["row_count"] == 100
    assert est["cost_per_lead_eur"] == ESTIMATED_ENRICHMENT_COST_EUR
    assert est["estimated_total_eur"] == round(100 * ESTIMATED_ENRICHMENT_COST_EUR, 4)


def test_estimate_import_cost_zero_rows():
    from utils.lead_import import estimate_import_cost
    assert estimate_import_cost(0)["estimated_total_eur"] == 0


# ---------------------------------------------------------------------------
# Error aggregation — fatal bij ≥10% queue failures
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_import_fatal_when_queue_failure_rate_above_threshold():
    """≥10% queue-failures bij auto-enrich → top-level fatal, geen silent."""
    db, _ = _mock_db([])
    rows = [{"company_name": f"X{i}", "email": f"x{i}@a.nl"} for i in range(10)]
    from unittest.mock import AsyncMock, patch
    # Maak 5 van 10 queue-calls fail → 50% failure rate
    call_count = {"n": 0}
    async def flaky_queue(**kw):
        call_count["n"] += 1
        if call_count["n"] % 2 == 0:
            raise Exception("queue table missing")
        return f"job-{call_count['n']}"
    with patch("job_queue.enrichment_queue.queue_lead_for_enrichment", side_effect=flaky_queue):
        result = await import_leads(rows, "aerys", db, auto_enrich=True)
    assert result["fatal"] is not None
    assert "queue" in result["fatal"].lower()
    assert result["summary"]["enrichment_queue_failures"] >= 1


@pytest.mark.asyncio
async def test_import_no_fatal_when_queue_failure_rate_below_threshold():
    """<10% queue-failures = warning, geen fatal."""
    db, _ = _mock_db([])
    rows = [{"company_name": f"X{i}", "email": f"x{i}@a.nl"} for i in range(20)]
    from unittest.mock import AsyncMock, patch
    call_count = {"n": 0}
    async def flaky_queue(**kw):
        call_count["n"] += 1
        if call_count["n"] == 1:  # alleen 1/20 fail = 5%
            raise Exception("transient")
        return f"job-{call_count['n']}"
    with patch("job_queue.enrichment_queue.queue_lead_for_enrichment", side_effect=flaky_queue):
        result = await import_leads(rows, "aerys", db, auto_enrich=True)
    assert result["fatal"] is None
    assert result["summary"]["enrichment_queue_failures"] == 1


# ---------------------------------------------------------------------------
# Merge-strategieën
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_merge_skip_default_does_not_update_existing():
    existing = [{"id": "ex1", "company_name": "Bestaand", "email": "info@be.nl", "phone": None}]
    db, _ = _mock_db(existing)
    rows = [{"company_name": "Bestaand", "email": "info@be.nl", "phone": "030-12345"}]
    result = await import_leads(rows, "aerys", db, auto_enrich=False, merge_strategy="skip")
    assert len(result["duplicates"]) == 1
    assert result["duplicates"][0]["merge_action"] == "skipped"


@pytest.mark.asyncio
async def test_merge_fill_blanks_only_fills_empty_fields():
    """Fill_blanks vult ontbrekende velden, raakt bestaande niet aan."""
    existing = [{
        "id": "ex1", "company_name": "Bestaand",
        "email": "info@be.nl", "phone": None, "city": "Utrecht",
    }]
    db, inserted = _mock_db(existing)
    rows = [{
        "company_name": "Bestaand", "email": "info@be.nl",
        "phone": "030-12345",   # vult lege phone
        "city": "Amsterdam",     # botst — wordt NIET overschreven (already filled)
    }]
    result = await import_leads(rows, "aerys", db, auto_enrich=False, merge_strategy="fill_blanks")
    dup = result["duplicates"][0]
    assert dup["merge_action"] == "fill_blanks"
    assert "phone" in dup["merged_fields"]
    assert "city" not in dup["merged_fields"]   # already filled, niet overschreven


@pytest.mark.asyncio
async def test_merge_overwrite_replaces_all_provided_non_null():
    existing = [{
        "id": "ex1", "company_name": "Bestaand",
        "email": "info@be.nl", "city": "Utrecht",
    }]
    db, _ = _mock_db(existing)
    rows = [{
        "company_name": "Bestaand", "email": "info@be.nl",
        "city": "Amsterdam",  # overschrijft Utrecht
    }]
    result = await import_leads(rows, "aerys", db, auto_enrich=False, merge_strategy="overwrite")
    assert result["duplicates"][0]["merge_action"] == "overwrite"
    assert "city" in result["duplicates"][0]["merged_fields"]


@pytest.mark.asyncio
async def test_merge_invalid_strategy_returns_fatal():
    db, _ = _mock_db([])
    result = await import_leads([{"email": "x@x.nl"}], "aerys", db, merge_strategy="onzin")
    assert result["fatal"] is not None
    assert "merge_strategy" in result["fatal"]


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# #4 — Protected fields blacklist
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_overwrite_does_not_clobber_score():
    """Overwrite-strategy mag NOOIT score, archetype etc. overschrijven —
    ook al heeft de CSV-row die kolom staan."""
    existing = [{
        "id": "ex1", "company_name": "X", "email": "x@x.nl",
        "score": 78, "archetype": "lichaamswerk_pragmatisch",
        "personalization_potential": 12,
    }]
    db, _ = _mock_db(existing)
    rows = [{
        "company_name": "X", "email": "x@x.nl",
        "score": 0,                       # zou bestaande 78 wegvagen
        "archetype": "onzin",             # zou bestaande archetype overschrijven
        "city": "Utrecht",                # mag wel
    }]
    result = await import_leads(rows, "aerys", db, auto_enrich=False, merge_strategy="overwrite")
    dup = result["duplicates"][0]
    assert "score" not in dup.get("merged_fields", [])
    assert "archetype" not in dup.get("merged_fields", [])
    assert "city" in dup.get("merged_fields", [])


@pytest.mark.asyncio
async def test_fill_blanks_does_not_clobber_score_when_existing_score_zero():
    """Edge: bestaande score=0 moet NIET worden vervangen (geprotect)."""
    existing = [{
        "id": "ex1", "company_name": "X", "email": "x@x.nl", "score": 0,
    }]
    db, _ = _mock_db(existing)
    rows = [{"company_name": "X", "email": "x@x.nl", "score": 99}]
    result = await import_leads(rows, "aerys", db, auto_enrich=False, merge_strategy="fill_blanks")
    assert "score" not in result["duplicates"][0].get("merged_fields", [])


# ---------------------------------------------------------------------------
# #3 — Strip-aware blanks (whitespace-only = blank)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fill_blanks_treats_whitespace_as_blank():
    """Bestaand veld met `"   "` of `"\\t"` moet als blank tellen → fill toegestaan."""
    existing = [{
        "id": "ex1", "company_name": "X", "email": "x@x.nl",
        "phone": "   ",  # whitespace-only
        "city": "\t\t",  # tabs
    }]
    db, _ = _mock_db(existing)
    rows = [{"company_name": "X", "email": "x@x.nl", "phone": "030-1234", "city": "Utrecht"}]
    result = await import_leads(rows, "aerys", db, auto_enrich=False, merge_strategy="fill_blanks")
    fields = result["duplicates"][0]["merged_fields"]
    assert "phone" in fields
    assert "city" in fields


def test_is_blank_helper():
    from utils.lead_import import _is_blank
    assert _is_blank(None) is True
    assert _is_blank("") is True
    assert _is_blank("   ") is True
    assert _is_blank("\t\n") is True
    assert _is_blank("Utrecht") is False
    assert _is_blank(0) is False    # number 0 is NOT blank
    assert _is_blank(False) is False


# ---------------------------------------------------------------------------
# #8 — Slim cache storage + cross-user replay safety
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_idempotency_cache_stores_slim_result_no_full_arrays():
    """import_runs.result mag GEEN imported[] of duplicates[] arrays bevatten —
    alleen summary, counts, lead_ids. Voorkomt 360MB DB-bloat."""
    db, _ = _mock_db([])
    rows = [{"company_name": f"X{i}", "email": f"x{i}@a.nl"} for i in range(3)]

    # Vang upsert calls om te checken wat we opslaan
    upsert_payloads: list[dict] = []
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.maybe_single.return_value = chain
    miss = MagicMock()
    miss.data = None
    chain.execute.return_value = miss

    def fake_insert(payload):
        ins = MagicMock()
        ins.execute.return_value = MagicMock(data=[{**payload, "id": f"new-{len(upsert_payloads)}"}])
        return ins

    def fake_upsert(payload, on_conflict=None):
        upsert_payloads.append(payload)
        u = MagicMock()
        u.execute.return_value = MagicMock(data=[payload])
        return u

    chain.insert.side_effect = fake_insert
    chain.upsert.side_effect = fake_upsert
    db = MagicMock()
    db.table.return_value = chain

    from unittest.mock import AsyncMock, patch
    with patch("job_queue.enrichment_queue.queue_lead_for_enrichment", new_callable=AsyncMock):
        await import_leads(rows, "aerys", db, import_run_id="run-abc", auto_enrich=False)

    # Vind de import_runs upsert
    runs_payload = next((p for p in upsert_payloads if "started_at" in p), None)
    assert runs_payload is not None
    cached = runs_payload["result"]
    # Slim cache mag deze keys NIET hebben
    assert "imported" not in cached
    assert "duplicates" not in cached
    # Wel deze
    assert "summary" in cached
    assert "imported_lead_ids" in cached
    assert isinstance(cached["imported_lead_ids"], list)


@pytest.mark.asyncio
async def test_idempotency_cross_user_replay_refused():
    """User-B die zelfde import_run_id gebruikt als user-A → geen replay."""
    cached_idem_data = MagicMock()
    cached_idem_data.data = {
        "result": {"summary": {"imported_count": 5}, "fatal": None, "imported_lead_ids": ["a"]},
        "completed_at": "2026-04-29T10:00:00+00:00",
        "imported_by": "user:alice@x.nl",
    }
    # Tweede execute() (= existing-leads SELECT) returnt lege lijst
    empty_leads = MagicMock()
    empty_leads.data = []
    db = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.maybe_single.return_value = chain
    chain.execute.side_effect = [cached_idem_data, empty_leads, MagicMock(data=[{"id": "new-1", "company_name": "X"}])]
    chain.insert.return_value = chain
    chain.upsert.return_value = chain
    db.table.return_value = chain

    # Bob met dezelfde id — moet NIET cached resultaat krijgen, maar fresh import
    result = await import_leads(
        [{"email": "fresh@new.nl"}],
        "aerys", db, import_run_id="shared-id",
        imported_by="user:bob@x.nl", auto_enrich=False,
    )
    # Niet als idempotent_replay teruggegeven
    assert result.get("idempotent_replay") is not True


@pytest.mark.asyncio
async def test_idempotency_same_user_replay_allowed():
    """Same principal met cached id → replay geleverd."""
    cached_data = {
        "summary": {"imported_count": 1},
        "fatal": None,
        "imported_lead_ids": ["existing-id"],
    }
    db = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.maybe_single.return_value = chain
    hit = MagicMock()
    hit.data = {
        "result": cached_data,
        "completed_at": "2026-04-29T10:00:00+00:00",
        "imported_by": "user:alice@x.nl",
    }
    chain.execute.return_value = hit
    chain.insert.return_value = chain
    chain.upsert.return_value = chain
    db.table.return_value = chain

    result = await import_leads(
        [{"email": "fresh@new.nl"}],
        "aerys", db, import_run_id="shared-id",
        imported_by="user:alice@x.nl", auto_enrich=False,
    )
    assert result["idempotent_replay"] is True
    assert result["cached_imported_lead_ids"] == ["existing-id"]


@pytest.mark.asyncio
async def test_idempotency_replay_returns_cached_result():
    """Tweede call met zelfde import_run_id returnt cached resultaat ipv opnieuw inserten."""
    cached_result = {
        "imported": [{"row_index": 0, "lead_id": "abc"}],
        "duplicates": [], "errors": [], "fatal": None,
        "summary": {"total_rows": 1, "imported_count": 1, "duplicate_count": 0,
                    "error_count": 0, "enrichment_queued_count": 1, "auto_enrich": True,
                    "merge_strategy": "skip"},
    }
    # Mock met cached row in import_runs
    db = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.maybe_single.return_value = chain
    hit = MagicMock()
    hit.data = {"result": cached_result, "completed_at": "2026-04-29T10:00:00+00:00"}
    chain.execute.return_value = hit
    chain.insert.return_value = chain
    chain.upsert.return_value = chain
    db.table.return_value = chain

    result = await import_leads(
        [{"email": "new@new.nl"}],  # Andere data dan eerste run
        "aerys", db, import_run_id="run-xyz", auto_enrich=False,
    )
    assert result["idempotent_replay"] is True
    assert result["summary"]["imported_count"] == 1   # van cached, niet nieuwe inserts
