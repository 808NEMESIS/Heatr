# CHANGES — Runtime-blockers fix

**Datum:** 2026-04-21
**Scope:** 2 runtime-blockers + 1 gerelateerde import-blocker uit `HEATR_AUDIT.md`.
**Doel:** endpoints `/leads/{id}/send-review-email`, `/website-intelligence/process-next` en `/enrichment/process-next` crashten met `ImportError` omdat 3 modules/functies ontbraken. Deze PR implementeert ze minimaal zonder refactors.

---

## Toegevoegd

### `campaigns/review_email_generator.py` (nieuw)
Entry point: `async def generate_review_email(lead, website_intelligence, anthropic_client=None, supabase_client=None) -> dict`.

- Rule-based `_derive_top_issue()` + `_compose_specific_observation()` voor concrete pijnpunt per lead.
- Claude Haiku call (`claude-haiku-4-5-20251001`) via AsyncAnthropic voor de 90-woorden email, prompt volgens CLAUDE.md regel 273-290.
- Post-validatie: body die met 'Ik' begint wordt lichtgewicht gecorrigeerd.
- Cost tracking naar `heatr_api_cost_log` (injected supabase client, optioneel).
- **Never-raise** contract — altijd dict terug, bij fout `{"error": "..."}`.

### `job_queue/website_analysis_queue.py` (nieuw)
Entry point: `async def process_next_website_analysis(workspace_id, supabase_client) -> dict | None`.

- Eligibility: `workspace_id` match, `domain NOT NULL`, `email_status IN (valid/risky/catch_all/catchall_risky)`, `status NIET IN (disqualified/unsubscribed/forgotten/bounced)`.
- Dedup tegen `heatr_website_intelligence` binnen 30-dagen venster (env: `WEBSITE_REANALYSIS_DAYS`).
- Draait `is_real_website()` prescreen → `analyze_website()`.
- Return `None` bij lege queue, dict bij success/failure. **Nooit raisen.**
- Failure logging via `_mark_failure()` naar nieuwe kolommen op `heatr_leads`.

### `migrations/005_website_analysis_failed_reason.sql` (nieuw)
2 optionele kolommen op `heatr_leads`:
- `website_analysis_failed_reason text`
- `website_analysis_failed_at timestamptz`

Idempotent (`ADD COLUMN IF NOT EXISTS`). Niet-breaking — bestaande code leest ze niet.

### `tests/test_review_email_generator.py` (nieuw)
14 tests: top-issue derivatie, incomplete lead guard, missing API key, happy path, 'Ik'-correctie, Claude exception, JSON parse fail, cost log insert.

### `tests/test_website_analysis_queue.py` (nieuw)
7 tests: lege queue, terminal-status filtering, WI-dedup, prescreen fail, happy path, analyze-raises, workspace_id isolatie.

### `tests/test_process_next_enrichment.py` (nieuw)
5 tests: lege queue, claim-raises, workspace mismatch, happy path, run-raises.

---

## Gewijzigd

### `job_queue/enrichment_queue.py`
Nieuwe export onderaan het bestand:
- `async def process_next_enrichment(workspace_id, supabase_client) -> dict | None` — wrapper rond `claim_next_enrichment_job` + `run_enrichment_for_lead`.
- Helpers: `_get_anthropic_client_for_worker()`, `_get_warmr_client_for_worker()`.
- `_NullWarmrClient` no-op stub voor omgevingen zonder `WARMR_API_URL` (enrichment steps die Warmr niet nodig hebben draaien door).

### `config/sectors.py`
Toegevoegd: `from __future__ import annotations` (Python 3.9 compat voor `str | None` syntax in `classify_subcategory()`). Geen content changes aan de sector-configs zelf — dat is scope voor PR2.

---

## Hoe te testen

```bash
python3 -m pytest tests/test_review_email_generator.py tests/test_website_analysis_queue.py tests/test_process_next_enrichment.py -v
# Expect: 26 passed
```

Import smoke test:
```bash
python3 -c "
from campaigns.review_email_generator import generate_review_email
from job_queue.website_analysis_queue import process_next_website_analysis
from job_queue.enrichment_queue import process_next_enrichment
import inspect
assert inspect.iscoroutinefunction(generate_review_email)
assert inspect.iscoroutinefunction(process_next_website_analysis)
assert inspect.iscoroutinefunction(process_next_enrichment)
print('All 3 blocker modules importable + async')
"
```

FastAPI app startup check:
```bash
python3 -c "from api.main import app; print('FastAPI loads:', len(app.routes))"
# Expect: FastAPI loads: 69
```

---

## Te draaien migration

```bash
# Supabase dashboard → SQL editor → paste + run:
cat migrations/005_website_analysis_failed_reason.sql
```

Optioneel. De queue werkt ook zonder (het `_mark_failure()` pad vangt DB-errors silent op).

---

## Verschil met CLAUDE.md

Geen inhoudelijke afwijking. CLAUDE.md regel 273-290 (review email prompt spec) en regel 115 (website analysis aparte queue) zijn 1-op-1 gevolgd. CLAUDE.md documentatie-sync is scope P3, aparte sessie per audit.

---

## Impact na merge

- `/leads/{id}/send-review-email` werkt — Aerys kan eerste review emails sturen
- `/website-intelligence/process-next` werkt — n8n workflow 04-website-analysis-worker werkt
- `/enrichment/process-next` werkt — n8n enrichment worker-loop werkt
- Geen breaking changes voor bestaande callers
- Geen nieuwe dependencies

---

## Niet in deze PR

Zoals afgesproken in scope (`BLOCKERS_SCOPE.md`):
- Schema-consolidatie (P2)
- CLAUDE.md doc-sync (P3)
- Scoring-weights wire-up (P4)
- JWT auth refactor (P5)
- `config/sectors.py` schema-overhaul — separate PR (PR2)
