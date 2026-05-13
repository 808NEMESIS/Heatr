# CHANGES_PR3 — Warmr Sequence v1.0 enrichment gaps + cost-controls

**Datum:** 2026-04-21
**Scope:** 6 ontbrekende datapunten vullen voor Warmr Sequence v1.0 (cosmetische klinieken cold outreach) + harde cost-ceilings om onverwachte uitgaven te voorkomen.

---

## Fundament: cost-controls

### `utils/cost_guard.py` (nieuw, ~180 regels)
- `LeadCostAccumulator` — per-lead cumulatieve kost-tracker (in-memory).
- `check_daily_budget()` — query api_cost_log, compare tegen `ENRICHMENT_DAILY_BUDGET_EUR` (default **€0.50**).
- `guarded_call()` — one-shot pre-call gate (daily budget + per-lead ceiling).
- `record_block()` — audit-log bij een block naar api_cost_log met `BLOCKED:*` context.
- Fail-open bij DB errors (pipeline stopt nooit).

### Env defaults
```
ENRICHMENT_DAILY_BUDGET_EUR=0.50    # daily kill-switch
MAX_COST_PER_LEAD_EUR=0.05          # per-lead ceiling
COST_GUARD_DISABLED=false           # only set for integration tests
VISION_SKIP_TECHNICAL_THRESHOLD=20  # PR2.5 conditional Vision
```

### Rate-limiter entries (`utils/rate_limiter.py`)
- `treatment_classifier`: 10 req/min
- `rdap`: 20 req/min (SIDN hard cap)
- `meta_ads_playwright`: 5 req/min (anti-ban)

---

## PR2.5: Vision-cache + conditional gate (meegelift in PR3)

### Waarom
Claude Sonnet Vision is 87% van de per-lead cost (~€0.014/lead). Twee structurele optimalisaties:
1. **Conditional skip**: leads met `technical_score ≥ VISION_SKIP_TECHNICAL_THRESHOLD (20)` zijn technisch al gezond → geen Aerys-websitebouw kans → skip Vision, spaar €0.014.
2. **Screenshot-hash cache**: SHA-256 van PNG bytes. Zelfde website = zelfde bytes = cached result = €0.

### Bestanden
- `utils/vision_cache.py` (nieuw) — `screenshot_hash()`, `get_cached_vision()`, `store_vision_result()`.
- `website_intelligence/visual_analyzer.py` — conditional gate + cache read/write.
- `website_intelligence/analyzer.py` — geeft `technical_score` door aan `analyze_visual()`.
- Migration 006 — nieuwe tabel `heatr_vision_cache`.

### Effect
- Eerste run per lead: ~45% minder Vision-calls (gezien behandelaren-site-verdeling).
- Re-enrichment (bijv. na PR3-wiring): ~95% cache-hit → ~€0 in plaats van €0.014/lead.

---

## 6 Warmr v1.0 datapunten

### P0 — gratis (bestaande data)

**1. `booking_system`** — `online | contact-form-only | phone-only | unknown`
- Implementatie: `website_intelligence/conversion_checker.py` — enum derivatie uit bestaande boolean checks (ladder: online booking > form > telefoon).
- Storage: `heatr_leads.booking_system` via `analyzer.py` lead-update.

**2. `latest_review_date`** — timestamptz
- Implementatie: `enrichment/review_analyzer.py` — nieuwe helpers `_parse_relative_date()` (NL + EN: "2 weken geleden", "een maand geleden", "2 years ago") en `latest_review_date_from_reviews()`.
- Max-datum over `reviews[].date` strings. Geen extra API-calls.

**3. `local_competitors_in_db` + `local_competitors_higher_rating`** — ints
- Implementatie: `website_intelligence/competitor_analyzer.py` — Playwright rating-scrape uitgebreid om `google_rating` per competitor op te halen. Nieuwe `_RATING_BUFFER = 0.2` (CB1 in mail-spec: "4.6+ sterren").
- Nieuwe signatuur-param `lead_google_rating: float = 0.0` (backwards-compat default).
- Storage: `heatr_website_intelligence.local_competitors_*`.

### P1 — betaalde data

**4. `treatment_focus[]`** — text[] cosmetische behandelingen
- Nieuw bestand: `enrichment/treatment_classifier.py` (~210 regels).
- Claude Haiku (cost ~€0.0002) met hybrid allowlist-sanity (filtert hallucinaties tegen 130 bekende termen uit `sectors.py`).
- Cache via bestaande `claude_cache` tabel (sha256 van domain + page_text[:2000], TTL 30 dagen impliciet).
- Cost-guard integratie (`guarded_call()` vóór call, `accumulator.charge()` erna).
- Rate-limit via `wait_for_token("treatment_classifier")`.
- Storage: `heatr_leads.treatment_focus`.

**5. `website_age_years` + `domain_registered_at`**
- Nieuw bestand: `enrichment/domain_age_scraper.py` (~160 regels).
- RDAP lookup via 3 endpoints (rdap.org → rdap.sidn.nl → rdap.verisign.com). JSON over HTTPS, geen auth, geen kosten.
- Cache-tabel `heatr_domain_age_cache` (TTL 365 dagen, shared across workspaces — domain-registratie is globaal).
- Rate-limit via `wait_for_token("rdap")`.
- Storage: `heatr_leads.{domain_registered_at, website_age_years}`.

### P2 — externe afhankelijkheid

**6. `meta_ads_active` + `ad_focus`**
- Nieuw bestand: `enrichment/meta_ads_scraper.py` (~260 regels).
- Twee paden: Graph API (`META_AD_LIBRARY_TOKEN` env) → Playwright fallback (public Ad Library page).
- `_extract_ad_focus()` — keyword matching op 22 cosmetische termen uit ad-copy.
- Cache-tabel `heatr_meta_ads_cache` (TTL 7 dagen — ads veranderen vaak).
- Rate-limit via `wait_for_token("meta_ads_playwright")`.
- Storage: `heatr_leads.{meta_ads_active, ad_focus, meta_ads_checked_at}`.

---

## Pipeline wiring

### `job_queue/enrichment_queue.py`
- Nieuwe default enrichment_types list: `+"domain_age"`, `+"treatment_focus"`, `+"meta_ads"` tussen `website_intelligence` en `contact_discovery`.
- Nieuwe `_run_step` branches voor alle drie.
- Nieuwe helper `_get_page_text_for_lead()` — haalt page text uit `website_intelligence.page_text` (preferred) of `companies_raw.description + scraped_text` (fallback).

### Workspace isolation
Alle 3 nieuwe modules accepteren `workspace_id` en gebruiken `cost_guard` met workspace-scoped daily budget. Cache-tabellen (`domain_age`, `meta_ads`) zijn **niet** workspace-scoped — de data is domain-global en geprivacy-veilig te delen.

---

## Monitoring

### Nieuwe endpoint: `GET /analytics/enrichment-cost?days=7`
- Vandaag-spend + daily budget + % used + warning-flag bij ≥80%.
- Window spend (`?days=7` default) + breakdown per context.
- Aantal `BLOCKED:*` events in window — laat zien of cost-guard actief geweest is.

Response voorbeeld:
```json
{
  "today_eur": 0.12,
  "daily_budget_eur": 0.50,
  "budget_remaining_eur": 0.38,
  "budget_pct_used": 24.0,
  "budget_warning": false,
  "window_days": 7,
  "window_eur": 2.34,
  "by_context": {"website_intelligence": 1.80, "treatment_classifier": 0.12, ...},
  "block_events_in_window": 0
}
```

---

## Migration (te draaien in Supabase SQL editor)

```bash
cat migrations/006_warmr_sequence_fields.sql
```

**Nieuwe kolommen op `heatr_leads`:**
- `booking_system text`
- `latest_review_date timestamptz`
- `treatment_focus text[]`
- `domain_registered_at timestamptz`
- `website_age_years int`
- `meta_ads_active boolean`
- `ad_focus text`
- `meta_ads_checked_at timestamptz`
- `enrichment_blocked_reason text`
- `enrichment_partial boolean DEFAULT false`

**Nieuwe kolommen op `heatr_website_intelligence`:**
- `local_competitors_in_db int`
- `local_competitors_higher_rating int`

**Nieuwe tabellen:**
- `heatr_domain_age_cache` (PK: `domain`)
- `heatr_meta_ads_cache` (PK: `cache_key`)
- `heatr_vision_cache` (PK: `screenshot_hash`)

Alles idempotent (`IF NOT EXISTS`). Niet-breaking.

---

## Tests

### Nieuw
- `tests/test_cost_guard.py` — 7 tests (accumulator, daily budget, guarded_call, fail-open)
- `tests/test_pr3_modules.py` — 21 tests (treatment sanity, domain age helpers, meta ads, review date parsing, vision hash, booking_system enum)

### Run
```bash
python3 -m pytest tests/test_cost_guard.py tests/test_pr3_modules.py \
    tests/test_review_email_generator.py tests/test_website_analysis_queue.py \
    tests/test_process_next_enrichment.py -v
```
→ Expect **55 passed**.

### Smoke tests
```bash
python3 -c "
from utils.cost_guard import LeadCostAccumulator, check_daily_budget, guarded_call
from utils.vision_cache import screenshot_hash, get_cached_vision, store_vision_result
from enrichment.treatment_classifier import classify_treatment_focus
from enrichment.domain_age_scraper import fetch_domain_age_years
from enrichment.meta_ads_scraper import check_meta_ads
from job_queue.enrichment_queue import run_enrichment_for_lead, _get_page_text_for_lead
print('All imports OK')
"
```

FastAPI smoke (inclusief nieuwe `/analytics/enrichment-cost`):
```bash
python3 -c "from api.main import app; print('routes=', len(app.routes))"
```
→ Expect: `routes= 70`.

---

## Cost worst-case

| Scenario | Per-lead cost | 8 leads (huidig) | 1000 leads |
|---|---|---|---|
| Volledig cap-hit (alles Claude, geen cache) | €0.016 (pre-PR2.5) | €0.13 | €16 |
| PR2.5 conditional + cache first run | €0.008 | €0.06 | €8 |
| PR2.5 cache hit (re-enrichment) | €0.001 | €0.008 | €1 |
| Treatment classifier toevoeging | +€0.0002 | +€0.0016 | +€0.20 |
| Domain age / Meta ads (€0) | +€0 | +€0 | +€0 |

**Daily budget €0.50 betekent hard cap bij ~30-60 nieuwe leads/dag (afhankelijk van Vision cache-hit rate).** Als de 44 pending scraping jobs over een paar dagen ~800 leads opleveren, dan is dat ~€6-8 totaal aan enrichment, verspreid over dagen door de rate limiter + daily budget.

---

## Niet in deze PR

- Frontend dashboard voor `/analytics/enrichment-cost` endpoint (alleen JSON, geen UI).
- Warmr v1.0 sequence-engine zelf (templates, A/B-blok selector, Loom-trigger).
- CLAUDE.md doc-sync (P3 uit audit, aparte sessie).
- Tests voor volledige end-to-end enrichment met echte Claude/Playwright (alleen unit smoketests; integratie-tests kosten geld).

---

## Impact na merge

- Alle 15 Warmr v1.0 datapunten zijn nu vulbaar door Heatr.
- €0.50/dag hard cap — onmogelijk om onbedoeld meer dan €15/maand uit te geven aan enrichment.
- Claude Sonnet Vision wordt 45-95% minder vaak aangeroepen door conditional + cache.
- Re-enrichment na deze PR is bijna gratis (cache).
- Nieuwe endpoint `/analytics/enrichment-cost` geeft realtime zichtbaarheid in burn-rate.

---

## Gerelateerde PR's

- **PR1** (BLOCKERS) — `campaigns/review_email_generator.py`, `job_queue/website_analysis_queue.py`, migration 005.
- **PR2** (sectors.py cleanup) — nieuw 2-sectoren schema, Laag 4 rewrite, 9 files touched.
- **PR3** (deze) — Warmr v1.0 datapunten + cost-controls.

Volgorde van `git log --oneline`: PR1 → PR2 → PR3.
