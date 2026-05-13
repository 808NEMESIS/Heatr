# PR3 SCOPE — Enrichment gaps voor Warmr Sequence v1.0

**Datum:** 2026-04-21
**Doel:** 6 ontbrekende datapunten implementeren die de Warmr Sequence v1.0 nodig heeft. Zonder deze zakken alle leads onder de 7/15-drempel.
**Niet in scope:** de Warmr sequence zelf bouwen (templates, Warmr API push), frontend UI updates, CLAUDE.md sync.

---

## De 6 datapunten

| # | Veld | Complexiteit | Bron | Kosten/lead | Priority |
|---|---|---|---|---|---|
| 1 | `booking_system` enum | LOW | Bestaande `conversion_checker.py` | €0 | P0 |
| 2 | `latest_review_date` (ISO) | LOW | Bestaande `review_analyzer.py` | €0 | P0 |
| 3 | `local_competitors_higher_rating` + `local_competitors_in_db` | LOW-MED | Bestaande `competitor_analyzer.py` | €0 | P0 |
| 4 | `treatment_focus[]` | MED | Nieuwe Claude Haiku classifier | ~€0.0002 | P1 |
| 5 | `website_age_years` | MED | Externe WHOIS of RDAP API | €0 | P1 |
| 6 | `meta_ads_active` + `ad_focus` | HIGH | Meta Ad Library API | €0 (geen keys nodig voor public-library) | P2 |

---

## Stappenplan (implementatie-volgorde)

### Stap 1 — P0 "done-by-default" datapunten (30 min, 3 kolommen)

**1a. `booking_system` uit `conversion_checker.py`**
- Uitbreiden van bestaande check: koppel aan `has_online_booking`, `has_contact_form`, `has_phone_clickable`.
- Return-dict krijgt `booking_system: "online" | "contact-form-only" | "phone-only" | "unknown"`.
- Nieuwe kolom `heatr_leads.booking_system text`.

**1b. `latest_review_date` uit `review_analyzer.py`**
- Bestaande review-scraper levert al `reviews[].date`. We voegen `max(reviews.date)` toe aan de update-payload.
- Nieuwe kolom `heatr_leads.latest_review_date timestamptz` (NULL als geen reviews).

**1c. `local_competitors_in_db` + `local_competitors_higher_rating` uit `competitor_analyzer.py`**
- Aggregaat: count competitors met `google_rating > lead.google_rating` + 0.2 threshold (buffer voor noise).
- Nieuwe kolommen `heatr_website_intelligence.local_competitors_in_db int`, `.local_competitors_higher_rating int`.

**DB migration** `migrations/006_warmr_sequence_fields.sql` — 4 kolommen, allemaal idempotent IF NOT EXISTS.

### Stap 2 — P1 middelzware (1-2 uur)

**2a. `treatment_focus[]` via Claude Haiku classifier**
- Nieuw bestand `enrichment/treatment_classifier.py`.
- Signature: `async def classify_treatment_focus(domain, page_html, anthropic_client, supabase_client) -> list[str]`.
- Prompt: "Welke cosmetische behandelingen biedt deze website primair aan? Return JSON: top 5." Output gaat door een allowlist-filter (enum uit sectors.py `cosmetische_behandelaars.subcategories`).
- Never-raise, cost log naar `api_cost_log`.
- Nieuwe kolom `heatr_leads.treatment_focus text[]`.

**2b. `website_age_years` via RDAP**
- Nieuw bestand `enrichment/domain_age_scraper.py`.
- Waarom RDAP i.p.v. WHOIS: RDAP is standaard JSON via `https://rdap.org/domain/{domain}`, geen auth, publieke API, gratis, rate-limit 10req/s. WHOIS vereist TCP-poort 43 parsing wat brittle is.
- Signature: `async def fetch_domain_age_years(domain) -> int | None`.
- Caching: response bewaren in `heatr_leads.domain_registered_at` om retries te voorkomen.
- Nieuwe kolommen `heatr_leads.domain_registered_at timestamptz`, `.website_age_years int`.

### Stap 3 — P2 zwaar (2-3 uur)

**3a. `meta_ads_active` + `ad_focus` via Meta Ad Library**
- Nieuw bestand `enrichment/meta_ads_scraper.py`.
- Meta Ad Library heeft een publieke zoek-URL: `https://www.facebook.com/ads/library/?ad_type=all&country=NL&q={company_name}`. Playwright fetcht, parse JSON uit page script.
- Alternatief: `https://graph.facebook.com/v18.0/ads_archive` — vereist app token (gratis). Als token aanwezig via env `META_AD_LIBRARY_TOKEN`: gebruik API. Anders fallback naar Playwright.
- `ad_focus`: eerste 1-2 behandelings-keywords uit de ad-copy (als ads gevonden). Als geen ads → beide velden NULL.
- Nieuwe kolommen `heatr_leads.meta_ads_active boolean`, `.ad_focus text`, `.meta_ads_checked_at timestamptz`.
- Rate-limiter: 5/min in `utils/rate_limiter.py`.

### Stap 4 — Pipeline wiring (30 min)

De 6 nieuwe vullers moeten door de enrichment pipeline aangeroepen worden. Huidige `enrichment/batched_enrichment.py` / `job_queue/enrichment_queue.py` orchestreert de stappen. Toevoegingen:
- Na `analyze_website` → `classify_treatment_focus` (2a)
- Na `qualify_and_create_lead` → `fetch_domain_age_years` (2b, eenmalig per domain)
- Parallel in enrichment run → `check_meta_ads` (3a, best-effort, faal-silent)

De P0 datapunten (stap 1) worden binnen bestaande modules ingebakken — geen nieuwe orchestration-hooks nodig.

### Stap 5 — Tests (1 uur)

- `tests/test_treatment_classifier.py` — mock Claude, verifieer allowlist-filter en never-raise.
- `tests/test_domain_age_scraper.py` — mock httpx, verifieer 3 RDAP-response formats + None op fouten.
- `tests/test_meta_ads_scraper.py` — mock HTTP, verifieer parsing + fallback.
- `tests/test_conversion_checker_booking_system.py` — unit test voor de nieuwe enum-logica.
- `tests/test_review_analyzer_latest_date.py` — verifieer MAX extraction.

Run target: ≥15 nieuwe tests, allemaal groen + bestaande 26 blocker-tests blijven groen.

---

## DB migration `migrations/006_warmr_sequence_fields.sql`

```sql
ALTER TABLE heatr_leads
    ADD COLUMN IF NOT EXISTS booking_system text,
    ADD COLUMN IF NOT EXISTS latest_review_date timestamptz,
    ADD COLUMN IF NOT EXISTS treatment_focus text[],
    ADD COLUMN IF NOT EXISTS domain_registered_at timestamptz,
    ADD COLUMN IF NOT EXISTS website_age_years int,
    ADD COLUMN IF NOT EXISTS meta_ads_active boolean,
    ADD COLUMN IF NOT EXISTS ad_focus text,
    ADD COLUMN IF NOT EXISTS meta_ads_checked_at timestamptz;

ALTER TABLE heatr_website_intelligence
    ADD COLUMN IF NOT EXISTS local_competitors_in_db int,
    ADD COLUMN IF NOT EXISTS local_competitors_higher_rating int;
```

**10 nieuwe kolommen. Allemaal nullable + idempotent. Niet-breaking.**

---

## Kritieke beslissingen die ik nu maak (weegbaar)

1. **RDAP > WHOIS voor domain age.** RDAP is de moderne standaard, JSON via HTTPS, geen parse-hel. Als RDAP niet werkt voor .nl specifiek (SIDN heeft eigen beleid), fallback naar `https://rdap.sidn.nl/domain/{domain}`.

2. **Treatment focus allowlist uit sectors.py v2.** In plaats van vrije Claude-output gebruiken we de 11 subcategory-labels uit `cosmetische_behandelaars.subcategories` als enum. Voorkomt spelling-drift en maakt aggregaten mogelijk.

3. **Meta Ads Library via Playwright fallback.** Graph API vereist app-review voor public-library access sinds 2023. Playwright is brozer maar werkt ZONDER account. Token-path is opt-in via env.

4. **`website_age_years` = floor((now - domain_registered_at).days / 365).** Integer, geen fractional years. Mail-spec BLOK A zegt "zo'n X jaar" → int volstaat.

5. **`local_competitors_higher_rating` met 0.2 buffer**. Lead op 4.4, concurrent op 4.5 telt NIET als "higher" — pas vanaf 4.6. Voorkomt noise bij ratings die binnen Google's eigen ronde-afwijking vallen. Mail-spec CB1 spreekt letterlijk van "4.6+".

6. **Never-raise pattern overal.** Elke enrichment-stap vangt exceptions en return `None`/empty, schrijft `*_failed_reason` kolom. Pipeline stopt nooit door één faalde stap.

---

## Estimated tijd

- Stap 1 (P0 done-by-default, 3 datapunten): 30 min
- Stap 2 (P1, treatment + domain age): 1-2 uur
- Stap 3 (P2, Meta Ads): 2-3 uur
- Stap 4 (pipeline wiring): 30 min
- Stap 5 (tests): 1 uur
- **Totaal: ~5-7 uur**

---

## Risico's

- **Meta Ad Library Playwright is fragile** (Meta verandert layout geregeld). Fallback: als Playwright-route faalt, laat `meta_ads_active = NULL`. Niet-fataal omdat Mail 1 BLOK C alleen gekozen wordt als `meta_ads_active = true`; bij NULL valt hij terug op BLOK D/E via de Observation Selector.
- **RDAP rate-limiting op .nl**: SIDN's RDAP heeft ~20 req/min limit. Bij bulk rescrape kan dat knellen. Mitigation: cache in `domain_registered_at`, re-check alleen >365 dagen oud.
- **Treatment classifier prompt-drift**: Claude kan buiten de allowlist gaan. Allowlist-filter achteraf is harde guard, geen prompt-dependency.

---

## Impact na merge

- Alle 15 Warmr v1.0 datapunten kunnen door Heatr worden gevuld.
- Minimum-drempel 7/15 per lead wordt voor de meeste leads gehaald (conservatieve schatting: 60-80%).
- Personalisatie-score wordt betekenisvol te berekenen (een aparte P3b taak).
- Nieuwe leads: +€0.0002/lead Claude cost (treatment classifier).
- Bestaande leads: herverrijking via `enrichment_version` bump → kan in staggered batch.

---

## Cost-controls (HARD GATES — ingebouwd in alle 6 modules)

Gezien gebruiker expliciet eist dat kosten niet "uit de spuigaten lopen", worden deze 5 guards toegevoegd vóór elke Claude-call en externe API:

### 1. Per-day budget kill-switch (module-niveau)
Nieuwe env var `ENRICHMENT_DAILY_BUDGET_EUR` (default: `1.00`). Nieuwe helper `utils/cost_guard.py`:
```python
async def check_budget(workspace_id, supabase) -> tuple[bool, float]:
    """Query api_cost_log voor today's sum. Return (allowed, spent_eur)."""
```
Elke Haiku/Sonnet call checkt eerst. Als `spent_eur >= ENRICHMENT_DAILY_BUDGET_EUR`: skip + log WARNING + zet `lead.enrichment_blocked_reason = 'daily_budget_exceeded'`. Pipeline stopt NIET — andere free steps draaien door.

### 2. Per-lead cost ceiling (per enrichment run)
Nieuwe env var `MAX_COST_PER_LEAD_EUR` (default: `0.05`). Accumulator in enrichment_queue per lead. Als een run deze limiet overschrijdt: stop Claude-calls voor die lead, markeer `enrichment_partial = true`.

### 3. Dedup-cache voor alle externe calls (hergebruik `utils/claude_cache.py`)
- Treatment classifier: cache-key = `sha256(domain + page_html[:2000])`. TTL 30 dagen. Hergebruik = €0.
- Domain age: primary key = `domain` in aparte tabel `heatr_domain_age_cache`. TTL 365 dagen.
- Meta Ads: cache op `company_name + domain` TTL 7 dagen (ads veranderen vaak).
- Competitor aggregates: hergebruik bestaande `heatr_website_intelligence` rij, geen extra API.

### 4. Rate-limiter hard caps via `utils/rate_limiter.py`
Nieuwe service-entries:
```python
"treatment_classifier": {"max_tokens": 5, "refill_rate": 0.166},  # 10/min
"rdap": {"max_tokens": 5, "refill_rate": 0.333},                   # 20/min — SIDN limit
"meta_ads_playwright": {"max_tokens": 2, "refill_rate": 0.083},    # 5/min — anti-ban
```
Workers moeten `wait_for_token()` aanroepen. Als rate lost: fail-silent, try-later pattern.

### 5. Audit-log verplicht op elke call
Elke Haiku/Playwright/HTTP enrichment call schrijft MOET naar `heatr_api_cost_log` met:
- `cost_eur` (exact, niet geschat)
- `context` (e.g. "treatment_classifier", "rdap_lookup")
- `lead_id`
- `workspace_id`

Bij ontbreken: call wordt geweigerd door een pre-insert check in `utils/cost_guard.py::assert_logged()`.

### Concrete cost-ceilings per onderdeel
| Module | Per-lead hard cap | Per-day hard cap | Failsafe |
|---|---|---|---|
| Treatment classifier | €0.0005 (2.5x budget) | €0.50 (2500 leads) | Skip + log |
| Domain age (RDAP) | €0 | N/A | Cache forever |
| Meta Ads Playwright | €0 | N/A | Rate-limit |
| Booking system | €0 | N/A | Al in bestaande check |
| Review date | €0 | N/A | Uit bestaande data |
| Competitor aggregaten | €0 | N/A | Uit bestaande data |

**Worst case per lead bij alles-hit-cap: €0.0005. Daily cap €1.00 = 2000 leads/dag. Monthly worst case: ~€30.** Huidige Heatr-totaalbudget staat op €10-15/m per CLAUDE.md, dus ik clamp `ENRICHMENT_DAILY_BUDGET_EUR` default op **€0.50** (niet €1.00). Dat levert max €15/m worst case.

### Monitoring endpoint (gratis)
Nieuwe endpoint `GET /analytics/enrichment-cost?days=7` — leest `heatr_api_cost_log`, returnt:
```json
{"today_eur": 0.23, "7d_eur": 1.45, "by_context": {"treatment_classifier": 0.12, ...}, "budget_remaining": 0.27}
```
Frontend dashboard widget toont huidige burn-rate. Waarschuwt zichtbaar als >80% budget.

---

## STOP-GATE (finale)

Wacht op user-akkoord vóór stap 1 start. Beslissingen die user-akkoord behoeven:
1. RDAP OK (of commerciële WHOIS)?
2. Meta Ads Playwright fallback OK (of alleen Graph API + skip zonder token)?
3. Competitor rating-buffer 0.2 OK?
4. Treatment focus: hybrid (Claude vrije tekst + allowlist-sanity-check) OK?
5. **Cost-guards:** `ENRICHMENT_DAILY_BUDGET_EUR=0.50` + `MAX_COST_PER_LEAD_EUR=0.05` als defaults OK? Of strakker?

Bij kaal "go": defaults op alle 5 (RDAP + Playwright + 0.2 + hybrid + €0.50/dag + €0.05/lead).
