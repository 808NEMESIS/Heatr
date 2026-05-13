# Heatr Audit Report — 2026-04-20

## TL;DR

- **Twee runtime-blockers**: `campaigns/review_email_generator.py` en `job_queue/website_analysis_queue.py` worden geïmporteerd in `api/main.py` (regel 434, 1530) maar bestanden bestaan niet → endpoints `/leads/{id}/send-review-email` en `/website-intelligence/process-next` crashen met ImportError zodra ze aangeroepen worden.
- **Schema-divergentie**: `supabase_schema.sql` (origineel, 26 tabellen) en `supabase_schema_prefixed.sql` (10 tabellen, heatr_ prefix) leven naast elkaar. De actieve productie-deployment gebruikt de prefixed versie via `config/database.py` wrapper, maar de prefixed versie mist 16 tabellen (o.a. `rate_limit_state`, `domain_cache`, `reply_inbox`, `system_state`, `enrichment_data`). We hebben de afgelopen dagen 4 losse migraties moeten draaien om ontbrekende kolommen bij te vullen.
- **Scoring gedrag ≠ CLAUDE.md spec**: CLAUDE.md regel 299-315 definieert platte `LEAD_SCORING_FACTORS` (has_valid_email=25, email_type_role=8, etc.). `config/scoring_weights.py` herhaalt deze, maar `scoring/lead_scoring.py` gebruikt ze **niet** — in plaats daarvan 4-dimensionale scoring (fit/data_quality/reachability/personalization). Functioneel OK, maar inconsistent met docs.
- **Email waterfall gedrag ≠ CLAUDE.md spec**: CLAUDE.md regel 166 zegt "stopt bij eerste succes". `enrichment/email_waterfall.py` regel 50-59 beschrijft bewust het tegengestelde: "collect ALL email candidates from ALL sources, then rank". Dit is een bewust besluit van latere sessie maar docs zijn niet bijgewerkt.
- **Frontend auth is dev-bypass**: `app.js` `requireAuth()` accepteert elke Bearer token als `SUPABASE_ANON_KEY='dev-mode'`. `api/main.py` `get_workspace()` valideert geen JWT — decodeert niet, trust alles, retourneert DEFAULT_WORKSPACE. Werkt voor single-tenant MVP, maar multi-tenant is stuk.

---

## Status per module

### 1. Database schema

- **Status:** 🟡 Deels — origineel schema compleet, prefixed variant (actief in productie) onvolledig.

- **Wat er is:**
  - `supabase_schema.sql` (43 KB, 26 tabellen) — volledige suite met RLS, indexen, FK's
  - `supabase_schema_prefixed.sql` (11.6 KB, 10 tabellen) — subset met `heatr_` prefix
  - `migrations/004_discovery_recontact_replies.sql` — voegt `heatr_lead_discovery_schedules`, `heatr_lead_outreach_snapshots` toe
  - Alle workspace-scoped tabellen hebben RLS policies (`using (workspace_id = current_setting('app.current_workspace_id', true))`)
  - Globale state tabellen (rate_limit_state, domain_cache, system_state) intentioneel zonder workspace_id (shared token buckets)
  - Indexen correct: `(workspace_id, domain)`, `(workspace_id, status)`, `(workspace_id, sector)` etc.

- **Wat ontbreekt in de prefixed versie (PRODUCTIE):**
  - `heatr_rate_limit_state` — pas toegevoegd via losse SQL (niet in prefixed.sql)
  - `heatr_domain_cache` — niet gemigreerd, email verifier catchall cache valt terug op geen cache
  - `heatr_system_state` — niet gemigreerd, Google Search CAPTCHA-blok werkt niet
  - `heatr_reply_inbox` (oorspronkelijk) — wel toegevoegd via migration 004
  - `heatr_enrichment_data` — ontbreekt in prefixed.sql
  - `heatr_crm_tasks`, `heatr_crm_deals`, `heatr_system_alerts`, `heatr_startup_log` — ontbreken
  - Kolommen op `heatr_scraping_jobs`: we hebben vandaag `total_found`, `total_new`, `total_enriched`, `worker_id`, `failed_at` los moeten toevoegen omdat prefixed schema ze miste

- **Gap met CLAUDE.md:** CLAUDE.md noemt 1 schema. In praktijk zijn het 2 schema's die uit sync lopen. De prefixed versie is niet volledig gelijk aan de niet-prefixed versie, dus RLS policies + indexes kunnen afwijken.

- **Risico/blokker:** 🔴 **Hoog**. We hebben vandaag 3x endpoints zien crashen door missende kolommen/tabellen. Elke nieuwe feature die de origineel-schema gebruikt faalt stil tot we een ALTER TABLE draaien.

---

### 2. Scrapers

- **Status:** ✅ Volledig geïmplementeerd, anti-detectie + rate limiting werkt.

- **Wat er is (per bestand):**
  - `google_maps_scraper.py` — `scrape_google_maps()` (regel 82), gebruikt `new_browser_context` (regel 121), `random_mouse_movement` (regel 194), `wait_for_token("google_maps")` (regel 118), CAPTCHA detection (regel 146), context rotatie elke 60 resultaten (regel 168-172)
  - `website_scraper.py` — dual-layer httpx+Playwright (regel 124-149), CMS fingerprints 14 platforms (regel 70), tracking tool detection (regel 85-94), booking platform detection (regel 97-102)
  - `google_search_scraper.py` — `search_for_email()` (regel 77), rate limiter (regel 113), 2-uur CAPTCHA block in `system_state` (regel 123), 5-12s delays
  - `kvk_scraper.py` — echte KvK API (`_KVK_BASE_URL = "https://api.kvk.nl/api/v1"`), SBI→industry mapping voor 30+ codes, fuzzy matching (SequenceMatcher 70% name + 30% city)
  - `directory_scraper.py` — 3 oude directories (Zorgkaart, NatuurlijkBeter, ClinicFinder) + 8 generic dispatch routes voor Funda/NVM/VBO/CoachFinder/Werkspot/Bouwend-NL/Thuisvakman
  - `discovery_scheduler.py` — **niet in CLAUDE.md**, voegt recurring scrape schedules toe (sector+city combos draaien elke N dagen via cron)

- **Wat ontbreekt:** niks uit CLAUDE.md-scope.

- **Gap met CLAUDE.md:** 
  - `discovery_scheduler.py` is volledig buiten scope — toegevoegd zonder docs update
  - Oude sectors in `directory_scraper.py` (Zorgkaart, ClinicFinder) referenceren sectoren die niet meer bestaan in `config/sectors.py`

- **Risico/blokker:** 🟢 **Laag**. Scrapers werken. Wel waarschuwing: directory scraper routes voor oude sectoren zijn dode code.

---

### 3. Email waterfall (4 stappen)

- **Status:** ✅ Volledig, maar gedragsmatig afwijkend van spec.

- **Wat er is:**
  - `enrichment/email_waterfall.py` — alle 4 stappen geïmplementeerd (website regel 85, patterns regel 101, Google Search regel 119, KvK regel 158), + step 5 not_found (regel 201)
  - `enrichment/email_verifier.py` — echte MX via dnspython, SMTP RCPT TO handshake, catch-all detectie via random prefix (regel 8-16), 7-dagen catchall cache (regel 96-105)
  - `enrichment/email_finder.py` — 23 role patterns (info, contact, hallo, praktijk, etc.) + 8 name patterns met Dutch tussenvoegsel handling, ASCII normalisatie (é→e)

- **Wat afwijkt:**
  - CLAUDE.md regel 166: "Stopt bij eerste succes"
  - Code regel 50-59: "collect ALL email candidates from ALL sources, then rank" — bewust omgedraaid
  - CLAUDE.md regel 192-195: "Score -25 punten" bij not_found
  - Code: email_status wordt gezet op 'not_found', maar het -25 pt penalty zit niet in scoring (zie scoring audit)

- **Gap met CLAUDE.md:** Waterfall gedrag is bewust "beter dan spec" maar docs zijn niet bijgewerkt.

- **Risico/blokker:** 🟢 **Laag**. Feature werkt, alleen verwarring voor nieuwe devs die de docs lezen.

---

### 4. Website intelligence (5 lagen)

- **Status:** ✅ Volledig.

- **Wat er is:**
  - `analyzer.py` — orchestrator, roept tech+visual+conv+sector+competitor achter elkaar aan (regel 93-150), upsert naar `heatr_website_intelligence`
  - `technical_checker.py` — PageSpeed API call (regel 113-138), SSL/mobile/CMS/schema/sitemap/server location checks, 25pt max
  - `visual_analyzer.py` — echte Claude Sonnet Vision (`claude-sonnet-4-6`), screenshot via Playwright → Supabase Storage, 8-dimensie analyse. Volgt CLAUDE.md prompt template nauwkeurig
  - `conversion_checker.py` — alle 7 elementen uit spec (CTA boven vouw, CTA kracht, phone, WhatsApp, booking, chatbot, form max 5 velden)
  - `sector_checker.py` — leest `sector_website_expectations` uit `config/sectors.py`, heuristic keyword matching voor 15 pt max
  - `competitor_analyzer.py` — top 3 via Google Maps Playwright, 7-dagen cache per city+sector, berekent `score_vs_market`
  - `opportunity_classifier.py` — classificeert in website_rebuild/conversie/chatbot/ai_audit met priority

- **Extra's niet in CLAUDE.md:**
  - `contact_extractor.py` — Claude Haiku parsing van team pages
  - `personalization_extractor.py` — Claude Haiku extract hooks voor outreach

- **Gap met CLAUDE.md:**
  - `visual_analyzer.py` gebruikt `claude-sonnet-4-6` (CLAUDE.md noemt geen specifieke model versie). Niet kritiek.
  - `conversion_checker.py` regel 161-170: CTA tekstkracht via heuristic keyword list, niet via Claude (CLAUDE.md regel 238 zegt "CTA tekst kracht door Claude"). Minder nauwkeurig dan spec.

- **Risico/blokker:** 🟡 **Medium**. Feature werkt. Alleen CTA-kracht is zwakker dan spec.

---

### 5. Scoring & ICP matching

- **Status:** 🟡 Functioneel, gedragsmatig anders dan spec.

- **Wat er is:**
  - `scoring/lead_scoring.py` — 4-dimensionale scoring (fit 0-40, data_quality 0-20, reachability 0-25, personalization 0-15)
  - `scoring/website_scorer.py` — pure wrapper, leest `total_score` uit `heatr_website_intelligence`
  - `scoring/icp_matcher.py` — match leads tegen sector `icp_keywords` + `exclude_keywords` + KvK SBI + size fit + rating + reviews
  - `scoring/feedback_processor.py` — **bestaat**, analyseert Warmr replies → patronen, sector reply rates
  - `scoring/recontact_signals.py` — **niet in CLAUDE.md**, 6 change signals
  - `config/scoring_weights.py` — 252 regels met LEAD_SCORING_WEIGHTS + WEBSITE_SCORING_WEIGHTS — concrete data
  - `config/sectors.py` — 4 sectors: makelaars, alternatieve_geneeskunde, cosmetische_behandelaars, bouwbedrijven

- **Gap met CLAUDE.md:**
  - CLAUDE.md regel 299-315 zegt `LEAD_SCORING_FACTORS = {has_valid_email: 25, email_type_role: 8, ...}` (platte factor-lijst)
  - `scoring/lead_scoring.py` gebruikt deze weights NIET. Eigen 4-dimensie logica
  - `config/scoring_weights.py` is decoratief (bevat de weights maar niemand importeert ze)
  - **Inconsistentie**: CLAUDE.md zegt "Sector 1 Alternatieve Zorg" + "Sector 2 Cosmetische Klinieken" (2 sectoren, regel 37-50). Werkelijkheid: 4 sectoren, deels andere namen
  - CLAUDE.md regel 316-317 "Website score beïnvloedt lead score NIET direct" — `lead_scoring.py` houdt zich daar aan (score_lead leest geen website_score) ✓

- **Risico/blokker:** 🟡 **Medium**. Scoring werkt, maar niemand kan uit docs afleiden hoe het echt berekend wordt. Voor audit/feedback-tuning is dit een probleem.

---

### 6. Campaigns & Warmr integratie

- **Status:** 🔴 Incomplete — 1 bestand mist volledig, 1 feature niet af.

- **Wat er is:**
  - `integrations/warmr_client.py` (372 regels) — `WarmrClient` class, Bearer token auth, `get_ready_inboxes()`, `push_lead()`, `push_leads_bulk()`, `create_campaign()`
  - `campaigns/sequence_engine.py` — sequence validation (MAX_SEQUENCE_STEPS=4, MIN_WAIT_DAYS=2), spintax parsing (regel 140-156 `replace("{{placeholder}}", value)` — basic), spam word filter
  - `integrations/reply_classifier.py` — **niet in CLAUDE.md**, classificeert replies met Claude
  - Webhook handler `/webhooks/warmr` in `api/main.py` — HMAC signature check, event routing (interested/replied/bounced/unsubscribed), crm_stage update

- **Wat ontbreekt:**
  - `campaigns/review_email_generator.py` — **bestaat NIET**, maar `api/main.py` regel 434 importeert `from campaigns.review_email_generator import generate_review_email` → **endpoint POST /leads/{id}/send-review-email crasht**
  - CLAUDE.md regel 268-292 beschrijft de exacte prompt (90 woorden, verwijs naar website_score + top_issue + score_vs_market + specific_observation). Deze prompt is nergens geïmplementeerd.

- **Gap met CLAUDE.md:**
  - Sequence engine spintax is basic string replace, niet full `{A|B}` OR-syntax zoals CLAUDE.md regel 352 impliceert
  - Sequence engine mist A/B testing toggle

- **Risico/blokker:** 🔴 **Hoog**. Review email generator is de **Trojan Horse** van Heatr (CLAUDE.md commerciële strategie). Werkt nu niet. Zonder review email kan Aerys geen eerste gesprekken starten.

---

### 7. Queues

- **Status:** 🟡 2 van 3 aanwezig.

- **Wat er is:**
  - `job_queue/scraping_queue.py` (489 regels) — Supabase-backed queue, atomic status flip (pending→running), 7-dag dedup, retry (max 3), MAX_CONCURRENT_SCRAPERS semaphore
  - `job_queue/enrichment_queue.py` (803 regels) — 9-step pipeline (website → email_waterfall → kvk → company_enrichment → website_intelligence → contact_discovery → data_verification → scoring → inbox_selection)
  - Queue technologie: pure Supabase tabellen, geen Redis/Celery. Worker loop met `asyncio.sleep(30)` als idle

- **Wat ontbreekt:**
  - `job_queue/website_analysis_queue.py` — **bestaat NIET**, maar `api/main.py` regel 1530 importeert `from job_queue.website_analysis_queue import process_next_website_analysis` → **endpoint POST /website-intelligence/process-next crasht**
  - CLAUDE.md regel 115 noemt expliciet `website_analysis_queue.py`

- **Dead letter handling:** na 3 retries → `status='failed'`, blijft staan. Geen apart DLQ. Worker slaat over.

- **Gap met CLAUDE.md:** directory heet `queue/` in CLAUDE.md maar is `job_queue/` in code (hernoemd omdat `queue` Python's stdlib shadow gaf).

- **Risico/blokker:** 🟡 **Medium**. 2 van 3 queues werken. Website analysis queue ontbreken betekent dat website_intelligence alleen synchroon via enrichment_queue kan draaien (geen aparte worker pool).

---

### 8. FastAPI endpoints

- **Status:** ✅ Volledig — alle 19 CLAUDE.md endpoints aanwezig + 46 extra.

- **Wat er is:**
  - 65 endpoints totaal
  - Alle 19 uit CLAUDE.md regel 358-381 zijn aanwezig
  - 46 extra endpoints: CRM (`/crm/*`), GDPR (`/gdpr/*`), alerts, timeline, discovery-schedules, replies, recontact-signals, sequences, health, briefing, tasks, costs, metrics
  - Auth: elke endpoint heeft `workspace_id: str = Depends(get_workspace)` dependency
  - Filter op workspace_id: elke DB call heeft `.eq("workspace_id", workspace_id)`

- **Gap met CLAUDE.md:**
  - `get_workspace()` in `api/main.py` regel 95-105 doet **geen JWT decode**. Accepteert elke Bearer token en retourneert `DEFAULT_WORKSPACE_ID="aerys"`. Comment op regel 98 erkent dit: "MVP: single-workspace, trust any valid-looking token"
  - CORS: `allow_origins=["*"]` (regel 44) — geen domain whitelist
  - `/warmr/inboxes` mist expliciete workspace_id filter — retourneert alle inboxes van de Warmr API, ongeacht workspace

- **Risico/blokker:** 🟡 **Medium voor multi-tenant, laag voor single-tenant MVP**. Huidige single-workspace setup werkt prima.

---

### 9. Frontend

- **Status:** ✅ Alle 11 files aanwezig. ⚠️ Design token mismatch met CLAUDE.md.

- **Wat er is:**
  - 10 HTML files (index, dashboard, search, leads, lead-detail, website-kansen, campaigns, inbox, crm, analytics)
  - `app.js` — shared JS met Supabase client, requireAuth (dev-bypass), apiCall wrapper, formatScore, renderSidebar, toasts
  - 3 CSS files: `style.css` (imports andere twee + Heatr-extensies), `tokens.css` (Claude Design handoff — colors + type), `kit.css` (Claude Design componenten)

- **Gap met CLAUDE.md:**
  - CLAUDE.md regel 324: "lichtpaarse gradient accenten" 
  - Realiteit: accent is `--blush-500: #D97757` (warm terracotta) met `--sakura-500: #D85E74` highlights
  - Design system komt uit een later opgeleverde Claude Design handoff (zie `/tmp/anthropic-design/heatr-design-system`)
  - `frontend/analytics.html` heeft hardcoded `const ACCENT = '#6c5ce7'` — de oude paarse kleur uit CLAUDE.md — **inconsistent met de rest van de app**
  - Fonts zijn wel correct: Fraunces (display) + Plus Jakarta Sans (UI), zoals CLAUDE.md voorschrijft ✓
  - `HEATR_CONFIG.API_BASE` hardcoded in elke HTML op `http://localhost:8001` — werkt lokaal, deploy vereist edit per-file
  - `HEATR_CONFIG.SUPABASE_ANON_KEY = 'dev-mode'` sentinel activeert dev-bypass — werkt voor lokaal maar productie vereist echte key

- **Risico/blokker:** 🟡 **Medium**. Visual drift tussen docs en code. analytics.html visueel afwijkend van rest. Deploy friction door hardcoded configs.

---

## Bestanden buiten de bedoelde architectuur

Niet genoemd in CLAUDE.md maar wel in de repo:

**Enrichment (niet in CLAUDE.md):**
- `enrichment/batched_enrichment.py` — combineert personalization + opener in 1 Claude call (kosten optimalisatie)
- `enrichment/contact_discovery.py` — vindt beslisser via team page + LinkedIn + seniority ranking
- `enrichment/data_verification.py` — cross-source confidence scores
- `enrichment/enrichment_gate.py` — skip expensive Claude voor low-score leads
- `enrichment/enrichment_validator.py` — post-enrichment validation met cross-check
- `enrichment/lead_qualifier.py` — pre-enrichment qualification gate
- `enrichment/opener_generator.py` — 3 ranked outreach openers op basis van pijnpunten
- `enrichment/review_analyzer.py` — Google reviews scraping + Claude analyse
- `enrichment/website_prescreener.py` — snelle "is this a real website?" check

**Utils (niet in CLAUDE.md):**
- `utils/alert_manager.py`, `utils/claude_cache.py`, `utils/gdpr_manager.py`, `utils/metrics_collector.py`, `utils/pipeline_metrics.py`, `utils/sending_guard.py`, `utils/startup_validator.py`

**Integrations:**
- `integrations/reply_classifier.py` — Claude classificatie van Warmr replies

**Scoring:**
- `scoring/recontact_signals.py` — trigger-based recontact detection

**Scripts:**
- `scripts/run_worker.py` — worker runner wrapper

**Tests:**
- `tests/test_e2e_pipeline.py`, `tests/test_google_maps_live.py`, `tests/run_enrichment.py`, `tests/run_full_enrichment.py`, `tests/run_optimized_enrichment.py`, `tests/test_outreach_rules.py`

**Documentation duplicates:**
- `CLAUDE.md` en `heatr_CLAUDE.md` zijn **beide aanwezig** — 14.9 KB elk. Mogelijk kopie?
- `MVP_GAPS.md` — uitgebreide gap-analyse doc
- `SKILL.md` — skill instructie doc

**Extra SQL:**
- `supabase_schema_prefixed.sql` (naast het origineel) — tweede schema variant

---

## Environment variables gap

Uit code gehaald maar **ontbreken in `.env.example`**:

| Var | Gebruikt in | Impact |
|---|---|---|
| `OPERATOR_EMAIL` | `api/main.py:1788`, `utils/alert_manager.py` | Kritieke alerts + briefings gaan nergens heen |
| `RESEND_API_KEY` | `api/main.py:1789` | Briefing/alert email disabled zonder warning |
| `HEATR_BASE_URL` | `api/main.py:1819` | Absolute URL in briefing emails broken |
| `HEATR_TABLE_PREFIX` | `config/database.py` | Cruciaal voor dual-schema setup, geen default doc |
| `DAILY_API_BUDGET_EUR` | `utils/cost_tracker.py` (mogelijk) | Budget cap voor Claude |
| `SUPABASE_ANON_KEY` | `frontend/*.html` | Frontend auth — wordt "dev-mode" gebruikt |
| `DEFAULT_WORKSPACE_ID` | Overal — wél in .env.example als 'aerys' ✓ | OK |

**Wel in `.env.example`** maar niet zichtbaar in code-audit gebruikt:
- Niets kritisch gevonden — .env.example is vrij volledig, de gaps zitten in nieuwere features.

---

## Dode code

- **Oude sector routing in `scrapers/directory_scraper.py`**: routes voor `zorgkaartnederland.nl` en `clinicfinder.nl` bestaan nog (regel 681-688 in dispatch) maar deze directories zijn niet meer in de actieve sector configs — ze horen bij de oude "alternatieve_zorg" / "cosmetische_klinieken" sectors uit CLAUDE.md regel 39-50.
- **`config/scoring_weights.py`** — bevat `LEAD_SCORING_WEIGHTS` en `WEBSITE_SCORING_WEIGHTS` (252 regels concrete config), maar **niemand importeert deze weights**. `scoring/lead_scoring.py` gebruikt eigen inline thresholds.
- **`tests/test_outreach_rules.py`** — bestaat, 17.5 KB, unittest suite. Laatste run onbekend. Status onbekend zonder draaien.
- **`scoring/feedback_processor.py`** — geïmplementeerd, maar geen enkele cron/endpoint roept het aan (wel endpoint `/scoring/process-feedback` zou moeten bestaan, is er niet).
- **Dubbele CLAUDE.md**: `CLAUDE.md` en `heatr_CLAUDE.md` zijn beide 14.9 KB — waarschijnlijk duplicaat.

---

## TODO/FIXME inventaris

Grep naar `TODO|FIXME|XXX|HACK|stub` in Python-codebase:

| Bestand | Regel | Type | Tekst |
|---|---|---|---|
| `job_queue/enrichment_queue.py` | 13 | stub reference | docstring: "5. scoring — stub (always 0 until session 4)" — **verouderd commentaar**, scoring is wel geïmplementeerd |
| `job_queue/enrichment_queue.py` | 365 | comment | "Pre-screen: is this a real website or a parked/placeholder domain?" (gewoon comment, geen TODO) |
| `utils/gdpr_manager.py` | 22 | comment | "Fields replaced with anonymized placeholder on forget" (gewone beschrijving) |
| `api/main.py` | 379 | comment | "Use first available inbox as campaign placeholder" (hack comment) |
| `enrichment/lead_qualifier.py` | 21 | data | `_JUNK_DOMAINS` lijst — "parked/placeholder/aggregator sites" (data, geen TODO) |
| `enrichment/website_prescreener.py` | 98 | comment | "Could be a single-page placeholder" (comment) |
| `scrapers/directory_scraper.py` | 614 | comment | "Substitute {city} placeholder" (comment) |
| `scrapers/website_scraper.py` | 66 | data | regex pattern voor placeholder domain detection |
| `campaigns/sequence_engine.py` | 140-156 | feature | spintax placeholder replacement (feature code, geen TODO) |

Geen echte `TODO` / `FIXME` / `XXX` / `HACK` markers in de codebase. Eén outdated comment (enrichment_queue.py:13).

---

## Aanbevolen prioriteiten

1. **Fix runtime-blockers** (🔴 Dag 1)
   - Implementeer `campaigns/review_email_generator.py` met de CLAUDE.md regel 273-290 prompt → Claude Haiku call. Zonder deze is de **Trojan Horse** (gratis website review als eerste contact) dood.
   - Maak `job_queue/website_analysis_queue.py` of strip de import uit `api/main.py` regel 1530. Op dit moment crasht de n8n website-analysis worker.
   - **Reden om eerst**: beide zijn directe ImportError crashes. Zolang deze niet gefixt zijn, staat de outreach flow stil.

2. **Schema consolideren naar prefixed variant** (🔴 Dag 2-3)
   - `supabase_schema_prefixed.sql` moet 1-op-1 alle tabellen/kolommen/indexes uit het originele schema bevatten — nu mist 16+ tabellen
   - Voeg een `schema_version` tabel + migration tracker toe zodat we niet telkens losse ALTERs doen
   - **Reden om snel**: elke dag ontdekken we nieuwe ontbrekende kolommen. Dit blokkeert elke nieuwe feature.

3. **Docs synchroniseren met werkelijkheid** (🟡 Dag 4)
   - CLAUDE.md update: 4 nieuwe sectoren i.p.v. 2 oude, collect-all email waterfall, 4-dimensie scoring, `heatr_` prefix strategy, dev-mode frontend bypass
   - Eén `CLAUDE.md` (verwijder `heatr_CLAUDE.md` duplicate)
   - Documenteer de ~30 extra modules die buiten scope gebouwd zijn
   - **Reden**: nieuwe devs hebben nu 3 verschillende bronnen van waarheid. Audits als deze zijn onnodig verwarrend.

4. **Scoring weights daadwerkelijk gebruiken** (🟡 Week 2)
   - `config/scoring_weights.py` is 252 regels decoratief. Of `scoring/lead_scoring.py` refactoren om deze weights te gebruiken, OF `scoring_weights.py` verwijderen.
   - **Reden**: voor feedback-tuning (via `feedback_processor.py`) moet één enkele config bepalend zijn.

5. **Multi-tenant JWT auth** (🟡 Week 3 of bij klantenrol)
   - `get_workspace()` in `api/main.py` regel 95-105 moet echte JWT decode doen en `app_metadata.workspace_id` uit de Supabase claim halen
   - Frontend `requireAuth()` moet `dev-mode` bypass uitschakelen voor productie
   - CORS: `allow_origins=["*"]` vervangen door whitelist
   - **Reden**: nu is Heatr functioneel single-tenant. Elke second workspace breekt. Niet urgent maar blokkerend voor saas-model.

---

*Audit uitgevoerd door claude-opus-4-7 op 2026-04-20. Alleen gelezen, geen code gewijzigd.*
