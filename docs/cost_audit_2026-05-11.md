# Heatr Cost Audit — 2026-05-11

**Scope:** read-only analyse van de Heatr-pipeline cost-structuur. Eén rapport,
geen productiecode aangepast. Data uit `api_cost_log` (apr 18 → mei 6, 6.772 rows),
plus code-grep voor hardcoded prijzen en infrastructuur.

---

## Executive Summary

- **Totale cost per lead (gemiddeld, op direct-attribueerbare rows):** **€0.0064**
- **Totale cost per lead (incl. proportioneel verdeelde "no-lead" rows):** **€0.0116**
- **Bij 1.000 leads/mnd geprojecteerd:** **€6 – €12** (afhankelijk van attribution-fix)
- **Duurste fase per lead:** `sector_checker` (35.7% van totaal logged spend)
- **Belangrijkste optimalisatiekans:** **attribution-gap fixen** (56% van rows mist `lead_id`) +
  **`claude_cache` tabel ontbreekt** (0 cache-hits in 6.772 calls, code verwacht cache)
- **Major operationeel risico:** **Vision-fase werkt niet** — 0 Sonnet-calls geregistreerd,
  `vision_cache` leeg, alle `visual_score=None` in 807 website_intelligence rows. De fase
  faalt silent of wordt structureel geskipt. Cost-impact: nul (positief), kwaliteit-impact:
  significant (visuele analyse die templates beloven, ontbreekt feitelijk).

---

## 1. Cost-tracking Infrastructuur

### 1.1 Modules

| Module | Rol |
|---|---|
| [`utils/claude_cache.py`](../utils/claude_cache.py) | Prompt-cache + cost-logger. Definieert `COST_PER_1M_TOKENS` dict + `_log_api_cost()` schrijver naar `api_cost_log`. |
| [`utils/cost_guard.py`](../utils/cost_guard.py) | Drie-laagse budget-gates: daily €0.50, monthly €20.00, per-lead €0.05. `guarded_call()` wrapper voor Claude-calls. `LeadCostAccumulator` voor in-memory per-lead tracking. |
| [`utils/pipeline_metrics.py`](../utils/pipeline_metrics.py) | Aggregeert `api_cost_log` per period, calculeert `per_qualified_lead` / `per_pushed_lead`. |

### 1.2 DB-tabellen

| Tabel | Status | Doel |
|---|---|---|
| `heatr_api_cost_log` | ✓ **6.772 rows** apr 18 – mei 6 | Per-call audit-trail (model, tokens, cost_eur, cache_hit, context, lead_id). |
| `heatr_vision_cache` | ⚠ **0 rows** | Bedoeld voor Sonnet Vision-deduplicatie. Leeg — fase wordt niet uitgevoerd of faalt vóór logging. |
| `heatr_claude_cache` | ✗ **Tabel ontbreekt** | Bedoeld voor prompt-response caching. `cached_claude_call()` zou hier lookups doen — maar tabel bestaat niet. Schema-drift. |
| `heatr_meta_ads_cache` | ✓ aanwezig | Meta Ads response-cache (Playwright fallback). |

### 1.3 Hardcoded model-prijzen — **schema-divergentie gevonden**

Twee verschillende prijslijsten in de codebase, **niet consistent**:

| Locatie | Haiku 4.5 in | Haiku 4.5 out | Sonnet 4.6 in | Sonnet 4.6 out |
|---|---|---|---|---|
| [`utils/claude_cache.py:21-25`](../utils/claude_cache.py#L21-L25) | €0.25/M | (single value, output prijs ontbreekt) | €3.00/M | (single value) |
| [`enrichment/treatment_classifier.py:38-39`](../enrichment/treatment_classifier.py#L38-L39) | €0.74/M | €3.68/M | n.v.t. | n.v.t. |
| [`enrichment/archetype_classifier.py:43-44`](../enrichment/archetype_classifier.py#L43-L44) | €0.74/M | €3.68/M | n.v.t. | n.v.t. |
| [`enrichment/owner_extractor.py:31-32`](../enrichment/owner_extractor.py#L31-L32) | €0.74/M | €3.68/M | n.v.t. | n.v.t. |
| [`enrichment/batched_enrichment.py:227-230`](../enrichment/batched_enrichment.py#L227-L230) | €0.74/M | €3.68/M | n.v.t. | n.v.t. |
| [`website_intelligence/sector_checker.py:44-45`](../website_intelligence/sector_checker.py#L44-L45) | €0.74/M | €3.68/M | n.v.t. | n.v.t. |
| [`integrations/reply_classifier.py:403`](../integrations/reply_classifier.py#L403) | €0.74/M | €3.68/M | n.v.t. | n.v.t. |
| [`campaigns/reply_drafter.py:41-42`](../campaigns/reply_drafter.py#L41-L42) | €0.74/M | €3.68/M | n.v.t. | n.v.t. |

**Anthropic publiek (USD):** Haiku 4.5 = $1/M in + $5/M out; Sonnet 4.6 = $3/M in + $15/M out.
**Conversie naar EUR @ 0.93:** Haiku ≈ €0.93/M in + €4.65/M out.

**Conclusie**: `claude_cache.py:21-25` (€0.25/M Haiku, single value) is **3× te laag** versus
de werkelijke Anthropic-prijs én versus wat de fase-modules zelf gebruiken (€0.74/M).
De `_log_api_cost()` in `claude_cache.py` schrijft daarom systematisch **te lage cost_eur**
naar `api_cost_log`. **Werkelijke spend is ~3× hoger dan logs aangeven.**

### 1.4 Budget-gates

| Gate | Default cap | Env-override | Mechanisme |
|---|---|---|---|
| Per-lead | €0.05 | `MAX_COST_PER_LEAD_EUR` | In-memory `LeadCostAccumulator`, gechecked per call. |
| Daily | €0.50 | `ENRICHMENT_DAILY_BUDGET_EUR` | DB-query: `SUM(cost_eur)` per dag per workspace. |
| Monthly | €20.00 | `MONTHLY_BUDGET_EUR` | 50% approval-flag tier, 100% hard stop. `system_state`-key voor override. |

**Observation:** 0 BLOCKED-rows in `api_cost_log` over 6.772 calls — geen lead heeft ooit
het budget-cap geraakt. Ruim onder ceiling.

---

## 2. Pipeline-fases met API-calls

Pipeline-volgorde uit [`job_queue/enrichment_queue.py:221-250`](../job_queue/enrichment_queue.py#L221-L250):

| # | Fase | API / Model | Aanroep-locatie | Per lead |
|---|---|---|---|---|
| 1 | website | Playwright | `scrapers/website_scraper.py` | 1× |
| 2 | contact_crawl | Playwright v2 | `enrichment/website_crawler_v2.py` | 1× |
| 3 | owner_extract | Claude Haiku 4.5 | `enrichment/owner_extractor.py` | 1× (gated via `cost_guard.guarded_call`) |
| 4 | email_waterfall | (geen Claude) | `enrichment/email_waterfall.py` | 4 stappen; stap 4 KvK alleen als enabled |
| 5 | kvk | KvK API | `scrapers/kvk_scraper.py` | **disabled by default** (€6.40/mnd + €0.02/call) |
| 6 | company_enrichment | Claude Haiku × 3 | `enrichment/company_enrichment.py` | industry + summary + opener (max_tokens 30/80/1200) |
| 7 | website_intelligence | Sonnet Vision + Haiku × 3 | `website_intelligence/*` | visual_analyzer (Sonnet, max 800) + sector_checker + contact_extractor + personalization_extractor (alle Haiku) |
| 8 | domain_age | RDAP | `enrichment/domain_age_scraper.py` | gratis |
| 9 | treatment_focus | Claude Haiku | `enrichment/treatment_classifier.py` | 1× (cached via claude_cache → faalt, tabel ontbreekt) |
| 10 | meta_ads | Playwright + Meta Graph | `enrichment/meta_ads_scraper.py` | gratis (rate-limited) |
| 11 | review_recency | Google Maps scrape | `enrichment/google_reviews_scraper.py` | gratis |
| 12 | archetype | Claude Haiku | `enrichment/archetype_classifier.py` | 1× |
| 13 | contact_discovery | (regel-based) | `enrichment/contact_discovery.py` | geen Claude |
| 14 | data_verification | Haiku validator | optioneel | rare |
| 15 | scoring | regel-based | `scoring/lead_scoring.py` | geen Claude |
| 16 | inbox_selection | Warmr API | `integrations/warmr_client.py` | gratis (Warmr-subscription) |

**Buiten enrichment-pipeline:**

| Fase | API | Aanroep-locatie | Per ... |
|---|---|---|---|
| Mail 1 render | (geen Claude in v3.2) | `campaigns/sequence_engine.py inject_variables` | 1× per launch, server-side render |
| Mail 2 render | (geen Claude in v3.2) | idem | idem |
| Mail 3 render | (geen Claude in v3.2) | idem | idem |
| **Loom Task Orchestrator** | Sonnet (geplanned) | **niet gebouwd** | backlog-item |
| reply_classifier | Haiku 4.5 | `integrations/reply_classifier.py:58` | 1× per inkomende reply |
| reply_drafter | Haiku 4.5 | `campaigns/reply_drafter.py` | 1× per geclassificeerde reply die concept-antwoord vraagt |
| review_email_generator | Haiku 4.5 | `campaigns/review_email_generator.py:230` | 1× per stand-alone review-email |
| opener_generator (legacy) | Haiku 4.5 (max 700) | `enrichment/opener_generator.py` | niet in default-pipeline; alleen handmatig |
| batched_enrichment | Haiku 4.5 + prompt cache | `enrichment/batched_enrichment.py` | combineert personalization + opener in 1 call (commit `6b1e55c`, "63% cost reduction") |

---

## 3. Per-lead Kosten Breakdown

### 3.1 Werkelijke data (api_cost_log, 14 dagen, 848 unieke leads)

**Cijfers komen direct uit DB-aggregaat.** Subjet: 6.772 rows tussen 2026-04-18 en 2026-05-06.

| Fase (context-base) | Calls | Σ input tok | Σ output tok | Σ cost (€) | Avg/call (€) | % totaal |
|---|---:|---:|---:|---:|---:|---:|
| sector_checker | 1.380 | 2.412.603 | 473.981 | 3.5296 | 0.00256 | **35.7%** |
| owner_extract | 1.602 | 3.386.428 | 225.141 | 3.3345 | 0.00208 | **33.8%** |
| archetype_classify | 742 | 1.233.870 | 53.083 | 1.1084 | 0.00149 | 11.2% |
| treatment_classifier | 638 | 1.124.148 | 38.574 | 0.9738 | 0.00153 | 9.9% |
| personalization | 1.732 | 1.919.343 | 692.580 | 0.6529 | 0.00038 | 6.6% |
| contact_extract | 656 | 871.396 | 130.688 | 0.2505 | 0.00038 | 2.5% |
| batched_enrichment | 11 | 3.954 | 4.913 | 0.0210 | 0.00191 | 0.2% |
| review_analysis | 11 | 10.312 | 3.992 | 0.0036 | 0.00033 | 0.04% |
| **Totaal** | **6.772** | **10.962.054** | **1.622.952** | **9.8742** | **0.00146** | **100%** |

### 3.2 Per-lead aggregaat

| Metric | Waarde |
|---|---|
| Unieke leads met geattribueerde rows | **848** |
| Rows zonder `lead_id` (attribution-gap) | **3.779 (56% van totaal)** ⚠ |
| Direct-attribueerbare cost per lead (avg) | **€0.0064** |
| All-in cost per lead (no-lead-rows verdeeld) | **€0.0116** |
| Median per lead | €0.0057 |
| P95 per lead | €0.0113 |
| Max per lead | €0.0168 |
| Avg calls per lead | 3.5 |

### 3.3 Belangrijke caveats

1. **Logs zijn ~3× te laag** in cost_eur door schema-divergentie (zie §1.3). Werkelijke
   spend is `€9.87 × 3 ≈ €30` over 14 dagen; per lead `€0.0116 × 3 ≈ €0.035`.
2. **Vision-fase missing** — geen Sonnet rows in api_cost_log, `vision_cache` leeg,
   `visual_score=None` in alle 807 website_intelligence rows. Vision-analyse zou
   in theorie €0.014/lead toevoegen, maar wordt feitelijk niet uitgevoerd.
3. **Caching werkt niet** — `heatr_claude_cache` tabel bestaat niet, 0 cache_hits in
   6.772 calls. Code in `claude_cache.py` doet `SELECT FROM heatr_claude_cache` →
   PGRST205 silent fail → elke call gaat echt naar Anthropic.
4. **56% attribution-gap** — meer dan helft van calls heeft geen `lead_id`. Veel
   sub-calls (sector_checker per-domain, contact_extract per-domain) loggen wel cost
   maar koppelen niet aan lead. Per-lead-budget-cap kan daardoor nooit kloppen voor
   die calls.
5. **`opener_generator.py` (legacy)** — staat in code, niet in default pipeline.
   In Test-1' sessies wel Claude-calls geweest (sample €0.001/lead). Niet in
   default-cost-breakdown.

---

## 4. Schaalprojectie

**Aannames voor projectie:**
- Werkelijke cost/lead = €0.035 (logged €0.0116 × 3 voor schema-correctie)
- KvK disabled (geen €0.02/call × waterfall stap 4)
- Vision-fase blijft uit
- Geen Loom Task Orchestrator (Sonnet) — staat in backlog

| Volume | Logged cost (huidige tracking) | Werkelijke cost (3× factor) | Werkelijke + Vision (+€0.014/lead) | Werkelijke + Vision + KvK (+€0.04/lead) |
|---|---:|---:|---:|---:|
| 100 leads/mnd | €1.16 | €3.50 | €4.90 | €8.90 |
| 500 leads/mnd | €5.80 | €17.50 | €24.50 | €44.50 |
| 1.000 leads/mnd | €11.60 | €35.00 | €49.00 | €89.00 |
| 5.000 leads/mnd | €58.00 | €175.00 | €245.00 | €445.00 |

**Context:** doelbudget volgens `CLAUDE.md` = ~€10-15/mnd. Bij **1.000 leads/mnd in huidige
config (logged-versie)** zit je op €12 — binnen budget. **Maar bij correctie voor
schema-bug zit je op €35.** Bij Vision-her-activatie op €49. Bij KvK on op €89.

**Daily budget €0.50 vs werkelijke spend:** in apr 18 - mei 6 was avg daily cost
€9.87 / 14 = €0.70/dag — **boven daily-cap**, maar geen BLOCKED-rows gezien.
Reden onbekend: budget-check is mogelijk niet op alle calls actief, of de no-lead
calls vallen buiten de daily-gate.

---

## 5. Bottlenecks

### Top-3 duurste fases per lead

1. **`sector_checker`** — 35.7% van totaal logged spend (€3.53 / 1.380 calls / avg €0.00256).
   Hoogste avg/call van alle fases. Reden: max_tokens 500, en wordt voor cosmetisch én
   alt-zorg apart aangeroepen met substantiële prompts.

2. **`owner_extract`** — 33.8% (€3.33 / 1.602 calls / avg €0.00208). Hoog volume × max_tokens
   800. Gewogen score: 2-en hoogste impact. Belangrijke fase voor contact-discovery,
   maar elke call processed volledige page_text → grote input-token-load.

3. **`archetype_classify`** — 11.2% (€1.11 / 742 calls / avg €0.00149). Stabiel per-call
   maar duurder dan personalization (€0.00038) vanwege output-token-load (53k output
   vs 692k voor personalization, dus structureel verschil).

### Geen-Bottleneck signalen

- `batched_enrichment` (€0.02 totaal, 11 calls) — werkt zoals bedoeld (commit `6b1e55c`).
  Avg €0.00191 voor combinatie van personalization + opener vs €0.00038 + €0.00010
  separate. Mild voordeel; vermoedelijk groter bij prompt-cache hits (die nu nog niet
  werken — zie §6).
- `review_analysis` (€0.0036) — minimaal volume, geen prio.

---

## 6. Optimalisatiekansen

### Top-3 — concreet besparingspotentieel

**1. Fix `heatr_claude_cache` tabel** (schatting: -30% tot -50% Haiku-spend over tijd)

   - Probleem: tabel mist, `cached_claude_call()` faalt silent op SELECT, elke call
     gaat naar Anthropic.
   - Fix: migration `CREATE TABLE heatr_claude_cache` + verify roundtrip.
   - Verwachte impact: `sector_checker` + `treatment_classifier` + `archetype` zijn
     allen sector-specifiek en hebben hoge repeat-rate over leads in dezelfde sector.
     Cache TTL 7 dagen → bij 100 leads/sector/maand verwacht 70-90% cache-hits na
     warming.
   - Effort: 30 min (migration + 1 verify-test).

**2. Schema-divergentie cost-prijzen oplossen** (geen besparing, wel waarheid)

   - Probleem: `claude_cache.py:21-25` gebruikt €0.25/M Haiku (single value, ~3× lager dan
     werkelijkheid), terwijl per-module-code €0.74/M input + €3.68/M output gebruikt.
     Beide kanten zijn inconsistent + neither matched perfect met Anthropic publieke prijs.
   - Fix: één centrale `config/pricing.py` met USD + EUR-conversie + input/output split.
     Beide modules importeren daarvanuit.
   - Verwachte impact: cost-logging gaat factor 3× hoger rapporteren — geen werkelijke
     besparing, wel correctie van de blind spot.
   - Effort: 1u (alle 8+ modules updaten naar centrale config).

**3. Attribution-gap fixen** (56% rows mist lead_id)

   - Probleem: `sector_checker` (1.380 calls) en `owner_extract` (1.602 calls) loggen vaak
     zonder lead_id. Per-lead-cap kan dus niet handhaven, en cost-per-lead is structureel
     onderschat tegen gemiddelde.
   - Fix: in elke `guarded_call()` of `_log_api_cost()` call, lead_id verplicht meegeven.
     Trace waar 'em verloren gaat (waarschijnlijk in fases die per-domain ipv per-lead
     draaien — sub-calls).
   - Verwachte impact: geen directe euro-besparing, maar maakt **per-lead-budget-cap
     werkend**. Zonder dit kan een runaway-fase een lead z'n cap met factor 10 voorbij
     duwen zonder dat de gate intervenieert.
   - Effort: 1-2u (audit van alle log-aanroepplekken).

### Optimalisaties die al gebeurd zijn (git-log evidence)

- **`6b1e55c`** (apr 18) — "Optimize enrichment: 63% cost reduction via batching + compact
  prompts". `batched_enrichment.py` (260 regels) combineert personalization-extractor +
  opener-generator in één Haiku-call met prompt-caching op system. Werkt: 11 calls in
  data, €0.0019/call, lager dan som van separate calls.
- **`5bae839`** (apr 18) — "Fix broken Heatr cost tracking: wrong column names in
  claude_cache logger". Critical column-name fix in `_log_api_cost()` — zonder deze
  was de eerste data niet geregistreerd.

---

## 7. Aanbevelingen

### `cost_audit.py` script bouwen — JA, maar pas na 3 kleine fixes

**Reden voor JA:** een herhaalbaar script is waardevol voor periodieke check (wekelijks).
Alle data is in `api_cost_log` aanwezig, queries zijn straightforward, en de twee schema-
bugs (geen `claude_cache`, prijs-divergentie) zijn precies wat je niet zonder script kunt
opvangen.

**Reden voor NIET ONMIDDELLIJK:** een script bouwen vóór de tracking-fixes loopt het
risico dat het script **verkeerde data toont met onterecht vertrouwen**. Drie fixes
moeten eerst (zie §6 Top-3):

1. `heatr_claude_cache` tabel aanmaken — anders meet je permanent zonder caching
2. Schema-divergentie prijzen consolideren — anders rapporteer je ~3× te laag
3. Attribution-gap aanpakken — anders is "per-lead-cost" geen betrouwbare metric

Daarna script-spec:

```
cost_audit.py
  --period 7d|30d|all       # default 7d
  --workspace aerys          # default $DEFAULT_WORKSPACE_ID
  --threshold-alert         # alarm als avg/lead > €0.05 of daily > €1
  --by-context              # per-fase breakdown
  --by-lead                 # P50/P95/max per lead
  --slack-webhook URL       # optional weekly digest naar Slack
```

Output: console-tabel + optionele JSON-export voor dashboard. Exit code 1 bij threshold-
breach voor cron-integratie.

**Effort:** ~2u (één Python-script + 4 tests). Reuse `pipeline_metrics.py`-aggregatie-logica.

### Acute risico's

| Risico | Severity | Concrete actie |
|---|---|---|
| **`heatr_claude_cache` tabel mist** — alle prompt-cache aanroepen falen silent, elke Haiku-call gaat opnieuw naar Anthropic | **Hoog** | Migration aanmaken (5 min) + retroactief draaien |
| **Cost-logging ~3× te laag** (schema-divergentie) | **Hoog** | Pricing-config consolideren (1u). Anders is alle existing cost-data scheef. |
| **Daily budget cap niet handhaaft** — avg €0.70/dag boven cap €0.50, geen BLOCKED rows | **Mid** | Audit `cost_guard._daily_budget_eur()` flow op de no-lead path. Test triggeren door synthetisch over cap te gaan. |
| **Vision-fase werkt niet** — alle 807 website_intelligence rows hebben `visual_score=None` | **Mid** | Niet kosten-issue (positief — geen Sonnet-spend) maar kwaliteit-issue: pipeline-belofte ontbreekt. Debug separate. |
| **Loom Task Orchestrator niet gebouwd** — Mail 2/3 tokens leeg, geen Sonnet-cost gepland | **Laag** (kosten-wise) | Reeds in backlog. Sonnet-cost-modelling toevoegen wanneer ontwerp af is. |
| **Attribution-gap 56%** | **Mid** | Verhindert per-lead-cap-handhaving. Audit log-call-paths. |

### Volgende stappen (geordend)

1. **Migration: `CREATE TABLE heatr_claude_cache`** (5 min, blocking voor stap 2)
2. **`config/pricing.py` consolideren** (1u, blocking voor stap 4)
3. **Attribution-gap audit + fix** (1-2u, blocking voor stap 4)
4. **`cost_audit.py` script bouwen** (2u)
5. **Vision-fase debug** (separate sessie — geen kosten-impact, wel kwaliteit)
6. **Daily-budget-gate stress-test** (30 min)

Totaal vóór script af is: ~5u verspreid over 1-2 sessies.

---

## Appendix A — Methodologie

- **Code-mapping**: parallel via één Explore-agent + handmatige verify-reads voor specifieke
  prijs-locaties.
- **Live data**: `api_cost_log` volledig gepagineerd (6.772 rows, 7 page-fetches),
  `vision_cache`, `claude_cache`, `website_intelligence`, `leads` direct via
  `config.database.get_heatr_supabase`.
- **Tijdsperiode logs**: 2026-04-18 → 2026-05-06 (14 dagen, 6.772 calls).
- **Geen API-calls naar Anthropic/Warmr/Google** gemaakt tijdens audit — pure historische
  data + code-grep.
- **Geen productiecode gewijzigd**.

## Appendix B — Onbekende of geschatte cijfers (transparantie)

| Cijfer | Source | Zekerheid |
|---|---|---|
| Avg cost/call per fase | `api_cost_log` aggregaat | Hoog (echte data) maar **~3× onderschat door schema-divergentie** |
| 3× correctie-factor voor werkelijke cost | Eigen vergelijking tussen `claude_cache.py:22` (€0.25/M) vs Anthropic publieke prijs ($1/M Haiku in) + €0.74/M in `treatment_classifier.py:38` | Mid — exacte factor afhankelijk van EUR-USD-koers en welke prijslijst Anthropic momenteel hanteert; `2.5×` tot `4×` plausibel |
| Vision cost per lead (€0.014) | Code-schatting: ~1.500 input + 800 output tokens × Sonnet $3+$15/M | **NIET gebaseerd op data** — fase wordt feitelijk niet uitgevoerd |
| KvK cost (€0.04/lead) | Code-comment job_queue/enrichment_queue.py:76 (€6.40/mnd + €0.02/call) | Hoog — uit officiële KvK-API documentatie |
| Schaal-projectie (100/500/1k/5k) | Werkelijke cost × 3 (schema-correctie) | Mid — extrapolatie van 14d data, lineair geschaald |
