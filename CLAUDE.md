# CLAUDE.md — Heatr (by Aerys)

## Wat is Heatr

Heatr is een B2B outbound platform gebouwd voor de BENELUX markt. Het combineert lead discovery, website intelligence, email enrichment, campagnebeheer en conversie analyse in één tool.

**De commerciële strategie achter Heatr:**
De website analyse is de Trojan Horse. Heatr biedt een gratis website review aan als ingang. Dat gesprek brengt pijn in kaart — een verouderde site, geen conversie-optimalisatie, geen chatbot. Die pijn leidt naar een websitebouw opdracht bij Aerys. Dat gesprek brengt nieuwe pijn in kaart — inefficiënte processen, geen automatisering. Die pijn leidt naar een AI audit bij Curio. Heatr is het begin van een volledige klantrelatie.

**De drie diensten die Heatr pitcht namens Aerys:**
1. Nieuwe website — trigger: website score < 40 of CMS ouder dan 5 jaar
2. Conversie optimalisatie — trigger: technisch ok maar geen WhatsApp/booking/chatbot
3. AI audit (via Curio) — trigger: na websitegesprek, pijn in kaart gebracht

---

## Positie in de stack

```
Heatr (dit project)
    ↓ ontdekt + enriched + scoort leads
    ↓ analyseert websites diepgaand
    ↓ POST /api/v1/leads naar Warmr
Warmr (infrastructuurlaag)
    ↓ warmt inboxes op
    ↓ verstuurt campagnes
    ↓ webhook: interested / replied / bounced
Heatr Feedback Processor
    ↓ verbetert ICP scoring automatisch
    ↓ update lead status in CRM-lite
```

Warmr is de motor onder de motorkap. De gebruiker ziet Heatr. Heatr roept Warmr aan via de publieke API voor alle sending.

---

## Doelsectoren

Bron van waarheid = `config/sectors.py` (`ACTIVE_SECTORS` + `get_active_sectors()`). Sectornamen NOOIT hardcoden — altijd via de config.

**Actief (`ACTIVE_SECTORS`):**
- `cosmetische_behandelaars` — botox/filler, laser, huidtherapie, plastisch chirurg, haartransplantatie, permanente cosmetiek, bodycontouring, schoonheidssalons. 11 subcategorieën, 7 SBI-codes. Instagram = positief signaal. **Primaire ICP.**
- `chiropractoren` — chiro/manueel. Klein, 0 SBI-codes (KvK opt-in uit).

**Inactief maar in de data (~390 legacy-leads):**
- `alternatieve_geneeskunde` — acupunctuur, osteopathie, natuurgeneeskunde, coaching e.d. Gedeactiveerd 2026-05-20; leads scoren nog wel door hetzelfde pad.

**Verwijderd uit ICP (2026-07):** `makelaars`, `bouwbedrijven` — `get_sector()` raist ValueError → `icp_match=0` (worden niet benaderd). Oude leads staan er nog.

Discovery deelt een sector op in subcategorieën (elk een scherpe Google Maps-query, bv. `botox kliniek {stad}`) — zie `POST /search` + `get_subcategory_keywords()`. Doelprofiel: 1-15 medewerkers, eigenaar = beslisser, email info@ of naam@praktijk.nl.

---

## Tech stack

| Laag | Tool |
|---|---|
| Taal | Python 3.11+ |
| API | FastAPI |
| Database | Supabase (PostgreSQL) |
| Browser | Playwright (async, headless Chromium) |
| HTTP | httpx (async) |
| Screenshots | Playwright naar Supabase Storage |
| Vision analyse | Claude Sonnet (website screenshots) — draait NIET inline (zie website-intelligence) |
| Email verificatie | **Externe API (Bouncer, EU/GDPR)** — eigen SMTP kan niet (host blokkeert uitgaand IPv4:25) |
| AI enrichment | Claude Haiku (bulk), Claude Sonnet (vision + diepte) |
| Warmr koppeling | httpx naar Warmr publieke API |
| Workers | launchd-daemons: `nl.aerys.heatr.{scraping,enrichment,website}-worker` + `.api` |
| Proxy | Gebouwd, standaard uitgeschakeld |
| Kosten doel | ~€10-15/maand (Claude + Bouncer) |

---

## Bestandsstructuur

```
/heatr
├── CLAUDE.md
├── .env
├── .env.example
├── requirements.txt
├── supabase_schema.sql
├── api/
│   └── main.py
├── scrapers/
│   ├── google_maps_scraper.py
│   ├── website_scraper.py
│   ├── google_search_scraper.py
│   ├── kvk_scraper.py
│   └── directory_scraper.py
├── enrichment/
│   ├── email_waterfall.py
│   ├── email_finder.py
│   ├── email_verifier.py
│   └── company_enrichment.py
├── website_intelligence/
│   ├── analyzer.py
│   ├── technical_checker.py
│   ├── visual_analyzer.py
│   ├── conversion_checker.py
│   ├── sector_checker.py
│   ├── competitor_analyzer.py
│   └── opportunity_classifier.py
├── scoring/
│   ├── lead_scoring.py
│   ├── website_scorer.py
│   ├── icp_matcher.py
│   └── feedback_processor.py
├── campaigns/
│   ├── warmr_sync.py
│   ├── sequence_builder.py
│   ├── campaign_launcher.py
│   └── review_email_generator.py
├── job_queue/                     # (heet job_queue, NIET queue)
│   ├── scraping_queue.py          #   scrape → companies_raw → auto-promote naar leads
│   ├── enrichment_queue.py        #   per-stap loop, per-stap timeout, completed_with_errors
│   ├── website_analysis_queue.py  #   losgekoppelde zware Vision-analyse (eigen worker)
│   └── inbox_recovery.py
├── enrichment/
│   ├── email_waterfall.py · email_verifier.py · verify_api.py   # verify_api = Bouncer
│   ├── company_enrichment.py      #   opener-generatie + normalisatie + QA-gate
│   ├── owner_extractor.py · lead_qualifier.py                    # qualify_and_create_lead
├── integrations/
│   └── warmr_client.py · reply_classifier.py
├── config/
│   ├── sectors.py · scoring_weights.py (LEAD_SCORING_WEIGHTS = DODE code)
│   └── database.py                #   heatr_-prefix wrapper: .table("leads") → heatr_leads
├── scripts/                       #   run_*_worker.py, reverify_email_full.py, rescore_leads_full.py,
│                                  #   promote_companies_to_leads.py, regenerate_openers.py,
│                                  #   pipeline_health.py, batch_readiness_report.py, canary_preview.py
├── deployment/launchd/            #   *.plist voor de workers (survives reboot)
├── frontend-next/                 #   React + Vite (NIET losse .html) — src/pages/*.tsx
└── utils/
    ├── text_normalizer.py         #   normalisatie + validate_opener_sendable (QA-gate)
    ├── pipeline_ops.py · pipeline_metrics.py · launch_readiness.py
    ├── email_sendability.py · deduplicator.py · rate_limiter.py
    └── proxy_manager.py · playwright_helpers.py
```

Alle Supabase-tabellen hebben prefix `heatr_` via `config/database.py`. Migraties draait de gebruiker zelf in de Supabase SQL-editor (MCP heeft geen prod-toegang).

---

## Environment variables

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_service_role_key
SUPABASE_STORAGE_BUCKET=screenshots
ANTHROPIC_API_KEY=sk-ant-...
KVK_API_KEY=                                    # OPT-IN: KvK API kost €6.40/m + €0.02/call. Default UIT.
WARMR_API_URL=https://your-warmr.com/api/v1
WARMR_API_KEY=your_warmr_api_key
WARMR_WEBHOOK_SECRET=your_hmac_secret
PLAYWRIGHT_HEADLESS=true
SCRAPE_DELAY_MIN=2
SCRAPE_DELAY_MAX=6
MAX_CONCURRENT_SCRAPERS=3
GOOGLE_MAPS_MAX_RESULTS=60
PROXY_ENABLED=false
PROXY_URL=http://user:pass@proxy.host:port
PROXY_COUNTRY=NL
EMAIL_VERIFY_TIMEOUT=10
EMAIL_VERIFY_PROVIDER=bouncer                    # externe verify-API; 'none' = uit
BOUNCER_API_KEY=                                 # usebouncer.com (EU/GDPR) — SMTP-host blokkeert :25
MAX_CONCURRENT_ENRICHMENTS=5
ENRICHMENT_STEP_TIMEOUT=120                      # harde per-stap timeout (async hangs)
WEBSITE_ANALYSIS_TIMEOUT=180                     # per website-analyse (aparte worker)
HEATR_ENRICHMENT_DAEMON=true                     # worker blijft draaien bij lege queue
HEATR_WEBSITE_DAEMON=true
CATCHALL_CHECK_ENABLED=true
PAGESPEED_API_KEY=your_google_pagespeed_api_key
SCREENSHOT_ENABLED=true
COMPETITOR_ANALYSIS_ENABLED=true
COMPETITOR_COUNT=3
MIN_SCORE_FOR_WARMR=55                           # herijkt 2026-07 na icp-normalisatie (was 65)
MIN_ICP_MATCH_FOR_WARMR=0.50                     # herijkt 2026-07 (was 0.6)
MIN_WEBSITE_SCORE_FOR_OPPORTUNITY=50
HEATR_ALLOW_RISKY_EMAILS=true                    # 'risky' alleen sendable mét verification_method
AUTO_PUSH_TO_WARMR=false
GDPR_MODE=strict
DEFAULT_WORKSPACE_ID=aerys
HEATR_API_KEY=<random 48+ char string>          # Service-to-service auth (worker, n8n, scripts)
SUPABASE_JWT_SECRET=<from Supabase project>     # Browser auth (Supabase JWT decode)
LEGACY_DEV_TOKEN_ALLOWED=false                  # Tijdelijk "true" tijdens frontend-cutover
ENABLE_CAMPAIGN_SENDS=false                     # Master kill-switch — geen sends tot expliciet aan
```

---

## Auth model

Twee paden, geen "valid-looking" toleranties:

| Caller | Header | Geldig wanneer |
|---|---|---|
| Service (worker, n8n, scripts) | `X-API-Key: <HEATR_API_KEY>` | Constant-time match met env-var |
| Browser (frontend-next) | `Authorization: Bearer <Supabase JWT>` | HS256-decode tegen `SUPABASE_JWT_SECRET`, audience=authenticated |
| Legacy cutover | `Authorization: Bearer dev-token` | Alleen als `LEGACY_DEV_TOKEN_ALLOWED=true` |

Endpoints met `Depends(require_service_key)` ipv `get_workspace` accepteren ALLEEN `X-API-Key` — browser-JWTs worden geweigerd. Voorbeeld: `POST /campaigns/launch` (sends mogen nooit per ongeluk via browser).

---

## Email discovery waterval

Stopt bij eerste succes. Verwachte coverage >80%.

> **Verificatie (2026-07):** de eigen SMTP-verifier is niet functioneel — de host blokkeert uitgaand IPv4:25, en de doelgroep is grotendeels IPv4-only NL-MX. Verificatie loopt daarom via een **externe API (`enrichment/verify_api.py`, Bouncer)**: `verify_email` probeert de API eerst en is **fail-closed** (bij API-fout → `not_checked`, geen SMTP-fallback). Massa-herverificatie: `scripts/reverify_email_full.py`. `is_sendable` accepteert 'risky' alleen mét `verification_method ∈ {smtp, bouncer_api}`.

```
Stap 1 — Website scraper
  Emails op homepage of contactpagina?
  JA → verificatie → klaar | NEE → stap 2

Stap 2 — Pattern generator
  info@, contact@, hallo@, praktijk@, kliniek@, receptie@
  + {voornaam}@{domein} indien naam bekend
  SMTP verificeer elk
  Geldig → klaar | Geen → stap 3

Stap 3 — Google Search fallback
  Query: "{bedrijfsnaam}" "{stad}" email OR "@"
  Query: "{bedrijfsnaam}" "@{domein}"
  Regex over snippets en titels
  Gevonden + geverifieerd → klaar | Niet gevonden → stap 4

Stap 4 — KvK fallback (OPT-IN, alleen NL)
  Vereist KVK_API_KEY env-var. Default UIT (KvK API: €6.40/m + €0.02/call).
  Wanneer aan: correspondentieadres heeft soms email.
  Gevonden → klaar | Niet gevonden → stap 5

Stap 5 — Markeer als not_found
  email_status = 'not_found'
  Score -25 punten
  Niet naar Warmr zonder handmatige goedkeuring
```

---

## Website intelligence — 5 lagen

### Laag 1 — Technisch (max 25 punten)
SSL aanwezig (3), mobile friendly (4), Pagespeed mobiel >50 (5), Pagespeed desktop >70 (3), CMS modern (4), server NL/BE (2), sitemap (1), schema markup (3).

Tools: Google Pagespeed API (gratis), dnspython, httpx headers.

### Laag 2 — Visueel via Claude Sonnet Vision (max 25 punten)
Playwright screenshot → Supabase Storage → Claude Sonnet Vision analyse.

Scores: algemene indruk 0-10 pts, professionele fotografie 0-5, typografie 0-5, kleur coherentie 0-5.

**Claude Vision prompt:**
```
Je bent een senior webdesigner gespecialiseerd in [sector] in Nederland en België.
Analyseer deze website screenshot.

Geef per onderdeel score 1-10 + één concrete zin:
1. ALGEMENE INDRUK — modern en professioneel in 2024?
2. TYPOGRAFIE — leesbaar, modern, consistente hiërarchie?
3. KLEURGEBRUIK — past bij [sector]? Coherent?
4. WITRUIMTE — genoeg ademruimte? Gebalanceerd?
5. AFBEELDINGEN — professioneel? Echte foto's of stock?
6. VERTROUWENSSIGNALEN — reviews, certificaten, team zichtbaar?
7. MOBIELE INDRUK — ziet het responsive-vriendelijk uit?
8. SECTOR AUTHENTICITEIT — past dit bij [sector]?
   [Cosmetische kliniek: luxe, clean, medisch vertrouwen]
   [Alternatieve zorg: warm, toegankelijk, holistisch]

Daarna:
- TOP 3 STERKSTE PUNTEN
- TOP 3 VERBETERPUNTEN (concreet en actionable)
- INSPIRATIE: 1-2 vergelijkbare sites die het beter doen
- OVERALL SCORE: gewogen gemiddelde 1-10

Antwoord in het Nederlands. Wees direct en eerlijk.
```

### Laag 3 — Conversie (max 30 punten)
Primaire CTA boven vouw (5), CTA tekst kracht door Claude (5), telefoon klikbaar (3), WhatsApp knop (4), online booking (6), chatbot of chat (4), contactformulier max 5 velden (3).

**Chatbot analyse:**
- Detecteer platform: Intercom, Drift, Tidio, Landbot, Trengo, WhatsApp Business
- Als chatbot aanwezig: Playwright triggert bot, stuurt testbericht, meet responstijd en kwaliteit
- Als geen chatbot: `chatbot_opportunity = true`

### Laag 4 — Sector specifiek (max 15 punten)
Configureerbaar per sector. Cosmetische klinieken: certificaten (5), behandelingen uitgelegd (5), social proof (5). Bonus: voor/na galerij (+3), Instagram feed (+3). Alternatieve zorg: kwalificaties (5), vergoeding info (5), behandelingen (5). Bonus: gratis kennismaking CTA (+3), persoonlijke foto (+3).

### Laag 5 — Concurrentievergelijking
Top 3 concurrenten ophalen via Google Maps (zelfde stad + sector). Snelle analyse op dezelfde metrics. Marktgemiddelde berekenen. `score_vs_market` berekenen (negatief = slechter dan markt). Dit is de kern van de salespitch.

---

## Dienst classificatie

`website_intelligence/opportunity_classifier.py`

Na analyse automatisch classificeren:

- **Website rebuild**: total_score < 40, of CMS ouder dan 5 jaar, of visual_score < 4
- **Conversie optimalisatie**: conversion_score < 15 bij total_score >= 40, of geen WhatsApp, of geen booking in zorg/kliniek sector
- **Chatbot**: geen chatbot en geen live chat
- **AI audit**: altijd toevoegen na websitegesprek

Priority: urgent (score < 30), high (score < 50), medium (2+ kansen), low (overige).

---

## Review email generator

`campaigns/review_email_generator.py`

Claude Haiku genereert op basis van werkelijke data:

```
Schrijf een email (max 90 woorden) in het Nederlands.
Van: {sender_name} van Aerys
Aan: {contact_name} van {company_name} in {city}

Website score: {total_score}/100
Grootste probleem: {top_issue}
Marktpositie: {score_vs_market} punten onder concurrenten in {city}
Specifieke observatie: {specific_observation}

Regels:
- Begin NIET met 'Ik'
- Stel ÉÉN concrete vraag
- Geen verkooppraatje
- Verwijs naar één specifiek probleem
- Eindig open
```

Verstuurd via warme Warmr inbox.

---

## Lead scoring

> ⚠️ `LEAD_SCORING_FACTORS` / `LEAD_SCORING_WEIGHTS` (config/scoring_weights.py) is **DODE code** — niet gebruikt. Het echte model staat in `scoring/lead_scoring.py`.

**4-dimensie model (`compute_lead_score`, totaal 0-100):**
- `fit_score` (0-40) = `int(icp_match * 40)` + review-count-bonus. Komt volledig uit **icp_match** (+ reviews).
- `data_quality_score` (0-20) — verificatie-confidence.
- `reachability_score` (0-25) — email-status, contact, telefoon, gdpr.
- `personalization_potential` (0-15) — hooks/observations (de facto ~0, datagap).

**`icp_match` (`scoring/icp_matcher.py compute_icp_match`, puur):** genormaliseerd over de ÉVALUEERBARE componenten (SBI + company-size tellen alleen mee als de data bestaat — KvK is opt-in uit). Keyword-saturatie op 5 absolute matches. Leest `kvk_sbi_code` (niet het phantom `sbi_code`). Vóór de fix (2026-07) capte icp op ~0.45 en score op ~51 → gate 65/0.6 onhaalbaar; nu p50≈0.56.

**Launch-gate:** `score ≥ MIN_SCORE_FOR_WARMR` (55) én `icp_match ≥ MIN_ICP_MATCH_FOR_WARMR` (0.50). Volledige readiness (compliance/completeness/cooldown) via `utils/launch_readiness.assess_launch_readiness`. Website-score beïnvloedt de lead-score NIET (slechte site = juist een Aerys-kans). Herscore-runner: `scripts/rescore_leads_full.py`.

---

## Frontend paginas (MVP)

> **Realiteit (2026-07):** de frontend is `frontend-next/` (**React + Vite + TanStack Query**, `src/pages/*.tsx` — Zoeken, Leads, LeadDetail, WebsiteKansen, Campagnes, CampagneLaunch, Inbox, CRM, Analytics, Control). Draait onder `/heatr/*`, praat met de API via `/api`-proxy. De losse `.html`-lijst hieronder is de oude MVP-schets — de intentie/vlakverdeling klopt nog, de bestandsvorm niet.

Design: licht, clean, lichtpaarse gradient accenten. Fonts: Fraunces (headings) + Plus Jakarta Sans (UI). Zelfde taal als Warmr.

```
index.html          ← Login via Supabase Auth
dashboard.html      ← Pipeline stats + website kansen widget
search.html         ← Sector + stad invoer + live scraping progress
leads.html          ← Lead database met filters + bulk acties
lead-detail.html    ← Volledig profiel + website intelligence kaart (tabs)
website-kansen.html ← Alle opportunities gesorteerd op prioriteit + screenshot
campaigns.html      ← Warmr inbox selector + sequence builder + launcher
inbox.html          ← Unified inbox (Warmr replies)
crm.html            ← Pipeline: ontdekt → benaderd → gewonnen
analytics.html      ← Funnel + email coverage + conversie stats
app.js              ← Supabase auth + API calls
```

**Website kansen pagina (kern van de Aerys pitch):**
Per lead: bedrijfsnaam, stad, sector, website score badge (rood/oranje/groen), screenshot thumbnail, top 3 issues, concurrentscore vergelijking, dienst tags [Website] [Conversie] [Chatbot] [AI Audit], acties: [Bekijken] [Markeer OK] [Stuur review email] [Urgent kans].

**Lead detail — website tab:**
Visuele score per laag, screenshot groot, Claude Vision analyse tekst, top 3 verbeterpunten, concurrentenvergelijking staafdiagram, dienst classificatie met actieknoppen.

---

## Campagne engine (via Warmr)

**Inbox selector:** `GET /warmr-api/inboxes?status=ready` — toont beschikbare inboxes met capaciteit.

**Sequence builder:** visuele timeline, variabelen `{{opener}}` `{{first_name}}` `{{company}}` `{{city}}`, spintax `{Hoi|Goedemiddag} {{first_name}}`, A/B toggle, preview met echte lead data.

**Campaign launcher:** leads selecteren → sequence kiezen → inboxes kiezen → push naar Warmr via `POST /api/v1/leads` → Warmr neemt sending over.

---

## FastAPI endpoints

```
POST /search                     → scraping job starten
GET  /jobs/{id}                  → job status
GET  /leads                      → leads met filters
GET  /leads/{id}                 → volledig lead profiel
POST /leads/enrich               → enrichment triggeren
POST /leads/send-to-warmr        → push naar Warmr (dry_run ondersteund)
POST /leads/disqualify           → disqualificeren met reden
GET  /leads/{id}/website         → website intelligence data
POST /leads/{id}/send-review-email → stuur review email via Warmr
PATCH /leads/{id}/website-review → markeer als ok/opportunity/urgent
GET  /website-opportunities      → alle leads met website kans
GET  /icp                        → ICP definities
POST /icp                        → nieuwe ICP aanmaken
GET  /warmr/inboxes              → beschikbare Warmr inboxes
POST /campaigns/launch           → campagne lanceren via Warmr
GET  /analytics/pipeline         → pipeline stats
GET  /analytics/website          → website intelligence aggregaten
POST /webhooks/warmr             → Warmr reply events ontvangen
GET  /sectors                    → beschikbare sectoren
```

---

## Coding conventies

- Python 3.11+, async/await overal
- `httpx.AsyncClient` voor alle HTTP
- Playwright async voor alle browsers
- `supabase-py` voor database
- `anthropic` SDK — Haiku voor bulk, Sonnet voor Vision en diepte
- Type hints + docstring op elke functie
- Vang exceptions per lead — nooit de pipeline stoppen
- Altijd filteren op workspace_id
- Nooit hardcoden: sectornamen, steden, bedrijfsnamen

---

## Operationeel (hands-off pijplijn, onder launchd)

```
scrape (google_maps)            → companies_raw
   ↓  qualify_and_create_lead   (na elke scrape, in scraping_queue)
leads (status='discovered')
   ↓  enrichment-worker         (per-stap timeout, completed_with_errors bij fouten)
      email-waterval → Bouncer-verify → owner → company_enrichment (opener+QA) → scoring → inbox_selection
leads (status='enriched', score/icp gezet)
   ↓  launch-gate (55/0.50) + readiness
verzendbaar   → (nog GEEN sends; kill-switch ENABLE_CAMPAIGN_SENDS=false)
```

- **`website_intelligence` (Vision) is LOSGEKOPPELD** van de inline enrichment (blokkeerde de single-threaded worker synchroon) → draait in `website_analysis_queue` via `scripts/run_website_worker.py`.
- **Fail-closed content-gates** (geen stille naden): e-mail (Bouncer + sendability), lead-kwalificatie (icp/score), **opener** (`validate_opener_sendable`: afgekapt / te kort / kliniek-stem / titel-aanhef / ongetoetste-website-claim → niet opgeslagen).
- **Observability:** `GET /analytics/ops-health` + `scripts/pipeline_health.py` (stall-detectie), `batch_readiness_report.py`, `canary_preview.py`.
- **Inbox:** `GET /reply-inbox` (frontend-contract) + filter-tabs; webhook zet crm_stage `gereageerd`/`verloren` (NIET `beantwoord`/`afgesloten` — bestaan niet in de frontend-enum). Reply-classifier via cron.

## Huidige status (2026-07-15)

Kernpijplijn discovery→enrich→score **operationeel** met echte data (workspace `aerys`, ~900 leads). Belangrijkste feiten voor een volgende sessie:

- **E-mail:** 493 valid via Bouncer; verifier fail-closed. ✅
- **Scoring:** icp-normalisatie gefixt, drempels 55/0.50; ~250 launchbaar. ✅
- **Discovery→leads:** promotie gewired + workers onder launchd (survives reboot). ✅
- **Openers:** 88% was afgekapt (stale, oude max_tokens) → geregenereerd + QA-gate → 98% verzendklaar. ✅
- **Nog open / bewust NIET gedaan:** géén productie-mail (kill-switch aan); migraties 029-031 draait de gebruiker in Supabase; personalisatie-dimensie (hooks) is de facto dood (datagap); de diepe sync-block in `analyze_website` is geïsoleerd, niet opgelost; A3-sequence/canary wacht op expliciete go.
- **Migraties niet zelf uitvoerbaar:** MCP zit op een andere org — DDL plakt de gebruiker in de Supabase SQL-editor.

 Maak fixes klein/afzonderlijk testbaar; workspace-safe; idempotent; bewijs runtime-gedrag met tests + gecontroleerde prod-data. Geen sends zonder expliciete go.

---

*Heatr ontdekt. Warmr verstuurt. Aerys bouwt. Curio automatiseert. Samen is het één klantrelatie.*
