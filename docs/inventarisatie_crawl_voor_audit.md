# Inventarisatie: Heatr-crawl vóór de audit-scorer

Read-only vaststelling. Elke bevinding heeft een vindplaats (`bestand:regel`).
Waar code en config elkaar tegenspreken: beide gemeld, geen conclusie. Geen
oplossingen, geen suggesties. Bevindingen die niet uit de code vast te stellen
zijn, staan als zodanig gemarkeerd.

Kernvraag: kan de audit-scorer draaien op data die de bestaande crawl al
oplevert, of moet er opnieuw gecrawld worden.

---

## 1. Wat de crawl vastlegt

Drie crawl-entrypoints draaien daadwerkelijk (bevestigde callers):
- `scrape_website()` — `scrapers/website_scraper.py:124` (callers: `job_queue/enrichment_queue.py:434`, `job_queue/scraping_queue.py:517`, `enrichment/email_waterfall.py:388`).
- `crawl_website_contact()` — `enrichment/website_crawler_v2.py:58` (caller: `job_queue/enrichment_queue.py:465`).
- `analyze_website()` — `website_intelligence/analyzer.py:33` (callers: `job_queue/website_analysis_queue.py:119`, `job_queue/enrichment_queue.py:661`).

### Ruwe HTML — NIET gepersisteerd
Overal geparsed en weggegooid. `fetch_page_httpx` geeft `response.text` (`website_scraper.py:308`), `fetch_page_playwright` geeft `page.content()` (`:335`); beide gaan door regex-extractors en worden verworpen. Het opgeslagen `raw_result` bevat alleen `domain, pages_visited, used_playwright, cms, tracking_tools, contact_name_raw` (`website_scraper.py:256-263`) — geen HTML. `analyze_website` haalt `page_html` één keer op en deelt het tussen lagen, maar persisteert het nooit (`analyzer.py:80-93`). `crawl_website_contact` bewaart `page.inner_text("body")` als `page_text` (tekst, geen HTML; cap 12000 → 10000 in DB, `website_crawler_v2.py:149`, `enrichment_queue.py:511`).

### Screenshots — capture-code bestaat, maar draait NIET in productie
- Actieve-pad-functie: `visual_analyzer._take_screenshot()` (`visual_analyzer.py:178-204`): desktop-viewport **1280×720** (`:197`), **full_page=True** (`:200`), base64 PNG → Supabase Storage bucket `SUPABASE_STORAGE_BUCKET` (default `screenshots`), object `{domain}.png`, `upsert=true` (`:99-108`).
- **Maar** de aanroepende `analyze_visual` is gated achter `enable_vision` (`analyzer.py:104`), en die parameter staat default `False` (`analyzer.py:40`). Repo-breed komt `enable_vision` alleen voor in `analyzer.py:40,57,104`; **geen enkele caller zet hem op True** (`website_analysis_queue.py:118-128`, `enrichment_queue.py:661-668` geven hem niet mee). → In het live pad wordt **nooit** een screenshot gemaakt of opgeslagen.
- Geen mobiel screenshot en geen device-emulatie in de code (alleen `set_viewport_size` desktop, `visual_analyzer.py:197`). Tegenstrijdigheid: de Vision-prompt vraagt om "MOBIELE INDRUK — ziet het responsive-vriendelijk uit?" (`visual_analyzer.py:132`) terwijl de capture uitsluitend een desktopbeeld levert.
- Dode functies (nul callers): `utils/playwright_helpers.py:236 take_screenshot()` (schrijft `/tmp/screenshots/{domain}.png`, `:253`) en `:257 upload_screenshot_to_supabase()`.

### Netwerk-requests — NIET GEVONDEN
Repo-breed geen `page.on("request")`, `page.on("response")`, `page.route(...)`, `context.route`, `record_har`/HAR. Geen enkele netwerk-request wordt onderschept, gelogd of opgenomen. Wél worden tracking-tools uit de **HTML** herkend (fingerprints GA, GTM, FB Pixel, HubSpot, LinkedIn Insight, Hotjar, Clarity, Intercom — `website_scraper.py:85,425`) → opgeslagen in `leads.tracking_tools` (`enrichment_queue.py:453`). Dat is aanwezigheid-in-HTML, geen netwerkgedrag/timing.

### Console-output — NIET GEVONDEN
Geen `page.on("console")` in de repo.

### Response headers — alleen in-memory, NIET gepersisteerd
Enige capture: `website_scraper.py:308` `return response.text, dict(response.headers)` (httpx, hoofddocument). Deze headers gaan uitsluitend naar `detect_cms()`, dat alleen `x-powered-by` leest (`:408-412`). Het Playwright-pad geeft `{}` als headers (`:207,:218`). `technical_checker` doet een `HEAD` op `/sitemap.xml` maar leest alleen `status_code`, niet de headers (`technical_checker.py:87`). Headers worden nergens naar DB/Storage geschreven; alleen de afgeleide `cms`-string wordt bewaard.

### Geëxtraheerde velden (met opslaglocatie)
Uit `scrape_website` → tabel `enrichment_data`, `source='website'` (`website_scraper.py:632-647`):

| Veld | Extractie | Opgeslagen |
|---|---|---|
| emails (GDPR-gefilterd) | `:355` + filter `:233-245` | `enrichment_data.email_candidate` = **alleen eerste** (`:638`) |
| cms | `:395 detect_cms` | `leads.cms_detected` (`enrichment_queue.py:443`) + `raw_result.cms` |
| tracking_tools | `:425` | `leads.tracking_tools` (`:453`) + `raw_result` |
| has_instagram | `:555` | `leads.has_instagram` (`:445`) |
| has_online_booking | `:565` | `leads.has_online_booking` (`:447`) |
| has_whatsapp | `:560` | `leads.has_whatsapp` (`:449`) |
| phone | `:575` | `leads.phone` (`:451`) |
| contact_name | `:581` | alleen `raw_result.contact_name_raw` (`:262`) — niet naar `leads` |
| has_cookie_banner | `:569` | **NERGENS** — berekend en weggegooid |

Uit `crawl_website_contact` → `enrichment_data`, `source='contact_crawl_v2'` (`enrichment_queue.py:499-514`): emails (`leads.email` eerste), phones, socials (instagram/facebook/linkedin naar `leads.*_url`; youtube/tiktok alleen `raw_result`), `schema_org` JSON-LD (`raw_result.schema_org`), `page_text` (`raw_result.page_text`).

### Pagina's per site & scope
- `scrape_website`: **max 3** pagina's (`_MAX_PAGES_PER_DOMAIN = 3`, `website_scraper.py:50`) — homepage + max 2 contact-subpagina's (`contact_page_urls[:2]`, `:186`). URL-keuze via `find_contact_page_links` (`:443`) tegen `DUTCH_CONTACT_PAGE_PATTERNS` (`playwright_helpers.py:77-93`). **Niet sitemap-gedreven.**
- `crawl_website_contact`: **max 3** paths (`MAX_PATHS = 3`, `website_crawler_v2.py:105`), 30s totaal (`:60`) — homepage + vaste fallback-lijsten (`_CONTACT_FALLBACKS`/`_TEAM_FALLBACKS`, `:31-32`). **Niet sitemap-gedreven.**
- `analyze_website`: **homepage-only** voor de HTML-lagen (`analyzer.py:80/89`), + max één team-pagina via `contact_extractor._TEAM_PATHS` (`contact_extractor.py:21-26,58-68`). `HEAD /sitemap.xml` is alleen een bestaanscheck (`technical_checker.py:87`).
- Scope: overal hard `https://{domain}` (uit `leads.domain`). Geen subdomein-volging, geen externe links, geen link-graaf.

### Browsercontext
Vers per run; **geen** `storage_state`/cookies/sessie hergebruikt tussen runs. `new_browser_context` bouwt telkens nieuwe browser+context met random UA (`playwright_helpers.py:210`), random viewport (`:204`) en stealth init-script (`:227-231`). Context wél hergebruikt tússen subpagina's van dezelfde run.

---

## 2. Wat er nu gescoord wordt

Orkestratie in `analyze_website()` (`analyzer.py:33-222`). Totaal:
`total = technical_score + (visual_score or 0) + conversion_score + sector_score`, daarna `min(total, 100)` (`analyzer.py:138-144`).

**Gevolg (vaststelling):** omdat `visual_score` altijd `None` is (zie §5), draagt Laag 2 (25 pt) live nooit bij → de feitelijke `total_score`-range in productie is technical(≤25) + conversion(≤30) + sector(≤15) = **max 70**, niet 100.

### Laag 1 — Technical (max 25) · `technical_checker.py:21-143`, cap `:142`
SSL +3 (`:52-56`), CMS +4 (`:68-73`), schema-markup +3 (`:78-81`), sitemap +1 (`:86-91`), server-land NL/BE/DE/LU +2 (`:98-105`), PageSpeed mobile ≥50 +5 en desktop ≥70 +3 (`:126-129`), mobile_friendly +4 (`:134-136`). PageSpeed alleen als `PAGESPEED_API_KEY` gezet (`:112`), anders 0 voor die checks (`:139-140`).

### Laag 2 — Visual (max 25) · `visual_analyzer.py:38-175` — DRAAIT LIVE NIET
Score-mapping `_calculate_visual_score` = `overall_1_to_10/10 * 25` (`:213-216`); alleen `overall_score` bepaalt de punten. Draait nooit live (`enable_vision=False`, §5) → `visual_score` blijft `None`.
- **Tegenstrijdigheid config↔code:** `config/scoring_weights.py:148-163` beschrijft 4 gewogen dimensies (overall 10, fotografie 5, typografie 5, kleur 5). De code gebruikt geen van deze gewichten (`visual_analyzer.py:213-216`).

### Laag 3 — Conversion (max 30) · `conversion_checker.py:37-197`, cap `:182`
CTA above-fold +5 (`:79-93`), telefoon klikbaar +3 (`:98-101`), WhatsApp +4 (`:106-110`), online booking +6 (`:114-129`), chatbot/chat +4 (`:134-139`), contactform ≤5 velden +3 / >5 +1 (`:150-166`), CTA-tekststerkte +5/+2 (`:170-180`).
- **Tegenstrijdigheid config↔code:** `config/scoring_weights.py:177-179` stelt dat Claude Haiku de CTA-copy scoort (1-5); de code gebruikt een keyword-match (`conversion_checker.py:170-180`), geen Claude.

### Laag 4 — Sector (max 15) · `sector_checker.py:57-242`
Claude Haiku (`_HAIKU_MODEL`, `:44`) classificeert sitetekst in tier A/B/C/D op `website_signals` uit `config/sectors.py` (`:90-92`); `_TIER_TO_POINTS = {A:15,B:10,C:5,D:0}` (`:54`, toegepast `:231`). Kostenbewaking vooraf (`guarded_call`, `:113-122`) kan de call skippen → 0.
- **Tegenstrijdigheid config↔code:** `config/scoring_weights.py:207-211` beschrijft per-`must_have`-item punten; de code gebruikt een 4-tier Haiku-oordeel op `website_signals`, per-signaal `checks` krijgen `points: 0` (`:201`).

### Laag 5 — Competitor (bonus, telt NIET mee) · `competitor_analyzer.py:31-149`
`score_vs_market` (`:104`), `market_avg_score` (`:95`), `lead_rank` (`:100`). Telt niet mee in `total_score`. **Draait live niet** (§5).

### Outputvorm & schrijf-paden
- **A. `website_intelligence` — UPSERT `on_conflict="lead_id"`** (`analyzer.py:173-191`): `total_score, technical_score, visual_score, conversion_score, sector_score, opportunity_types, priority, technical_details, conversion_details, sector_details, personalization, team_contacts, opportunity_reasons, analyzed_at`.
- **B. `leads` — UPDATE** (`analyzer.py:196-211`): `website_score = total_score` (`:198,211`), + `company_positioning, personalization_hooks, personalization_observations, booking_system`.
- **C. Competitor-writer** (`competitor_analyzer.py:123-139`) — niet live geraakt.

### Herberekenbaar / idempotent / overschrijven
- **Overschrijven:** ja — UPSERT op `lead_id` (`analyzer.py:173,191`), `leads.website_score` is UPDATE (`:211`). Een nieuwe run overschrijft de vorige.
- **Re-score-pad (tijdgestuurd):** `website_analysis_queue._find_next_eligible_lead()` slaat leads over met een WI-rij binnen `WEBSITE_REANALYSIS_DAYS` (default 30, `:39,:244-249`); daarna weer eligible.
- **Idempotentie:** niet gegarandeerd qua waarde — HTML wordt elke run opnieuw gefetcht en live checks (PageSpeed, Haiku) zijn niet deterministisch. Rij-identiteit wel (upsert op lead_id).
- `scoring/website_scorer.py::score_website()` (`:16-38`) is puur lezend (leest `total_score`, herberekent niets) en is **nergens aangeroepen** (geïmporteerd in `lead_scoring.py:20`, 0 call-sites).
- `scripts/rescore_leads_full.py` herberekent alleen `icp_match`+lead-`score`, **raakt `website_score`/`website_intelligence` niet aan**. Geen batch-herberekening van de website-score gevonden.

---

## 3. Het datamodel

Geen aparte, genormaliseerde "site"-entiteit. De website is een `domain text`-kolom op `companies_raw` (`supabase_schema.sql:56`), `leads` (`:95`) en `website_intelligence` (`:181`).

Keten: `companies_raw` (pre-lead, uniek op `(workspace_id, lower(domain))`, `migrations/021_datamodel_integrity.sql:75-77`) → `leads.company_raw_id references companies_raw(id)` (`supabase_schema.sql:93`) → scanresultaat direct aan de lead via `website_intelligence.lead_id`.

### `website_intelligence` (`supabase_schema.sql:177-240`) — kolommen met analyse-output
Scores `total_score/technical_score/visual_score/conversion_score/sector_score/score_vs_market` (`:184-189`); Laag 1 `has_ssl,is_mobile_friendly,pagespeed_mobile,pagespeed_desktop,cms_detected,server_country,has_sitemap,has_schema_markup,technical_data jsonb` (`:192-200`); Laag 2 `screenshot_url, screenshot_local_path, claude_vision_analysis jsonb` (`:203-205`); Laag 3 `has_primary_cta,cta_above_fold,cta_text,cta_strength_score,phone_clickable,has_whatsapp,has_online_booking,has_chatbot,chatbot_platform,chatbot_response_time,contact_form_fields,conversion_data jsonb` (`:211-222`); Laag 4 `sector_data jsonb` (`:225`); Laag 5 `competitor_data jsonb` (`:228`), `local_competitors_in_db/_higher_rating` (`migrations/006_warmr_sequence_fields.sql:22-24`); `opportunity_types text[], opportunity_priority text, chatbot_opportunity bool` (`:232-235`).

**Vaststelling:** de kolommen `screenshot_url, screenshot_local_path, claude_vision_analysis, visual_score` bestaan in het schema maar worden in productie niet gevuld (Vision draait niet, §5).

### Scan-historie vs. overschrijven
- **Code-intentie = overschrijven:** UPSERT `on_conflict="lead_id"` (`analyzer.py:173,191`); mutaties `.update().eq("lead_id",…)` (`competitor_analyzer.py:123-128`, `api/main.py:1365`).
- **Schema-constraint ontbreekt:** géén unique/PK op `website_intelligence.lead_id` — alleen niet-unieke index (`supabase_schema.sql:248-249`); in migraties geen `UNIQUE`/`CREATE UNIQUE INDEX` op `website_intelligence(lead_id)` gevonden. Drift met het `on_conflict="lead_id"` in de code. De queue leest defensief mogelijk meerdere WI-rijen per lead en neemt `max(analyzed_at)` (`website_analysis_queue.py:238-249`).
- **Echte historie bestaat elders (append-only):** `enrichment_data` ("one row per step", `.insert`, `supabase_schema.sql:260`); `heatr_email_verifications` ("Nooit overschrijven", `migrations/030:22-23,45-46`); `heatr_lead_outreach_snapshots` — per-lead snapshot-historie met o.a. `website_hash, score_vs_market` (`migrations/004_discovery_recontact_replies.sql:26-40`); `heatr_teardown_pages` — per-lead deelbare website-analyse-pagina (`migrations/029_teardown_pages.sql:15-30`) + `heatr_page_views` (`029:42-51`).

### Drift (gemeld, geen conclusie)
- **FK `website_intelligence.lead_id`:** inline gedeclareerd (`supabase_schema.sql:180`), maar in `migrations/021_datamodel_integrity.sql:134-136` **uitgecommentarieerd**; `api/main.py:1383` behandelt de FK als afwezig ("kunnen we geen inner-join doen via PostgREST").
- **Kolomnaam-drift code↔schema (`website_intelligence`):** code schrijft `technical_details/conversion_details/sector_details` (`analyzer.py:184-186`), schema heet `technical_data/conversion_data/sector_data` (`supabase_schema.sql:200,222,225`). `personalization, team_contacts, opportunity_reasons, priority` (geschreven `analyzer.py:187-189`), `review_status` (`api/main.py:1365`), `page_text` (gelezen `enrichment_queue.py:1274`): **niet gevonden** in schema/migraties.
- **Allowlist↔schema:** `competitor_cache` staat in `_HEATR_TABLES` (`config/database.py:34`) maar heeft geen `create table` in schema/migraties. `icp_definitions`/`icp_feedback` staan in het schema maar niet in de allowlist. `domain_cache` dubbel gedefinieerd met afwijkende defaults (`supabase_schema.sql:528-534` vs `migrations/031_domain_cache.sql:11-17`).

---

## 4. De pipeline

### Trigger
**Geen expliciete enqueue / geen job-tabel voor website-analyse — pull/self-selecting.** `website_analysis_queue._find_next_eligible_lead()` (`:184-256`) kiest zelf: hoogste `score`, `domain != null`, `email_status IN (valid,risky,catch_all,catchall_risky)` (`:208`), niet-terminale status (`:219-222`), geen WI-rij binnen 30 dagen (`:229-254`). Geen "na scrape"/"na enrichment"-hook. Twee drivers roepen `process_next_website_analysis()`: n8n-workflow 04 elke minuut → `POST /website-intelligence/process-next` (`api/main.py:5116-5127`), en de launchd-website-worker (`scripts/run_website_worker.py:44-84`).

**Vision losgekoppeld:** `website_intelligence` is uit de default `enrichment_types` gehaald (`enrichment_queue.py:89-93`); de inline stap-handler bestaat nog (`:646-671`) maar draait alleen als `website_intelligence` handmatig meegegeven wordt. In beide paden zonder `enable_vision=True` (§5).

### Waar het draait
Launchd: `nl.aerys.heatr.api` (uvicorn :8001), `.scraping-worker`, `.enrichment-worker` (`HEATR_ENRICHMENT_DAEMON=true`), en de **website-worker** `deployment/launchd/nl.aerys.heatr.website-worker.plist` (`scripts/run_website_worker.py`, `HEATR_WEBSITE_DAEMON=true`, `KeepAlive=true`). n8n 03/04 zijn een parallelle HTTP-driver naar dezelfde endpoints.

### Externe API's + keys (in het `analyze_website`-pad)
- **ip-api.com** (server-land, geen key) — `technical_checker.py:99`.
- **Google PageSpeed** — `technical_checker.py:117`, key `PAGESPEED_API_KEY` (`:18`, `.env.example:62`).
- **Anthropic Sonnet Vision** (feitelijk uit) — `visual_analyzer.py:142`, model `claude-sonnet-4-6`; key `ANTHROPIC_API_KEY`.
- **Anthropic Haiku** — sector (`sector_checker.py:165`), personalisatie (`personalization_extractor.py:95`), contact (`contact_extractor.py:139`); key `ANTHROPIC_API_KEY`.
- **Supabase Storage** — `visual_analyzer.py:103` (alleen als Vision draait).
Bredere enrichment (aparte stappen): **Bouncer** `BOUNCER_API_KEY` (`verify_api.py:63`), **KvK** `KVK_API_KEY` default uit (`enrichment_queue.py:80`), **RDAP** (geen key, `domain_age_scraper.py:41`), **Meta Ad Library** `META_AD_LIBRARY_TOKEN` (`meta_ads_scraper.py:36` — **niet gevonden in `.env.example`**), **Resend** `RESEND_API_KEY`. Geen aparte "DNS"-API (RDAP + MX zijn het dichtstbij). `config/pricing.py` bevat geen keys.

### Rate limiting / queueing / concurrency
- Token-bucket `utils/rate_limiter.py` (`RATE_LIMITS:29-89`). **De buckets van het website-pad (`pagespeed_api`, `claude_sonnet`, `claude_haiku`) zijn gedefinieerd maar hebben nul consumers** — die calls lopen niet via de rate-limiter, alleen via `cost_guard` + timeouts. Anomalie: `directory_scraper.py:86` roept `wait_for_token("website")` aan terwijl `"website"` niet in `RATE_LIMITS` staat (`ValueError`-pad, `rate_limiter.py:281-284`).
- Concurrency: scrapers `MAX_CONCURRENT_SCRAPERS=3`, enrichment `MAX_CONCURRENT_ENRICHMENTS=5`; **website-worker heeft géén semaphore/MAX_CONCURRENT** — strikt sequentieel, 1 lead per iteratie.
- Timeouts: website-analyse hard `WEBSITE_ANALYSIS_TIMEOUT=180s` (`website_analysis_queue.py:42,118`), httpx 15s, PageSpeed 30s, Playwright goto 20s; dedup-venster 30 dagen.

### Lead → outreach (naar Warmr)
- **`POST /leads/send-to-warmr`** (`api/main.py:866`): gates `compliance_check` → `email_status IN (verified,catch_all)` → `score >= MIN_SCORE_FOR_WARMR` (inline default 65, **icp niet gecheckt**) → 90-dagen-cooldown → dispatcher → enrollment.
- **`POST /campaigns/launch`** (`api/main.py:1725`, service-key only): kill-switch `ENABLE_CAMPAIGN_SENDS` → completeness (`HARD_REQUIRED_FIELDS = archetype,score,sector`) → cooldown → actieve-campagne-block (fail-closed) → push.
- Readiness `utils/launch_readiness.py:98-115`: `MIN_SCORE_FOR_WARMR` + `MIN_ICP_MATCH_FOR_WARMR` als block; **website-analyse-aanwezigheid alleen review, niet-blokkerend** (`:110-115`). Default-drift: `.env.example:70-71` = 55/0.50; hardcoded fallbacks in `lead_scoring.py`/`launch_readiness.py` = 65/0.6.

---

## 5. Gaten (waar het antwoord "niet aanwezig" is)

- **Ruwe HTML:** niet gepersisteerd (§1). Wordt elke analyse-run opnieuw gefetcht.
- **Screenshots:** in het live pad niet geproduceerd — `analyze_visual` gated achter `enable_vision=False`, geen caller zet True (`analyzer.py:40,104`; `website_analysis_queue.py:118-128`; `enrichment_queue.py:661-668`). Capture-code bestaat maar is desktop-only, geen mobiel (`visual_analyzer.py:197`).
- **Netwerk-requests:** niet onderschept/gelogd — geen `page.on`/`page.route`/HAR in de repo.
- **Console-output:** niet opgevangen — geen `page.on("console")`.
- **Response headers:** niet gepersisteerd; alleen transient voor CMS-detectie (`website_scraper.py:308,408-412`).
- **Visual score (Laag 2) & competitor (Laag 5):** worden live niet berekend → `visual_score` altijd `None`, `score_vs_market` leeg in het live pad (§2/§5).
- **`has_cookie_banner`:** berekend en weggegooid, nergens opgeslagen (`website_scraper.py:569`).
- **Meerdere pagina's / diepe crawl:** max 3 pagina's, niet sitemap-gedreven, geen link-graaf (§1).
- **Scan-historie voor de website-analyse:** geen — code overschrijft (upsert op `lead_id`); geen unique-constraint (drift). Historie bestaat alleen voor andere artefacten (`lead_outreach_snapshots`, `enrichment_data`, `teardown_pages`).
- **Batch-herberekening website-score:** niet gevonden.
- **Rate-limiting op de website-API-calls:** buckets gedefinieerd, niet geconsumeerd (§4).

---

## Kernvraag — beantwoord

**Gedeeltelijk voldoende.**

Wat de crawl al persisteert en direct herbruikbaar is als scorer-input: de gestructureerde signalen op `leads` en `website_intelligence` — Laag 1 (technisch: SSL, CMS, PageSpeed, server-land, sitemap, schema), Laag 3 (conversie: CTA, telefoon, WhatsApp, booking, chatbot, formulier), Laag 4 (sector-tier), plus geëxtraheerde velden (tracking-tools-aanwezigheid, booking/whatsapp/instagram-flags, telefoon, e-mail).

Wat de crawl NIET oplevert en wat een tweede, prospect-facing scorer juist nodig heeft voor pitchmateriaal: (1) **screenshots** — die worden in productie helemaal niet gemaakt (Vision gated-off), en de capture-code die er is, is desktop-only zonder mobiel; (2) **netwerkgedrag** — requests, response-headers en console worden niet vastgelegd, waardoor een pre-consent tracking-check niet op bestaande data kan draaien (alleen aanwezigheid-van-trackers-in-HTML is bekend, niet hun timing/gedrag); (3) **ruwe HTML** wordt niet bewaard, maar is triviaal opnieuw op te halen (de analyzer fetcht elke run opnieuw).

De audit-scorer kan dus de reeds-opgeslagen technische/conversie/sector-signalen hergebruiken, maar kan **niet** volledig draaien op de huidige gepersisteerde output: een visuele rubric en een netwerk/tracking-rubric vereisen nieuwe crawl-instrumentatie (screenshot-capture incl. mobiel, en netwerk/response-capture). De eerste bouwtaak ligt bij de crawl, niet bij de scorer.
