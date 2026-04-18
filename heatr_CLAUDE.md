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

## Eerste doelsectoren

**Sector 1 — Alternatieve Zorg**
Fysiotherapeuten, osteopaten, acupuncturisten, homeopaten, psychologen (privépraktijk), coaches, diëtisten, energetisch therapeuten, manueel therapeuten.
- 1-5 medewerkers, eigenaar = behandelaar = beslisser
- Email: info@ of naam@praktijk.nl
- KvK SBI: 86.90, 86.21, 86.22, 86.23, 85.59

**Sector 2 — Cosmetische Klinieken**
Botox/filler klinieken, laserklinieken, huidtherapiepraktijken, schoonheidsklinieken premium.
- 2-15 medewerkers, eigenaar bereikbaar
- Instagram aanwezigheid = positief signaal
- KvK SBI: 86.21, 96.02, 96.01

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
| Vision analyse | Claude Sonnet (website screenshots) |
| Email verificatie | Eigen SMTP + MX via dnspython |
| AI enrichment | Claude Haiku (bulk), Claude Sonnet (vision + diepte) |
| Warmr koppeling | httpx naar Warmr publieke API |
| Proxy | Gebouwd, standaard uitgeschakeld |
| Kosten doel | ~€10-15/maand (Claude API) |

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
├── queue/
│   ├── scraping_queue.py
│   ├── enrichment_queue.py
│   └── website_analysis_queue.py
├── integrations/
│   └── warmr_client.py
├── config/
│   ├── sectors.py
│   └── scoring_weights.py
└── utils/
    ├── proxy_manager.py
    ├── rate_limiter.py
    ├── deduplicator.py
    └── playwright_helpers.py
```

---

## Environment variables

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_service_role_key
SUPABASE_STORAGE_BUCKET=screenshots
ANTHROPIC_API_KEY=sk-ant-...
KVK_API_KEY=your_kvk_api_key
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
MAX_CONCURRENT_ENRICHMENTS=5
CATCHALL_CHECK_ENABLED=true
PAGESPEED_API_KEY=your_google_pagespeed_api_key
SCREENSHOT_ENABLED=true
COMPETITOR_ANALYSIS_ENABLED=true
COMPETITOR_COUNT=3
MIN_SCORE_FOR_WARMR=65
MIN_ICP_MATCH_FOR_WARMR=0.6
MIN_WEBSITE_SCORE_FOR_OPPORTUNITY=50
AUTO_PUSH_TO_WARMR=false
GDPR_MODE=strict
DEFAULT_WORKSPACE_ID=aerys
```

---

## Email discovery waterval

Stopt bij eerste succes. Verwachte coverage >80%.

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

Stap 4 — KvK fallback (alleen NL)
  Correspondentieadres heeft soms email
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

## Lead scoring factoren

```python
LEAD_SCORING_FACTORS = {
    'has_valid_email': 25,
    'email_type_role': 8,
    'email_discovery_website': 5,
    'website_quality': 8,
    'has_kvk_data': 7,
    'company_size_fit': 8,
    'google_rating_above_4': 8,
    'google_review_count': 5,
    'has_contact_name': 5,
    'cms_detected': 4,
    'has_instagram': 4,
    'has_online_booking': 5,
    'tracking_tools_detected': 3,
    'gdpr_safe': 3,
    'catchall_penalty': -10,
}
# Website score beinvloedt lead score NIET direct
# Slechte website = juist goede kans voor Aerys
```

---

## Frontend paginas (MVP)

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

## Huidige status

- [ ] Supabase schema gemigreerd
- [ ] Sector configs geladen
- [ ] google_maps_scraper.py gebouwd
- [ ] website_scraper.py gebouwd (NL logica)
- [ ] google_search_scraper.py gebouwd (email fallback)
- [ ] kvk_scraper.py gebouwd
- [ ] directory_scraper.py gebouwd
- [ ] email_waterfall.py orkestreert alle stappen
- [ ] email_verifier.py met catch-all detectie
- [ ] company_enrichment.py met Claude Haiku
- [ ] website_intelligence/ volledig gebouwd
- [ ] lead_scoring.py gebouwd
- [ ] website_scorer.py gebouwd
- [ ] opportunity_classifier.py gebouwd
- [ ] icp_matcher.py gebouwd
- [ ] feedback_processor.py gebouwd
- [ ] warmr_client.py gebouwd en getest
- [ ] review_email_generator.py gebouwd
- [ ] FastAPI compleet
- [ ] Frontend gebouwd (alle paginas)
- [ ] End-to-end test: Google Maps → enrich → website analyse → Warmr

---

*Heatr ontdekt. Warmr verstuurt. Aerys bouwt. Curio automatiseert. Samen is het één klantrelatie.*
