---
name: heatr
description: Use this skill whenever working on Heatr — the B2B lead generation and website intelligence platform built by Aerys. Triggers on any mention of: lead generation, website analyse, scraping, email waterval, website intelligence, ICP matching, Heatr, website kansen, review email, conversie optimalisatie analyse, competitor analyse, opportunity classifier, of wanneer de gebruiker vraagt om scrapers, enrichment, website scoring of campagne integratie met Warmr te bouwen. Ook triggeren op: "voeg sector toe", "analyseer website", "stuur review email", "website score", "Claude Vision analyse", "Playwright scraper NL". Dit is het centrale kennisbestand voor alle Heatr ontwikkeling — gebruik het bij elke vraag over dit project.
---

# Heatr Skill

Heatr is een B2B outbound platform voor de BENELUX markt. Het ontdekt leads, analyseert hun websites diepgaand, enriched contactgegevens, en verstuurt gepersonaliseerde campagnes via Warmr.

**De commerciële kern:**
Website analyse = Trojan Horse → gratis review aanbod → pijn in kaart → website opdracht bij Aerys → AI audit bij Curio.

**Lees altijd eerst CLAUDE.md** voordat je iets bouwt. Die bevat het volledige schema, alle conventies, sector configs en de complete architectuur.

---

## Projectstructuur snel overzicht

```
/heatr
├── CLAUDE.md                    ← Altijd eerst lezen
├── scrapers/                    ← Google Maps, website, Google Search, KvK, directories
├── enrichment/                  ← Email waterval (4 stappen), verificatie, Claude summaries
├── website_intelligence/        ← 5-laags website analyse + Claude Vision + competitor
├── scoring/                     ← Lead scoring, website scoring, ICP matcher, feedback
├── campaigns/                   ← Warmr sync, sequence builder, launcher, review emails
├── queue/                       ← Scraping + enrichment + website analysis queues
├── integrations/                ← Warmr API client
├── config/                      ← Sector configs, scoring weights
├── utils/                       ← Playwright helpers, proxy (optioneel), rate limiter
├── api/                         ← FastAPI alle endpoints
└── frontend/                    ← Vanilla HTML/CSS/JS + Supabase Auth
```

---

## De vijf website intelligence lagen

Dit is het hart van Heatr. Elke website krijgt een score 0-100 over vijf lagen:

| Laag | Max | Tool |
|---|---|---|
| 1. Technisch | 25 | Google Pagespeed API + httpx headers |
| 2. Visueel | 25 | Playwright screenshot + Claude Sonnet Vision |
| 3. Conversie | 30 | Playwright + regex detectie |
| 4. Sector specifiek | 15 | Configureerbaar per sector |
| 5. Concurrenten | bonus | Google Maps top 3 + snelle analyse |

Score < 40 = website rebuild kans. Score < 60 maar conversie laag = conversie optimalisatie kans. Geen chatbot = chatbot kans.

---

## Email waterval (4 stappen)

Stap 1: Website scraper (NL contactpagina logica)
Stap 2: Pattern generator (info@, contact@, hallo@, praktijk@, etc.)
Stap 3: Google Search fallback (eigen Playwright, max 10/uur)
Stap 4: KvK API fallback (alleen NL)
Geen resultaat: email_status = 'not_found', score -25

---

## Sector configs

Twee actieve sectoren in `config/sectors.py`:
- `alternatieve_zorg` — fysiotherapeuten, osteopaten, psychologen, coaches, etc.
- `cosmetische_klinieken` — botox, laser, huidkliniek, schoonheidskliniek, etc.

Nieuwe sector toevoegen = nieuwe entry in sectors.py, geen nieuwe code.

---

## Warmr integratie

Heatr gebruikt Warmr als sending infrastructuur via de Warmr publieke API:
- `GET /warmr-api/inboxes?status=ready` — beschikbare inboxes ophalen
- `POST /warmr-api/leads` — leads pushen met enrichment data
- `POST /warmr-api/campaigns` — campagne aanmaken
- Warmr webhooks → Heatr `POST /webhooks/warmr` → feedback_processor.py

Leads gaan alleen naar Warmr als: gdpr_safe=true, email_status in (valid, risky), score >= MIN_SCORE_FOR_WARMR, icp_match >= MIN_ICP_MATCH_FOR_WARMR, niet al in actieve campagne.

---

## Anti-detectie regels (Playwright)

Altijd toepassen via `utils/playwright_helpers.py`:
- Roteer user agent (Chrome Mac/Windows realistisch)
- Locale: nl-NL, timezone: Europe/Amsterdam
- Accept-Language: nl-NL,nl;q=0.9,en;q=0.8
- Random mouse movements voor klikken
- Random delays: SCRAPE_DELAY_MIN tot SCRAPE_DELAY_MAX seconden
- Max 60 Google Maps resultaten per browser context
- Max 10 Google Search queries per uur
- Bij CAPTCHA: pauzeer 2 uur, ga door zonder die stap

---

## GDPR regels (altijd handhaven)

- Alleen zakelijke emailadressen (role emails: info@, contact@, etc.)
- Nooit persoonlijke domeinen (gmail, hotmail, outlook, yahoo)
- `firstname.lastname@domain` = gdpr_safe=false in strict mode
- Altijd `gdpr_safe` boolean per lead
- Nooit GDPR-onveilige leads naar Warmr sturen, ook niet als API dit vraagt

---

## Coding standaarden

- Python 3.11+, async/await overal
- httpx.AsyncClient voor HTTP, Playwright async voor browsers
- supabase-py voor database, anthropic SDK voor Claude
- Claude Haiku voor bulk (summaries, openers, email patterns)
- Claude Sonnet voor Vision analyse en diepte-analyse
- Type hints + docstring op ELKE functie
- Catch exceptions per lead — nooit de pipeline stoppen
- Altijd filteren op workspace_id in elke query

---

## Snelle referentie: wat gaat waar

| Vraag | Bestand |
|---|---|
| Hoe werkt de Google Maps scraper? | scrapers/google_maps_scraper.py |
| Hoe worden emails gevonden? | enrichment/email_waterfall.py |
| Hoe wordt een website gescoord? | website_intelligence/analyzer.py |
| Hoe werkt Claude Vision? | website_intelligence/visual_analyzer.py |
| Hoe worden concurrenten vergeleken? | website_intelligence/competitor_analyzer.py |
| Welke dienst wordt gepitcht? | website_intelligence/opportunity_classifier.py |
| Hoe wordt een review email gegenereerd? | campaigns/review_email_generator.py |
| Hoe gaan leads naar Warmr? | integrations/warmr_client.py |
| Wat zijn de sector configs? | config/sectors.py |
| Wat zijn de scoring gewichten? | config/scoring_weights.py |
| Hoe werkt de FastAPI? | api/main.py |
| Wat is het database schema? | supabase_schema.sql |

---

## Frontend design

Zelfde taal als Warmr:
- Achtergrond: #fafafa
- Primaire kleur: #6c5ce7 (paars)
- Gradient: linear-gradient(135deg, #a29bfe, #6c5ce7)
- Font display: Fraunces (headings)
- Font UI: Plus Jakarta Sans
- Border radius: 12px cards, 8px inputs
- Vanilla HTML/CSS/JS + Supabase JS SDK via CDN

Kernpagina's: dashboard, search, leads, lead-detail, website-kansen, campaigns, inbox, crm, analytics.

---

## Wanneer te zoeken in CLAUDE.md

- Volledig Supabase schema nodig → CLAUDE.md sectie "Supabase schema"
- Sector config details → CLAUDE.md sectie "Sector configuratie"
- Environment variables → CLAUDE.md sectie "Environment variables"
- Volledige Claude Vision prompt → CLAUDE.md sectie "Claude Vision prompt"
- Scoring gewichten exact → CLAUDE.md secties "Lead scoring" en "Website score factoren"
- Review email prompt → CLAUDE.md sectie "Review email generator"
- FastAPI endpoints volledig → CLAUDE.md sectie "FastAPI endpoints"
