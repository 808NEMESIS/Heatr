# Heatr Backlog (latente, niet-blocking)

Items die niet kritiek zijn maar vastgelegd moeten worden zodat ze niet vergeten
raken. Geen prioritering, geen oplossing — alleen registratie + trigger.

---

## #1 — review_recency scraper vindt 0 relatieve datums op Google Maps

- **Diagnose:** `enrichment/google_reviews_scraper.py` opent google_maps_url maar parse-regex matcht niet op de huidige Google Maps DOM (vermoedelijk DOM-update of geo-blocking/cookie-banner blokkeert JS-rendering).
- **Doe dit pas als:** eerste 20-leads cohort is verzonden EN je BLOK B (review-cadans observation) wilt benutten in Mail 1.

## #2 — email_verifier 0% verified ondanks Pad A

- **Diagnose:** 207 leads (24%) hebben status `risky` ipv `verified`/`catchall`/`not_found` — onbekend of dit van een externe verifier komt of dat onze `enrichment/email_verifier.py` step nooit echt draait.
- **Doe dit pas als:** bounce-rate van eerste cohort >2% is via `/analytics/email-status-breakdown`.

## #3 — `email_verified` kolom mist op `heatr_enrichment_data`

- **Diagnose:** [scrapers/website_scraper.py](scrapers/website_scraper.py) probeert `email_verified` veld in te voegen op een tabel zonder die kolom; insert returnt PGRST204, lead-status op heatr_leads wordt wel correct gezet.
- **Doe dit pas als:** je log-warnings wilt opruimen (cosmetisch, 5-min fix).

## #4 — `heatr_startup_log` tabel mist

- **Diagnose:** `utils/startup_validator.py` probeert resultaten te schrijven naar deze tabel die nooit gemigreerd is; PGRST205 error gelogd bij elke API-startup.
- **Doe dit pas als:** je startup-logs wilt opschonen (cosmetisch, 5-min fix — migration of try/except).

## #5 — Completeness-score met threshold-override

- **Diagnose:** Huidige binary gate (hard-required: archetype/score/sector) kan na eerste cohort te streng blijken — bv. opener mist maar score 95 op een goede lead. Vervangen door numeric completeness-score + configurable threshold + manual `override_completeness_check` flag per lead.
- **Doe dit pas als:** in de eerste 100 leads minstens 5 cases zijn waar je een blocked lead handmatig had willen doorlaten. Geen vroegere actie.

---

## Loom Task Orchestrator — kritieke productie-feature

**Status:** Kritiek voor productie. Mail 2 + 3 in v3.1_ai_audit en v3.1_workflow bruggen bevatten {{LOOM_LINK}} of {{VIDEO_LINK}} tokens. Zonder integratie pauzeert/breekt de sequence wanneer Mail 2/3 triggeren — deze tokens worden nu leeg geleverd, wat resulteert in lege regels in de mail-body waar de link zou moeten staan.

**Ontdekt:** 8 mei 2026, tijdens Test 3 voorbereiding. De gerenderde Mail 2/3 hadden lege LOOM_LINK / VIDEO_LINK velden zonder error of notificatie aan Sami.

**Wat de feature moet doen:**

1. **Pre-trigger detectie:** voor elke geplande Mail 2/3 die binnen X uur (bv. 24u) gepland staat, scan op lege Loom/video-tokens.
2. **Notificatie naar Sami met:**
   - Welke lead (naam, bedrijf, sector, archetype, signaal_blok)
   - Welke mail (2 of 3) en wanneer 'ie zou triggeren
   - Concrete Loom-briefing op basis van lead enrichment, bv:
     > "Voor PCG: open met observatie over hun 49 reviews & 5.0 rating, daarna 2 dingen die wij kunnen oppakken op hun site (call-to-action visibility, before-after gallery), plus 1 tip die ze zelf direct kunnen doen (sneller laden hero-image)"
3. **Wacht-mechanisme:** Sami krijgt tijd om Loom op te nemen en URL in te voeren via dashboard. Mail wacht.
4. **URL-injectie:** Wanneer Sami URL invoert, wordt {{LOOM_LINK}} vervangen in payload, send wordt geactiveerd.
5. **Fallback-policy bij no-response:** wat gebeurt na 24u zonder URL-invoer? Drie opties (TBD): annuleren / pauzeren indefinite / send zonder link (slecht).

**Design-vragen om eerst te beantwoorden:**

- Notificatie-kanaal: dashboard-alert / email / Slack / SMS? Sami werkt op Mac, dashboard is realistisch.
- Briefing-generatie: hardcoded per brug-archetype, of Sonnet-gegenereerd op basis van enrichment?
- Pauze-mechanisme: Heatr scheduler of Warmr pause-endpoint?
- Mail 3 (VIDEO_LINK voor persoonlijke video) gebruikt zelfde flow of aparte? Aparte heeft mogelijk andere briefing nodig.

**Trigger-conditie voor bouwen:** vóór eerste echte cohort. Voor nu: Aerys test-campaign 75648438 staat op `draft`, gaat niet vanzelf triggeren.

**Geschat:** 2-3 sessies werk inclusief design-review.

## Warmr schema-debt — meerdere mismatches Pydantic vs DB

**Status:** Klasse-bug. Drie mismatches geobserveerd in 2 dagen. Niet specifiek aan één endpoint maar lijkt structureel.

**Ontdekt:**

1. 8 mei: `leads.company_name` (Pydantic) vs `leads.company` (DB) — gefixt via Fix C selectief mapping in `_insert_lead_single`, `_insert_leads_bulk`, CSV-import (api/main.py), nested select api/main.py:681
2. 8 mei: `leads.imported_at` (Pydantic) vs niets (DB) — gedropt in inserts (DB heeft created_at met DEFAULT now())
3. 9 mei: `email_events.client_id` (Warmr's stats-code in api/public_api.py:901) vs niets (DB) — stats-endpoint geeft HTTP 500
4. Ook bekend (uit Fix C diagnose): `q.order("imported_at")` in api/public_api.py:665 zal 500 throw'en bij GET /api/v1/leads
5. Ook bekend: `LeadResponse` declareert `notes`, `imported_at`, `campaign_id` — geen van die bestaat in DB. Frontend krijgt None.

**Hypothese:** Migrations zijn ergens ooit niet gerund of zijn handmatig anders dan de Pydantic-modellen. Pydantic-modellen zijn per-endpoint geschreven zonder cross-check tegen werkelijk schema.

**Twee fix-paden:**

**Pad α — Fix A (volledig):**
- ALTER TABLE leads RENAME COLUMN company TO company_name
- ALTER TABLE leads ADD imported_at, notes
- Migration voor email_events.client_id
- Pydantic LeadResponse uitbreiden met werkelijke DB-velden
- Spintax-engine + campaign_scheduler aanpassen die nu lead["company"] direct lezen

**Pad β — Inventarisatie eerst:**
- Schrijf een schema-audit script dat voor elke Pydantic-model in api/ checked of alle velden in DB bestaan en omgekeerd
- Output: complete lijst mismatches
- Daarna gerichter fixen

**Trigger-conditie:** vóór Pro Plan-upgrade (zodat Fix A met PITR-vangnet kan), of zodra een tweede consumer (frontend, andere service) de stats/leads endpoints serieus gaat gebruiken.

**Geschat:** Pad α: 1.5-2 uur. Pad β: 30 min audit + variabele fix-tijd op basis van wat gevonden wordt.

## Personalization-gate honoreert is_test_lead niet

**Status:** Open vanaf 8 mei. Werkaround vandaag via custom sequence override (body.sequence non-empty bypassed gate).

**Probleem:** Completeness-check heeft is_test_lead-bypass uit top-4 sprint van april. Personalization-gate niet. Plus is_test_lead-kolom bestaat nog niet in Heatr DB ondanks dat in top-4 sprint gepland.

**Fix vereist:**
1. DB-migratie 017 (of nieuwer nummer) voor `leads.is_test_lead boolean DEFAULT false`
2. Code-aanpassing in `_gate_leads_for_template` om bypass te respecteren
3. Tests die bypass-flow valideren

**Trigger-conditie:** vóór tweede cohort live sends. Voor de Aerys test-campaign niet kritiek (workaround werkte).

**Geschat:** 30-45 min.
