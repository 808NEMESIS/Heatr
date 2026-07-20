# Audit: compliance — doet Heatr wat de verwerkersdocumentatie belooft?

Read-only (2026-07-19). Vergelijkt de Notion-pagina "Verwerkersovereenkomsten &
Subverwerkers (AVG)" (bijgewerkt 2026-07-07) met de Heatr-code. **Geen juridisch
oordeel** — belofte naast code. Niets verwijderd, geen gate aangeraakt.

---

## 0. De kernbevinding vóór alles: het document dekt Heatr niet

De verwerkersdocumentatie beschrijft **Aerys OS** (het kliniek-/patiëntproduct:
Supabase-patiëntdossiers, WAGHL, GHL, Google Calendar, VAPI). De rolverdeling
("Aerys = verwerker namens de kliniek") past niet op Heatr: bij outbound lead-
intelligence is Aerys geen verwerker namens een kliniek maar degene die zélf
doel en middelen bepaalt voor prospect-persoonsgegevens (contactpersonen van
klinieken). **Geen enkele Heatr-specifieke verwerking of subverwerker staat in
het register.** Het scherpste voorbeeld: het register zegt *"LLM-provider —
toekomstig, nu alleen FAKE, DPA + zero-retention vóór livegang, aparte go"* —
terwijl Heatr vandaag duizenden echte Anthropic-calls doet met namen,
bedrijfsdata en gesprekstranscripten in de prompts.

## 1. Subverwerkers: register vs. code

| Dienst | In code (vindplaats) | Persoonsgegevens erheen | In register? |
|---|---|---|---|
| **Supabase** (DB+Storage) | overal (`config/database.py`) | volledige lead-records, transcripten, screenshots | ✅ (maar voor Aerys-OS-doeleinden beschreven) |
| **Anthropic** | `company_enrichment` (openers mét voornamen), `sector_checker`, `personalization_extractor`, `contact_extractor` (namen+bio van teampagina's), `visual_analyzer` (screenshots), `calls/report_generator` (**gesprekstranscripten**) | ja — namen, vrije tekst, transcripten | ❌ als "toekomstig/FAKE" — **onjuist, is live** |
| **Bouncer** (usebouncer.com) | `enrichment/verify_api.py:30` | e-mailadressen (persoonsgebonden bij naam@) | ❌ **ontbreekt volledig** |
| **Warmr** | `integrations/warmr_client.py` (push: email, naam, custom_fields incl. opener/bedrijfsdata) | ja | ❌ ontbreekt (eigen infra op localhost, maar Warmr's eigen mail-providers zijn onbekend vanuit deze codebase) |
| **Resend** | `utils/alert_manager.py:110`, `api/main.py` (briefing) | alerts ogen aggregaat (coverage-%); briefing niet regel-voor-regel geverifieerd | ❌ ontbreekt |
| **Google PageSpeed** | `technical_checker.py:117` | alleen bedrijfs-URL | ❌ (grensgeval, wel documenteren) |
| **ip-api.com** | `technical_checker.py:99` | domein → geo (bedrijfsdata) | ❌ (grensgeval) |
| **RDAP** (rdap.org/sidn) | `domain_age_scraper.py:41` | domeinnamen | ❌ (grensgeval) |
| **Meta Ad Library** | `meta_ads_scraper.py:37` | bedrijfsnamen | ❌ |
| **Google Places** (Tier 2, wacht op key) | `audit/places.py` | bedrijfsnaam+stad | ❌ — toevoegen vóór de key erin gaat |
| Andersom: **WAGHL/GHL/VAPI/Google Calendar/Slack** | niet in de Heatr-codebase | — | staan wél in het register (Aerys OS — correct, maar het register mengt twee producten zonder dat te zeggen) |

## 2. Gegevenscategorieën: document vs. werkelijk opgeslagen

Het document noemt patiënt-/art.9-categorieën (Aerys OS). Wat Heatr werkelijk
opslaat en dat nergens gedocumenteerd is:

- **Contactpersonen**: namen (`contact_name/first/last/tussenvoegsel`), titels,
  `contact_linkedin_url`, `contact_why_chosen` — plus **1.722 rijen in
  `lead_contacts`** (full_name, title, bio_snippet, confidence).
- **Vrije tekst uit AI-extractie**: `personalization_hooks/observations`,
  `company_positioning`, `review_best_quote` (kan klant-/patiëntnamen uit
  reviews bevatten), `website_intelligence.personalization` + `team_contacts`
  (namen + bio's).
- **Gesprekstranscripten**: `heatr_call_records.transcript` (NOT NULL, volledig)
  + `participants` (e-mails) — het meest privacy-zware Heatr-artefact.
- **Screenshots** van websites (Storage: oud `{domain}.png`, nieuw
  `captures/{domain}/desktop.webp|mobile.webp`) — kunnen gezichten/teamfoto's
  bevatten.
- **Netwerk-logs** per site (`heatr_website_network_log`) en
  **e-mailverificatie-audit** (`heatr_email_verifications`, expliciet
  append-only "nooit overschrijven").

Verzameling is deels geminimaliseerd (GDPR_MODE=strict filtert persoonlijke
e-mails bij scraping, `website_scraper.py:233-245`), maar de opslag-categorieën
zelf zijn nergens beschreven.

## 3. Bewaartermijn en verwijdering

- **De code belooft zelf retentie die niet bestaat**: `utils/gdpr_manager.py:274,311,326`
  genereert privacy-teksten met *"Bewaartermijn: 2 jaar na laatste contact,
  daarna automatisch geanonimiseerd"* en *"Campagnedata 1 jaar, replies 2 jaar"*
  — er is **geen enkele purge-/anonimiserings-job** in de codebase (grep op
  retention/purge/cleanup: alleen cache-TTL's). Alle tabellen groeien onbeperkt;
  drie zijn zelfs bewust append-only.
- **`POST /gdpr/forget/{lead_id}`** (`api/main.py:5029` → `gdpr_manager.forget_lead`)
  bestaat en is degelijk gebouwd (per-lead-unieke redactie, fail-loud, suppressie
  van het originele adres tegen her-scrapen) — **maar voor het schema van vóór
  de recente migraties**. Wat de erase NIET raakt:
  - `heatr_call_records` — **transcripten blijven volledig staan**
  - `lead_contacts` (1.722 naam-rijen), `heatr_lead_outreach_snapshots`
    (o.a. `kvk_bestuurder_name`), `heatr_email_verifications` (e-mail per
    poging), `heatr_website_network_log`, `heatr_audit_reports`
  - `website_intelligence.team_contacts` + `.personalization` (namen/vrije tekst)
  - leads-kolommen buiten de redactielijst: `contact_name`, `contact_title`,
    `contact_why_chosen`, `review_best_quote`, `personalization_*`,
    `kvk_bestuurder_name`
  - Storage: wist alleen het **oude pad** `{domain}.png` (`gdpr_manager.py:144`)
    — de nieuwe `captures/…`-WebP's blijven staan
  - idem de **export** (recht op inzage, `gdpr_manager.py:223-265`): leest alleen
    de oude tabellen
- **`heatr_gdpr_log` bestaat niet in prod** — de audittrail-insert (stap 9)
  faalt stil. Nota bene: dit ontsnapte aan `verify_migrations` omdat die alleen
  `migrations/*.sql` parseert en `gdpr_log` in het basis-`supabase_schema.sql`
  staat — een blinde vlek van de nieuwe check zelf.
- De flow is bovendien **nooit gebruikt** (geen forgotten-leads, geen log).

## 4. De suppressie- en compliance-gates

- `compliance_check` blokkeert `unsubscribed/forgotten/disqualified/bounced`
  hard, zonder test-bypass — klopt met de belofte (verzenden).
- `forgotten` wordt óók uitgesloten van de website-analyse-worker
  (`website_analysis_queue.py:48 _TERMINAL_LEAD_STATUSES`) en van de
  recontact-lijsten (`api/main.py:3650`).
- **Gat**: in `job_queue/enrichment_queue.py` is geen `forgotten`-filter
  gevonden — de bulk-enqueue pakt alleen onverrijkte leads (de facto geen
  forgotten), maar een handmatige re-enqueue op lead_id zou een vergeten lead
  opnieuw door de e-mail-waterval halen; de suppressielijst voorkomt alleen het
  opnieuw **aanschrijfbaar** worden, niet het opnieuw **verwerken**.

## 5. De pre-consent-tracking-ironie — externe verificatie voor Sami

Niet vast te stellen vanuit deze codebase: **draait aeryssolution.nl zelf schoon
vóór consent?** Heatr's audit flagt klinieken hierop (53% van de gemeten leads
had een pre-consent tracker); als de eigen site GTM/GA vóór consent laadt, is
dat precies het terugkaats-risico. → Losse check voor Sami (eigen site door
dezelfde meting halen kán later met de eigen tooling, maar is bewust niet hier
ingevuld).

---

## Slot: wat moet dicht vóór de eerste kliniek-mail — en wat is doc vs. code

**Code-fixes (vóór de eerste mail):**
1. `forget_lead` + export uitbreiden naar het huidige schema: call_records
   (transcripten), lead_contacts, outreach_snapshots, email_verifications,
   network_log, audit_reports, WI-personalization/team_contacts, de ontbrekende
   leads-kolommen, en beide Storage-paden. (Het verwijderrecht moet werken op de
   dag dat de eerste mail uitgaat — de eerste reply kan een verwijderverzoek zijn.)
2. `heatr_gdpr_log` aanmaken (basis-schema-tabel die nooit is toegepast) —
   anders is er geen erasure-audittrail. Plus: `verify_migrations` ook het
   basis-schema laten dekken (blinde vlek).
3. De retentie-belofte in `gdpr_manager`-teksten ófwel waarmaken (purge-job)
   ófwel uit de gegenereerde privacy-tekst halen tot 'ie bestaat — een belofte
   in eigen teksten die de code niet uitvoert is het slechtste van beide.

**Documentatie-updates (kunnen parallel):**
4. Heatr een eigen sectie/rol in het AVG-document geven (Aerys als
   verantwoordelijke voor prospect-data — jurist bevestigt) met een eigen
   subverwerkerslijst: **Anthropic (live, niet "FAKE"), Bouncer, Warmr, Resend**,
   + de grensgevallen (PageSpeed, ip-api, RDAP, Meta) en **Google Places vóór de
   key erin gaat**.
5. De werkelijk opgeslagen gegevenscategorieën documenteren (incl. transcripten,
   lead_contacts, vrije-tekst-extracties, screenshots).

**Extern (Sami):** de eigen-site-tracking-check (§5); jurist-review zoals het
document zelf al eist.
