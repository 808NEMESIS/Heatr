# Heatr Codebase Audit — 2026-05-20

Audit op 7 punten per Sami's prompt. Werkwijze: 7 parallelle Haiku Explore-agents
voor scan-fase, daarna gerichte Read-verificatie op kritieke claims door
hoofdagent (Opus). Geen code-wijzigingen.

**Baseline:** HEAD = `c153454` (ICP-scope herziene naar cosmetische klinieken +
chiropractoren). `alternatieve_geneeskunde` blijft in SECTORS dict voor
backwards-compat maar valt uit `list_sectors()`. ACTIVE_SECTORS =
`["cosmetische_behandelaars", "chiropractoren"]`.

---

## Executive Summary

| # | Audit-punt | Bevindingen | Hoogste severity |
|---|---|---|---|
| 1 | Sector-consistentie | 4 (2 actief, 2 inactief/dood) | HIGH |
| 2 | GDPR-enforcement | 3 actieve gaps + 4 mitigated paden | CRITICAL |
| 3 | Per-lead exception-isolatie | 2 exposed bulk-loops | HIGH |
| 4 | workspace_id filtering | 10+ tenant-leak-paden | CRITICAL |
| 5 | Warmr-client hardening | 2 gaps (5/7 mitigated) | MEDIUM |
| 6 | Rate-limit + anti-detectie | 0 gaps (all enforced) | — |
| 7 | Scoring-drempels | 0 echte gaps, 1 foutief counter-evidence | LOW |

**Top-3 acute issues:**
1. **`reply_classifier.process_reply` — `workspace_id` parameter ontvangen maar nooit toegepast in 8+ DB-mutations** (cross-tenant data-mutation mogelijk via valse webhook)
2. **`/leads/{id}/send-review-email` pusht naar Warmr zonder enige GDPR/status-check** — unsubscribed leads kunnen review-emails krijgen
3. **`filter_launchable_leads()` checkt geen `gdpr_safe` of `status`** — geunsubscribede leads kunnen via campaigns/launch alsnog Warmr-pushed worden

---

## 1. Sector-consistentie

### 1.1 — ACTIVE HIGH: `scrapers/directory_scraper.py:463`
```python
listing["sector"] = "cosmetische_klinieken"
```
**Issue:** sector-key `"cosmetische_klinieken"` bestaat niet in `SECTORS` dict
(juiste key: `"cosmetische_behandelaars"`). ClinicFinder-scraped leads krijgen
foutieve sector → `get_sector()` raises ValueError of stil overslaan in
downstream icp_matcher.

**Impact:** ingest-data-corruptie voor ClinicFinder-bron.

**Fix:** vervang `"cosmetische_klinieken"` → `"cosmetische_behandelaars"`.

### 1.2 — ACTIVE MEDIUM: `enrichment/company_enrichment.py:367-370`
```python
tone_guidance = {
    "makelaars": "zakelijk, lokaal betrokken, ...",
    "alternatieve_geneeskunde": "warm, persoonlijk, ...",
    "cosmetische_behandelaars": "stijlvol, resultaatgericht, ...",
    "bouwbedrijven": "direct, vakkundig, ...",
}.get(sector_key, "professioneel en persoonlijk")
```
**Issue:** `makelaars` + `bouwbedrijven` zijn oude sectoren, niet meer actief.
`.get(...,default)` faalt niet, maar dode keys verwarren.

**Impact:** geen runtime-fail; cleanup-debt.

**Fix:** verwijder `makelaars` + `bouwbedrijven` entries. Behoud
`alternatieve_geneeskunde` (legitiem inactief-maar-in-dict).

### 1.3 — DEAD LOW: `config/sequence_templates.py:645` (`v1_alternatieve_zorg`)
**Status:** DEPRECATED-comment regel 619 markeert de template; v1.0 is uit
gebruik per build-log 2026-05-07. Template wordt niet actief gerouteerd
(`pick_brug` kiest v3.x).

**Impact:** geen, template is dood per design.

**Fix:** geen actie nodig (dood code per design; verwijder bij volgende
template-major-cleanup).

### 1.4 — AMBIGUOUS LOW: `utils/sector_impact.py:21`
```python
"alternatieve_zorg": "mensen ondersteunt in hun herstel",
```
**Issue:** `alternatieve_zorg` is **geen** sector-key in SECTORS dict. Volgens
file-comment is dit een "generieke sector-key (zoals user-spec)" gebruikt door
v3.2 Mail 1 templates. De DB-aliases (`alternatieve_geneeskunde`,
`cosmetische_behandelaars`) zorgen dat live leads matchen.

**Impact:** geen runtime-fail. Verwarrend voor latere lezers — twee
naming-conventies (generiek + DB-alias) coexisteren.

**Fix:** docstring uitbreiden met expliciete uitleg waarom `alternatieve_zorg`
hier staat ondanks dat het geen DB-sector-key is.

---

## 2. GDPR / `gdpr_safe`-enforcement

### 2.1 — CRITICAL GAP: `api/main.py:634-664` (`/leads/{id}/send-review-email`)
**Verificatie:** regel 661 doet `await client.push_lead(lead, ...)` direct na
`generate_review_email`. **Geen `gdpr_safe`-check, geen status-check, geen
`is_sendable()`-call.**

**Impact:** een unsubscribed of forgotten lead kan via deze endpoint alsnog
een Warmr-mail krijgen → GDPR-overtreding + reputation-risico.

**Fix:** vóór regel 661 toevoegen:
```python
if not lead.get("gdpr_safe") or lead.get("status") in ("unsubscribed", "forgotten", "disqualified"):
    raise HTTPException(status_code=403, detail="Lead niet sendable (GDPR/status)")
```

### 2.2 — CRITICAL GAP: `utils/enrichment_check.py:90-117` (`filter_launchable_leads`)
**Verificatie:** `check_lead_completeness()` checkt `archetype` + `score` +
`sector` + soft fields. **Geen `gdpr_safe`-check, geen `status`-check.**

**Impact:** `/campaigns/launch` flow gebruikt deze filter pre-push. Leads die
voldoen aan completeness maar `gdpr_safe=False` of `status='unsubscribed'`
hebben, glijden door.

**Mitigatie elders:** `campaigns/sequence_engine.SendingGuard.check_can_send()`
zit DOWNSTREAM aan dispatch-tijd (per-send) en checkt wel gdpr_safe +
status. Dus uiteindelijke schade beperkt tot leads die door beide
gates komen — maar dat vereist dat sequence-engine altijd over launch heen
komt, wat niet gegarandeerd is bij directe Warmr-push.

**Fix:** voeg `gdpr_safe` + `status` aan `_hard_required_fields` of als aparte
pre-filter in `filter_launchable_leads()`.

### 2.3 — PARTIAL GAP: `api/main.py:585-588` (`/leads/send-to-warmr`)
**Verificatie:** filter is
```python
l.get("gdpr_safe") and l.get("email_status") in ("verified", "catch_all") and (l.get("score") or 0) >= int(os.getenv("MIN_SCORE_FOR_WARMR", 65))
```
**`gdpr_safe` aanwezig ✓**, MIN_SCORE aanwezig ✓, **maar `status NOT IN
('unsubscribed','forgotten','disqualified')` ONTBREEKT**.

**Impact:** een lead met `gdpr_safe=True` (niet door GDPR-unsub geraakt) maar
`status='disqualified'` (handmatig of door reply-classifier) kan alsnog pushed
worden.

**Fix:** voeg toe aan de filter:
```python
and l.get("status") not in ("unsubscribed", "forgotten", "disqualified")
```

### 2.4 — MITIGATED (overige paden)
- `campaigns/sequence_engine.SendingGuard.check_can_send()` checkt gdpr_safe +
  status comprehensively bij elke send (per-send dispatch-time)
- `api/main.py POST /webhooks/warmr` unsubscribe-handlers updaten status + stop
  sequences atomisch
- `integrations/reply_classifier.classify_reply` UNSUBSCRIBE-tak zet
  `gdpr_safe=False` + `status='unsubscribed'` atomisch

---

## 3. Per-lead exception-isolatie

### 3.1 — EXPOSED HIGH: `api/main.py:602-603` (`/leads/send-to-warmr` timeline)
```python
for lead in eligible:
    _insert_timeline_event(db, workspace_id, lead["id"], "email_sent", f"...")
```
**Issue:** geen `try/except` per lead. Eén DB-constraint-error (bv. uniqueness
op timeline-event-id) stopt alle remaining inserts.

**Impact:** Warmr-push is al gebeurd (bulk-call op regel 601), maar als
timeline-log midden in de loop crasht, ontbreken audit-rows voor remaining
leads → "we hebben gepushed maar weten niet meer wie".

**Fix:** wrap `_insert_timeline_event(...)` in try/except + log warning.

### 3.2 — EXPOSED HIGH: `api/main.py:1212-1217` (`/campaigns/launch` timeline)
Identiek patroon: per-bucket timeline-insert-loop zonder try/except.

**Impact:** zelfde als 3.1 — bulk-push gedaan, audit-trail kan halverwege
breken bij eerste DB-error.

**Fix:** identiek aan 3.1.

### 3.3 — FRAGILE MEDIUM: `job_queue/enrichment_queue.py:148-155`
```python
for lead in leads:
    result = await queue_lead_for_enrichment(
        lead_id=lead["id"], ...
    )
```
**Issue:** `queue_lead_for_enrichment()` heeft INTERN try/except — als die toch
een uncaught exception raised (bv. Supabase connection timeout vóór de
try-blok), stopt de hele loop.

**Impact:** "queue all unenriched leads" bulk-action kan halverwege stoppen.

**Fix:** explicit per-lead try/except wrapper rond de call voor belt-and-braces.

### 3.4 — PROTECTED (false alarms uit audit-3)
- `integrations/warmr_client.py:327-351` heeft per-lead try/except ✓
- `api/main.py:2166-2178` re-enqueue heeft try/except ✓
- Diverse in-memory bucket-loops zonder DB-call — geen crash-risk

---

## 4. workspace_id filtering — TENANT-LEAK-RISICO

### 4.1 — CRITICAL: `integrations/reply_classifier.process_reply` (8+ sites)
**Verificatie regel 188 + 206-320:** functie ontvangt `workspace_id` parameter
(regel 188) maar gebruikt het ALLEEN voor `lead_timeline.insert` (regel 295+),
NOOIT in de SELECT/UPDATE chains.

Concrete sites met ontbrekend `.eq("workspace_id", ...)`:
- regel 206: `db.table("leads").select("*").eq("id", lead_id)`
- regel 244-247: UPDATE leads (unsubscribe-flow): `gdpr_safe=False, status='unsubscribed'`
- regel 257-261: UPDATE leads (not-interested-flow)
- regel 269-273: UPDATE leads (snooze)
- regel 283-285: UPDATE leads (auto-reply)
- regel 294-298: UPDATE leads (wrong-person)
- regel 305-308: UPDATE leads (interested)
- regel 317-319: UPDATE leads (question)
- regel 326-329, 339-342: `lead_campaign_history` UPDATE (stop/pause sequences)

**Impact:** een tenant-A webhook met geknutselde `lead_id` (UUID guess of leak)
kan leads in tenant-B updaten naar status='unsubscribed' of gdpr_safe=False.
UUID-uniqueness is een gedeeltelijke mitigatie — vereist UUID-knowledge —
maar geen vervanger voor expliciete tenant-filter.

**Fix:** in elke `.eq("id", lead_id)` / `.eq("lead_id", lead_id)` toevoegen:
`.eq("workspace_id", workspace_id)`.

### 4.2 — HIGH: `website_intelligence/analyzer.py:211`
```python
supabase_client.table("leads").update(lead_update).eq("id", lead_id).execute()
```
**Issue:** geen workspace_id. UUID-uniqueness mitigeert (lead_id is unieke
UUID), maar het is niet defense-in-depth.

**Impact:** als UUID-guess slaagt → mogelijke cross-tenant lead-update.

**Fix:** `.eq("workspace_id", workspace_id)` toevoegen. `analyze_website()`
ontvangt al `workspace_id` als param.

### 4.3 — HIGH: `website_intelligence/competitor_analyzer.py:123 + 135`
```python
# Regel 123:
supabase_client.table("website_intelligence").update({...}).eq("lead_id", lead_id).execute()
# Regel 135:
supabase_client.table("leads").update({...}).eq("id", lead_id).execute()
```
**Issue:** zelfde als 4.2 — UUID-only filter.

**Fix:** voeg `workspace_id`-filter toe. Functie ontvangt `workspace_id` via
caller.

### 4.4 — MITIGATED-BUT-WEAK
- `analyzer.py:170` upsert `on_conflict="lead_id"` — sterk argument voor
  composite-key check, maar lead_id is wereldwijd uniek qua UUID → low risk in
  praktijk.

---

## 5. Warmr-client hardening

### 5.1 — GAP MEDIUM: Draft-detection ontbreekt
**Verificatie:** `warmr_client.create_campaign` (regel 390-396) valideert dat
de Warmr-response een `id` of `campaign_id` veld heeft, maar checkt
**niet** of `campaign.status == 'active'`. Warmr kan een 200 + draft-status
teruggeven; Heatr accepteert blindelings.

**Impact:** een draft-campaign krijgt leads toegewezen maar verzendt niets →
silent stall.

**Fix:** post-create check op `response.get("status")`. Bij `'draft'`:
expliciete activatie-call of `WarmrAPIError("campaign in draft, niet
geactiveerd")`.

### 5.2 — GAP MEDIUM: 4xx vs 5xx differentiatie
**Verificatie:** `_request()` regel 127-129 raises uniform `WarmrAPIError` voor
alle non-2xx. Geen retry-logic voor transient errors (429, 503, 504).

**Impact:** transiente Warmr-issues (rate-limit, brief outage) leiden tot
hard-fails. Heatr-side moet handmatig opnieuw triggeren.

**Fix:** exponential backoff retry voor status_code IN (429, 502, 503, 504),
max 3 attempts. Geen retry voor 4xx (auth/validation errors).

### 5.3 — MITIGATED (5/7 failure-modes)
- Response validation ✓ (regel 390-396)
- Empty-sequence pre-check ✓ (api/main.py:1157-1159 + sequence_engine.py:143)
- Timeout config ✓ (regel 114: 30s global)
- Bookkeeping sync ✓ (regel 287-359 `_writeback_bulk_bookkeeping`)
- Network-error → WarmrAPIError ✓ (regel 122-125)

---

## 6. Rate-limit + anti-detectie — ALL ENFORCED

Audit-agent vond **0 gaps**. Verificatie steekproef bevestigt:

- `wait_for_token("google_search")` actief in google_search_scraper.py:113
  (limiet 10/uur via `utils/rate_limiter.py:35-39`)
- Browser-context rotatie elke 60 results in google_maps_scraper.py:152-174
- CAPTCHA-detection + 2h block via `_store_captcha_block()` actief
- Rate-limiter service-entries voor alle 10 services in
  `utils/rate_limiter.py:29-80`
- Random delays via `SCRAPE_DELAY_MIN/MAX` env-vars in playwright_helpers
- Anti-detectie multi-layered: mouse-curves, header-randomization, UA-rotation,
  webdriver-stealth-mask in `utils/playwright_helpers.py:127-231`

**Geen actie nodig.**

---

## 7. Scoring-drempels — bijna alles consistent, één foutief counter-evidence

### 7.1 — Consistent gates ✓
- `MIN_SCORE_FOR_WARMR=65` toegepast in `scoring/lead_scoring.py:192` + dubbel
  in `api/main.py:587` (defense-in-depth)
- `MIN_ICP_MATCH_FOR_WARMR=0.6` toegepast in `scoring/lead_scoring.py:194`
- `config/scoring_weights.py` is sector-agnostisch, geen stale
  makelaars/bouwbedrijven-weights

### 7.2 — CORRECTIE OP AUDIT-CLAIM: foutief counter-evidence
Audit-agent 7 claimt: *"icp_matcher.py raises ValueError als sector niet in
ACTIVE_SECTORS → match_icp returns 0.0 (hard disqualify)"*.

**Dit is onjuist.** `get_sector(sector)` (config/sectors.py:484) raises alleen
als de key niet in `SECTORS` dict zit. `alternatieve_geneeskunde` zit nog in
SECTORS (backwards-compat) maar is **inactief per ACTIVE_SECTORS**. Bestaande
leads met `sector='alternatieve_geneeskunde'` krijgen dus **geen hard
disqualify** — ze worden gewoon door ICP-matcher gehaald met de oude
keywords/signals.

**Impact:** lage prioriteit — inactieve sector leverde toch al weinig nieuwe
leads. Maar de claim "dead-code-guarded" klopt niet.

**Mogelijke fix (optioneel):** in `icp_matcher.match_icp` of `score_lead`,
toevoegen:
```python
from config.sectors import ACTIVE_SECTORS
if sector not in ACTIVE_SECTORS:
    logger.info("Lead %s in inactieve sector %s — skip ICP-scoring", lead_id, sector)
    return 0.0
```
Niet kritiek; bewaak in feedback-data of inactive-sector-leads écht onder
threshold blijven.

---

## Acties — prioriteit-volgorde

| Prio | Audit | Locatie | Actie | Effort |
|---|---|---|---|---|
| P0 | 4.1 | `integrations/reply_classifier.py:206-329` | Voeg `.eq("workspace_id", workspace_id)` toe aan 8+ SELECT/UPDATE chains | 30 min |
| P0 | 2.1 | `api/main.py:634-664` | gdpr_safe + status gate vóór push_lead in /send-review-email | 10 min |
| P0 | 2.2 | `utils/enrichment_check.py:90-117` | gdpr_safe + status check in filter_launchable_leads | 15 min |
| P1 | 4.2-4.3 | `website_intelligence/analyzer.py:211` + `competitor_analyzer.py:123+135` | workspace_id-filter toevoegen | 15 min |
| P1 | 2.3 | `api/main.py:585-588` | status-filter toevoegen aan /send-to-warmr eligibility | 5 min |
| P1 | 3.1-3.2 | `api/main.py:602-603` + `1212-1217` | per-lead try/except rond timeline-insert | 10 min |
| P2 | 1.1 | `scrapers/directory_scraper.py:463` | sector-key fix `cosmetische_klinieken` → `cosmetische_behandelaars` | 1 min |
| P2 | 5.1 | `integrations/warmr_client.py:390+` | draft-detection post-create campaign | 20 min |
| P2 | 5.2 | `integrations/warmr_client.py:_request` | retry-logic voor 429/502/503/504 | 30 min |
| P3 | 1.2 | `enrichment/company_enrichment.py:367-370` | verwijder makelaars/bouwbedrijven tone-guidance entries | 2 min |
| P3 | 3.3 | `job_queue/enrichment_queue.py:148-155` | per-lead try/except in queue_all_unenriched | 5 min |
| P3 | 7.2 | `scoring/icp_matcher.py` of `lead_scoring.py` | ACTIVE_SECTORS-gate voor 100% dead-code-guard | 10 min |

**Totaal effort acute fixes (P0+P1):** ~1u 25min. P2+P3: ~50min extra.

---

## Methode

- **Scan-fase:** 7 parallelle subagents (model: Haiku, subagent_type: Explore),
  elk een audit-punt. Token-bewust per Sami's instructie.
- **Verificatie-fase:** hoofdagent (Opus) leest gerichte file:line-locaties via
  Read + Bash grep om elke kritieke claim te bevestigen of weerleggen.
- **Synthese-fase:** hoofdagent consolideert in dit rapport. Eén bevinding van
  audit-agent 7 ("dead-code-guarded via ACTIVE_SECTORS") bleek onjuist na
  verificatie — gecorrigeerd in sectie 7.2.
- **Geen code-wijzigingen** — rapport is read-only audit.
