# CHANGES_PR2 — sectors.py breaking-change cleanup

**Datum:** 2026-04-21
**Scope:** Alle productie-code aanpassen aan het nieuwe `config/sectors.py` v2-schema dat de user heeft geïntroduceerd (SBI 2025 5-cijferig, subcategories, `website_signals`, geen per-criterium punten meer).
**Niet in scope:** CLAUDE.md sync (P3), schema-consolidatie (P2), frontend updates, enrichment-gaps voor Warmr v1.0 sequence (dat wordt PR3).

---

## Gewijzigd — mechanische key-renames

### `scoring/icp_matcher.py`
- `exclude_keywords` → `disqualifiers` (en subcategory-disqualifiers mee-gemerged)
- `icp_keywords` → geflattend uit `lead_keywords` + `subcategories[*].icp_signals`
- `kvk_sbi_codes` → `sbi_codes` (5-cijferig; bidirectional prefix-match voor 4→5 migratie-periode)
- `typical_company_size` → nieuwe module-constante `_FALLBACK_COMPANY_SIZE_RANGE = (1, 15)`
- Nieuwe helper: `_collect_icp_signals(sector_config)`

### `enrichment/lead_qualifier.py`
- `exclude_keywords` → `disqualifiers` (globaal + per-subcategory)
- `icp_keywords` → `lead_keywords` + `subcategories[*].icp_signals`
- Disqualification-reason prefix `exclude_keyword:` → `disqualifier:`

---

## Gewijzigd — dode sector-branches gestript

### `enrichment/opener_generator.py`
- `GAP_TO_PAIN[*].sectors`: `"makelaars"` en `"bouwbedrijven"` uit alle lijsten.
- `SECTOR_LABELS` teruggebracht tot `{"alternatieve_geneeskunde": "therapeuten", "cosmetische_behandelaars": "klinieken"}`.

### `website_intelligence/visual_analyzer.py`
- `sector_context` dict — `makelaars`/`behandelaren`/`bouwbedrijven` keys vervangen door `alternatieve_geneeskunde` + `cosmetische_behandelaars` met rijkere context-strings.

### `enrichment/company_enrichment.py`
- `_INDUSTRY_LIST_MAKELAARS` + `_INDUSTRY_LIST_BOUWBEDRIJVEN` verwijderd (43 regels).
- `_INDUSTRY_LISTS` dict teruggebracht tot 2 sectoren.

### `enrichment/contact_discovery.py`
- `_generate_why_chosen()`: `sector` parameter verwijderd, `typical_company_size` size_hint weg.
- `_SENIORITY_MAP`: makelaar/aannemer entries vervangen door behandelaars-specifieke titels (acupuncturist, osteopaat, kliniekhouder, cosmetisch arts, etc.).
- Nieuwe constante `_BEHANDELAARS_DECISION_MAKER_TITLES` als fallback (sectors.py v2 heeft geen per-sector `decision_maker_titles` meer).

---

## Gewijzigd — `scoring_boosts` verwijderd

### `scoring/lead_scoring.py`
- Hele `scoring_boosts`-tak weggehaald (~10 regels in `score_lead`).
- Dode helpers `_check_boost()` en `_email_starts_with_name()` volledig verwijderd (~35 regels).
- Fit-score = `icp_match * 40` + review-count signalen. Sector-specifieke boosts vervallen tot feedback data het terug rechtvaardigt.

---

## Herschreven — Laag 4 met Claude Haiku

### `website_intelligence/sector_checker.py` (volledige rewrite)
- Oud: keyword-matching tegen `sector_website_expectations.must_have/should_have/nice_to_have` met expliciete punten per criterium.
- Nieuw: Claude Haiku classificeert de website op een 4-tier schaal (A=15 / B=10 / C=5 / D=0 punten), gebaseerd op `website_signals.positive` + `website_signals.negative` uit sectors.py v2.
- Output contract uitgebreid met `tier` + `rationale` (voor UI + opener/review generator gebruik).
- Cost: ~€0.00015 per lead (~1% van per-lead Claude-budget).
- Per-signal `checks[]` lijst blijft behouden voor UI-transparantie.
- **Signature change**: `check_sector_specific(domain, html, sector, conversion, technical)` → `check_sector_specific(domain, html, sector, anthropic_client=None, supabase_client=None)`. Caller in `analyzer.py` aangepast om AsyncAnthropic door te geven.

---

## Overige

### `config/sectors.py`
- `from __future__ import annotations` toegevoegd (Python 3.9 compat voor `str | None` return type in `classify_subcategory()`).

### Dode data in DB
- Bestaande rijen met `sector IN ('makelaars','bouwbedrijven')` in `heatr_companies_raw` + `heatr_leads` blijven staan als baseline.
- Niet actief archiveren of verwijderen. Worden niet meer verrijkt omdat `get_sector()` raiset en alle enrichment stappen er silent op fallen.

---

## Pre-existing bug opgemerkt (NIET gefixt in deze PR)

`tests/test_outreach_rules.py`: 4 tests falen omdat `_good_step()` default subject `"Gratis website review voor {{company}}"` bevat — `gratis` triggert de spam-word-validatie in `validate_sequence_config()`. Dit staat los van PR2. Fix: helper-default subject aanpassen naar iets zonder spam-word. Separate PR.

---

## Hoe te testen

Smoke test — alle PR2-touched modules importeerbaar + FastAPI app laadt:
```bash
python3 -c "
from config.sectors import SECTORS, list_sectors, get_sector, get_all_sbi_codes
from scoring.icp_matcher import match_icp
from scoring.lead_scoring import score_lead
from enrichment.lead_qualifier import qualify_raw_company
from enrichment.contact_discovery import discover_contacts
from enrichment.company_enrichment import enrich_company
from enrichment.opener_generator import SECTOR_LABELS
from website_intelligence.sector_checker import check_sector_specific
from website_intelligence.analyzer import analyze_website
assert list_sectors() == ['alternatieve_geneeskunde', 'cosmetische_behandelaars']
print('OK')
"
```
→ Expect: `OK` zonder ImportError.

FastAPI:
```bash
python3 -c "from api.main import app; print(len(app.routes))"
```
→ Expect: `69`.

Blocker test suites (PR1):
```bash
python3 -m pytest tests/test_review_email_generator.py tests/test_website_analysis_queue.py tests/test_process_next_enrichment.py -v
```
→ Expect: `26 passed`.

---

## Impact na merge

- `get_sector("makelaars")` en `get_sector("bouwbedrijven")` raisen nu `ValueError` — alle productie-paden vangen dat silent af (geen crashes, leads uit die sectors worden niet meer verrijkt).
- Laag 4 scoring werkt nu via Claude Haiku i.p.v. keyword matching → meer genuanceerd, meer kosten (~€0.00015/lead).
- `sbi_code` matching werkt met zowel 4- als 5-cijferige lead-waarden (bidirectional prefix).
- Geen breaking changes voor bestaande n8n workers of API endpoints.

---

## Niet in deze PR — vooruitkijken

PR3 (enrichment gaps voor Warmr sequence v1.0):
- `website_age_years` — WHOIS/Wayback lookup
- `meta_ads_active` + `ad_focus` — Meta Ad Library scraper
- `latest_review_date` + review cadans — Google Business detail scraper
- `booking_system` detectie — `conversion_checker.py` uitbreiden
- `treatment_focus[]` — Claude Haiku classifier op website tekst
- `local_competitors_higher_rating` + `local_competitors_in_db` — competitor_analyzer aggregaten
