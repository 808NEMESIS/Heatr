# Attribution-Gap Audit — 2026-05-11

**Scope:** read-only verkenning van waarom 55.8% van `heatr_api_cost_log` rows geen
`lead_id` heeft. Per aanroepplek gecategoriseerd als A (correct), B (bug — lead_id in
scope maar niet doorgegeven) of C (architectureel — lead_id niet in scope). Geen
code-wijzigingen.

## Executive Summary

- **Totaal `api_cost_log` rows** (sinds 2026-04-18): **6.776**
- Rows **met** `lead_id`: **2.993** (44.2%)
- Rows **zonder** `lead_id`: **3.783** (**55.8%**)
- Hoogste no-lead-rate fase: **`personalization`** (100% van 1.732 rows)
- Aanroepplekken categorie B (echte bugs): **6**
- Aanroepplekken categorie C (architectureel): **0** (geen pure per-domain dedup-flow gevonden)
- **De gap is een schone binary split**: elke fase scoort 0% of 100% no-lead. Geen mid-range. Dit is **code-shape**, niet semantiek.

---

## 1. Alle aanroepplekken van `_log_api_cost()` / direct insert

### 1a. Helper-call sites (via `log_api_cost(lead_id=...)`)

| # | File:line | Caller | `lead_id` in scope? | Doorgegeven? |
|---|---|---|---|---|
| 1 | [enrichment/treatment_classifier.py:205](enrichment/treatment_classifier.py#L205) | `classify_treatment_focus()` | ✓ param (line 100) | ✓ `lead_id=lead_id` |
| 2 | [enrichment/archetype_classifier.py:249](enrichment/archetype_classifier.py#L249) | `classify_archetype()` | ✓ derived line 135 `lead.get("id")` | ✓ `lead_id=lead_id` |
| 3 | [enrichment/owner_extractor.py:183](enrichment/owner_extractor.py#L183) | `extract_team_from_page_text()` | ✓ param (line 53) | ✓ `lead_id=lead_id` |
| 4 | [campaigns/reply_drafter.py:358](campaigns/reply_drafter.py#L358) | `draft_reply()` | ✓ via `lead` dict | ✓ `lead_id=(lead or {}).get("id")` |

### 1b. Direct `.insert("api_cost_log")` sites

| # | File:line | Caller | `lead_id` in scope? | Doorgegeven? |
|---|---|---|---|---|
| 5 | [website_intelligence/sector_checker.py:186-192](website_intelligence/sector_checker.py#L186-L192) | `check_sector_specific()` | ✗ geen param in signature; **caller** `analyze_website(lead_id=...)` HEEFT 'em | ✗ niet in insert-dict |
| 6 | [enrichment/batched_enrichment.py:239-247](enrichment/batched_enrichment.py#L239-L247) | `batched_enrich()` | ✓ param `lead_id` | ✓ `"lead_id": lead_id` |
| 7 | [integrations/reply_classifier.py:111-118](integrations/reply_classifier.py#L111-L118) | `classify_reply()` | ✗ geen param in signature; **caller** `process_reply(lead_id=...)` HEEFT 'em | ✗ niet in insert-dict |
| 8 | [campaigns/review_email_generator.py:299-307](campaigns/review_email_generator.py#L299-L307) | `generate_review_email()` | ✓ via `lead` dict | ✓ `lead_id=lead.get("id")` |
| 9 | [utils/claude_cache.py:278 + 286](utils/claude_cache.py#L278) | `_log_api_cost()` zelf (defensieve fallback voor missing-column) | ✓ kan via param | ✓ via `_log_api_cost(lead_id=...)`-keten — maar **callers** geven 'em vaak niet door |
| 10 | [utils/cost_guard.py:294-302](utils/cost_guard.py#L294-L302) | `record_block()` (audit-block, niet pricing-call) | ✓ param | ✓ `"lead_id": lead_id` (0 rows in data — geen BLOCKED-events) |

### 1c. Implicit via `cached_claude_call()` → `_log_api_cost()` keten

`cached_claude_call()` heeft **geen `lead_id`-parameter** in zijn signature (zelfs niet
na Fix 1). Alle callers loggen daarom zonder lead-attribution, ongeacht of de caller zelf
`lead_id` in scope heeft.

| # | Caller-file:line | `cache_key_suffix` (= context-string in cost-log) | `lead_id` in caller-scope? |
|---|---|---|---|
| 11 | [website_intelligence/personalization_extractor.py:67](website_intelligence/personalization_extractor.py#L67) | `personalization:{domain}` | ✓ via parent `analyze_website(lead_id=...)` |
| 12 | [website_intelligence/contact_extractor.py:102](website_intelligence/contact_extractor.py#L102) | `contact_extract:{domain}` | ✓ via parent `analyze_website(lead_id=...)` |
| 13 | [enrichment/review_analyzer.py:298](enrichment/review_analyzer.py#L298) | `review_analysis:{company_name}` | ✓ via caller `analyze_reviews()` |

---

## 2. Categorie B — Echte bugs (lead_id beschikbaar in upstream scope maar niet doorgegeven)

### B1. `cached_claude_call()` mist `lead_id`-parameter — **root cause voor 4 contexts**

**Plek:** [`utils/claude_cache.py:106-126`](utils/claude_cache.py#L106-L126) — function signature.

**Probleem:** `cached_claude_call(prompt, cache_key_suffix, model, max_tokens, system,
ttl_hours, supabase_client, context)` heeft **geen `lead_id`-parameter**. Het is een
generieke wrapper; alle 4 callers (personalization_extractor, contact_extractor,
review_analyzer, plus toekomstige) hebben echter `lead_id` in scope maar kunnen het
niet doorgeven.

**Impact in api_cost_log:**

| Context | Rows zonder lead_id | % van totaal-gap |
|---|---:|---:|
| `personalization` | 1.732 | 45.8% |
| `contact_extract` | 656 | 17.3% |
| `review_analysis` | 11 | 0.3% |
| **Subtotaal via cached_claude_call** | **2.399** | **63.4%** |

**Fix-effort:** klein. Voeg `lead_id: str \| None = None` aan signature toe, geef door
aan `_log_api_cost`. Plus update 4 callers om 'em door te geven.

### B2. `check_sector_specific()` mist `lead_id`-parameter — 1.382 rows

**Plek:** [`website_intelligence/sector_checker.py:53-59`](website_intelligence/sector_checker.py#L53-L59) — signature.

**Probleem:** `check_sector_specific(domain, page_html, sector_key, anthropic_client,
supabase_client)` heeft geen `lead_id`. Caller `analyze_website(lead_id=...)`
([website_intelligence/analyzer.py](website_intelligence/analyzer.py)) heeft 'em wel.
Insert op regel 186-192 hardcoded dict zonder `lead_id`-veld.

**Architectureel-vraagteken:** sector_checker is conceptueel **per-domain** — als je
domain-dedup over leads zou doen (1 sector_check per uniek domain), zou 1 call meerdere
leads moeten attribueren. **Maar zo'n dedup-laag bestaat niet in de code**: elke lead
loopt zijn eigen `analyze_website`-flow, met zijn eigen sector_checker-call. 1-op-1 met
lead in praktijk → lead_id valid om mee te geven.

**Impact:** 1.382 rows zonder lead_id (36.5% van de gap).

**Fix-effort:** klein. Add `lead_id` param + insert-veld.

### B3. `classify_reply()` mist `lead_id`-parameter — 2 rows

**Plek:** [`integrations/reply_classifier.py:58-64`](integrations/reply_classifier.py#L58-L64).

**Probleem:** `classify_reply(reply_text, reply_from, lead_company, supabase_client,
anthropic_client)`. Caller `process_reply(lead_id: str, ...)` heeft het. Insert op
regel 111-118 zonder lead_id.

**Impact:** 2 rows (test-data). Volume is laag; bug-status is hetzelfde als B2.

**Fix-effort:** klein, identiek aan B2.

---

## 3. Categorie C — Architectureel (lead_id niet semantisch beschikbaar)

**Geen plekken in deze categorie.**

In de huidige code-base is er **geen aanroeppad dat één Claude-call deelt tussen meerdere
leads**. Sector_checker en personalization_extractor zijn per-domain in concept, maar
worden in praktijk per-lead aangeroepen (geen domain-dedup-laag). Een toekomstige
implementatie van domain-dedup zou ze in C zetten — voor nu zijn ze B.

**Optionele C-overweging:** als je domain-dedup wilt invoeren (1 sector_check per uniek
domain, gedeeld over N leads), moet de cost-attributie omgegooid naar:
- `lead_ids: list[str]` ipv `lead_id: str | None`
- Of een `domain` + `domain_first_seen_lead_id`-koppeling

Maar dat is een ontwerpkeuze, geen bug-fix. Geen rows die hierdoor momenteel zonder
attribution staan.

---

## 4. Data-side — no-lead-rate per context

Live SQL-aggregaat van `heatr_api_cost_log` per context-base (suffix gestript). Run-date:
2026-05-11. Data-window: 2026-04-18 → 2026-05-06.

| context | total | with_lead | no_lead | % missing | Categorie |
|---|---:|---:|---:|---:|:---:|
| personalization | 1.732 | 0 | 1.732 | **100.0%** | B (B1) |
| sector_checker | 1.382 | 0 | 1.382 | **100.0%** | B (B2) |
| contact_extract | 656 | 0 | 656 | **100.0%** | B (B1) |
| review_analysis | 11 | 0 | 11 | **100.0%** | B (B1) |
| reply_classifier | 2 | 0 | 2 | **100.0%** | B (B3) |
| owner_extract | 1.602 | 1.602 | 0 | 0.0% | A |
| archetype_classify | 742 | 742 | 0 | 0.0% | A |
| treatment_classifier | 638 | 638 | 0 | 0.0% | A |
| batched_enrichment | 11 | 11 | 0 | 0.0% | A |
| **Totaal** | **6.776** | **2.993** | **3.783** | **55.8%** | |

**Observatie:** binary split. Elke fase scoort exact 0% of 100% — geen mid-range.
Bevestigt categorisatie: dit is een **code-shape probleem** (signatures missen `lead_id`),
geen mid-runtime-stochastiek.

---

## 5. Aanbevelingen

### 5a. Prio-fix-volgorde (op rows-impact)

| Prio | Fix | Rows geraakt | Effort |
|---|---|---:|:---:|
| 1 | **B1** — `cached_claude_call()` + 4 callers (personalization, contact_extract, review_analysis, + nieuwe) | 2.399 (63%) | klein (1 signature + 4 callers) |
| 2 | **B2** — `check_sector_specific()` + caller | 1.382 (37%) | klein (1 signature + 1 caller) |
| 3 | **B3** — `classify_reply()` + caller | 2 (0.05%) | klein (1 signature + 1 caller) |

Alle drie zijn fundamenteel hetzelfde patroon: signature mist `lead_id`-param, caller
heeft 'em. **Drie fixes los geen 90% van de attribution-gap op, maar 100%** (3.783 / 3.783).

### 5a-bis. Fix-volgorde voor volgende sessie(s)

| # | Fix | Effort | Rows opgelost | Risico-reductie | Notitie |
|---|---|---|---:|---|---|
| 1 | B1 — `cached_claude_call()` lead_id-param + 4 callers | klein | 2.399 (63%) | hoog | Grootste impact, één signature-fix |
| 2 | B2 — `check_sector_specific()` lead_id-param + caller + insert-dict | klein | 1.382 (37%) | hoog | Tweede grote brok |
| 3 | B3 — `classify_reply()` lead_id-param + caller + insert-dict | klein | 2 (0.05%) | laag (volume nihil, wel consistency) | Voltooiing |
| 4 | Soft-warning in `_log_api_cost()` bij ontbrekend `lead_id` | klein | 0 (preventief) | mid (future drift) | Voorkomt nieuwe attribution-bugs |
| 5 | Accumulator-gap (5 fases een `accumulator: LeadCostAccumulator \| None`-param accepteren + `charge()` aanroepen) | **groot** | n.v.t. (separate issue) | hoog (cap-handhaving) | Apart traject; zie accumulator-bevinding |

Fixes 1-3 (B-cluster) lossen samen 100% van de attribution-gap op in `api_cost_log`
(3.783/3.783 rows). Eén volgende sessie zou alle drie kunnen oplossen — hetzelfde patroon,
drie verschillende signatures.

Fix 4 (soft-warning) is preventief: voorkomt dat een toekomstige nieuwe fase opnieuw
`lead_id` vergeet door te geven. Geen rows-impact nu, wel guardrail voor drift.

Fix 5 (accumulator-gap) is een apart en groter traject — zie "Bonus-bevinding" hieronder
voor context. Niet onderdeel van een attribution-fix-sessie maar wel rationeel om er kort
na te plannen.

### 5b. Optie: `lead_id` verplicht maken in `_log_api_cost()`-signature

**Aanbevolen: JA, maar als typed-default-fail.** Concreet:

```python
async def _log_api_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_eur: float,
    *,
    context: str = "",
    lead_id: str | None = None,  # Houd None toelaten voor cost_guard.record_block / overige niet-lead-calls
    ...
):
    if not lead_id and not context.startswith(("BLOCKED:", "system:")):
        logger.warning("Cost log without lead_id (context=%s) — attribution-gap", context)
```

Niet hard-fail, wel auditeerbaar. Hard-required maken zou cost_guard's BLOCKED-flow
breken (die heeft geen lead_id-koppeling per definitie).

### 5c. Optie: `lead_ids: list[str]` voor multi-lead calls

**Niet nodig nu.** Geen multi-lead-shared-call-pad in code (geen categorie C). Pas
overwegen als domain-dedup wordt geïntroduceerd.

### 5d. Optie: `domain` als secundaire attributie-key

**Aanbevolen: JA als nice-to-have**, geen blocker. Voor B1-fases (`personalization`,
`contact_extract`, `sector_checker`) is domain inherent in de call-data. Een
`domain TEXT` kolom op `heatr_api_cost_log` maakt:
- Cross-lead-cost-vergelijking per domain mogelijk
- Future domain-dedup-architecturen (categorie C) attribueerbaar via `domain` ipv `lead_id`

Effort: 1 migration ALTER + insert-call updates. Niet kritiek voor go-live.

---

## 6. Schatting impact op per-lead-cost-cap

> **Scope-disclaimer:** deze bevinding viel buiten de scope van de attribution-gap-audit,
> maar werd ontdekt tijdens de code-walk van `_log_api_cost()`-aanroepplekken. Hij is
> significant omdat hij de werking van een belangrijke safety-rail (per-lead budget-cap
> van €0.05) ondermijnt — 63% van Haiku-calls draagt momenteel niet bij aan die cap.
> Aanbeveling: aparte attributie-vervolgaudit na de B-cluster fixes, of meenemen in
> dezelfde sessie als fix 5 hieronder.

**Per-lead-budget (`MAX_COST_PER_LEAD_EUR=0.05`):** wordt gehandhaafd door
`LeadCostAccumulator.charge()` in [utils/cost_guard.py](utils/cost_guard.py). Dit is
**in-memory per worker-process** en gevoed via expliciete `accumulator.charge(...)`
aanroepen in de fase-modules.

**Belangrijke nuance:** de **in-memory cap is independent van `lead_id` in api_cost_log**.
- Treatment_classifier roept `accumulator.charge(cost_eur, "treatment_classifier")` aan
  (zie [enrichment/treatment_classifier.py:218](enrichment/treatment_classifier.py#L218))
  — accumulator is doorgegeven via param, niet via DB-lookup.
- Owner_extract / archetype / batched doen idem.
- **Sector_checker doet NIET aan accumulator** — geen `accumulator`-parameter in zijn
  signature. Dat is **separate van de attribution-gap** maar ook een gap.
- Personalization_extractor, contact_extractor, review_analyzer: idem, geen
  accumulator-binding.

**Gevolg:**
- ~63% van Haiku-calls (de B1-set via cached_claude_call) draagt **niet bij aan de
  per-lead in-memory cap**. Plus 36% (sector_checker B2) idem.
- **Theoretisch runaway-risico:** een lead die door alle 5 B-fases loopt, accumuleert
  in-memory **0 cost**. Cap kan dus niet triggeren, zelfs niet bij €1+ kosten in B-fases.
  Praktisch: deze fases hebben max ~€0.003/call, dus runaway tot honderden euro's
  vereist honderden duplicate-calls — onwaarschijnlijk maar mogelijk bij een infinite
  loop bug.

**Daily-budget (`ENRICHMENT_DAILY_BUDGET_EUR=0.50`):** werkt **wel** correct — sumeert
`api_cost_log.cost_eur` ongeacht lead_id (zie [utils/cost_guard.py:259](utils/cost_guard.py#L259)).
Daily cap heeft geen attribution-gap risico.

### Rows die een gefixte attributie zou toewijzen

Als B1+B2+B3 gefixt zouden zijn op moment van data-collectie:
- 2.399 + 1.382 + 2 = **3.783 rows** zouden lead_id gekregen hebben
- Dat is **100% van de huidige no-lead-rows** — geen restant van categorie C te overwegen
- Per-lead aggregaat zou stijgen van avg €0.0064 (huidig direct-attribueerbaar) naar
  **avg €0.0116** (alle rows verdeeld over 848 unieke leads — dezelfde €9.87 totaal,
  alleen nu compleet geattribueerd)

---

## 7. Wat NIET in dit rapport zit (uit scope)

- **Geen code-wijzigingen** — alleen analyse
- **Geen UPDATE op historische rows** (retroactive attribution onmogelijk zonder
  reconstructie van call-context)
- **`accumulator`-gap** (5 fases die niet via LeadCostAccumulator gaan) is gerelateerd
  maar separate — gedocumenteerd in §6 als nuance, niet als hoofd-issue
- **Externe APIs** (KvK, Google PageSpeed) niet meegenomen — geen rows in api_cost_log
- **Sonnet Vision** — 0 rows in periode, niet relevant voor gap
- **Recommended SQL queries voor follow-up** (geen scope deze sessie):

  ```sql
  -- After fix-deploy: re-check no-lead rate over 7d window
  SELECT context,
         COUNT(*) AS total,
         COUNT(lead_id) AS with_lead,
         ROUND(100.0 * (COUNT(*) - COUNT(lead_id)) / COUNT(*), 1) AS pct_missing
  FROM heatr_api_cost_log
  WHERE created_at >= '2026-05-11'
  GROUP BY context
  ORDER BY pct_missing DESC;
  ```

  Verwachte uitkomst na fixes: alle contexts op 0% no-lead behalve `BLOCKED:*` en
  eventuele `system:*` audit-rows.
