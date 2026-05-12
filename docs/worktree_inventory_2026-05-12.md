# Heatr Werkboom Inventarisatie — 2026-05-12

Read-only inventarisatie. Geen code-wijzigingen, geen git-acties.
Doel: Sami beslist per werk-stroom commit / parkeren / weggooien.

---

## Executive Summary

- **Modified files in werkboom:** 43 (incl 14 deletions van frontend/)
- **Untracked files:** 122 (~75 code-files + 14 migrations + 8 docs + 25+ tests)
- **Werk-stromen geïdentificeerd:** **9** (Stream J vervalt — gefuseerd in G/D/H per Sami's instructie)
- **AF-stromen veilig commit-baar nu:** 4 — A (accumulator), C (PR2 sectors), G (sequence engine), I (cost-audit docs)
- **WIP-stromen:** 3 — B (PR1 blockers, mig 005 niet gedraaid), D (PR3, foundation untracked), H (CRM-sprint, mig 017 niet gedraaid)
- **AF maar reviewt-eerst:** 2 — E (frontend-cutover, geen scope-doc), F (api/main.py +2619 regels, kruisstromen)
- **WEG-kandidaten:** `.env.bak` + `.env.backup-pre-cleanup` (backup-files, niet in git), oude `frontend/*.html/css/js` (cutover-deletes)
- **Kritieke migration-gaps:** 005 + 017 NIET gedraaid in Supabase. Rest 006-016 wel.
- **Migration-nummer-conflicten:** 015 (claude_cache vs crm_status_override) + 016 (api_cost_log_cache_hit vs import_runs). Beide untracked-zijden zijn al gedraaid; advies: hernumeren naar 018 + 019 vóór commit.
- **Dominant blocker voor schone commits:** `utils/cost_guard.py` + `utils/vision_cache.py` zijn UNTRACKED maar dependency van Stream A (deze sessie). Stream A kan technisch niet committen zonder eerst PR3-foundation te committen.

---

## 1. Volledige Werkboom State

### 1.1 git status (samengevat)

```
On branch main
Your branch is ahead of 'origin/main' by 9 commits.

43 modified files (incl 14 frontend/ deletions)
122 untracked files (75 code + 14 migrations + 8 docs + 25+ tests + 1 frontend-next/)
0 staged
0 stashes
1 branch (main)
```

### 1.2 git diff --stat totalen

```
43 files changed, 4342 insertions(+), 5972 deletions(-)
```

Grootste diffs:
- `api/main.py`: +2619 regels (Stream F + H)
- `config/sectors.py`: 899 regels diff (Stream C)
- `job_queue/enrichment_queue.py`: +512 regels (Streams D + A + B mixed)
- `campaigns/sequence_engine.py`: +185 regels (Stream G)
- `enrichment/company_enrichment.py`: +102 regels (Stream C of G/H?)
- `website_intelligence/visual_analyzer.py`: +81 regels (Stream D)
- `enrichment/contact_discovery.py`: +83 regels (Stream C)
- Frontend deletes: 14 files, ~5430 regels weg (Stream E)

### 1.3 Recent commits in HEAD

```
1ef52f5 fix(attribution): close attribution-gap in 3 B-clusters
8e26047 docs(cost-audit): attribution-gap trace report
2100254 fix(cost-tracking): consolidate API pricing in config/pricing.py
8ad5f6c fix(cache): create heatr_claude_cache table + per-context TTL logic
8596bb8 feat(research-library): categorie 5 ...
... (research-library categories 1-5)
d7294cb 3 features: discovery schedules + trigger-based recontact + reply classifier
0a11334 Apply Claude Design handoff: warm blush/ivory system
f96052c Frontend redesign: Claude Design system with data-dense UI
6b1e55c Optimize enrichment: 63% cost reduction via batching + compact prompts
5bae839 Fix broken Heatr cost tracking: wrong column names
784961d Add 3 enrichment features: competitor benchmarking, review analysis, opener generation
ae959cd Split behandelaren into alternatieve_geneeskunde + cosmetische_behandelaars
e8fdc28 Initial commit: Heatr B2B lead discovery platform
```

---

## 2. Context-MD Samenvattingen

| File | Datum | Kern |
|---|---|---|
| `BLOCKERS_PR3_SCOPE.md` | 2026-04-21 | Scope-doc voor PR3 = 6 Warmr v1.0 datapunten + cost-controls. Specificeert env-vars (ENRICHMENT_DAILY_BUDGET_EUR=0.50, MAX_COST_PER_LEAD_EUR=0.05) + rate-limiter entries + migration 006. **STOP-GATE** vraagt user-akkoord op 5 design-keuzes (RDAP, Playwright, 0.2 buffer, hybrid allowlist, cost-defaults). |
| `BLOCKERS_SCOPE.md` | 2026-04-20 | Scope-doc PR1 = 2 runtime-blockers: `campaigns/review_email_generator.py` (Trojan Horse review email) + `job_queue/website_analysis_queue.py` (separate worker queue). Plus `process_next_enrichment` wrapper in enrichment_queue. Migration 005 optioneel. Geen refactors, geen schema-consolidatie. |
| `CHANGES.md` | 2026-04-21 | Implementation-report van PR1. 3 modules toegevoegd, 26 tests pass, FastAPI laadt 69 routes. Niet-breaking. |
| `CHANGES_PR2.md` | 2026-04-21 | Implementation-report PR2 = sectors.py v2 cleanup. 9 files met mechanische key-renames (`exclude_keywords` → `disqualifiers`, etc.) + dood-sector-branches strippen (makelaars/bouwbedrijven verwijderd) + sector_checker volledig herschreven naar Claude Haiku tier-classifier. |
| `CHANGES_PR3.md` | 2026-04-21 | Implementation-report PR3 = Warmr v1.0 datapunten + cost-controls + PR2.5 Vision-cache. ~25 files, 55 tests pass, migration 006. Worst-case €15/maand bij daily-budget €0.50. |
| `CLAUDE.md` | 2026-04-30 | Centraal project-doc. Bevat updates voor auth-model (X-API-Key + Supabase JWT + LEGACY_DEV_TOKEN_ALLOWED) + nieuwe env-vars (HEATR_API_KEY, SUPABASE_JWT_SECRET, ENABLE_CAMPAIGN_SENDS). Bewijst dat Stream F (auth-hardening) tussen 21 en 30 april is uitgevoerd. |
| `HEATR_AUDIT.md` | 2026-04-20 | Stand-van-zaken audit waar PR1+PR2+PR3 uit voortvloeiden. Beschrijft 2 runtime-blockers + schema-divergentie + scoring drift + email-waterfall behavior. Inhoudelijk inmiddels verouderd door PR1-3 + cost-audit chain. |
| `MVP_GAPS.md` | 2026-04-07 | Sessie-7 hardening-audit (pre-PR1). 16 issues van gestubde functies + missing indexes + error-paths. Grotendeels al opgelost in latere commits. Historisch document. |
| `SKILL.md` | 2026-04-05 | Skill-instructie voor "heatr" trigger-words. Project overview. Historisch — projectstructuur en sector-info inmiddels gedateerd (noemt nog 2 oude sectoren + frontend/ vanilla). |
| `heatr_CLAUDE.md` | 2026-04-05 | **DUPLICAAT van CLAUDE.md** — andere datum, oudere content. Audit (HEATR_AUDIT.md) noemt dit ook als duplicate. **WEG-kandidaat.** |

**Niet in root maar wel relevant:**

| File | Inhoud |
|---|---|
| `docs/cost_audit_2026-05-11.md` | Cost-audit rapport waaruit accumulator-fix is voortgekomen (deze sessie + voorgaande in chain). Read-only, klaar voor commit. |
| `docs/attribution_gap_audit_2026-05-11.md` | Sister-rapport, leidde tot B-cluster commits (8ad5f6c → 1ef52f5). Read-only, klaar voor commit. |
| `docs/sessions/build-log.md` | 886 regels, 12+ sessie-headers (Apr 30 → May 7). **Hoofdbron voor de "wat is wanneer gebouwd" reconstructie.** Bevat top-4 CRM-sprint + sequence v1.0/v3.x werk. |
| `docs/sessions/crm-*.md` | 4 docs (data-audit, feature-gap, automation-shortlist, priorities) van 1-5 mei. **Scope-bronnen voor Stream H (CRM-tooling).** |
| `docs/sessions/go-live.md` | 30 april, 3-blockers checklist voor eerste live mail (Warmr-koppeling, email-verifier, e2e-smoke). |
| `docs/sessions/backlog.md` | Lopende lijst van latente issues (review_recency parse-bug, email_verifier 0%-issue, Loom Task Orchestrator, Warmr schema-debt, personalization-gate test-lead-bypass). |
| `docs/sessions/test-prompts.md` | 4 opeenvolgende smoke-test-prompts voor go-live. |

---

## 3. Werk-stromen

### Stream A — Accumulator-fix (huidige sessie)
**Beschrijving:** Wire `LeadCostAccumulator` per-lead instance door alle 9 fases die Claude-calls doen, zodat €0.05/lead-cap daadwerkelijk gehandhaafd wordt.
**Bron:** Geen MD-doc. User-spec van 2026-05-12, voortvloeiend uit cost-audit chain.
**Bestanden:**
- Modified (pure accumulator-fix, ~30-36 regels elk): `enrichment/batched_enrichment.py`, `enrichment/review_analyzer.py`, `integrations/reply_classifier.py`, `website_intelligence/analyzer.py`, `website_intelligence/contact_extractor.py`, `website_intelligence/personalization_extractor.py`, `website_intelligence/sector_checker.py`
- Modified (mixed met PR3): `job_queue/enrichment_queue.py` (accumulator ~50 regels van de 512 totaal)
- Untracked: `scripts/verify_accumulator.py`

**Status:** **AF** (verify 4/4 ✓, 318/318 tests pass, klaar voor commit)
**Afhankelijkheden:** **KRITIEK** — Stream D's `utils/cost_guard.py` is untracked. Stream A imports `from utils.cost_guard import LeadCostAccumulator, guarded_call`. Commit van A zonder eerst D's foundation = broken HEAD.
**Risico bij commit:** 🟢 Groen, mits D-foundation eerst gecommit. Pure additie, geen breaking changes.

---

### Stream B — PR1 Runtime blockers (2026-04-21)
**Beschrijving:** 3 modules toevoegen die door api/main.py worden geïmporteerd maar niet bestaan → ImportError-crashes op `/leads/{id}/send-review-email`, `/website-intelligence/process-next`, `/enrichment/process-next`.
**Bron:** `BLOCKERS_SCOPE.md` + `CHANGES.md`.
**Bestanden:**
- Untracked (new): `campaigns/review_email_generator.py`, `job_queue/website_analysis_queue.py`
- Untracked (test): `tests/test_review_email_generator.py`, `tests/test_website_analysis_queue.py`, `tests/test_process_next_enrichment.py`
- Untracked (migration): `migrations/005_website_analysis_failed_reason.sql` ⚠ **NIET GEDRAAID**
- Modified (process_next_enrichment toevoeging zit in): `job_queue/enrichment_queue.py` (mixed met PR3 + accumulator)

**Status:** **AF-functioneel maar uncommitted** (per CHANGES.md "26 tests passed" + bestand-inhoud is af. Migration 005 nog niet in Supabase.)
**Afhankelijkheden:** Geen externe. Wel: `process_next_enrichment` zit in enrichment_queue.py wat ook door D+A wordt aangeraakt.
**Risico bij commit:** 🟢 Groen, mits migration 005 vóór commit gedraaid wordt. Niet-breaking (alleen toevoegingen, kolommen optioneel).

---

### Stream C — PR2 sectors.py v2 cleanup (2026-04-21)
**Beschrijving:** Productie-code aanpassen aan nieuw `config/sectors.py` v2-schema (SBI 2025 5-cijferig, subcategories, `website_signals`, geen per-criterium punten). Mechanische renames + dood-sector-branches strippen (makelaars/bouwbedrijven verwijderd) + `sector_checker.py` herschreven naar Haiku tier-classifier.
**Bron:** `CHANGES_PR2.md`.
**Bestanden:**
- Modified: `config/sectors.py` (899-regel diff — grootste delta in scope C), `scoring/icp_matcher.py`, `scoring/lead_scoring.py`, `enrichment/lead_qualifier.py`, `enrichment/company_enrichment.py`, `enrichment/contact_discovery.py`, `tests/test_outreach_rules.py`
- **Al gecommit per sector_checker.py docstring + Apr-21-datum:** `website_intelligence/sector_checker.py` Haiku rewrite zit al in HEAD (via `1ef52f5`). Huidige diff op sector_checker is alleen Stream A.

**Status:** **AF** (CHANGES_PR2 zegt "26 passed + FastAPI 69 routes"; 5 weken oud)
**Afhankelijkheden:** **Risk van drift** — 5 weken oud werk. Sequence-engine v3.x (Stream G) en CRM-sprint (Stream H) zijn ER bovenop gebouwd. Test-suite is daarna doorgegroeid van 26 → 318. Re-test verplicht vóór commit.
**Risico bij commit:** 🟡 Geel — herhaal pytest om regressions te detecteren; mogelijk merge-conflict met Stream G/H wijzigingen aan dezelfde files (icp_matcher, lead_qualifier).

---

### Stream D — PR3 Warmr v1.0 datapunten + cost-controls foundation (2026-04-21)
**Beschrijving:** 6 ontbrekende datapunten voor Warmr Sequence v1.0 (booking_system, latest_review_date, treatment_focus, website_age_years, meta_ads_active, local_competitors) + cost-controls foundation (utils/cost_guard.py, utils/vision_cache.py, ENRICHMENT_DAILY_BUDGET_EUR=0.50, MAX_COST_PER_LEAD_EUR=0.05) + PR2.5 vision-cache + 7 nieuwe enrichment-steps in pipeline.
**Bron:** `BLOCKERS_PR3_SCOPE.md` + `CHANGES_PR3.md`.
**Bestanden:**
- **Untracked foundation (KRITIEK voor Stream A):** `utils/cost_guard.py`, `utils/vision_cache.py`
- Untracked new modules: `enrichment/domain_age_scraper.py`, `enrichment/meta_ads_scraper.py`, `enrichment/google_reviews_scraper.py`, `enrichment/website_crawler_v2.py`, `enrichment/treatment_from_google.py`
- Untracked tests: `tests/test_cost_guard.py`, `tests/test_pr3_modules.py`
- Untracked scripts: `scripts/run_enrichment_worker.py` (continuous worker-loop voor PR3-pipeline)
- Modified (pure PR3): `website_intelligence/visual_analyzer.py` (vision-cache), `website_intelligence/competitor_analyzer.py` (local_competitors), `website_intelligence/conversion_checker.py` (booking_system enum), `enrichment/company_enrichment.py` (partial), `enrichment/contact_discovery.py` (partial), `utils/rate_limiter.py` (3 new rate-limit entries)
- Modified (mixed): `job_queue/enrichment_queue.py` (~460 regels = 7 nieuwe steps + page_text helper)
- Migrations: `006_warmr_sequence_fields.sql` ✓ GEDRAAID, `007_enrichment_schema_fixes.sql` ✓ GEDRAAID, `APPLY_ME_008.sql` ✓ GEDRAAID, `APPLY_ME_IN_SUPABASE.sql` ✓ GEDRAAID (gecombineerd 006+007+crawler)
- **Al gecommit (deels):** `enrichment/batched_enrichment.py` foundation in `6b1e55c`; `enrichment/archetype_classifier.py` + `enrichment/owner_extractor.py` + `enrichment/treatment_classifier.py` + `integrations/reply_classifier.py` in eerdere commits

**Status:** **AF-grotendeels** (CHANGES_PR3 zegt "55 passed", migrations gedraaid, foundation werkt — cost-audit chain bouwde 4 commits op cost_guard.py zonder defecten te vinden). **WIP-scherpe-rand:** foundation-bestanden cost_guard.py + vision_cache.py zijn STILL untracked na 5 weken.
**Afhankelijkheden:** Geen externe. Wel: Stream A loopt op deze foundation.
**Risico bij commit:** 🟡 Geel — enorme scope (~25 files); enrichment_queue.py kruisstromen-conflict met A + B.

---

### Stream E — Frontend-cutover (vanilla HTML → React/Vite)
**Beschrijving:** Hele oude `frontend/` (vanilla HTML+CSS+JS + Claude Design handoff) verwijderd, vervangen door `frontend-next/` (React + Vite + TS app).
**Bron:** **Geen scope-MD gevonden.** Indirect bewijs in build-log dat top-4 CRM-sprint (May 5-7) frontend-next/ actief gebruikt (LeadDetail.tsx, CRMActivity.tsx, CampagneLaunch.tsx aangeraakt).
**Bestanden:**
- Deleted (14 files, ~5430 regels): `frontend/analytics.html`, `app.js`, `campaigns.html`, `crm.html`, `dashboard.html`, `inbox.html`, `index.html`, `kit.css`, `lead-detail.html`, `leads.html`, `search.html`, `style.css`, `tokens.css`, `website-kansen.html`
- Untracked (~50 files): `frontend-next/` — `package.json`, `package-lock.json`, `vite.config.ts`, `tsconfig.*.json`, `eslint.config.js`, `index.html`, `src/{App.tsx, main.tsx, index.css, lib/{api.ts, cn.ts, format.ts, types.ts}, components/layout/{CostBadge.tsx, PageHeader.tsx, Shell.tsx, Sidebar.tsx, WorkerStatus.tsx}, components/ui/{badge, button, card, input, tabs}.tsx, pages/{Analytics, CRM, CRMActivity, CampagneLaunch, Campagnes, Dashboard, Inbox, LeadDetail, Leads, LeadsImport, Placeholder, WebsiteKansen, Zoeken}.tsx, assets/{hero.png, react.svg, vite.svg}}, public/{favicon.svg, heatr-logo.png, icons.svg}`

**Status:** **AF-functioneel maar uncommitted + geen scope-MD.** Top-4 sprint heeft frontend-next/ live gebruikt → werkt.
**Afhankelijkheden:** Pages in frontend-next gebruiken nieuwe endpoints van Stream F (`/leads/{id}/test-mode`, `/leads/{id}/thread`, `/timeline/{id}?compact`, `/campaigns/launch` met completeness banner, etc.).
**Risico bij commit:** 🔴 Rood-voor-bundelen, 🟡 Geel-voor-eigen-commit — frontend-next/ is een hele React-app (~50 files + lock-files); hoort in een **eigen frontend-cutover commit** met duidelijke message "feat(frontend): React/Vite cutover from vanilla HTML".

---

### Stream F — API auth-hardening + endpoint-uitbreidingen
**Beschrijving:** `api/main.py` is met +2619 regels diff de grootste mutatie. Bevat (per CLAUDE.md update Apr 30 + go-live.md + build-log):
1. Auth-model: X-API-Key (HEATR_API_KEY) + Supabase JWT decode (SUPABASE_JWT_SECRET) + LEGACY_DEV_TOKEN_ALLOWED escape hatch
2. ENABLE_CAMPAIGN_SENDS master kill-switch
3. Endpoints van top-4 sprint (zit dus over in Stream H): `/leads/{id}/test-mode`, `/leads/{id}/thread`, `/timeline/{lead_id}?compact`, `/campaigns/launch` + `/campaigns/preview` met completeness-check
4. CRM-endpoints (Stream H): `/crm/activity-board`, `/crm/activity`, etc.
5. Mogelijk nieuwe sequence/launch-endpoints (Stream G)
**Bron:** CLAUDE.md (Apr 30 sectie "Auth model"), go-live.md, build-log meerdere sessies.
**Bestanden:**
- Modified: `api/main.py` (+2619), `config/database.py` (+10), `utils/startup_validator.py` (+18), `.env.example` (+4), `CLAUDE.md` (+25)
- Untracked: `tests/test_auth.py`

**Status:** **Mixed AF/WIP** — auth-hardening is live (CLAUDE.md zegt "Legacy dev-token tijdelijk true tijdens frontend-cutover"; ENABLE_CAMPAIGN_SENDS=false default).
**Afhankelijkheden:** Endpoints in deze file linken aan Streams G, H, B (process_next_enrichment).
**Risico bij commit:** 🔴 Rood — 2619-regel diff in 1 file met mengsels uit 4+ streams. Niet veilig in één commit zonder hunk-analyse. Aanbeveling: aparte vervolg-sessie om api/main.py te ontvlechten of bewust als "API consolidation snapshot" committen na alle dependency-streams.

---

### Stream G — Sequence engine v1.0 → v3.2 + templates + principles
**Beschrijving:** Multi-sessie werk (Apr 30 — May 7) op de Warmr-sequence-engine. Includes:
- v1.0 sequence-templates voor cosmetische klinieken (3 bruggen × 3 mails)
- v3.x iteraties: pick_brug, pick_signaal_blok 6-tier resolver, sector_impact_frame token, stad_of_sector fallback
- Opener-fix in company_enrichment.py (Test 1' findings, May 7)
- Contact-confidence rule in lead_naming.py
**Bron:** `docs/sessions/build-log.md` (12+ sessie-headers).
**Bestanden:**
- Modified: `campaigns/sequence_engine.py` (+185), `enrichment/company_enrichment.py` (gedeeld met C/H)
- Untracked: `config/principles_loader.py`, `config/sequence_templates.py`, `config/opener_principles.md`, `utils/sector_impact.py`, `utils/signal_picker.py`, `utils/lead_naming.py`
- Untracked tests: `tests/test_principles_loader.py`, `tests/test_sequence_gate.py`, `tests/test_pick_brug.py`, `tests/test_sector_impact.py`, `tests/test_signal_picker.py`
- Untracked scripts: `scripts/render_warmr_payload.py` (Warmr-payload inspectie zonder send)

**Status:** **AF** (build-log 2026-05-07 zegt v3.2 live + 318/318 tests + smoke 3/3 leads OK)
**Afhankelijkheden:** Stream H deelt utils/lead_naming.py + utils/sector_impact.py (zelfde files, samen-gegroeid). Stream F linkt voor /campaigns/launch endpoint.
**Risico bij commit:** 🟡 Geel — overlap met Stream H op utils-files; vereist ofwel samen-committen met H, ofwel duidelijke afbakening per file.

---

### Stream H — Top-4 CRM-sprint (2026-05-05 → 2026-05-07)
**Beschrijving:** 4-fase sprint na CRM-audit (May 1):
1. Test-mode flag per lead (May 5) — migratie 017, utils/email_sendability, integrations/warmr_client BCC, test-toggle endpoint+UI
2. Pre-launch enrichment-completeness check (May 6) — utils/enrichment_check, /campaigns/launch+preview preflight, CampagneLaunch.tsx banners
3. Email-thread-view per lead (May 6) — utils/lead_thread, GET /leads/{id}/thread, LeadDetail Thread-tab
4. Activity-timeline op kanban-card-flip (May 6) — GET /timeline/{id}?compact+limit, CRMActivity RecentActivity
**Bron:** `docs/sessions/crm-priorities.md` + 4 sessie-headers in build-log.
**Bestanden:**
- Untracked utils: `utils/email_sendability.py`, `utils/enrichment_check.py`, `utils/lead_thread.py`, `utils/lead_activity.py`, `utils/lead_import.py`
- Modified: `scoring/feedback_processor.py` (+46), `integrations/warmr_client.py` (+38, BCC voor test-mode)
- Untracked tests: `tests/test_email_sendability.py`, `tests/test_warmr_test_mode.py`, `tests/test_enrichment_check.py`, `tests/test_lead_thread.py`, `tests/test_timeline_compact.py`, `tests/test_lead_activity.py`, `tests/test_lead_import.py`, `tests/test_lead_naming.py`, `tests/test_contact_inference.py`, `tests/test_reply_drafter.py`
- Migrations: `009_lead_campaign_history.sql` ✓, `010_review_recency.sql` ✓, `011_reply_inbox_body_html.sql` ✓, `012_feedback_runs.sql` ✓, `013_campaigns_audit.sql` ✓, `014_archetype_classification.sql` ✓, `015_crm_status_override.sql` ✓ (number conflict), `016_import_runs.sql` ✓ (number conflict), `017_is_test_lead.sql` ⚠ **NIET GEDRAAID**
- Frontend (zit in Stream E): `LeadDetail.tsx`, `CRMActivity.tsx`, `CampagneLaunch.tsx`

**Status:** **WIP-grotendeels-AF** — code af + tests groen (build-log 290/290 → 318/318), maar migratie 017 niet gedraaid (build-log expliciet: "Toggle faalt op echte lead omdat migratie 017 nog niet applied is").
**Afhankelijkheden:** Stream G deelt utils/lead_naming.py + sector_impact.py. Stream F endpoints. Stream E frontend-pages.
**Risico bij commit:** 🟡 Geel — vereist migratie 017 + hernumeren van 015/016 conflicten + samen-committen met G of duidelijke utils-splitsing.

---

### Stream I — Cost-audit chain docs (read-only)
**Beschrijving:** Restant van cost-audit + attribution-gap chain die al gecommit is (8ad5f6c → 1ef52f5). Alleen rapport-docs zijn untracked.
**Bron:** Zichzelf — de docs zijn de bron.
**Bestanden:**
- Untracked: `docs/cost_audit_2026-05-11.md`, `docs/attribution_gap_audit_2026-05-11.md`
- Modified (mogelijk noise): `.claude/scheduled_tasks.lock` (deleted, claude-runtime artifact)

**Status:** **AF** (rapporten zijn final state, chain is gecommit)
**Afhankelijkheden:** Geen.
**Risico bij commit:** 🟢 Groen — pure docs, zero-risk.

---

### Streams die afvallen / niet werkstroom

**Stream J — Misc scripts (per Sami's instructie ontbonden):**
- `scripts/render_warmr_payload.py` → Stream G (sequence-engine inspectie)
- `scripts/run_enrichment_worker.py` → Stream D (PR3-pipeline continuous worker)
- `scripts/run_worker.py` → Orphan (scraping-worker, infrastructure pre-dates streams; geen MD-doc). **Sami's beslissing nodig** of dit bij Stream H of als standalone "infra" commit.

**Misc files niet bij een stream:**
- `.env.bak`, `.env.backup-pre-cleanup` → **WEG** (backup-files, niet in git op te nemen)
- `.claude/scheduled_tasks.lock` (deleted) → claude-runtime artifact, **WEG** (geen actie nodig)
- 10 MD-files in root (BLOCKERS_*, CHANGES*, HEATR_AUDIT, MVP_GAPS, SKILL, heatr_CLAUDE) → docs. **Aparte mini-stream "docs sweep"?** Sami beslist of in `/docs` consolideren of in root laten.

---

## 4. File-conflicten (meerdere streams in zelfde file)

| File | Stream-mix | Verhouding (geschat) |
|---|---|---|
| `job_queue/enrichment_queue.py` | D + A + B | 90% D (7 new steps + helper), ~10% A (accumulator instantiate + propagate), <2% B (process_next_enrichment wrapper) |
| `api/main.py` | F + H + G + B | ~40% F (auth + general endpoints), ~30% H (top-4 endpoints), ~20% G (sequence/launch), ~10% B (process_next imports) |
| `enrichment/company_enrichment.py` | C + G | ~50% C (sector rename + lijst-strippen), ~50% G (opener-fix May 7) |
| `enrichment/contact_discovery.py` | C + G/H? | ~80% C (sector rewrite), rest mogelijk Stream H?? — unclear |
| `utils/lead_naming.py` (untracked) | G + H | Beide streams hebben 'em aangeraakt; logica overlapt |
| `utils/sector_impact.py` (untracked) | G | 100% G (Mail 1 v3.2 token-mapping) |
| `tests/test_outreach_rules.py` | C + G | C bug-fix + G's testupdates voor sequence gating |

**Niet-conflict (single-stream files):**
- Alle 6 fase-modules in Stream A scope (batched_enrichment, review_analyzer, reply_classifier, contact_extractor, personalization_extractor, sector_checker) — pure A
- `website_intelligence/visual_analyzer.py`, `competitor_analyzer.py`, `conversion_checker.py` — pure D
- `website_intelligence/analyzer.py` — pure A (pass-through)
- `config/sectors.py`, `scoring/icp_matcher.py`, `scoring/lead_scoring.py`, `enrichment/lead_qualifier.py` — pure C

---

## 5. Migrations Status

### 5.1 Tracked in HEAD
| Migration | Stream | In Supabase? |
|---|---|---|
| `004_discovery_recontact_replies.sql` | Pre-PR1 (al in HEAD) | ✓ Gedraaid |
| `015_claude_cache.sql` | Cost-audit chain (8ad5f6c) | ✓ Gedraaid |
| `016_api_cost_log_cache_hit.sql` | Cost-audit chain (2100254) | ✓ Gedraaid |

### 5.2 Untracked
| Migration | Stream | Beoogde kolom/tabel | In Supabase? |
|---|---|---|---|
| `005_website_analysis_failed_reason.sql` | B (PR1) | `leads.website_analysis_failed_reason` | **✗ NIET gedraaid** |
| `006_warmr_sequence_fields.sql` | D (PR3) | `leads.booking_system`, `treatment_focus`, `meta_ads_active`, `enrichment_blocked_reason`, etc. | ✓ Gedraaid |
| `007_enrichment_schema_fixes.sql` | D (PR3) | `leads.kvk_employee_count_range`, `enrichment_data.email_candidate` | ✓ Gedraaid |
| `009_lead_campaign_history.sql` | H (CRM) | `lead_campaign_history` (table) | ✓ Gedraaid |
| `010_review_recency.sql` | H (CRM) of G? | `leads.review_recency_checked_at` | ✓ Gedraaid |
| `011_reply_inbox_body_html.sql` | H (CRM) | `reply_inbox.body_html` | ✓ Gedraaid |
| `012_feedback_runs.sql` | H (CRM) | `feedback_runs` (table) | ✓ Gedraaid |
| `013_campaigns_audit.sql` | H (CRM) | `campaigns` (table) | ✓ Gedraaid |
| `014_archetype_classification.sql` | D (PR3) | `leads.archetype` | ✓ Gedraaid |
| `015_crm_status_override.sql` ⚠ NUMMERCONFLICT | H (CRM) | `leads.manual_status_override` | ✓ Gedraaid |
| `016_import_runs.sql` ⚠ NUMMERCONFLICT | H (CRM) | `import_runs` (table) | ✓ Gedraaid |
| `017_is_test_lead.sql` | H (CRM top-4 #1) | `leads.is_test_lead` | **✗ NIET gedraaid** |
| `APPLY_ME_008.sql` | D (PR3) | `leads.has_whatsapp`, `personalized_opener`, `enrichment_data` kolommen | ✓ Gedraaid |
| `APPLY_ME_IN_SUPABASE.sql` | D (PR3, gecombineerd) | 006 + 007 + crawler ext kolommen | ✓ Gedraaid (overlapt met 006+007) |

### 5.3 Nummer-conflicten
- **015**: `claude_cache.sql` (tracked, in HEAD) ⊥ `crm_status_override.sql` (untracked, Stream H)
- **016**: `api_cost_log_cache_hit.sql` (tracked, in HEAD) ⊥ `import_runs.sql` (untracked, Stream H)

**Sami's voorkeur (vóór deze sessie vastgelegd):** hernumeren van de **untracked** kant → `018_crm_status_override.sql` + `019_import_runs.sql`. Geen risico op history-corruption.

**Empty slot:** `008` (alleen APPLY_ME_008.sql, geen genummerd bestand).

### 5.4 Aanbeveling per migration
| Voor commit van | Eerst draaien |
|---|---|
| Stream A (accumulator) | — (geen mig nodig; werkt op gedraaide cost_guard structuren) |
| Stream B (PR1) | **`005`** vóór commit |
| Stream H (CRM top-4) | **`017`** vóór commit + hernumeren `015→018` + `016→019` |
| Stream G (sequence) | — (geen mig nodig) |

---

## 6. Voorgestelde Commit-volgorde

**Kernprincipe:** dependencies eerst. Stream A leeft op Stream D's foundation. Stream H leeft op migratie 017. Stream B leeft op migratie 005. Stream E heeft Stream F endpoints nodig.

### Commit 1: **Stream I — cost-audit + attribution rapport-docs**
- Files: `docs/cost_audit_2026-05-11.md`, `docs/attribution_gap_audit_2026-05-11.md`
- Title: `docs(cost-audit): cost + attribution audit rapporten`
- Migrations: geen
- Effort: 5 min
- Risico: 🟢 Groen — pure read-only docs
- Waarom eerst: warming-up commit, valideert dat git-flow werkt

### Commit 2: **Stream D-foundation — utils/cost_guard.py + vision_cache.py**
- Files: `utils/cost_guard.py`, `utils/vision_cache.py`, `tests/test_cost_guard.py`
- Title: `feat(cost-controls): per-lead accumulator + vision-cache foundation`
- Migrations: ✓ 006 + 007 al gedraaid, niets extra
- Effort: 15 min (test-suite verifiëren met deze foundation alleen)
- Risico: 🟢 Groen — toevoegingen, geen wijzigingen aan bestaande code
- Waarom eerst: **deblocker voor Stream A** (commit van A heeft cost_guard.py in HEAD nodig)

### Commit 3: **Stream A — Accumulator-fix (deze sessie)**
- Files: 7 fase-modules + analyzer.py + verify_accumulator.py + accumulator-hunks van enrichment_queue.py
- Title: `fix(cost): instantiate LeadCostAccumulator across 9 phases — per-lead cap actief`
- Migrations: geen
- Effort: 30 min (hunk-staging op enrichment_queue.py + verify run + 318 tests)
- Risico: 🟢 Groen mits Commit 2 eerst
- Waarom hier: D-foundation in HEAD, rest van D komt later

### Commit 4: **Stream B — PR1 Runtime blockers**
- Files: `campaigns/review_email_generator.py`, `job_queue/website_analysis_queue.py`, 3 test-files, `migrations/005_*.sql`, process_next_enrichment-hunk van enrichment_queue.py
- Title: `feat(blockers): review_email_generator + website_analysis_queue + process_next wrappers`
- Migrations: **eerst `005` draaien in Supabase**
- Effort: 45 min (migration + hunk-staging + 26 tests verify)
- Risico: 🟢 Groen mits migration eerst
- Waarom hier: scope-doc Apr 21, los van A en D-rest, eenvoudig

### Commit 5: **Stream D-rest — PR3 datapunten + pipeline-wiring**
- Files: alle untracked enrichment/* + website_intelligence/* PR3-modified + rest van enrichment_queue.py + utils/rate_limiter.py + tests/test_pr3_modules.py + scripts/run_enrichment_worker.py
- Title: `feat(pr3): Warmr v1.0 datapunten (booking, reviews, treatment, domain-age, meta-ads, competitor)`
- Migrations: ✓ 006/007/008/APPLY_ME al gedraaid
- Effort: 1-2 uur (zorgvuldige hunk-staging op enrichment_queue.py + test 55+ passed verify)
- Risico: 🟡 Geel — grote scope, raakt vele files; doe een extra smoke-test
- Waarom hier: foundation in HEAD, accumulator-fix beneden, blokkeert frontend cutover niet

### Commit 6: **Stream C — PR2 sectors.py v2 cleanup**
- Files: `config/sectors.py`, `scoring/icp_matcher.py`, `scoring/lead_scoring.py`, `enrichment/lead_qualifier.py`, sector-renames in `company_enrichment.py` + `contact_discovery.py`, `tests/test_outreach_rules.py`
- Title: `refactor(sectors): v2 schema cleanup — SBI 2025 + subcategories + website_signals`
- Migrations: geen
- Effort: 1 uur (re-test op huidige 318-test-baseline; mogelijk merge-conflict met G/H-edits op icp_matcher+lead_qualifier)
- Risico: 🟡 Geel — 5 weken oud, drift-risk; vereist test-pass na rebase op huidige main
- Waarom hier: G + H bouwen erop voort; moet vóór G/H

### Commit 7: **Stream G — Sequence engine v1.0 → v3.2 + principles**
- Files: `campaigns/sequence_engine.py`, untracked config/principles_loader.py + sequence_templates.py + opener_principles.md, untracked utils/sector_impact.py + signal_picker.py + lead_naming.py, scripts/render_warmr_payload.py, 5 untracked test-files, opener-fix-hunks van company_enrichment.py
- Title: `feat(sequence): warmr v1.0 → v3.2 templates + pick_brug + signaal_blok resolver + principles loader`
- Migrations: geen
- Effort: 1 uur (verify 318 tests + smoke render_warmr_payload op 1 lead)
- Risico: 🟡 Geel — overlap met H op utils/lead_naming.py + sector_impact.py
- Waarom hier: na C (sectoren-rename), vóór H (CRM bouwt op sequence engine)

### Commit 8: **Stream H — Top-4 CRM-sprint + migrations**
- Files: untracked utils/email_sendability + enrichment_check + lead_thread + lead_activity + lead_import, scoring/feedback_processor.py (modified), integrations/warmr_client.py (BCC), 10 untracked CRM-test-files, **alle migrations 009-017** (na hernumeren 015→018, 016→019)
- Title: `feat(crm): top-4 sprint — test-mode + completeness + thread-view + activity-timeline`
- Migrations: **eerst `017` (en hernumerde `018` + `019`) draaien**
- Effort: 1-2 uur (hunk-staging + migration + 318 tests + handmatig hernumeren)
- Risico: 🟡 Geel — vereist 017-mig + nummer-conflict-resolutie
- Waarom hier: na G (sequence engine in HEAD)

### Commit 9: **Stream F — API auth + endpoint-uitbreidingen**
- Files: `api/main.py` (+2619), `config/database.py`, `utils/startup_validator.py`, `.env.example`, `CLAUDE.md`, `tests/test_auth.py`
- Title: `feat(api): auth-hardening + top-4 endpoints + sequence-launch routes`
- Migrations: geen
- Effort: **2-3 uur** of meer (gigantische diff, mogelijk eerst hunk-analyse-sessie nodig)
- Risico: 🔴 Rood — 2619-regel diff over 4 streams; aparte sessie aanbevolen om te ontvlechten
- Waarom hier: alle dependency-code in HEAD, endpoints koppelen aan B+D+G+H

### Commit 10: **Stream E — Frontend cutover (React/Vite)**
- Files: 14 frontend/* DELETIONS, alle `frontend-next/` files
- Title: `feat(frontend): cutover from vanilla HTML to React/Vite app`
- Migrations: geen
- Effort: 30 min (`git rm` voor oude HTML + `git add frontend-next/`)
- Risico: 🟡 Geel — geen scope-MD; review nodig of frontend-next compleet is
- Waarom laatst: backend-endpoints uit F moeten in HEAD vóór frontend-next eindgebruik

### Commit 11 (optioneel): **Docs sweep**
- Files: BLOCKERS_*, CHANGES*, HEATR_AUDIT.md, MVP_GAPS.md, SKILL.md (verplaatsen naar docs/), VERWIJDER `heatr_CLAUDE.md` (duplicate)
- Title: `docs: consolidate scope/audit MD-files into docs/`
- Effort: 15 min
- Risico: 🟢 Groen

### Werk dat NIET veilig commit-baar is

**`scripts/run_worker.py`** (scraping-worker) — geen scope-MD, geen stream-claim. Twee opties:
- (a) Aparte mini-commit met message "chore: add scraping worker entry script" als infrastructure-file
- (b) Bewust niet committen tot duidelijk is of dit nog gebruikt wordt (`scripts/run_enrichment_worker.py` is de nieuwere variant per Stream D)

**`.env.bak` + `.env.backup-pre-cleanup`** — backup-files, **NIET committen**. Toevoegen aan `.gitignore` of handmatig verwijderen.

**`.claude/scheduled_tasks.lock`** — claude-runtime artifact dat als deleted toont. Geen actie, blijft buiten commits.

---

## 7. Open Vragen voor Sami

### Q1 — Stream E (frontend-cutover) scope
Geen MD-doc gevonden. Per build-log is de cutover impliciet tijdens top-4 sprint (May 5-6) gebruikt. **Was de switch van vanilla HTML naar React/Vite een aparte sessie of mee-lift?** En: is `frontend-next/` definitief de vervanger, of staat oude `frontend/` op come-back? Het deleting-of-frontend/-files-pattern suggereert "definitief weg", maar bevestigen helpt.

### Q2 — Stream F (api/main.py +2619 regels) ontvlechting
Te groot voor 1 commit. **Wil je een aparte sessie waarin we api/main.py hunk-staging doen** (auth-routes / CRM-routes / sequence-routes / process_next-routes), of bewust als 1 "API consolidation snapshot" committen na alle dependency-streams in HEAD staan? De tweede optie is sneller maar geeft een grote diff in git-log.

### Q3 — `scripts/run_worker.py` status
Scraping-worker entry-script, geen scope-MD, niet door mij gewijzigd in deze sessie. **Wordt deze nog gebruikt, of is `scripts/run_enrichment_worker.py` (Stream D) de vervanger?** Voor commit-strategie maakt het verschil of het bij H of als standalone moet.

### Q4 — Stream C (PR2) drift-risico
PR2 is 5 weken oud en Stream G/H heeft op dezelfde files (icp_matcher.py, lead_qualifier.py, company_enrichment.py, contact_discovery.py) doorgewerkt. **Wil je PR2 nu nog committen, of beschouwen we het als "al opgegaan in latere commits" en gooien we de PR2-only hunks weg?** Risk-check: test-baseline staat op 318/318 dus daadwerkelijke regressies zouden door pytest gepakt zijn — als test groen blijft na rebase, is PR2-commit veilig.

### Q5 — MD-files in root (Stream "docs sweep")
10 MD-files (BLOCKERS_*, CHANGES_*, HEATR_AUDIT.md, MVP_GAPS.md, SKILL.md, heatr_CLAUDE.md) staan in root. Sommige zijn historie (Apr 5-21), andere actuele scope-bron (CHANGES_PR3). **Wat doen we ermee?** Drie opties:
- (a) Bewaar in root, commit ze los — historie voor latere auditors
- (b) Verplaats naar `docs/` of `docs/archive/`, behoud `CLAUDE.md` in root
- (c) Niet committen (zijn ook nu untracked)

`heatr_CLAUDE.md` is duplicate van `CLAUDE.md` per HEATR_AUDIT.md — die kan **WEG**.

### Q6 — Stream B vs D foundation-volgorde
Stream B (PR1) heeft `process_next_enrichment` toevoeging in `enrichment_queue.py` die ALS hunk gemixt zit met PR3-werk. **Wil je B en D-rest samen-committen** (eenvoudiger), of strikt apart houden (cleaner historie)? Mijn voorstel hierboven was apart, maar het is een trade-off.

### Q7 — Live re-verify vóór elke commit?
Voor Stream C+D+G+H is er 5 weken drift tussen scope-doc en huidige code. **Wil je vóór elk van deze commits een full pytest run + handmatige smoke-test, of vertrouw je dat 318/318 tests representatief zijn?** Voor go-live-kritieke streams (D, G, H) aanbevolen.

### Q8 — Migration 005 + 017 draaien vóór commit
Beide untracked, beide nog niet in Supabase. Stream B en H zijn afhankelijk. **Wil je deze nu (vóór begin van commit-sequence) draaien, of one-at-a-time vlak vóór elk afhankelijke commit?**

---

## Bijlage — Bestaande memory-context die relevant is

Uit `/Users/nemesis/.claude/projects/-Users-nemesis-Heatr/memory/`:
- **`project_warmr_sequence_v1.md`**: 3-mail cosmetische klinieken, 15 datapoints, gaps in enrichment. Stream G + D context.
- **`feedback_no_sends_without_offer.md`** (LIFTED): pre-2026-04-21 hard block opgeheven; sends mogen onder v1.0 operational rules.
- **`feedback_focus_behandelaren.md`**: sector-focus = cosmetisch + alternatief + tandartsen + mondhygiënisten. NIET makelaars/bouwbedrijven (verklaart PR2's strip).
- **`project_session8_pipeline.md`**: Full pipeline built (17 files), Warmr live met crontab, shared Supabase met heatr_ prefix.

---

*Inventarisatie uitgevoerd door claude-opus-4-7 op 2026-05-12. Read-only, geen code-wijzigingen, geen git-acties. Sami beslist commit-strategie vóór uitvoering.*
