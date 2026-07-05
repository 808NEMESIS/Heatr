# Heatr Strategische Audit — 2026-07-05

Volledige audit met strategisch + technisch overzicht.
Baseline HEAD = `e1abdbe`, 858 leads in productie-DB, ICP recent versmald
naar cosmetische klinieken + chiropractoren (`c153454`, 2026-06-16).

Methode: 3 parallelle Explore-agents (frontend / backend+data / tests+kwaliteit)
+ eigen kennis uit vorige audit-artefacten:
`docs/audit_heatr_2026-05-20.md`, `docs/cost_audit_2026-05-11.md`,
`docs/attribution_gap_audit_2026-05-11.md`,
`docs/worktree_inventory_2026-05-12.md`, `docs/sessions/build-log.md`,
plus 30 recente commits.

---

## Executive Summary

Heatr is een **operationeel B2B-outbound platform** met verrassend brede
scope: van scrape → enrich → score → website-analyse → sequence-render →
push-naar-Warmr → reply-classify → CRM-pipeline. De **kern werkt** — er
zitten 858 verrijkte leads in DB, cost-controls bewaken €0.05/lead met
hard gates, en de nieuwe React/Vite-frontend heeft een consistent Shell +
design-token-systeem.

Maar het platform staat op een **kantelpunt**: het is te groot voor
"solo-dev prototype" en te fragiel voor "productie SaaS". Kritieke gaten:

1. **`api/main.py` is 4181 regels met 132 functies zonder eigen test-file** —
   het hart van de app is ongecontroleerd.
2. **Frontend heeft geen ErrorBoundary, geen error-toasts, geen route-guards** —
   elke API-hiccup = witte pagina zonder feedback.
3. **Auth is werkelijk drie paden** (X-API-Key + Supabase JWT + legacy
   dev-token), maar frontend-next gebruikt **hardcoded `dev-token`** en
   legacy-flag stond bij restart-issue niet aan → 858 leads waren
   onzichtbaar in UI.
4. **Anthropic-calls hebben geen 429-retry**: één rate-limit-piek
   verspilt kostbare enrichment-work en geeft silent failures.
5. **`filter_launchable_leads()` en `/leads/send-to-warmr` filteren niet
   op status='unsubscribed'/'forgotten'/'disqualified'** — GDPR-risico
   blijft open sinds mei-audit.

Sterke kanten: cost-guard-architectuur (per-lead + daily + monthly caps),
sequence v3.2 met Sami-stem-brief, workspace_id-hygiëne is inmiddels
grotendeels op orde (reply_classifier gefixt in `e1abdbe`), Warmr-integratie
is defense-in-depth (dubbele-gates op MIN_SCORE), en de commit-hygiëne is
uitzonderlijk hoog (elke fix eigen commit, eerlijke fix(_)-prefixes bij
gemist werk).

De grootste **strategische kans**: **focus**. De scope is te breed. Twee
smalle personas (cosmetische klinieken + chiropractoren) verdienen twee
smalle flows. De hele "alternatieve_geneeskunde"-tak inclusief 8
subcategorieën is inactief maar leeft nog in code, tests en templates.
Deprecatie afmaken = 10-15% code-reductie zonder feature-verlies.

---

## 1. Product & UX

### 1.1 Wat Heatr feitelijk doet

Van scrape tot pipeline in 6 fases:

1. **Discovery** — Google Maps scraping per sector+stad
   ([scrapers/google_maps_scraper.py:82](scrapers/google_maps_scraper.py#L82))
2. **Qualify** — sector-keywords + disqualifiers + rating-thresholds
   ([enrichment/lead_qualifier.py](enrichment/lead_qualifier.py))
3. **Enrich** — email-waterfall (4 stappen), company-enrichment via Claude
   Haiku, website-analyse in 5 lagen, archetype-classifier
4. **Score** — 4-dimensionale scoring (fit/data/reach/personalisation),
   pushes naar `MIN_SCORE_FOR_WARMR=65` gate
5. **Send** — Warmr-integration via `/leads/send-to-warmr` OF
   `/campaigns/launch` met sequence v3.2
6. **Reply-loop** — Warmr-webhook → `classify_reply` → status-update +
   snooze/pause/interested → CRM-pipeline

### 1.2 Sterke flows

- **Sequence-engine v3.2** ([campaigns/sequence_engine.py](campaigns/sequence_engine.py)):
  `pick_brug` routing tussen 3 bruggen + `signaal_blok` 6-tier resolver +
  `stad_of_sector` fallback. De opener-fix van 7 mei (v3.2-prompt) is
  echt goed werk — voorbeelden in [docs/sessions/build-log.md](docs/sessions/build-log.md)
  bevestigen sterke Sami-stem-output.
- **Cost-controls** ([utils/cost_guard.py](utils/cost_guard.py) +
  [utils/vision_cache.py](utils/vision_cache.py)): drie-laags budget-gates
  + Vision-cache + `LeadCostAccumulator` per lead. Fail-open bij DB-errors
  = correct default (pipeline stopt nooit door budget-check-fail).
- **Warmr-integration defense-in-depth**:
  [scoring/lead_scoring.py:192-195](scoring/lead_scoring.py#L192) checkt
  MIN_SCORE + MIN_ICP; [api/main.py:587](api/main.py#L587) re-checked
  MIN_SCORE bij push. Dubbele gate = correct pattern.
- **CRM Top-4 sprint** (`faf8abd`): test-mode-flag, pre-launch completeness,
  email-thread-view, activity-timeline. Vier features met echte
  operator-waarde.

### 1.3 Fricties + gaten

**Auth-onduidelijkheid.** Vandaag zag Sami zelf: frontend-next stuurt
`Bearer dev-token`, maar `LEGACY_DEV_TOKEN_ALLOWED` was niet in `.env`
→ **858 leads onzichtbaar in de UI**. Dit is precies het pattern uit
[docs/audit_heatr_2026-05-20.md sectie 2](docs/audit_heatr_2026-05-20.md):
"geen expliciete-gate-review vóór send". Toepassing hier: geen
expliciete-gate-review vóór **inloggen**.

- Impact: HOOG (gebruiker ziet niks)
- Urgentie: HOOG (blocker voor UI-gebruik)
- Moeite: LAAG (5 min: `.env` regel + API-restart)
- Aanbeveling: `LEGACY_DEV_TOKEN_ALLOWED=true` default aan tijdens
  cutover, plus toevoegen aan `.env.example` met duidelijke waarschuwing
  ("uitschakelen zodra frontend-next Supabase JWT ondersteunt")

**Geen error-feedback in UI.** Frontend-next patroon:
```tsx
queryFn: () => api.get<CrmStats>('/crm/stats').catch((): CrmStats => ({})),
```
Elke API-fout wordt stilletjes opgeslokt → gebruiker ziet lege lijst
zonder reden. Geen ErrorBoundary, geen toast, geen "kon niet laden".

- Impact: HOOG (elke prod-hiccup = verwarrende UX)
- Urgentie: MEDIUM (blocker voor externe operator-gebruik)
- Moeite: MEDIUM (1-2 dagen: ErrorBoundary + toast-lib + query-fout-hook)
- Aanbeveling: sonner + globale `onError` in QueryClient +
  ErrorBoundary rond `<Shell>`

**Losstaande operator-touchpoints.** Geen unified inbox-view voor
"actie-vereist-vandaag": snoozed-tot-vandaag + hoge-priority-replies +
completeness-blockers + campaigns paused-by-bounce-rate. Nu verspreid over
Inbox.tsx, CRM.tsx, CRMActivity.tsx.

- Impact: MEDIUM (fricties in dagelijkse workflow)
- Urgentie: LOW
- Moeite: MEDIUM (1 week: nieuwe Dashboard.tsx "Today"-panel)
- Aanbeveling: samenvoegen tot één "Wat moet ik vandaag doen?"-widget

**Preview-only smoke-test-loop is versplinterd.** `scripts/render_warmr_payload.py`
werkt, `POST /campaigns/preview` werkt, maar geen UI-pad om beide te zien
naast elkaar (payload + gerenderd mail-body). Bij smoke-tests moet Sami
tussen terminal + browser wisselen.

- Impact: MEDIUM (langzamer debuggen bij template-issues)
- Urgentie: LOW
- Moeite: LOW (halve dag: `/campaigns/preview` → "toon payload + body"-tab)

### 1.4 Kansen — UX

1. **Onboarding-checklist widget**: welke env-vars ontbreken (via
   `/health/startup`), welke migrations niet gedraaid, welke inboxes
   niet 'ready'. Momenteel is dit terminal-only debugging.
2. **"Waarom deze lead"-panel** op LeadDetail.tsx — icp_match-breakdown
   + disqualifier-hits + review-analysis samengevat. Volgens
   [enrichment/contact_discovery.py:_generate_why_chosen](enrichment/contact_discovery.py)
   bestaat de string al, maar wordt niet gestructureerd getoond.
3. **Bulk-status-override met undo**. Momenteel `POST /leads/bulk-status`
   zonder soft-delete. Één verkeerde bulk = irreversible.

---

## 2. Codekwaliteit

### 2.1 Architectuur + folderstructuur

Grofweg goed opgezet: `scrapers/` + `enrichment/` + `scoring/` +
`website_intelligence/` + `campaigns/` + `integrations/` + `job_queue/` +
`api/` + `utils/` + `config/` + `frontend-next/`. Verantwoordelijkheden
zijn correct verdeeld.

**Maar één kritieke uitzondering**: `api/main.py` = **4181 regels, 132
functies** ([api/main.py](api/main.py)). Dit is een monoliet die alle
routes + helpers + gates + DB-queries + business-logic bevat.

- Impact: HOOG (elke wijziging = merge-conflict-risico, tests moeilijk)
- Urgentie: MEDIUM (werkt nog, maar remt tempo)
- Moeite: HOOG (2-3 weken, moet gefaseerd)
- Aanbeveling: routes-per-resource extraheren naar `api/routes/leads.py`,
  `api/routes/campaigns.py`, `api/routes/webhooks.py`, `api/routes/gdpr.py`.
  Helpers naar `api/services/`. Auth in `api/deps.py`.

### 2.2 Duplicatie + technical debt

Uit tests+kwaliteit-audit + eigen scan:

- **DB-chain-pattern gedupliceerd 285x** in `api/main.py`:
  `db.table("leads").select("...").eq("id", lead_id).eq("workspace_id", ws).execute()`.
  Geen abstractie zoals `LeadRepo.get(id, ws)` of vergelijkbaar.
- **Try/except-blocks 62 in api/main.py alleen**: consistent pattern,
  maar mist een decorator/context-manager voor "wrap-in-try-and-log".
- **62 files >200 regels, 5 files >1000 regels** (email_waterfall.py,
  company_enrichment.py, directory_scraper.py, enrichment_queue.py,
  main.py). Refactor-noodzaak groeit exponentieel per regel.
- **Config-drift**: `HEATR_BASE_URL`, `HEATR_SCHEDULING_URL`,
  `RECONTACT_COOLDOWN_DAYS`, `SENDER_NAME`, `HEATR_TABLE_PREFIX`,
  `HEATR_EUR_USD_RATE` — allen gebruikt in code, geen in `.env.example`.

### 2.3 Naming-consistentie

Uit tests+kwaliteit-audit — echte gaten:

- `lead_id` (749 refs) vs bare `id` (263 refs) in lead-context. Bij
  argument-passing kan `id` shadowen op built-in.
- `sector` (364) vs `sector_key` (65) — onduidelijk of het label of
  primary-key is
- `_gate_leads_for_template` (module-level in main.py) vs
  `enrichment/enrichment_gate.py` — semantische overlap

Aanbeveling: **glossary + rename in eigen commit**. `sector` → `sector_key`
overal waar het een SECTORS-dict-key betreft; `sector_label` waar het UI
is. `lead_id` overal waar het een lead-context is.

### 2.4 Type-hints

- 152 `Any` refs terwijl **TypedDict al voor sectoren wordt gebruikt**
  ([config/sectors.py:24](config/sectors.py#L24)). Precedent bestaat,
  wordt niet gevolgd.
- **1 dataclass** in hele codebase ([enrichment/data_verification.py](enrichment/data_verification.py))
- **0 Pydantic-modellen voor DB-rijen** — alleen voor request-bodies

Aanbeveling: `config/types.py` met `LeadRow`, `CampaignRow`, `InboxRow`,
`WebsiteIntelligenceRow` TypedDicts. Progressive migration — begin met
`_lead_row_to_dict` return-type.

---

## 3. Frontend

### 3.1 Wat er staat (positief)

- **13 pagina's** (Dashboard, Leads, LeadDetail, LeadsImport, Zoeken,
  Campagnes, CampagneLaunch, CRM, CRMActivity, Inbox, WebsiteKansen,
  Analytics, Placeholder) — dekt de operator-workflow.
- **React Query dominant** — geen ad-hoc `useState + useEffect` voor
  data-fetching. `staleTime: 10s`, `retry: 1`, `refetchOnWindowFocus: false`.
  Leads-lijst refetched elke 8s (queue-freshness).
- **Design-tokens** in `src/index.css`: Fraunces + Plus Jakarta Sans +
  JetBrains Mono, warm palette (stone/ivory/blush), semantic
  success/warning/danger/info + skeleton-animatie.
- **Consistent Layout** via `Shell.tsx`: Sidebar + sticky topbar met
  `WorkerStatus` + `CostBadge`. Route-nesting onder Shell = layout-inheritance.
- **UI-primitives** (button/card/input/badge/tabs) — foundation voor
  design-system.

### 3.2 Kritieke gaten (uit frontend-agent-scan)

**Geen ErrorBoundary.** React-rendering-crashes → witte pagina zonder
recovery. Bij één stack-trace in een subcomponent gaat de hele UI plat.

- Impact: HOOG
- Urgentie: MEDIUM
- Moeite: LAAG (1 uur: root-level ErrorBoundary met fallback + reset-knop)

**Geen global error-toast.** Elke `api.get().catch((): T => ({}))` slikt
errors zonder feedback. `/api/leads` → 401 → lege lijst, geen indicatie
dat het een auth-issue is.

- Impact: HOOG
- Urgentie: MEDIUM
- Moeite: MEDIUM (halve dag: sonner-lib + `QueryClient` `onError`-config
  + auth-401-redirect)

**Geen route-guards.** Elk pad (incl. `/crm/activity` met bulk-tools) is
zonder auth-check bereikbaar. In dev-mode oké, in productie niet.

- Impact: HOOG (na Supabase-JWT-migratie)
- Urgentie: LAAG (voor nu single-tenant)
- Moeite: MEDIUM (1 dag: `<ProtectedRoute>` + Supabase-session-hook +
  redirect-to-login)

**Dev-token hardcoded** in [frontend-next/src/lib/api.ts:26-30](frontend-next/src/lib/api.ts#L26).
`sessionStorage.getItem('heatr_token') || 'dev-token'` — geen fallback
naar Supabase-session-token als sessionStorage leeg is.

- Impact: HOOG (blocker voor productie)
- Urgentie: HOOG (zodra frontend-next ergens anders wordt gebruikt dan
  Sami's laptop)
- Moeite: MEDIUM (1-2 dagen: Supabase-Auth-provider + `useAuth`-hook +
  token-refresh)

**Geen loading-skeleton-components.** De CSS-`.skeleton`-class bestaat
maar wordt niet consistent gebruikt. Pagina-loads = blanke content,
niet placeholder.

- Impact: MEDIUM
- Urgentie: LOW
- Moeite: LAAG (4 uur: `<TableSkeleton>`, `<CardSkeleton>` primitives)

### 3.3 Performance-risico's

- **Leads.tsx** refetched **elke 8 seconden**. Voor 858 leads = 858
  rows-fetch/8s. Bij 5000 leads = performance-issue.
- **Geen virtualisation** — tanstack/react-table zit in dependencies maar
  geen expliciete `react-virtual` of vergelijkbaar
- **Geen code-splitting** — geen `React.lazy` in App.tsx-routes → alle
  13 pagina's in initial bundle

Aanbeveling voor >2000 leads: pagination server-side + react-virtual
voor lijst-rendering.

### 3.4 Design-system kansen

Foundation is er (tokens + primitives). Volgende laag:
- **Table-component** met sort/filter/pagination-props (uit
  tanstack/react-table)
- **Toast-component** (sonner of custom)
- **Modal/Sheet** (radix-ui of headlessui) — nu ontbrekend maar nodig
  voor bulk-actions en confirm-dialogs
- **Empty-state**-component ("Geen leads gevonden — probeer filter X")
- **StatusPill** — herbruikbaar voor lead-status, campaign-status,
  inbox-status

Aanbeveling: één sprint (1 week) om Table + Modal + Toast + EmptyState
+ StatusPill te maken. Daarna alle pages refactoren naar deze primitives.

---

## 4. Backend / data / integraties

### 4.1 Data-model

- **`heatr_leads`** — hoofdtable, 40+ velden. Recente uitbreidingen via
  migraties: 006 (warmr_sequence_fields), 014 (archetype), 017
  (is_test_lead), 019 (import_runs). Gaat richting "God table".
- **`heatr_website_intelligence`** — per-lead 1-op-1 join. Bevat 5-laags
  score-breakdown + competitor_data + technical/conversion/sector details.
- **`heatr_campaigns` + `heatr_lead_campaign_history` + `heatr_email_events`
  + `heatr_reply_inbox`** — send-tracking. `reply_inbox.body_html`-kolom
  toegevoegd in migratie 011.
- **`heatr_api_cost_log`** — cost-attribution per-call (workspace + lead +
  context + model + tokens).
- **`heatr_claude_cache`** (migratie 015) + **`heatr_vision_cache`** —
  per-context prompt-caching (dedupe).
- **9 CRM-tabellen** (crm_tasks, crm_deals, feedback_runs, campaigns_audit,
  crm_status_override etc.)

### 4.2 API-inventarisatie (94 endpoints)

Uit backend-agent: 94 endpoints verdeeld over 12 resource-domains. Geen
dead routes. Grofweg CRUD + operationeel (process-next / dispatch /
webhooks / analytics).

### 4.3 Security + privacy — OPEN P0 uit vorige audit

Deze 2 issues zijn nog **niet gecommit** (Fix 2 in werkboom):

- **`/leads/{id}/send-review-email`** ontbreekt GDPR/status-gate. Ik
  heb de fix klaar in werkboom staan (+8 regels in `api/main.py`),
  wacht op groen-licht voor commit.
- **`filter_launchable_leads()`** filtert niet op `gdpr_safe` of `status`.
  Zie [utils/enrichment_check.py:90-117](utils/enrichment_check.py#L90).
  Niet gestart.

Nieuwe bevindingen uit backend-agent:

- **Anthropic no-retry** ([enrichment/batched_enrichment.py:232](enrichment/batched_enrichment.py#L232)):
  bare `except Exception → fallback sync client`. Bij 429 rate-limit
  wordt de lead-enrichment silent gefaald + cost al gemaakt.
  - Impact: MEDIUM (kost + kwaliteit)
  - Urgentie: HOOG (bij batch-runs > 20 leads)
  - Moeite: MEDIUM (45 min: exponential backoff met retry-after-header)

- **Sync Anthropic in async endpoints (3×)**:
  [api/main.py:3567](api/main.py#L3567) `/replies/classify`,
  [api/main.py:3999](api/main.py#L3999) `/replies/process-unclassified`,
  [enrichment/batched_enrichment.py:224](enrichment/batched_enrichment.py#L224).
  Blokkeert event-loop tijdens `messages.create`.
  - Impact: MEDIUM (concurrent-request-degradation)
  - Urgentie: MEDIUM
  - Moeite: LAAG (20 min: switch naar `AsyncAnthropic` + singleton)

### 4.4 Foutafhandeling

25+ `except Exception:` in api/main.py — de meeste met logger.warning,
maar **10 bare `pass`** ([regels 1308, 1723, 1741, 1865, 1884, 2149,
2711, 3274, 3929, 3956](api/main.py)). Bij minder happy paden verdwijnen
signals stilletjes.

Aanbeveling: audit deze 10 sites — sommige zijn correct
(Warmr-unreachable-fallback), andere zijn silent bugs.

### 4.5 Schaalbaarheid

- **Supabase** = managed Postgres. Voor 5k-50k leads werkt het. Boven
  100k begint indexering krities: momenteel geen expliciete `CREATE INDEX`
  in migraties beyond default-PK.
- **Rate-limits**: Google Search 10/uur (gehandhaafd), Anthropic 0 (open
  P0), Warmr client-side niet gehandhaafd (server-side wel).
- **Concurrency**: `MAX_CONCURRENT_ENRICHMENTS=5` env-var (per
  `.env.example`), maar workflow serialiseert via `enrichment_queue`.
  Bottleneck is Claude-call-latency (~2-4s per lead), niet DB.

### 4.6 Recente wins (uit git log)

- `e1abdbe` — workspace_id-fix in reply_classifier (P0 uit mei-audit) ✓
- `ca776da` — is_test_lead bypass ook op personalisation-gate ✓
- `03a752f` — Warmr push payload campaign_id top-level ✓
- `458cb8e` — env-vars die "slipped" in `.env.example` ✓
- `c153454` — ICP-scope smaller (2 sectoren) ✓

Ritme is goed. Fix-prefix-hygiëne is uitzonderlijk.

---

## 5. Businesskansen

### 5.1 Logische volgende features

1. **Bulk-actie-safety-net**. Momenteel: `POST /leads/bulk-status`,
   `POST /admin/re-enqueue-stale-leads`, `POST /leads/disqualify` zonder
   undo. Één misclick = verloren state.
   - Feature: soft-delete-pattern + "vorige actie ongedaan maken"-knop
   - Impact op vertrouwen: HOOG
2. **A/B-split per subject-line + opener**. Sequence-engine ondersteunt
   variants (ab_test_engine in Warmr), maar geen UI-tab om resultaten
   naast elkaar te zien. Twee subject-lines op 20 leads elk → welke
   opent beter?
3. **Live-dashboard voor cost + queue-health**. `CostBadge.tsx` toont
   nu enrichment-cost. Uitbreiden naar "vandaag: 12 leads verrijkt,
   €0.24 gebruikt, 3 in queue-blocked".
4. **Reply-drafter met tone-selector**. `campaigns/reply_drafter.py`
   genereert 1 draft. Selector "korter/formeler/informeler" geeft 3
   varianten om te kiezen.

### 5.2 Quick wins met impact

Zie [Roadmap](#6-prioriteitenroadmap) — de top-5 in "quick wins" zijn:

1. **Fix 2 commit** (send-review-email GDPR-gate) — al klaar
2. **Fix 3 implement** (filter_launchable_leads GDPR-check)
3. **`LEGACY_DEV_TOKEN_ALLOWED=true` in `.env`** — al gedaan, doc-update
4. **`.env.example` completeren** — 6 env-vars
5. **`directory_scraper.py:463`**: `cosmetische_klinieken` → `cosmetische_behandelaars`

### 5.3 Conversie/retentie/betrouwbaarheid

Betrouwbaarheid nu grootste flessehals:
- **Test-coverage 26 files / 362 tests, maar main.py + scrapers +
  job_queue ongetest**. Elke deploy = roulette.
- **Geen ErrorBoundary** = één render-crash op 3-uur-lang-scrapen
  = data-loss

Conversie (leads → mails-verstuurd):
- Blocker is **auth-friction** (leads onzichtbaar tot legacy-flag),
  niet product-kwaliteit
- Sequence v3.2 is goed genoeg voor eerste live-cohort

### 5.4 Focus-scherpte

De grootste strategische kans: **finish the deprecation-pass**.
`alternatieve_geneeskunde` blijft in `SECTORS` dict, in
`sequence_templates.py` (`v1_alternatieve_zorg`), in
`utils/sector_impact.py`, in `enrichment/company_enrichment.py:367-370`
(nu ook `makelaars` + `bouwbedrijven` daar).

10-15% van de code verwijzt naar oude sectoren. Twee dagen werk om alles
te schonen → helderder codebase + minder test-noise + minder
"waarom-staat-dat-daar"-momenten.

---

## 6. Prioriteitenroadmap

### 6.1 Quick wins (< 1 dag)

| # | Actie | Bestand:regel | Impact | Moeite |
|---|-------|---------------|--------|--------|
| Q1 | Commit Fix 2 (send-review-email GDPR-gate) — al in werkboom | api/main.py:652 | GDPR-risico weg | 5 min |
| Q2 | Bonus 1.1: sector-key fix ClinicFinder | scrapers/directory_scraper.py:463 | Ingest-data-corruptie weg | 2 min |
| Q3 | `.env.example` completeren (6 vars) | .env.example | Config-drift weg | 5 min |
| Q4 | Fix 3: `filter_launchable_leads` gdpr_safe + status | utils/enrichment_check.py:90 | GDPR-risico | 15 min |
| Q5 | Legacy dev-token doc-update | CLAUDE.md | Auth-confusion weg | 10 min |
| Q6 | `enrichment/company_enrichment.py:367-370` — verwijder `makelaars` + `bouwbedrijven` uit tone_guidance | 5 min | Deprecation-hygiëne | 5 min |
| Q7 | Anthropic-switch naar `AsyncAnthropic` in 2 endpoints | api/main.py:3567 + :3999 | Event-loop unblock | 20 min |
| Q8 | Commit strategic audit rapport | docs/audit_heatr_strategic_2026-07-05.md | Documentatie | 2 min |

**Totaal ~1 werkdag** voor alle 8. Ratio impact/moeite excellent.

### 6.2 Korte termijn (1–2 weken)

| # | Actie | Impact | Moeite |
|---|-------|--------|--------|
| K1 | Anthropic 429-retry met exponential backoff | Kost + kwaliteit | 45 min |
| K2 | Frontend: ErrorBoundary + sonner-toast + QueryClient onError | Prod-UX HIGH | 1-2 dagen |
| K3 | Frontend: loading-skeleton-componenten voor 13 pages | UX polish | 1 dag |
| K4 | `.env.example` audit + `/health/startup` verrijken met env-drift-check | Onboarding | 1 dag |
| K5 | Deprecation-pass: alternatieve_geneeskunde uit template + sector_impact + tone_guidance | Codebase 10% kleiner | 2 dagen |
| K6 | `conftest.py` met centrale `_mock_db()` + `test_lead_factory` | Test-hygiëne | 1 dag |
| K7 | `api/main.py` route-extraction fase 1: `/gdpr/*` + `/analytics/*` naar `api/routes/` | Monoliet kleiner | 2 dagen |
| K8 | Config `.env.example` scherper + `HEATR_BASE_URL` verplicht bij startup | Robustness | 4 uur |

### 6.3 Middellange termijn (1–2 maanden)

| # | Actie | Impact | Moeite |
|---|-------|--------|--------|
| M1 | `api/main.py` volledig refactoren naar `api/routes/` per resource (12 files) | Onderhoudbaarheid | 2 weken |
| M2 | Supabase-Auth-integratie in frontend-next (Supabase Auth-provider + `<ProtectedRoute>`) | Productie-readiness | 1 week |
| M3 | Design-system uitbreiden: Table + Modal + Toast + EmptyState + StatusPill | Snellere feature-dev | 1 week |
| M4 | `tests/test_api_endpoints.py` met FastAPI TestClient — 20-30 endpoints dekken | Betrouwbaarheid | 1-2 weken |
| M5 | `tests/test_scrapers.py` met mocked Playwright | Scrapers-guardrail | 1 week |
| M6 | TypedDict-migratie: `LeadRow`, `CampaignRow`, `InboxRow` in `config/types.py` | Type-safety | 1 week |
| M7 | Bulk-actie safety-net: soft-delete + undo-buffer | Vertrouwen | 1 week |
| M8 | A/B-split UI voor sequence-variants | Conversie | 1 week |

### 6.4 Grote strategische kansen (>2 maanden)

| # | Strategisch | Waarom |
|---|-------------|--------|
| S1 | **Multi-tenant productie**: Supabase JWT + workspace_id per klant + row-level-security policies actief | Van "Aerys interne tool" naar SaaS |
| S2 | **Publieke API voor Heatr** (naast operator-API) — customers push hun eigen lead-lijsten | Nieuwe verdienmodellen |
| S3 | **Real-time cost-guard-dashboard** met monthly-budget-approval-flow | Vertrouwen bij enterprise-klanten |
| S4 | **Reply-engagement-loop v2**: leads die interested-repliceren → auto-book-meeting via HEATR_SCHEDULING_URL | Conversie automatiseren |
| S5 | **Sector-uitbreiding**: van 2 naar 5-8 ICP's op basis van feedback-data (feedback_processor.py) | Groei |
| S6 | **Discovery-scheduler autonoom**: launchd/cron die per stad+sector-combo weekly nieuwe leads binnentrekt | Passieve leadgen |
| S7 | **Warmr integratie-testsuite** (mock-server voor `/api/v1/leads/bulk` + webhook) | E2E-betrouwbaarheid |

---

## Top 10 Actiepunten (nu meteen)

1. **[QW]** Commit Fix 2 (send-review-email GDPR-gate) — werkboom heeft
   'm al ([api/main.py:652](api/main.py#L652)). **5 min.**
2. **[QW]** Sector-key fix in
   [scrapers/directory_scraper.py:463](scrapers/directory_scraper.py#L463):
   `cosmetische_klinieken` → `cosmetische_behandelaars`. **2 min.**
3. **[QW]** Fix 3: `filter_launchable_leads` gdpr_safe + status-check in
   [utils/enrichment_check.py:90](utils/enrichment_check.py#L90). **15 min.**
4. **[QW]** `.env.example` completeren met `HEATR_BASE_URL`,
   `HEATR_SCHEDULING_URL`, `SENDER_NAME`, `RECONTACT_COOLDOWN_DAYS`,
   `HEATR_TABLE_PREFIX`, `HEATR_EUR_USD_RATE`, plus
   `LEGACY_DEV_TOKEN_ALLOWED=true` (met warning). **10 min.**
5. **[QW]** Verwijder `makelaars` + `bouwbedrijven` uit
   [enrichment/company_enrichment.py:367-370](enrichment/company_enrichment.py#L367)
   tone_guidance dict. **5 min.**
6. **[K]** Anthropic 429-retry: exponential backoff in
   [enrichment/batched_enrichment.py:232](enrichment/batched_enrichment.py#L232).
   **45 min.**
7. **[K]** ErrorBoundary + sonner-toast + `QueryClient.onError` in
   frontend-next → alle 13 pages profiteren automatisch. **1-2 dagen.**
8. **[K]** `conftest.py` centralization + reduceer test-boilerplate. **1
   dag.**
9. **[K]** Deprecation-pass: `alternatieve_geneeskunde` weg uit
   templates, sector_impact, tone_guidance (blijft in SECTORS dict). **2
   dagen.**
10. **[M]** `api/main.py` fase-1 route-extraction (`/gdpr/*` +
    `/analytics/*` → `api/routes/`). **2 dagen.**

Effort-totaal top-10: **~7 werkdagen**, waarvan de eerste 5 in **~40
minuten** zitten.

---

## Sluitwoord — eerlijke bevinding

Heatr staat er verrassend goed voor voor een tool die door 1 solo-dev
in 3 maanden is gebouwd tot een 35.000-regel codebase met 858 leads,
858 rows website-intelligence, 15 migraties, 94 endpoints, 26 test-files.
De **onderliggende architectuur is verdedigbaar**, de sequence-engine
werkt echt goed, en de cost-controls zijn defense-in-depth.

Wat het remt: **`api/main.py` is te groot, frontend mist error-recovery,
en er zit een halfaf deprecation-pad in de code voor 6 oude sectoren**.
Als deze drie in 2 weken worden geadresseerd is Heatr klaar voor
externe-operator-gebruik (niet SaaS, wel "één klant met eigen team").

Als er een **strategische keuze** in de komende maand gemaakt moet
worden: **finish the focus**. Twee smalle personas (cosmetisch +
chiropractoren) met scherpe UX + defense-in-depth op GDPR/auth
> vijf halve personas met scope-creep.

---

*Audit uitgevoerd door claude-opus-4-7 met 3 parallelle
Explore-agents (Haiku) voor scan-fase. Geen code-wijzigingen tijdens
audit — rapport is read-only. Werkboom-state bij audit: `M api/main.py`
(Fix 2 gate ongecommit), `?? docs/audit_heatr_2026-05-20.md` (vorige
audit-doc ongecommit), plus nu `?? docs/audit_heatr_strategic_2026-07-05.md`
(dit rapport).*
