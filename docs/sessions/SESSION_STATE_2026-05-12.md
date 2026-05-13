# Session State — 2026-05-12

Handover van een 12u+ inventarisatie + commit-sprint sessie. De volgende sessie
kan deze file lezen en weet exact waar het is gebleven zonder context-rebuild.

---

## Wat is bereikt deze sessie

**Begintoestand:** 7 weken aan uncommitted werk in werkboom (43 modified + 122
untracked files). Cost-audit chain was net afgerond (commit `1ef52f5`).
Accumulator-fix was klaar maar nog niet gecommit.

**Eindtoestand:** 7 nieuwe commits bovenop `1ef52f5`. Vrijwel alles wat
inventariseerbaar was, is nu in version-control. Migrations 005 + 017 zijn
gedraaid in Supabase. 318/320 pytest-tests groen (de 2 errors zijn
pre-existing fixture-issues in `test_e2e_pipeline.py` + `test_google_maps_live.py`).

---

## Commits gemaakt (chronologisch)

```
faf8abd feat(crm): Top-4 sprint — test-mode + completeness + thread-view + activity-timeline
b3ad331 feat(sequence): Warmr v1.0 → v3.2 sequence-engine + principles loader + opener-fix
c84d760 refactor(sectors): PR2 v2 schema cleanup — SBI 2025 + subcategories + website_signals
6d61724 feat(blockers): website_analysis_queue + migratie 005 + PR1 test-suite
629f660 feat(cost-controls): PR3 enrichment datapunten + LeadCostAccumulator activatie
d954761 feat(cost-controls): per-lead accumulator + vision-cache foundation
86c0b20 docs(audit): cost-audit rapport + worktree inventory + commit-strategie decisions
```

| # | Hash | Stream | Beschrijving |
|---|---|---|---|
| 1 | `86c0b20` | I (docs) | 3 audit-rapporten incl. worktree-inventarisatie + Sami's decisions per open vraag |
| 2 | `d954761` | D-foundation | `utils/cost_guard.py` + `utils/vision_cache.py` + test_cost_guard.py — fundament dat al gebruikt werd door cost-audit chain maar nooit gecommit was |
| 3 | `629f660` | D-rest + A bundeld | PR3 datapunten (booking_system, latest_review_date, treatment_focus, website_age_years, meta_ads, local_competitors) + accumulator-activatie in 9 fases + orchestrator-binding. **Per-lead cost-cap is vanaf deze commit daadwerkelijk operationeel** (was vóór alleen in api_cost_log post-hoc te zien). |
| 4 | `6d61724` | B (PR1) | `website_analysis_queue.py` + migratie 005 + PR1 test-suite. `review_email_generator.py` zat al in HEAD via `2100254`. `process_next_enrichment` wrapper zat al in commit 3 vanwege hunk-mengsel. |
| 5 | `c84d760` | C (PR2) | sectors.py v2 schema cleanup (SBI 2025 + subcategories + website_signals). Hunk-staging op company_enrichment.py (hunks 1-4: dead-sector strip → C, hunks 5-7: async-fix + opener → G). |
| 6 | `b3ad331` | G | Sequence engine v1.0 → v3.2 (pick_brug, signaal_blok 6-tier resolver, stad_of_sector token) + principles loader + opener-fix May 7. |
| 7 | `faf8abd` | H | Top-4 CRM-sprint (test-mode, completeness check, thread-view, activity-timeline). Migrations 015→018 + 016→019 hernumerd (filename-conflict met tracked 015_claude_cache + 016_api_cost_log_cache_hit). |

---

## Supabase migration-state

**Gedraaid in Supabase:**
- `005_website_analysis_failed_reason.sql` (2026-05-12 deze sessie)
- `006_warmr_sequence_fields.sql` (eerder, via APPLY_ME_IN_SUPABASE.sql)
- `007_enrichment_schema_fixes.sql` (idem)
- `009-014` + `017` + `018` (was 015) + `019` (was 016) — alle via eerdere APPLY_ME-runs + 017 deze sessie
- `015_claude_cache.sql` (al in HEAD via `8ad5f6c`)
- `016_api_cost_log_cache_hit.sql` (al in HEAD via `2100254`)
- `APPLY_ME_008.sql` + `APPLY_ME_IN_SUPABASE.sql` (eerder gedraaid)

**Niet meer relevant om te draaien** — alle untracked migrations zijn al in DB
ge-apply'd.

---

## Werkboom-state na sessie (nog uncommitted)

### Stream F — api/main.py auth-hardening + endpoint-uitbreidingen (NIET GEDAAN)

Per [decisions.md Q2](worktree_inventory_2026-05-12_decisions.md): aparte
sessie aanbevolen, hunk-staging per route-cluster, 3-4 uur effort.

**Files:**
- `api/main.py` — **+2619 regels diff** (auth + CRM-endpoints + sequence-endpoints + process_next-routes)
- `config/database.py` — +10 (auth wrapper)
- `utils/startup_validator.py` — +18 (env-validation)
- `.env.example` — +4 (HEATR_API_KEY, SUPABASE_JWT_SECRET, LEGACY_DEV_TOKEN_ALLOWED, ENABLE_CAMPAIGN_SENDS)
- `CLAUDE.md` — +25 (auth-tabel + env-vars)
- `tests/test_auth.py` (untracked, ~tests voor JWT + X-API-Key)

**Aanbevolen aanpak:** hunk-staging per route-cluster in nieuwe sessie:
1. Auth-routes (X-API-Key + JWT decode + LEGACY_DEV_TOKEN_ALLOWED)
2. CRM-routes (top-4 endpoints van Stream H)
3. Sequence-routes (van Stream G)
4. process_next-routes (van Stream B-rest)

### Stream E — Frontend cutover (NIET GEDAAN)

Per [decisions.md Q1](worktree_inventory_2026-05-12_decisions.md): definitief
weg, één commit.

**Files:**
- 14 deletions: `frontend/*.html/css/js`
- Untracked: `frontend-next/` (~50 files: React + Vite + TS, top-4 sprint
  heeft 'm actief gebruikt)

**Effort:** 30 min (`git rm` voor frontend/* + `git add frontend-next/`).
Heeft Stream F-endpoints in HEAD nodig vóór commit.

### Stream "docs sweep" — Commit 10 (NIET GEDAAN)

Per [decisions.md Q5](worktree_inventory_2026-05-12_decisions.md): optie (b)
verplaatsen naar `docs/archive/`.

**Acties:**
- Verplaats naar `docs/archive/`: `BLOCKERS_SCOPE.md`, `BLOCKERS_PR3_SCOPE.md`,
  `CHANGES.md`, `CHANGES_PR2.md`, `CHANGES_PR3.md`, `HEATR_AUDIT.md`,
  `MVP_GAPS.md`
- Blijf in root: `CLAUDE.md`, `SKILL.md`
- DELETE: `heatr_CLAUDE.md` (duplicate per HEATR_AUDIT.md)
- Mogelijk: commit `docs/sessions/*` apart of via deze sweep

### `scripts/run_worker.py` (NIET GEDAAN)

Per Q3-check deze sessie: het is een scraping-worker entry-script (`scraping_jobs` queue,
Playwright + directory-scrapes). **NIET** bij Stream D-rest (enrichment).
Aanbeveling: standalone mini-commit `chore: add scraping worker entry script`.

### WEG-kandidaten

- `.env.bak` — backup file, niet committen, **deleten** of toevoegen aan `.gitignore`
- `.env.backup-pre-cleanup` — idem
- `.claude/scheduled_tasks.lock` (deleted) — claude-runtime artifact, blijft buiten commits

---

## Belangrijke beslissingen + context voor volgende sessie

### 1. Migration nummer-conflicten opgelost
- `015_crm_status_override.sql` → `018_crm_status_override.sql` (filesystem-mv)
- `016_import_runs.sql` → `019_import_runs.sql` (filesystem-mv)
- Reden: `015_claude_cache.sql` + `016_api_cost_log_cache_hit.sql` waren al in
  HEAD via cost-audit chain (`8ad5f6c` + `2100254`). Renamen van tracked
  migrations zou history corrumperen.

### 2. Hunk-staging gebruikt voor `enrichment/company_enrichment.py`
- Hunks 1-4 (dead-sector strip + defensieve schema-comments) → Commit 5 (C)
- Hunks 5-7 (async-fix + opener v3.1 system-prompt May 7) → Commit 6 (G)
- Reden: atomair gekoppeld (3 sync→async conversies moeten samen, anders
  runtime-mismatch), maar conceptueel ander timeframe dan PR2.
- Tool: `git diff > patch && head -N patch > C_only.patch && git apply --cached`

### 3. Stub-commits vermeden
- Optie 1 (Commit 3 = A alleen, Commit 5 = D-rest) → afgewezen.
- Optie 2 (bundel A + D-rest in Commit 3) → gekozen.
- Reden: een Commit 3 die alleen accumulator-acceptatie toevoegt zonder
  orchestrator-binding zou functioneel een no-op zijn tot Commit 5. Dat is
  een commit-message die lacht naar zichzelf.

### 4. File-toewijzing op basis van inhoud, niet filename
- `enrichment/email_waterfall.py` → Stream D (PR3 schema-gap workaround voor
  migratie 007), niet PR2.
- `scripts/run_worker.py` → standalone (scraping worker, NIET enrichment).
- `scoring/feedback_processor.py` → Stream H (migratie 012 context).
- `tests/test_contact_inference.py` → Stream H met naming-note (filename
  volgt feature-naam, niet module-naam; target is `_infer_contact_from_email`
  in `contact_discovery.py`).
- `tests/test_reply_drafter.py` + `tests/test_lead_naming.py` → Stream H
  ook al zit hun target-module elders (HEAD via `2100254` resp. Commit 6).

### 5. pytest 320 collected sinds eerste re-verify
- pytest gebruikt filesystem-collection, niet git-tracking. Alle test-files
  in `tests/` werden vanaf de eerste re-verify-run opgepakt, ongeacht of ze
  tracked/untracked waren.
- Implicatie: Stream G + H voegen feitelijk geen tests toe aan de
  collection — alleen tracken de filenames in version-control.
- 318 passed / 320 collected / 2 pre-existing fixture-errors (test_e2e_pipeline
  + test_google_maps_live, niet onze fix).

---

## Voor de volgende sessie — concrete actie-lijst

In aanbevolen volgorde:

1. **`scripts/run_worker.py` standalone commit** (5 min). Een `chore: add
   scraping worker entry script` commit. Geen scope-vraag meer.

2. **WEG-kandidaten opruimen** (5 min): `rm .env.bak .env.backup-pre-cleanup`
   en evt. toevoegen aan `.gitignore`. Geen commit; pure werkboom-cleanup.

3. **Stream F — api/main.py hunk-staging (zware sessie, 3-4u)**:
   - Inspecteer api/main.py-structuur (welke routes, welke imports)
   - Bouw hunk-plan per route-cluster
   - Stage en commit per cluster apart
   - `tests/test_auth.py` mee met auth-cluster
   - Voor elke sub-commit: pytest re-verify

4. **Stream E — Frontend cutover** (30 min). Eenmaal Stream F in HEAD: commit
   alle `frontend/` deletions + `frontend-next/` als één
   `feat(frontend): cutover from vanilla HTML to React/Vite`.

5. **Docs sweep** (15 min):
   - `mkdir -p docs/archive/`
   - `mv` 7 root-MDs naar `docs/archive/`
   - `rm heatr_CLAUDE.md`
   - Commit `docs/sessions/*` mee of in aparte commit
   - Title: `docs: archive scope/audit MD-files + dedupe CLAUDE.md`

6. **Push naar remote** (na alle commits):
   ```bash
   git log --oneline origin/main..main | wc -l   # verwacht: 16+ commits
   git push origin main
   ```

---

## Test-baseline voor toekomstige re-verify

```
pytest tests/ --collect-only -q  → 320 tests collected
pytest tests/ -q                  → 318 passed, 2 errors (pre-existing fixtures)
```

Bij volgende re-verify-run: verwacht 318/320 ✓. Afwijking = regressie.

---

## Cross-stream-claim verificaties (voor zekerheid)

Deze claims uit commit-messages zijn waar in HEAD na deze sessie:

- ✓ `LeadCostAccumulator` wordt geïnstantieerd in
  `job_queue/enrichment_queue.run_enrichment_for_lead` (Commit 3)
- ✓ Per-lead cap (€0.05) is actief via `accumulator.blocked` check vóór elke
  pipeline-stap (Commit 3)
- ✓ `list_sectors()` retourneert alleen `['alternatieve_geneeskunde',
  'cosmetische_behandelaars']` (Commit 5)
- ✓ Migraties 005 + 017 zijn gedraaid in Supabase (handmatig deze sessie)
- ✓ `enrichment/company_enrichment.py` heeft `await
  anthropic_client.messages.create(...)` in alle 3 calls (Commit 6)

---

*Handover door claude-opus-4-7 op 2026-05-12. Volgende sessie kan deze
file lezen, dan direct doorgaan met actie-lijst. CLAUDE.md + dit document
+ docs/worktree_inventory_2026-05-12.md + docs/worktree_inventory_2026-05-12_decisions.md
zijn de complete context.*
