# Werkboom Inventarisatie — Beslissingen

Reactie van Sami op de 8 open vragen uit
[worktree_inventory_2026-05-12.md](worktree_inventory_2026-05-12.md).
Vastgelegd zodat de commit-volgorde uitgevoerd kan worden zonder elke
sub-beslissing opnieuw aan Sami te vragen.

---

## Q1 — Stream E (frontend-cutover) scope

**Beslissing:** Definitief weg. Geen come-back van oude `frontend/`.

**Behandeling:** Stream E als reguliere cutover-commit (Commit 10 in voorgestelde
volgorde). Message benoemt expliciet "cutover from vanilla HTML to React/Vite"
zodat in git-history zichtbaar blijft dat oude HTML opzettelijk is verwijderd.
De 14 `frontend/*.html/css/js` deletions gaan samen met de `frontend-next/`
toevoeging in één commit — niet apart.

---

## Q2 — `api/main.py` (+2619 regels) ontvlechting

**Beslissing:** Aparte sessie. Hunk-staging per route-cluster
(auth / CRM / sequence / process_next), niet één grote
"API consolidation snapshot" commit.

**Reden:** Bij bisect of revert is een 2619-regel mengsel-commit moeilijk
terug te draaien zonder collateral damage. Hunk-staging vooraf kost meer tijd
maar geeft per-feature traceability. Stream F wordt dus opgesplitst in
sub-commits binnen Commit 9.

**Effort-revisie:** Commit 9 wordt waarschijnlijk 3-5 sub-commits, niet één.
Plan totaal: 3-4 uur in een aparte sessie.

---

## Q3 — `scripts/run_worker.py` status

**Antwoord op basis van Sami's terminal-output:**
- `head -20 scripts/run_worker.py` → docstring "Start the Heatr scraping worker"
  voor `scraping_jobs` queue (Playwright + directory scrapes, NIET enrichment)
- `git log --all --oneline -- scripts/run_worker.py` → **leeg** (nooit gecommit)

**Beslissing:** Bundle met Stream D-rest (Commit 5). Reden: het is een
worker-entry-script net als `scripts/run_enrichment_worker.py` dat al bij
Stream D zit. Beide zijn pipeline-infrastructure entry-points. Samen committen
geeft een logische "alle worker-runners in één plek" groepering en voorkomt
commit-noise.

**Niet bij Stream H:** scraping ≠ CRM. Niet apart standalone: zou een commit
met één file zijn (commit-noise zonder narratief).

**Override door Sami mogelijk:** als Sami het scraping-worker-werk los van
enrichment wil houden, dan apart in een "chore: worker entry scripts" commit
vóór of na Commit 5.

---

## Q4 — Stream C (PR2) drift-risico

**Beslissing:** Committen, niet weggooien.

**Reden:** 318/318 tests groen = wijzigingen zijn intern consistent. Tests zijn
de waarheid. Risk-check: vóór commit (Commit 6) een pytest-run als
veiligheidscheck. Als regressies opduiken, dan PAS overwegen om PR2-hunks
selectief weg te gooien.

---

## Q5 — MD-files in root

**Beslissing:** Optie (b) — verplaatsen naar `docs/archive/`.

**Behoud in root:** `CLAUDE.md`, `SKILL.md`.

**Naar `docs/archive/`:** `BLOCKERS_SCOPE.md`, `BLOCKERS_PR3_SCOPE.md`,
`CHANGES.md`, `CHANGES_PR2.md`, `CHANGES_PR3.md`, `HEATR_AUDIT.md`,
`MVP_GAPS.md`.

**WEG (delete):** `heatr_CLAUDE.md` — duplicate per HEATR_AUDIT.md regel 250-251
en 322-323. `git rm` na verplaatsing van CLAUDE.md.

**Reden:** Root-cleanliness helpt project-navigatie. Archive bewaart historie
zonder ruis in de root-listing.

**Behandeling:** Commit 11 (optionele docs-sweep) wordt verplicht in de volgorde.
Message: `docs: archive scope/audit MD-files to docs/archive/ + dedupe CLAUDE.md`.

---

## Q6 — Stream B vs D-rest volgorde

**Beslissing:** Apart houden. Commit B als Commit 4, Commit D-rest als Commit 5.

**Reden:** Cleaner conceptuele scheiding (PR1 blockers vs PR3 datapunten +
foundation). Makkelijker bisecten bij toekomstige bugs. Git-history leest
chronologisch begrijpelijk.

---

## Q7 — Re-verify (pytest + smoke) vóór elke commit

**Beslissing:** Ja voor 5/6/7/8/9. Nee voor 1/2/3/4/10/11.

| Commit | Stream | Re-verify? | Reden |
|---|---|---|---|
| 1 | I (docs) | Nee | Pure docs, zero-risk |
| 2 | D-foundation | Nee | Klein scope (cost_guard + vision_cache), pre-getest in cost-audit chain |
| 3 | A (accumulator) | Nee | Tests al groen in huidige sessie (318/318) |
| 4 | B (PR1) | Nee | Klein scope, CHANGES.md zegt 26 passed |
| 5 | D-rest | **Ja** | Grote scope (~25 files), 5 weken oud, drift-risk |
| 6 | C (PR2) | **Ja** | 5 weken oud, drift met G/H mogelijk |
| 7 | G (sequence) | **Ja** | Multi-sessie werk, opener-fix + signal-picker drift mogelijk |
| 8 | H (CRM) | **Ja** | Vereist mig 017 + hernumeren; test-suite afhankelijk |
| 9 | F (api/main) | **Ja** | Auth-paden + 4-stream mengsel, kritiek |
| 10 | E (frontend) | Nee | Frontend isoleert, geen pytest-impact |
| 11 | Docs sweep | Nee | Pure file moves |

**Standaard re-verify commando:**
```bash
python3 -m pytest --tb=short -q
```
Verwacht: 318/318 pass + 2 pre-existing live-test errors (test_e2e_pipeline,
test_google_maps_live — niet onze fix).

---

## Q8 — Timing van migrations 005 + 017

**Beslissing:** One-at-a-time vlak vóór elke afhankelijke commit.

**Concreet:**
- **Migration 005** (`website_analysis_failed_reason`) → draaien in Supabase
  vlak vóór Commit 4 (Stream B). Niet eerder.
- **Migration 017** (`is_test_lead`) + hernumeren `015→018`, `016→019` →
  draaien in Supabase vlak vóór Commit 8 (Stream H). Niet eerder.

**Reden:** Nooit migrations draaien voor code er klaar voor is. Als migratie
005 vandaag draait en commit B duurt 3 dagen, draait er ondertussen code die
de nieuwe kolom nog niet kent. Risico op data-corruptie is hier minimaal
(kolommen zijn nullable + optioneel), maar netjes is netjes.

**Hernumeren (per Sami's eerdere bevestiging):**
```bash
git mv migrations/015_crm_status_override.sql migrations/018_crm_status_override.sql
git mv migrations/016_import_runs.sql migrations/019_import_runs.sql
```
Niet draaien — beide kolommen/tabellen zijn al gedraaid in Supabase via
APPLY_ME_IN_SUPABASE.sql; hernumeren is puur filename-correctie voor history.

---

## Bijwerkingen op commit-volgorde

Geen substantive wijzigingen aan de 10-commit-volgorde uit het inventory-rapport.
Wel verfijningen:

- **Commit 9 (Stream F api/main.py)** wordt opgesplitst in 3-5 sub-commits in
  een aparte sessie — niet één commit. Voer Commits 1-8 + 10 + 11 eerst uit;
  api/main.py-ontvlechting komt na als losse milestone.
- **Commit 11 (Docs sweep)** is niet meer optioneel — wordt mandatory ergens
  na Commit 1 (zodat archive/ folder bestaat) en vóór Commit 10 (zodat
  frontend cutover een schone root achterlaat).
- **`scripts/run_worker.py`** wordt geïncludeerd in Commit 5 (Stream D-rest).

---

*Beslissingen vastgelegd door Sami op 2026-05-12. Ga verder met Commit 1
(Stream I docs) zodra dit document ge-committeerd is.*
