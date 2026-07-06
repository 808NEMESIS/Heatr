# Sprint 1 — Reliability Verification Report

**Datum:** 2026-07-06 · **Baseline:** HEAD `486ebe1` bij aanvang audit
**Rol:** onafhankelijke review van Sprint 0-garanties. Elk verdict met bewijs
(grep, call graph, live test, screenshot). Fixes uit deze audit: `b97cfd4` +
`b0410cb` (zie sectie 1/6).

Verdicten: **BEWEZEN** / **GEDEELTELIJK** / **NIET BEWEZEN** — status ná de
twee fixes, met pre-fix-staat expliciet vermeld waar relevant.

---

## 1. Compliance-verificatie — GEDEELTELIJK → BEWEZEN (na fixes)

**Claim (Sprint 0):** elke outbound-flow loopt door één centrale
compliance-gate (`utils/enrichment_check.compliance_check`).

**Bewijs — call-sites van `compliance_check` (grep, sectie-bewijs 1a):**

| Call-site | Pad |
|---|---|
| `utils/enrichment_check.py:97` | `check_lead_completeness` → `filter_launchable_leads` → launch (`api/main.py:1116`) + preview (`:989`) |
| `utils/launch_readiness.py:69` | readiness-endpoint + UI-panel |
| `api/main.py:590` | `/leads/send-to-warmr` eligibility-filter |
| `api/main.py:661` | `/leads/{id}/send-review-email` 403-gate |
| `utils/sending_guard.py` (**ná fix b0410cb**) | dispatch-pad mail 2/3 |

**Gevonden gaten (pre-fix):**

1. **`POST /leads/disqualify` zette `status` niet** — alleen
   `crm_stage="verloren"` (bewijs 1f: update-dict zonder status-key). De
   compliance-gate leest `status` → een "gediskwalificeerde" lead bleef
   volledig launchbaar op alle vier de Sprint 0-paden. Sequences werden ook
   niet gestopt. **Gefixt in `b97cfd4`.**
2. **SendingGuard velde een eigen oordeel** (bewijs 1d:
   `sending_guard.py:62-76` checkte gdpr/unsubscribed/forgotten zelf) en
   **miste `disqualified`**: een ná launch gediskwalificeerde lead kreeg
   mail 2/3 door. **Gefixt in `b0410cb`** — SendingGuard roept nu
   `compliance_check` aan.

**Post-fix verdict:** alle 5 outbound-paden lopen aantoonbaar door dezelfde
beslissing. Regressietest `test_disqualified_blocks_on_dispatch` bewaakt het
gevonden gat.

---

## 2. Single Source of Truth — launchability — GEDEELTELIJK

**Matrix (bewijs 1b/1c — volledige grep-output in audit-log):**

| Locatie | gdpr_safe | status-oordeel | Rol |
|---|---|---|---|
| `utils/enrichment_check.compliance_check` | **bepaalt** | **bepaalt** (BLOCKED_STATUSES) | ✅ DE bron |
| `utils/launch_readiness.py` | consumeert (via gate) | consumeert (via gate) | ✅ composer |
| `api/main.py:590, :661` | consumeert (via gate) | consumeert (via gate) | ✅ |
| `utils/sending_guard.py` | ~~bepaalde zelf~~ → consumeert (na `b0410cb`) | idem | ✅ na fix |
| `scoring/lead_scoring.py:198-200` | **bepaalt zelf** (eigen tuple gdpr + 3 statussen voor `push_eligible`) | **bepaalt zelf** | ⚠️ VERDACHT — duplicaat-oordeel, zelfde semantiek, eigen implementatie |
| `utils/deduplicator.py:170-174` | bepaalt zelf (re-scrape-eligibility) | bepaalt zelf (disqualified) | ⚠️ ander doel (ingest-dedup, geen outbound) — acceptabel maar drift-gevoelig |
| `utils/email_sendability.py:29` | — | `_NEVER_SENDABLE` bevat "unsubscribed" als **email_status** (ander veld) | ℹ️ ander domein (email-status ≠ lead-status) |
| `job_queue/website_analysis_queue.py:44` | — | `_TERMINAL_LEAD_STATUSES` (analyse-eligibility, cost-guard) | ℹ️ geen outbound |
| `api/main.py:3445, :4107` + `scoring/recontact_signals.py:195` | query-filter `.eq("gdpr_safe", True)` | deels | ℹ️ consumeren via DB-filter — semantisch consistent maar buiten de functie-gate om |

**Verdict GEDEELTELIJK:** outbound-launchability heeft ná de fixes één bron.
Maar `scoring/lead_scoring.py:198-200` herhaalt exact hetzelfde oordeel in
eigen code voor `push_eligible` — geen actief lek (zelfde tuple), wel de
plek waar toekomstige drift ontstaat als BLOCKED_STATUSES ooit wijzigt.
**Actie (buiten fix-scope, klein):** lead_scoring laten consumeren van
`compliance_check`. Genoteerd als invariant-onderhoud voor Sprint 2.

---

## 3. Error Recovery — BEWEZEN (live, met screenshots)

**Methode:** productie-build (`vite preview` op dist) + Playwright
route-interception. Geen kunstmatige throw — S5 gebruikt een kapotte
API-respons (`{"leads": "dit-is-geen-array"}`) die door `useMemo`/render
loopt (`.filter` op string → TypeError tijdens render).

**Meetfout eerst:** de eerste run registreerde routes in verkeerde volgorde
(Playwright: laatst-geregistreerde wint; de catch-all overschreef de
scenario-mock) → 4/5 scenario's vals-negatief. Na correctie:

| Scenario | Verwacht | Waargenomen | Screenshot |
|---|---|---|---|
| S1: API 500 op /leads | error-state + retry + toast | ✅ alle drie | `docs/audits/evidence/s1_api_500.png` |
| S2: API 401 overal | AuthErrorBanner "Niet geautoriseerd" + healthz-hint, GÉÉN dubbele toast | ✅ banner + hint; toast terecht afwezig | `s2_api_401.png` |
| S3: network-abort | error-state + retry | ✅ | `s3_network_fail.png` |
| S4: malformed (HTML i.p.v. JSON) | error-state + toast (JSON.parse-fout via query-error-pad) | ✅ | `s4_malformed.png` |
| S5: render-crash door kapotte data-shape | ErrorBoundary-fallback "Er ging iets mis" + "Probeer opnieuw", geen witte pagina | ✅ | `s5_render_crash.png` |

**Beperking (eerlijk):** ErrorBoundary vangt geen event-handler/async-crashes
— dat is React-semantiek. Mutations lopen via MutationCache.onError → toast
(gedekt); onClick-handlers met eigen throw zijn niet gedekt maar komen in de
huidige codebase niet voor buiten mutations (grep: geen kale throws in
handlers).

---

## 4. Retry-verificatie — GEDEELTELIJK

**Bewezen via tests met delay-assertions** (niet commit-message — de tests
asserten de daadwerkelijke sleep-waarden):

- `test_429_retries_then_succeeds`: 2×429 → succes op poging 3;
  backoff-curve geasserteerd als `sleeps == [2.0, 4.0]` ✅
- `test_retry_after_header_wins_over_backoff`: header "7" → `sleeps == [7.0]` ✅
- `test_max_retries_exhausted_raises`: raise + log met `lead_id` én context
  geasserteerd via caplog ✅
- `test_non_retryable_error_passes_through_immediately`: 1 call, 0 sleeps ✅

**Niet gedekt (live aangetoond, bewijs 4b — ad-hoc run):**

```
APITimeoutError: 1 call(s) -> GEEN RETRY (direct raise)
APIConnectionError: 1 call(s) -> GEEN RETRY (direct raise)
```

Timeouts en transient network-failures worden NIET geretried. Dit is
*gedocumenteerd* gedrag (helper claimt alleen 429/503/529), maar een gap
t.o.v. brede transient-resilience. Gevolg beperkt: de sync-fallback in
`batched_enrichment` vangt niet-API-fouten op één pad; overige callers
falen hard. **Genoteerd als invariant-schuld** (geen fix-scope — retry is
geen afgedwongen-invariant).

---

## 5. Readiness als enige bron — GEDEELTELIJK

`assess_launch_readiness` **composeert** de echte gates (compliance,
is_sendable, completeness, score, cooldown) — het is een view, geen gate.
De daadwerkelijke beslissingen zitten in de onderliggende functies, en die
zijn wél single-source (sectie 2). Maar er is duplicatie op de flankchecks:

- `/leads/send-to-warmr` (api/main.py:590) herhaalt inline
  `email_status in ("verified","catch_all")` + score-drempel — zelfde
  semantiek als readiness-checks 2+4, eigen code.
- `scoring/lead_scoring.py:192-195` herhaalt de score/icp-drempels
  (zelfde envs — consistent, maar tweemaal geïmplementeerd).
- UI: `WhyThisLead` consumeert het readiness-endpoint ✅;
  `CampagneLaunch` consumeert het endpoint **niet** (zie sectie 8).

**Actie:** send-to-warmr-filter en lead_scoring-gates laten delen op één
helper — Sprint 2-onderhoud, geen actief lek (drempels komen uit dezelfde
env-vars).

---

## 6. Hidden Outbound Inventory — DE vondst

**Volledige egress-lijst** (bewijs 6a-6e: grep op push/create-sites, smtplib,
Resend, httpx, schedulers):

| # | Pad | Aanroeper | Gate (pre-audit) | Gate (nu) |
|---|---|---|---|---|
| 1 | `api/main.py:603` push_leads_bulk | POST /leads/send-to-warmr (operator) | compliance_check ✅ | idem |
| 2 | `api/main.py:671` push_lead | POST /leads/{id}/send-review-email (operator) | compliance_check ✅ | idem |
| 3 | `api/main.py:1225+1233` create_campaign + bulk | POST /campaigns/launch (operator; preview deelt filter) | filter_launchable_leads → compliance_check ✅ | idem |
| **4** | **`campaigns/sequence_engine.py:415` push_lead** | **POST /sequences/process-send ← n8n elke 15 min (scheduler!)** | **SendingGuard, eigen oordeel, disqualified ontbrak ❌** | **compliance_check via `b0410cb` ✅** |
| 5 | `job_queue/enrichment_queue.py:1300` `_NullWarmrClient.push_lead` | — | raise-stub (dead path) | n.v.t. |
| 6 | `api/main.py:4173` Resend | POST /briefing/generate → operator-email | query filtert `.eq("gdpr_safe", True)` (:4107); egress naar OPERATOR, niet prospect | acceptabel; genoteerd |
| 7 | `utils/alert_manager.py:103` Resend | kritieke alerts → operator-email | geen lead-content-gate; alert-teksten kunnen lead-namen bevatten | laag risico; genoteerd |
| 8 | `enrichment/email_verifier.py` SMTP | RCPT TO-handshake naar mail-servers van de lead | technisch egress, geen content-verzending | ℹ️ inventaris-item |
| 9 | scrapers (Maps/RDAP/Meta/websites) | ingress-fetches | geen lead-data uitgaand | ℹ️ |

**Antwoord op de kernvraag:** de vier bekende paden waren NIET de enige — het
**vijfde pad bestond** (rij 4: het sequence-dispatch-pad, nota bene het pad
dat het vaakst draait — elke 15 minuten via n8n, zonder operator ernaast).
Gevonden, gefixt (`b0410cb`), en met regressietest bewaakt.

---

## 7. Operator Safety — GEDEELTELIJK

| Actie | Dubbele side-effect mogelijk? | Event gelogd? | Bewijs |
|---|---|---|---|
| POST /admin/re-enqueue-stale-leads | ❌ dedupt op pending/running jobs (7d: regels 212-238) + cost-guard capt per lead | ✅ | 7d |
| POST /leads/send-to-warmr | ⚠️ geen Heatr-side check op `pushed_to_warmr_at`; Warmr-side bulk-dedup vangt op (`duplicates`-teller, warmr_client:241-282) | ✅ email_sent per lead (:606) | 7a/7b |
| POST /leads/{id}/send-review-email | ⚠️ **geen idempotency** — 2× klikken = 2× push_lead; geen `pushed_to_warmr_at`-check (7a: grep leeg) | ✅ review_email_sent | 7a |
| POST /campaigns/launch | ⚠️ zelfde: herhaald launchen dupliceert campagnes (Warmr-side lead-dedup binnen campagne helpt deels) | ✅ email_sent per lead (:1244) | 7c-correctie |
| POST /leads/bulk-status | ✅ idempotent (override-set) | ❌ **geen timeline-event**; wel attributie-kolommen (override_by/at/reason via identify_principal) | 7c |
| POST /leads/{id}/test-mode | ✅ idempotent toggle | ❌ geen event (bekende keuze, build-log 2026-05-05) | 7c |
| POST /leads/disqualify | ✅ idempotent | ✅ deal_lost | 1f |

**Meetnotitie:** de eerste event-scan rapporteerde launch als 0-events — dat
was een kapot sed-patroon (slash in "campaigns/launch"); handmatige
verificatie toont `_insert_timeline_event` op regel 1244. Gecorrigeerd.

**Kern-gap:** geen idempotency-keys op push-acties. Warmr-side dedup is de
enige vangrail. **Invariant-schuld → Control Plane-sprint** (per fix-scope).

---

## 8. Verborgen businesslogica in UI — GEDEELTELIJK

- **Silent catches na Sprint 0** (bewijs 8c): 8 resterende sites —
  Analytics (2), CRMActivity (2), LeadDetail (3), CampagneLaunch (1), plus
  3 bewust-stille layout-widgets. Allemaal *display*-datapunten; geen enkele
  bepaalt launchability. Laag risico, wel ruis: LeadDetail-thread die bij
  fout `{thread: []}` toont, oogt als "nog geen mails".
- **`eligibleLeads` in CampagneLaunch** (8d): server-gefilterde query
  (`/leads?status=enriched&has_email=true`) — de UI bepaalt níet zelf;
  naam suggereert meer dan het is. De echte gate zit op POST-launch. Geen
  schending; wel UX-gap (toont leads die de launch daarna weigert —
  readiness-endpoint zou dit vooraf tonen).
- **`RISKY_EMAIL_STATUSES` in CampagneLaunch:88** (8e): clientside
  risico-definitie die **afwijkt** van `utils/email_sendability`:
  UI rekent `catchall` risky terwijl de server (default
  `HEATR_ALLOW_RISKY_EMAILS=true`) 'm accepteert, en mist juist `risky`.
  Het is een waarschuwings-badge, geen beslissing — maar de operator krijgt
  een ander risicobeeld dan de gate hanteert. **Actie:** CampagneLaunch de
  readiness/`is_sendable`-output laten consumeren i.p.v. eigen set —
  genoteerd voor Sprint 2 (UI-ombouw is feature-werk, geen audit-fix).

**Verdict:** geen beslissings-lek; wel twee weergave-duplicaties die drift
veroorzaken (risicobeeld + eligibility-preview).

---

## Samenvatting verdicten

| # | Verificatie | Verdict |
|---|---|---|
| 1 | Compliance één gate op alle paden | **BEWEZEN** (na `b97cfd4` + `b0410cb`; pre-fix: 2 gaten) |
| 2 | SSoT launchability | **GEDEELTELIJK** (lead_scoring dupliceert oordeel) |
| 3 | Error recovery live | **BEWEZEN** (5/5 scenario's, screenshots) |
| 4 | Retry | **GEDEELTELIJK** (429-pad volledig; timeout/network niet geretried) |
| 5 | Readiness als enige bron | **GEDEELTELIJK** (composer ✅; flank-duplicaten + CampagneLaunch consumeert niet) |
| 6 | Outbound-paden compleet | **BEWEZEN** — vijfde pad gevonden én gedicht |
| 7 | Operator safety | **GEDEELTELIJK** (geen idempotency-keys; 2 acties zonder event) |
| 8 | Geen UI-businesslogica | **GEDEELTELIJK** (geen beslissingen; wel definitie-drift) |

Zie `docs/architecture_invariants.md` voor de invariant-verdicten en
benodigde acties.
