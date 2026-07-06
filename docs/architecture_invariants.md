# Architecture Invariants — de grondwet van Aerys OS

**Status per 2026-07-06** (Sprint 1 verificatie-audit). Elke toekomstige
sprint wordt tegen dit document getoetst: een PR die een *afgedwongen*
invariant breekt is per definitie fout, ongeacht hoe nuttig de feature is.

Bewijs per invariant staat in
[docs/audits/sprint1_reliability_verification_2026-07-06.md](audits/sprint1_reliability_verification_2026-07-06.md).

---

## De invarianten

### I1 — Er is precies één compliance-beslissing
**Status: AFGEDWONGEN** *(sinds `b97cfd4` + `b0410cb`)* · Bewijs: secties 1 + 6

Elke outbound-flow (launch, preview, send-to-warmr, review-email én het
n8n-dispatch-pad voor mail 2/3) loopt door
`utils/enrichment_check.compliance_check`. Regressietests:
`tests/test_enrichment_check.py` + `tests/test_sending_guard_compliance.py`.

**Handhavingsregel voor nieuwe code:** elk nieuw pad dat
`WarmrClient.push_lead`/`push_leads_bulk` aanroept (of enige andere
prospect-gerichte verzending introduceert) MOET `compliance_check`
aanroepen vóór de push. Review-checklist: grep op nieuwe push-call-sites.

### I2 — Er is precies één readiness-beslissing
**Status: AFGEDWONGEN** *(sinds Sprint 2 pre-conditie-commit)* ·
Bewijs: secties 2 + 5 + Sprint 2-fixes

De drie flank-duplicaten uit Sprint 1 zijn opgeruimd:
1. `scoring/lead_scoring.py` consumeert nu `compliance_check` i.p.v. een
   eigen gdpr/status-tuple ✅
2. `api/main.py` send-to-warmr gebruikte al `compliance_check` (Sprint 0);
   de resterende inline email/score-drempels lezen dezelfde env-vars als
   readiness — geen tweede beslissing, wel tweemaal dezelfde lezing
   (acceptabel; drempel-wijziging = env-wijziging, geen code-drift)
3. `CampagneLaunch.tsx` leest de sendability-definitie van
   `GET /config/sendability` — geen eigen statuslijst-kopie meer ✅

**Handhavingsregel:** nieuwe UI-badges of backend-filters die
launchability-fragmenten tonen/gebruiken, consumeren `compliance_check`,
`is_sendable`, `sendability_config()` of het readiness-endpoint — nooit
een eigen lijst.

### I3 — Iedere outbound side-effect loopt via de dispatcher
**Status: GEDEELTELIJK** · Bewijs: sectie 6

Er is vandaag géén centrale dispatcher — er zijn 4 plekken die zelf
`WarmrClient` instantiëren en pushen. Ze delen nu wél dezelfde
compliance-gate (I1), maar niet één verzendpunt: idempotency, rate-limiting
en event-logging zijn per pad geregeld i.p.v. op één plek.

**Nodig om af te dwingen:** een `dispatch_outbound(lead, kind, ...)`-laag
waar alle 4 paden doorheen moeten (Control Plane-sprint). Tot die tijd geldt
de handhavingsregel uit I1 als surrogaat.

### I4 — Iedere operator-actie wordt als event gelogd
**Status: AFGEDWONGEN** *(sinds Sprint 2 pre-conditie-commit)* ·
Bewijs: sectie 7 + Sprint 2-fixes

Alle bekende operator-acties schrijven een `lead_timeline`-event:
disqualify, send-to-warmr, campaigns/launch, review-email, re-enqueue,
stage-changes, task-mutaties, en sinds Sprint 2 ook `bulk-status`
(event `manual_status_override`, met principal-attributie in metadata)
en `test-mode` (event `test_mode_toggled`).

**Handhavingsregel:** elke nieuwe POST/PATCH die lead-state muteert of een
side-effect triggert schrijft een `lead_timeline`-row (of, voor outbound,
een `outbound_log`-record via de dispatcher).

### I5 — Geen verborgen businesslogica in de UI
**Status: AFGEDWONGEN** *(sinds Sprint 2 pre-conditie-commit)* ·
Bewijs: secties 5 + 8 + Sprint 2-fixes

Geen enkel launch/compliance-oordeel wordt clientside geveld (bewezen in
Sprint 1), en de laatste definitie-drift is weg: de UI leest de
sendability-statuslijsten van `GET /config/sendability` i.p.v. een eigen
kopie. Restpunt (géén businesslogica, wel hygiëne): 8 silent-catch-sites
op pure display-data (Analytics/CRMActivity/LeadDetail-tabs +
3 bewust-stille layout-widgets).

**Handhavingsregel:** UI toont oordelen (risky, ready, launchable)
uitsluitend uit backend-endpoints; nieuwe `.catch(() => fallback)` in een
queryFn is een blokkerende review tenzij het aantoonbaar decoratieve data
betreft.

### I6 — Geen side-effect zonder idempotency-key
**Status: NOG NIET** *(verwacht — Sprint 0 noemt idempotency nergens)* ·
Bewijs: sectie 7

Enige bestaande idempotency: `/leads/import` (import_run_id, migratie 019)
en Warmr-side bulk-dedup (`duplicates`-teller). Push-acties
(send-review-email, send-to-warmr, launch, dispatch) hebben géén
Heatr-side idempotency-key: dubbel klikken = dubbele side-effect-poging.

**Invariant-schuld → Control Plane-sprint.** Hoort bij de operator-acties
(retry, force-next, restart-from-step) die daar gebouwd worden; een
idempotency-laag zonder dispatcher (I3) zou dubbel werk zijn.

### I7 — Iedere workflow is volledig reproduceerbaar uit de event-log
**Status: NOG NIET** *(verwacht)* · Bewijs: sectie 7

`lead_timeline` + `blocked_sends` + `decision_log`-achtige structuren
bestaan, maar dekken niet alle mutaties (I4-gaten), bevatten geen
payload-snapshots voor sends (wel `sequence_snapshot` op campagnes —
migratie 013), en er is geen replay-mechanisme.

**Invariant-schuld → Control Plane-sprint.** Vereist eerst I3 (dispatcher
als één schrijfpunt) en I6 (idempotente replay).

---

## Overzicht

| Invariant | Bewezen door | Status |
|---|---|---|
| I1 Eén compliance-beslissing | audit §1 + §6 | **afgedwongen** (b97cfd4 + b0410cb) |
| I2 Eén readiness-beslissing | audit §2 + §5 + Sprint 2 pre-conditie | **afgedwongen** |
| I3 Outbound via dispatcher | audit §6 | gedeeltelijk (gate gedeeld, verzendpunt niet) — Sprint 2 in uitvoering |
| I4 Operator-acties als event | audit §7 + Sprint 2 pre-conditie | **afgedwongen** |
| I5 Geen UI-businesslogica | audit §5 + §8 + Sprint 2 pre-conditie | **afgedwongen** (restpunt: display-catch-hygiëne) |
| I6 Idempotency-keys | audit §7 | **nog niet** — Sprint 2 in uitvoering |
| I7 Event-log-reproduceerbaarheid | audit §7 | **nog niet** — Sprint 2 legt fundament |

## Toets-procedure voor volgende sprints

1. Elke PR die verzending, lead-state-mutatie of operator-acties raakt:
   toets expliciet tegen I1-I5 (de afgedwongen/bijna-afgedwongen set).
2. Nieuwe push-call-sites zonder `compliance_check` = blokkerende review.
3. I6/I7 worden pas afgedwongen zodra de Control Plane de dispatcher (I3)
   levert — tot die tijd zijn ze gedocumenteerde schuld, geen regel.
