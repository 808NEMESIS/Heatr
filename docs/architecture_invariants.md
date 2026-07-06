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
**Status: GEDEELTELIJK** · Bewijs: secties 2 + 5

`assess_launch_readiness` composeert de gates correct, en de compliance-kern
is single-source. Maar drie flanken dupliceren oordeel-fragmenten:
1. `scoring/lead_scoring.py:198-200` — eigen gdpr/status-tuple voor
   `push_eligible`
2. `api/main.py:590` — inline email/score-filter in send-to-warmr
3. `CampagneLaunch.tsx:88` — eigen `RISKY_EMAIL_STATUSES` die afwijkt van
   `email_sendability`

**Nodig om af te dwingen:** (1)+(2) laten consumeren van
`compliance_check`/`is_sendable`/readiness (klein refactor-ticket);
(3) CampagneLaunch ombouwen naar het readiness-endpoint (Sprint 2 UI-werk).

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
**Status: GEDEELTELIJK** · Bewijs: sectie 7

Gelogd: disqualify, send-to-warmr (per lead), campaigns/launch (per lead),
review-email, re-enqueue, stage-changes, task-mutaties.
NIET gelogd als event: `bulk-status` (wel attributie-kolommen op de
lead-row: override_by/at/reason) en `test-mode`-toggle (bewuste keuze,
build-log 2026-05-05 — maar tegen deze invariant).

**Nodig om af te dwingen:** timeline-event toevoegen aan beide endpoints
(~20 min) + review-regel: elke nieuwe POST/PATCH die lead-state muteert
schrijft een `lead_timeline`-row.

### I5 — Geen verborgen businesslogica in de UI
**Status: GEDEELTELIJK** · Bewijs: secties 5 + 8

Geen enkel launch/compliance-oordeel wordt clientside geveld (bewezen).
Wel twee weergave-duplicaties die drift veroorzaken:
`RISKY_EMAIL_STATUSES` (ander risicobeeld dan de server hanteert) en 8
resterende silent-catch-sites op display-data.

**Nodig om af te dwingen:** risico/eligibility-badges voeden vanuit
`/leads/{id}/launch-readiness`; lint-regel of review-checklist tegen
`.catch(() => ...)` in queryFn's.

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
| I2 Eén readiness-beslissing | audit §2 + §5 | gedeeltelijk |
| I3 Outbound via dispatcher | audit §6 | gedeeltelijk (gate gedeeld, verzendpunt niet) |
| I4 Operator-acties als event | audit §7 | gedeeltelijk (bulk-status + test-mode missen) |
| I5 Geen UI-businesslogica | audit §5 + §8 | gedeeltelijk (geen beslissingen; definitie-drift) |
| I6 Idempotency-keys | audit §7 | **nog niet** — schuld naar Control Plane |
| I7 Event-log-reproduceerbaarheid | audit §7 | **nog niet** — schuld naar Control Plane |

## Toets-procedure voor volgende sprints

1. Elke PR die verzending, lead-state-mutatie of operator-acties raakt:
   toets expliciet tegen I1-I5 (de afgedwongen/bijna-afgedwongen set).
2. Nieuwe push-call-sites zonder `compliance_check` = blokkerende review.
3. I6/I7 worden pas afgedwongen zodra de Control Plane de dispatcher (I3)
   levert — tot die tijd zijn ze gedocumenteerde schuld, geen regel.
