# Architecture Invariants — de grondwet van Aerys OS

**Status per 2026-07-06** (Sprint 1 verificatie-audit + Sprint 2 Control
Plane v0). Elke toekomstige
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
**Status: AFGEDWONGEN** *(sinds Sprint 2, commit `77bbe56`)* ·
Bewijs: `utils/outbound_dispatcher.py` + `tests/test_outbound_dispatcher.py`

Alle prospect-gerichte egress-paden lopen door
`utils.outbound_dispatcher.dispatch_outbound`: send-to-warmr (single +
bulk), review-email, campaigns/launch (campaign-create + push),
sequence-dispatch (n8n-pad, mail 2/3), morning-briefing en critical-alerts
(record-only). De dispatcher doet drie dingen op één plek:
compliance-vangnet (`DispatchBlocked` bij I1-schending), idempotency-check
(I6) en een append-only record in `heatr_outbound_log` (migratie 020) —
óók voor geblokkeerde, geskipte en gefaalde pogingen.

Fail-open by design: als de ledger-tabel ontbreekt (migratie 020 nog niet
gedraaid) gaat de send dóór met een luide error-log — sends bricken is
erger dan tijdelijk zonder dedup draaien.

**Handhavingsregel:** elke nieuwe call-site van
`WarmrClient.push_lead`/`push_leads_bulk`/`create_campaign`, of enige
nieuwe prospect-gerichte verzending, gaat door `dispatch_outbound` met een
deterministische idempotency-key. Directe WarmrClient-pushes buiten de
dispatcher = blokkerende review.

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
**Status: AFGEDWONGEN** *(sinds Sprint 2, commit `77bbe56`; operationeel
zodra migratie 020 gedraaid is)* · Bewijs: `utils/outbound_dispatcher.py`

Elke dispatch heeft een deterministische key; een tweede poging met
dezelfde key wordt geskipt (`skipped_duplicate`) zonder send. Conventies:

| Pad | Key |
|---|---|
| send-to-warmr bulk | `warmr-bulk:{campaign}:{ids_hash}` |
| review-email | `review-email:{lead_id}` |
| campaign-create | `campaign-create:{naam}:{template}:{ids_hash}` |
| campaign-push | `campaign-push:{camp_id}:{ids_hash}` |
| sequence-send (n8n) | `seq-send:{record_id}:step:{step_index}:epoch:{restart_epoch}` |
| morning-briefing | `briefing:{ws}:{date}` |
| critical-alerts | record-only (`enforce_idempotency=False` — suppressie is gevaarlijker dan een dubbele melding) |

De `restart_epoch` (kolom op `lead_campaign_history`, migratie 020) lost de
spanning idempotency ↔ bewuste herzending op: een accidentele duplicate
heeft dezelfde key en wordt geblokkeerd; een operator-restart bumpt de
epoch → nieuwe key → herzendbaar. Restart is daarmee de ENIGE route naar
een tweede send van dezelfde stap.

**Handhavingsregel:** nieuwe dispatch-kinds krijgen een deterministische
key (geen timestamps/randoms in de key); `enforce_idempotency=False` is
alleen toegestaan voor operator-gerichte meldingen, nooit voor
prospect-gerichte sends.

### I7 — Iedere workflow is volledig reproduceerbaar uit de event-log
**Status: FUNDAMENT GELEGD** *(Sprint 2)* ·
Bewijs: `heatr_outbound_log` (migratie 020) + `utils/run_state.py`

Wat er nu is: één append-only ledger voor álle outbound side-effects
(inclusief geblokkeerd/geskipt/gefaald, met actor, key, result en
metadata), plus de Inspect-laag (`GET /leads/{id}/run-state`) die het
run-beeld per lead componeert uit bestaande data. Zie
`docs/audits/sprint2_runstate_inventory.md` voor de volledige bronnen-map.

Wat bewust schuld blijft (gemarkeerd als `gaps` in de run-state-payload):
per-attempt retry-history (anthropic-retries loggen alleen naar de
logger), step-timestamps binnen een enrichment-run, en een
replay-mechanisme. Volledige replay vereist payload-snapshots per event —
overvragen we niet tot er een concrete replay-behoefte is.

**Handhavingsregel:** de ledger is append-only — geen UPDATE/DELETE op
`heatr_outbound_log`; correcties zijn nieuwe records.

---

## Overzicht

| Invariant | Bewezen door | Status |
|---|---|---|
| I1 Eén compliance-beslissing | audit §1 + §6 | **afgedwongen** (b97cfd4 + b0410cb) |
| I2 Eén readiness-beslissing | audit §2 + §5 + Sprint 2 pre-conditie | **afgedwongen** |
| I3 Outbound via dispatcher | dispatcher + tests (Sprint 2) | **afgedwongen** (77bbe56) |
| I4 Operator-acties als event | audit §7 + Sprint 2 pre-conditie | **afgedwongen** |
| I5 Geen UI-businesslogica | audit §5 + §8 + Sprint 2 pre-conditie | **afgedwongen** (restpunt: display-catch-hygiëne) |
| I6 Idempotency-keys | dispatcher + key-conventies (Sprint 2) | **afgedwongen** (operationeel zodra migratie 020 gedraaid is) |
| I7 Event-log-reproduceerbaarheid | outbound-ledger + run-state (Sprint 2) | fundament gelegd — replay is bewuste schuld |

## Toets-procedure voor volgende sprints

1. Elke PR die verzending, lead-state-mutatie of operator-acties raakt:
   toets expliciet tegen I1-I6 (de afgedwongen set).
2. Nieuwe push-call-sites buiten `dispatch_outbound` om, of zonder
   `compliance_check` = blokkerende review.
3. Nieuwe dispatch-kinds zonder deterministische idempotency-key =
   blokkerende review; `enforce_idempotency=False` alleen voor
   operator-meldingen.
4. I7-replay blijft gedocumenteerde schuld tot er een concrete
   replay-behoefte is — de ledger + run-state zijn het fundament.
