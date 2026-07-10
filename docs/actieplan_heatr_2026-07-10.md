# Heatr — Actieplan na Architecture Validation & Future-State Audit v2

Datum: 2026-07-10
Bron: docs/lifecycle_audit_v2_2026-07-09.md
Doel: de huidige productie- en juridische risico's eerst beheersen, daarna het platform gecontroleerd voorbereiden op multi-tenancy en schaal.

---

## 1. Uitgangspunten

De belangrijkste conclusie uit audit v2 is dat Heatr niet als eerste breekt op nurture- of recontactlogica, maar op de fundamenten rond verzending:

1. idempotency;
2. suppression en compliance;
3. deliverability-guards;
4. centrale uitschakelbaarheid;
5. tenantisolatie;
6. kosten en schaalbaarheid.

Daarom wordt de lifecycle niet direct volledig herbouwd. Eerst worden de bestaande paden veilig gemaakt en meetbaar gemaakt.

### Technische uitgangspunten

- Warmr blijft voorlopig eigenaar van de server-side dripsequence.
- Heatr verstuurt niet zelfstandig sequence-stappen zolang dit ownershipmodel geldt.
- Nieuwe lead_campaign_history-rijen zijn voorlopig uitsluitend trackingrecords en mogen niet door get_due_sends worden opgepakt.
- Geen grote schema- of state-machinewijziging zonder migratieplan, backfill en rollbackpad.
- Iedere productie-send moet via één centrale egressfunctie lopen.
- Iedere fix krijgt minimaal: unit-tests; een integratie- of concurrencytest waar relevant; logging en metrics; een expliciet rollbackpad.

---

## 2. Doelarchitectuur voor de tussenfase

Voordat de uiteindelijke Heatr v2-architectuur wordt gebouwd, wordt eerst een veilige tussenarchitectuur gerealiseerd.

### Sendmodel

Voorlopig geldt: **Warmr beheert de sequence en verstuurt de sequence-stappen. Heatr beheert selectie, compliance, enrollmenttracking en verwerking van events.**

Heatr mag in deze fase:
- een campagne aanmaken;
- leads bij Warmr enrollen;
- een trackingrecord aanmaken;
- Warmr-events verwerken;
- campagnes pauzeren;
- suppressie afdwingen;
- rapporteren en auditen.

Heatr mag in deze fase niet:
- zelfstandig mail 2, 3 of verdere sequence-stappen versturen;
- dezelfde enrollment als pending aanbieden aan sequence_engine;
- twee concurrerende modellen voor dezelfde sequence actief houden.

### Centrale beveiligingslaag

Alle outbound-acties moeten uiteindelijk door één centrale flow:

```
caller
  → send kill-switch
  → tenant/context-validatie
  → suppression/compliance
  → SendingGuard/capacity
  → idempotency-reservering
  → Warmr-command
  → resultaatregistratie
```

Geen endpoint of background worker mag deze flow rechtstreeks omzeilen.

---

## 3. Fasering

| Fase | Onderwerp | Doel |
|---|---|---|
| 0 | Stabilisatie en observatie | Productiegedrag zichtbaar maken en wijzigingsrisico begrenzen |
| 1 | Idempotency en kill-switch | Dubbele sends voorkomen en verzending centraal kunnen stoppen |
| 2 | Suppression en GDPR | Unsubscribes, bounces en forget betrouwbaar afdwingen |
| 3 | Campaigntracking en SendingGuard | Enrollmenthistorie vullen zonder tweede send-engine te activeren |
| 4 | Multi-tenancy en kosten | Tenantisolatie en schaalbare cost-control |
| 5 | Lifecycle v2 | Twee-assen state machine, eventledger, nurture en recontact |

---

## 4. Fase 0 — Stabilisatie en meetbaarheid

Prioriteit: direct. Doel: voorkomen dat fixes worden uitgerold zonder zicht op het huidige gedrag.

### 4.1 Productiesends inventariseren

Maak één definitieve lijst van alle codepaden die een extern bericht kunnen veroorzaken.

Minimaal controleren: /campaigns/launch; /leads/send-to-warmr; /leads/{id}/send-review-email; process_due_send; operatorbriefings; bulk-pushes; webhookgedreven vervolgacties; eventuele n8n- of cronpaden.

Per pad vastleggen: caller; gebruikte functie; type bericht; huidige kill-switch; huidige compliancecheck; huidige idempotencykey; gebruikte tabel; externe side effect.

**Acceptatiecriterium:** er bestaat één actuele send-path-matrix in `docs/outbound_send_paths.md`. Ieder productiepad is daarin opgenomen en gekoppeld aan file:line.

### 4.2 Tijdelijke waarschuwingslogging

Voeg vóór de grotere wijzigingen tijdelijk structured logging toe rondom: iedere dispatch_outbound; ledger-writefouten; suppressionbeslissingen; Warmr-timeouts; dubbele idempotencykeys; onbekende webhookevents; SendingGuard-fouten.

Minimale context: workspace_id; lead_id; campaign_id; idempotency_key; event_id; send_type; decision; reason.

Geen volledige e-mailinhoud of andere onnodige PII loggen.

### 4.3 Baseline meten

Leg vóór de eerste fix minimaal vast:
- aantal outbounddispatches per dag;
- aantal ledger-writefouten;
- aantal Warmr-timeouts;
- aantal unsubscribe- en bounce-events;
- aantal leads met email_status='unsubscribed' maar een andere status;
- aantal leads met email_status='bounced' die nog campaign-eligible zijn;
- huidige grootte van heatr_outbound_log;
- huidige grootte van heatr_lead_campaign_history.

**Deliverable:** `docs/pre_fix_baseline_2026-07.md`

---

## 5. Fase 1 — Idempotency en centrale kill-switch

Dit is de eerste echte productiefix.

### PR 1 (code-PR 2) — Outbound-ledger correct bedraden

1. Voeg "outbound_log" toe aan _HEATR_TABLES in config/database.py.
2. Controleer dat .table("outbound_log") runtime naar heatr_outbound_log wordt vertaald.
3. Verwijder of verbeter misleidende foutmeldingen die suggereren dat migratie 020 niet is uitgevoerd terwijl de tabelnaam verkeerd wordt geprefixt.
4. Laat ledgerfouten niet stil verdwijnen: log als error; voeg metric toe; bepaal per sendtype of fail-closed vereist is.

**Belangrijk:** deze codewijziging mag niet afzonderlijk naar productie zonder de databaseconstraint uit PR 2. Alleen de tabelnaam repareren maakt het systeem zichtbaar werkend, maar nog steeds racegevoelig.

### PR 2 (code-PR 3) — Database-level idempotency

Migratie: `UNIQUE (workspace_id, idempotency_key)`. Controleer eerst of bestaande dubbele keys aanwezig zijn:

```sql
SELECT workspace_id, idempotency_key, COUNT(*)
FROM heatr_outbound_log
GROUP BY workspace_id, idempotency_key
HAVING COUNT(*) > 1;
```

Omdat de tabel waarschijnlijk grotendeels leeg is, worden weinig conflicten verwacht. Toch mag dit niet worden aangenomen.

**Gewenste dispatchflow** — vervang check-then-send door een atomisch reserveringsmodel:

1. INSERT ledgerrecord met status='in_flight'
2. Conflict op UNIQUE: completed → niet opnieuw versturen; in_flight → niet parallel opnieuw versturen; failed/retryable → expliciet retrybeleid
3. Externe call naar Warmr
4. Ledgerrecord → completed of failed

Statussen minimaal: in_flight; completed; failed_retryable; failed_terminal.

**Belangrijk risico:** een UNIQUE-constraint alleen voorkomt dat twee ledgerrecords worden gemaakt, maar niet automatisch dat de code na een conflict alsnog verstuurt. De dispatcher moet de insertuitkomst als eigenaarstoewijzing behandelen.

**Tests:** twee gelijktijdige dispatches met dezelfde key; eerste in_flight → tweede stopt; eerste completed → tweede deduplicated; externe timeout na vermoedelijk succesvolle acceptatie; DB niet bereikbaar vóór reservering; DB niet bereikbaar ná externe send.

**Acceptatiecriteria:** maximaal één worker verkrijgt ownership van een idempotencykey; geen send zonder succesvolle ledgerreservering; herhaalde calls met dezelfde key leveren geen tweede externe call op; iedere dispatch heeft een auditeerbaar resultaat.

### PR 3 (code-PR 4) — Kill-switch naar centrale egress

Verplaats de check op ENABLE_CAMPAIGN_SENDS naar de centrale outboundlaag. De check mag niet alleen in /campaigns/launch staan.

**Beleid per berichttype:**

| Berichttype | Master-switch |
|---|---|
| Campaign launch | Ja |
| Sequence follow-up | Ja |
| Ad-hoc lead push | Ja |
| Review-mail | Ja |
| Operatorbriefing | Beslissen; bij voorkeur aparte switch |
| Transactionele interne melding | Aparte categorie |

Voorkom dat een algemene campaignswitch kritieke interne operationele meldingen blokkeert. Gebruik zo nodig twee flags: ENABLE_PROSPECT_SENDS; ENABLE_INTERNAL_NOTIFICATIONS.

**Tests:** switch uit vóór launch; switch uit na launch maar vóór follow-up; switch uit tijdens worker-run; alle externe prospectpaden geblokkeerd; interne meldingen volgen eigen policy.

### Fase-1 acceptatie

Fase 1 is gereed wanneer: iedere prospectsend centraal geblokkeerd kan worden; de ledger correct schrijft; de ledger database-level unique is; concurrencytests aantonen dat dezelfde send niet dubbel wordt uitgevoerd; operationele metrics beschikbaar zijn.

---

## 6. Fase 2 — Suppression, bounce en GDPR

Deze fase moet direct na idempotency worden uitgevoerd.

### PR 4 (code-PR 5) — Directe compliance-hotfix

1. Voeg bounced toe aan de blokkerende statussen.
2. Laat unsubscribe-events zowel de legacyvelden als de leidende globale lifecycle-overlay bijwerken.
3. Laat hard-bounce-events dezelfde globale blockstatus zetten.
4. Laat alle launch- en sendpaden dezelfde centrale compliancefunctie gebruiken.
5. Verwijder of beperk de is_test_lead-bypass: testleads mogen alleen suppressie omzeilen in een expliciete niet-productieomgeving; nooit op basis van alleen een leadveld.

**Tijdelijk overgangsmodel** — totdat de suppressiontabel bestaat, controleert de gate minimaal: status; email_status; manual_status_override; bestaande campaign/enrollmentstatus; eventuele andere bekende suppressievelden. Dit is overgangslogica, geen eindmodel.

**Tests:** unsubscribed via webhook; unsubscribe dubbel ontvangen; hard bounce; soft bounce; disqualified; forgotten; testlead in productie; dezelfde lead in een latere campagne; ad-hoc send na unsubscribe.

### PR 5 (code-PR 6) — Forget-flow repareren

**Probleem:** alle vergeten leads naar hetzelfde e-mailadres wijzigen veroorzaakt een unique-conflict vanaf de tweede vergeten lead.

**Veilige oplossing:** per-lead-unieke, niet-herleidbare placeholder, bijvoorbeeld `forgotten+<random_or_hash>@invalid.local`.

Eisen: niet terug te leiden naar het origineel zonder aparte sleutel; uniek per lead; ongeldig als afleveradres; geen origineel e-mailadres in logs of placeholder; status pas forgotten nadat de anonimisering volledig is geslaagd.

**Transactie** — de forget-operatie moet atomisch zijn: (1) suppressionrecord toevoegen; (2) PII anonimiseren; (3) actieve enrollments stoppen; (4) openstaande sends annuleren; (5) lifecycle naar forgotten; (6) auditrecord schrijven. Bij een fout mag de functie niet ten onrechte succes retourneren.

**Tests:** twee forgotten leads binnen dezelfde workspace; retry van dezelfde forget; actieve campagne tijdens forget; databasefout halverwege; opnieuw scrapen van hetzelfde oorspronkelijke e-mailadres.

### PR 6 (code-PR 7) — Centrale cross-workspace suppressiontabel

```
heatr_suppressions
- id
- normalized_email
- domain
- suppression_type
- reason
- source
- source_workspace_id
- lead_id
- campaign_id
- event_id
- created_at
- revoked_at
- created_by
```

**Uniqueness:** actieve e-mailsuppressie uniek op genormaliseerd adres via partial unique index: `UNIQUE (normalized_email) WHERE revoked_at IS NULL`. Voor domeinsuppressie een afzonderlijke key of tabel.

**Suppressiontypen minimaal:** unsubscribe; hard_bounce; complaint; forgotten; manual_global; domain_block.

**Privacyafweging:** cross-workspace suppressie betekent niet dat tenants elkaars gegevens mogen inzien. De centrale gate mag alleen antwoorden `allowed=false, reason=globally_suppressed` — niet welke tenant, welke campagne of details, tenzij de eigen workspace daarvoor bevoegd is.

**Gedrag:** de suppressiontabel wordt geraadpleegd vóór enrollment; vóór iedere outbounddispatch; bij retries; vóór ad-hoc sends; bij Warmr-commandgeneratie.

### Acceptatiecriteria fase 2

- Een unsubscribe blokkeert iedere toekomstige campagne.
- Een hard bounce blokkeert iedere toekomstige e-mailsend.
- Dezelfde persoon in een andere workspace wordt eveneens geblokkeerd.
- Forget is idempotent en laat geen originele PII staan.
- Actieve en geplande sends worden bij suppression gestopt.
- De centrale gate is het enige toegestane compliancepad.

---

## 7. Fase 3 — Campaigntracking en SendingGuard

Deze fase vereist een expliciete ownershipbeslissing.

### Besluit vóór implementatie

Leg vast in `docs/adr/ADR-001-sequence-send-ownership.md`:

**Warmr is eigenaar van de dripsequence. Heatr maakt uitsluitend tracking-enrollments aan en verwerkt lifecycle-events.** Dit besluit blijft gelden totdat een afzonderlijke migratie naar Heatr-owned sequencing wordt goedgekeurd.

### PR 7 (code-PR 9) — Tracking-only enrollmentrecords

Bij succesvolle Warmr-enrollment maakt Heatr één record per `workspace × lead × campaign × service`.

**Status:** gebruik geen bestaande pending-status wanneer get_due_sends die selecteert. Introduceer external_active / external_completed / external_stopped / external_failed, **of** voeg expliciet `send_owner='warmr'` toe en laat get_due_sends uitsluitend records met `send_owner='heatr'` selecteren. De tweede aanpak is toekomstvaster.

**Vereiste velden minimaal:** workspace_id; lead_id; campaign_id; service_type; send_owner; status; is_active; warmr_campaign_id; warmr_lead_id; template_version_id; enrolled_at; completed_at; stopped_at; last_event_sequence_no.

**Unique constraint:** `UNIQUE (workspace_id, lead_id, campaign_id, service_type)`. Her-enrollment moet een expliciete nieuwe campagne of enrollmentversie opleveren, niet stil dezelfde rij overschrijven.

### PR 8 (code-PR 10) — Webhook-eventledger en lifecycle

```
heatr_webhook_events
- event_id UNIQUE
- workspace_id
- event_type
- occurred_at
- sequence_no
- lead_id
- campaign_id
- message_id
- payload
- processing_status
- processed_at
- error
```

**Verwerkingsvolgorde:** (1) event opslaan; (2) duplicate event_id → reeds verwerkt/accepted; (3) campaign en lead resolven; (4) sequence-order controleren; (5) transitie uitvoeren; (6) event als processed markeren; (7) pas daarna 200 retourneren.

**Eventordering:** voor dezelfde enrollment wordt een event met lager sequence_no dan laatst verwerkt genegeerd of apart gelogd; terminale statussen mogen niet door een zwakker later event worden teruggedraaid; campaign.completed mag replied, unsubscribed of bounced niet overschrijven.

**Unknown objects:** geen stil {"ok": true} meer. Onbekende lead/campaign: eerst fallbackcorrelatie op Warmr-ID; daarna dead-letter; response volgens afgesproken retrybeleid.

### PR 9 (code-PR 11) — SendingGuard functioneel maken

**Huidige blokkades:** lege enrollmenttabel; niet-bestaande sending_domain; niet-bestaande reply_inbox.event_type.

**Nieuwe bronnen** — caps steunen op feitelijke sendrecords: outbound_log voor sendvolume; webhook/message-eventledger voor bounces; Warmr-capacityevents voor mailboxcapaciteit; suppressiontabel voor compliance.

**Guards minimaal:** per inbox; per sending domain; per workspace; platformbreed; hard-bounce rate; complaint rate; mailbox health; globale kill-switch.

**Belangrijke eigenschap:** SendingGuard moet fail-closed of expliciet degraded-safe werken. Niet `query failed → return None → allow send`, wel `guard data unavailable → block prospect send` met duidelijke operationele melding.

### Fase-3 acceptatie

- iedere Warmr-enrollment heeft een trackingrecord;
- trackingrecords worden niet door Heatr als due send opgepakt;
- events zijn idempotent en geordend;
- is_active wordt bij completion/suppression correct gesloten;
- bounce- en volumeguards gebruiken bestaande, gevulde tabellen;
- campaignstatussen zijn per campaign gescoped.

---

## 8. Fase 4 — Multi-tenancy en schaalbare kostenbeheersing

### PR 10 (code-PR 12) — Workspace-identiteit fail-closed

- verwijder DEFAULT_WORKSPACE als fallback voor externe authenticatie;
- JWT zonder geldige workspaceclaim → 401/403;
- service keys worden expliciet aan één workspace gekoppeld;
- background jobs dragen altijd een workspace_id;
- onbekende workspacecontext → geen query.

Een lokale developmentfallback mag alleen bestaan achter een expliciete developmentflag.

**Tests:** geldige JWT met workspace; JWT zonder workspace; JWT met onbekende workspace; service key voor tenant A op tenant B-resource; workerjob zonder workspace.

### PR 11 (code-PR 13) — Concrete cross-tenant lekken sluiten

Minimaal repareren: companies_raw lookup op alleen domain; deal-wonupdate op alleen id; webhookupdates op alleen lead-id; Claude-cachekey zonder workspace; queueclaims zonder correcte workspacecontext.

**Queryregel:** iedere tenanttabelquery bevat aantoonbaar `workspace_id = authenticated_workspace`, tenzij de tabel bewust globaal is (zoals de interne suppressionindex).

**Automatische controle:** breid een statische lint- of CI-check uit die directe tenanttabelqueries zonder workspacefilter signaleert. Geen volledige garantie, wel regressiepreventie.

### PR 12 (code-PR 14) — RLS als tweede verdedigingslinie

- publieke/user-facing requests gebruiken waar mogelijk scoped JWT-clients;
- service_role uitsluitend in expliciete worker- en adminmodules;
- consistente sessionkey voor workspace-RLS;
- alle tenanttabellen hebben workspace_id NOT NULL;
- policies gebruiken dezelfde workspacecontext;
- service-rolequeries worden gecentraliseerd en gelogd.

Dit is een grotere migratie en moet na de directe queryfixes worden uitgevoerd.

### PR 13 (code-PR 15) — Cost-guard naar databaseaggregatie

Vervang Pythonfull-scans door één databaseaggregaat per relevante periode, bijvoorbeeld:

```sql
SELECT COALESCE(SUM(cost_eur), 0)
FROM heatr_api_cost_log
WHERE workspace_id = $1
  AND created_at >= $2
  AND created_at < $3;
```

Index: `(workspace_id, created_at)`.

Bij zeer hoog volume kan later een rolluptabel worden toegevoegd. Begin niet direct met complexe counters als een goed geïndexeerde SQL-sum voldoende is.

### PR 14 (code-PR 16) — Globaal platformbudget

Naast workspacebudgetten: dagelijks platformbudget; maandelijks platformbudget; waarschuwing op 70%, 85% en 95%; harde stop op 100%; onderscheid tussen kritieke en niet-kritieke AI-calls.

**Degradatiemodel bij budgetdruk:** (1) cache gebruiken; (2) goedkoper model; (3) deterministische fallback; (4) niet-kritieke verrijking uitstellen; (5) pas daarna hard blokkeren. Compliance- en suppressiebeslissingen blijven zonder AI functioneren.

### PR 15 (code-PR 17) — Resumable enrichment

- steps_completed na iedere stap opslaan;
- outputs per stap idempotent bewaren;
- restart slaat voltooide stappen over;
- spent_eur per job/lead persisteren;
- retrybudget blijft over restarts heen behouden;
- stappen hebben afzonderlijke idempotencykeys.

### Acceptatiecriteria fase 4

- tenant B kan geen data van tenant A lezen of muteren via bekende paden;
- ontbrekende workspacecontext faalt gesloten;
- cost-guard doet geen full scans meer per AI-call;
- globaal platformbudget bestaat;
- enrichmentretry herhaalt geen voltooide stappen.

---

## 9. Fase 5 — Lifecycle v2 en structurele architectuur

Pas nadat fasen 1 tot en met 4 stabiel zijn, wordt de lifecycle structureel herbouwd.

### 9.1 Twee-assen state machine

**As 1 — Lead-lifecycle:** new; enriching; enrichment_failed; ready; customer; manual_hold; disqualified; suppressed; email_dead; forgotten.

**As 2 — Campaign enrollment:** enrolled; sending; waiting_reply; engaged; handoff; completed_no_response; cooldown; recontact_ready; stopped; failed.

**Message-level state** — niet op lead of enrollment, maar in message/eventledger: intended; accepted; sent; delivered; soft_bounced; hard_bounced; replied; failed.

### 9.2 Eén state-eigenaar

Maak een centrale lifecyclemodule. Andere modules mogen geen losse statusvelden rechtstreeks schrijven; zij sturen commands of events: SuppressLead; CreateEnrollment; RegisterReply; CompleteCampaign; MarkEmailDead; PlaceManualHold. De lifecyclemodule valideert de transitie en schrijft historie.

### 9.3 Templateversioning

Introduceer campaign_templates; campaign_template_versions; immutable templateversies; FK vanuit enrollment; vastlegging van prompt/model/contentversie. Hiermee kan later worden vastgesteld welke exacte sequence een lead heeft ontvangen.

### 9.4 Nurture en recontact

Pas bouwen wanneer: enrollments betrouwbaar bestaan; completion correct wordt opgeslagen; snapshots worden gemaakt; suppression absoluut is; eventordering werkt.

Recontact mag uitsluitend starten wanneer: vorige enrollment terminal is; cooldown is verstreken; lead niet suppressed/email-dead is; er een geldig signaal bestaat; de vorige outreachsnapshot beschikbaar is; de nieuwe dienst/campagne expliciet verschilt of opnieuw benaderen toegestaan is.

---

## 10. PR-volgorde

| PR | Onderwerp | Afhankelijkheid |
|---|---|---|
| 1 | Logging, send-path-inventaris en baseline | Geen |
| 2 | Prefixfix outbound ledger | Samen met PR 3 releasen |
| 3 | UNIQUE-ledger + atomische reservering | PR 2 |
| 4 | Centrale kill-switch | PR 2/3 |
| 5 | Compliance-hotfix bounce/unsubscribe | PR 4 bij voorkeur gereed |
| 6 | Forget-flow | PR 5 |
| 7 | Cross-workspace suppression | PR 5/6 |
| 8 | ADR send-ownership | Voor campaigntracking |
| 9 | Tracking-only enrollments | PR 8 |
| 10 | Webhook-eventledger | PR 9 |
| 11 | SendingGuard herbedraden | PR 9/10 |
| 12 | Workspace fail-closed | Geen harde DB-afhankelijkheid |
| 13 | Cross-tenant queryfixes | PR 12 |
| 14 | RLS/backstop | PR 12/13 |
| 15 | Cost-guard SQL-aggregatie | Geen |
| 16 | Globaal budget | PR 15 |
| 17 | Resumable enrichment | PR 15 |
| 18+ | Twee-assen lifecycle en templates | Na stabilisatie |

PR 2 en PR 3 worden technisch gescheiden voor reviewbaarheid, maar atomair uitgerold. PR 9, PR 10 en PR 11 mogen pas worden geactiveerd nadat het Warmr-owned sendmodel expliciet is vastgelegd.

---

## 11. Rolloutstrategie

**Omgevingen** — iedere risicovolle wijziging doorloopt: (1) lokale tests; (2) CI; (3) staging met gesimuleerde Warmr; (4) shadow mode; (5) beperkte productiecanary; (6) volledige productie.

**Shadow mode** — voor nieuwe suppression- en SendingGuardregels: eerst beslissing berekenen; nog niet blokkeren; verschil met huidige beslissing loggen; false positives beoordelen; daarna enforcement activeren. Uitzondering: evidente unsubscribe en hard bounce mogen direct fail-closed worden afgedwongen.

**Canary** — start met: één interne workspace; één mailbox; beperkt leadsegment; geen automatische follow-ups vanuit Heatr.

**Rollback** — elke migratie bevat: down- of compensatiescript; featureflag voor nieuwe code; mogelijkheid om nieuwe worker uit te zetten; geen destructieve kolomverwijdering in dezelfde release als introductie van het nieuwe model.

**Expand-and-contract:** (1) nieuwe tabel/kolom toevoegen; (2) dual-write; (3) backfill; (4) reads omschakelen; (5) oude velden pas later verwijderen.

---

## 12. Teststrategie

**Unit-tests:** suppressionbeslissingen; statustransities; idempotencykeygeneratie; kill-switchbeleid; eventordering; templateversionselectie.

**Integratietests** (echte testdatabase): UNIQUE-races; CAS-claims; RLS; workspacefilters; migraties; eventdeduplicatie; transactionele forget-flow.

**Concurrencytests:** twee workers, één outboundkey; twee workers, één enrollment; webhook tweemaal; completion en reply in omgekeerde volgorde; suppression terwijl send in_flight is; kill-switch tijdens workerloop.

**Contracttests Warmr:** idempotent campaign create; idempotent lead enroll; timeout na acceptatie; partial bulkfailure; webhook redelivery; unknown lead; out-of-order event; hard bounce; unsubscribe; campaign completion.

**Migratietests:** lege database; realistische bestaande data; dubbele records; rerun; rollback of compensatie; lockduur; productie-indexbouw waar nodig met CONCURRENTLY.

---

## 13. Operationele metrics en alerts

**Outbound:** outbound_dispatch_total; outbound_deduplicated_total; outbound_failed_total; outbound_in_flight_age; kill_switch_block_total.

**Compliance:** suppression_block_total; unsubscribe_event_total; hard_bounce_event_total; forget_failed_total; suppressed_send_attempt_total.

**Warmr:** commandtimeout; partial bulkfailure; webhooklag; duplicate event; dead-letter count; unknown object count.

**Queue:** pending/running/dead-letter per workspace; oudste pending job; jobs per workspace; workerconcurrency; reaper count.

**Kosten:** spend per workspace; platform spend; cost per enriched lead; repeated AI-step count; cache hit rate.

**Alerts — direct alarmeren bij:** send ondanks suppression; dubbele idempotencykey met twee externe sends; ledger-writefout; kill-switch uit maar send uitgevoerd; hard-bounce-rate boven drempel; dead-lettergroei; workspacecontext ontbreekt; platformbudget boven 85%; oldest pending boven afgesproken grens.

---

## 14. Definition of Done per fase

Een fase is niet afgerond omdat code is gemerged. Een fase is pas gereed wanneer:

1. migraties succesvol op staging en productie zijn toegepast;
2. tests groen zijn;
3. metrics zichtbaar zijn;
4. relevante alerts actief zijn;
5. runbook is bijgewerkt;
6. rollback is getest of aantoonbaar mogelijk is;
7. documentatie en ADR's zijn bijgewerkt;
8. minimaal één productiecanary succesvol is verlopen;
9. geen nieuwe silent exceptions zijn geïntroduceerd;
10. de oorspronkelijke auditbevinding aantoonbaar niet meer reproduceerbaar is.

---

## 15. Beslispunten

**Besluit 1 — Wie beheert de sequence?**
Aanbeveling: Warmr beheert voorlopig de dripsequence. Heatr houdt tracking en compliance bij.

**Besluit 2 — Is suppression platformbreed?**
Aanbeveling: ja. Een unsubscribe, complaint, forget of hard bounce blokkeert het e-mailadres platformbreed, zonder tenantdetails bloot te geven.

**Besluit 3 — Fail-open of fail-closed?**
Aanbeveling: compliance unavailable → fail-closed; ledger unavailable → fail-closed; kill-switch unavailable → fail-closed; analytics unavailable → fail-open; niet-kritieke enrichment unavailable → retry/degrade; Warmr capacity unavailable → prospectsend blokkeren.

**Besluit 4 — Service-role-strategie**
Aanbeveling: service role uitsluitend in centrale repositories en workers. User-facing requests zo veel mogelijk uitvoeren met een tenant-scoped databasecontext.

---

## 16. Eerste concrete werkpakket

Het eerste werkpakket moet klein blijven en geen campaignownership wijzigen.

### Werkpakket A — Outbound Safety Foundation

**Scope:**
1. send-path-inventaris;
2. structured outboundlogging;
3. "outbound_log" toevoegen aan _HEATR_TABLES;
4. duplicate-data-check;
5. UNIQUE(workspace_id, idempotency_key);
6. atomische in_flight-reservering;
7. centrale prospect-kill-switch;
8. concurrency- en timeouttests;
9. runbook en metrics.

**Buiten scope:** lead_campaign_history vullen; sequence-engine wijzigen; nurture; recontact; state-machineherbouw; multi-tenancyherbouw; Warmr-contract volledig vervangen.

**Succescriteria:**
- Dezelfde logical send kan onder concurrency maximaal één keer ownership verkrijgen.
- Iedere externe send heeft een ledgerrecord.
- Een ledgerfout blokkeert verzending.
- De master-switch stopt alle prospectgerichte sendpaden.
- De huidige Warmr-owned drip blijft functioneel onveranderd.
- Geen nieuwe pending-enrollmentrecords worden aangemaakt.

---

## 17. Samenvatting van de aanbevolen volgorde

1. Maak outbound zichtbaar.
2. Repareer ledgerprefix en database-idempotency als één release.
3. Plaats de kill-switch centraal.
4. Blokkeer unsubscribe en hard bounce overal.
5. Maak forget atomisch en werkelijk idempotent.
6. Introduceer platformbrede suppression.
7. Leg Warmr-owned sequence-ownership vast.
8. Maak tracking-only enrollments en een webhook-eventledger.
9. Herbouw SendingGuard op feitelijke send- en bouncegegevens.
10. Dwing workspacecontext af en sluit concrete tenantlekken.
11. Maak cost-control schaalbaar en platformbreed.
12. Bouw daarna pas de twee-assen lifecycle, templates, nurture en recontact.

**De belangrijkste discipline:** de campaigntrackingfix mag niet vooruitlopen op het sendownership. Een pending-enrollment naast Warmr's server-side drip is geen tussenoplossing, maar een nieuwe dubbele-sendmachine. Eerst moeten idempotency, suppression en ownership vaststaan; daarna kan de lifecycle veilig worden aangesloten.
