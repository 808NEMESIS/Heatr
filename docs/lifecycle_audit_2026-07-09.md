# Heatr — Lead Lifecycle Audit & Enterprise-herontwerp

**Datum:** 2026-07-09
**Methode:** drie onafhankelijke code-tracersessies (verzendpad · statusmodel · recontact/nurture/templates), elk verankerd in `file:line`. Aangevuld met web-research (HubSpot-lifecycle, RevOps-progressiecriteria, re-engagement-cadans, AI send-time/reply-scoring).
**Aanname-beleid:** geen — alleen de werkelijke implementatie. Geen code gewijzigd; dit is diagnose + plan.

---

## Kernoordeel

**Heatr heeft een lead-*begin*, geen lead-*lifecycle*.** Een lead wordt ontdekt, verrijkt, gescoord en één keer naar Warmr geduwd — daarna verdwijnt hij in een gat.

De oorzaak is structureel: het live verzendpad (`/campaigns/launch`) schrijft **nooit** een enrollment-rij naar `lead_campaign_history`, terwijl bijna elke dedup-, cooldown- en recontact-check juist op die tabel leunt. Gevolg: dubbele campagnes zijn mogelijk, de 90-dagen-cooldown is blind, de signaal-gestuurde recontact is architectonisch dood, "nurture" bestaat niet (0 code-treffers), en er zijn vijf templates die alle vijf dezelfde koude 3-mail-flow zijn. Bovendien draaien er **vier ongesynchroniseerde statussystemen** op één lead die op elk moment onderling kunnen tegenspreken.

---

## 01 — De huidige lifecycle: volledig flowdiagram

```
[1] SCRAPING ───────────────► companies_raw
    gate: is_domain_known()                          utils/deduplicator.py:47

[2] LEAD-CREATIE ───────────► leads  (status=discovered)
    gate: is_email_known() · is_duplicate_entity()   deduplicator.py:80,299

[3] ENRICHMENT ─────────────► email + contact + personalisatie  (status=enriched)
    worker: job_queue/enrichment_queue.py:312   gate: filter_launchable_leads

[4] SCORING ────────────────► leads.score  (push_eligible berekend…)
    gate: score>=65 · icp_match>=0.6 · compliance_check   lead_scoring.py:192-205
    ↳ 'scored' en 'qualified' status worden NOOIT geschreven — pipeline springt enriched→pushed

    ┌─────────────────────────────────────────────────────────────────┐
    │  MODEL A — "hand off aan Warmr"   (dit is het LIVE pad)          │
    └─────────────────────────────────────────────────────────────────┘
[5] CAMPAIGN LAUNCH ────────► POST /campaigns/launch   api/main.py:1449 (service-key only)
    kill-switch: ENABLE_CAMPAIGN_SENDS
    gate: filter_launchable_leads · campaign_cooldown_block() ← leest LEGE tabel → blokt nooit
          personalisatie-gate · dispatcher-idempotency (op campagnenaam)
    ► create_campaign(sequence_steps=…) + push_leads_bulk   Warmr neemt sending over
    ► schrijft heatr_leads-boekhouding + audit-rij in 'campaigns'
    ✗ schrijft GEEN lead_campaign_history-rij  ◄── DE BREUK
                     │
                     ▼  (Warmr dript mail 1→2→3 zelf, buiten Heatr's zicht)
[6] REPLY / BOUNCE ─────────► POST /webhooks/warmr   api/main.py:3102 (HMAC fail-closed)
    interested → crm_stage=gereageerd        (zet leads.status NIET)
    replied    → crm_stage=beantwoord
    bounced/unsub → email_status + history    (zet leads.status NIET → compliance hangt op 2e gate)
    campaign.completed → history.status='no_response'  (is_active NIET false; leads.status NIET gezet)
                     │
    reply_classifier.process_reply  integrations/reply_classifier.py:240
    not_interested → status=disqualified + next_contact_after=+365d
    not_now/auto_reply → paused   (hervat NOOIT automatisch)

[7] GEEN REPLY → COOLDOWN → RECONTACT
    get_recontact_ready vereist status='no_response' (recontact_signals.py:194)
    → op live pad nooit vervuld → signaal-recontact vuurt NOOIT

[8] EINDE?  terminale leads.status: unsubscribed · disqualified · forgotten
    GEEN 'archived' / 'closed'. "Geen reply" heeft geen eindstaat → lead circuleert eeuwig.

    ┌─────────────────────────────────────────────────────────────────┐
    │  MODEL B — "Heatr-owned drip"   (ONTWORPEN, staat leeg, draait   │
    │  op een tabel zonder enkele insert → verwerkt eeuwig 0 rijen)    │
    └─────────────────────────────────────────────────────────────────┘
    n8n /15min → GET /sequences/due-sends → process_due_send()
    leest lead_campaign_history WHERE status='pending' AND is_active   ← altijd leeg
    timing: next_send_at = now + max(delay_days, MIN_WAIT_DAYS=2)   seq_engine.py:498
    _complete_sequence → status='no_response' + next_contact_after=+90d  ← draait nooit
```

### De ene breuk die bijna alles verklaart
Een grep over de hele repo (buiten tests) vindt **nul** `insert`/`upsert` op `lead_campaign_history` — alleen `UPDATE`s. `/campaigns/launch` pusht leads naar Warmr maar registreert de enrollment nooit lokaal. Daardoor zijn `is_lead_in_active_campaign()`, `campaign_cooldown_block()` en de hele `SendingGuard`-cooldown structureel blind op het enige pad dat écht mails verstuurt. Model B (de complete n8n due-send-loop) verwerkt intussen eeuwig een lege tabel. → Fix = **C1** in sectie 9.

---

## 02 — Logische fouten (geordend op ernst)

### F1 · KRITIEK — Dezelfde lead kan twee keer dezelfde boodschap krijgen
Omdat de app-niveau dedup blind is, is de enige echte rem de dispatcher-idempotencykey. Die bevat de **campagnenaam**: `campaign-create:{naam}:{template}:{ids_hash}`. Zelfde leads + zelfde template, andere naam → nieuwe key → nieuwe Warmr-campagne → leads opnieuw gepusht. Bovendien gebruiken `/leads/send-to-warmr` (`warmr-bulk:`) en `/campaigns/launch` (`campaign-push:`) verschillende namespaces op mogelijk verschillende campaign_id.
*main.py:1600,1628,646 · deduplicator.py:113,147 — laatste vangnet is Warmr's eigen e-mail-dedup.*

### F2 · KRITIEK — Launch registreert geen enrollment → alle campagne-dedup uit
De wortel van F1. `/campaigns/launch` (main.py:1449-1712) insert nooit een `lead_campaign_history`-rij. `is_lead_in_active_campaign()` en `campaign_cooldown_block()` retourneren daarom altijd "niets". SendingGuard-stap 5/5b, de cooldown-filters én check 4+5 van `should_allow_warmr_push` blokkeren nooit.
*sending_guard.py:86-99 · deduplicator.py:250,255.*

### F3 · HOOG — Twee cooldown-kolommen die elkaar niet zien
`next_contact_after` (geschreven door engine + classifier, gelezen door SendingGuard) vs `recontact_after` (alleen geschreven door `/leads/bulk-status` "recontact_later", gelezen door CRM-UI). SendingGuard kijkt niet naar `recontact_after` — een handmatig op recontact_later gezette lead wordt bij relaunch tóch meteen gestuurd.
*sending_guard.py:78 · main.py:4316 · lead_activity.py:212.*

### F4 · HOOG — De recontact-loop is dood op het live pad
`get_recontact_ready` vereist `leads.status='no_response'`. Die wordt alleen gezet in `_complete_sequence` (Model B) — dat nooit draait. Webhook `campaign.completed` zet wél de history-status maar niet `leads.status`. Gelaunchte niet-reagerende leads bereiken dus nooit `no_response` → recontact vuurt nooit voor de grootste groep.
*recontact_signals.py:194 · seq_engine.py:553 · main.py:3190-3233.*

### F5 · HOOG — Signaal-recontact volledig dood: baseline wordt nooit opgeslagen
`save_outreach_snapshot()` wordt door geen enkele geautomatiseerde flow aangeroepen — alleen door een handmatig endpoint. Zonder baseline is `has_signal` altijd False en geeft `get_recontact_ready` altijd een lege lijst. n8n-workflow 09 is twee nodes: een GET en daarna niets. `suggested_opener_angle` bereikt de renderer nooit.
*recontact_signals.py:225-271 · deployment/n8n-workflows/09-recontact-suggestions.json.*

### F6 · HOOG — "Paused" sequences hervatten nooit automatisch
Out-of-office/"later" zet de sequence op `paused` (is_active=True). Niets flipt `paused→pending` terug: `wake_snoozed_leads` raakt alleen crm_stage, `reactivate_snoozed_tasks` alleen crm_tasks. Voor auto_reply wordt `next_send_at=until` gezet maar status blijft paused, dus de poller (filtert op status='pending') pakt hem nooit op.
*reply_classifier.py:366-375 · seq_engine.py:369,622,648 · main.py:934.*

### F7 · HOOG — campaign.completed sluit enrollment niet af → permanente vastloper (Model B)
Webhook `campaign.completed` roept niet `stop_all_sequences_for_lead` aan en zet is_active niet op false. Als Model B gevuld wordt, blijft de rij is_active=True → lead blokkeert elke toekomstige launch voorgoed.
*main.py:3190,3233.*

### F8 · MIDDEL — Scoring-gate en verzend-gate lopen uiteen: push_eligible genegeerd
`score_lead` berekent `push_eligible` incl. `icp_match>=0.6`, maar geen verzendpad leest dat veld. `/leads/send-to-warmr` checkt score>=65 maar niet ICP; `/campaigns/launch` checkt de score-drempel helemaal niet opnieuw. Een lead met icp_match=0.3 kan alsnog vertrekken.
*lead_scoring.py:190-206 · main.py:620,1478.*

### F9 · MIDDEL — Verkeerde-template-risico: geen assert dat template bij sector past
`_resolve_template_for_lead → pick_brug` kiest bij ontbrekende data een default-frame; `inject_variables`-fallbacks (`stad_of_sector → "jullie sector"`, `first_name → "daar"`) maskeren dat maar corrigeren de keuze niet. Een chiropractor kan een cosmetische-kliniek-frame krijgen. `pick_brug` negeert contact-historie volledig — weet niet of dit poging 1 of 5 is.
*main.py:1536 · seq_engine.py:279-309 · sequence_templates.py:579-615.*

### F10 · MIDDEL — Race in de due-poller: geen claim/lock, dedup op één fail-open tabel
`get_due_sends` selecteert zonder te claimen; `process_due_send` markeert pas ná de push. Twee overlappende n8n-ticks lezen dezelfde rij. De seq-send-idempotencykey vangt de dubbele push af — mits `heatr_outbound_log` bestaat. De dispatcher faalt bewust **open** als die tabel ontbreekt.
*seq_engine.py:352,450,504 · outbound_dispatcher.py:108-128.*

### Plus: statusveld-desyncs (zie sectie 3)
Webhook zet crm_stage maar niet leads.status (classifier andersom) → unsubscribe-blokkade hangt toevallig op de 2e gate; crm_stage verlaat 'ontdekt' nooit bij launch → analytics telt actief-benaderde leads als "niet-verstuurd"; `manual_status_override` wint altijd en verloopt nooit → later positief antwoord genegeerd.

---

## 03 — Alle leadstatussen: vier ongesynchroniseerde systemen

### As 1 · `leads.status` (Engelse pipeline — niet client-schrijfbaar)

| Status | Gezet door | Verlaat naar | Diagnose |
|---|---|---|---|
| `discovered` | lead_qualifier:207 · lead_import:354 | enriched / disqualified | gezond |
| `enriched` | enrichment_queue:312 | pushed_to_warmr | springt scored+qualified over |
| `scored` | — geen writer — | — | **dood: nooit geschreven** |
| `qualified` | — geen writer — | — | **dood: nooit geschreven** |
| `pushed_to_warmr` | warmr_client:360 | replied / no_response / disqualified… | gezond |
| `replied` | reply_classifier:307,319 | — nooit automatisch — | **dead-end: blijft eeuwig** |
| `snoozed` | reply_classifier:271 | — nooit — | **dead-end + desync met crm_stage** |
| `needs_review` | reply_classifier:296 | — nooit — | **dead-end** |
| `no_response` | seq_engine:553 (Model B) | recontact via cooldown | writer draait nooit live (F4) |
| `disqualified` | main:687 · reply_classifier:259 | terminaal | terminaal (BLOCKED) |
| `unsubscribed` | reply_classifier:246 | terminaal | terminaal — maar webhook zet 'm niet |
| `forgotten` | gdpr_manager:73 | terminaal | terminaal (GDPR) |

### As 2 · `leads.crm_stage` (NL kanban — vrij client-schrijfbaar, GEEN enum-constraint)

| Stage | Gezet door | Diagnose |
|---|---|---|
| `ontdekt` | default · wake_snoozed:633 | gezond |
| `later` | reply_classifier:274 | → ontdekt na snooze |
| `benaderd` | — geen writer — | **dood: launch verplaatst crm_stage niet → kanban liegt** |
| `gereageerd` | reply_classifier:308 · webhook:3156 | nooit exit |
| `beantwoord` | webhook:3164 | nooit exit |
| `afgesloten` | webhook:3182 | nooit exit |
| `verloren` | main:688 | nooit exit |
| `gewonnen` | main:3512 (/crm/deals) | enige echte sales-uitkomst |
| *willekeurig* | PATCH /leads/{id}:572 | **ongevalideerd → typefout maakt stille fantoom-stage** |

### As 3 · `lead_campaign_history.status` + `is_active`
Runtime: `pending ⇄ paused`, en `pending →` {`sequence_complete, replied, bounced, unsubscribed, stopped, blocked, error, no_response`} (alle terminaal, is_active=false). **Twee tegenstrijdige schemadefinities**: `supabase_schema.sql:416` (default `active`) vs `migrations/009:17` (default `pending`). `active`/`completed` worden nergens geschreven. `is_active` is het enige drift-vrije "in-flight"-signaal.

### As 4 · `manual_status_override` → `derive_status` (afgeleid, wint altijd)
9 waarden in `CRM_STATUSES` (lead_activity.py:33-43). Enige writer: `/leads/bulk-status`. `derive_status` laat de override altijd winnen; geen enkel systeem-pad ruimt 'm op. Operator zet `geen_interesse`, lead antwoordt later positief → status blijft hangen op `geen_interesse`.

### Ontbrekende statussen voor een volwassen lifecycle
- Geen `in_campaign`/`sending` op leads.status (moet uit join met is_active).
- Geen `customer`/`won`/`lost` op de Engelse pipeline (alleen crm_stage) — geen terminale sales-uitkomst.
- Geen re-entry-status: na cooldown gaan er weer mails uit terwijl label `no_response` blijft.
- Geen `bounced` op leads.status (alleen email_status).
- Geen `archived` — niet-reagerende, niet-unsub/DQ leads komen nooit in een eindstaat.

---

## 04 — Recontact-strategie: vandaag naïef, zo hoort het

**Vraag:** krijgt een eerder-benaderde lead een andere aanpak dan een verse lead? **Nee.** Een 90-dagen-timer vuurt exact dezelfde koude `pick_brug`-sequence opnieuw af. `pick_brug` leest alleen statische website-signalen en negeert elke historie (eerdere mails, replies, afwijzingen, aantal pogingen, verstreken tijd, nieuwe signalen).

### Herontwerp: recontact vertakt op de afsluitreden van het vorige contact

**Ingang A — nooit geopend / geen reactie**
- Trigger: sequence afgerond, 0 opens/replies, pogingen < 2
- Aanpak: volledig andere brug (ander pijnpunt/openingsbeeld), nooit template-hergebruik
- Cooldown: 60 dagen · Stop: na 2 koude cycli → `archived_cold`

**Ingang B — geopend, niet geantwoord**
- Trigger: opens > 0, replies = 0
- Aanpak: "ik zag je keek — 1 concrete vraag", verwijs naar eerdere mail, geen herhaling
- Cooldown: 30 dagen · AI: reply-probability bepaalt of hij terug de wachtrij in mag

**Ingang C — positief geweest, daarna koud (win-back)**
- Trigger: ooit replied/interested, geen deal, > 60 dagen stil
- Aanpak: "we spraken eerder over X — sindsdien Y veranderd", 3-touch win-back, niet koud
- Cooldown: 45 dagen · max 1 cyclus · mag mens-in-de-loop

**Ingang D — expliciet afgewezen**
- Trigger: disqualified via not_interested — **niet op een timer**
- Aanpak: alleen heropenen bij een écht nieuw signaal (nieuwe site, verhuizing, funding, vacature)
- Cooldown: 365d minimum én signaal verplicht

**Ingang E — signaal-gedreven (voor élke koude lead)**
Het bestaande `recontact_signals.py`-idee is juist, alleen nooit aangesloten. Herontwerp: sla bij elke launch een `outreach_snapshot` op (F5-fix), laat een wekelijkse job websites/ratings/vacatures/KvK opnieuw meten, en zet meetbaar-veranderde leads bovenaan de wachtrij met `suggested_opener_angle` als voorgevulde brug. Dit is het enige recontact-mechanisme dat Apollo/Instantly niet standaard hebben.

---

## 05 — Nurture: bestaat vandaag niet

Grep op `nurture`, `drip`, `educational`: **0 treffers**. Wat "nurture" heet is herhaalde koude outreach met een timer; mail 3 sluit de relatie zelfs expliciet af (break-up). Geen niet-verkopende touch, geen waarde-eerst-contact, geen educatief spoor.

### Herontwerp: een echt nurture-spoor naast het verkoopspoor

```
COLD ──(sequence uitgeput, geen reactie)──► NURTURE_SLOW
                                               │  maandelijkse waarde-touch:
                                               │  · mini-website-teardown uit hun eigen sector
                                               │  · "3 klinieken in {stad} deden dit"
                                               │  · seizoen: nieuwjaar/zomer-drukte-hook
INTERESTED ──(reply, geen deal)──► NURTURE_WARM
                                      │  2-wekelijks, persoonlijker, mens-zichtbaar
elke NURTURE-baan ──(gedrag: opent 2x / klikt / nieuw signaal)──► RE-ENGAGE ──► terug naar COLD-sequence (warme opener)
                  └──(tijd: 6 mnd geen interactie)──────────────► ARCHIVED_NURTURE
```

- **Promotie nurture→verkoop:** opent 2 opeenvolgende mails · klikt teardown-link · recontact-signaal vuurt · reply (hoe kort ook)
- **Regressie verkoop→nurture:** sequence uitgeput zonder reactie · "nu niet, later" · positief geweest maar deal stokte > 60d
- **Uitgang nurture→archief:** 6 mnd geen open/klik/reply · 2 volledige cycli zonder promotie · bounce/unsub

**Waarom dit voor Aerys anders werkt:** de Trojan-Horse-strategie (gratis review → websitebouw → AI-audit) is inherent een nurture-verhaal — de waarde-touch *is* het product-in-het-klein. Elke nurture-mail kan een échte mini-teardown van hun eigen site zijn (data die Heatr al genereert). Geen generiek platform kan dat leveren.

---

## 06 — Template-model: vijf keer dezelfde koude mail

5 keys (`v1_cosmetisch_audit`, `v1_alternatieve_zorg` [deprecated], `v3_1_website`, `v3_1_workflow`, `v3_1_ai_audit`). Alle vijf structureel identiek: koude 3-mail-flow op [0, 3-4, 5-6], eindigend in break-up. Variatie zit alleen in het **onderwerp**, niet in de **relatiefase**.

| Template-type | Wanneer | Vandaag? |
|---|---|---|
| Eerste contact | koude lead, poging 1 | aanwezig (×5 onderwerpen) |
| Follow-up (2e/3e) | stap 2-3 in sequence | aanwezig als steps, geen aparte kunst |
| Laatste poging / break-up | einde sequence | aanwezig (mail 3) |
| **Re-engagement** | na cooldown, was koud | **ontbreekt** |
| **Win-back** | was interested, deal stokte | **ontbreekt** |
| **Signaal-getriggerd** | nieuwe site/vacature/funding | **ontbreekt** (opener_angle bereikt renderer niet) |
| **Nurture / educatief** | slow-baan, waarde-eerst | **ontbreekt volledig** |
| **Seizoensgebonden** | nieuwjaar / zomerdrukte | **ontbreekt** |
| **Referral** | tevreden klant / intro | **ontbreekt** |
| **Bestaande relatie / upsell** | website-klant → AI-audit | **ontbreekt (kern van de Aerys-ladder!)** |

**Het model:** `pick_brug` uitbreiden van "kies onderwerp uit website-data" naar "kies template uit (relatiefase, brug, pogingnr)". Relatiefase komt uit de state machine (sectie 8), brug uit website-intelligence, pogingnr uit de dan-wél-geschreven `lead_campaign_history`. Zo krijgt een chiropractor bij poging 3 gegarandeerd een andere mail dan bij poging 1 — vandaag onmogelijk (F9).

---

## 07 — AI-personalisatie: waar het écht rendement geeft

1. **Template-selectie** — Claude Haiku kiest relatiefase + brug + toon met reden, i.p.v. statische drempel-logica. Hook: `_resolve_template_for_lead` (main.py:1292).
2. **Send-time optimization** — beste venster per lead/sector leren uit open-data die via de webhook al binnenkomt (~26% meer opens in de literatuur). Hook: `next_send_at` (seq_engine.py:498).
3. **Reply-probability** — Lavender-achtige score die zwakke leads/mails uit de wachtrij houdt; onder een drempel → naar nurture i.p.v. koud. Hook: nieuwe gate in launch, naast push_eligible (F8).
4. **Spam-probability** — Claude checkt elke gegenereerde mail op spam-triggers/toon vóór hij naar Warmr gaat; harde block bij hoog risico beschermt de warme inbox. Hook: `review_email_generator` + personalisatie-gate.
5. **Recontact-worthiness** — AI beoordeelt of een gedetecteerd signaal écht een aanleiding is; filtert ruis uit de signaal-wachtrij. Hook: `detect_recontact_signals` (recontact_signals.py:39).

**Kostenkader:** Haiku voor selectie/scoring (bulk), Sonnet alleen voor diepte. STO en reply-probability zijn grotendeels lokale modellen op webhook-data — nauwelijks API-kosten. Alles achter de bestaande `cost_guard`/accumulator. Blijft binnen ~€10-15/mnd.

---

## 08 — Enterprise lifecycle: één state machine

Vervang de vier tegensprekende statusassen door **één** canonieke `leads.status` als bron van waarheid.

```
NEW ──enrichment gestart──► ENRICHING ──compleet+gescoord──► READY
                                │                                │ push_eligible?
                                └──enrich faalt / DQ──► DISQUALIFIED
                                                                 ▼
READY ──launch (schrijft enrollment!)──► CONTACTING ──mail verstuurd──► WAITING_REPLY
                                             │                             │ geen reply, seq klaar
                               reply binnen? │                             ▼
                                             ▼                         FOLLOW_UP ──stappen op──► WAITING_REPLY
                                         ENGAGED                           │ uitgeput
                          positief │ negatief                             ▼
                                   ▼         ▼                        COOLDOWN ──timer/signaal──► RECONTACT_READY
                              CUSTOMER   DISQUALIFIED                     │ geen signaal, 2e cyclus │ nieuwe brug
                                                                         ▼                        ▼
                                                                     NURTURE ◄──promotie── CONTACTING (warm)
                                                                         │ 6 mnd stil │ bounce/unsub
                                                                         ▼
                                                                     ARCHIVED   UNSUBSCRIBED
```

### Toegestane transities (met trigger-event)
- NEW→ENRICHING — enrichment-job geclaimd
- ENRICHING→READY — completeness + score≥65 + icp≥0.6
- READY→CONTACTING — launch, **insert enrollment-rij**
- CONTACTING→WAITING_REPLY — mail bevestigd verzonden
- WAITING_REPLY→ENGAGED — reply-webhook
- WAITING_REPLY→FOLLOW_UP — volgende stap due
- FOLLOW_UP→COOLDOWN — sequence uitgeput
- COOLDOWN→RECONTACT_READY — timer óf signaal
- RECONTACT_READY→CONTACTING — nieuwe brug gekozen
- COOLDOWN→NURTURE — 2e cyclus zonder signaal
- NURTURE→CONTACTING — gedrags-promotie
- ENGAGED→CUSTOMER — deal gewonnen
- elke→UNSUBSCRIBED/DISQUALIFIED — hard stop

### Verboden transities (moeten hard falen)
- READY→CONTACTING **zonder enrollment** — precies de huidige F2-breuk
- *→CONTACTING terwijl is_active elders — dubbele campagne (F1)
- UNSUBSCRIBED/DISQUALIFIED→wat dan ook — terminaal, alleen GDPR-forget
- CUSTOMER→CONTACTING — geen koude outreach naar klanten (upsell = apart spoor)
- COOLDOWN→CONTACTING **zonder nieuwe brug** — verbiedt "zelfde koude mail opnieuw"
- override die reply overschrijft — een binnenkomende reply heft altijd een handmatige status op
- enige stap zonder is_active-check — geen send zonder centrale in-flight-gate

### Migratiepad (niet big-bang)
1. Maak `leads.status` de bron van waarheid; `crm_stage` wordt een **projectie** (afgeleid, niet apart geschreven) → doodt de desyncs van sectie 3.
2. Dwing de enrollment-insert af (F2) zodat de is_active-gate echt werkt.
3. Voeg ontbrekende toestanden toe (CUSTOMER, ARCHIVED, NURTURE, RECONTACT_READY). Elke stap los testbaar.

---

## 09 — Concrete verbeteringen (geprioriteerd)

**Uitvoervolgorde:** C1 → C2 (deblokkeren alles) → H2 (statusbron) → H1/H3/H4 → M-reeks → L-reeks.

---

### C1 · KRITIEK — Launch registreert de enrollment lokaal · complexiteit M
- **Probleem:** `/campaigns/launch` pusht naar Warmr maar insert geen `lead_campaign_history`-rij. Alle dedup/cooldown/recontact hangt aan die tabel (F2, F4).
- **Impact:** dubbele campagnes mogelijk, cooldown blind, recontact dood, "in-flight" onmeetbaar. Enkele oorzaak achter F1, F2, F4 en halve F7.
- **Oplossing:** bij succesvolle push per lead een enrollment-rij inserten (`status='pending'`, `is_active=True`, campaign_id, frozen `sequence_steps`, `next_send_at`). Idempotent op (lead_id, campaign_id).
- **Bestanden:** api/main.py:1631-1712 · integrations/warmr_client.py:357 · migrations/009_lead_campaign_history.sql
- **Stappen:** (1) één `enroll_leads()` helper, idempotent via unique index lead_id+campaign_id. (2) aanroepen direct na `push_leads_bulk` succes, in dezelfde scope als de heatr_leads-boekhouding. (3) verifieer dat `is_lead_in_active_campaign`/`campaign_cooldown_block` nu echt raken (unit-test met/zonder rij). (4) geen backfill nodig.

### C2 · KRITIEK — Idempotency op lead-identiteit, niet op campagnenaam · complexiteit S
- **Probleem:** dispatcher-key bevat de campagnenaam; zelfde leads onder andere naam → nieuwe push (F1). Losse namespaces tussen endpoints.
- **Impact:** dubbele launch (of via beide endpoints) stuurt dezelfde persoon twee keer dezelfde mail — reputatie + spam-risico op de warme inbox.
- **Oplossing:** key baseren op `(lead_id, template, dag-bucket)` i.p.v. campagnenaam, gedeeld tussen beide endpoints. C1-enrollment als tweede DB-slot.
- **Bestanden:** api/main.py:1600,1628,646 · utils/outbound_dispatcher.py
- **Stappen:** (1) vervang naam in create/push-key door lead-set-hash + template + dagbucket. (2) unificeer namespace `warmr-bulk:`/`campaign-push:`. (3) C1-enrollment-uniekheid als tweede barrière.

### H1 · HOOG — Eén cooldown-bron: consolideer next_contact_after & recontact_after · complexiteit M
- **Probleem:** twee kolommen voor "wanneer opnieuw"; SendingGuard ziet de handmatige niet (F3).
- **Impact:** handmatig op recontact_later gezette leads worden tóch direct gestuurd bij relaunch; UI en verzendlogica oneens over cooldown.
- **Oplossing:** `next_contact_after` canoniek. `/leads/bulk-status` schrijft die kolom; `recontact_after` wordt read-only alias of weg-gemigreerd.
- **Bestanden:** sending_guard.py:78 · api/main.py:4316 · utils/lead_activity.py:212 · migrations/018
- **Stappen:** (1) SendingGuard → één kolom, beide schrijvers daarheen. (2) data-migratie max(next_contact_after, recontact_after). (3) UI-teller op dezelfde kolom.

### H2 · HOOG — leads.status = bron van waarheid; crm_stage wordt projectie · complexiteit L
- **Probleem:** vier ongesynchroniseerde statusassen; webhook/classifier raken verschillende velden; dode/dead-end statussen (sectie 3).
- **Impact:** compliance-blokkade hangt op toevallige 2e gate; analytics telt actief-benaderde leads als niet-verstuurd; positieve reply na override genegeerd.
- **Oplossing:** implementeer de state machine (sectie 8) op `leads.status`. `crm_stage` afgeleid. Reply heft override altijd op.
- **Bestanden:** reply_classifier.py:240-320 · api/main.py:3155-3235 · utils/lead_activity.py · enrichment_check.py:35
- **Stappen:** (1) status-enum + toegestane transities als één module met `transition()`-guard. (2) webhook én classifier via die guard (geen directe writes). (3) CHECK-constraint; dode statussen weg; crm_stage als afleiding. (4) binnenkomende reply wist de override.

### H3 · HOOG — Activeer signaal-recontact: schrijf de snapshot bij launch · complexiteit M
- **Probleem:** `save_outreach_snapshot` nooit automatisch aangeroepen → geen baseline → signalen vuren nooit (F5).
- **Impact:** het enige recontact-mechanisme dat Aerys onderscheidt is volledig dood; n8n-09 is dead end.
- **Oplossing:** roep `save_outreach_snapshot` aan in de C1-enrollmentstap; bouw de wekelijkse detect-job af; wire `suggested_opener_angle` naar de renderer.
- **Bestanden:** recontact_signals.py:172-271 · deployment/n8n-workflows/09-recontact-suggestions.json · seq_engine template-render
- **Stappen:** (1) snapshot bij launch (naast C1). (2) koppel `no_response`-status correct (afh. van H2) zodat get_recontact_ready leads vindt. (3) breid n8n-09 uit: detect → AI-worthiness → wachtrij met voorgevulde opener.

### H4 · HOOG — Paused hervat automatisch + campaign.completed sluit af · complexiteit S
- **Probleem:** paused hervat nooit (F6); campaign.completed zet is_active niet false (F7).
- **Impact:** out-of-office-leads blijven permanent hangen en blokkeren relaunch; voltooide campagnes laten de lead als in-flight staan.
- **Oplossing:** wake-job flipt `paused→pending` zodra `next_send_at<=now`; webhook `campaign.completed` roept `stop_all_sequences_for_lead` aan.
- **Bestanden:** seq_engine.py:369,622 · reply_classifier.py:366-375 · api/main.py:3190-3233
- **Stappen:** (1) tak in due-poller die paused-rijen met verstreken next_send_at naar pending zet. (2) campaign.completed sluit enrollment netjes af.

### M1 · MIDDEL — Verzendpaden lezen push_eligible i.p.v. losse drempels · complexiteit S
- **Probleem:** push_eligible (incl. icp_match) genegeerd; launch checkt score-drempel niet opnieuw (F8).
- **Impact:** off-ICP-leads (icp_match 0.3) kunnen vertrekken — verspilde sends, slechtere respons.
- **Oplossing:** één verzend-gate die `push_eligible + push_block_reasons` leest, gedeeld door beide endpoints.
- **Bestanden:** lead_scoring.py:190-206 · api/main.py:620,1478
- **Stappen:** (1) centraliseer gate in SendingGuard. (2) launch én send-to-warmr roepen die aan; log block_reasons.

### M2 · MIDDEL — Template-selectie op relatiefase (bouw de matrix uit sectie 6) · complexiteit L
- **Probleem:** 5 identieke koude templates; pick_brug negeert historie; geen re-engagement/win-back/nurture/signaal-template (F9).
- **Impact:** 4e poging identiek aan 1e; recontact voelt als spam; Aerys-ladder heeft geen upsell-template.
- **Oplossing:** breid pick_brug uit naar (relatiefase, brug, pogingnr) + voeg ontbrekende types toe. Fase uit H2, pogingnr uit C1.
- **Bestanden:** config/sequence_templates.py:579-721 · api/main.py:1292 · seq_engine render
- **Stappen:** (1) voeg templatetypes toe. (2) herschrijf pick_brug met fase+pogingnr als sleutel. (3) optioneel AI-selectie (sectie 7.1).

### M3 · MIDDEL — Race-hardening op de due-poller + fail-closed dedup · complexiteit M
- **Probleem:** get_due_sends claimt niet; dedup op één fail-open tabel (F10).
- **Impact:** zodra Model B live gaat kan dezelfde stap dubbel verzonden worden als heatr_outbound_log ontbreekt.
- **Oplossing:** app-niveau claim (status→`sending` conditioneel) vóór de push; dispatcher fail-closed als log-tabel weg is.
- **Bestanden:** seq_engine.py:352,450,504 · outbound_dispatcher.py:108-128
- **Stappen:** (1) atomische claim `UPDATE … SET status='sending' WHERE status='pending'`, doorgaan bij rowcount 1. (2) dispatcher fail-closed voor live pad.

### L1 · LAAG — Opruimen: dode statussen, CHECK-constraints, kanban-benaderd · complexiteit S
- **Probleem:** dode statussen (scored, qualified, paused-job, active/completed-history), ongevalideerde crm_stage, ontbrekende 'benaderd'-writer.
- **Oplossing:** verwijder/documenteer dode waarden, CHECK-constraints, laat launch crm_stage→'benaderd' zetten (of afleiden via H2).
- **Bestanden:** supabase_schema.sql:129,301,416 · api/main.py:282,572,1860

### L2 · LAAG — Send-time optimization + reply/spam-scoring (AI-laag) · complexiteit L
- **Probleem:** geen timing-optimalisatie, geen pre-send reply/spam-score (sectie 7.2-4).
- **Oplossing:** leer send-vensters uit webhook-open-data; Haiku-score voor reply-kans en spam-risico als gate/sortering.
- **Bestanden:** seq_engine.py:498 · review_email_generator.py · nieuwe scoring-module

---

## Benchmark — voorbij HubSpot, Apollo, Instantly

**Wat de grote platformen doen:** HubSpot draait een 7-fasen lifecycle met expliciete progressie-/automatiserings-/regressie-criteria per fase. Apollo & Instantly vertakken sequences op reply-gedrag (geopend/geklikt/geantwoord) + A/B op openers. Lavender-achtige tools scoren reply-waarschijnlijkheid vóór verzenden; STO levert ~26% meer opens. Win-back is standaard een 3-touch sequence met relatie-erkenning.

**Waar Heatr uniek kan winnen** — twee dingen die generieke platformen structureel *niet* hebben:
1. **Website-intelligence als nurture-content** — elke waarde-touch kan een échte mini-teardown van de eigen site van de prospect zijn (data die Heatr al genereert).
2. **Verandering-in-de-wereld-detectie** — het `recontact_signals`-idee dat een lead heropent zodra zijn website/rating/vacatures meetbaar veranderen.

Beide zitten al half in de code; ze zijn alleen niet aangesloten (H3). Zodra de fundamenten (C1, C2, H2) staan, is dit precies het onderscheid dat Aerys' Trojan-Horse-strategie tot een lifecycle maakt in plaats van een lijst.
