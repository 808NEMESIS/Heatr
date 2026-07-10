# Heatr — Architecture Validation & Future-State Audit (v2)

**Datum:** 2026-07-09 (vervangt de eerdere v2-versie van dezelfde dag)
**Rol:** tweede, volledig onafhankelijke Principal-Engineer / SaaS-architect-review.
**Opdracht:** audit v1 (`docs/lifecycle_audit_2026-07-09.md`) *falsifiëren*, niet bevestigen. Bepalen waar Heatr als eerste breekt bij honderdduizenden leads, duizenden workspaces, miljoenen campagnes, veel gelijktijdige workers, retries/timeouts/webhook-redelivery en meerdere diensten per lead.
**Methode:** vier onafhankelijke onderzoekslijnen (lifecycle · deliverability/idempotency · multi-tenancy/GDPR · scale/perf) + eigen directe hertracing van elke draaggevende claim. 15 verplichte adversariële scenario's door de code getraceerd.
**Bronwaarheid:** de runtime draait op de `heatr_`-geprefixte tabellen uit `migrations/*.sql` via de auto-prefix-wrapper (`config/database.py:26,63-81`) op de **service_role**-key (RLS bypass, `021:9-11`). `supabase_schema.sql` is grotendeels dood/design-only; waar code die kolommen leest, faalt het. Een deel van `migrations/021` is een handmatige, gefaseerde migratie — claims die afhangen van of Sami SECTIE B/C/D draaide, zijn als zodanig gemarkeerd.
**Bewijslegenda:** `[CONFIRMED]` = zelf de relevante broncode/migratie gelezen. `[REVIEW]` = door een onafhankelijke tracer gevonden, plausibel, niet elke regel opnieuw gedraaid. `[REVIEW-021]` = hangt af van of migratie 021 live is.

---

## 1. Managementsamenvatting

**Wijst audit v1 de goede richting aan? — Technisch correct op de kern, maar onvolledig én met een gevaarlijke remedie.**

De draaggevende claim van v1 houdt stand en is het stevigst onderbouwd: `/campaigns/launch` schrijft nooit een `lead_campaign_history`-rij — nul `insert`/`upsert` op die tabel in de hele repo `[CONFIRMED]`. Maar v1 keek alleen naar lifecycle-*logica* en miste de zwaardere klasse problemen die als eerste breken bij groei: idempotency, deliverability, suppression, multi-tenancy en kosten. Drie daarvan zijn **nu al defect**, niet pas over 2 jaar. En v1's eigen kernremedie (C1: "launch schrijft een enrollment-rij") is naïef doorgevoerd **actief gevaarlijk** — hij creëert dubbele sends.

**De eerste vijf breuklijnen, in volgorde:**

1. **De idempotency-ledger schrijft naar een niet-bestaande tabel.** `outbound_dispatcher` schrijft naar `outbound_log`; die naam staat niet in `_HEATR_TABLES`, dus wordt niet geprefixt; migratie 020 maakt `heatr_outbound_log`. Elke ledger-write faalt en wordt fail-soft geslikt → **idempotency faalt OPEN op elke send**. `[CONFIRMED]`
2. **GDPR-suppression-lek + hard bounce naar de verkeerde kolom.** `compliance_check` leest alleen `leads.status`; unsubscribe/bounce schrijven `email_status`. Suppressie is een mutabel per-workspace veld, geen cross-workspace lijst. `[CONFIRMED]`
3. **SendingGuard is cosmetisch.** Alle volume-caps en de bounce-rate-breaker bevragen de lege `lead_campaign_history` of niet-bestaande kolommen → geen enkele rem vuurt ooit. `[CONFIRMED/REVIEW]`
4. **De master-kill-switch gate't alleen `/campaigns/launch`.** `ENABLE_CAMPAIGN_SENDS` staat op één plek; follow-ups, ad-hoc pushes en review-mails blijven versturen als je hem uitzet. `[CONFIRMED]`
5. **Multi-tenancy is een stub.** Auth collapst elke caller naar `DEFAULT_WORKSPACE` ("aerys"); meerdere hot paths missen `workspace_id`; er is geen RLS-backstop (service_role). Eén concrete cross-tenant data-bleed bevestigd. `[CONFIRMED]`

**Onderste regel:** fix de tafelinzet (idempotency, suppression, deliverability-remmen, kill-switch) vóór je aan de lifecycle-herbouw begint. De lifecycle-logica is niet waar het eerst breekt.

---

## 2. Hercontrole van audit v1

| ID (v1) | v1-claim | Verdict | Onderbouwing |
|---|---|---|---|
| **F1** dubbele mail via campagnenaam-key | HOUDT STAND (genuanceerd) | `campaign-create`-key bevat `camp_name` (`main.py:1600`) `[CONFIRMED]`. Maar "verschillende namespaces" is een red herring — echte oorzaak is F2. Cross-campagne-dedup ontbreekt sowieso. |
| **F2** launch schrijft geen enrollment-rij | **HOUDT STAND (sterkst)** | Nul inserts op `lead_campaign_history` repo-breed `[CONFIRMED]`. Launch roept `is_lead_in_active_campaign` niet eens aan — alleen `campaign_cooldown_block` (`main.py:1510`), dat op de lege tabel None geeft. |
| **F3** twee cooldown-kolommen | **AFGEWAARDEERD** (Hoog→Middel) | Feiten kloppen (`sending_guard.py:78` vs `main.py:4316`) maar het is *latent*: zonder enrollment-rijen draait SendingGuard toch niet op deze leads. Geen actueel dubbele-send-pad. `[CONFIRMED]` |
| **F4** recontact vereist nooit-gezette `no_response` | HOUDT STAND | `get_recontact_ready` eist `status='no_response'` (`recontact_signals.py:194`), alleen gezet in `_complete_sequence` (Model B, draait nooit). |
| **F5** signaal-snapshot nooit opgeslagen | HOUDT STAND | `save_outreach_snapshot` alleen via handmatig endpoint. Dubbele dead-end. |
| **F6** paused hervat nooit | HOUDT STAND | `wake_snoozed_leads` raakt alleen `crm_stage`; niets flipt `paused→pending` behalve handmatige resume. |
| **F7** `campaign.completed` sluit `is_active` niet | HOUDT STAND | `main.py:3190-3233` raakt alleen `status`. Wordt kritiek zodra C1 landt. |
| **F8** `push_eligible` genegeerd | **OPGEWAARDEERD** | Erger dan v1: `push_eligible` wordt niet eens gepersisteerd (`lead_scoring.py:212-219` laat het veld weg) `[REVIEW]`; geen verzendpad leest het. |
| **F9** verkeerde template / geen historie | GENUANCEERD | `pick_brug` levert cross-sector *bruggen*; een "template past bij sector"-assert is deels niet van toepassing. Historie-blindheid houdt stand. |
| **F10** race in due-poller | **OPGEWAARDEERD** | Niet hypothetisch: door P0-1 is de idempotency-ledger nú al fail-open, dus de race is een *actueel* risico op elk dispatch-pad. `[CONFIRMED]` |

**Over v1's voorgestelde fixes:**
- **C1 (enrollment-rij bij launch) — GEVAARLIJK als geïsoleerde fix.** Een rij met `status='pending'` wordt door `get_due_sends` (`sequence_engine.py:368`) opgepikt en door `process_due_send` verstuurd — bovenop Warmr's server-side drip (`create_campaign` post de volledige `steps`, `warmr_client.py:400-405`) → **dubbele mail 2/3**. `[CONFIRMED]` C1 mag alleen als **atomaire bundel** met F7 (webhook zet `is_active=False`+`sent_at`) en F4, én na een expliciete ownership-keuze (§7). Zonder F7 blijft de rij eeuwig `is_active=True` → lead permanent "in campagne" → nooit meer benaderbaar.
- **H1 (cooldown consolideren) is grotendeels al gebeurd** — de gate is al gewired in launch (`main.py:1510`) `[CONFIRMED]`; alleen het voeden van de tabel ontbreekt. v1 stelde H1 groter voor dan nodig.
- **v1's één-status-per-lead state machine is VERWORPEN** — zie §5.

---

## 3. Nieuwe P0/P1/P2/P3-problemen

### P0 — nu al defect / juridisch / dubbele sends / dataverlies

**P0-1 · Idempotency-ledger schrijft naar niet-bestaande tabel → fail-open op elke send.** `[CONFIRMED]`
- *Huidige toestand:* `outbound_dispatcher.py:97,112` doet `.table("outbound_log")`; `_HEATR_TABLES` (`config/database.py:29-44`) bevat die naam niet → geen prefix → query op literal `outbound_log`; migratie 020 maakt `heatr_outbound_log`. Insert throwt → geslikt (`:99-105`); `_find_completed` throwt → None (`:122-128`) → dispatcher faalt open.
- *Trigger:* elke `dispatch_outbound`-aanroep (elke send).
- *Blast radius:* alle prospect-facing sends + de operator-briefing dedup (`main.py:4659`).
- *Failure mode:* geen dedup (dubbele mail bij Warmr-timeout+retry), geen append-only bewijs waaróm/wanneer gemaild → GDPR-provability nul.
- *Detecteerbaarheid:* laag — `/control/outbound` (`main.py:848`) toont eeuwig "migratie 020 niet gedraaid?", wat de bug maskeert als "migratie niet toegepast".
- *Schaalpunt:* nú, bij de eerste Warmr-timeout.
- *Minimale veilige fix:* `"outbound_log"` toevoegen aan `_HEATR_TABLES`.
- *Structureel:* zie P0-1b.

**P0-1b · Zelfs met juiste naam is de dedup niet race-proof.** `[CONFIRMED]` `idx_outbound_log_key` is een **gewone index**, geen UNIQUE (`020:28-29`); 021 voegt bewust geen unique toe (`021:75-85`). `dispatch_outbound` is check-then-insert (`_find_completed:198` → `send():219` → `_append_record:230`). Twee gelijktijdige dispatches met dezelfde key lezen beide "geen completed" en versturen beide. *Een index is geen constraint.* → **UNIQUE(workspace_id, idempotency_key)** vereist.

**P0-2 · GDPR-suppression-lek + hard bounce op de verkeerde kolom.** `[CONFIRMED]`
- *Huidige toestand:* `compliance_check` leest **alleen** `lead["status"]` tegen `BLOCKED_STATUSES=("unsubscribed","forgotten","disqualified")` (`enrichment_check.py:35,52`). De unsubscribe-webhook schrijft `email_status="unsubscribed"`+`crm_stage` (`main.py:3179-3183`), de bounce-webhook `email_status="bounced"` (`main.py:3172`) — **nooit `status`**. `"bounced"` staat niet in `BLOCKED_STATUSES`.
- *Trigger:* een uitgeschreven/gebouncede lead in een volgende campagne (bv. de AI-audit-dienst).
- *Blast radius:* elke her-push; cross-workspace elke tenant met dezelfde persoon.
- *Failure mode:* de suppressie-belofte wordt gebroken. `/campaigns/launch` heeft geen `email_status`-gate (`main.py:1477`) → een bounced/unsubscribed lead is launchbaar; alleen `/leads/send-to-warmr` filtert `email_status` (`main.py:620`). Suppressie leeft per-workspace-rij (`heatr_leads` uniek op `(workspace_id, lower(email))`) → unsubscribe in A onderdrukt niets in B. `is_sendable` heeft een `is_test_lead`-bypass die suppressie negeert (`email_sendability.py:82`).
- *Bijkomend:* migratie 021's `email_status`-CHECK bevat `'bounced'` niet (`021:47`), terwijl `leads.status` het wél toestaat (`021:38-42`) — bounce is naar de verkeerde kolom geschreven; bij `VALIDATE` faalt de bounce-UPDATE. `[REVIEW-021]`
- *GDPR-forget-collision:* `forget_lead` zet elk vergeten lead op `email="verwijderd@anoniem.nl"` (`gdpr_manager.py:66`); met de unique index faalt de **tweede** forget in een workspace → geslikt (`:76`) → PII niet geanonimiseerd, `status` niet `forgotten`, functie retourneert succes. Art.17-schending. `[REVIEW-021]`
- *Minimale veilige fix:* `'bounced'` in `BLOCKED_STATUSES`; bounce/unsubscribe óók `leads.status` zetten; forget met per-lead-unieke redactie.
- *Structureel:* cross-workspace, append-only **suppression-tabel op `lower(email)`/domein**, geraadpleegd door één gedeelde gate.

**P0-3 · SendingGuard is cosmetisch — geen werkende verzendrem.** `[CONFIRMED/REVIEW]`
- *Huidige toestand:* alle volume-tellers query'en `lead_campaign_history` (leeg → count 0, `sending_guard.py:114-137`); de domein-cap filtert op kolom `sending_domain` die niet in de runtime-tabel bestaat; de bounce-rate-breaker query't `reply_inbox.event_type='bounced'` terwijl `heatr_reply_inbox` (migratie 004) geen `event_type`-kolom heeft (`sending_guard.py:186`) → exception → `return None` → breaker vuurt nooit.
- *Trigger:* elke volume-piek.
- *Blast radius:* mailboxreputatie van elke inbox/domein.
- *Failure mode:* geen enkele cap of breaker triggert → onbegrensd volume, reputatieschade.
- *Detecteerbaarheid:* laag (geen alert; caches i.p.v. live reputatie).
- *Schaalpunt:* nú, bij de eerste burst.
- *Minimale fix:* breaker op een bestaande kolom; caps voeden zodra `lead_campaign_history` gevuld is (P0-4-bundel).
- *Structureel:* reputatie/capacity als event van Warmr (§7).

**P0-4 · Master-kill-switch gate't alleen launch.** `[CONFIRMED]` `ENABLE_CAMPAIGN_SENDS` staat uitsluitend in `/campaigns/launch:1457` (repo-brede grep). `process_due_send`, `/leads/send-to-warmr` (`:652`), `/leads/{id}/send-review-email` (`:756`) en de briefing (`:4659`) negeren de vlag. *Failure mode:* de switch uitzetten stopt in-flight follow-ups, ad-hoc pushes en review-mails niet. *Minimale fix:* de check centraal in `dispatch_outbound` (de enige verplichte egress). CLAUDE.md noemt dit een "master kill-switch"; de implementatie honoreert dat niet.

### P1 — breekt bij tienduizenden leads of een tweede tenant

**P1-1 · Multi-tenancy is een stub + cross-tenant data-bleed.** `[CONFIRMED]`
- *Huidige toestand:* `_service_key_workspace` retourneert altijd `DEFAULT_WORKSPACE` (`main.py:133`); `_jwt_workspace` valt terug op `DEFAULT_WORKSPACE` bij ontbrekende claim (`main.py:159`). Geen RLS-backstop (service_role, `021:9-11`). Isolatie = de conventie `.eq("workspace_id")`, die meerdere hot paths missen.
- *Concrete leks:* `_get_page_text_for_lead` bevraagt `companies_raw` op `.eq("domain")` only (`enrichment_queue.py:1167`) → tenant B trekt tenant A's gescrapte tekst; `mark-deal-won` muteert `leads` op `.eq("id")` only (`main.py:3512`); webhook-`leads`-updates id-only (`main.py:3156-3180`); `claude_cache`-key mist `workspace_id` (`claude_cache.py:75`) → cross-tenant gedeelde cache met mogelijk PII.
- *Trigger:* tweede echte tenant, of een JWT zonder `workspace_id`-claim (leest "aerys"-data, scenario 11).
- *Blast radius:* cross-tenant PII-lezen/muteren.
- *Minimale fix:* ontbrekende claim → 401/403 i.p.v. default; `workspace_id` toevoegen aan de genoemde queries.
- *Structureel:* workspace-identiteit in de service-key/JWT verplicht + RLS als tweede linie.

**P1-2 · Cost-guard O(n) full-scan per AI-call, ongeïndexeerd.** `[CONFIRMED]` `cost_guard.py:173-181,258-268` haalt álle `cost_eur`-rijen van de workspace/maand op en somt in Python; de enige index is op kolom `date` (`supabase_schema.sql:777`) terwijl de query op `created_at` filtert (`:177`). Bij 100k leads × ~10 calls = 1M+ rijen/workspace/maand, elke enrichment-stap 2 full-scans. **O(n) per call → O(n²) over een batch.** *Minimale fix:* SQL-aggregaat (RPC `sum()`) + filter op de geïndexeerde kolom. (Wél fail-closed bij DB-fout — correct.)

**P1-3 · Retry re-runt alle 15 enrichment-stappen + accumulator reset.** `[CONFIRMED]` `steps_completed` wordt alleen bij init geschreven (`enrichment_queue.py:100`), nergens bijgewerkt; de loop (`:266`) draait de volledige lijst onvoorwaardelijk. Crash op stap 14 → alle Claude-stappen opnieuw. `claude_cache` dempt (SHA-256 van prompt+model, 7-30d TTL) maar `NO_CACHE_CONTEXTS` cachet niet en de per-lead-accumulator is in-memory → **reset naar 0 bij restart** → het €0,05/lead-plafond wordt per retry vers toegekend (3× overschrijdbaar). *Minimale fix:* `steps_completed` echt bijhouden + skip; per-lead `spent_eur` op de job-rij persisteren.

### P2 — schaalplafond (tienduizenden–100k)

- **P2-1 · Geen queue-fairness → starvation.** `[CONFIRMED]` `claim_next_enrichment_job:180-188` selecteert globaal op `priority, created_at` zonder `workspace_id`-filter. Tenant A's 100k jobs draineren vóór tenant B's 5 urgente. Plus: n8n post-hoc mismatch (`:1233`) strandt een verkeerd-geclaimde job in `running` zonder reset → 30 min tot de reaper. *Fix:* claim per `workspace_id` (of weighted round-robin) + reset op mismatch.
- **P2-2 · Ongebonden worker-loops + semafoor binnen de task.** `[REVIEW]` `run_enrichment_worker` (`enrichment_queue.py:1019-1034`) en `run_scraping_worker` (`scraping_queue.py:353-364`) claimen→`create_task`→loopen met de semafoor *binnen* de task → markeren de hele pending-queue in-memory `running`, duizenden coroutines, geheugen-explosie + orphan-`running`-rijen bij crash.
- **P2-3 · Analytics: full-window fetch + ongebonden `.in_()` + Python-aggregatie + deep-offset + ILIKE + count=exact.** `[REVIEW]` `main.py:2124-2225,2289-2305,3549,4193`; `list_leads` (`:533,544`). Een `.in_()` met tienduizenden UUID's → PostgREST 414/OOM. *Fix:* server-side aggregatie + keyset-paginatie.
- **P2-4 · Ontbrekende samengestelde indexen op de live `heatr_`-tabellen** (`heatr_leads(workspace_id, created_at DESC)`, `(workspace_id, email_status)`, `heatr_website_intelligence(workspace_id, total_score)`, `heatr_reply_inbox(workspace_id, received_at DESC)`). `[REVIEW]`
- **P2-5 · Geen globaal kostenplafond.** `[CONFIRMED]` caps zijn per-workspace (`cost_guard.py:176,263`); 1.000 tenants × €20 = €20k/maand zonder platform-kill-switch.

### P3 — observability, ops, DR, onderhoudbaarheid

- **P3-1 · ~90 bare `except Exception` slikken fouten**, ergst op het reply-pad (`reply_classifier.py:265-323`) `[CONFIRMED]`: faalt de DB-update voor een "interested"-reply, dan blijft de sequence iemand mailen die al reageerde — onzichtbaar. *Onzichtbaar incident:* een lead die reageerde blijft aangeschreven.
- **P3-2 · Geen structured logging / correlation-id / Sentry / OTel / Prometheus**; `/healthz` checkt geen Supabase/Warmr/Anthropic; metrics alleen in de 23:55-batch (`metrics_collector.py:196`). `[REVIEW]` *Onzichtbaar incident:* een 09:00-bounce-storm wordt 15 uur niet gealarmeerd.
- **P3-3 · Warmr zonder weerbaarheid:** verse `httpx.AsyncClient` per call, geen retry/backoff/circuit-breaker; `push_leads_bulk` dropt gefaalde chunks stil (`warmr_client.py:288-296`, `warmr_lead_id` blijft NULL). `[CONFIRMED via client-lezing]` Geen dead-letter.
- **P3-4 · Migraties handmatig, drift-gevoelig:** geen `schema_migrations`-tabel; `021:120-124` voegt een constraint zonder existence-guard toe (re-run faalt); dubbele `APPLY_ME_*.sql` overlappen genummerde migraties; twee schema-bronnen die de code door elkaar leest. `[REVIEW]`
- **P3-5 · Riskantste paden ongetest:** de sequence-race en cross-tenant-isolatie zijn weg-gemockt (`claim_next_enrichment_job` gestubd). Auth, dedup/cooldown, cost-guard-fail-closed en de reaper zijn wél goed getest. `[REVIEW]`

---

## 4. Adversariële scenarioresultaten

| # | Scenario | Huidig gedrag (`file:line`) | Gewenst |
|---|---|---|---|
| 1 | Warmr accepteert, Heatr timeout+retry | Geen retry, geen `Idempotency-Key` naar Warmr (`warmr_client.py:114`); ledger-write faalt (P0-1) → retry vindt geen completed → **dubbele bezorging**. `[CONFIRMED]` | Idempotency-key naar Warmr + `in_flight`-record vóór send + UNIQUE-ledger. |
| 2 | Twee due-workers, zelfde enrollment | `get_due_sends` claimt niets atomair; side-effect (Warmr-push) vóór statewissel; enige guard = inerte ledger-key (`sequence_engine.py:352,450,504`) → **dubbele send**. *Enrichment-pad heeft wél correcte CAS* (`enrichment_queue.py:197-211`). `[CONFIRMED]` | CAS-claim `UPDATE…SET status='sending' WHERE status='pending'` + UNIQUE-ledger. |
| 3 | Unsubscribe-event 2× | Geen event-dedup (`main.py:3102-3242`) → dubbele `reply_inbox`-rij; `campaign.completed` maakt elke keer een `crm_task`. Lead-update zelf idempotent. `[CONFIRMED]` | `event_id` UNIQUE + short-circuit op replay. |
| 4 | `campaign.completed` vóór `reply.received` | `status_map`-update zonder `campaign_id`-scoping en zonder terminal-guard (`main.py:3233`) → `no_response` **overschrijft** `replied`, en op **alle** campagne-rijen van de lead. `[CONFIRMED]` | Scope op `campaign_id`; nooit `replied/unsub/bounced` downgraden; order op `sequence_no`. |
| 5 | Hard-bounced lead in nieuwe campagne | `'bounced'` niet in `BLOCKED_STATUSES`; bounce schrijft `email_status`, niet `status` (`main.py:3172`); launch heeft geen `email_status`-gate → **launchbaar**. Breaker dood (geen `event_type`-kolom). `[CONFIRMED]` | `'bounced'` blokkeren op `status`; breaker op bestaande kolom. |
| 6 | Launch maakt enrollment-rij terwijl Warmr al dript | Vandaag geen dubbele send (geen rij); maar zodra iemand `status='pending'` enrollt naast Warmr's server-side drip → **gegarandeerd dubbel** (`warmr_client.py:400`, `sequence_engine.py:368`). `[CONFIRMED]` | Kies één send-model (§7); tracking-rij is niet-`pending`. |
| 7 | Worker crasht na externe call, vóór commit | Enrichment: crash vóór cache-write → **dubbele Claude-kosten**; correctheid ok (CAS). Outbound: completed-record naar dode tabel → reaper-requeue **her-sendt**. `[CONFIRMED]` | Durable UNIQUE-upsert in dezelfde unit als send-intent; cache transactioneel. |
| 8 | Kill-switch uit tijdens actieve campagne | Alleen `/campaigns/launch` gate't (`main.py:1457`); follow-ups/pushes/review-mails blijven sturen. `[CONFIRMED]` | Check centraal in `dispatch_outbound`. |
| 9 | Enrichment faalt stap 14/15, herstart | `steps_completed` nooit bijgewerkt (`enrichment_queue.py:100,266`) → alle 15 opnieuw; accumulator reset → plafond 3× vers. `[CONFIRMED]` | Skip voltooide stappen; per-lead `spent_eur` persisteren. |
| 10 | Tenant A 100k jobs, tenant B 5 urgent | Globale FIFO zonder `workspace_id` (`enrichment_queue.py:180-188`) → **B verhongert**; n8n-mismatch strandt job in `running`. `[CONFIRMED]` | Claim per workspace / weighted round-robin + reset op mismatch. |
| 11 | JWT zonder workspace-claim | `_jwt_workspace:159` → `DEFAULT_WORKSPACE` → leest/muteert **aerys**-data. `[CONFIRMED]` | Ontbrekende claim → 401/403. |
| 12 | Zelfde e-mail unsubscribet in één workspace | Alleen die rij krijgt `email_status` (`main.py:3179`); andere workspace-rij blijft mailbaar; forget-collision (P0-2). `[CONFIRMED]` | Cross-workspace suppression op `lower(email)`. |
| 13 | Multi-campagne per lead | Bewust geen unique op `lead_id` (`021:83`) maar **geen enkele insert** → een lead kan in 2 diensten-campagnes zonder dat iets het voorkomt of vastlegt. `[CONFIRMED]` | Enrollment-rij per lead×campagne; multi-dienst = expliciete auditeerbare rij. |
| 14 | Webhook naar onbekende lead/campaign | Ontbrekend `heatr_lead_id` → skip + `{"ok":true}` (`main.py:3242`); onbekend id → 0-rij-updates zonder fout; orphan `reply_inbox`-rij (geen FK). `[CONFIRMED]` | Lookup + dead-letter + distinct non-2xx. |
| 15 | DB onbereikbaar tijdens compliance/cost-check | Cost-guard **fail-closed** (`cost_guard.py:182-191,269-274`); compliance-fetch zonder try/except → 500 = fail-closed. `[CONFIRMED]` | Behouden; alleen nette "compliance_unavailable"-respons i.p.v. opaque 500. |

---

## 5. State-machinebeoordeling

**v1's één-`leads.status`-machine is VERWORPEN.** Migratie `021:83` zegt letterlijk "bewust GEEN unique op `lead_id`" — het datamodel ondersteunt met opzet meerdere campagnes per lead `[CONFIRMED]`, en Aerys pitcht meerdere bruggen (`opportunity_types text[]`; website/workflow/ai_audit, `sequence_templates.py:356`). De webhook bewijst de bug al: `status_map` overschrijft alle campagne-rijen zonder `campaign_id` (`main.py:3233`). Eén status per lead kan twee sporen niet dragen.

**Betere machine — twee assen + harde overlays.**

**As 1 · Lead-lifecycle (per lead, workspace-globaal):**
```
NEW → ENRICHING → { ENRICH_FAILED → (requeue) → ENRICHING | READY }
READY → (mag in ≥1 enrollment tegelijk)
Harde overlays (winnen ALTIJD, forceren alle enrollments → STOPPED, blokkeren nieuwe enroll):
  SUPPRESSED (unsubscribe)  — absoluut terminaal, CROSS-workspace op e-mail/domein
  EMAIL_DEAD (hard bounce)  — terminaal voor e-mail; ander kanaal mag
  DISQUALIFIED (reason)     — terminaal tenzij operator heropent
  MANUAL_HOLD (override)    — pauzeert alle campagnes
  CUSTOMER (≥1 deal won)    — NIET terminaal: heropenbaar voor nieuwe dienst
```
**As 2 · Enrollment-status, per `(lead × campaign × dienst)` in een GEVULDE `lead_campaign_history`:**
```
ENROLLED → SENDING(step_i) → WAITING_REPLY → { ENGAGED(reply) → HANDOFF
                                             | STEP_DUE → SENDING(step_i+1)
                                             | COMPLETED(no reply) → COOLDOWN(90d) → RECONTACT_READY }
elke actieve enroll → STOPPED bij: reply | bounce | unsubscribe | disqualify | manual_hold
```
**Eigenaar per transitie (enige schrijver):** enrichment-pipeline (NEW↔READY) · suppression-service (SUPPRESSED/EMAIL_DEAD) · operator (DISQUALIFIED/MANUAL_HOLD/CUSTOMER) · Campaign/Send (ENROLLED, SENDING) · webhook-handler (WAITING_REPLY→ENGAGED/COMPLETED/STOPPED, mét `campaign_id`-scoping). Message/send-status leeft op de ledger, niet op de lead.

---

## 6. Datamodelreview

| Tabel | Ownership | Oordeel (`file:line`) |
|---|---|---|
| `leads` | Lifecycle | God-table: 4 status-assen (`status`/`crm_stage`/`email_status`/`manual_status_override`) zonder CHECK op de eerste twee → drift; geen score-/status-historie; geen `created_by`. |
| `lead_campaign_history` | Campaign/Send | **Twee onverenigbare defs** (`supabase_schema.sql:410` `status='active'` vs `migrations/009` `status='pending'`+`is_active`); code leest `sending_domain` die in geen van beide runtime-tabellen bestaat; **nooit gevuld**; sequence als JSONB-blob i.p.v. FK naar `template_version`. |
| `campaigns` | Campaign/Send | `lead_ids` bewust `[]` geschreven (`main.py:1699`) → audit-belofte niet ingelost; geen `updated_at`/status-CHECK. Wél goede `created_by/via/ip`. |
| `reply_inbox` | Campaign/Send | Runtime `heatr_reply_inbox` mist `warmr_message_id` (UNIQUE-key) én `event_type` → webhook-dedup + bounce-breaker breken. |
| `outbound_log` | Suppression/Ledger | Concept goed; index **niet UNIQUE** (`020:28`) → niet race-proof; verkeerde tabelnaam gewired (P0-1). |
| `lead_outreach_snapshots` | Recontact | **Goed:** append-only, correct geïndexeerd (`migrations/004:41`). Behouden. |
| `crm_deals` | CRM | `value float` (moet `numeric`), `workspace_id` **nullable**, RLS-sleutel `app.workspace_id` ≠ `app.current_workspace_id` van `leads` → policy matcht nooit; geen `stage`/`updated_at`/historie. |
| templates | (geen) | **Bestaat niet als tabel** — sequences als code + JSONB-blobs → geen versie/A-B-historie, niet auditeerbaar bij miljoenen sends. |
| analytics/metrics | Analytics | Live uit Warmr per pageview (`main.py:1721`) → N calls/pageview, geen tijdreeks. |
| suppression | (geen) | **Bestaat niet** — suppressie = mutabel veld op de lead-rij, per-workspace (P0-2). |
| webhook-event-ledger | (geen) | **Bestaat niet** — geen `event_id`-dedup (scenario 3). |
| outbox-events | (geen) | **Bestaat niet** — geen transactionele event-publicatie. |

**Systemisch:** vrijwel geen FK's op de `heatr_`-tabellen (021 sectie E uitgecommentarieerd → orphans mogelijk); kies één schema-bron.

---

## 7. Warmr-integratie en contract

**Ownership per sequence-stap vandaag = "beide/betwist".** Launch post de volledige `steps` → **Warmr dript server-side** (`warmr_client.py:400-405`); de dispatcher (`process_due_send`) rendert per stap en claimt `render_owner=heatr` (`sequence_engine.py:469`). Beide modellen kunnen dezelfde stap sturen. Inbox-selectie is stuurloos: caps zijn per-inbox maar `push_leads_bulk` leest `lead.preferred_inbox_id` (nooit gezet), send-to-warmr pakt `inboxes[0]` (`main.py:646`). **Verplichte keuze: Warmr dript + Heatr houdt een niet-`pending` tracking-rij bij (aanbevolen), OF Heatr dispatcht elke stap en Warmr slaat één stap op. Nooit beide tegelijk.**

**Voorgesteld contract (event-driven, idempotent, append-only).**

*Heatr → Warmr (commands, HTTP + `Idempotency-Key`):* `campaign.create` (key `sha256(name+template_version+lead_set)` → server-side dezelfde `campaign_id`), `lead.enroll` bulk (key per lead `sha256(campaign_id+email)`; Warmr weigert bij suppression, retourneert per lead `{enrolled|suppressed|duplicate}`), `campaign.pause/resume` (idempotent). Nooit stil mislukken; per-lead resultaat terug.

*Warmr → Heatr (events, async, at-least-once).* Payload MUST: `event_id`, `event_type`, `occurred_at`, `sequence_no`, `workspace_id`, `heatr_lead_id`, `warmr_lead_id`, `campaign_id`, `message_id`.
- Events: `message.sent/delivered/bounced(type:hard|soft)`, `reply.received`, `lead.interested`, `lead.unsubscribed`, `campaign.completed`, `inbox.reputation_changed`, `inbox.warmup_completed`, `inbox.capacity`.
- **Dedup:** `event_id` UNIQUE, insert-or-ignore vóór verwerking.
- **Ordering:** verwerk op `(lead_id, campaign_id, sequence_no)`; negeer lagere `sequence_no` → lost `completed`-vóór-`replied` op.
- **Retry/response:** `200` alleen na commit; interne fout → `5xx` (Warmr retryt); onbekende lead → correleer op `warmr_lead_id`, anders **dead-letter**, nooit stil `ok:true`.
- **Suppression/reputatie als eerste-klas events** → gedeeld read-model, geen stale cache.

---

## 8. Multi-tenancy en GDPR

- **Auth collapst naar één tenant** (`main.py:133,159`) → geen echte scheiding; claimloze JWT = aerys-data (scenario 11). `[CONFIRMED]`
- **Geen RLS-backstop** (service_role, `021:9-11`); isolatie = de `.eq("workspace_id")`-conventie die hot paths missen: `companies_raw`-by-domain (cross-tenant bleed, `enrichment_queue.py:1167`), deal-won id-only (`main.py:3512`), webhook id-only (`main.py:3156-3180`), gedeelde `claude_cache` (`claude_cache.py:75`). `[CONFIRMED]`
- **Suppressie is niet cross-workspace en niet cross-campagne** (P0-2); unsubscribe/bounce op de verkeerde kolom; forget-collision laat PII staan. `[CONFIRMED/REVIEW-021]`
- **Kan Heatr bewijzen waarom/wanneer gemaild?** Nee — de append-only ledger is leeg (P0-1). Geen auditeerbaar unsubscribe/send-bewijs.
- **Aanbevolen:** verplichte workspace-claim (geen default), RLS als tweede linie, cross-workspace suppression-tabel, forget met per-lead-unieke redactie + hard suppress i.p.v. placeholder-email, append-only event-ledger.

---

## 9. Schaalbaarheid en performance

| Pad | Complexiteit | Probleem |
|---|---|---|
| Cost-guard per AI-call | **O(n) → O(n²)/batch** | Full-scan + ongeïndexeerd `created_at` (P1-2). |
| Analytics-funnel/cost/pipeline | O(n) fetch + ongebonden `.in_()` | 414/OOM bij tienduizenden (P2-3). |
| `list_leads` | O(n) + deep-offset + ILIKE + `count=exact` | Traag bij 100k ongeacht index (P2-3). |
| Enrichment-queue-claim | O(log n) claim, **geen fairness** | Starvation bij tweede tenant (P2-1). |
| Enrichment-retry | O(15 stappen) opnieuw | Dubbele AI-kosten, plafond 3× (P1-3). |
| Worker-loops | O(pending) coroutines in-memory | Geheugen-explosie + orphans (P2-2). |
| Platformkosten | per-workspace caps | Geen globaal plafond (P2-5). |

---

## 10. AI-architectuur

**Drie klassen.** *Deterministisch (nooit AI):* suppression/compliance-gate, volume-caps, cooldown-timers, hard-bounce, score-drempels — juridisch/veiligheid, reproduceerbaar en auditeerbaar. *AI-ondersteund (binnen harde regels):* template/brug-keuze (Haiku, binnen sector-toegestane set `sequence_templates.py:29`), send-time-optimalisatie (lokaal model op webhook-open-data, ~26% open-lift, nauwelijks kosten), reply-probability als **sortering**. *AI-suggestie (mens beslist):* recontact-worthiness, win-back-timing, spam/toon-review vóór verzenden (harde block alleen bij extreem).

**Eisen:** één **AI-gateway** waar elke call doorheen loopt (cost-guard + globaal plafond + rate-limit + cache); fix P1-2 en P2-5 vóór AI-volume omhoog gaat; retries mogen geen dubbele kosten maken (P1-3). Compliance/suppression **nooit** door een probabilistisch model.

---

## 11. Observability, tests en DR

- **Fail-open door fail-soft:** P0-1 (ledger geslikt), P3-1 (reply-updates geslikt → blijft mailen), P0-3 (breaker-exception geslikt). Een geslikte exception is hier een veiligheidslek, geen ruis.
- **Blind tijdens incident:** geen structured logging/correlation-id/Sentry/OTel; `/healthz` pingt geen dependencies; alerts alleen in de 23:55-batch (P3-2). Een 09:00-bounce-storm blijft 15u onzichtbaar.
- **Geen dead-letter** voor Warmr-drops (P3-3) of onbekende webhook-leads (scenario 14).
- **Migratie-DR:** geen `schema_migrations`-tracking, dubbele `APPLY_ME_*.sql`, constraint zonder existence-guard (P3-4).
- **Tests:** de riskantste paden (sequence-race, cross-tenant) zijn weg-gemockt (P3-5). Wél goed: auth, dedup/cooldown, cost-guard-fail-closed, reaper.

---

## 12. Wat goed is en behouden moet blijven (zelf bevestigd)

- **Webhook-HMAC fail-closed** — leeg secret → 503, bad sig → 401 (`main.py:3116-3121`). `[CONFIRMED]`
- **Cost-guard fail-closed bij DB-fout** (`cost_guard.py:182-191,269-274`). `[CONFIRMED]`
- **Enrichment-queue CAS-claim is race-veilig** — `UPDATE…WHERE status='pending'` + lege-`data`-check (`enrichment_queue.py:197-211`). `[CONFIRMED]` (Repliceer dit patroon naar het sequence-pad.)
- **Reaper** requeuet/dead-lettert vastgelopen `running`-jobs met retry-ceiling + luide log (`queue_reaper.py`). `[REVIEW]`
- **`lead_outreach_snapshots`** — enige model dat historie goed doet (append-only + index). `[CONFIRMED]`
- **Dispatcher-*ontwerp*** (append-only + idempotency-key + gedeelde compliance-gate) is correct — alleen naar de verkeerde tabelnaam gewired. Behouden, herbedraden.
- **Dedup/cooldown via `is_active`** i.p.v. status-strings is drift-resistent — zodra de tabel gevuld wordt.

---

## 13. Future-state Heatr v2

**Modulaire monoliet, één transactionele Postgres, Postgres-outbox voor async events. Geen microservices.** Bounded contexts, elk met eigen tabellen, één schrijver, gepubliceerde events, idempotency-grens en tenant-handhaving:

| Context | Bezit | Enige schrijver | Publiceert | Idempotency | Tenant |
|---|---|---|---|---|---|
| **Discovery** | `companies_raw` | Discovery | `LeadDiscovered` | dedup op `(ws, domain)` | ws in query |
| **Enrichment** | `enrichment_data` | Enrichment-worker | `LeadEnriched` | per stap `steps_completed` | ws in claim |
| **Scoring & ICP** | scores | Scoring | `LeadScored` (met `push_eligible`) | deterministisch | ws |
| **Lifecycle/State** | `leads.status` (As 1) | Lifecycle | `LeadStateChanged` | transitie-guard | ws |
| **Campaign/Send** | `lead_campaign_history` (As 2), `outbound_log` | Campaign/Send | `EnrollmentCreated`,`MessageSent` | UNIQUE ledger + CAS-claim | ws |
| **Suppression** | `suppression` (cross-ws op e-mail/domein) | Suppression | `LeadSuppressed` | UNIQUE op `lower(email)` | **cross-ws** |
| **CRM/Deals** | `crm_deals` | CRM | `DealWon` | — | ws NOT NULL |
| **Analytics** | tijdreeks-tabellen | Analytics (read-model) | — | idempotent op event_id | ws |
| **AI Gateway** | `api_cost_log`,`claude_cache` | AI-gateway | — | cache-key incl. ws | globaal plafond |
| **Identity/Workspace** | `workspaces`,`api_keys` | Identity | — | — | **bron van ws** |

Events idempotent op `event_id`, verwerkt op volgorde per `(lead,campaign)`. Kill-switch centraal in Campaign/Send. Observability (JSON-logs + correlation-id + Sentry + `/ready`) vanaf dag één.

---

## 14. Scores (1–10)

| Dimensie | Score | Onderbouwing |
|---|---:|---|
| Architectuur | **4** | Twee schema-bronnen, god-table, dode dispatcher-wiring, geen bounded contexts. |
| Lifecycle | **4** | v1-kern klopt; fix complexer (bundel) dan v1 stelde. |
| Schaalbaarheid | **3** | Cost-guard O(n²)/batch, geen queue-fairness, ongebonden loops. |
| AI | **5** | Werkt + kosten-bewaakt; mist globaal plafond + gateway + resumability. |
| Performance | **3** | Full-scan + ongeïndexeerde live-tabellen + analytics-`in_()`. |
| Deliverability | **2** | SendingGuard cosmetisch, ledger fail-open, geen retry/idempotency naar Warmr. Grootste correctie op v1. |
| CRM | **4** | 4 status-assen, nullable `workspace_id` op deals, RLS-sleutel-mismatch. |
| Nurture | **2** | Bestaat niet. |
| Recontact | **2** | Architectonisch dood (F4/F5). |
| Testbaarheid | **4** | Riskantste paden weg-gemockt; goede basis elders. |
| Onderhoudbaarheid | **3** | Twee schema's, ~90 stille excepts, handmatige drift-gevoelige migraties. |
| Enterprise Readiness | **2** | GDPR-suppression-lek, multi-tenant-stub, geen observability/DR. |

**Gemiddeld ~3,2.** De post-recovery-audit (`heatr_audit_v2.html`) landde op 5,4 — maar mat "werkt/veilig na recovery", niet "houdt stand op enterprise-schaal". Andere lat, strengere blootlegging; geen verslechtering. Scherpste correctie: **Deliverability** (v1 vertrouwde SendingGuard als rem; die rem bestaat feitelijk niet).

---

## 15. Geprioriteerde deblokkeer-route

**Minimale hotfixes (klein, geen ownership-conflict, reduceren productie/juridisch risico):**
1. **P0-1:** `"outbound_log"` toevoegen aan `_HEATR_TABLES` **+** `UNIQUE(workspace_id, idempotency_key)` op `heatr_outbound_log` — landt **atomair als bundel** (naam-fix zonder unique = nog steeds racy).
2. **P0-4:** kill-switch-check verplaatsen naar `dispatch_outbound`.
3. **P0-2 (deel):** `'bounced'` in `BLOCKED_STATUSES`; bounce/unsubscribe óók `leads.status` zetten; forget met per-lead-unieke redactie.

**Veilige structurele patches:**
4. **P0-2 (structureel):** cross-workspace suppression-tabel op `lower(email)`, geraadpleegd door één gedeelde gate.
5. **P0-3 + F2/C1-bundel:** SendingGuard-breaker op een bestaande kolom **en** `lead_campaign_history` vullen bij launch **als tracking-only rij (niet `pending`)** mét webhook-lifecycle (F7: `is_active=False`+`sent_at`; F4: `no_response`). **Deze drie MOETEN samen landen** — een `pending`-rij naast Warmr's drip = dubbele sends (scenario 6). Kies eerst het send-ownership (§7).
6. **P1-1:** ontbrekende `workspace_id`-filters + claimloze JWT → 401.
7. **P1-2 / P2-5:** cost-guard SQL-aggregaat + globaal plafond.

**Architecturale migratie:**
8. Twee-assen state machine (§5) met één status-eigenaar; webhook-event-ledger + ordering; daarna nurture/recontact/templates (v1 §4-6) bovenop een fundament dat dan pas klopt.

---

*Tweede review, onafhankelijk uitgevoerd via vier tracersessies + eigen hertracing. Geen productiecode gewijzigd. Audit v1 wijst de goede richting op lifecycle-logica, maar de eerste breuklijnen bij groei liggen bij idempotency, deliverability, GDPR-suppression en multi-tenancy — en v1's C1-remedie is onveilig zonder de F7-bundel. Fix de tafelinzet vóór het onderscheid.*
