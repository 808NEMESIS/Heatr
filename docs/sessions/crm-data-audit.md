# CRM Data Audit (Fase 1A)

**Doel:** voor elk veld in de pipeline bepalen of het in de CRM-UI wordt
getoond, of het waarde toevoegt om wél te tonen, en welke zwakte uit de
state-of-Heatr analyse het zou adresseren.

## Zwakte-codes

- **Z1** — Sequence-tekst niet door Sami gereviewd (per-lead preview/edit-flow nodig)
- **Z2** — Worker-uptime onbetrouwbaar (alerting + daemon-supervisie)
- **Z3** — Email-verifier `risky`-output / bounce-risk (cohort-monitoring + auto-pause)
- **Z4** — Reply-flow nooit getest (eerste replies extra zichtbaar)
- **Z5** — 0% archetype-coverage initieel (enrichment-completeness vóór sequence-toelating)
- **Z6** — Geen E2E-tests (test-mode toggle per lead)

## CRM-UI scope (huidig zichtbaar)

Wat momenteel in `/crm/activity` (kanban) + `/leads` (lijst) + `/leads/{id}` (detail)
zichtbaar is:

- company_name, domain, city, sector, archetype (badge + tooltip), score
- status (derived), status_reason, status_changed_at, manual_override flag
- last_outbound_at, last_inbound_at, last_inbound_category
- email_status (alleen in `/campagnes/nieuw` lead-picker, NIET in CRM)
- score (totaal), filters: sector / archetype / min_score

Niet zichtbaar in CRM (wel in dashboard/analytics): cost-tracker, queue-health,
worker-status (alleen pulse-icon).

---

## Tabel: `heatr_leads` (70+ velden)

### Wel zichtbaar in CRM ✓
| Veld | UI | Plek |
|---|---|---|
| company_name | ✓ | kanban-card titel |
| domain | ✓ | lead-detail |
| city | ✓ | kanban-card subtitle |
| sector | ✓ | filter dropdown |
| archetype | ✓ | badge + tooltip |
| score | ✓ | kanban-card + filter |
| status | ✓ | kolom-kop kanban |
| crm_stage | ✓ (legacy) | /crm pipeline-view |
| created_at | ✓ | implicit (ordering) |

### Gedeeltelijk zichtbaar (alleen bij hover/inspect of ander scherm)
| Veld | UI | Waarde toevoegen? | Zwakte |
|---|---|---|---|
| email_status | gedeeltelijk (campagne-launcher only) | **hoog** | **Z3** — bounce-risk niet zichtbaar voor je commit |
| manual_status_override | ✓ via lock-icon | ok | — |
| status_reason | ✓ in why-here tooltip | ok | — |
| recontact_after | gedeeltelijk (alleen achtergrond-logica) | midden | Z1 (zichtbaar maken wanneer terugkomen) |

### Niet zichtbaar in CRM ✗ — hoog waarde-potentieel
| Veld | Waarde toevoegen? | Zwakte | Toelichting |
|---|---|---|---|
| **personalization_potential** | hoog | Z1 | Drempel ≥70 voor v1.0; nu enkel in launcher zichtbaar — moet pre-flight CRM-signal zijn |
| **fit_score / data_quality_score / reachability_score** | hoog | Z1 + Z6 | Sub-scores onthullen waarom totaalscore zo is; cruciaal voor smoke-test selectie |
| **personalized_opener** (Claude-gegenereerd) | **hoog** | **Z1** | Sami moet de Claude-tekst kunnen lezen + edit zonder /campagnes/nieuw te openen |
| **company_summary** (Claude) | midden | Z1 | Quick context bij lead-card hover |
| **personalization_hooks[]** | hoog | Z1 | Hooks vertellen je waarom een lead persoonlijk te benaderen is — nu onzichtbaar |
| **personalization_observations[]** | hoog | Z1 | Idem — quoteable observaties uit website |
| **review_best_quote** | hoog | Z1 + Z4 | Klant-quote die in mail kan; bij replies waardevol om context op te halen |
| **treatment_focus[]** | midden | Z1 | Sector-context voor sequence-keuze; zonder dit weet je niet welk archetype de lead serveert |
| **archetype_reason** (Claude motivatie) | midden | Z5 | Bij twijfel "waarom Z archetype?" zonder DB-query |
| **archetype_confidence** | hoog | Z5 | <0.7 = handmatige review nodig — moet zichtbaar zijn |
| **latest_review_date / days_since_last_review** | midden | Z1 | BLOK B (review-cadans) selectie; zonder dit raden |
| **local_competitors_higher_rating** | midden | Z1 | Pijn-druk voor BLOK B mail; competitive context |
| **score_vs_market** | midden | Z1 | "X punten onder concurrenten" — sales-pitch ammunitie |
| **meta_ads_active / ad_focus** | midden | Z1 | BLOK C (ads) selectie; nu invisible |
| **website_age_years** | midden | Z1 | BLOK A (website) trigger — leeftijd zichtbaar maakt waarom blok gekozen |
| **website_score / visual_score** | hoog | Z1 | Trojan-horse waarde-indicator (slecht site = goede pitch) |
| **has_online_booking / has_whatsapp / has_instagram** | midden | Z1 | Tech-signals voor BLOK D (operational) |
| **booking_system** (gedetecteerd platform) | laag | — | Te specifiek voor kaart, ok in detail |
| **cms_detected** | laag | — | Idem |
| **email_status NIET-verified context** (risky/catchall reden) | hoog | **Z3** | Per lead: waarom is deze risky? Welke check faalde? |
| **kvk_number / kvk_sbi_code** | n.v.t. | — | KvK opt-in geskipt (kost) |
| **imported_at / imported_by / imported_source** | midden | Z6 | Audit: deze lead kwam uit CSV-X op datum-Y; voor smoke-test recon |
| **next_send_at** (uit lead_campaign_history join) | hoog | Z1 + Z4 | "Volgende mail uitgaat over X uur" — niet nu zichtbaar |
| **outbound_count / inbound_count** | midden | Z4 | Hoe vaak hebben we gestuurd / replied; per lead. |
| **next_action_due** (uit derive_status) | hoog | Z1 | recontact_later toont datum — actie-driven CRM |
| **contact_first_name / contact_last_name / contact_title / contact_linkedin_url** | hoog | Z1 + Z6 | Zonder contact_first_name → "Hoi daar" fallback (niet ideaal). Zichtbaar maken voor manual-fix |

---

## Tabel: `heatr_lead_campaign_history`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| sequence_steps (frozen JSON) | ✗ | hoog | **Z1** — exacte verstuurde tekst per lead reproduceerbaar |
| step_index | ✗ | hoog | Z1 + Z4 — "lead is op step 2/3" — zichtbaar in card |
| sent_at (last) | gedeeltelijk (last_outbound_at) | ok | Z4 |
| next_send_at | ✗ | **hoog** | Z1 — wanneer stuurt de volgende mail? |
| status (pending/blocked/sequence_complete/etc.) | gedeeltelijk | hoog | Z1 + Z4 — granulariteit ontbreekt in kanban |
| block_reason | ✗ | hoog | **Z2 + Z3** — waarom geblokkeerd (bv. cost-cap, bounce, dedup)? |
| is_active | gedeeltelijk (afgeleid in status-derivatie) | ok | Z1 |
| inbox_id | ✗ | midden | Z2 — vanuit welke inbox verzonden? Voor reputation-monitoring |

---

## Tabel: `heatr_reply_inbox`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| body_text / body_html | gedeeltelijk (inbox-pagina) | hoog | **Z4** — preview in CRM-card cruciaal voor eerste replies |
| classification (interested/not_now/etc.) | gedeeltelijk | hoog | Z4 — kanban-kaart kleur-coderen op laatste classifier |
| classifier_summary (Claude) | gedeeltelijk | hoog | Z4 — quick-glance "wat zei deze lead?" |
| sentiment | ✗ | midden | Z4 — voor cohort-trend |
| return_date (uit not_now reply) | ✗ | hoog | Z1 — wanneer terugkomen, datum-zichtbaar |
| referred_to (wrong_person reply) | ✗ | hoog | Z1 + Z4 — naar wie doorverwezen? |
| from_email / from_name (responder) | gedeeltelijk | midden | Z4 |
| received_at | gedeeltelijk (last_inbound_at) | ok | Z4 |

---

## Tabel: `heatr_enrichment_jobs` (queue-state)

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| status (pending/running/failed) | ✗ in CRM (wel in /analytics) | **hoog** | **Z2 + Z5** — per lead: "deze wacht nog op enrichment" zichtbaar |
| current_step | ✗ | hoog | Z2 + Z5 — bij sticky job: welke step hangt? |
| retry_count | ✗ | midden | Z2 — herhaalde failures per lead detecteren |
| error_message | ✗ | hoog | Z2 + Z6 — wat ging mis bij deze lead? |
| completed_at | ✗ | midden | Z6 — enrichment-vers-heid (oud = re-enrich nodig) |

---

## Tabel: `heatr_blocked_sends`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| reason | ✗ | hoog | **Z3 + Z2** — bounce-rate cohort, cost-cap, dedup-block — onzichtbaar |
| timestamp | ✗ | midden | Z3 — bounce-rate-trend per dag |
| lead_id + campaign_id | ✗ | hoog | Z3 — welk percent van een campagne werd geblokkeerd? |

---

## Tabel: `heatr_lead_timeline`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| event_type (email_sent / reply_received / bounced / sequence_complete / manual_override) | gedeeltelijk (lead-detail) | hoog | **Z4 + Z6** — chronologische timeline ontbreekt in kanban-card-flip |
| title | gedeeltelijk | midden | Z4 |
| metadata | ✗ | midden | Z6 — debug bij rare cases |
| created_by (user/service/feedback_processor) | ✗ | midden | Z6 — audit |

---

## Tabel: `heatr_campaigns` (audit-trail)

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| sequence_snapshot (frozen sequence) | ✗ | hoog | **Z1** — wat is precies verstuurd, achteraf? Voor compliance + replay |
| personalization_gate (auto/review/skip counts) | ✗ | midden | Z1 — campagne-launch retrospectie |
| created_by / created_via | ✗ | midden | Z6 — service vs user trigger |
| inbox_ids gebruikt | ✗ | midden | Z2 — inbox-rotatie tracking |

---

## Tabel: `heatr_api_cost_log`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| cost_eur per lead | ✗ in CRM (wel in /analytics) | midden | Z6 — per-lead cost zichtbaar bij debugging |
| context (welke step) | ✗ | midden | Z2 — welke step kostte het meest |

---

## Tabel: `heatr_feedback_runs`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| reply_rate / bounce_rate per run | ✗ in CRM (wel in /analytics) | hoog | **Z3** — trend zichtbaar per cohort |
| insights[] (Claude-output) | ✗ | midden | Z3 — "deze sector heeft 30% lagere reply-rate" → CRM-banner |
| adjustments[] | ✗ | laag | — out-of-scope voor CRM |

---

## Tabel: `heatr_website_intelligence`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| visual_analysis_text (Sonnet Vision-output) | ✗ | hoog | Z1 — Trojan-horse pijn-bewijs voor sales-pitch |
| screenshot_url | ✗ | hoog | Z1 — visueel referentiepunt bij lead-card |
| top_strengths[] / top_weaknesses[] | ✗ | hoog | Z1 — instant pitch-ammunitie |
| competitor_data | gedeeltelijk (analytics aggregate) | midden | Z1 |
| score_vs_market | gedeeltelijk (lead-veld mirror) | hoog | Z1 |

---

## Tabel: `heatr_companies_raw`

Niet relevant voor CRM (pre-classificatie data, wordt al verwerkt naar
`heatr_leads`). Skip.

---

## Tabel: `heatr_competitor_cache`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| competitors[] (top 3 per sector+city) | ✗ | midden | Z1 — concrete benchmark-namen voor pitch-context |

Te detailistisch voor kanban; ok in lead-detail.

---

## Tabel: `heatr_crm_tasks`

Bestaande task-tabel. Onbekend hoeveel actief gebruikt — vermoedelijk
onderbenut.

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| title / description | gedeeltelijk (legacy /crm pipeline view) | hoog | Z1 + Z4 — task-systeem als reminder hoort in /crm/activity |
| due_date / status / priority | gedeeltelijk | hoog | Z1 |
| snoozed_until | ✗ | hoog | Z1 — recontact_later moet hier landen |

---

## Tabel: `heatr_crm_deals`

Bestaande deals-tabel. Niet eens in `_HEATR_TABLES` allowlist gechecked
recent — mogelijk dood. Voor solo-tool met geen revenue-tracking nu: laag.

---

## Tabel: `heatr_lead_outreach_snapshots`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| pre_send_lead_state (frozen) | ✗ | midden | Z6 — debugging "wat was de lead-data toen ik 'm pushte" |

Niet kritiek voor CRM-card, wel waardevol bij retrospectie.

---

## Tabel: `heatr_lead_discovery_schedules`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| schedule_name + cadence | ✗ | midden | Z2 — recurring discovery zichtbaar in CRM-header? |
| next_run_at | ✗ | midden | Z2 |

Out-of-scope voor lead-CRM, hoort op een schedules-pagina.

---

## Tabel: `heatr_import_runs`

| Veld | UI | Waarde | Zwakte |
|---|---|---|---|
| started_at + summary | ✗ | midden | Z6 — recent imports filterable in CRM ("toon mij alleen leads van CSV-X") |
| imported_by | ✗ | laag | — solo, audit-niveau-detail |

---

## Tabel: `heatr_system_state`

Configuration cache (override-flags etc.). Geen direct lead-niveau impact, skip.

---

## Tabel: `heatr_rate_limit_state`

Pure infrastructure. Skip.

---

## Tabel: `heatr_claude_cache` / `heatr_vision_cache`

Cache-state. Skip voor CRM.

---

## Tabel: `heatr_workspaces` / `heatr_sector_configs`

Config-tabellen. Skip.

---

## Samenvatting per zwakte

### Z1 — Sequence-tekst niet gereviewd (=hoogste data-onbenutting)
**Onbenutte velden die direct adresseren:**
`personalized_opener`, `personalization_hooks[]`, `personalization_observations[]`,
`review_best_quote`, `archetype_reason`, `next_send_at`, `next_action_due`,
`sequence_snapshot`, `step_index`, `top_strengths/weaknesses`, `screenshot_url`,
`visual_analysis_text`, `latest_review_date`, `local_competitors_higher_rating`,
`score_vs_market`, `recontact_after`, `contact_first_name`.

→ De huidige CRM toont status maar geen **inhoud** van wat verstuurd zou worden.

### Z2 — Worker-uptime
**Onbenutte velden:** `enrichment_jobs.status/current_step/retry_count/error_message`,
`block_reason` op campaign_history, `inbox_id` rotatie, `discovery_schedules.next_run_at`.

→ Worker-pulse in header toont aanwezigheid; **welke specifieke leads vast zitten** is invisible.

### Z3 — Bounce-risk / email-verifier
**Onbenutte velden:** `email_status` met reden (niet alleen 'risky' label),
`blocked_sends.reason`, `feedback_runs.bounce_rate`.

→ Bounce-rate per cohort / per inbox / per archetype ontbreekt volledig in CRM.

### Z4 — Reply-flow
**Onbenutte velden:** `reply_inbox.body_text/classification/sentiment/return_date/referred_to`,
`lead_timeline.event_type` chronologie, `step_index`, `inbound_count`.

→ Eerste replies zijn extra fragiel — moeten extra zichtbaar worden voor manual review.

### Z5 — Archetype/enrichment-coverage
**Onbenutte velden:** `archetype_confidence`, `enrichment_jobs.current_step`,
per-lead "X van 16 steps complete" indicator.

→ Sequence-launch zou geblokkeerd moeten zijn voor leads met <100% enrichment OF
confidence <0.7 — nu gebeurt dat niet zichtbaar.

### Z6 — Geen E2E-tests
**Onbenutte velden:** `imported_source` (filter "test-leads"), `lead_outreach_snapshots`,
`enrichment_jobs.error_message`, timeline-events met `created_by="service"`.

→ Test-mode-toggle per lead ontbreekt; met goede metadata-velden in CRM kan je
"deze lead is een testlead, kill-switch override" expliciet maken.

---

## Conclusie

**Top-5 hoogwaardige onbenutte velden** (impact op meerdere zwaktes tegelijk):

1. `personalized_opener` (Claude tekst) — Z1, kern van sequence-review-flow
2. `next_send_at` — Z1 + Z4, "wat staat gepland?" zichtbaarheid
3. `enrichment_jobs.status/error_message` — Z2 + Z5 + Z6, debug-laag
4. `archetype_confidence` — Z5, manual-review-trigger
5. `email_status` zichtbaar in CRM-card (niet alleen launcher) — Z3

**Rondom deze 5 hoort de Fase-2 prioritering te draaien.**
