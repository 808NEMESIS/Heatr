# Sprint 2 / Stap 0 — Heatr Run-State Inventaris

**Datum:** 2026-07-06 · Read-only inventaris vóór de Control Plane-bouw.
Vraag: welke Control Plane-velden kunnen uit bestaande data getoond worden,
en welke bestaan nog niet?

## Bestaat — bruikbaar voor Inspect-laag v0

| Control Plane-veld | Bron | Granulariteit |
|---|---|---|
| Pipeline-stap (scrape→enrich→score→send→reply) | `leads.created_at` / `enrichment_version`+`enriched_at` / `scored_at` / `pushed_to_warmr_at` / `reply_inbox`-rows | **Grofmazig**: stap-afleiding uit timestamps, geen per-stap-events |
| Send-historie per lead | `lead_campaign_history` (step_index, status, is_active, next_send_at, sent_at, block_reason, **frozen `sequence_steps` JSONB**) + `email_events` (Warmr-webhook) + `reply_inbox` | Per campagne-koppeling; frozen sequence = wat er verstuurd is/wordt |
| Queue-state | `enrichment_jobs` (status, current_step, steps_completed[], retry_count, error_message), `scraping_jobs`, n8n-dispatch via `/sequences/due-sends` | Per job; steps_completed is array **zonder timestamps** |
| Geblokkeerde sends | `blocked_sends` (reason, lead, inbox, tijdstip) — geschreven door SendingGuard | Per block ✅ |
| Errors | `enrichment_jobs.error_message`, `leads.website_analysis_failed_reason/_at`, `blocked_sends.reason` | Laatste fout per domein |
| Retries | `enrichment_jobs.retry_count` (teller) | **Alleen teller** — zie "bestaat niet" |
| Cost per lead | `api_cost_log` (lead_id-attributie 100% sinds `1ef52f5`; context per enrichment-stap; cache_hit) | Per call ✅ |
| "Waarom deze lead" | `contact_why_chosen`, `archetype`+`archetype_reason`, `personalization_hooks`, `review_pain_points`, `icp_match` + `GET /leads/{id}/launch-readiness` | ✅ compleet (Capability 1+2) |
| Operator-events | `lead_timeline` — 10 event-types (email_sent, review_email_sent, deal_lost, stage_changed, snoozed, campaign_done, warmr_event, task_*, system) + reply_classified | Per actie, ná pre-conditie-fixes ook bulk-status + test-mode |
| Side-effect-bron | Sprint 1 Hidden Outbound Inventory: 9 paden (4 prospect-Warmr, 1 dead stub, 2 operator-Resend, SMTP-handshake, scraper-fetches) | Catalogus ✅; **geen ledger** — zie hieronder |

## Bestaat NIET — nieuw fundament (geen verificatie)

| Ontbrekend | Impact op v0 | Plan |
|---|---|---|
| **Append-only side-effect-ledger** over alle egress-paden | Inspect kan niet tonen "wat ging er wanneer, via welk pad, met welk resultaat naar buiten" | `heatr_outbound_log` via de dispatcher — **deze sprint** (I7-fundament) |
| **Idempotency-keys** op side-effects | Control-acties (retry/force/restart) zouden dubbele sends kunnen triggeren | Dispatcher-key per send-intentie — **deze sprint** (I6) |
| Per-attempt retry-history | `anthropic_retry` logt alleen naar de logger (bewijs S0c: geen DB-writes); enrichment_jobs heeft alleen een teller | v0 toont de teller; per-attempt-detail = I7-schuld, gemarkeerd in UI als "vereist run-history-fundament" |
| Step-timestamps in enrichment-runs | `steps_completed` is een naamlijst zonder tijden → geen echte step-timeline | v0 toont afgeleide grofmazige pipeline + jobvelden; fijnmazig = I7-schuld |
| Warmr-side send-bevestiging per mail | `email_events` dekt webhook-events (sent/opened/replied) voor zover Warmr ze stuurt | v0 toont wat er is; gaps zijn Warmr-koppeling, geen Heatr-schuld |

## Conclusie voor de bouw

Inspect v0 kan **echt** tonen: grofmazige pipeline-positie, volledige
send-historie + frozen sequences, queue/job-state met errors en
retry-tellers, alle blocks, cost per lead per stap, readiness + why, en —
zodra de dispatcher staat — de outbound-ledger. De fijnmazige step-timeline
en per-attempt-retries zijn eerlijk gemarkeerde I7-schuld.
