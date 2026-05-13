# Heatr — Go-Live Session

## Context
Heatr staat op 8/10 productie-readiness. 23+ endpoints, 15-step enrichment
(KvK opt-in geskipt), 9-status CRM, 248/248 tests groen, 856 leads in pipeline.
**Geen mail is ooit via deze pipeline verstuurd.** Alle Warmr-koppeling = theorie.

CLAUDE.md + MEMORY.md auto-geladen — bevat state-of-analyse van vorige sessies.

## Hoe Heatr en Warmr samenwerken
**Heatr = brein, Warmr = handen.**
- Heatr discovers, enricht, scoort, classificeert archetype, kiest sequence,
  rendert per lead unieke opener via Claude Haiku
- Heatr pusht naar Warmr via `WarmrClient.push_lead()` met `custom_fields.opener`
  + 25 andere velden
- Warmr substitueert `{{first_name}}` + `{{opener}}` in stored sequence body,
  verzendt vanaf warm-inbox
- Reply → Warmr webhook naar `/webhooks/warmr` (HMAC via WARMR_WEBHOOK_SECRET)
- Heatr classificeert + drafter geeft antwoord-suggestie in /inbox

## Doel
Eerste live campagne mogelijk maken: 1 testlead → 5 → 20. Geen features.

## Eerst zelf checken (geen vraag stellen)
- /healthz, /analytics/queue-health, /analytics/enrichment-coverage
- pgrep run_enrichment_worker
- ps eww | grep -E "WARMR_|HEATR_" om gezette env-vars te zien
- python3 scripts/render_warmr_payload.py --first om Mail 1 visueel te checken
- Pas dán vragen wat ontbreekt

## 3 blockers, in deze volgorde

### 1. Warmr-koppeling functioneel
- Env-vars: WARMR_API_URL, WARMR_API_KEY, WARMR_WEBHOOK_SECRET
- **Eerst handmatige curl-smoketest naar Warmr buiten Heatr om** —
  anders debug je twee onbekenden tegelijk
- Daarna `WarmrClient.push_lead` integratie testen
- Warmr-side: inbox cap ≥30/dag + warmup ≥70, sequence template met
  {{first_name}}+{{opener}} placeholders, webhook met matching secret
- `_build_lead_payload` schema (integrations/warmr_client.py) valideren
  tegen daadwerkelijke Warmr API-spec

### 2. Email-verifier (Pad A staat al aan)
HEATR_ALLOW_RISKY_EMAILS=true is default — `risky` leads passeren gate.
- Verify: filter_sendable_leads() correct in /campaigns/launch flow
- Bounce-rate eerste cohort meten via /analytics/email-status-breakdown
- Als bounces >2%: switch naar Pad B (eigen SMTP-verifier in
  enrichment/email_verifier.py)

### 3. End-to-end smoke test
- 1 testlead (jouw email of vriend)
- ENABLE_CAMPAIGN_SENDS=true, launch via /campaigns/launch met X-API-Key
- Stop-criterium: Mail 1 ontvangen + opener correct + reply binnen webhook +
  classifier-tag + drafter-suggestie
- Pas dán naar 5 leads. Niet 20 vóór 5-cohort 24u zonder bounces gedraaid heeft.

## Niet deze sessie
- Frontend Supabase JWT (45m, geen blocker local-only)
- Pagespeed API
- Worker als systemd
- Sequence-tekst review (Sami's stem, eigen sessie)
- Nieuwe features (zeg me als ik scope-creep, ik wijs je erop)

## Werkwijze
- 1-2 taken per prompt
- Test bij elke wijziging
- Eerlijk: "geverifieerd" vs "aanname"
- Kill-switch blijft uit tot blok 3 stop-criterium gehaald

## Vragen
1. WARMR_API_URL + WARMR_API_KEY al beschikbaar?
2. Welke email als testlead?
