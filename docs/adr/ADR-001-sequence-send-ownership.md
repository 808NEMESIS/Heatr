# ADR-001 — Sequence send-ownership: Warmr dript, Heatr trackt

**Status:** Geaccepteerd · 2026-07-10
**Context:** actieplan fase 3 (docs/actieplan_heatr_2026-07-10.md §7) · audit v2 §7 (docs/lifecycle_audit_v2_2026-07-09.md)

## Probleem

Er bestaan twee verzendmodellen die elkaar tegenspreken:

1. **Model A (live):** `/campaigns/launch` post de volledige sequence-steps naar Warmr (`create_campaign`, `warmr_client.py:400-405`) — **Warmr dript mail 1/2/3 server-side**.
2. **Model B (ontworpen, inert):** `process_due_send` (sequence_engine) rendert en verstuurt per stap, gepolld door n8n op `lead_campaign_history WHERE status='pending' AND is_active=true`.

Zolang niets `lead_campaign_history` vult, is Model B dood en zijn álle dedup-, cooldown- en volume-checks blind (audit v2 F2). Maar de tabel *naïef* vullen met `status='pending'` activeert Model B **naast** Warmr's drip → **gegarandeerd dubbele mails 2/3** (audit v2, scenario 6). Er moet dus eerst één eigenaar zijn.

## Besluit

**Warmr is eigenaar van de dripsequence. Heatr maakt uitsluitend tracking-enrollments aan en verwerkt lifecycle-events.**

Concreet:
- Elke succesvolle Warmr-enrollment krijgt één `lead_campaign_history`-rij met **`send_owner='warmr'`** en `status='active'` — nooit `'pending'`.
- **`get_due_sends` selecteert uitsluitend `send_owner='heatr'`-rijen.** Dit is de harde technische grens die dubbele drip uitsluit, óók als iemand later per ongeluk een `pending`-status op een warmr-rij zet.
- Heatr verstuurt in deze fase **geen** zelfstandige sequence-stappen. `process_due_send` blijft bestaan (voor een toekomstige, expliciete migratie naar Heatr-owned sequencing) maar heeft per definitie nul warmr-rijen te verwerken.
- De webhook sluit enrollments af: `campaign.completed`/reply/bounce/unsubscribe zetten `is_active=false` + `sent_at` (cooldown-anker) + terminale status, gescoped op `campaign_id` waar de payload die levert, met een terminal-guard (een `no_response` overschrijft nooit `replied`/`unsubscribed`/`bounced`).

## Gevolgen

**Positief:** dedup (`is_lead_in_active_campaign`), 90-dagen-cooldown (`campaign_cooldown_block`) en de volume-guards krijgen voor het eerst echte data zonder een tweede send-engine te activeren; multi-dienst-enrollment wordt een expliciete, auditeerbare rij; de feedback-processor ziet echte terminale statussen.

**Beperkingen (geaccepteerd):** Heatr kent de exacte send-momenten van mail 2/3 niet (Warmr dript autonoom) — `sent_at` wordt bij afronding/stop gezet, waardoor de 90d-cooldown vanaf dát moment loopt (conservatiever = langere bescherming). Exacte message-events volgen in PR 10 (webhook-eventledger) en het Warmr-contract.

**Verboden zolang dit ADR geldt:** `status='pending'`-rijen aanmaken vanuit launch; `send_owner='warmr'`-rijen aanbieden aan `process_due_send`; een tweede render/verzendpad voor dezelfde sequence.

## Heroverwegen wanneer

Migratie naar Heatr-owned sequencing (Model B) is een aparte beslissing die pas op tafel komt als: (a) de webhook-eventledger live is (ordering + dedup), (b) Warmr een relay-only-modus heeft (campagne zonder server-side steps), en (c) per-stap-verzending aantoonbaar nodig is (bv. AI-send-time-optimalisatie per stap). Tot die tijd wint operationele eenvoud: één dripper.
