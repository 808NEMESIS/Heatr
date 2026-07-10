# Outbound send-path-matrix

**Datum:** 2026-07-10 · **Bron:** Werkpakket A (Outbound Safety Foundation), actieplan §4.1
**Verificatie:** repo-brede grep op `dispatch_outbound` + alle directe `warmr_client`-send-aanroepen. Elke rij is `file:line`-verankerd.

## Kernconclusie

Er bestaan **7 dispatcher-callsites** en **0 send-paden die om de dispatcher heen lopen**. Alle directe `WarmrClient`-calls (`push_lead`, `push_leads_bulk`, `create_campaign`) zitten binnen `send=lambda:`-closures die uitsluitend door `dispatch_outbound` worden aangeroepen. `utils/outbound_dispatcher.dispatch_outbound` is dus de feitelijke centrale egress — de kill-switch en de idempotency-reservering kunnen daar op één plek landen.

## Matrix

| # | Pad | Caller | Kind | Kill-switch (vóór WP-A) | Compliance | Idempotency-key | Ledger-tabel | Extern side-effect |
|---|---|---|---|---|---|---|---|---|
| 1 | `POST /leads/send-to-warmr` | operator/n8n (service-key) | `warmr_bulk_push` | ❌ geen | `compliance_check` per lead (main.py:620) + dispatcher-vangnet | `warmr-bulk:{campaign_id}:{ids_hash}` (main.py:654) | `outbound_log` → **niet-bestaand vóór fix** | `client.push_leads_bulk` (main.py:657) |
| 2 | `POST /leads/{id}/send-review-email` | operator | `warmr_push` | ❌ geen | dispatcher-vangnet | `review-email:{lead_id}` (main.py:756) | idem | `client.push_lead` (main.py:761) |
| 3 | `POST /campaigns/launch` — campaign create | operator (service-key only) | `warmr_campaign_create` | ✅ `ENABLE_CAMPAIGN_SENDS` (main.py:1457) | `filter_launchable_leads` + cooldown + personalisatie-gate + dispatcher-vangnet | `campaign-create:{naam}:{template}:{ids_hash}` (main.py:1600) | idem | `client.create_campaign` (main.py:1603) — **Warmr dript daarna server-side** |
| 4 | `POST /campaigns/launch` — bulk push | idem | `warmr_bulk_push` | ✅ idem (zelfde endpoint) | idem | `campaign-push:{camp_id}:{ids_hash}` (main.py:1628) | idem | `client.push_leads_bulk` (main.py:1631) |
| 5 | `process_due_send` (Model B, n8n /15min) | scheduler | `warmr_push` | ❌ geen | SendingGuard + dispatcher-vangnet | `seq-send:{record}:step:{i}:epoch:{e}` (sequence_engine.py:450) | idem | `wc.push_lead` met gerenderde subject/body (sequence_engine.py:453) — *vandaag inert: lege enrollment-tabel* |
| 6 | Operator-briefing (dagelijkse cron) | scheduler | `operator_email` | ❌ geen | n.v.t. (geen prospect) | `briefing:{workspace}:{date}` (main.py:4659) | idem | interne e-mail |
| 7 | Alerts (`utils/alert_manager.py:122`) | systeem | `operator_email` | ❌ geen | n.v.t. | record-only (`enforce_idempotency=False`) | idem | interne melding — suppressie gevaarlijker dan duplicaat |

**Niet-sendende Warmr-calls (geen bericht, geen dispatcher nodig):** `get_ready_inboxes` (main.py:642, 1799), campaign-stats reads (main.py:1721-1728), `pause/resume`-controls. Webhook-inbound (main.py:3102) verstuurt zelf niets.

## Bekende gaten (gefixt in WP-A)

1. **Ledger schrijft naar niet-bestaande tabel** — `outbound_log` staat niet in `_HEATR_TABLES` (config/database.py:29-44); migratie 020 maakte `heatr_outbound_log`. Elke insert/lookup faalt → idempotency fail-open op alle 7 paden. → WP-A stap 3.
2. **Geen database-UNIQUE op de key** — `idx_outbound_log_key` (020:28) is een gewone index; check-then-send is racegevoelig. → migratie 022 + atomische `in_flight`-reservering.
3. **Kill-switch dekt alleen pad 3/4** — paden 1, 2 en 5 hebben géén master-switch. → centrale check in `dispatch_outbound` (`ENABLE_PROSPECT_SENDS`, fallback `ENABLE_CAMPAIGN_SENDS`); operator-paden 6/7 achter `ENABLE_INTERNAL_NOTIFICATIONS`.
4. **Fail-open bij ledger-fout** — module-docstring documenteert bewust fail-open; Werkpakket A draait dit om naar **fail-closed voor prospect-kinds** (Besluit 3), fail-soft voor operator-kinds.

## Residueel risico (bewust buiten WP-A-scope)

- **Timeout-na-acceptatie bij Warmr:** zonder `Idempotency-Key`-header richting Warmr kan een geaccepteerde-maar-getimeoute send bij retry alsnog dubbel bezorgd worden (de ledger markeert `failed_retryable`; de externe staat is onbekend). Definitieve fix = Warmr-contract (actieplan fase 3+/§7 audit v2). Het reserveringsmodel verkleint het venster; de runbook beschrijft de handmatige check.
- **Pad 5 blijft inert** zolang `lead_campaign_history` niet gevuld wordt — bewust: geen enrollment-writes in WP-A (geen tweede send-engine naast Warmr's drip).
