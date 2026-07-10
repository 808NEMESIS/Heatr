# Pre-fix baseline — Outbound Safety Foundation

**Datum meting:** _in te vullen door Sami (draai de queries in de Supabase SQL-editor VÓÓR migratie 022 + deploy)_
**Doel:** vastleggen wat het systeem deed vóór Werkpakket A, zodat de "na"-meting betekenis heeft (actieplan §4.3).

> **Verwachting vooraf (falsificatie-check):** `heatr_outbound_log` heeft **0 rijen** — de dispatcher schreef door de prefix-bug naar een niet-bestaande tabel. Een niet-lege tabel zou betekenen dat er een onbekend schrijfpad bestaat → eerst uitzoeken vóór migratie 022 sectie B.

## Queries

```sql
-- 1. Ledger-omvang (verwacht: 0)
SELECT COUNT(*) AS outbound_log_rows FROM heatr_outbound_log;

-- 2. Enrollment-tabel-omvang (verwacht: 0 — geen enkele insert in de code)
SELECT COUNT(*) AS lch_rows FROM heatr_lead_campaign_history;

-- 3. Suppression-desync: unsubscribed op email_status maar status niet geblokkeerd
--    (dit zijn de leads die audit v2 P0-2 opnieuw aanschrijfbaar noemde)
SELECT COUNT(*) AS unsub_desync
FROM heatr_leads
WHERE email_status = 'unsubscribed'
  AND status NOT IN ('unsubscribed', 'forgotten', 'disqualified');

-- 4. Bounce-desync: gebounced maar nog campaign-eligible op status
SELECT COUNT(*) AS bounce_eligible
FROM heatr_leads
WHERE email_status = 'bounced'
  AND status NOT IN ('unsubscribed', 'forgotten', 'disqualified');

-- 5. Webhook-volume laatste 30 dagen als proxy voor unsubscribe/bounce-events
SELECT classification, COUNT(*)
FROM heatr_reply_inbox
WHERE received_at > now() - interval '30 days'
GROUP BY classification ORDER BY 2 DESC;

-- 6. Sends per dag (proxy via lead_timeline email_sent-events, laatste 14 dagen)
SELECT date_trunc('day', created_at) AS dag, COUNT(*)
FROM heatr_lead_timeline
WHERE event_type = 'email_sent' AND created_at > now() - interval '14 days'
GROUP BY 1 ORDER BY 1 DESC;

-- 7. Vergeten leads met de gedeelde placeholder (forget-collision-risico, PR 5/fase 2)
SELECT COUNT(*) AS forgotten_placeholder
FROM heatr_leads WHERE email = 'verwijderd@anoniem.nl';
```

## Resultaten (invullen)

| # | Metriek | Waarde | Datum |
|---|---|---|---|
| 1 | outbound_log_rows | | |
| 2 | lch_rows | | |
| 3 | unsub_desync | | |
| 4 | bounce_eligible | | |
| 5 | reply-classificaties 30d | | |
| 6 | sends/dag 14d | | |
| 7 | forgotten_placeholder | | |

## Na-meting (na deploy, zelfde queries + extra)

```sql
-- Ledger leeft: verwacht groeiend, met status-verdeling
SELECT status, COUNT(*) FROM heatr_outbound_log GROUP BY status;
-- Unique index actief:
SELECT indexname FROM pg_indexes WHERE indexname = 'uq_outbound_log_active_key';
```

Succescriterium WP-A: metriek 1 groeit vanaf deploy (elke send = een rij), en er verschijnen géén dubbele `completed`-rijen per (workspace_id, idempotency_key).
