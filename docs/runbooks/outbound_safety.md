# Runbook — Outbound Safety Foundation (Werkpakket A + fase 2)

**Datum:** 2026-07-10 · **Scope:** dispatcher-reservering, ledger, kill-switch, suppression/GDPR
**Code:** `utils/outbound_dispatcher.py`, `utils/suppression.py`, `utils/gdpr_manager.py`
**Migraties:** `022_outbound_ledger_unique.sql`, `023_email_status_check_fix.sql`, `024_suppressions.sql`

---

## 1. Deploy-volgorde (VERPLICHT)

1. **Eerst** in de Supabase SQL-editor, in deze volgorde:
   - migratie **022** (ledger-UNIQUE; sectie A moet 0 rijen geven),
   - migratie **023** (email_status-CHECK-fix — de huidige CHECK wijst de bounce/unsub-webhook-writes af),
   - migratie **024** (suppressions-tabel — de dispatcher is er fail-closed op),
   - migratie **025** (tracking-enrollments: send_owner-kolom + UNIQUE — launch is er fail-closed op),
   - migratie **026** (webhook-eventledger — inbound is er fail-soft op, maar zonder tabel geen dedup).
2. **Daarna** de code deployen (commits `a4623af` t/m `abbc794` — WP-A + fase 2 + fase 3).
3. **Env-check vóór restart:** staat `ENABLE_CAMPAIGN_SENDS=true` in de productie-`.env`? Dan blijft alles versturen zoals nu (de nieuwe `ENABLE_PROSPECT_SENDS` valt daarop terug). Staat hij op `false`, dan blokkeert de dispatcher vanaf deploy **álle** prospect-sends — dat is de bedoelde master-switch-semantiek, maar verifieer dat dit gewenst is.

Andersom deployen (code vóór migraties) is fail-closed maar disruptief: elke prospect-send krijgt `DispatchLedgerUnavailable` tot de tabellen bestaan… wat nog steeds veiliger is dan het oude fail-open.

### Fase 2 — wat er extra bij kwam (2026-07-10)
- `bounced` is een geblokkeerde status; bounce/unsubscribe-webhooks zetten nu óók `leads.status` (de kolom die `compliance_check` leest).
- `is_test_lead` bypasst suppressie-statussen niet meer.
- `forget_lead` gebruikt een per-lead-unieke placeholder (`verwijderd+{uuid}@anoniem.invalid`), retourneert eerlijk `ok/errors`, en `/gdpr/forget` geeft 500 `forget_incomplete` bij een gedeeltelijke erasure (retry is idempotent).
- **`heatr_suppressions`** = platformbrede lijst (cross-workspace, op genormaliseerd adres). Writers: warmr-webhook, reply-classifier, gdpr-forget. Reader: de dispatcher, vóór elke prospect-send, **fail-closed**.
- Suppressie **revoken** (alleen operator, bewuste actie):
  ```sql
  UPDATE heatr_suppressions SET revoked_at = now()
  WHERE normalized_email = '<adres>' AND revoked_at IS NULL;
  ```
  Nooit rijen deleten (append-only). Let op: revoke heft alleen de platformlijst op; de per-lead `status`/`email_status` blijft staan en blokkeert mogelijk nog via `compliance_check`.

### Fase 3 — tracking-enrollments (ADR-001, 2026-07-10)
- **Besluit (docs/adr/ADR-001):** Warmr dript; Heatr trackt. Launch en `/leads/send-to-warmr` schrijven per gepushte lead een `lead_campaign_history`-rij met `send_owner='warmr'`, `status='active'` — **nooit `'pending'`**. `get_due_sends` selecteert uitsluitend `send_owner='heatr'`; dat is de harde grens tegen dubbele drip.
- **Launch blokkeert nu echt dubbele campagnes:** leads met een `is_active=true`-enrollment worden geweigerd (óók onder een andere campagnenaam), en na afronding geldt de 90-dagen-cooldown (anker = `sent_at`, gezet bij closure). Fail-closed: is `lead_campaign_history` niet leesbaar, dan weigert launch (503).
- **Webhook sluit enrollments:** reply/bounce/unsubscribe/`campaign.completed` → `is_active=false` + terminale status, gescoped op `campaign_id` (indien in payload) en met terminal-guard (een `no_response` overschrijft nooit `replied`). 
- **Incident "lead onterecht geblokkeerd als in-actieve-campagne":** check `SELECT * FROM heatr_lead_campaign_history WHERE lead_id='<id>' AND is_active=true;` — hoort na `campaign.completed` leeg te zijn. Blijft een rij hangen (webhook gemist), sluit handmatig: `UPDATE heatr_lead_campaign_history SET is_active=false, status='no_response', stopped_at=now(), sent_at=COALESCE(sent_at, now()) WHERE id='<rij>';`

### Fase 3 — webhook-eventledger + herbedrade SendingGuard (PR 10/11)
- **Elke inbound Warmr-event** krijgt een rij in `heatr_webhook_events` (dedup op `event_id`; zonder Warmr-event_id synthetiseert Heatr er één over de payload). Redelivery → `{"ok": true, "duplicate": true}`, geen dubbele side-effects.
- **Dead-letter-reconciliatie (wekelijks checken):**
  ```sql
  SELECT event_type, error, created_at FROM heatr_webhook_events
  WHERE processing_status IN ('dead_letter','error')
  ORDER BY created_at DESC LIMIT 50;
  ```
  `dead_letter` = event voor een onbekende lead (na warmr_lead_id-fallback). Onderzoek de bron; een gefixte lead-koppeling + handmatige replay van de payload verwerkt hem alsnog (de event_id botst dan — verwijder desnoods bewust die ene rij, gedocumenteerde uitzondering op append-only).
- **SendingGuard is nu fail-closed:** guard-data onbereikbaar (lch/suppressions niet leesbaar) = prospect-send geblokkeerd met een "Interne fout in SendingGuard"-reden. Dat is bedoeld gedrag — herstel de data-laag, omzeil de guard niet.
- **Bounce-breaker** telt `heatr_suppressions` type `hard_bounce` (vandaag, per workspace) tegen de pushes van vandaag; drempel `MAX_BOUNCE_RATE` (default 3%, min. 10 sends). De **domein-cap is bewust uitgeschakeld** tot Warmr capacity-events levert — er is geen kolom die hem eerlijk kan voeden.

### Fase 4 — workspace fail-closed (PR 12)
**Een JWT zonder `app_metadata.workspace_id`-claim wordt GEWEIGERD** (401). Vóór deze deploy of direct erna: provision de claim voor bestaande Supabase-users, anders sluit de frontend zichzelf buiten:
```sql
-- In de Supabase SQL-editor (past alleen users aan die de claim nog missen):
UPDATE auth.users
SET raw_app_meta_data = COALESCE(raw_app_meta_data, '{}'::jsonb)
                        || '{"workspace_id": "aerys"}'::jsonb
WHERE raw_app_meta_data->>'workspace_id' IS NULL;
-- Verificatie (verwacht: 0):
SELECT count(*) FROM auth.users WHERE raw_app_meta_data->>'workspace_id' IS NULL;
```
Let op: bestaande sessies dragen de oude JWT tot refresh — gebruikers moeten mogelijk opnieuw inloggen. Noodknop tijdens de cutover: `HEATR_JWT_WORKSPACE_FALLBACK=true` (tijdelijk! elke fallback wordt luid gelogd; zet uit zodra de verificatie-query 0 geeft). De service-key-route blijft `DEFAULT_WORKSPACE` — per-workspace service keys zijn een aparte migratie (actieplan PR 12-vervolg).

## 2. Wat is er veranderd (operator-samenvatting)

| Vóór WP-A | Ná WP-A |
|---|---|
| Ledger schreef naar niet-bestaande tabel → 0 rijen, idempotency fail-open | Ledger schrijft naar `heatr_outbound_log`; **INSERT `in_flight` = eigenaarstoewijzing** (partial UNIQUE) |
| Check-then-send race: dubbele sends mogelijk | Max. één eigenaar per key; verliezer krijgt `skipped_duplicate` |
| Kill-switch alleen op `/campaigns/launch` | `ENABLE_PROSPECT_SENDS` stopt élk prospect-pad centraal in de dispatcher |
| Ledger-fout → send ging gewoon door | Prospect-kinds **fail-closed** (`DispatchLedgerUnavailable`); alleen `operator_email` fail-soft |

## 3. Statussen in `heatr_outbound_log`

| Status | Betekenis | Muteerbaar? |
|---|---|---|
| `in_flight` | Reservering geclaimd, send loopt | → completed / failed_* (CAS) |
| `completed` | Send bevestigd gelukt | nee (terminaal, blokkeert key) |
| `failed_retryable` | Netwerk/timeout/5xx — retry mag (nieuwe reservering) | nee |
| `failed_terminal` | Definitieve 4xx — request zelf fout | nee |
| `blocked_compliance` / `blocked_killswitch` / `skipped_duplicate` | Append-only audit-rijen | nooit |

## 4. Incident-scenario's

### 4a. "Sends worden geblokkeerd met DispatchLedgerUnavailable"
Ledger onbereikbaar. Check: (1) migratie 020+022 gedraaid? (2) staat `"outbound_log"` in `_HEATR_TABLES` (config/database.py)? (3) Supabase bereikbaar? Dit is **bewust fail-closed** — niet omzeilen door de dispatcher te patchen; herstel de ledger.

### 4b. "Key blijft hangen op in_flight"
Worker gecrasht tussen reservering en finalisatie. Na `OUTBOUND_INFLIGHT_STALE_MINUTES` (default 15) neemt de eerstvolgende dispatch de reservering automatisch over (rij → `failed_retryable`, nieuwe poging). Handmatig eerder vrijgeven:
```sql
UPDATE heatr_outbound_log
SET status='failed_retryable', error='handmatig vrijgegeven (runbook 4b)'
WHERE id='<rij-id>' AND status='in_flight';
```
**Let op:** controleer éérst bij Warmr of de send daadwerkelijk NIET vertrokken is (4c).

### 4c. `external_state_unknown` — timeout ná mogelijke acceptatie
Een `failed_retryable`-rij met `[external_state_unknown]` in de error betekent: Warmr kán de send geaccepteerd hebben terwijl Heatr een timeout zag. **Vóór een handmatige retry:** check in Warmr (campagne-leadlijst / verzendlog) of de mail al vertrok. Zo ja: markeer de rij handmatig `completed` i.p.v. te retryen. Dit residuele risico verdwijnt pas met een Idempotency-Key richting Warmr (actieplan fase 3).

### 4d. Noodstop: alle prospect-sends stoppen
```
ENABLE_PROSPECT_SENDS=false   # (of ENABLE_CAMPAIGN_SENDS=false als de nieuwe var niet gezet is)
```
+ proces-herstart (env wordt per call gelezen; herstart maakt het onmiddellijk en overal zeker). Interne briefings/alerts blijven lopen tenzij je óók `ENABLE_INTERNAL_NOTIFICATIONS=false` zet. Verifieer via de decision-logs: `blocked_killswitch`-regels verschijnen bij elke geweigerde poging.

## 5. Metrics (log-gebaseerd, tot er Prometheus is)

Elke dispatch logt één regel: `outbound_dispatch decision=<X> kind=… key=… workspace_id=… actor=… lead_count=… reason=…` (geen mailinhoud/PII).

```bash
# per decision tellen (bv. laatste 24h uit de app-log)
grep "outbound_dispatch" app.log | grep -oE "decision=[a-z_]+" | sort | uniq -c
```

| Decision | Alarm bij |
|---|---|
| `executed` | — (baseline-volume) |
| `skipped_duplicate` | plotselinge piek (dubbele ticks/retries) |
| `blocked_compliance` | **elke** — caller-gate heeft een gat |
| `blocked_killswitch` | onverwacht (switch hoort bewust om te staan) |
| `failed_retryable` met `external_state_unknown` | **elke** — runbook 4c draaien |
| `ledger_unavailable` | **elke** — runbook 4a |

SQL-equivalent (ledger zelf):
```sql
SELECT status, COUNT(*) FROM heatr_outbound_log
WHERE created_at > now() - interval '24 hours'
GROUP BY status ORDER BY 2 DESC;
```

## 6. Rollback

- **Code:** revert commits `869de08` en/of `a4623af`. Let op: revert van `a4623af` heropent P0-1 (fail-open ledger) — alleen doen bij een acute regressie, en de kill-switch (`ENABLE_PROSPECT_SENDS=false`) eerst dichtzetten.
- **Migratie 022:** `DROP INDEX IF EXISTS uq_outbound_log_active_key;` + `ALTER TABLE heatr_outbound_log DROP CONSTRAINT IF EXISTS heatr_outbound_log_status_check;` — niet-destructief, geen dataverlies.

## 7. Expliciet BUITEN scope van WP-A

Geen `lead_campaign_history`-writes (geen tweede send-engine naast Warmr's drip), geen suppression-tabel (fase 2), geen Warmr-Idempotency-Key (fase 3), geen workspace-hardening (fase 4). Zie `docs/actieplan_heatr_2026-07-10.md`.
