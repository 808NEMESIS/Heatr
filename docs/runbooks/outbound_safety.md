# Runbook — Outbound Safety Foundation (Werkpakket A)

**Datum:** 2026-07-10 · **Scope:** dispatcher-reservering, ledger, kill-switch
**Code:** `utils/outbound_dispatcher.py` · **Migratie:** `migrations/022_outbound_ledger_unique.sql`

---

## 1. Deploy-volgorde (VERPLICHT)

1. **Eerst** migratie 022 in de Supabase SQL-editor draaien (sectie A t/m E; A moet 0 rijen geven).
2. **Daarna** de code deployen (commits `a4623af` + `869de08`).
3. **Env-check vóór restart:** staat `ENABLE_CAMPAIGN_SENDS=true` in de productie-`.env`? Dan blijft alles versturen zoals nu (de nieuwe `ENABLE_PROSPECT_SENDS` valt daarop terug). Staat hij op `false`, dan blokkeert de dispatcher vanaf deploy **álle** prospect-sends — dat is de bedoelde master-switch-semantiek, maar verifieer dat dit gewenst is.

Andersom deployen (code vóór migratie) is fail-closed maar disruptief: elke prospect-send krijgt `DispatchLedgerUnavailable` tot de index bestaat… wat nog steeds veiliger is dan het oude fail-open.

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
