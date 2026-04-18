# MVP_GAPS.md — Heatr hardening audit
*Gegenereerd sessie 7. Bijgewerkt naarmate gaps worden gedicht.*

---

## 1. Gestubde functies (return 0 / pass / leeg)

### 1a. Scoring stub in `queue/enrichment_queue.py`
**Locatie:** regel 357–364  
**Voor:** `# stub (always 0)` → score blijft 0 voor elke lead  
**Na:** Aangeroepen `scoring.lead_scoring.score_lead()` zodra die module beschikbaar is.  
**Tijdelijk fix (sessie 7):** Basisscoring direct in `_run_scoring_step()` — berekent score op basis van aanwezige velden.

### 1b. `sector_breakdown` in `GET /analytics/pipeline`
**Locatie:** `api/main.py` regel 621  
**Voor:** `"sector_breakdown": {}` — altijd leeg  
**Na:** Berekend vanuit leads tabel.

### 1c. `website_opportunities` in `GET /analytics/pipeline`
**Locatie:** `api/main.py` regel 629  
**Voor:** altijd 0  
**Na:** Count from `website_intelligence` where `opportunity_types != '{}'`.

### 1d. `disqualification_reason` kolom ontbreekt in `leads`
**Locatie:** `api/main.py` regel ~344 gebruikt de kolom in een `update()` maar die bestaat niet in het schema  
**Na:** `ALTER TABLE leads ADD COLUMN IF NOT EXISTS disqualification_reason TEXT;` toegevoegd aan schema.

### 1e. `company_enrichment.py` stille `pass` in cleanup
**Locatie:** regels 518, 548  
**Voor:** `except Exception: pass` — fouten worden stilzwijgend genegeerd  
**Na:** `logger.warning(...)` toegevoegd.

### 1f. `email_waterfall.py` stille `pass`
**Locatie:** regel 459  
**Voor:** `except Exception: pass`  
**Na:** `logger.warning(...)` met context.

---

## 2. Error paths zonder echte afhandeling

| Bestand | Probleem | Fix |
|---|---|---|
| `api/main.py` `/search` | `create_scraping_job` lazy import faalt als `queue/` ontbreekt | Try/except met 500 response |
| `api/main.py` `send_leads_to_warmr` | Warmr client constructor faalt als env vars missen — unhandled | Wrapped in try/catch, geeft 503 |
| `warmr_client.py` `_request()` | `return {}` bij 4xx/5xx zonder log | WarmrAPIError wordt nu gelogd |
| `enrichment_queue.py` scoring stub | `logger.warning("Scoring stub")` maar score blijft 0 forever | Basisscoring geïmplementeerd |

---

## 3. Missing database indexes voor 1.000+ leads

Toegevoegd in sessie 7 aan `supabase_schema.sql`:

```sql
-- leads tabel — meest bevraagde kolommen na workspace_id
create index if not exists leads_crm_stage       on leads(workspace_id, crm_stage);
create index if not exists leads_score           on leads(workspace_id, score desc);
create index if not exists leads_email_status    on leads(workspace_id, email_status);
create index if not exists leads_snoozed_until   on leads(snoozed_until) where snoozed_until is not null;
create index if not exists leads_next_contact    on leads(next_contact_after) where next_contact_after is not null;

-- lead_timeline — feed queries
create index if not exists timeline_workspace_created on lead_timeline(workspace_id, created_at desc);

-- crm_tasks — vandaag + overdue queries
create index if not exists tasks_due_open on crm_tasks(workspace_id, due_date, status) where status = 'open';

-- api_cost_log — dagelijkse aggregaten
create index if not exists cost_log_date_ws on api_cost_log(date, workspace_id);

-- blocked_sends — analyse
create index if not exists blocked_sends_ws_date on blocked_sends(workspace_id, blocked_at);

-- daily_metrics — periode queries
create index if not exists daily_metrics_ws_date on daily_metrics(workspace_id, date desc);
```

---

## 4. Niet-gevalideerde `.env` variabelen bij startup

Variabelen die worden gebruikt maar niet gevalideerd:

| Variabele | Gebruikt in | Gevolg als ontbreekt |
|---|---|---|
| `SUPABASE_URL` | `api/main.py` | `KeyError` bij eerste request |
| `SUPABASE_KEY` | `api/main.py` | `KeyError` bij eerste request |
| `ANTHROPIC_API_KEY` | `company_enrichment.py` | `AuthenticationError` bij eerste Claude call |
| `WARMR_API_URL` | `warmr_client.py` | Silent — Warmr calls mislukken |
| `WARMR_API_KEY` | `warmr_client.py` | 401 op elke Warmr call |
| `KVK_API_KEY` | `kvk_scraper.py` | Stap 4 waterval mislukt stilzwijgend |
| `PAGESPEED_API_KEY` | (toekomstig) | Pagespeed scores worden overgeslagen |
| `WARMR_WEBHOOK_SECRET` | `api/main.py` | Webhook verificatie uitgeschakeld |

**Fix:** `utils/startup_validator.py` — valideert alle kritieke vars en logt warnings voor optionele vars. Aangeroepen via FastAPI lifespan event.

---

## 5. Fixes per bestand

### `api/main.py`
- `sector_breakdown` berekend vanuit leads data
- `website_opportunities` count ingevuld
- `disqualification_reason` kolom gebruik bewaard, kolom toegevoegd aan schema
- Lazy imports gewrapped in try/except met informatieve 503 responses
- Lifespan event toegevoegd met `startup_validator.validate_startup()`
- `/health` endpoint toegevoegd
- Alle nieuwe endpoints toegevoegd (zie sectie 7 in prompt)

### `enrichment/company_enrichment.py`
- Stille `pass` exceptions vervangen door `logger.warning()`
- `cached_claude_call` gebruikt voor `infer_industry_claude()` en `generate_company_summary()`

### `enrichment/email_waterfall.py`
- Stille `pass` exception vervangen door `logger.warning()`

### `queue/enrichment_queue.py`
- `_run_scoring_step()` doet nu basisscoring in plaats van altijd 0

---

## 6. Kolommen die ontbraken in schema

Toegevoegd via ALTER TABLE in sessie 7:

```sql
-- Leads tabel
ALTER TABLE leads ADD COLUMN IF NOT EXISTS disqualification_reason TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS unsubscribed_at TIMESTAMPTZ;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS unsubscribe_source TEXT;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS next_contact_after TIMESTAMPTZ;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS contact_attempt_count INT DEFAULT 0;
ALTER TABLE leads ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';
  -- active | unsubscribed | forgotten | no_response | disqualified
```

---

*Alle gaps zijn gedicht in sessie 7. Volgende audit aanbevolen na sessie 8 (scoring engine).*
