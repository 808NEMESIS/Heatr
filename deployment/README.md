# Heatr — Deployment Guide

## Prerequisites

- Docker + Docker Compose (local) **or** a Railway account (cloud)
- Supabase project with schema migrated (`supabase_schema.sql`)
- Anthropic API key
- Warmr instance running + API key
- n8n instance (self-hosted or n8n Cloud)
- Resend account for alert + briefing emails (optional but recommended)

---

## 1. Environment variables

Copy `.env.example` to `.env` and fill in all values:

```bash
cp .env.example .env
```

Required:
```
SUPABASE_URL
SUPABASE_KEY          # service_role key
ANTHROPIC_API_KEY
WARMR_API_URL
WARMR_API_KEY
WARMR_WEBHOOK_SECRET
DEFAULT_WORKSPACE_ID  # e.g. "aerys"
OPERATOR_EMAIL        # receives critical alerts + daily briefing
```

Optional but recommended:
```
RESEND_API_KEY        # for alert + briefing emails
PAGESPEED_API_KEY
KVK_API_KEY
HEATR_BASE_URL        # public URL of this API, e.g. https://heatr.up.railway.app
```

---

## 2. Database migration

Run the schema against your Supabase project once:

```bash
# Via Supabase CLI
supabase db push

# Or paste supabase_schema.sql into the Supabase SQL editor
```

---

## 3a. Local — Docker Compose

```bash
docker compose up --build
```

API available at `http://localhost:8000`.
Docs at `http://localhost:8000/docs`.

---

## 3b. Cloud — Railway

```bash
# Install Railway CLI
brew install railway

# Login + link project
railway login
railway link

# Deploy
railway up
```

Railway reads `railway.toml` automatically. Set all env vars in the Railway dashboard under **Variables**.

Healthcheck: `GET /health` — Railway will restart the service if this returns non-2xx.

---

## 4. n8n workflows

Import all 10 workflows from `deployment/n8n-workflows/` into your n8n instance:

1. Open n8n → **Workflows** → **Import from file**
2. Import each JSON file
3. Set the `HEATR_BASE_URL` environment variable in n8n to your API URL
4. Set up an **HTTP Header Auth** credential named `heatr-api` with:
   - Header name: `Authorization`
   - Header value: `Bearer <your_supabase_service_role_key>`
5. Activate all workflows

### Workflow schedule overview

| # | Workflow | Schedule | Endpoint |
|---|---|---|---|
| 01 | Sequence Due Sends | Every 15 min | `GET /sequences/due-sends` → `POST /sequences/process-send/{id}` |
| 02 | Snooze Wake-up | Every 15 min | `POST /crm/wake-snoozed`, `POST /tasks/reactivate-snoozed` |
| 03 | Enrichment Worker | Every 1 min | `POST /enrichment/process-next` |
| 04 | Website Analysis Worker | Every 1 min | `POST /website-intelligence/process-next` |
| 05 | Daily Metrics | 23:55 daily | `POST /analytics/collect-metrics` |
| 06 | Daily Reset | 00:00 daily | `GET /health` |
| 07 | Blacklist Monitor | 06:00 daily | `GET /warmr/inboxes` |
| 08 | Alert Check | 08:00 daily | `GET /alerts` |
| 09 | Recontact Suggestions | Monday 09:00 | `GET /leads/recontact-ready` |
| 10 | Daily Briefing | 07:00 daily | `POST /briefing/generate` |

---

## 5. Frontend

The frontend is static HTML/CSS/JS in `frontend/`. Serve it from any static host:

```bash
# Local dev (Python simple server)
cd frontend && python -m http.server 3000

# Or deploy to Netlify / Vercel / Cloudflare Pages
# Point the root to /frontend
```

Update `window.HEATR_CONFIG` in each HTML file (or centralise in `app.js`) with:
- `SUPABASE_URL` — your Supabase project URL
- `SUPABASE_ANON_KEY` — your Supabase anon key
- `API_BASE` — your Heatr API URL (e.g. `https://heatr.up.railway.app`)

---

## 6. Warmr webhook

In your Warmr dashboard, set the webhook URL to:

```
https://<your-heatr-url>/webhooks/warmr
```

Set the webhook secret to match `WARMR_WEBHOOK_SECRET` in your `.env`.

Events handled: `replied`, `interested`, `bounced`, `unsubscribed`, `spam`, `opened`.

---

## 7. Startup validation

On every boot, Heatr runs `utils/startup_validator.py`. Check logs for:

- `FAIL:` lines — hard errors that prevent correct operation (fix before going live)
- `WARN:` lines — optional features not configured (OK to ignore for MVP)

You can also call `GET /health/startup` to re-run checks on demand.

---

## 8. Cost monitoring

Claude API costs are tracked per call in `api_cost_log`. View them via:

```
GET /analytics/costs?days=30
```

Target: €10–15/month. If costs exceed this, check `hit_count` in `claude_cache` — low
cache hit rates mean similar content is being re-analysed.
