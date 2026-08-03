# Security-audit (A5) — systeemtechnisch — 2026-08-03

Scope: auth-paden · secrets · webhooks/CORS · rate-limiting · service-key-oppervlak ·
RLS-rest · frontend-token-flow · injectie. Niet: juridisch (dat is Spoor J).
Methode: read-only inspectie + één live-fix (.env-permissie). 126 routes onderzocht.

**Eindoordeel: systeemtechnisch gezond.** De defensieve basis is opvallend sterk —
timing-safe auth, fail-closed HMAC-webhooks, CORS-wildcard-guard, RLS aan, geen
secret-lekken. Eén live-fix gedaan; de rest is bewust uitgesteld tot de hosting-stap
(op localhost laag risico). De hoogst-risico-check (service-role-key in de frontend)
is expliciet uitgevoerd en **schoon**.

---

## ✅ Geverifieerd schoon (geen actie)

| Vlak | Bevinding |
|---|---|
| **API-key-auth** | `secrets.compare_digest` — timing-safe. `require_service_key` op 7 gevoelige endpoints (o.a. campaigns/activate) weigert browser-JWT's. |
| **JWT-auth** | HS256 tegen `SUPABASE_JWT_SECRET`, `audience=authenticated`, workspace uit `app_metadata.workspace_id`, fail-closed zonder claim. |
| **Legacy-paden UIT** | `LEGACY_DEV_TOKEN_ALLOWED=false` én `HEATR_JWT_WORKSPACE_FALLBACK=false` — beide dicht. |
| **Webhooks** | `/webhooks/warmr` (HMAC-sha256, `WARMR_WEBHOOK_SECRET`) én `/webhooks/zoom` (x-zm-signature) — **beide fail-closed**; ontbrekend secret → weigeren/404, ongeldige sig → 401. |
| **CORS** | Bij wildcard worden credentials automatisch UITgezet (recovery-guard) — geen `*`+credentials-lek. |
| **Secrets** | Niet in logs (cron_logged.sh-fix hielp), niet in git-history, `.env` in `.gitignore`. Geen hardcoded key/secret-fallbacks in de code. |
| **🎯 Service-role-key** | **NIET in de frontend-bundle.** De enige JWT in `dist/` is de anon-key (`role=anon`, geverifieerd gedecodeerd). Dit is dé kritieke check en 'ie is clean. |
| **Injectie** | Alle queries via PostgREST-parameters; de enige `.rpc()` (cost_guard → `heatr_cost_sum`) gebruikt named params, geen string-bouw. |
| **RLS** | AAN op alle `heatr_`-tabellen, anon/authenticated ge-REVOKE'd (P1-audit 021b). Backend op service_role, browser praat alleen via de API. |

## 🔧 Nu gefixt
- **`.env` + `.env.local`: 644 → 600.** Waren wereld-leesbaar; nu alleen owner. Laag risico op een single-user laptop, echt probleem op een gedeelde/hosted machine — daarom meteen dicht.

## 🟠 Must-before-hosting (nu localhost = laag, verplicht vóór publieke URL)

1. **Geen inbound-API-rate-limiting.** `utils/rate_limiter` gate't alleen outbound-services
   (google_search etc.), niet de FastAPI-endpoints. Op een publieke host betekent dat:
   ongelimiteerde brute-force op de X-API-Key/JWT-paden en ongelimiteerde webhook-POST's.
   **Fix bij hosting**: reverse-proxy rate-limit (nginx/Caddy) of `slowapi` op auth- +
   webhook-routes. *Bouwbaar wanneer de hosting-vorm bekend is.*
2. **8 no-auth GET-endpoints** (`/healthz`, `/sectors`, `/sectors/full`, `/config/sendability`,
   `/sequences/templates[/{id}]`) — functioneel prima (statische config), maar `/config/sendability`
   en `/sequences/templates` lekken interne bedrijfslogica/copy. **Bewuste keuze vereist vóór
   hosting**: publiek laten of achter auth. Nu geen risico (localhost).
3. **`HEATR_ALLOWED_ORIGINS` zetten** (bekend uit UI-audit): nu wildcard → credentials uit.
   Vóór hosting expliciete frontend-origin, dan mogen credentials weer.

## 🟢 Nice-to-have (laag)
- **5 POST/PATCH-endpoints met rauwe `body: dict`** i.p.v. Pydantic-model → zwakkere
  shape-validatie (geen injectie, wél kans op onverwachte payloads):
  `/control/campaign-records/{id}/force-next` · `.../restart` · `/leads/{id}/test-mode` ·
  `/icp` · `/analytics/enrichment-raise-monthly-cap`. Alle vijf achter auth. Bij gelegenheid
  omzetten naar Pydantic-models.

---

## Verdict per hosting-gate
- **Localhost (nu):** geen blokkerende bevindingen. De .env-fix is gedaan.
- **Vóór 24/7-publieke hosting:** rate-limiting (#1) + no-auth-GET-beslissing (#2) +
  `HEATR_ALLOWED_ORIGINS` (#3). Geen van drieën is groot werk; alle drie bouwbaar zodra
  de hosting-vorm (reverse-proxy?) bekend is.

Herhaalbaarheid: bij een hosting-migratie deze audit opnieuw draaien — m.n. de
service-role-in-bundle-check en de rate-limit-status.
