# Operator-shell deploy (A0 same-origin) + hardening

Hoe je Heatr + Warmr in prod onder één origin zet zodat de gedeelde
Supabase-sessie werkt (één login), plus de auth/CORS-hardening uit de
A0-review. Lokaal (dev) regelt de Vite-proxy dit al; dit gaat over prod.

## Topologie: één origin

```
https://app.aerys.nl
  /                → Warmr  (frontend + eigen API, op origin-root)
  /heatr/*         → Heatr  frontend (statische build, base=/heatr/)
  /api/*           → Heatr  backend (uvicorn :8001)
  /warmr-api/*     → Warmr  backend (optioneel, voor volledig same-origin)
```

Reverse-proxy: zie [deploy/Caddyfile](../deploy/Caddyfile) (nginx-equivalent is
1-op-1). Same-origin ⇒ `localStorage` (`sb-<ref>-auth-token`) is gedeeld ⇒ één
login geldt voor Heatr én Warmr, identiek aan dev.

## Build + config

1. **Heatr frontend:** `cd frontend-next && npm run build` (base=/heatr/ staat
   in `vite.config.ts`). Serveer `dist/` onder `/heatr/`.
2. **Heatr backend:** draait op :8001. Env via `.env` (wordt bij startup geladen
   — `load_dotenv`). Gesuperviseerd via
   [launchd/nl.aerys.heatr.api.plist](../launchd/nl.aerys.heatr.api.plist) (macOS)
   of een systemd-unit (Linux VPS).
3. **Warmr frontend:** `config.js` → `heatrUrl: '/heatr/'` (staat al zo). Voor
   volledig same-origin API: `apiBase: '/warmr-api'` en het `/warmr-api`-blok in
   de Caddyfile aanzetten. Anders blijft Warmr's API op zijn eigen origin en
   regelt CORS het.

## Auth (na de A0-review)

- **Eén login, beide kanten.** Heatr draait nu een echte `@supabase/supabase-js`
  client (`src/lib/supabase.ts`) met hetzelfde project + storageKey als Warmr.
  Same-origin ⇒ gedeelde, **zelf-verversende** sessie (geen stilstand meer na
  ~1u token-verloop). Inloggen kan op beide kanten (Heatr heeft nu een
  `LoginPage` + `AuthGate`).
- **dev-token is dicht.** `LEGACY_DEV_TOKEN_ALLOWED=false`. Service-callers
  (cron, n8n, worker) gebruiken `X-API-Key: <HEATR_API_KEY>` — de twee Heatr-
  cronjobs zijn gemigreerd. Browser-auth = Supabase-JWT. Zet `LEGACY=false` ook
  in prod.

## CORS-hardening

- **Heatr-backend:** `allow_origins` is nu env-gedreven. Zet in prod
  `HEATR_ALLOWED_ORIGINS=https://app.aerys.nl` (komma-gescheiden voor meerdere).
  Zonder de env blijft het `*` (dev-gemak).
- **Warmr-backend:** `ALLOWED_ORIGINS=https://app.aerys.nl` (nu dev:
  `…,http://localhost:5173`). Geen wildcard.
- Bij de volledig-same-origin-opzet (`/warmr-api` via de proxy) is CORS voor de
  frontend-calls niet eens meer nodig — alles is same-origin. Houd de origins
  toch strak voor service-to-service en directe API-toegang.

## Bekende beperking

De toggle is symmetrisch (Heatr→Warmr via de topbar-toggle, Warmr→Heatr via
Warmr's product-switcher), maar er is geen SSO-buiten-Supabase: één keer
inloggen op één van beide kanten volstaat, daarna deelt de origin de sessie.
