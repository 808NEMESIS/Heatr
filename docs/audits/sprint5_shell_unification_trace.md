# Sprint 5 (pre) — Heatr + Warmr onder één operator-Shell: haalbaarheids-trace

**2026-07-06 · beslisdocument, geen code.** Doel: één frontend, één login, een
toggle rechtsboven die switcht tussen Heatr-view en Warmr-view. Backends blijven
volledig gescheiden — puur UI-samenvoeging.

**Kernvondst vooraf:** de twee frontends draaien op een **fundamenteel andere
generatie**. Dat verschuift de rekensom precies zoals de sprint anticipeerde:
Optie A is geen goedkope toggle maar een volledige port. Er is een goedkopere,
niet-geïnventariseerde route (A0) die het stack-verschil juist benut.

---

## Inventaris

| Dimensie | Heatr (`frontend-next`) | Warmr (`frontend`) | Botsing? |
|---|---|---|---|
| **Stack** | React 19 + Vite 8 + TypeScript, `react-router-dom` 7, TanStack Query 5, Tailwind 4, design-tokens + `Shell.tsx` | **Vanilla multi-page HTML/CSS/JS.** Geen `package.json`, geen build, geen React. 15 losse `.html`-pagina's + één `app.js` (~1200 r) + `style.css`. Supabase via CDN-UMD. CSS-custom-properties als tokens. | **JA — grootste kostenpost.** Niets is herbruikbaar tussen de twee; een "port" is een herschrijving. |
| **Auth** | `Authorization: Bearer <token>` — Supabase-JWT (HS256-decode tegen `SUPABASE_JWT_SECRET`) met legacy `dev-token`-fallback | `_supabase.auth.getSession()` → `Authorization: Bearer <session.access_token>` (echte Supabase-JWT). Backend `verify_token` decodet met `jose`, `client_id = payload["sub"]` | **NEE — sterker: al verenigd.** Zie hieronder. |
| **Supabase-project** | `zomdrygdcaenjnrrpcpw` | `zomdrygdcaenjnrrpcpw` | **Identiek.** Eén login → één JWT die **beide** backends accepteren. |
| **Tenant** | `workspace_id` (default `aerys`) | `client_id`, afgeleid uit JWT-`sub` (service-role + handmatige check) | **NEE voor solo-operator.** Beide leiden hun tenant zelfstandig af uit dezelfde token; de toggle hoeft niets met tenant-context te doen. |
| **CORS** | `allow_origins=["*"]` | `allow_origins=ALLOWED_ORIGINS` (env-lijst; fallback `localhost:3000/8080/127.0.0.1:5500`) | Klein: Heatr accepteert alles; Warmr's env moet de unified-origin bevatten. **Env-config, geen code.** |

### De doorslaggevende feiten

1. **Auth is al verenigd op identiteitsniveau.** Beide backends valideren
   dezelfde Supabase-JWT uit hetzelfde project. Eén Supabase-login levert één
   `access_token`; beide API-clients sturen exact dezelfde
   `Authorization: Bearer <JWT>`. Geen header-botsing, geen dubbele login, geen
   auth-unificatiewerk. Dit is de grootste meevaller.

2. **De stacks delen niets.** React/TS/Vite vs. vanilla MPA. Elke Warmr-pagina
   die "onder Heatr's Shell" moet, moet van hand-geschreven HTML/JS naar
   React/TSX + React Query + Tailwind herschreven worden. ~12.000 regels over
   15 pagina's, meerdere zeer dik (`campaigns.html` 97 KB, `domains.html`
   47 KB, `inboxes.html` 45 KB, `settings.html` 42 KB). Dit is de kost.

3. **Tenant is een non-issue** voor de solo-single-tenant-situatie. De toggle is
   puur "welke pagina-set toon ik", niet "wissel van tenant".

---

## Opties — moeite, auth, CORS, backend-impact

### Optie A0 — Shell-toggle zonder port (niet in de briefing, maar de goedkoopste)
Het stack-verschil maakt porten duur — maar maakt embedden juist aantrekkelijk.
Heatr's topbar krijgt een toggle; de Warmr-view is Warmr's **bestaande** static
app, geserveerd **onder dezelfde origin** als de Heatr-frontend (reverse-proxy /
zelfde domein, ander pad), getoond via iframe of link-out.

- **Moeite:** 0,5–2 dagen. Toggle + routing + same-origin serving-config.
- **Auth:** géén werk *mits same-origin*. Supabase-JS bewaart de sessie in
  `localStorage` per **origin** (`sb-<ref>-auth-token`). Zelfde origin → beide
  views delen automatisch dezelfde sessie → één login. (Cross-origin iframe deelt
  de sessie **niet** → dan alsnog een token-handoff of tweede login nodig; vandaar
  de same-origin-eis.)
- **CORS:** unified-origin toevoegen aan Warmr's `ALLOWED_ORIGINS` (env).
- **Backend:** geen. (iframe-caveat: als Warmr's static-server ooit
  `X-Frame-Options: DENY` zet, breekt embedden — link-out-toggle omzeilt dat.)
- **Breekt er iets:** nee — Warmr's UI blijft draaien zoals hij is; nul
  regressierisico op werkende pagina's.
- **Nadeel:** geen gedeelde look-and-feel; twee visuele werelden onder één dak.
  Voor "stoppen met wisselen tussen twee apps" is dat prima; voor "één product"
  niet.

### Optie A — Nav-toggle, Warmr-pagina's geport naar Heatr's `frontend-next`
Warmr's 15 pagina's herschrijven naar React/TSX onder Heatr's Shell; twee
API-clients, elk met dezelfde Supabase-JWT.

- **Moeite:** **4–8 weken** focus-werk. 1–2 dagen per eenvoudige pagina, 3–5 per
  dikke (campaigns/domains/inboxes/settings), plus `app.js`-logica (notificaties,
  thema, JWT-decode, API-client) opnieuw in React. Dit is de port-vraag die de
  briefing noemde.
- **Auth:** triviaal (zelfde JWT, zelfde header).
- **CORS:** unified-origin in Warmr's `ALLOWED_ORIGINS` (env).
- **Backend:** geen vereist. (Reden dat het nee is: identiteit is al gedeeld;
  alleen de UI verhuist.)
- **Breekt er:** hoog regressierisico — een werkende, dichte operator-UI
  herschrijven herintroduceert bugs; testlast op 15 pagina's.
- **Wanneer wél:** als je de Warmr-view langdurig binnen Heatr's designtaal wilt
  doorontwikkelen.

### Optie B — Gedeelde Shell + gedeelde primitives
Als A, plus Heatr en Warmr delen `Table`/`Toast`/`StatusPill` etc.

- **Moeite:** **6–10 weken.** A + design-reconciliatie tussen Heatr's
  token-systeem en Warmr's CSS-conventies + een gedeelde componentlaag.
- **Auth/CORS/backend:** identiek aan A (geen backend-wijziging).
- **Wanneer wél:** alleen als je verwácht beide views veel parallel door te
  ontwikkelen; anders is de gedeelde-primitives-investering niet terug te
  verdienen.

### Optie C — Nieuwe operator-shell-repo die beide backends aanspreekt
Aparte shell-app, Heatr en Warmr als views eronder, schoonste scheiding.

- **Moeite:** **8–12+ weken.** Alles van A/B + nieuwe repo, build, deploy, auth-
  bootstrap, twee API-lagen.
- **Auth/CORS/backend:** geen backend-wijziging; wel een derde deploy-artefact.
- **Verdict:** **overkill voor solo-operator.** Markeer als "voor later, als dit
  een product wordt en er een team op komt."

---

## Aanbeveling

**Doe Optie A0 (same-origin shell-toggle, geen port).** Voor de
solo-operator-situatie — "stoppen met wisselen tussen twee apps" — levert A0
99% van de waarde voor <5% van de kosten:

- De duurste post (stack-mismatch) wordt **omzeild** i.p.v. betaald: Warmr's
  werkende UI blijft staan, nul port, nul regressierisico.
- De grootste meevaller (gedeelde Supabase-identiteit) wordt volledig benut: één
  login werkt voor beide zodra ze **same-origin** geserveerd worden.
- Het enige echte werk is deployment-plumbing (beide onder één origin) + een
  toggle + één env-regel in Warmr's `ALLOWED_ORIGINS`. Geen backend-wijziging,
  geen auth-unificatie, geen tenant-werk.

**Optie A** (volledige port) is de sprint-verwachting, maar die verwachting gold
"tenzij de stacks fundamenteel verschillen" — en dat doen ze. Bewaar A voor het
moment dat je de Warmr-view écht binnen Heatr's designtaal wilt doorontwikkelen;
tot dan is porten 4–8 weken kopen voor cosmetische eenheid. **B en C** zijn pas
zinvol bij een product-met-team, niet bij een solo-operator.

**Concreet als A0 gekozen wordt (aparte bouwsprint):** (1) serveer beide
frontends onder één origin (reverse-proxy of zelfde domein/ander pad); (2)
Heatr-topbar krijgt een Heatr⇄Warmr-toggle (link of iframe); (3) voeg de
unified-origin toe aan Warmr's `ALLOWED_ORIGINS`. Buiten dat blijft alles —
Control Plane, dispatcher, invarianten, Warmr-contract — exact zoals het is.
