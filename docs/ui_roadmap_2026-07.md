# UI-roadmap — alles wat geen scherm heeft, geordend (2026-07-29)

## Context
De backend is ~120 endpoints (één monoliet `api/main.py`); de frontend (14 pagina's)
consumeert er ~47. Sami: "alles moet UI krijgen, in logische volgorde." Deze roadmap ordent
elk UI-gat (uit een 3-cluster code-assessment) langs de klant-funnel + risico/afhankelijkheid.
De receptie-preview (LeadDetail-tab + `/leads/{id}/receptie` + cohort-badge/filter) is al af.

**Ordening-logica:** ① fix wat kapot/dood is → ② veilig kunnen versturen (kritiek pad naar de
25) → ③ de Aerys-pitch zichtbaar (commerciële kern) → ④ operator-zicht (diagnostiek/alerts) →
⑤ discovery-beheer + leerlus. Effort: S=pagina op bestaand endpoint · M=pagina + wat glue ·
L=nieuwe endpoints/flows.

---

## Fase 0 — Kapot + dood opruimen (risicoloos, direct)
| Item | Effort | Wat |
|---|---|---|
| Queue-health render-fix in Control | S | Control leest `queueHealth.queues` — die key bestaat NIET in de response (`status_counts/fail_rate_pct/top_failing_steps/…`), dus het blok toont nu niets. Pure render-fix op de echte shape. `Control.tsx` / `api/main.py:4136`. |
| Verwijder `frontend-next/src/pages/Placeholder.tsx` | S | Dode code, nergens geïmporteerd. |
| Verwijder `campaigns/observation_opener.py` (+ test) | S | Fase-A opener, alleen door eigen test geïmporteerd; live-pad = `personalized_opener`. |
| Verwijder `campaigns/teardown_generator.py` (+ test) | S | Dode Fase-A generator. **LET OP:** alleen de módule; `heatr_teardown_pages` (migr. 029) blijft — `gdpr_manager` raakt de tabel voor erasure. |

## Fase 1 — Veilig kunnen versturen (kritiek pad naar de 25)
| Item | Effort | Wat / afhankelijkheid |
|---|---|---|
| **Compliance-vlaggen paneel** (in Control) | M · +2 endpoints | `assert_no_open_flags` is fail-closed → één open vlag legt de héle drip stil, nu alleen via SQL te deblokkeren. Nieuw: `GET` open flags + `POST` acknowledge (bovenop `utils/compliance_flags.py`). Maakt de afmeld-sweep-output zichtbaar. **Scherpste gat.** |
| **Due-sends "outbox"** (read-only) | S | `GET /sequences/due-sends` — zien wát de engine na arming zou versturen, vóór de eerste batch. Nu blind. |
| **Send-model-UI corrigeren** (CampagneLaunch) | M | Launch-knop is hard-disabled én noemt de verkeerde kill-switch (`ENABLE_CAMPAIGN_SENDS` i.p.v. `ENABLE_PROSPECT_SENDS` op activate). De browser-send-funnel loopt dood op "Preview". Pagina moet launch=draft / activate=muur correct tonen. |
| **GDPR-console** | M | `GDPR_MODE=strict`; forget/export/register + auditlog zijn nu alleen via curl. Compliance-pagina (register + log) + `Vergeten`/`Exporteer` in LeadDetail. `forget` is destructief → confirm-modal. |

## Fase 2 — De Aerys-pitch zichtbaar maken (Trojan Horse = commerciële kern)
| Item | Effort | Wat / afhankelijkheid |
|---|---|---|
| **WebsiteKansen herbedraden** op `/website-opportunities` | M | Nu draait de pagina op `/leads` + `topIssueFromScore()` (heuristiek) en "vs markt: niet berekend". Het echte endpoint levert screenshot, `opportunity_types` (dienst-tags), `score_vs_market`, `opportunity_priority`. |
| **LeadDetail Website-tab diepte** | M | Tab tekent nu enkel een score-balk. Bouw de laag-scores (technisch/visueel/conversie/sector), grote screenshot, Vision-tekst, top-3, concurrent-staafdiagram — via het bestaande `GET /leads/{id}/website`. |
| **Website-review actieknoppen** (OK/Opportunity/Urgent) | S | `PATCH /leads/{id}/website-review` — triage van de kansenlijst. Plaatsing rijdt mee op de twee items hierboven. |
| **Audit-scorer tab** (Stap 2) | L | `POST /leads/{id}/audit` bestaat (tier 1/2, append-only), maar er is **geen GET-leespad** → eerst een leesroute (anders re-runt de UI = versie-bloat). Tier-2 vereist `GOOGLE_PLACES_API_KEY` + leadreactie. |

## Fase 3 — Operator-zicht: diagnostiek + alerts (veel S/M, pure wiring)
| Item | Effort | Wat |
|---|---|---|
| **Alerts-bell** in de Shell-header | S | `GET /alerts` + `PATCH /alerts/{id}/read`. Feed wordt actief gevuld (budget-cap, stall, send-guard) maar is onzichtbaar. |
| **Ops-health verdict/stall-banner** in Control | S | `GET /analytics/ops-health` (degraded/stalled + stall-alerts). Nu alleen een sidebar-pilletje. |
| Analytics: **funnel-cohort** (week × archetype) | M | `GET /analytics/funnel` — welk segment converteert; nu alleen een platte 5-rijen snapshot. |
| Analytics: **cost-attribution** (kosten/lead/reply) | M | `GET /analytics/cost-attribution` — ROI per segment. |
| Analytics: **email-status-breakdown** | M | `GET /analytics/email-status-breakdown` — verifier-diagnose + hints. |
| Analytics: **enrichment-coverage** (silent-break) | S | `GET /analytics/enrichment-coverage` — waar stappen stil falen. |
| Analytics: **Claude-kosten per model** + cache | S | `GET /analytics/costs` — sturen op het €10-15/mnd-doel. |
| Analytics: **daily-metrics trendlijn** | M | `GET /analytics/metrics` (vereist collect-metrics-cron gevuld). |
| **Scraping-live feed** op Zoeken | M | `GET /analytics/scraping-live` — companies_raw + per sector/stad-opbrengst tijdens een lopende scrape. |
| **Leads-CSV export knop** | S | `GET /analytics/export/leads.csv` — StreamingResponse; vereist een directe fetch met auth-header (de JSON-parsende `api.ts` kan geen blob). |

## Fase 4 — Discovery-beheer + leerlus + rest
| Item | Effort | Wat / caveat |
|---|---|---|
| **Discovery-schedules beheer** (nieuwe nav) | M | `/discovery-schedules/*` CRUD — herhaal-scrapes (sector+stad, freq, next/last-run). De motor onder de funnel-top, nu alleen via curl/n8n. |
| **Recontact-signalen paneel** | M | `/leads/recontact-ready-signals` + `/leads/{id}/recontact-signals` — trigger-based heropening (verse site-verandering), nu sterven ze in n8n. |
| **Feedback-processor inzichten** | M · productbesluit | `GET /scoring/feedback-history` tonen. **Caveat:** de auto-apply-lus is dood (`scoring_weights.py` = dode code), `process-feedback` is service-only → alleen display tot je 'm data-driven maakt. |
| **ICP-beheer** | S · **skip tenzij** | `GET/POST /icp` bestaat, maar de scorer leest `config/sectors.py`, niet `icp_definitions` → een CRUD-pagina verandert nu niets. Alleen bouwen als ICP data-driven moet worden (dan L: `icp_matcher` herbedraden). |

---

## Expliciet GÉÉN UI (bewust headless — niet "vergeten")
- **Campagne-arming** (`POST /campaigns/{id}/activate`) — het verzendmoment blijft service-key/CLI **buiten** de browser; een knop zou de laatste send-muur slopen. *Optioneel additief:* een **read-only** arm-status paneel (kill-switch-stand, welke drafts arm-klaar) — dat is geen send-knop.
- **Webhooks** `/webhooks/{warmr,zoom}` — machine-to-machine; effect al zichtbaar in Inbox/Gesprekken.
- **Health-probes** `/healthz`, `/health`, `/health/startup` — WorkerStatus dekt de operator-heartbeat.
- **Briefing** `/briefing/generate` — e-mail/scheduler-side-effect; inhoud staat al op het Dashboard.
- **Pure libraries** (email_verifier/enrollment/lead_scoring) — output al zichtbaar in LeadDetail/Leads/Control-ledger.

## Twee productbesluiten die items gaten (niet-technisch)
1. **Review-email** (`/leads/{id}/send-review-email`) — legacy/kapot, mist een uitsluiting-gate tegen receptie-enrolled leads (kruisbesmetting). **Niet bouwen** tot die gate er is, of pensioneer 'm naast de receptie-sequence. (L, Fase 2/outreach — bewust geparkeerd.)
2. **Feedback-lus + ICP data-driven** — zie Fase 4; nu voedt de lus niks terug.
