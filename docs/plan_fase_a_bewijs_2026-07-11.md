# Fase A — "Bewijs" · Werkplan (4-6 weken)

**Datum:** 2026-07-11 · **Bron:** strategie_heatr_vs_markt_2026-07-11.md, fase A
**Doel:** 10 gevoerde review-gesprekken, ≥3 betaalde opdrachten, elke uitkomst vastgelegd.
**Kernproduct:** de **teardown-pagina** — de website-analyse als deelbaar, persoonlijk artefact; de mail wordt bezorger van waarde i.p.v. vraag om tijd.

---

## 0. Wat er al ligt (en de gaten die dit plan moet dichten)

| Asset | Staat | Gat |
|---|---|---|
| `website_intelligence`-data (scores, lagen, top-issues, vision-tekst) | ✅ live per lead | — |
| Screenshots | ⚠️ code werkt, maar **Storage-bucket `screenshots` bestaat niet in prod** (startup-FAIL bij deploy 11-07) | bucket aanmaken |
| Concurrentiebenchmark | ⚠️ code bestaat, key-crash gefixt, **nooit gewired in de analyzer** (audit-item 7) | wiren of v1 zonder (beslispunt B2) |
| Sequences v3.1 (3 bruggen) | ✅ live | herschrijven rond de link (v4) |
| Verzendlaag (dispatcher/suppressie/enrollments/guards) | ✅ sinds deze week betrouwbaar | — |
| Webhook-eventledger + crm_tasks | ✅ | intent-events (pageviews) aansluiten |
| Inboxen | ⚠️ **2 ready** (net hersteld), campaign-target 5/dag/inbox → ~10 campagne-mails/dag | capaciteit = kritiek pad |
| Publieke hosting | ❌ Heatr-API draait lokaal op :8001 | beslispunt B1 |
| GitHub-backup | ❌ PAT verlopen, 61+ commits alleen lokaal | Sami: token vernieuwen |

---

## 1. Beslispunten (VÓÓR de bouw — week 1)

**B1 · Hosting van de teardown-pagina's.**
De pagina moet publiek bereikbaar zijn; de Heatr-API is dat niet.
*Aanbeveling:* **statisch pre-renderen → Supabase Storage (publieke bucket `teardowns`) achter token-URL's**, met kliktracking via een klein publiek edge-endpoint (Supabase Edge Function die de view logt en doorstuurt/embed). Redenen: geen nieuwe server, geen tunnel naar de Mac (security), Supabase is er al, en statisch = niets te hacken. Alternatief (VPS/reverse-proxy naar :8001) alleen als dynamiek per bezoek nodig blijkt — niet in fase A.

**B2 · Concurrentie-sectie in v1 van de pagina?**
De benchmark is de scherpste pitch ("2 punten onder {concurrent} in {stad}") maar is nooit gewired en kost per lead extra scrapes.
*Aanbeveling:* **v1 mét**, maar alleen voor de eerste 50 hand-geselecteerde leads (batch-run van de bestaande `competitor_analyzer` op die 50) — niet generiek wiren. Generiek wiren = fase B.

**B3 · Verzenddomein-strategie.**
Nooit koude bulk vanaf het hoofddomein.
*Aanbeveling:* 2-3 look-alike domeinen kopen (bv. `aerys-review.nl`, `meetaerys.nl`), elk 2 Google-Workspace-inboxen. Teardown-URL's op een **apart, net domein** (bv. `review.meet-aerys.nl` subdomein → Storage/CDN) — het linkdomein in de mail moet vertrouwen wekken, niet naar een tracking-domein ruiken.

**B4 · Publicatie-kwaliteit.**
De vision-tekst van Claude wordt extern zichtbaar, mét bedrijfsnaam — een verkeerde of botte zin is reputatieschade.
*Aanbeveling:* **review-queue verplicht voor de eerste 50**: pagina wordt gegenereerd als `draft`, operator keurt (of redigeert 1 regel) → `published`. Pas daarna beoordelen of auto-publish verantwoord is. Fail-closed: de mail-template weigert te verzenden zonder `published` teardown (zelfde patroon als de personalisatie-gate).

---

## 2. Werkpakketten (Heatr-repo, PR-volgorde)

### PR A1 — Datamodel + generator
- **Migratie 029:** `heatr_teardown_pages` (id, lead_id, workspace_id, token UNIQUE niet-raadbaar, status draft|published|revoked, html_path, generated_at, published_at, approved_by, content_hash) + `heatr_page_views` (token, lead_id, viewed_at, ua_hash, referer; index op (lead_id, viewed_at)).
- `campaigns/teardown_generator.py`: rendert per lead een statische HTML uit `website_intelligence` (+ screenshot-URL + optioneel concurrentie-sectie). Huisstijl: licht, Fraunces + Plus Jakarta Sans (CLAUDE.md), mobiel-eerst — de ontvanger bekijkt dit op een telefoon. Secties: score-hero, 3 verbeterpunten (concreet, respectvol), screenshot-annotaties, [concurrentie], "wat dit voor jouw praktijk betekent", CTA = 15-min gesprek (Calendly/telefoonlink), voettekst met wie/waarom + opt-out.
- `noindex`-meta + `X-Robots-Tag`; token = 128-bit random.
- Tests: render met volledige/deels lege data (geen vision → sectie weg, geen crash), token-uniciteit, draft-default.

### PR A2 — Publicatie + kliktracking + intent-actie
- Publish-flow: HTML → Storage-bucket `teardowns/{token}.html`; status → published (alleen vanuit review-queue, B4).
- **View-tracking:** Supabase Edge Function `GET /t/{token}` → logt `page_views`-rij → redirect/serve. Bot-lightfilter (HEAD/uptime-agents negeren).
- **Intent → actie (het hart):** nieuwe n8n-poll of API-cron: eerste view per lead per dag → `crm_task` "🔥 Teardown bekeken — bel binnen 24u" (priority high, met view-count + tijdstip) + `lead_timeline`-event + dagelijkse briefing-sectie "vandaag bekeken".
- Dedupe: max 1 taak per lead per dag; view ná gesprek-gepland → geen taak.
- Tests: view→task-dedupe, revoked token → 410.

### PR A3 — Sequence v4 "teardown" + gate
- `config/sequence_templates.py`: v4-varianten per brug. Mail 1: één observatie + de link ("ik heb jullie site naast twee praktijken in {stad} gelegd — dit viel me op: {…}. De hele analyse staat hier: {teardown_url}"). Mail 2 (dag 4): tweede inzicht uit de anályse + zachte vraag. Mail 3 (dag 7): kort, deur open. Geen prijzen (bestaande regel), max 90 woorden, geen tracking-pixels — de link zelf is de meting.
- `{{teardown_url}}` in de renderer (`inject_variables`); **fail-closed**: lead zonder published teardown → geblokkeerd in `_gate_leads_for_template` (zelfde mechaniek als personalisatie-gate).
- Tests: gate blokkeert zonder pagina; URL-injectie; template-selectie per brug.

### PR A4 — Launch-integratie + bulk-publish
- Launch-flow stap: vóór de push per bucket controleren dat alle leads een published teardown hebben; response toont `missing_teardowns`.
- Bulk-generate endpoint `POST /teardowns/generate` (lead_ids) → drafts klaar voor review; idempotent op content_hash (her-generatie alleen bij gewijzigde analyse).
- Dispatcher-metadata: teardown-token mee in `outbound_log` (I7: wat er verstuurd is, incl. welke pagina).

### PR A5 — Uitkomst-registratie (de flywheel-basis)
- `crm_deals` uitbreiden: verplichte uitkomst per gevoerd gesprek — `outcome` (won|lost|no_show|later) + `lost_reason` + `source_trigger` (welke brug/teardown). Migratie 030 (kolommen + CHECK).
- Wekelijkse funnel-query in de briefing: verstuurd → views → replies → gesprekken → deals, per brug.
- Dit is bewust minimaal: fase C bouwt hier de lerende lus op; fase A legt alleen vast.

---

## 3. Infra-acties (Sami, parallel — week 1 is het kritieke pad!)

| # | Actie | Waarom nu |
|---|---|---|
| S1 | **Domeinen kopen + Google Workspace** (2-3 domeinen × 2 inboxen) + SPF/DKIM/DMARC | **Warmup duurt 28 dagen** (promotiecriterium) — elke dag uitstel schuift de capaciteit op. Week 1 starten = ready rond week 5 |
| S2 | Nieuwe inboxen in Warmr registreren (warmup start automatisch; de gefixte promotie doet de rest) | idem |
| S3 | Storage-buckets aanmaken: `screenshots` (private — fixt ook de startup-FAIL) + `teardowns` (public) | PR A1/A2 hangen erop |
| S4 | Subdomein `review.meet-aerys.nl` → Storage/edge (B1/B3) | linkdomein in de mail |
| S5 | GitHub PAT vernieuwen + `git push` (Heatr én Warmr) | 61+ commits zonder backup is onacceptabel risico |
| S6 | Calendly (of gelijkwaardig) voor de CTA | pagina zonder afspraaklink lekt intent |

---

## 4. Tijdlijn

| Week | Bouw | Operatie |
|---|---|---|
| 1 | Beslispunten B1-B4 · PR A1 + A2 | S1-S6 (domeinen = dag 1!) |
| 2 | PR A3 + A4 · eerste 50 teardowns gegenereerd → review-queue | 50 leads hand-picken (beste ICP-match, mét concurrentiedata via B2-batch); smoke-test op eigen adres |
| 3 | PR A5 · iteratie op pagina/copy o.b.v. eerste views | Eerste echte batch (~30-40 leads over 2 inboxen, ±10/dag) · beltaken draaien |
| 4-5 | polish + meting | Tweede batch · nieuwe inboxen komen ready (S1) → volume ×3 |
| 6 | — | Fase-A-review: doelen gehaald? → go/no-go fase B |

## 5. Succescriteria & meting

- **Hard doel:** ≥10 gevoerde gesprekken, ≥3 betaalde opdrachten, 100% uitkomsten geregistreerd.
- **Leidende indicatoren:** teardown-viewrate (doel ≥25% van bezorgde mail-1), view→reply of view→gesprek ≥15%, taak-opvolging <24u in ≥90% van de views.
- **Vergelijking:** v3.1-cijfers (huidige sequences) als baseline naast v4 — zelfde leads-kwaliteit, andere mail.
- Alles uit bestaande data: `page_views`, `webhook_events`, `outbound_log`, `crm_deals`.

## 6. Risico's

| Risico | Mitigatie |
|---|---|
| AI-tekst publiek zichtbaar → toon/feitfout | B4 review-queue eerste 50; respectvolle toon in de prompt ("direct maar constructief"); bedrijf kan opt-out (revoked → 410) |
| Link in koude mail → spamfilter | net subdomein, geen pixel/derde-partij-trackers, één link, tekst-mail; A/B: mail 1 mét vs zónder link |
| Capaciteit blijft 2 inboxen | S1 op dag 1 (28d doorlooptijd); batches passen binnen huidige caps |
| Pagina's scrapen elkaar / concurrent ziet analyse | token niet-raadbaar, noindex, revoke-mogelijkheid; er staat niets op dat niet publiek waarneembaar is |
| Operator-tijd (reviews + bellen) | review = 1-2 min/pagina; beltaken gecapped door batch-grootte |

## 7. Buiten scope van fase A (bewust)

Markt-brede change-detection (fase B) · generieke competitor-wiring (fase B) · nurture/recontact-lifecycle (fase B) · multi-tenant/product-beslissing (fase C) · WhatsApp/multi-channel (fase D). Eén ding tegelijk: eerst bewijzen dat waarde-eerst converteert.

---

*Definition of done fase A: drie betalende klanten waarvan we exact weten welke brug, welke teardown en welk moment ze binnenbracht — en een machine die dat kan herhalen.*
