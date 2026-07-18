# Inventarisatie: mail-sequences — ontwerp vs. code vs. verzonden

Read-only vaststelling (2026-07-18). Elke bevinding met vindplaats; bij twijfel is
het ontwerp tot de code het tegendeel bewijst. Er is tijdens deze inventarisatie
niets gedraaid en geen send-functie aangeraakt; verzendbewijs komt uitsluitend uit
tabellen (SELECT-counts). De lopende website-backfill raakt geen van de hier
gebruikte bronnen (send-logs, enrollments, templates, env).

---

## 0. Waar leeft de outreach?

**Content + orkestratie in Heatr; transport in Warmr; n8n als klok.**

- **Templates + routing + rendering: Heatr.** `config/sequence_templates.py`
  (5 templates), `pick_brug()` (routing), `campaigns/sequence_engine.py`
  (`render_step`, deterministische seed per lead+stap).
- **Sequence-administratie: Heatr.** `heatr_lead_campaign_history` draagt per lead
  `sequence_steps`, `step_index`, `next_send_at`, `restart_epoch`.
- **Verzendbeslissing: Heatr.** `process_due_send` (`sequence_engine.py:385`):
  SendingGuard → render → `dispatch_outbound` (idempotency-key
  `seq-send:{record}:step:{n}:epoch:{e}`, gerenderde subject+body **bevroren** in
  `heatr_outbound_log` — I7/I8). Endpoint `api/main.py:5058`; per comment gedreven
  door een n8n-tick elke 15 min (of het n8n-schedule daadwerkelijk actief is, is
  niet uit deze repo vast te stellen).
- **Transport: Warmr** (apart systeem, `WARMR_API_URL`): `push_lead` →
  `POST /leads` met `custom_subject`/`custom_body` per stap
  (`integrations/warmr_client.py:179-215`); Warmr doet SMTP, inbox-beheer en stuurt
  replies terug via `POST /webhooks/warmr`.

De grens: **de mail-inhoud is volledig in deze codebase te vinden** (Heatr rendert
en bevriest per stap); alleen de feitelijke SMTP-verzending en inbox-warming zitten
in Warmr.

---

## 1. Ontwerp-element × staat × vindplaats × verzendbewijs

| Ontwerp-element | Staat | Vindplaats | Bewijs van verzending |
|---|---|---|---|
| **Drie-mail-sequence** | **Gebouwd, nooit gebruikt** | `config/sequence_templates.py:618` — 5 templates × 3 stappen. Productie = v3.1-brug-set (`v3_1_website/workflow/ai_audit`), delays **[0, 3, 5]**; v1 deprecated (gererouted server-side sinds 2026-05-07, `:260-264`). Mail 2+3 in reply-thread (`"Re: …"`), prijs-vrij (constraint `:…"Geen prijzen in body"`) | **Geen** — `outbound_log`: 0 rijen |
| **Routing op websitescore** (laag→website-angle, hoog→systemen) | **Gebouwd & aangeroepen — maar drempel op de oude schaal + dode tak** | `pick_brug()` (`sequence_templates.py:~735`): `visual_score`-tak is **dood** op de call-site (`api/main.py:1621` geeft de leads-rij door; `leads` heeft geen `visual_score`-kolom) → routeert effectief op `leads.website_score < 50` (+40 pt richting website-brug; ≥70 nodig). AUTO-mode in `_resolve_template` (`api/main.py:1600-1626`), ook in `/campaigns/preview` | **Geen** — nooit een echte launch uitgevoerd |
| **AI-openers via Claude Haiku** | **Gebouwd & actief** (enrichment-pad, niet send-pad) | `enrichment/company_enrichment.py:413 generate_personalized_opener` — per lead gegenereerd tijdens enrichment, **opgeslagen** op `leads.personalized_opener` ná QA-gate `validate_opener_sendable` (`utils/text_normalizer.py:82`; afgekeurd → niet opgeslagen, `company_enrichment.py:249-257`). Niet live-bij-verzending: send rendert de opgeslagen waarde | Generatie-bewijs: ja (openers-regeneratie 2026-07-15, 98% verzendklaar). Verzend-bewijs: **geen** |
| **Em-dash-verbod** | **Gebouwd** (in de prompt) | Opener-system-prompt, verboden-lijst: *"Em-dashes (—) — gebruik komma of punt"* (`company_enrichment.py`, system_prompt-blok) | n.v.t. |
| **Loom-first** | **Half gebouwd** | Templatetekst bestaat (v3.1-bodies: *"Ik kan een korte Loom maken … Geen template-Loom"*, `sequence_templates.py:428-429`; `{{LOOM_LINK}}` in bodies `:443,:491`). Substitutie bestaat: `{{LOOM_LINK}} → lead.get("loom_link") or ""` (`sequence_engine.py:311`) — maar **`loom_link` bestaat niet als kolom** (niet in schema/migraties/types). TODO zegt "handmatig invullen per send" (`:408`), zonder mechanisme (geen kolom, geen UI). Bij verzending rendert de plek **leeg** | **Geen** |
| **90-dagen reactivatie** | **Gesplitst: cooldown gebouwd & afgedwongen; reactivatie-campagne afwezig** | Cooldown: `CAMPAIGN_COOLDOWN_DAYS = 90` (`utils/deduplicator.py:24`), afgedwongen in `/leads/send-to-warmr` (`api/main.py:889-896`) én `/campaigns/launch`. Re-entry-plumbing bestaat (`next_contact_after`, `GET /leads/recontact-ready` `api/main.py:5098`, `wake_snoozed_leads`), maar er is **geen reactivatie-mailsequence** en geen trigger die na 90 dagen automatisch een campagne start | Cooldown nooit geoefend (0 enrollments) |
| **Gates vóór verzending** | Zie §2 | — | — |

### Overige vaststellingen bij het ontwerp

- **"Lage score → gratis concept-website"**: de website-brug-mail biedt een Loom met
  concrete aanpassingen aan, geen concept-website. Het routing-idee bestaat
  (`pick_brug` → `v3_1_website`); de exacte propositie wijkt af van het ontwerp.
- **"Hoge score → automatisering/systemen"**: de workflow-brug routeert niet op hoge
  websitescore maar op praktijk-drukte-signalen (reviews ≥30 / behandelingen ≥3 /
  locaties ≥2, elk +30, ≥70 nodig). Default bij twijfel: `ai_audit`.

---

## 2. De gates — staat in prod

| Gate | Code | Staat in prod | Werkelijk effect |
|---|---|---|---|
| Kill-switch | `_prospect_sends_enabled()` (`utils/outbound_dispatcher.py:86-97`) | **OPEN** — `.env:29 ENABLE_CAMPAIGN_SENDS=true`, `ENABLE_PROSPECT_SENDS` afwezig → fallback true | De dispatcher blokkeert **niet**. "Geen sends" komt niet door de switch |
| Compliance | `compliance_check` (`utils/enrichment_check.py`): `gdpr_safe` + status ∉ {unsubscribed, forgotten, disqualified, bounced} | actief (hard, geen test-bypass) | werkt zoals ontworpen |
| E-mail-gate (`/leads/send-to-warmr`) | `email_status in ("verified", "catch_all")` (`api/main.py:883`) | **DOOD OP WAARDEN** — de data bevat geen van beide: werkelijke waardes `valid` (518), `not_found` (190), `catchall_risky` (174), `not_checked` (44), `invalid` (33), `risky` (1) | dit pad laat **0 leads** door; `/campaigns/launch` heeft géén email_status-gate (bekend gat) |
| Score-drempel | `MIN_SCORE_FOR_WARMR` — `.env:67` = **55** (code-fallbacks 65/55 verschillen per plek) | actief | leads.score ≥ 55 |
| Cooldown | 90 dagen (`deduplicator.py:24`) | actief, nooit geoefend | zou her-enrollment blokkeren |
| Completeness | `HARD_REQUIRED_FIELDS = (archetype, score, sector)` (`enrichment_check.py:30`); `personalized_opener` slechts **SOFT_RECOMMENDED** (`:62`) | actief | een lead **zonder opener passeert** de launch |
| Auto-push | `AUTO_PUSH_TO_WARMR=false` | uit | geen automatische pushes |

## 3. Personalisatie-drift

- Bij verzending wordt **niets geparsed**: `inject_variables` rendert opgeslagen
  velden; elk ontbrekend veld wordt een **lege string** (`{{opener}} →
  lead.get("personalized_opener") or ""`, `sequence_engine.py:302` e.o.). Gevolg
  bij een lead zonder opener: **mail 1 gaat uit met een leeg observatie-blok** in
  het uniforme frame — geen fallback-tekst, geen overgeslagen mail.
- De JSON-parse-fouten uit de backfill-log zitten aan de **generatie-kant**
  (`personalization_extractor`, fail-soft) — ze verhinderen dat hooks/observations
  worden opgeslagen, maar breken de verzending niet; ze verarmen 'm stil.
- De opslag-QA (`validate_opener_sendable`) voorkomt dat een kápotte opener wordt
  opgeslagen, niet dat een lege ontbreekt. `is_quality_opener`
  (`sequence_engine.py:78`) heeft **geen productie-call-site** (alleen genoemd in
  een comment) — gedefinieerd, niet afgedwongen.

## 4. De trechter: van "klaar voor outreach" tot mail

```
lead (score≥55, compliant)
  │ 1. POST /campaigns/launch (service-key)      ← ██ NOOIT UITGEVOERD ██
  │    → maakt Warmr-campagne + enrollment-rijen (lead_campaign_history)
  │                                                 bewijs: 0 rijen, ooit
  │ 2. n8n-tick → POST /campaigns/process-due-sends (api/main.py:5058)
  │    → get_due_sends: leest lead_campaign_history  → 0 rijen → draait leeg
  │ 3. SendingGuard → render_step (deterministisch) → dispatch_outbound
  │    → kill-switch: OPEN  → idempotency + bevriezen in outbound_log (0 rijen)
  │ 4. warmr_client.push_lead → Warmr → SMTP
  ▼
verzonden: 0   (pushed_to_warmr_at: 0 leads · warmr_lead_id: 0 · outbound_log: 0)
```

Het ad-hoc pad (`/leads/send-to-warmr`) stokt eerder: de email_status-gate matcht
op waardes die niet bestaan → 0 eligible, nog vóór de cooldown.

Anomalie: `lead_timeline` bevat **3 × `reply_received`** terwijl er nooit iets is
verstuurd — vermoedelijk webhook-tests; niet read-only vast te stellen.

---

## Slotoordeel

**Wordt er vandaag werkelijk outreach verstuurd? Nee — en er is ook nooit iets
verstuurd.** Al het verzendbewijs staat op nul: `outbound_log` 0 rijen,
`lead_campaign_history` 0 rijen, `pushed_to_warmr_at` 0, `warmr_lead_id` 0. De
machinerie zelf is opvallend áf: templates, per-lead-routing, rendering met
bevroren ledger-content, idempotente dispatch, guards — en de kill-switch staat
zelfs **open**. Het ene ding dat ertussen zit is dat **er nooit een campagne is
gelanceerd**: geen enkele `POST /campaigns/launch` heeft ooit enrollment-rijen
gemaakt, dus de dispatcher heeft niets om te versturen. Wie vandaag zou lanceren
moet daarbij twee dingen weten: het ad-hoc pad is dood op de email_status-waardes
(`"verified"` bestaat niet in de data), en de brug-routing hangt op een
`website_score < 50`-drempel die vóór de schaalnormalisatie is gekozen — na de
backfill routeert die stil anders dan bij het ontwerp bedoeld.
