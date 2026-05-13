# CRM Priorities (Fase 2 — Prioritering)

Synthese van drie audits ([data](crm-data-audit.md), [feature-gap](crm-feature-gap.md),
[automation](crm-automation-shortlist.md)) tot één gerangschikte bouwlijst.

## Scoring-methodiek

```
Score = (impact × risk-reductie) / bouwtijd-multiplier

impact (1-5):         hoeveel zwaktes raakt het + hoe diep
bouwtijd-multiplier:  <2u=1  |  2-8u=2  |  8-24u=4  |  >24u=8
risk-reductie (1-5):  zou Heatr zonder dit een incident hebben in eerste 100 leads?
```

**Cruciaal:** Sami zit in studieperiode → kan niet deployen → features die
**alleen** gevalideerd kunnen worden mét live productie-data krijgen risk-reductie
**verlaagd met -1** (max 3). Want: niet door Sami testbaar = waarde uitgesteld.

## Zwakte-codes (recap)

- **Z1** Sequence-tekst niet door Sami gereviewd
- **Z2** Worker-uptime onbetrouwbaar
- **Z3** Email-verifier risky / bounce-risk
- **Z4** Reply-flow nooit getest
- **Z5** 0% archetype-coverage initieel
- **Z6** Geen E2E-tests

---

## 🏆 Top 5 — bouwen vóór eerste 100 leads

### #1 — Test-mode flag per lead (F1 + F2)

| | |
|---|---|
| Zwaktes | **Z6** (kritisch) + Z3 (BCC-zichzelf) |
| Impact | 5 — ontgrendelt smoke-test, voorkomt accidenteel-naar-echte-prospect |
| Bouwtijd | <2u (1u) — migratie + `is_test_lead` boolean + `is_sendable()` aanpassing + `_build_lead_payload` BCC-logica |
| Risk-reductie | 5 — eerste echte send zonder test-mode = hoog risico |
| **Score** | (5 × 5) / 1 = **25.0** |

**Reasoning:** Heatr's hele eerste cohort hangt af van smoke-test 1 → 5 → 20.
Zonder test-mode-flag is "1 testlead" gewoon een lead waar je hoopt dat het
goed gaat. Dit is de enige feature waarmee je vóór go-live kan oefenen zonder
risico voor je domein-reputation.

### #2 — Email-thread-view per lead (A5)

| | |
|---|---|
| Zwaktes | **Z4** (kritisch) + Z6 |
| Impact | 4 — reply-context cruciaal voor handmatige respons in eerste cohort |
| Bouwtijd | 2-8u (4u) — joinen `lead_campaign_history.sequence_steps[step_index]` met `reply_inbox` rows, weergeven in lead-detail |
| Risk-reductie | 4 — eerste replies handmatig pareren = grootste workload-risico van go-live |
| **Score** | (4 × 4) / 2 = **8.0** |

**Reasoning:** Reply-flow is nooit getest. De drafter geeft suggesties, maar
zonder volledige thread (verstuurd → reply → verstuurd → reply) kan Sami niet
beoordelen of de suggestie past bij wat we eerder zeiden. Voor 5-20-leads
cohort handmatig draaibaar; daarboven essentieel.

### #3 — Pre-launch enrichment-completeness check (D1)

| | |
|---|---|
| Zwaktes | **Z5** + Z1 |
| Impact | 4 — voorkomt halve mails bij niet-volledig-verrijkte leads |
| Bouwtijd | <2u (45min) — `/campaigns/launch` preflight: weiger leads waar archetype IS NULL of personalization < threshold |
| Risk-reductie | 4 — leads zonder archetype krijgen verkeerde toon (live geverifieerd: 14% had nog geen archetype na 24u) |
| **Score** | (4 × 4) / 1 = **16.0** |

**Reasoning:** Personalization-gate bestaat al deels, maar archetype-completeness
is niet expliciet gechecked. Eén missende archetype = mismatch tussen
v1.0_cosmetisch en alt-zorg sequence. Snel te bouwen, hoog effect.

### #4 — Per-lead activity-timeline op kanban-card-flip (deel van Z4)

| | |
|---|---|
| Zwaktes | **Z4** + Z6 |
| Impact | 4 — chronologisch zicht op events: import → enrich → email_sent → reply → status-change |
| Bouwtijd | 2-8u (3u) — endpoint `/leads/{id}/timeline` + frontend collapsible op CRM-card |
| Risk-reductie | 4 — bij rare cases ("waarom is deze lead in geen_interesse zonder reply?") direct evident |
| **Score** | (4 × 4) / 2 = **8.0** |

**Reasoning:** `heatr_lead_timeline` tabel bestaat al + wordt gevuld; onbenutte
data. Geen nieuwe DB-structuur nodig. Maakt elk debuggen-moment 5× sneller.
Gecombineerd met #2 (thread-view) krijg je volledig retroperspectief per lead.

### #5 — Per-lead manual log entry (call notes / DM context)

| | |
|---|---|
| Zwaktes | Z1 + Z4 |
| Impact | 3 — Sami doet 1-op-1 calls, die context moet ergens landen |
| Bouwtijd | <2u (1.5u) — migratie `lead_notes` table + `POST /leads/{id}/notes` + UI textarea op detail-page |
| Risk-reductie | 3 — zonder dit verlies je context tussen contactmomenten, lijdt aan "wat zei deze lead vorige week?" |
| **Score** | (3 × 3) / 1 = **9.0** |

**Reasoning:** Sales-CRM-fundament. Eerst echt zinnig zodra je sales-gesprekken
voert (= ná eerste cohort), maar bouwen kost <2u en faciliteert betere context
zonder live data. Defensieve keuze: bouwen vóór nodig is.

---

## 🥈 Mid 5-15 — bouwen ná eerste cohort feedback

### #6 — Worker-down alert (C1)

| | |
|---|---|
| Zwaktes | **Z2** |
| Impact | 4 |
| Bouwtijd | 2-8u (3u) — cron-check + Slack/Discord webhook integration |
| Risk-reductie | 3 (verlaagd van 5 — Sami kan niet deployen, dus alerts werken niet richting echte ops-context) |
| **Score** | (4 × 3) / 2 = **6.0** |

**Waarom niet eerder:** Worker-status is al passief zichtbaar in sidebar.
Alert-infra (Slack-integration) heeft externe service nodig. Bouwen na go-live
zodra je weet waar je echte ops-monitoring zit (mail? Slack? Telegram?).

### #7 — Bounce-rate cohort circuit-breaker (B1)

| | |
|---|---|
| Zwaktes | **Z3** |
| Impact | 4 |
| Bouwtijd | 2-8u (4u) — cron + `heatr_blocked_sends` analyse + auto-pause-status op campaign |
| Risk-reductie | 2 (verlaagd van 5 — geen live productie-data om te valideren tot eerste cohort draait) |
| **Score** | (4 × 2) / 2 = **4.0** |

**Waarom niet eerder:** Cruciaal-pad blocker maar niet testbaar zonder echte
sends. Bouwen tegelijk met of direct ná eerste cohort als bounce-data binnen is.

### #8 — Pre-flight bounce-risk score (B5)

| | |
|---|---|
| Zwaktes | Z3 + Z6 |
| Impact | 3 |
| Bouwtijd | 2-8u (3u) — cohort-analyse op `email_status` percentages |
| Risk-reductie | 2 (verlaagd — heuristic zonder live bounce-history is gokken) |
| **Score** | (3 × 2) / 2 = **3.0** |

**Waarom niet eerder:** Score-formule heeft historische bounce-data per
status-bucket nodig om calibratie-zinvol te zijn. Eerste 100 leads geven die
data.

### #9 — Auto-snooze bij OOO-reply uitbreiden (A3)

| | |
|---|---|
| Zwaktes | Z4 + Z1 |
| Impact | 3 |
| Bouwtijd | <2u (1u) — webhook-handler bij `auto_reply` met `return_date` → `recontact_after` setten |
| Risk-reductie | 2 — kleine verbetering, niet essentieel |
| **Score** | (3 × 2) / 1 = **6.0** |

**Waarom niet eerder:** Heel goedkoop, maar pas relevant zodra echte OOO-replies
binnen komen (3-5/maand verwacht). Geen go-live blocker.

### #10 — Quick-actions per kanban-card (G1)

| | |
|---|---|
| Zwaktes | Z4 + Z1 |
| Impact | 3 — UX-versnelling, niet feature-uitbreiding |
| Bouwtijd | 2-8u (4u) — hover-buttons + endpoints voor pause/draft/mark-interested |
| Risk-reductie | 2 — workflow-luxe, voorkomt geen incidenten |
| **Score** | (3 × 2) / 2 = **3.0** |

**Waarom niet eerder:** Bestaande kanban + drag-drop dekken 80% van interacties.
Quick-actions worden zinnig zodra dagelijkse CRM-gebruik patronen toont.

### #11 — Sequence-snapshot inspect per send

| | |
|---|---|
| Zwaktes | Z1 |
| Impact | 3 |
| Bouwtijd | <2u (1.5u) — `heatr_lead_campaign_history.sequence_steps` rendered weergeven in lead-detail |
| Risk-reductie | 2 |
| **Score** | (3 × 2) / 1 = **6.0** |

**Waarom niet eerder:** Audit-data bestaat, niet gerendert. Pas nodig voor
retrospectie ("wat is er twee maand geleden naar deze lead gestuurd?"). Geen
go-live blocker.

### #12 — Deal-rotting / stuck-detector (E3)

| | |
|---|---|
| Zwaktes | Z4 |
| Impact | 3 |
| Bouwtijd | <2u (1u) — `_stuck=true` flag in activity-board response + visueel rood |
| Risk-reductie | 2 (verlaagd — geen live data om threshold te calibreren) |
| **Score** | (3 × 2) / 1 = **6.0** |

**Waarom niet eerder:** Threshold (>14d in `actief_gesprek`) is gokken zonder
data. Bouw na eerste 30 leads zodat realistic threshold gekozen kan worden.

### #13 — Inbox-reputation-tracking per inbox

| | |
|---|---|
| Zwaktes | Z3 + Z2 |
| Impact | 3 |
| Bouwtijd | 2-8u (5u) — Warmr-API integratie + per-inbox metrics-endpoint |
| Risk-reductie | 2 (verlaagd — Warmr-side data nodig) |
| **Score** | (3 × 2) / 2 = **3.0** |

**Waarom niet eerder:** Zinvol bij multi-inbox setup; eerste cohort gaat via
1-2 inboxes. Premature optimization.

### #14 — Smoke-test-tracker page (F3)

| | |
|---|---|
| Zwaktes | Z6 |
| Impact | 3 |
| Bouwtijd | 2-8u (3u) — `/admin/smoke-test` page met checklist (mail-1 / reply / classifier / drafter / etc.) |
| Risk-reductie | 3 |
| **Score** | (3 × 3) / 2 = **4.5** |

**Waarom niet eerder:** Eenmalig zinvol — alleen voor de allereerste smoke-test.
Daarna dood. Hand-tracked checklist in markdown is goedkoper alternatief.

### #15 — Personalization-veld zichtbaar op kanban-card

| | |
|---|---|
| Zwaktes | Z1 |
| Impact | 3 |
| Bouwtijd | <2u (1u) — `personalized_opener`, `personalization_hooks` op kanban-detail |
| Risk-reductie | 2 |
| **Score** | (3 × 2) / 1 = **6.0** |

**Waarom niet eerder:** Onbenutte data, hoge waarde — maar overlapt met #2 +
#11. Bundel met die twee in één UI-iteratie.

---

## 📚 Backlog — bouwen indien ooit

### #16 — Cost-tier alert via webhook (C3)
- Cost-guard werkt al UI-side. Webhook = convenience, niet kritiek.
- **Waarom niet:** geen externe ops-stack om naar te alerten zolang Sami solo werkt.

### #17 — Auto-archive bij N maanden silence (A4)
- Hygiëne-feature voor 1000+ stale leads.
- **Waarom niet:** te aggressief = data-verlies; pas zinvol bij 6+ maanden gebruik.

### #18 — Saved views / smart lists
- "Toon mij elke ochtend leads klaar voor recontact"
- **Waarom niet:** URL-params + localStorage doen 80% al; saved-views is SaaS-luxe.

### #19 — Lead-merge UI (handmatig samenvoegen duplicaten)
- **Waarom niet:** Dedup werkt bij import; <1000 leads = handmatig acceptabel.

### #20 — Activity-feed cross-lead (recente events overall)
- "Laatste 20 events" stream
- **Waarom niet:** sidebar-counts + per-lead timeline samen dekken 90% van use-case.

### #21 — Stale-enrichment detector (C5)
- Re-enqueue leads waar `archetype_classified_at < 90d` ago
- **Waarom niet:** pas zinvol bij 6+ maanden; nu zijn alle leads vers.

### #22 — Per-domain throttle (max X mails/dag naar gmail.com)
- Deliverability-feature.
- **Waarom niet:** Warmr-territorium; Heatr beheert geen per-domain throttles.

### #23 — Sequence-A/B test
- Statistical significance tussen template-varianten.
- **Waarom niet:** je hebt 2 templates en geen volume voor zinvolle stat-sig (~1000 sends per variant nodig).

### #24 — Sales-pipeline Won/Lost-tracking (deals-tabel activeren)
- **Waarom niet:** geen revenue-data in Heatr; deals-tracking hoort in factuursysteem.

### #25 — Email-opens / clicks tracking
- **Waarom niet:** Warmr-territorium; Heatr ziet alleen wat Warmr's webhook doorstuurt.

---

## ❌ Expliciet afgewezen

Features die in audits stonden maar voor Heatr's solo-context overkill zijn:

### Multi-user permissions / role-based access
**Reden:** 1 gebruiker (Sami). Permissions = SaaS-luxe.

### Workflow-builder visueel (Zapier-style)
**Reden:** Heatr's workflows zijn code-defined in Python. Visual builder voegt complexity zonder waarde toe.

### Custom fields per lead
**Reden:** 70 vaste velden dekken de v1.0 spec. Custom fields zijn voor multi-tenant SaaS waar elke klant ander schema wil.

### Lead-tagging (vrije tags)
**Reden:** Status + archetype + sector + score + `manual_status_override` dekken al alle filtering-cases.

### Marketplace / third-party integrations / Zapier-connector
**Reden:** Heatr is interne tool, geen platform.

### Native mobile app
**Reden:** Web werkt op mobiel; Sami werkt vanaf laptop.

### Sequence-builder GUI
**Reden:** Sequences zijn code-defined in `config/sequence_templates.py`. GUI = onderhoudslast zonder snelheidswinst voor solo-user.

### Power-dialer / telefonie-integratie
**Reden:** Heatr is email-only outbound.

### Slack/Discord-team-messaging
**Reden:** Solo, geen team.

### LinkedIn-koppeling (auto-message)
**Reden:** Juridisch grijs + buiten Heatr's scope.

### Per-lead custom retry-policies
**Reden:** Frameworks (cost-guard, retry_count) doen het goed genoeg.

### Right-to-be-forgotten flow met data-erasure
**Reden:** Nu nog te klein om GDPR-engineering te verantwoorden; bestaand `unsubscribe`-pad is voldoende voor v1.

### Spam-trigger detector (geavanceerd ML)
**Reden:** Spam-word-blocklist in `sequence_engine.py` plus Lavender-rubric in opener_principles dekt 95% al. ML-detector = overengineering.

### Approval-workflow / manager-OK-flow
**Reden:** Geen manager.

### Enterprise SSO / SAML
**Reden:** Solo + local-only.

### Workflow-templates marketplace
**Reden:** Niemand om mee te delen.

### A/B sequence-comparator met statistical significance
**Reden:** Volume te laag (<5000 sends/jaar verwacht in jaar 1) voor zinvolle statistiek.

### Dashboards-deeplink met saveable filter-presets
**Reden:** URL-params doen al 80%; localStorage rest. Saveable presets = scope-creep.

### Reputation-scoring per inbox via externe blacklist-checks
**Reden:** Warmr's verantwoordelijkheid; Heatr is data-laag, niet deliverability-laag.

### Calendar-slot-widget bij interested-reply (eigen booking-engine)
**Reden:** Cal.com-link in opener doet dit al; eigen widget = NIH-syndroom.

### Cohort-deeplink-builder met saveable URLs
**Reden:** Browser-bookmark werkt.

---

## Samenvatting van scoring

| Rank | Feature | Zwakte | Score |
|---|---|---|---|
| 1 | Test-mode flag | Z6 + Z3 | 25.0 |
| 2 | Pre-launch completeness check | Z5 + Z1 | 16.0 |
| 3 | Per-lead manual log entry | Z1 + Z4 | 9.0 |
| 4 | Email-thread-view | Z4 + Z6 | 8.0 |
| 5 | Activity-timeline op kanban-card | Z4 + Z6 | 8.0 |

**Top-5 collectief dekt:**
- ✓ **Z6** (test-mode + smoke-flow + timeline) → kritisch voor go-live
- ✓ **Z5** (completeness check) → voorkomt halve-data sends
- ✓ **Z4** (thread-view + timeline + log entry) → reply-flow zichtbaar
- ✓ **Z1** (completeness + log entry + timeline-context) → sequence-content zichtbaar pre-send
- ⚠ **Z2** (worker-uptime) → niet in top-5; alerts in mid-tier (#6) want niet by Sami testbaar nu
- ⚠ **Z3** (bounce-risk) → niet in top-5; circuit-breaker in mid-tier (#7) want vereist live data

**Niet gedicht in top-5:** Z2 + Z3 — beide afhankelijk van live data of externe ops-infra. Dat is acceptabel voor go-live want:
- Z2 wordt gemonitord via sidebar-pulse (passief)
- Z3 wordt voorkomen via existing `is_sendable()` + spam-words + cost-guard

## Totaal bouwtijd top-5

~10.5 uur, verdeeld over 2-3 sessies van 4u. Past binnen 1-2 weken werkbaar
plan voorafgaand aan eerste 100-leads cohort.
