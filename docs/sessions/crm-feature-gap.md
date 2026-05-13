# CRM Feature Gap (Fase 1B)

**Doel:** standaard B2B outbound CRM-features langs Heatr leggen — wat heeft
hij wel, wat niet, past het bij Heatr's positionering, welke zwakte adresseert
het.

**Heatr's positionering:** interne Aerys-tool, 1 gebruiker (Sami), ~100/week
target volume, ~1000/maand schaal, alle data lokaal in Supabase. Geen SaaS,
geen multi-user, geen billing-views, geen marketplace.

## Zwakte-codes

- **Z1** — Sequence-tekst niet door Sami gereviewd
- **Z2** — Worker-uptime onbetrouwbaar
- **Z3** — Email-verifier risky-output / bounce-risk
- **Z4** — Reply-flow nooit getest
- **Z5** — 0% archetype-coverage initieel
- **Z6** — Geen E2E-tests

---

## Lead Management

| Feature | Heatr | Past? | Zwakte | Toelichting |
|---|---|---|---|---|
| Lead-database met search/filter | ✓ ja | ✓ | — | sector/archetype/score-filters geïmplementeerd |
| Bulk import + dedup | ✓ ja | ✓ | — | CSV met 4-laagse dedup, idempotency, merge-strategieën |
| Bulk-edit (multi-select status) | ✓ ja | ✓ | — | Bulk-status override op kanban |
| Lead-merge (handmatig samenvoegen) | ✗ nee | midden | Z6 | Bij dedup-fout handmatig kunnen mergen — niet kritiek bij <1000 leads |
| Lead-deduplication suggestions | ✓ gedeeltelijk | ✓ | — | Werkt bij import, niet als achtergrondscan op bestaande |
| Custom fields per lead | ✗ nee | nee | — | Heatr heeft 70 vaste velden; custom fields zijn SaaS-luxe, hier overkill |
| Lead enrichment (auto) | ✓ ja | ✓ | Z5 | 15 stappen pipeline |
| Manual enrichment trigger | ✓ ja | ✓ | Z5 | Re-enqueue admin-knop |
| Lead-tagging (vrije tags) | ✗ nee | midden | Z1 + Z6 | Voor Sami: "test-lead", "review-needed" — handig maar niet blocker |
| Lead-list smart segments | ✓ gedeeltelijk | ✓ | — | Filters persisted in URL/localStorage; geen save-as-segment |
| Saved views / smart lists | ✗ nee | midden | — | "Toon mij elke ochtend leads klaar voor recontact" |

---

## Activity Tracking

| Feature | Heatr | Past? | Zwakte | Toelichting |
|---|---|---|---|---|
| Email-sent tracking | ✓ ja | ✓ | Z4 | Via lead_campaign_history |
| Email-opens tracking | ✗ nee | midden | Z3 + Z4 | Vereist Warmr-integratie; niet door Heatr beheersbaar |
| Email-click tracking | ✗ nee | laag | — | Idem — Warmr-territorium |
| Reply tracking + classifier | ✓ ja | ✓ | Z4 | Webhook + Claude classifier |
| Per-lead activity timeline (chronologisch) | ✗ gedeeltelijk | **ja** | **Z4 + Z6** | `heatr_lead_timeline` bestaat maar wordt niet als scrollable feed getoond op kanban-card |
| Manual log entry (call notes / DM) | ✗ nee | **ja** | Z1 + Z4 | Sami doet 1-op-1 calls — die context moet ergens landen |
| Last-activity widget per lead | ✓ gedeeltelijk | ✓ | Z4 | last_outbound/inbound_at tonen, geen activity-detail |
| Activity-feed cross-lead (recente events overall) | ✗ nee | midden | Z4 | "Laatste 20 events": replies, sends, status-changes |

---

## Communication

| Feature | Heatr | Past? | Zwakte | Toelichting |
|---|---|---|---|---|
| Email-thread view per lead | ✗ nee | **ja** | **Z4** | Verzonden + reply chronologisch in 1 thread; cruciaal voor reply-context |
| Reply draft-suggestion | ✓ ja | ✓ | Z4 | Claude reply-drafter |
| Reply-send vanuit CRM | ✗ nee | midden | Z4 | Niveau 2 promotie — guard nodig |
| In-app messaging tussen team | ✗ nee | nee | — | Solo |
| Email templates (statisch) | ✓ ja | ✓ | Z1 | Sequence-templates zijn dit |
| Snippets / canned responses | ✗ nee | midden | Z4 | "FAQ-antwoorden" voor `question`-replies — Claude doet dit deels |
| Sequence-builder GUI | ✗ nee | midden | Z1 | Heatr heeft code-defined sequences. UI-bouwer = SaaS-complexity |
| Sequence-A/B test | ✗ nee | midden | Z1 + Z6 | Eerst echte sends nodig vóór A/B zinnig is |

---

## Pipeline / Stage Management

| Feature | Heatr | Past? | Zwakte | Toelichting |
|---|---|---|---|---|
| Kanban-board met statuskolommen | ✓ ja | ✓ | — | 9 statussen, drag-drop |
| Drag-and-drop tussen stages | ✓ ja | ✓ | — | Met type-to-confirm guard |
| Custom stages | ✗ nee | nee | — | Zou SaaS-luxe zijn; Heatr's 9 statussen zijn afgeleid |
| Stage-time tracking (hoe lang in elke stage) | ✗ gedeeltelijk | **ja** | **Z4** | status_changed_at bestaat; "12 dagen in cooldown" zichtbaar maken |
| Stage-conversion-rate | ✗ nee | **ja** | Z3 + Z4 | "X% van 'in_sequence' → 'actief_gesprek'" — funnel-analytics aan kanban-headers |
| Won/Lost-tracking | ✗ gedeeltelijk | midden | — | crm_deals tabel bestaat, niet actief gebruikt |
| Deal-value (€-tracking) | ✗ nee | midden | — | Bij CRM voor sales-team gebruikelijk; voor Heatr: optioneel |
| Pipeline-visualisatie (funnel) | ✓ gedeeltelijk | ✓ | Z3 | /analytics/funnel bestaat — niet zichtbaar in CRM |

---

## Tasks & Reminders

| Feature | Heatr | Past? | Zwakte | Toelichting |
|---|---|---|---|---|
| Task-systeem (per lead) | ✓ gedeeltelijk | ✓ | Z1 + Z4 | `heatr_crm_tasks` bestaat, niet zichtbaar in /crm/activity |
| Reminder bij datum (recontact_later) | ✓ gedeeltelijk | ✓ | Z1 | recontact_after kolom; geen automatische notificatie |
| Snooze-functie | ✓ gedeeltelijk | ✓ | Z1 | snoozed_until kolom in tasks |
| Daily/weekly task-digest email | ✓ ja | ✓ | — | /briefing/generate endpoint bestaat |
| Push-notifications | ✗ nee | nee | — | Geen mobile; solo |

---

## Reporting & Analytics

| Feature | Heatr | Past? | Zwakte | Toelichting |
|---|---|---|---|---|
| Pipeline overview-dashboard | ✓ ja | ✓ | — | /analytics/pipeline endpoint |
| Reply-rate metrics | ✓ gedeeltelijk | ✓ | Z3 + Z4 | Endpoints bestaan, niet zichtbaar in CRM-context |
| Bounce-rate per cohort | ✓ gedeeltelijk | ✓ | **Z3** | /analytics/email-status-breakdown — niet als banner in CRM |
| Cost-tracking | ✓ ja | ✓ | — | Per maand + per context |
| Cost-per-reply / cost-per-interested | ✓ ja | ✓ | — | /analytics/cost-attribution |
| CSV-export | ✓ ja | ✓ | — | /analytics/export/leads.csv |
| Custom report-builder | ✗ nee | nee | — | SaaS-overkill |
| Dashboards-deeplink (per archetype/sector) | ✓ gedeeltelijk | ✓ | — | URL-params op CRM-board |
| Sequence-performance per template | ✗ nee | midden | Z1 | Zodra sends draaien: cohort-analytics per template |
| Inbox-warmup status | ✓ gedeeltelijk | ✓ | Z2 + Z3 | get_ready_inboxes endpoint; niet prominent |
| Deliverability-dashboard (spam-rate / blocklist-status) | ✗ nee | midden | Z3 | Belangrijk zodra echte sends draaien |

---

## Automation

(zie ook crm-automation-shortlist.md voor detail)

| Feature | Heatr | Past? | Zwakte | Toelichting |
|---|---|---|---|---|
| Sequence-auto-stop bij reply | ✓ ja | ✓ | Z4 | stop_all_sequences_for_lead via webhook |
| Auto-classifier op reply | ✓ ja | ✓ | Z4 | Claude classifier |
| Auto-status-derive uit timeline | ✓ ja | ✓ | — | derive_status logic |
| Auto-pause bij budget-tier | ✓ ja | ✓ | — | tier_50_hit / tier_100_hit |
| Auto-pause bij bounce-rate >X% | ✗ nee | **ja** | **Z3** | Cohort-niveau circuit-breaker |
| Auto-archive bij N maanden silence | ✗ nee | midden | — | Voor lange-termijn hygiëne |
| If-then automation builder (visueel) | ✗ nee | nee | — | Zapier-territorium; voor Heatr code-defined |
| Workflow-templates | ✗ gedeeltelijk | ✓ | Z1 | Sequence-templates zijn dit |

---

## Integration & Data

| Feature | Heatr | Past? | Zwakte | Toelichting |
|---|---|---|---|---|
| Native email-integration (sending) | ✓ via Warmr | ✓ | — | |
| Webhook in/out | ✓ ja | ✓ | Z4 | /webhooks/warmr |
| API access | ✓ ja | ✓ | — | 30+ endpoints |
| Calendar integration | ✗ nee | midden | Z4 | Bij `interested`-reply: direct slot-scheduler. Cal.com link in opener al. |
| LinkedIn-koppeling | ✗ nee | nee | — | Juridisch grijs + buiten scope |
| Slack/Discord-alerts | ✗ nee | midden | **Z2 + Z3** | "Worker is 30min stil" / "Bounce-rate >2%" → alert |
| Zapier/Make connector | ✗ nee | nee | — | Heatr is interne tool, niet voor SaaS-integraties |
| Data-export (CSV/JSON/Excel) | ✓ ja | ✓ | — | CSV via endpoint |
| GDPR data-export per lead | ✓ gedeeltelijk | ✓ | — | gdpr_log tabel bestaat |
| Right-to-be-forgotten flow | ✗ gedeeltelijk | midden | — | unsubscribe-flow er, maar geen echte data-erasure |

---

## Quality / Governance

| Feature | Heatr | Past? | Zwakte | Toelichting |
|---|---|---|---|---|
| Audit-trail (wie deed wat wanneer) | ✓ ja | ✓ | — | heatr_campaigns audit, heatr_lead_timeline |
| Send-kill-switch | ✓ ja | ✓ | — | ENABLE_CAMPAIGN_SENDS env |
| Cost-limits | ✓ ja | ✓ | — | 3-laagse cost-guard |
| Rate-limiting per endpoint | ✗ gedeeltelijk | midden | — | rate_limit_state bestaat, scope onduidelijk |
| Test-mode per lead (geen send) | ✗ nee | **ja** | **Z6** | "Deze lead is testlead, voer alle stappen uit maar stuur niets" |
| Approval-workflow (manager moet OK geven) | ✗ nee | nee | — | Solo, geen manager |
| Spam-word detection | ✓ ja | ✓ | Z3 | sequence_engine validator |
| Domain-blocklist (eigen) | ✗ nee | midden | Z3 | "Stuur nooit naar @gmail.com / consumer-domains" — defensieve filter |

---

## CRM-specifieke best-practices van competitor-analyse

### HubSpot/Pipedrive-style
| Feature | Heatr | Past? | Zwakte |
|---|---|---|---|
| Lead-scoring met visual breakdown | ✓ gedeeltelijk | ✓ | Z1 + Z5 |
| Activity-aggregaten op lead-card | ✗ gedeeltelijk | ✓ | Z4 |
| Multi-pipeline support | ✗ nee | nee | — |
| Deal-rotting alert (stage te lang) | ✗ nee | **ja** | Z4 — "lead in cooldown >120d zonder reactie" |

### Close-style (sales-engagement focus)
| Feature | Heatr | Past? | Zwakte |
|---|---|---|---|
| Power-dialer | n.v.t. | nee | — geen telefonie |
| Smart-views met focus-mode | ✗ nee | midden | — |
| Email-syncing (alle replies in CRM) | ✓ via webhook | ✓ | Z4 |

### Apollo/Outbound-tool style
| Feature | Heatr | Past? | Zwakte |
|---|---|---|---|
| Contact-database scraping | ✓ ja | ✓ | — |
| Persona-targeting | ✓ ja (archetype) | ✓ | Z5 |
| Sequence A/B met statistical sig | ✗ nee | midden | Z1 |
| Buying-intent signals | ✗ gedeeltelijk | ✓ | Z1 — meta_ads_active is dit deels |

### Instantly/Smartlead-style (deliverability focus)
| Feature | Heatr | Past? | Zwakte |
|---|---|---|---|
| Inbox-rotation | ✓ via Warmr | ✓ | Z2 |
| Warmup-monitoring | ✓ gedeeltelijk | ✓ | Z2 + Z3 |
| Reputation-score per inbox | ✗ nee | midden | Z3 |
| Bounce-rate cohort-circuit-breaker | ✗ nee | **ja** | **Z3** |
| Spam-trigger detector (woordlijst + context) | ✓ ja | ✓ | Z3 |
| Per-domain throttle | ✗ nee | midden | Z3 — "max 5 emails/dag naar gmail.com" |

---

## Top-level features die echt ontbreken (gerangschikt op zwakte-impact)

### Hoog (Z3 / Z4 / Z2)
1. **Email-thread-view per lead** — Z4 — vol thread (verstuurd + reply) chronologisch in lead-detail
2. **Per-lead activity-timeline (events feed)** — Z4 + Z6 — chronologische events op kanban-card-flip
3. **Bounce-rate cohort circuit-breaker** — Z3 — auto-pause bij >2%
4. **Worker-down alert (Slack/Discord/email)** — Z2 — proactief ipv passieve pulse-icon
5. **Per-inbox reputation-tracking** — Z3 — welke inbox is gezond, welke vragend

### Midden (Z1)
6. **Per-lead manual log entry** — Z1 — call-notes, DM-context, vrij-tekst veld
7. **Sequence-snapshot inspect per send** — Z1 — wat is er werkelijk verstuurd
8. **Deal-rotting alert** — Z4 — lead te lang in stage-X zonder progressie
9. **Calendar-slot widget bij interested-reply** — Z4 — direct Cal.com-link genereren

### Laag (out-of-scope of edge case)
- Lead-merge UI
- Custom fields
- Saved views / smart lists
- Lead-tagging
- Sequence-builder GUI
- Domain-blocklist
- Per-domain throttle

---

## Wat NIET past bij Heatr's positionering

Skip permanent (overkill voor solo Aerys-tool):

- Multi-user permissions / role-based access
- Workflow-builder visueel (Zapier-style)
- Marketplace / third-party app-store
- White-label / co-branding
- Salesforce/HubSpot two-way sync (Heatr is de bron, niet de target)
- Enterprise SSO / SAML
- Sub-accounts voor klanten van Aerys
- Native mobile app

---

## Conclusie

**Heatr heeft als CRM een 8/10:** kanban + drag-drop + filters + audit-trail
+ analytics zijn solid. De gaten zitten in:

1. **Reply-flow zichtbaarheid** (Z4) — thread-view, timeline, classifier-trends
2. **Deliverability-laag** (Z3) — bounce-cohorts, inbox-reputation, auto-pause
3. **Worker-observability** (Z2) — actieve alerts ipv passief
4. **Pre-send transparency** (Z1) — sequence-content + opener zichtbaar in CRM-card

**Die vier blokken** zijn waar Fase-2 prioritering tussen gaat kiezen. Alles
daarbuiten is óf al goed (lead-management, audit, cost-tracking) óf
buiten-scope (multi-user, marketplaces).
