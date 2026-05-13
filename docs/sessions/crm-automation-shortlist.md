# CRM Automation Shortlist (Fase 1C)

**Doel:** handmatige acties in `/crm/activity` (en aangrenzende flows) die
geautomatiseerd zouden moeten worden — gekoppeld aan zwaktes uit de
state-of-Heatr analyse.

## Zwakte-codes

- **Z1** — Sequence-tekst niet door Sami gereviewd
- **Z2** — Worker-uptime onbetrouwbaar
- **Z3** — Email-verifier risky-output / bounce-risk
- **Z4** — Reply-flow nooit getest
- **Z5** — 0% archetype-coverage initieel
- **Z6** — Geen E2E-tests

---

## A. Reply-handling automations

### A1. Auto-stop sequence bij elke reply
| Status | Detail |
|---|---|
| Trigger | Webhook `/webhooks/warmr` met event=replied/interested/etc. |
| Actie | `stop_all_sequences_for_lead()` + `crm_stage` update + 90d cooldown |
| **Heatr-status** | ✓ **Bestaat** sinds migration 009 |
| Zwakte | Z4 |

### A2. Auto-tag "actief_gesprek" bij interested-classifier
| Status | Detail |
|---|---|
| Trigger | reply-classifier returnt `category=interested` |
| Actie | `manual_status_override` op leads niet zetten — derive_status doet dit al |
| Heatr-status | ✓ **Bestaat** via derive_status logica |
| Zwakte | Z4 |

### A3. Auto-snooze bij out-of-office
| Status | Detail |
|---|---|
| Trigger | reply-classifier returnt `category=auto_reply` met `return_date` |
| Actie | `recontact_after = return_date`, status → `recontact_later` |
| **Heatr-status** | ⚠ **Half** — classifier extraheert return_date, maar het wordt niet automatisch op de lead gezet |
| Voorgesteld | Webhook handler bij `auto_reply`: zet `recontact_after` als classifier dit returnt |
| Zwakte | Z4 + Z1 |
| Schaal | klein (3-5 OOO replies/maand) |

### A4. Auto-archive bij N maanden silence
| Status | Detail |
|---|---|
| Trigger | Lead in `niet_aangeschreven`/`klaar_voor_recontact` >180 dagen zonder activiteit |
| Actie | `manual_status_override = "afgemeld"` + reden "auto_archived_stale" |
| Heatr-status | ✗ **Bestaat niet** |
| Voorgesteld | Cron-job 1×/week |
| Zwakte | Geen directe Z, wel hygiëne (CRM-board vervuilt anders) |
| Risico | Te aggressief = je verliest leads die na 7 maanden alsnog terugbellen |

### A5. Auto-email-thread reconstruction
| Status | Detail |
|---|---|
| Trigger | Reply binnen webhook |
| Actie | Joinen verstuurde mail uit `lead_campaign_history.sequence_steps[step_index]` met de reply, presentatie als chronologisch thread |
| Heatr-status | ⚠ **Bestaat niet als view**, data is er wel |
| Voorgesteld | Lead-detail-pagina krijgt thread-component |
| Zwakte | **Z4** — kritisch voor reply-context |

---

## B. Deliverability automations

### B1. Auto-pause campaign bij bounce-rate >X%
| Status | Detail |
|---|---|
| Trigger | Cohort van een campaign heeft bounce-rate >2% over laatste 24u |
| Actie | Set campaign `status=paused`, log naar `system_alerts`, send Slack/email alert |
| **Heatr-status** | ✗ **Bestaat niet** |
| Voorgesteld | Cron 1×/u: query `heatr_blocked_sends` per campaign + diff tegen totaal verzonden |
| Zwakte | **Z3** — kritisch |
| Schaal | 1 check/u, lichte query |

### B2. Auto-blacklist bouncing-domain
| Status | Detail |
|---|---|
| Trigger | 3+ leads van zelfde domain bouncen binnen week |
| Actie | Voeg domain toe aan `email_blocklist` table; nieuwe leads van dat domein → status `not_found` direct |
| Heatr-status | ✗ **Bestaat niet** |
| Voorgesteld | Wacht tot je echte data hebt; nu speculatief |
| Zwakte | Z3 (preventief) |

### B3. Auto-skip catchall na N bounces
| Status | Detail |
|---|---|
| Trigger | catchall-email lead heeft eerdere bounce |
| Actie | Markeer als not-sendable, blok in `is_sendable()` |
| Heatr-status | ⚠ Generiek bounce-block bestaat, niet catchall-specifiek |
| Voorgesteld | Verfijn `is_sendable()` met bounce-history check |
| Zwakte | Z3 |

### B4. Auto-rotate inboxes bij overcap
| Status | Detail |
|---|---|
| Trigger | Verzending zou inbox over daily-cap duwen |
| Actie | Switch naar volgende `ready` inbox |
| Heatr-status | ⚠ **Aanname** — Warmr-side feature, niet door Heatr beheersbaar |
| Zwakte | Z2 + Z3 |
| Conclusie | Geen Heatr-actie nodig (handsoff naar Warmr) |

### B5. Pre-flight bounce-risk score
| Status | Detail |
|---|---|
| Trigger | Bij `/campaigns/launch` |
| Actie | Bereken expected bounce-rate o.b.v. cohort: % `risky` × historische bounce-pct + % catchall × historisch + % verified × historisch. Block als >5%. |
| Heatr-status | ✗ **Bestaat niet** |
| Voorgesteld | Pre-launch dialog "Verwachte bounce: 3.2% — doorgaan?" |
| Zwakte | **Z3** |

---

## C. Worker / pipeline observability automations

### C1. Worker-down alert na X min stilte
| Status | Detail |
|---|---|
| Trigger | `enrichment_jobs.completed_at` heeft >30min geen update terwijl pending>0 |
| Actie | Slack/Discord/email alert via webhook |
| **Heatr-status** | ⚠ Sidebar pulse-icon toont status passief, geen alert |
| Voorgesteld | Cron 1×/15min: check + push naar `HEATR_ALERT_WEBHOOK_URL` env-var |
| Zwakte | **Z2** |
| Schaal | klein, 96 checks/dag |

### C2. Auto-restart worker (daemon-supervisie)
| Status | Detail |
|---|---|
| Trigger | Worker-process crashes |
| Actie | systemd / launchd start opnieuw |
| Heatr-status | ✗ **Niet geïmplementeerd** — depends op `caffeinate -dimsu` op Mac |
| Voorgesteld | Out-of-app: systemd-service of launchd plist op de Mac |
| Zwakte | **Z2** kritisch |

### C3. Cost-tier alert via webhook
| Status | Detail |
|---|---|
| Trigger | tier_50_hit OF tier_100_hit |
| Actie | Push naar `HEATR_ALERT_WEBHOOK_URL` |
| Heatr-status | ⚠ Tier-detection bestaat, alleen UI-feedback (geen webhook) |
| Voorgesteld | Klein endpoint-uitbreiding |
| Zwakte | — (cost is goed bewaakt; alert is convenience) |

### C4. Auto-retry failed jobs met backoff
| Status | Detail |
|---|---|
| Trigger | `enrichment_jobs.status=failed` AND `retry_count < 3` |
| Actie | Reset to `pending` na exponential-backoff (1u, 4u, 24u) |
| Heatr-status | ⚠ retry_count veld bestaat, auto-retry-loop onduidelijk |
| Voorgesteld | Verifieren in `process_next_enrichment` of dit echt gebeurt |
| Zwakte | Z2 + Z6 |

### C5. Stale-enrichment detector
| Status | Detail |
|---|---|
| Trigger | Lead heeft `archetype_classified_at < 90d ago` (data-vers-heid) |
| Actie | Markeer als "needs_refresh" in CRM, optioneel auto-re-enqueue |
| Heatr-status | ✗ Bestaat niet |
| Zwakte | Z5 |
| Schaal | Pas zinvol bij 6+ maanden ouderdom |

---

## D. Sequence-launch automations

### D1. Pre-launch enrichment-completeness check
| Status | Detail |
|---|---|
| Trigger | `/campaigns/launch` met lead waar archetype=NULL |
| Actie | **Block launch** met error "deze lead heeft nog geen archetype, re-enqueue eerst" |
| **Heatr-status** | ⚠ Personalization-gate werkt, archetype-completeness niet expliciet check |
| Voorgesteld | Toevoegen aan launch endpoint preflight |
| Zwakte | **Z5** |

### D2. Auto-select template op basis van sector
| Status | Detail |
|---|---|
| Trigger | `/campaigns/launch` zonder `template_id` |
| Actie | `template_for_sector(first_lead.sector)` |
| **Heatr-status** | ✓ **Bestaat** |
| Zwakte | Z1 |

### D3. Personalization-score gate
| Status | Detail |
|---|---|
| Trigger | Per lead bij launch: pers_score < threshold |
| Actie | Skip lead, log in personalization_gate response |
| **Heatr-status** | ✓ **Bestaat** met buckets auto/review/skip |
| Zwakte | Z1 |

### D4. Tone-mismatch warning
| Status | Detail |
|---|---|
| Trigger | template.sector ≠ lead.sector |
| Actie | UI-banner + bulk-deselect-knop |
| **Heatr-status** | ✓ **Bestaat** in CampagneLaunch.tsx |
| Zwakte | Z1 |

### D5. Pre-send opener-quality validation
| Status | Detail |
|---|---|
| Trigger | Per lead in launch flow |
| Actie | `is_quality_opener()` check + clean_claude_opener strip |
| **Heatr-status** | ✓ **Bestaat** sinds vorige sessie |
| Zwakte | Z1 + Z6 |

### D6. Inbox-capacity check pre-launch
| Status | Detail |
|---|---|
| Trigger | selected_leads_count > sum(inbox.daily_cap) |
| Actie | UI-warning |
| **Heatr-status** | ✓ **Bestaat** in CampagneLaunch.tsx |
| Zwakte | Z2 |

### D7. Mailing-window enforcement
| Status | Detail |
|---|---|
| Trigger | Bij scheduled send: time-of-day buiten 08:30-11:00 lokaal |
| Actie | Vertraag tot eerstvolgende valid window |
| Heatr-status | ⚠ **Aanname** — vermoedelijk Warmr-side, niet door Heatr enforced |
| Zwakte | Z3 (deliverability) |
| Conclusie | Verifiëren bij eerste smoke-test |

### D8. Per-archetype sequence-routing
| Status | Detail |
|---|---|
| Trigger | Op archetype-detection (lichaamswerk vs cosmetisch) |
| Actie | Use sector-aware template + observation-block-allowlist |
| **Heatr-status** | ✓ **Bestaat** via `_ARCHETYPE_ALLOWED_BLOCKS` |
| Zwakte | Z1 |

---

## E. Status / lifecycle automations

### E1. Auto-derive status uit timeline
| Status | Detail |
|---|---|
| Trigger | Bij elke fetch |
| Actie | 9-status beslissingsboom |
| **Heatr-status** | ✓ **Bestaat** via `derive_status` |
| Zwakte | Z4 |

### E2. Recontact-due reminder
| Status | Detail |
|---|---|
| Trigger | `recontact_after <= NOW()` |
| Actie | Sidebar-badge count + (toekomst) email-notify |
| **Heatr-status** | ✓ **Badge bestaat**, geen email-notify |
| Voorgesteld | Briefing-mail uitbreiden met "X leads klaar voor recontact vandaag" |
| Zwakte | Z1 |

### E3. Deal-rotting detector
| Status | Detail |
|---|---|
| Trigger | Lead in `actief_gesprek` >14d zonder nieuwe inbound |
| Actie | Markeer als "stuck", visueel rood op kanban |
| Heatr-status | ✗ **Bestaat niet** |
| Voorgesteld | Bij activity-board endpoint: `days_since_last_inbound >14 AND status='actief_gesprek'` → `_stuck=true` flag |
| Zwakte | Z4 |

### E4. Auto-bulk-deselect risky leads bij launch
| Status | Detail |
|---|---|
| Trigger | Selectie bevat `is_risky_email()` leads |
| Actie | UI-warning met deselecteer-knop |
| **Heatr-status** | ✓ **Bestaat** in CampagneLaunch.tsx |
| Zwakte | Z3 |

---

## F. Test / smoke-flow automations

### F1. Test-mode toggle per lead
| Status | Detail |
|---|---|
| Trigger | Lead heeft `is_test_lead=true` flag |
| Actie | Pipeline draait alle stappen behalve daadwerkelijke send naar Warmr |
| Heatr-status | ✗ **Bestaat niet als veld** |
| Voorgesteld | Migration 017: `is_test_lead boolean default false` + send-flow check |
| Zwakte | **Z6** kritisch — eerste smoke test heeft dit nodig |

### F2. Test-mail naar eigen inbox spiegelen
| Status | Detail |
|---|---|
| Trigger | `is_test_lead=true` of lead's email is in `HEATR_TEST_EMAIL_DOMAINS` |
| Actie | Push naar Warmr maar BCC `OPERATOR_EMAIL` zodat Sami ziet wat verzonden is |
| Heatr-status | ✗ **Bestaat niet** |
| Zwakte | Z6 |
| Risico | Verdere Warmr-coupling bij implementatie |

### F3. Smoke-test-result tracking
| Status | Detail |
|---|---|
| Trigger | Manueel: "Begin smoke test" knop |
| Actie | Track 1 lead → mail-1 → reply → Mail-2 cancellation → reply-classifier-tag → drafter-suggestie. Alle 6 mijlpalen vinkjes zetten. |
| Heatr-status | ✗ **Bestaat niet** |
| Voorgesteld | Apart `/admin/smoke-test`-page met checklist |
| Zwakte | **Z6** |

---

## G. Manual-effort eliminators (UX-automations)

### G1. Quick-actions per kanban-card
| Status | Detail |
|---|---|
| Trigger | Hover op card |
| Actie | Buttons: pauzeer sequence / open thread / draft reply / mark interested |
| Heatr-status | ⚠ Drag-drop bestaat; quick-action-icons nog niet |
| Voorgesteld | UI-uitbreiding |
| Zwakte | Z4 + Z1 |

### G2. Bulk-action: enqueue voor enrichment-step X
| Status | Detail |
|---|---|
| Trigger | Selectie + admin-action "re-enqueue voor archetype" |
| Actie | Bulk re-enqueue met `enrichment_types=["archetype"]` |
| **Heatr-status** | ⚠ **Bestaat in /admin/re-enqueue-stale-leads** maar niet binnen kanban-bulk-actions |
| Zwakte | Z5 |

### G3. Auto-fill missing first_name uit website-scrape
| Status | Detail |
|---|---|
| Trigger | Lead heeft email maar geen `contact_first_name` |
| Actie | Owner-extractor draait extra op /team page voor enkel-lead |
| Heatr-status | ✓ Owner-extractor bestaat als step in pipeline |
| Voorgesteld | Trigger als G2 — re-enqueue voor `["owner_extract"]` |
| Zwakte | Z1 |

### G4. Suggest-time slot bij interested-reply
| Status | Detail |
|---|---|
| Trigger | reply-classifier=interested EN drafter genereert suggestion |
| Actie | Reply-drafter voegt expliciete cal.com link toe (al gebouwd) + UI-quick-action "Open Cal.com voor 1 op 1" |
| Heatr-status | ✓ Cal.com auto-injection bestaat (zodra `HEATR_SCHEDULING_URL` gezet is) |
| Zwakte | Z4 |

---

## Samenvatting per zwakte

### Z1 — Sequence-tekst review
**Bestaat al:** D2, D3, D4, D5, D6, D8, E1, E2.
**Te bouwen (high impact):** A5 (thread-view), G1 (quick-actions), G3 (first_name-fill).

### Z2 — Worker-uptime
**Bestaat al:** Sidebar pulse, queue-health endpoint.
**Te bouwen (kritisch):** **C1 (worker-down alert)**, C2 (daemon-supervisie out-of-app), C4 (verify retry-logic).

### Z3 — Bounce-risk / deliverability
**Bestaat al:** spam-word detector, `is_sendable()`, gebruiker-warning bij risky-selection.
**Te bouwen (high impact):** **B1 (bounce-rate auto-pause)**, **B5 (pre-flight bounce-risk score)**, B2 (domain-blacklist), B3 (catchall + bounce history).

### Z4 — Reply-flow
**Bestaat al:** A1, A2, classifier, reply-drafter, sidebar-badge.
**Te bouwen (kritisch):** **A5 (email-thread-view)**, **A3 fix (auto-snooze OOO)**, E3 (deal-rotting).

### Z5 — Archetype/coverage
**Bestaat al:** archetype-classifier step, /admin/re-enqueue admin-action.
**Te bouwen:** **D1 (pre-launch completeness check)**, C5 (stale-detector), G2 (bulk-step-enqueue).

### Z6 — Geen E2E-tests
**Bestaat:** unit-tests, render-script.
**Te bouwen (kritisch voor go-live):** **F1 (test-mode flag)**, **F2 (BCC-spiegel)**, **F3 (smoke-test-tracker)**.

---

## Top-10 te bouwen (gerangschikt op zwakte-coverage × bouwtijd)

Ranking is **suggestief — definitieve prioritering = Fase 2**:

1. **C1** — Worker-down alert (Z2, klein, hoge frequentie)
2. **A5** — Email-thread-view (Z4, midden bouwtijd, kritisch voor eerste replies)
3. **B1** — Bounce-rate auto-pause (Z3, midden bouwtijd, voorkomt rampscenario's)
4. **F1** — Test-mode flag per lead (Z6, klein, ontgrendelt smoke-test)
5. **D1** — Pre-launch enrichment-completeness check (Z5, klein, voorkomt halve sends)
6. **A3** — Auto-snooze OOO uitbreiden (Z4 + Z1, klein, finishing touch)
7. **B5** — Pre-flight bounce-risk score (Z3, midden, sales-confidence)
8. **F3** — Smoke-test-tracker (Z6, midden, eenmalig zinvol)
9. **G1** — Quick-actions per card (Z4 + Z1, midden, UX-luxe maar dagelijkse winst)
10. **E3** — Deal-rotting detector (Z4, klein, pure visuele alert)

---

## Wat NIET te automatiseren

- Sequence-content schrijven (Sami's stem, blijft handmatig)
- Reply-versturen (Niveau 2 — bewust handmatig houden voor v1)
- Lead-scoring weights (statisch in `config/scoring_weights.py`, geen auto-tuning)
- Archetype-grenswaarden (Claude beslist)
- Manual override-acties (sleep-actie blijft confirm-vraag triggeren)

---

## Conclusie

Heatr heeft al een solide automation-laag binnen de pipeline. **De overgebleven
gaten zitten in observability (alerts), deliverability (auto-pause), en
testbaarheid (test-mode)** — niet in core-flow. Dat is geruststellend voor
go-live.

**Drie automations zijn echte blockers** voor productie:
1. **C1** worker-down alert (Z2)
2. **B1** bounce-rate circuit-breaker (Z3)
3. **F1** test-mode-flag (Z6)

Rest is hygiëne en UX-finishing.
