# Preflight — C-canary (DE arming-gate; armen = deze lijst aflopen)

> Eén document, één waarheid. Als elk vakje kan worden afgevinkt mag de campagne
> actief; zo niet, dan niet. (Verbeterplan Sami 2026-08-29.)

## Vooraf vastgelegd (niet meer onderhandelbaar op de dag zelf)
- **Succesdrempel:** ≥2 positieve replies op 14 binnen ~7 werkdagen → door naar
  fase 2 (batch ~35 van de C-ready-pool). 0 replies → frame-gesprek, niet harder duwen.
- **Stopregels:** 2+ fouten in een 0b-steekproef → cohort dicht · spam-klacht of
  bounce-piek → alles pauze (Warmr bounce-handler + compliance-hold staan hierop) ·
  elke gevonden fout wordt een fixture.
- **Copy-freeze:** canary-copy v11, geborgd met hash-test
  (`test_canary_copy_frozen_v11`). Wijziging = bewuste ont-vriezing + nieuwe review.

## Checklist (afvinken bij arming)
- [ ] `logs/0b_checklist_canary.md` is v11 en toont 14/14 OK (gerenderd via het
      échte verzendpad — wat je leest ís wat er uitgaat)
- [ ] Sami's 0b-blik: 14 begroetingsregels gelezen, geen bezwaren
- [ ] Reply-notificaties AAN (n8n-workflow "Heatr → Slack" activeren — Sami's klik)
      — zonder dit landt een "ja" stil en gaat de 2-werkdagen-belofte stuk
- [ ] Ja-flow gereed: `scripts/prepare_concept_pack.py` getest (werkmap in 1 commando)
- [ ] Warmr: 2 inboxen ready, daily_campaign_target 12/inbox, venster 08–17 werkdagen
- [ ] Campagne staat als **draft** in Warmr (launch=draft is veilig; dispatcher
      verstuurt alleen 'active')
- [ ] Kill-switch/arming: Sami zet de campagne zelf op active — dit is en blijft
      de ene menselijke muur

## Na verzending (dag 1–7)
- Replies → Inbox/CRM (webhook zet crm_stage) + Slack; élke "ja" → direct
  concept-pack draaien (2-werkdagen-klok start bij hun reply)
- Dag 3: mail 3 (shrink-ask) rendert live via hetzelfde pad — mail 2 is voor de
  canary bewust overgeslagen (frame-coherentie; zie fase-2-experimentontwerp)
- Dag ~7: telling tegen de succesdrempel → besluit fase 2
