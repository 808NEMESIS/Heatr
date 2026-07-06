# Warmr render-contract (bron van waarheid)

**Vastgesteld:** 2026-07-06 (Sprint 4 pre) · **Methode:** code-trace +
empirische probe tegen Warmr's echte render-pad · **Status:** definitief.

Dit document beantwoordt de vraag die de Model A→B-cutover (Sprint 3)
blokkeerde: **verstuurt Warmr `custom_subject`/`custom_body` letterlijk, of
alleen waar de frozen template een token bevat?**

---

## Conclusie: TOKEN-ONLY (passthrough), níet verbatim

Warmr verstuurt **nooit** `custom_subject`/`custom_body` als body op zichzelf.
De uitgaande body is **altijd** de frozen template uit `sequence_steps`,
gerenderd via spintax + variabele-substitutie. Heatr's `custom_*`-waarden
belanden in `lead.custom_fields` en zijn **uitsluitend** bereikbaar als de
template ze expliciet aanroept — en dan met de syntax **`{{custom:custom_body}}`**,
niet `{{custom_body}}`.

**Twee gevolgen voor de cutover:**
1. "Heatr rendert, Warmr verstuurt letterlijk" vereist **passthrough-templates**
   in Warmr: de sequence-step-body moet letterlijk `{{custom:custom_body}}` zijn
   (en subject `{{custom:custom_subject}}`). Er is geen verbatim-modus die de
   frozen template omzeilt.
2. **Heatr's huidige docstring is fout.** [warmr_client.py:193](../integrations/warmr_client.py#L193)
   claimt dat de velden beschikbaar zijn als `{{custom_subject}}`/`{{custom_body}}`.
   Die syntax is een ONBEKEND token voor Warmr → het lekt **letterlijk**
   `{{custom_body}}` de e-mail in naar de prospect. De juiste prefix is
   `{{custom:...}}`. (Corrigeren hoort bij de cutover-sprint — buiten scope hier.)

---

## Code-bewijs (het feitelijke verzendpad)

Warmr-repo: `/Users/nemesis/warmr`.

1. **Body-compositie op verzendmoment** —
   `campaign_scheduler.send_campaign_email` (regel 857-861):
   ```python
   raw_subject = step.get("subject") or ""     # uit sequence_steps (frozen)
   raw_body    = step.get("body") or ""        # uit sequence_steps (frozen)
   subject = process_content(raw_subject, lead, step_number=…, spintax_enabled=…)
   body    = process_content(raw_body, lead, step_number=…, spintax_enabled=…)
   ```
   `step` komt uit `load_sequence_steps` → tabel `sequence_steps` (bevroren bij
   campagne-creatie). **`custom_subject`/`custom_body` worden nergens in het
   verzendpad gelezen.** Grep over de hele Warmr-repo: 0 hits op
   `custom_subject`/`custom_body` — alleen `custom_fields`.

2. **Variabele-substitutie** — `spintax_engine.substitute_variables` →
   `_resolve_var` (regel 288-343):
   - `{{custom:KEY}}` → `lead["custom_fields"][KEY]`, lege string indien afwezig.
   - Onbekende tokens (bv. `{{custom_body}}`) → **blijven letterlijk staan**
     (`return f"{{{{{name}}}}}"`), bewust zichtbaar zodat template-bugs opvallen.
   - Er is geen `custom_body`/`custom_subject`-builtin.

3. **`process_content`** (spintax_engine.py:373) = `process_spintax` (geseed op
   lead_id + step_number — Warmr's spintax is óók deterministisch) gevolgd door
   `substitute_variables`. Niets leest `custom_*` als body-bron.

---

## Empirische bevestiging (A/B, tegen de echte render-code)

`process_content` is exact de functie die `send_campaign_email` aanroept om de
body samen te stellen. Uitgevoerd met een lead waarvan `custom_fields` de
Heatr-`custom_body` bevat (`DIT-IS-DE-HEATR-GERENDERDE-BODY`):

| Scenario | Template-body | Heatr-body in output? | `{{custom_body}}` lekt? |
|---|---|---|---|
| **A1** `{{custom_body}}` (Heatr's gedoc. syntax) | `…{{custom_body}}…` | **nee** | **ja — letterlijk in de mail** |
| **A2** `{{custom:custom_body}}` (Warmr's echte syntax) | `…{{custom:custom_body}}…` | **ja** | nee |
| **B** geen token, `custom_body` wél meegestuurd | `…vaste tekst…` | **nee** | nee |

Rendered output letterlijk:
- **A1** → `Hallo Lumen,\n\n{{custom_body}}\n\nGroet` (kapot token in de mail)
- **A2** → `Hallo Lumen,\n\nDIT-IS-DE-HEATR-GERENDERDE-BODY\n\nGroet`
- **B**  → `Hallo Lumen,\n\nVaste template-tekst, geen custom token.\n\nGroet`

Het verschil **A2 vs B** is het antwoord: de Heatr-body verschijnt **alleen**
als de template het token bevat → token-only. **B** bewijst dat een
meegestuurde `custom_body` zonder token spoorloos verdwijnt (geen verbatim).
**A1** legt de syntax-bug bloot.

> Methode-noot: geverifieerd door het echte render-pad (`process_content`) uit
> te voeren, niet via een black-box SMTP-send. Dat is de functie die bepaalt
> wát er in de body komt; downstream volgen alleen unsubscribe-footer,
> signature, HTML-wrapping en tracking — geen daarvan leest `custom_*`. Een
> over-the-wire testsend zou hetzelfde bevestigen zonder extra zekerheid over
> het contract. Reproduceerbaar via [docs/probes/warmr_render_probe.py](probes/warmr_render_probe.py)
> (vereist de Warmr-repo op `/Users/nemesis/warmr`; draai met Warmr's venv).

---

## Wat dit betekent voor de cutover-sprint

De juiste tak is **passthrough**, niet verbatim:

1. **Warmr-side:** de sequence-step-templates voor Heatr-gedreven campagnes
   worden `{{custom:custom_subject}}` / `{{custom:custom_body}}` (dumme
   passthrough-frame). Heatr levert de volledige gerenderde inhoud in
   `custom_fields`.
2. **Heatr-side:** de payload zet `custom_subject`/`custom_body` al in
   `custom_fields` ([warmr_client.py:208](../integrations/warmr_client.py#L208)) —
   dat klopt. Alleen de **docstring** (`{{custom_subject}}` → `{{custom:custom_subject}}`)
   moet gecorrigeerd, plus de eventuele default-templates.
3. **Determinisme sluit aan:** zowel Heatr's `render_step` (Sprint 3, I8) als
   Warmr's `process_spintax` seeden per lead+stap. Met passthrough draait Warmr
   geen spintax meer over al-gerenderde Heatr-tekst (die bevat geen spintax),
   dus geen dubbele resolutie.
4. **Model B blijft nodig:** passthrough alleen levert nog geen per-stap
   Heatr-controle — daarvoor moet er alsnog een writer komen die
   `sequence_steps`+`pending`+`next_send_at` op `lead_campaign_history` vult
   (zie sprint3_render_ownership_trace.md).
