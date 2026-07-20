# Besluit — outreach-templates herschrijven naar de Fase A-strategie

Beslissing (2026-07-20) op basis van `docs/audit_outreach_content.md`. Dit stuk
legt vast **wat er verandert en waarom**; de rewrite is ready-to-apply maar wordt
pas in `config/sequence_templates.py` gezet op expliciete go (outward-facing copy
naar echte klinieken — verdient één menselijke leesronde, en de kill-switch staat
dicht dus er is geen haast).

## Het kernbesluit

De v3.1-templates (2026-05-07) implementeren de **oude** strategie: ze pitchen
Aerys en bieden een audit/Loom aan **in mail 1**, en mail 2/3 claimen een
opgenomen Loom/video die niet bestaat. Dat botst frontaal met de
Fase A-kernbeslissing (2026-07-11): **koude mail = één observatie + één vraag; het
aanbod/teardown komt pas ná een positieve reply (consent).**

**Besluit: de koude sequence wordt puur observatie-gedreven. Alle aanbod, Loom,
video en audit-pitch verdwijnen uit mail 1-3 en verhuizen naar de
post-positieve-reply-flow** (waar de teardown-pagina van migratie 029 thuishoort).
Dit lost in één klap vier bevindingen uit de audit op: de pitch-in-mail-1, de
niet-bestaande Loom-claims, de meervoudige vragen, en de strategiedrift.

## Gevolg: de drie bruggen collaberen in de koude fase

Zonder aanbod in mail 1 is er geen brug-specifieke pitch meer nodig. `pick_brug`
blijft bestaan, maar bepaalt voortaan alleen **welke observatie-hoek** je kiest en
**welk aanbod ná de reply** volgt — niet de koude copy. De koude mail wordt één
uniform frame met een per-lead observatie. Dat is simpeler én juister.

| | Oud (v3.1) | Nieuw (Fase A) |
|---|---|---|
| Mail 1 | observatie + pitch + audit/Loom-aanbod + 2-3 vragen | observatie + **één** open vraag, geen aanbod |
| Mail 2 | "ik heb een Loom opgenomen" (bestaat niet) | zachte nudge, tweede observatie-hoek, geen aanbod |
| Mail 3 | "persoonlijk videootje" (bestaat niet) + mini-audit | zachte afsluiter, geen claim, deur open |
| Aanbod/Loom/teardown | in de koude mails | **ná** positieve reply (aparte flow) |

## De herschreven copy

Observatie = de per-lead `{{opener}}` (de QA-gate Haiku-observatie), met
`{{signaal_blok}}` als deterministische fallback. Geen em-dashes, geen aanhef-gat.

**Onderwerp (alle mails):** `{{bedrijfsnaam}}, één ding dat me opviel`
*(em-dash uit het oude `— even kort` verwijderd; komma i.p.v. gedachtestreep.)*

**Mail 1 — observatie + vraag (dag 0)**
```
Hoi {{first_name}},

{{opener}}

Even los daarvan, uit nieuwsgierigheid: merken jullie dat zelf ook terug in
wie er binnenkomt via de site?

Groet,
Sami Jansema
Aerys Solution · aeryssolution.nl
```

**Mail 2 — zachte nudge, tweede hoek (dag 3, in de thread)**
```
Hoi {{first_name}},

Geen reactie op mijn vorige, helemaal goed. Ik bleef alleen nog even hangen op
{{signaal_blok_kort}} — dat is precies het soort ding waar een site vaak nog
niet op meebeweegt.

Nieuwsgierig of dat bij jullie speelt, meer niet. Zin om er kort over te
sparren?

Sami
```

**Mail 3 — zachte afsluiter (dag 5, in de thread)**
```
Hoi {{first_name}},

Laatste van mijn kant, beloofd. Als het nu niet uitkomt: helemaal prima, ik laat
'm hier.

Mocht je er later toch eens naar willen kijken, weet je me te vinden. Alle goeds
met {{bedrijfsnaam}}.

Sami
```

> **Wat NIET meer in de koude mail staat:** "Ik kan een Loom maken", "gratis AI
> Audit van 15 minuten", "mini-audit klaarliggen", "persoonlijk videootje". Die
> horen bij het gesprek ná een positieve reply. De teardown-pagina (029) is daar
> het bezorgmiddel, niet de koude mail.

## Bijbehorende mechanica-fixes (code, klein, apart van de copy)

1. **`{{opener}}` gaat renderen in mail 1.** Nu gebruikt mail 1 `signaal_blok`
   terwijl de 954 QA-gate openers ongebruikt meereizen. De koude mail wordt de
   plek waar de opener-pijplijn (incl. de harde launch-eis) daadwerkelijk landt.
   `signaal_blok` blijft de fallback als er geen opener is.
2. **Em-dash-afdwinging op de opener-output.** `validate_opener_sendable`
   (`utils/text_normalizer.py`) krijgt een em-dash-check: 86/954 openers (9%)
   bevatten er nu één terwijl de prompt ze verbiedt — de gate toetst het niet.
   Afgekeurde openers → regenereren (bestaande runner).
3. **Voornaam-fallback in het frame.** 29,4% mist `contact_first_name` → "Hoi
   daar," is zwak. Besluit: bij ontbrekende voornaam een frame zonder aanhef-gat
   (bv. openen met de observatie i.p.v. "Hoi daar,"). `display_first_name` levert
   nu "daar"; het frame wordt aangepast zodat het zonder naam natuurlijk leest.
4. **`{{signaal_blok_kort}}`** — mail 2 heeft een korte variant nodig; die kan uit
   de bestaande `signal_picker` met een compacte tier-tekst.

## Wat er ná dit besluit gebeurt

- **Op jouw go:** ik zet deze copy + mechanica in `config/sequence_templates.py`
  en `sequence_engine.py`, met tests (droog-render-assert: geen aanbod-woorden,
  geen em-dash, één vraag, geen onopgeloste tokens, geen Loom-claim).
- **De post-reply-flow** (aanbod + teardown-pagina) is een aparte bouwtaak; die
  hangt op de reply-classifier + migratie 029 (teardown-tabellen). Buiten deze
  rewrite.
- **De brug-routing-drempel** blijft geblokkeerd op de backfill-eindstaat
  (ongewijzigd door dit besluit — routing bepaalt nu alleen de observatie-hoek en
  het latere aanbod, niet de koude copy).

## Openstaand voor jou

- **Go/no-go** op deze rewrite-richting (dan wire ik 'm in met tests).
- De toon van mail 1's vraag is nu website-gericht ("wie er binnenkomt via de
  site"); voor de workflow-/ai_audit-hoek kan de vraag mee-varen met `pick_brug`.
  Zeg of je één uniforme vraag wilt of per-hoek een variant.
```
