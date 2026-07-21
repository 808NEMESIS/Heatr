# Drempel-herijking op de eindstaat (2026-07-21)

Backfill compleet: **953/962 gescoord** (9 dode/onbereikbare domeinen, 0,9% —
onder de 5%-gate). Zuivere eindstaat, niet score-sorteer-vertekend. Getallen op de
**genormaliseerde `total_score` (0-100)**, ICP-subset (cosmetiek + alt-med, n=838;
legacy makelaars/bouw apart, chiro n=0).

## De verdeling (genormaliseerd 0-100)

| | p10 | p25 | p50 | p75 | p90 | max |
|---|---|---|---|---|---|---|
| **alle (953)** | 29 | 37 | **45** | 53 | 59 | 74 |
| cosmetiek (419) | 33 | 40 | 47 | 53 | 60 | 74 |
| alt-med (419) | 27 | 37 | 45 | 53 | 59 | 70 |
| legacy (115) | 30 | 34 | 38 | 43 | 46 | 56 |

**Kern: de schaal is samengedrukt.** Mediaan **45**, en **niets scoort boven 74**.
De oude drempels (30/40/50) zijn gezet toen de mediaan ~37 was (pre-normalisatie).
Op de nieuwe schaal vangt "opportunity <50" nu **520 van 838 leads (62%)** — te veel
om nog "kans" te heten. Cosmetiek en alt-med liggen vrijwel gelijk (mediaan 47 vs 45).

## ⚠️ Visual-skip-artefact (raakt élke drempel)

67% van de leads (636/953) heeft **geen visuele laag** (Vision-skip). En die groep
scoort **systematisch ~5 punten hóger**:

| | met visual | zonder visual |
|---|---|---|
| cosmetiek mediaan | 44 | **49** |
| alt-med mediaan | 40 | **47** |

Dit is een **normalisatie-artefact, geen kwaliteitsverschil**: zonder visual
normaliseert de score over een kleinere noemer (de visuele 8/25 valt weg), en omdat
de visuele scores laag zijn (gem. 14,5/25 = 58%) trekt de visuele laag de mét-visual-
leads juist omlaag. Elke drempel die je nu kiest zit dus scheef zolang tweederde
geen visual heeft.

**Dekking kopen kost ~€5** (636 × €0,008) en haalt het artefact weg. Aanbeveling:
doen — €5 is verwaarloosbaar en het maakt de hele verdeling (en dus élke drempel)
zuiver. Kanttekening: de skippers zakken dan ~5 punten (de visuele laag telt mee),
en het raakt óók de interne classifier (gedeelde Vision-uitkomst).

## Opportunity-drempels — twee opties (banden op ICP n=838)

| | urgent | rebuild | opportunity/high | ≥ high |
|---|---|---|---|---|
| **huidig 30/40/50** | 89 | 220 | 520 | 318 |
| **A · absoluut (oude intentie → 39/49/59)** | 196 | 476 | 726 | 112 |
| **B · percentiel p25/p50/p75 → 39/46/53** | 196 | 390 | 599 | 239 |

- **A** houdt de oude *bedoeling* aan (urgent≈onderste 26%, opportunity≈top 11%),
  omgerekend naar de nieuwe verdeling.
- **B** legt de drempels op vaste percentielen — **overleeft de volgende
  schaalcorrectie** (de grens beweegt mee met het cohort), maar een lead kan van band
  wisselen zonder dat z'n site veranderde.

**Aanbeveling: B (percentiel).** De schaal is al één keer verschoven; percentiel-
verankerde drempels voorkomen dat je dit na elke re-score opnieuw moet doen.

## Routing-drempel (`pick_brug`) — belangrijke nuance

Simulatie over 838 ICP-leads: bij drempel 50 → 387 "website" / 451 "ai_audit" /
**0 workflow**. Maar in de **Fase A 2-brug-copy mappen website én ai_audit allebei
naar `conceptsite`** — de workflow-brug vuurt op aparte signalen (reviews≥30 +
behandelingen≥3 + locaties≥2), en `locaties≥2` is zeldzaam → workflow ≈ nooit.

**Gevolg: de website-score-drempel is voor Fase A vrijwel moot** — hij wisselt
website↔ai_audit die tóch beide conceptsite worden. De echte routing-vraag is niet
"welk getal", maar **"wil je de workflow-brug überhaupt, en zo ja, laat je de
locaties≥2-eis vallen zodat 'ie ooit vuurt?"**

## De 4 beslissingen (samengevat)

1. **Opportunity-drempels:** A (absoluut 39/49/59) of **B (percentiel, aanbevolen)**.
2. **Routing:** workflow-brug schrappen, of de signaal-eisen versoepelen zodat 'ie vuurt.
3. **Visual-skip:** **dekking kopen (~€5, aanbevolen)** of accepteren + artefact melden.
4. **Banden gelijktrekken:** classifier-banden, frontend-drempels (<30/<45/<70) en
   de nergens-afgedwongen `MIN_WEBSITE_SCORE_FOR_OPPORTUNITY` op één set trekken.

*Toepassen gebeurt pas ná je keuzes — dit document is de meting, niet de wijziging.*
