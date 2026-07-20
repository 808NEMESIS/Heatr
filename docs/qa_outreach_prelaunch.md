# Pre-launch QA — outreach Fase A (read-only meting, 2026-07-20)

Gemeten tijdens het backfill-venster op de werkelijke cohort. Backfill-
onafhankelijk (hangt op lead-velden, niet op website-scores) → deze cijfers zijn
definitief, niet-tussenstand. Verstuurt niets; kill-switches dicht.

## 1. Funnel-trechter (van gescrapet tot verzendbaar)

| Stap | Aantal | % van leads |
|---|---|---|
| gescrapet (companies_raw) | 1092 | — |
| leads aangemaakt | 962 | 88% |
| verrijkt (enriched/score) | 962 | 100% |
| compliance-veilig | 616 | 64% |
| verzendbaar e-mailadres (valid-only) | 418 | 43% |
| score ≥ 55 | 217 | 23% |
| opener aanwezig | 216 | 22% |
| **niet in cooldown = VERZENDBAAR** | **216** | **22%** |

End-to-end: **216 verzendbare leads van 1092 gescrapet (20%)**. Grootste lekken:
compliance −346 (gdpr_safe / geblokkeerde legacy-sectoren), strict-valid-email
−198, score-gate −201. Cooldown kost niets (0 enrollments ooit).

## 2. Personalisatie-dekking mail 2 (over alle 216 launchbare leads)

| Mail 2-variant | Aantal | % |
|---|---|---|
| both (detail_2 + concurrent_signaal) | 84 | 39% |
| detail_2 only | 132 | 61% |
| concurrent only | 0 | 0% |
| **none (kale degradatie)** | **0** | **0%** |

- **100% krijgt minstens `detail_2`** (elke launchbare lead mist online-afspreken
  of WhatsApp → er is altijd een tweede eigen-site-observatie).
- **39% krijgt óók een concurrent-signaal** (een verdedigbare regio-achterstand
  binnen 15 km).
- **0 anomalieën** over de hele cohort: geen "waar jullie er 0 hebben", geen
  kapotte/negatieve zinnen, geen detail_2⇄concurrent-booking-overlap. De
  0-review-fix hield.

## 3. Routing-vondst (input voor fase B)

**216/216 launchbare leads routeren naar de conceptsite-brug; 0 naar workflow.**
De workflow-brug vereist reviews≥30 + behandelingen≥3 + locaties≥2 (≥70 punten,
en > de website-score) — locaties≥2 is zeldzaam voor solo-klinieken, dus de brug
vuurt in de praktijk vrijwel nooit. **De workflow-copy is de facto dood onder de
huidige routing.**

→ Beslissing voor de fase-B routing-herijking (één van de vier keuzevragen):
ófwel de workflow-criteria versoepelen (bv. locaties-eis laten vallen), ófwel
accepteren dat cosmetische klinieken standaard de conceptsite-brug krijgen en de
workflow-brug voorlopig niet inzetten. Niet nu beslist — feit vastgelegd.

## 4. Wat dit betekent voor de lancering

- De koude sequence is **verzendklaar qua content**: geen gaten, geen kapotte
  personalisatie, altijd minstens één eigen observatie + de Founding-Five-hook.
- 216 leads vormen de eerste-campagne-cohort (allemaal conceptsite).
- Nog niet gedaan (bewust): live-wiren in `/campaigns/launch` + de routing-
  drempel-herijking (fase B). De kill-switch blijft de bewuste rem.
