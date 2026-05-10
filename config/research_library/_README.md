# Heatr Research Library

Statische verzameling markdown-claims met YAML-frontmatter, bedoeld voor **Mail 2 value-add** in
Heatr's outreach-flow. De Sonnet-render-laag kiest een passende claim per prospect (op basis van
sector, archetype en patroon-tags) en weeft die als "concrete observatie + bron" in de mail.

Elke claim is een geverifieerde, citeerbare statistiek. Geen wishful-thinking-getallen. Geen
circulaire blog-citaties. De library is bewust klein en streng.

---

## Folder-structuur

```
config/research_library/
├── _README.md                                  # Deze file
├── _kandidaten/                                 # Pre-verificatie buffer
│   └── .gitkeep
├── _afgewezen/                                  # Gedocumenteerde rejects (per sector)
│   └── zorg-cosmetisch_cliniek.md
├── zorg/                                        # Sector: zorg
│   ├── cosmetisch_cliniek.md                    # LIVE claims voor cosmetische clinieken
│   ├── tandarts.md                              # Placeholder
│   └── algemeen.md                              # Placeholder
├── zakelijke_dienstverlening/                   # Sector: zakelijke dienstverlening
│   └── algemeen.md                              # Placeholder
└── techniek_ambacht/                            # Sector: techniek & ambacht
    └── algemeen.md                              # Placeholder
```

---

## Schema-spec (YAML-frontmatter per claim)

Elke claim is één markdown-block in een sector-file met deze frontmatter:

```markdown
---
id: <unique-slug>
sectoren_specifiek: []          # leeg = niet sector-gebonden; gevuld = ALLEEN voor deze sectoren
sectoren_breed: [list]          # voor welke sectoren matching-laag mag kiezen
patroon: [list]                 # mail_2_value_add, reactietijd, reviews, etc.
geo_specifiek: <nl|us|eu|null>  # optioneel; null = wereldwijd
stat_kort: |
  Korte mail-bruikbare formulering. Maximaal 2-3 zinnen.
attributie_kort: "<bron + jaar>"
apa7_volledig: |
  Volledige APA7-citatie.
sample: "<beschrijving sample-grootte + datum>"
methodologie: "<korte uitleg methodologie + uitvoerder>"
bron_url: <https://...>
last_verified: 2026-05-09
ai_round_1: PASS
ai_round_2: PASS
caveat: "<wat de matching-laag/Sonnet moet weten over zwakke punten>"
status: LIVE                    # LIVE | KANDIDAAT | AFGEWEZEN
relevantie_zorg_cosmetisch_cliniek: |
  Optionele block per sector waarin Sonnet aanvullende framing kan vinden.
---
```

---

## Workflow voor nieuwe claims

1. **Pre-verificatie** — nieuwe claims komen eerst in `_kandidaten/` als markdown-file met
   `status: KANDIDAAT`. Frontmatter zo volledig mogelijk ingevuld; `last_verified`, `ai_round_1`,
   `ai_round_2` blijven leeg tot verificatie.

2. **AI-verificatie tweetraps:**
   - **Round 1 — bron-existence + plausibiliteit.** Bestaat de bron? Bestaat de auteur/uitgever?
     Klopt het jaar? Past het claim-bereik bij wat dit type onderzoek typisch oplevert?
   - **Round 2 — claim-tracing in bron via URL-fetch.** Is de exacte stat letterlijk in de bron
     terug te vinden, op de aangegeven URL, met de aangegeven sample en methodologie?
   - **Disagreement-protocol:** bij conflict tussen Round 1 en Round 2 wint **FAIL altijd**.

3. **Bij PASS** (beide rounds):
   - Verplaats claim naar productie-file (`<sector>/<sub>.md`)
   - `status: LIVE`
   - `last_verified: <datum-vandaag>`
   - `ai_round_1: PASS`, `ai_round_2: PASS`

4. **Bij FAIL** (één van beide):
   - Verplaats naar `_afgewezen/<sector-slug>.md`
   - `status: AFGEWEZEN`
   - Voeg `afgewezen_reden`, `afgewezen_op`, `afgewezen_in_categorie` toe

---

## Onderhoud

Halfjaarlijks `last_verified` opnieuw checken via `_verify.py` (komt in een latere prompt).
Verlopen entries (>12 maanden zonder herverificatie) krijgen een waarschuwing in de Sonnet-render-laag.

---

## Hard criteria voor LIVE-status

- **Recent**: ≥2022 publicatie of update.
- **Sample-transparantie**: sample-grootte expliciet gerapporteerd in bron.
- **Methodologie**: publiek beschreven (geen "uit eigen onderzoek" zonder details).
- **Direct citeerbaar**: primary source bereikbaar — geen circulaire blog-citatie zonder
  onderliggende studie.
- **Sector-correctheid**: geen e-commerce-stats voor service-business; geen US-only data
  zonder NL/EU-cross-check als de claim NL-specifiek wordt ingezet.
- **Disagreement-protocol**: bij conflict tussen Round 1 en Round 2 wint FAIL altijd.
- **Paywalled bronnen**: automatic FAIL (kan niet door Sonnet of Sami gecheckt worden).

---

## Status van library

**Status van library:** Prompt 2 van 5 uitgevoerd. Categorieën gevuld: Reactietijd (3), Reviews + Social Proof (5). Komende: AI-adoptie, Slechte website-kosten, Automatisering. Totaal LIVE: 8 claims.
