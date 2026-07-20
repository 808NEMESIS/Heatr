# Besluit — Outreach-sequence Fase A definitief (observatie + Founding Five)

Besluit 2026-07-20 (v2, vervangt v1 van dezelfde dag). Vervangt de v3.1-templates.
Ready-to-apply; wordt pas in `config/sequence_templates.py` gezet op expliciete go
(outward-facing copy naar echte klinieken, kill-switch staat dicht — geen haast).

**Kernprincipe:** koude mail = observatie + één vraag + Founding Five-aanbod.
Concept (conceptsite-brug) of analyse (workflow-brug) is **gratis**; de echte bouw
is de deal. Loom wordt genoemd als belofte, **nooit vooraf gestuurd** — pas gemaakt
ná een positieve reply. Geen em-dashes. Eén vraag per mail.

## Twee bruggen

`pick_brug` bepaalt welke set gebruikt wordt (bepaalt nu dus de **koude copy**,
niet enkel de observatie-hoek):

- **Conceptsite-brug** — zwakke/verouderde site. Aanbod = gratis conceptontwerp.
- **Workflow-brug** — goede site, winst zit in gemiste contacten. Aanbod = gratis
  inventarisatie van waar het weglekt. **Scope-grens: alleen beloven wat de
  huidige receptionist kan — gemiste bellers/appjes opvangen en terugbellen. Geen
  "AI regelt je hele receptie", want niet alle intents zijn af.**

---

## CONCEPTSITE-BRUG

### Mail 1 (dag 0)
```
Onderwerp: {{bedrijfsnaam}}, één ding dat me opviel

Hoi {{first_name}},

{{opener}}

Klinieken die dit wél strak hebben staan, winnen 'm nu van
de buurpraktijk nog voor de eerste telefoon. Puur omdat de
site meteen vertrouwen wekt.

Ik doe nu iets eenmaligs: voor de eerste vijf klinieken
maak ik kosteloos een concept voor hun nieuwe site. Zeg je
ja, dan laat ik je in een Loom, een persoonlijke
videoboodschap, precies zien wat ik voor {{bedrijfsnaam}}
in gedachten heb.

Het concept en die video zijn gratis, daar zit je nergens
aan vast. En spreekt het je aan, dan bouwen we 'm echt, voor
deze eerste vijf tegen een gereduceerd tarief in ruil voor
je verhaal als referentie. Maar dat is helemaal aan jou.

Zal ik er een voor {{bedrijfsnaam}} maken?

Groet,
Sami Jansema
Aerys Solution · aeryssolution.nl
```

### Mail 2 (dag 3, in de thread)
```
Hoi {{first_name}},

Uit enthousiasme was ik eigenlijk al begonnen. {{detail_2}}
liet me niet los, dus ik zat al wat te schetsen voor
{{bedrijfsnaam}}.

En ik zag iets in jouw regio: {{concurrent_signaal}}. Dat
is precies waar twijfelaars nu op afgaan. De mensen die
tussen jullie kiezen, landen op dit moment bij hen, elke
week opnieuw, zonder dat jij het merkt. En dat soort
voorsprong dijt vanzelf uit, want reviews en ranking
versterken zichzelf.

Zal ik dat concept afmaken en je in een korte video laten
zien hoe je dat terugpakt? Kost je niks, en spreekt het aan
dan bouwen we 'm echt.

Sami
```

### Mail 3 (dag 5, in de thread) — scherpste first-mover
```
Hoi {{first_name}},

Laatste van mijn kant voor nu, beloofd.

Waarom ik aandring: negen van de tien mensen die op je site
landen en twijfelen, zijn binnen een paar seconden weg naar
de volgende praktijk. Je ziet het nooit, want ze bellen je
niet om te zeggen dat ze afhaakten. De meeste klinieksites
laten dat gewoon gebeuren, en dat is precies je kans: een
site die die twijfelaar wél vasthoudt, heeft bijna niemand
in deze markt nu al.

Maar dat raam sluit. Elke maand stappen er praktijken over,
en wie straks pas begint, haalt in plaats van voorloopt.

Die vijf plekken tegen gereduceerd tarief zijn er zo niet
meer. Spreekt het je aan, dan is dit het moment.

Alle goeds met {{bedrijfsnaam}}.

Sami
```

---

## WORKFLOW-BRUG

### Mail 1 (dag 0)
```
Onderwerp: {{bedrijfsnaam}}, één ding dat me opviel

Hoi {{first_name}},

{{opener}}

De voorkant staat goed. Waar het bij de meeste praktijken
stilletjes weglekt, zit erachter: de beller buiten
kantoortijd die geen voicemail inspreekt, het appje dat
pas de volgende dag wordt gezien. Precies de patiënt die je
al bijna binnen had.

Ik doe nu iets eenmaligs: voor de eerste vijf klinieken
breng ik kosteloos in kaart hoeveel daar bij hen blijft
liggen. Zeg je ja, dan loop ik je in een Loom, een
persoonlijke videoboodschap, door wat ik zie en wat het je
waarschijnlijk kost.

Die analyse is gratis, daar zit je nergens aan vast. En wil
je die gemiste contacten daarna echt opvangen, dan zet ik
dat voor deze eerste vijf op tegen een gereduceerd tarief
in ruil voor je verhaal als referentie. Maar dat is helemaal
aan jou.

Zal ik er voor {{bedrijfsnaam}} eens naar kijken?

Groet,
Sami Jansema
Aerys Solution · aeryssolution.nl
```

### Mail 2 (dag 3, in de thread)
```
Hoi {{first_name}},

Uit enthousiasme was ik eigenlijk al gaan kijken.
{{detail_2}} liet me niet los, dus ik ben je opzet alvast
wat gaan doorlopen.

En ik zag iets in jouw regio: {{concurrent_signaal}}. Daar
zit 'm nu de winst: de praktijk die als eerste reageert,
houdt de twijfelaar. Elke gemiste beller buiten kantoortijd
is er nu eentje die ergens anders wél wordt teruggebeld,
zonder dat jij het terugziet in je agenda.

Zal ik in een korte video laten zien hoeveel er bij
{{bedrijfsnaam}} blijft liggen en hoe je dat opvangt? Kost
je niks, en wil je het echt oppakken dan zetten we 'm op.

Sami
```

### Mail 3 (dag 5, in de thread) — first-mover
```
Hoi {{first_name}},

Laatste van mijn kant voor nu, beloofd.

Waarom ik aandring: bijna geen enkele praktijk in deze markt
vangt die gemiste contacten nu al slim op. Wie er vroeg bij
is pakt de patiënten die de rest laat lopen. Over een jaar
doet iedereen het en is het gewoon bijhouden.

Maar dat raam sluit. Elke maand stappen er praktijken over,
en wie straks pas begint, haalt in plaats van voorloopt.

Die vijf plekken tegen gereduceerd tarief zijn er zo niet
meer. Spreekt het je aan, dan is dit het moment.

Alle goeds met {{bedrijfsnaam}}.

Sami
```

---

## Tokens

| Token | Bron | Status |
|---|---|---|
| `{{first_name}}` | enrichment | bestaat; voornaam-fallback nog bouwen (29,4% mist) |
| `{{bedrijfsnaam}}` | enrichment | bestaat |
| `{{opener}}` | QA-gate Haiku-observatie | bestaat; moet nog in mail 1 renderen |
| `{{signaal_blok}}` | signal_picker | fallback voor opener |
| `{{detail_2}}` | tweede observatie per lead | **bouwen** |
| `{{concurrent_signaal}}` | radius-pool + achterstand-selector | **bouwen (zie spec)** |

## Hormozi-lens (waarom elke mail werkt)

- **Mail 1** — dream outcome + risk-reversal (gratis, nergens aan vast) +
  Founding Five schaarste. Geen over-promise.
- **Mail 2** — cost-of-inaction via lokale concurrent-achterstand. De twijfelaar
  landt "nu al, elke week" bij de rivaal; verlies is onzichtbaar.
- **Mail 3** — first-mover arbitrage + actief-sluitend raam + two-futures-spiegel
  ("voorloopt vs. haalt in") + Founding Five deadline.

Coherentie mail 2↔3: één regio-rivaal is wakker (mail 2), de markt als geheel
slaapt nog (mail 3). Botst niet.

**Merkspanning:** drie mails lang stevig duwen zit aan de bovengrens van
premium/niet-pusherig. Bewust gekozen dosis. Bewaken bij verdere aanscherping.

---

## Bouwtaken (blokkeren live-zetten)

1. **`{{detail_2}}`** — tweede observatie per lead, anders dan mail 1's opener.
2. **`{{concurrent_signaal}}`** — in-house radius/achterstand-selector (spec onder).
3. **Plekken-teller (5 → 0)** — anders is de Founding Five-claim in mail 3 hol.
   Handmatig of via Heatr bijwerken zodra een concept/analyse vergeven is.
4. **`{{opener}}` in mail 1 renderen** + em-dash-gate op opener-output (bevestigd:
   86/954 = 9% bevat er nu een; `validate_opener_sendable` toetst het niet) +
   voornaam-fallback (frame zonder aanhef-gat).
5. **Droog-render-tests:** geen em-dash, één vraag, geen onopgeloste tokens, geen
   Loom-claim-als-bestaand, concept/website-scheiding intact.

---

## Heatr bouwspec — in-house radius/signaal-selector

**Doel:** per lead, binnen een instelbare straal, de grootste verdedigbare
achterstand t.o.v. regio-concurrenten vinden en als één anonieme zin in
`{{concurrent_signaal}}` gieten. Extern zeg je "in jouw regio"; intern onderbouwd.

> ### ⚠️ Geverifieerd 2026-07-20 — de premisse klopt NIET zonder extra werk
> De spec ging ervan uit dat laag 5 concurrent-**locatie** al scrapet. Dat is niet
> zo. `scrapers/google_maps_scraper.py` retourneert alleen `name / address / phone
> / rating / reviews` — **geen coördinaten**, niet extracted en niet opgeslagen.
> `competitor_analyzer.py` rekent alleen met rating/rank, geen afstand. Er zijn
> **geen lat/lng-kolommen** in `leads` of `companies_raw` (live gecheckt).
>
> **Gevolg:** coördinaten-capture is een **echte prerequisite**, niet gratis
> hergebruik. De goedkoopste route (nog steeds zonder geocoding-API): lat/lng
> extraheren uit de Google-Maps place-URL (`…/@52.37,4.89,17z…`) tijdens de scrape
> en opslaan op lead + concurrent. Pas dán is Haversine echt nul-API. Zonder deze
> stap kan de selector niet draaien; `{{concurrent_signaal}}` valt terug op null
> (mail 2 rendert zonder regio-zin).

**Script-logica (nieuw bestand, bv. `website_intelligence/regio_selector.py`), ná
de coördinaten-capture:**
```
1. Input: lead (lat, lng, scores, reviews, ranking, features)
          + concurrent-pool uit laag 5 (zelfde velden per concurrent)
2. Filter pool op Haversine-afstand <= RADIUS_KM (config per stad/sector)
3. Bereken per dimensie de achterstand van de lead:
     reviews-aantal · Google-ranking · site-snelheid/techniek (laag 1)
     · features (online afspreken, chatbot) — voor workflow-brug
4. Selecteer de grootste, verdedigbare kloof (drempel: alleen als kloof > X;
   anders null → geen verzonnen kloof forceren)
5. Render tot één anonieme NL-zin, brug-bewust:
     conceptsite: reviews/ranking/snelheid · workflow: reageersnelheid/features
6. Output: {{concurrent_signaal}} string, of null
```
**Config:** `RADIUS_KM` per stad/sector (grote stad ~15 km, landelijk ~30 km).
**Openstaand:** selector brug-bewust maken (aparte signaal-types per brug).
Aanbeveling: ja — houdt de claim specifiek.

---

## Afwijkingen t.o.v. besluit v1 (zelfde dag)

v1 zei "geen aanbod in mail 1-3, alles ná positieve reply". Losgelaten:
- Founding Five-aanbod (gratis concept/analyse + Loom-belofte) staat nu wél in de
  koude mails.
- `pick_brug` bepaalt nu de koude copy (twee volledige sets), niet enkel de
  observatie-hoek.
- Loom blijft belofte-ná-ja (nooit vooraf gestuurd) — ongewijzigd t.o.v. v1.

## Status randvoorwaarden

- **Tier 2 / Places** — live bewezen 2026-07-20 (Alpha Haarkliniek: 4.9 / 68
  reviews vs. scrape 49 → Places is de verse bron). Niet nodig voor deze sequence,
  maar relevant: de reviews-achterstand in `{{concurrent_signaal}}` kan later óók
  uit Places komen i.p.v. de stale scrape.
- **Brug-routing-drempel** — blijft geblokkeerd op de backfill-eindstaat.
- **Live-zetten** wacht op: go op deze richting + de 5 bouwtaken.
