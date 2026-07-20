# Outreach Fase A — implementatie-document (blauwdruk)

Bij [`docs/besluit_template_herschrijf.md`](besluit_template_herschrijf.md) (v2).
Bevat: (1) mail 2-degradation voor beide bruggen, (2) de vijf bouwspecs in
uitvoerbare vorm. **Repo-werk gebeurt in Antigravity; dit is de blauwdruk.**

Businesskeuzes vastgelegd:
- **Plek "vergeven" = getekende deal** (niet verzonden concept). Meest integere
  drempel, vereist live-render van de teller (spec 3).
- **`{{detail_2}}` = uit bestaande website-analyse**, met laag-uitsluiting zodat
  het niet dubbelt met `{{opener}}` (spec 5b).

> ### ✅ Twee dingen live geverifieerd (2026-07-20) — verwerkt in spec 1 & 4
> - **Coördinaten bestaan al.** 200/200 leads (steekproef) hebben `@lat,lng` in
>   `leads.google_maps_url` (bv. `…/@52.3695925,4.7550965,12z/…`). → **Spec 4 heeft
>   voor de bestaande ~962 leads GEEN her-scrape nodig**: parse de coördinaten uit
>   de al-opgeslagen URL-kolom (pure backfill). De scraper-wijziging is alleen voor
>   tóékomstige leads. Zie spec 4.
> - **Echte gate-signatuur:** `validate_opener_sendable(text: str | None) ->
>   tuple[bool, str]` — geeft een tuple terug, niet `bool`. De em-dash-check moet
>   de tuple-vorm volgen (spec 1).

---

## 1. Mail 2 degradation — vier toestanden per brug

De engine kiest de variant op basis van beschikbare tokens. Doel: nooit een mail
met gaten, nooit een verzonnen claim, altijd persoonlijk-ogend.

### CONCEPTSITE-BRUG

**Beide tokens:**
```
Hoi {{first_name}},

Uit enthousiasme was ik eigenlijk al begonnen. {{detail_2}}
liet me niet los, dus ik zat al wat te schetsen voor
{{bedrijfsnaam}}.

En ik zag iets in jouw regio: {{concurrent_signaal}}. Dat
is precies waar twijfelaars nu op afgaan. De mensen die
tussen jullie kiezen, landen op dit moment bij hen, elke
week opnieuw, zonder dat jij het merkt.

Zal ik dat concept afmaken en je in een korte video laten
zien hoe je dat terugpakt? Kost je niks, en spreekt het aan
dan bouwen we 'm echt.

Sami
```

**Alleen detail_2:**
```
Hoi {{first_name}},

Uit enthousiasme was ik eigenlijk al begonnen. {{detail_2}}
liet me niet los, dus ik zat al wat te schetsen voor
{{bedrijfsnaam}}.

Dat soort dingen zie ik vaker, en het kost stilletjes de
bezoekers die twijfelen, precies degenen die je bijna al
binnen had.

Zal ik dat concept afmaken en je in een korte video laten
zien hoe je het terugpakt? Kost je niks, en spreekt het aan
dan bouwen we 'm echt.

Sami
```

**Alleen concurrent_signaal:**
```
Hoi {{first_name}},

Ik bleef nog even hangen bij {{bedrijfsnaam}}, dus ik ben
alvast wat gaan schetsen.

En ik zag iets in jouw regio: {{concurrent_signaal}}. Dat
is precies waar twijfelaars nu op afgaan. De mensen die
tussen jullie kiezen, landen op dit moment bij hen, zonder
dat jij het merkt.

Zal ik dat concept afmaken en je in een korte video laten
zien hoe je het terugpakt? Kost je niks, en spreekt het aan
dan bouwen we 'm echt.

Sami
```

**Geen van beide:**
```
Hoi {{first_name}},

Ik bleef nog even hangen bij {{bedrijfsnaam}}, dus ik ben
alvast wat gaan schetsen voor je nieuwe site.

Ik wil 'm alleen niet zomaar afmaken zonder dat je 't wilt.
Zal ik doorgaan en je in een korte video laten zien wat ik
in gedachten heb? Kost je niks, en spreekt het aan dan
bouwen we 'm echt.

Sami
```

### WORKFLOW-BRUG

**Beide tokens:**
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

**Alleen detail_2:**
```
Hoi {{first_name}},

Uit enthousiasme was ik eigenlijk al gaan kijken.
{{detail_2}} liet me niet los, dus ik ben je opzet alvast
wat gaan doorlopen.

Dat soort dingen kost stilletjes de bellers buiten
kantoortijd, precies de patiënten die je al bijna binnen
had.

Zal ik in een korte video laten zien hoeveel er bij
{{bedrijfsnaam}} blijft liggen en hoe je dat opvangt? Kost
je niks, en wil je het echt oppakken dan zetten we 'm op.

Sami
```

**Alleen concurrent_signaal:**
```
Hoi {{first_name}},

Ik bleef nog even hangen bij {{bedrijfsnaam}}, dus ik ben
je opzet alvast wat gaan doorlopen.

En ik zag iets in jouw regio: {{concurrent_signaal}}. Daar
zit 'm nu de winst: de praktijk die als eerste reageert,
houdt de twijfelaar. Elke gemiste beller is er nu eentje
die ergens anders wél wordt teruggebeld.

Zal ik laten zien hoeveel er bij {{bedrijfsnaam}} blijft
liggen en hoe je dat opvangt? Kost je niks, en wil je het
oppakken dan zetten we 'm op.

Sami
```

**Geen van beide:**
```
Hoi {{first_name}},

Ik bleef nog even hangen bij {{bedrijfsnaam}}, dus ik ben
je opzet alvast wat gaan doorlopen.

Wat me opvalt bij de meeste praktijken: de bellers en appjes
buiten kantoortijd blijven liggen, precies de patiënten die
je al bijna binnen had.

Zal ik in een korte video laten zien hoe je dat opvangt?
Kost je niks, en wil je het oppakken dan zetten we 'm op.

Sami
```

---

## 2. Bouwspecs (vijf taken, in uitvoervolgorde)

### Spec 1 — opener-render in mail 1 + em-dash-gate
**Bestanden:** `config/sequence_templates.py`, `utils/text_normalizer.py`

- Mail 1 rendert nu `{{signaal_blok}}`; wijzig naar `{{opener}}` met
  `{{signaal_blok}}` als fallback wanneer geen opener beschikbaar is.
- `validate_opener_sendable`: voeg em-dash-check toe. Bevestigd 86/954 (9%) bevat
  een em-dash terwijl de prompt ze verbiedt en de gate ze nu niet toetst.
  > **Let op — echte signatuur is `(text: str | None) -> tuple[bool, str]`.** De
  > check moet de tuple-vorm volgen, bv.:
  > ```python
  > if text and any(d in text for d in ("—", "–")):  # em-, en-dash
  >     return False, "em_dash"
  > ```
- Afgekeurde openers → bestaande regenerate-runner (`scripts/regenerate_openers.py`).

### Spec 2 — voornaam-fallback (frame zonder aanhef-gat)
**Bestand:** `config/sequence_templates.py`

- 29,4% mist `contact_first_name`. `display_first_name` levert nu "daar" → "Hoi
  daar," is zwak.
- Bij ontbrekende voornaam een frame-variant die opent op de observatie:
  - **Met naam:** `Hoi {{first_name}},\n\n{{opener}}`
  - **Zonder naam:** `Hoi,\n\n{{opener}}` — de opener draagt de personalisatie,
    geen "daar"-gat.

### Spec 3 — plekken-teller met live-render (KRITIEK voor eerlijke schaarste)
**Bestanden:** nieuwe tabel/kolom in Supabase, `sequence_engine.py`

- **Drempel = getekende deal** (niet een verzonden concept).
- **Live-render-eis:** de "vijf plekken"-claim (mail 3 + aanbod-frame mail 1) mag
  GEEN statische 5 zijn. Bij elke verzending leest de engine de actuele teller,
  **per niche** (cosmetisch/chiro apart — Founding Five geldt per niche).
  ```
  vrije_plekken = 5 - COUNT(getekende_deals WHERE niche = lead.niche)
  ```
- Render-logica mail 3:
  - `>= 2`: "die vijf plekken tegen gereduceerd tarief" (of het actuele getal)
  - `== 1`: "de laatste plek tegen gereduceerd tarief"
  - `== 0`: laat de Founding-Five-alinea VALLEN; mail 3 draait puur op first-mover-
    urgentie (staat los van de plekken).
- **Waarom kritiek:** zonder live-render lopen mails uit met "vijf plekken" terwijl
  er al getekend is → precies de holle claim die v3.1's fout was. Enige taak die de
  schaarste eerlijk houdt.

### Spec 4 — coördinaten-capture (prerequisite voor spec 5)
**Bestanden:** migratie (lat/lng-kolommen), backfill-script, `scrapers/google_maps_scraper.py`

- **Geverifieerd probleem:** scraper retourneert alleen name/address/phone/rating/
  reviews; geen coördinaten; geen lat/lng-kolommen.
- > **✅ Maar coördinaten bestaan al in bestaande data.** 200/200 leads hebben
  > `@lat,lng` in `leads.google_maps_url`. **Splits spec 4 daarom:**
  > 1. **Backfill (nu, geen her-scrape):** nieuwe `lat`/`lng`-kolommen + script dat
  >    parseert uit de al-opgeslagen `google_maps_url`:
  >    ```python
  >    import re
  >    m = re.search(r'/@(-?\d+\.\d+),(-?\d+\.\d+)', lead["google_maps_url"] or "")
  >    lat, lng = (float(m.group(1)), float(m.group(2))) if m else (None, None)
  >    ```
  > 2. **Toekomst:** dezelfde extractie in `google_maps_scraper.py` bij nieuwe
  >    scrapes, zodat lat/lng meteen gevuld worden.
  > Concurrent-records hebben (nog) geen opgeslagen URL/coördinaat → daar is de
  > scraper-capture wél de enige bron (zie spec 5 pool-opbouw).
- Zonder deze stap draait spec 5 niet en valt `{{concurrent_signaal}}` op null
  (mail 2 → degradation-variant).

### Spec 5 — concurrent-selector
**Bestand:** nieuw `website_intelligence/regio_selector.py`

**5a — Haversine + achterstand-selectie:**
```
1. Input: lead (lat, lng, reviews, ranking, laag1-score, features)
          + concurrent-pool (zelfde velden)
2. Filter pool op Haversine <= RADIUS_KM (config per stad/sector, ~15 stad / ~30 landelijk)
3. Per dimensie: achterstand lead vs. beste concurrent in pool
4. Selecteer grootste verdedigbare kloof; onder drempel X → return null
5. Render tot één anonieme NL-zin, brug-bewust
6. Output: string of null
```
```python
from math import radians, sin, cos, asin, sqrt
def haversine_km(lat1, lng1, lat2, lng2):
    dlat, dlng = radians(lat2-lat1), radians(lng2-lng1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlng/2)**2
    return 2*6371*asin(sqrt(a))
```

**5b — laag-uitsluiting (voorkomt dubbeling met opener):**
- `{{detail_2}}` plukt uit dezelfde website-analyse als `{{opener}}` → herhaalrisico.
- Registreer welke laag/dimensie de opener claimde; `detail_2` MOET een andere laag
  pakken (opener = laag 2 visueel → detail_2 = laag 1 techniek of laag 3 conversie).
- Test op echte leads: rendert mail 1 en mail 2 aantoonbaar verschillende
  bevindingen? Zo niet → mail 2 voelt herkauwd.

**Brug-bewuste signaal-types:**
- conceptsite: reviews-aantal · Google-ranking · site-snelheid
- workflow: reageersnelheid · bereikbaarheid · features (online afspreken/chatbot)

**Optioneel later:** reviews-achterstand uit Places (verse bron, bewezen 2026-07-20:
Alpha Haarkliniek 4.9/68 via Places vs. 49 in scrape) i.p.v. de stale scrape.

---

## 3. Render-tests (spec 1-5 afsluitend)

Droog-render-asserts per gegenereerde mail:
- geen em-dash / en-dash
- precies één vraag (één ask)
- geen onopgeloste `{{tokens}}`
- geen Loom-claim-als-bestaand ("ik heb opgenomen" mag NIET voorkomen)
- concept/website-scheiding intact (gratis = concept/analyse+Loom; betaald = bouw)
- degradation: mail 2 valide in alle vier token-combinaties
- plekken: mail 3 valide bij vrije_plekken 5/2/1/0

---

## Volgorde-afhankelijkheid

```
Spec 1 (opener-gate)       ─┐
Spec 2 (voornaam-fallback)  ├─ onafhankelijk, meteen → verzendbare basis
Spec 3 (plekken live-render)┘   (spec 3 = businesskritisch)

Spec 4 (coördinaten: backfill uit google_maps_url) ──→ Spec 5 (selector) → volledige personalisatie
```
Basis-sequence (spec 1-3) is verzendbaar met degradation op de regio-zin, terwijl
spec 4-5 parallel gebouwd worden. Niks hiervan hangt op de backfill.
