# Heatr — Haakje-mapping voor Mail 1 (hyperpersonalisatie)

> Canonieke spec (aangeleverd door Sami, 2026-07-22). Vervangt de Claude-gegenereerde
> opener + `build_site_observatie` door een deterministische **signaal-ladder →
> vaste template-hook**. Reden: de oude "vlei-dan-kraak-af"-ingang voelde als een
> klap na een compliment; deze ingang toont één ongezien lek (Hormozi).

Eén scherp, feitelijk haakje per lead. Gebouwd op Hormozi-principes. Dit document
beschrijft wat de scraper meet, in welke volgorde signalen voorrang krijgen, welke
openingszin erbij hoort, en welke guardrails aan moeten staan zodat er nooit een
aantoonbaar onjuist haakje de deur uit gaat.

---

## De Hormozi-logica onder dit systeem

1. **Sell the outcome, not the mechanism.** Niet "een nieuwe site" of "beter
   design" — wel "de patiënt die je binnenhaalt en weer verliest". Elk haakje
   koppelt aan geldverlies/gemiste patiënt, nooit aan smaak.
2. **De opener is de lead magnet in het klein: onthul een probleem dat ze niet
   zagen.** Niet complimenteren (weet ze al), niet afkraken (aanval). Wél: één ding
   tonen dat lekt zonder dat zij het merkt. Nieuwsgierigheid, geen defensie.
3. **Shrink the ask.** Mail 1 vraagt niet om call/budget. Alleen: "Zal ik er een
   maken?" Drempel bijna nul.
4. **Bewijs dat het echt over háár gaat.** Een getal (48 reviews) is inwisselbaar.
   Een observatie die alleen klopt als je écht keek, bewijst alles. Hyperpersonalisatie
   = het haakje is niet te faken en niet inwisselbaar met een andere lead.

---

## Prioriteitsladder (welk haakje wint)

Check in deze volgorde, pak het **eerste** signaal dat vuurt. Bovenaan sterkst aan
geldverlies, onderaan esthetisch vangnet.

| # | Signaal | Meetmethode | Waarom hoog/laag |
|---|---------|-------------|------------------|
| 1 | Geen online boekoptie (alleen telefoon) | DOM: geen boekformulier/-widget, wel `tel:`-link | Hardste geldkoppeling: avond-boeker haakt af |
| 2 | Boek-CTA onder de vouw op mobiel / >2 taps tot formulier | DOM + viewport 390px, tap-telling | Onweerlegbaar, direct pijnlijk |
| 3 | Laadtijd >4s tot interactief op mobiel | Playwright: time-to-interactive, 4G-throttle | Concreet, koppelt aan afhaker |
| 4 | Tap-targets te klein/overlappend op mobiel | DOM: knopgrootte <44px of overlap | Meetbaar, mild |
| 5 | Verouderde footer (jaar ≥3 terug) of laatste blog ≥18 mnd | DOM: footer-jaar, blogdatum | Feit, zwakker signaal |
| 6 | Vision: één benoembaar gedateerd element | Claude Vision op mobiele screenshot | Vangnet, alleen als 1–5 niks geven |

**Routing bij niks:** vuurt geen van 1–5 én Vision `GEEN` (of score onder drempel),
dan is de site waarschijnlijk in orde → géén website-angle → route naar
**automatisering-angle**. Forceer nooit een zwak haakje op een goede site.

**Footer-drempel:** ≥3 jaar (strenger dan 2), anders vuurt het te vaak en verzwakt.

---

## Signaal → openingszin (2 varianten per signaal, random gekozen)

Elke zin: bewijst dat je keek (a) + legt de pijn bij de patiënt, niet bij haar smaak (b).
Velden: `{naam}` `{stad}` `{reviews}` `{sterren}` `{jaar}` `{vision_element}`.

### Signaal 1 — Geen online boekoptie
- **A:** Ik wilde op mijn telefoon even kijken hoe ik bij jullie een afspraak zou maken, en kwam alleen een telefoonnummer tegen. De meeste mensen bellen 's avonds niet meer — die willen op dát moment even boeken, en zijn anders zo weer weg.
- **B:** Op zoek naar een boekknop op jullie site kwam ik alleen een telefoonnummer tegen. Voor wie 's avonds op de bank zit te twijfelen is bellen net die drempel te veel, en dan gaat 'ie verder naar de volgende praktijk.

### Signaal 2 — CTA onder de vouw / diep weggestopt
- **A:** Ik wilde op mijn telefoon kijken hoe ik een afspraak zou maken, en moest een paar schermen scrollen voor ik een knop vond. Dat is precies het moment waarop iemand die twijfelt afhaakt.
- **B:** Op mijn telefoon kostte het me even zoeken voor ik doorhad waar ik kon boeken. Klein ding, maar het is net dat zoekmoment waarop een twijfelaar besluit het toch maar te laten.

### Signaal 3 — Trage laadtijd
- **A:** Ik opende jullie site op mijn telefoon en 'ie deed er een paar tellen over voor er iets stond. Klinkt klein, maar dat is genoeg voor iemand om terug te tikken naar de volgende praktijk.
- **B:** Toen ik jullie site op mobiel opende, keek ik een paar seconden naar een leeg scherm. In die paar seconden ben je de ongeduldige bezoeker al kwijt, en die zie je nooit terug in je cijfers.

### Signaal 4 — Tap-targets te klein/overlappend
- **A:** Op mijn telefoon zat ik een paar keer mis te tikken op de knoppen, ze staan wat dicht op elkaar. Zonde, want het is net dat kleine wat iemand doet afhaken bij het boeken.
- **B:** Ik merkte op mobiel dat de knoppen wat klein en dicht op elkaar staan — een paar keer raak ik de verkeerde. Onbenullig, tot iemand die snel een afspraak wil het net te veel gedoe vindt.

### Signaal 5 — Verouderde footer/blog
- **A:** Klein detail dat me opviel: onderaan de site staat nog {jaar}. Technisch niks mis mee, maar een bezoeker die twijfelt leest dat als "zijn ze hier nog actief?" — en dat is nooit de indruk die je wilt.
- **B:** Onderaan jullie site zag ik nog {jaar} staan. Jij weet dat je volop draait, maar een nieuwe bezoeker weet dat niet, en zo'n jaartal zaait net dat kleine twijfeltje op het verkeerde moment.

### Signaal 6 — Vision, één element
- **A:** Wat me op de site meteen opviel is {vision_element}. Op zich een detail, maar het is het soort ding dat iemand die twijfelt onbewust laat afhaken.
- **B:** Eén ding sprong er voor mij meteen uit op de site: {vision_element}. Kleinigheid, maar precies zoiets bepaalt in een paar seconden of een bezoeker je vertrouwt of doorklikt.

---

## De rest van Mail 1 (structuur blijft)

De openingszin vervangt de eerste twee zinnen. Daarna komen de review-getallen wél
terug — nu als onderbouwing van "zonde", niet als opening.

```
[haakje-zin, variant A of B]

Bij {reviews} reviews en een {sterren} is dat zonde: je krijgt ze binnen, en op
dat ene moment laat de site een deel weer los.

[bestaande aanbod-alinea — gratis concept + Loom, eerste vijf, gereduceerd tarief
in ruil voor referentie, nergens aan vast]

Zal ik er een voor {naam} maken?

Groet,
Sami Jansema
Aerys Solution · aeryssolution.nl
```

Ingang verschuift van "vleien-dan-afkraken" naar "één scherpe observatie die pijn
doet zonder aan te vallen". De shrink-the-ask-vraag onderaan blijft.

---

## De Vision-prompt (signaal 6)

```
Je krijgt een mobiele screenshot van de homepage van een kliniek. Noem het ÉNE
visuele element dat een bezoeker het eerst als verouderd of onprofessioneel zou
registreren. Antwoord met een korte, concrete zelfstandige omschrijving die in een
zin past (bijv. "een fotocarrousel die automatisch doorschuift", "stockfoto's van
modellen in plaats van de eigen praktijk", "tekst die over een drukke
achtergrondfoto valt"). Geen algemene oordelen als "gedateerd" of
"onprofessioneel". Als er niks noemenswaardigs opvalt, antwoord exact met: GEEN.

Geef daarnaast een zekerheidsscore 0–100 voor hoe sterk/opvallend het signaal is.

Formaat exact:
ELEMENT: <omschrijving of GEEN>
SCORE: <0-100>

Max 12 woorden voor het element.
```

---

## Guardrails (technisch — moeten aan)

**Guardrail 1 — False-positive-check op signaal 1 & 2** (grootste risico). Vóór het
signaal mag vuren: scroll volledige pagina (lazy-load), wacht op `networkidle`, sluit
cookie-/consent-banners weg, check pas dán op boekformulier/-widget/CTA-positie.
Bij twijfel: niet vuren, zak door naar volgende in de ladder. Liever een haakje
minder dan één keer aantoonbaar mis.

**Guardrail 2 — Vision-score-drempel.** Onder **60** telt het signaal als `GEEN` →
wegrouten naar automatisering-angle. Vision mag nooit forceren.

**Guardrail 3 — Human-in-the-loop op signaal 6.** `{vision_element}` gaat ongefilterd
de mailzin in → elke mail op signaal 6 landt in de review-stap vóór verzending.
Signaal 1–5 zijn feitelijk genoeg om (na guardrail 1) automatisch te mogen.

---

## Verzendlogica & personalisatie-enrichment (Heatr-regels)

### Deliverability — gaat vóór elke haakje-optimalisatie
- **Apart outreach-domein**, niet het hoofddomein (nu: `meet-aerys.nl`; brand `aeryssolution.nl` blijft schoon).
- **SPF, DKIM, DMARC** correct op dat domein vóór de eerste verzending.
- **Domein opwarmen**: laag beginnen, opbouwen (Warmr warmup-engine; bij de 5 niet relevant, wél bij opschalen).
- **Plain-text, geen tracking-pixel** — vastgelegd via `EMAIL_TRACKING_ENABLED=false`; niet per ongeluk aanzetten.

### Send-timing per signaal
Haakjes gaan over "ik keek 's avonds op mijn telefoon" → verzend in het **begin van de
avond** (past bij de zin én wanneer een eigenaar zelf mail leest). Vermijd diep weekend;
**dinsdag t/m donderdag vroege avond** als veilige default.

### Naam-enrichment (tilt personalisatie meer dan welke haakje-variant ook)
Waterfall vóór de "Hoi,"-fallback: 1) KvK-inschrijving (eigenaar/bestuurder) → 2)
"over ons"/team-pagina → 3) LinkedIn → 4) "Hoi,". Alleen gebruiken bij redelijke zekerheid
dat de naam bij déze praktijk hoort — verkeerde naam is erger dan geen naam (zie de
begroeting-confidence-gate).

### Dedup & suppressie (vóór opschalen verplicht)
- **Dedup** op KvK-nummer of genormaliseerd domein (niet de bedrijfsnaam-string) → één
  kliniek nooit twee keer in de flow.
- **Suppressie** op dezelfde sleutel: wie "nee" zei, uitschreef, of de volledige 3-mail-flow
  al doorliep zonder reactie, komt niet opnieuw in de founding-five-flow.

---

## Wat de eerste vijf leads wél en niet testen

Met 5 leads en 6 signalen vuurt elk signaal bij 0 of 1 lead → je kunt **niet**
concluderen welk haakje het beste werkt (ruis). Deze vijf testen of de *machine*
deugt: klopt elk haakje feitelijk (open elke site zelf vóór verzending), voelt de
aanhef als een mens, komt er beweging. Trek pas conclusies over signaal-sterkte
richting **40–50 leads**. Zet dat getal nu vast.
