---
name: heatr-copy
description: Use this skill whenever generating, rewriting, or reviewing outreach copy in Heatr — cold emails, follow-ups, subject lines, openers, review emails, or any text that will be sent to a prospect. Triggers on: "schrijf een mail", "genereer opener", "review email", "email template", "onderwerpregel", "sequentie", "follow-up", "campagne copy", "personalisatie", of wanneer er wordt gewerkt aan campaigns/review_email_generator.py, campaigns/sequence_builder.py, of enige prompt die e-mailtekst produceert. Gebruik deze skill ALTIJD naast de heatr skill wanneer de output tekst is die een mens gaat lezen in zijn inbox.
---

# Heatr Copy Skill

Dit is de redactienorm voor alles wat Heatr naar een prospect stuurt. De `heatr` skill beschrijft hoe het systeem werkt; deze beschrijft wat er in de mail mag staan.

**Uitgangspunt:** Heatr genereert copy op schaal. Copy op schaal drift altijd richting generiek, want generiek is wat een taalmodel makkelijk vindt. Deze skill bestaat om die drift meetbaar te maken in plaats van er op te hopen.

---

## Regel 0 — Claimdekking (blokkerend)

**Elke feitelijke bewering in een mail moet herleidbaar zijn naar een veld in de database.**

Voor elke zin die iets beweert over de lead, moet je kunnen aanwijzen: dit komt uit `website_score`, uit `visual_analysis`, uit de reviewsamenvatting, uit `conversion_checks`. Kun je dat niet, dan gaat de zin eruit. Niet afzwakken — eruit.

Verboden zonder meting:
- Getallen die niet gemeten zijn ("zeven scrolls", "40% haakt af", "twee seconden laadtijd")
- Vergelijkingen met concurrenten die niet daadwerkelijk geanalyseerd zijn
- Uitspraken over hun bezoekers, agenda, omzet of aanvragen — die data heb je nooit
- Uitspraken over wat "klinieken die het wél goed doen" bereiken

Als een frame een meting nodig heeft die niet bestaat, is dat een bouwopdracht, geen schrijfopdracht. Meld het als ontbrekende meting en gebruik een frame dat wél gedekt is.

**Bij twijfel of een veld de bewering echt draagt: markeer als `onbepaald` en gebruik het niet.** Gokken is hier duurder dan een saaiere mail.

### Regel 0a — Detectorclaims zijn geen metingen

Een negatieve detectoruitkomst (`has_online_booking=False`, `phone_not_clickable`, en dergelijke) betekent **"niet gevonden", niet "niet aanwezig".** Die twee zien er in de database identiek uit. Een negatieve uitkomst mag alleen een claim dragen als de detector **aantoonbaar dekt wat hij zoekt** — dus als er een test bestaat die bewijst dat hij dit patroon vindt wanneer het er is. (Ontstaan 2026-08-12: `has_online_booking=False` was backlog-breed onbetrouwbaar omdat de detector NL-boekpatronen als Crossuite en self-hosted boekpagina's miste — vier van vijf cohort-1-leads hadden aantoonbaar wél een boekoptie.)

### Regel 0b — Steekproef vóór elke verzending

Ongeacht cohortgrootte: **minimaal vijf leads handmatig op mobiel controleren op de exacte claim die in hun mail staat.** Bij één fout: die lead eruit en oorzaak melden. Bij twee of meer: **cohort dicht, geen verzending, oorzaak eerst uitzoeken.** Deze regel geldt óók als alle geautomatiseerde gates groen staan — vier-van-vijf-fout is precies zo ontstaan.

---

## Regel 1 — Het probleem is frictie, geen smaak

Een probleemstelling is bruikbaar als de ontvanger 'm zelf kan verifiëren op zijn eigen telefoon, binnen dertig seconden, zonder jouw mening nodig te hebben.

**Bruikbaar (frictie — meetbaar, niet-beledigend):**
- Afspraakknop staat onder de vouw / ontbreekt op mobiel
- Telefoonnummer niet klikbaar
- Contactformulier met meer dan vijf velden
- Geen openingstijden vindbaar
- Overlappende of onleesbare elementen op mobiel
- Site laadt traag op 4G

**Onbruikbaar (smaak — subjectief, en vaak een keuze die zij zelf betaald hebben):**
- "ziet er uit als 2010"
- "generieke stockfoto's"
- "tekeningen in plaats van foto's"
- "weinig witruimte"
- "geen actuele designelementen"
- "tekstgroottes zijn klein" (tenzij gekoppeld aan leesbaarheidsfalen, niet aan esthetiek)

Regel: smaakoordelen beschuldigen de ontvanger van slechte smaak. Frictieoordelen beschrijven iets dat niemand bewust heeft ontworpen. Dat tweede kun je zeggen zonder ego-schade.

---

## Regel 2 — De vier waardevariabelen moeten alle vier geraakt worden

Een mail die er twee overslaat, laat de ontvanger die zelf invullen — en hij vult ze pessimistisch in.

| Variabele | Moet in de mail staan als |
|---|---|
| Uitkomst | Wat er anders wordt, niet wat je levert. Niet "een nieuwe site" maar "het pad naar de afspraak is één tik" |
| Geloof | Waarom dit gaat werken. Bij nul cases: het gratis concept ís het bewijs — benoem dat het vooraf komt |
| Tijd | Concrete doorlooptijd. "Drie weken van akkoord tot live" |
| Inspanning | Wat jij van hén nodig hebt. "Eén gesprek van een half uur en je bestaande foto's" |

Tijd en inspanning zijn de goedkoopste winst en worden het vaakst vergeten.

---

## Regel 3 — Blacklist

Deze zinnen en patronen mogen niet in gegenereerde copy voorkomen. Ze zijn allemaal een keer echt verstuurd en allemaal dood.

**Letterlijk verboden:**
- "wat steeds terugkomt is..."
- "de basis zit goed"
- "nergens aan vast"
- "Klinieken die dat wél strak hebben, winnen 'm van de buurpraktijk"
- "iets eenmaligs"
- "wat ik in gedachten heb"
- "ik hoop dat deze mail je goed bereikt"
- "even sparren"
- "vrijblijvend"

**Patronen verboden:**
- Compliment gevolgd door "maar" of een probleem. Dat is de standaard agency-opening en de lezer herkent 'm in één regel. Een compliment mag alleen als het inhoudelijk werk doet in het argument (zie frame B).
- Concurrentiedreiging in abstracte vorm ("anderen doen dit wel"). Alleen toegestaan met een genoemde, daadwerkelijk geanalyseerde concurrent.
- Superlatieven over het eigen aanbod: strak, modern, professioneel, state of the art, op maat.
- Meer dan één vraagteken in de hele mail.

---

## Regel 4 — Onderwerpregel

Saai wint van slim bij koud zakelijk verkeer. Slim ziet eruit als een campagne.

Toegestane patronen:
- `[Naam] op mobiel`
- `[domein] klopt niet met je reviews`
- `gratis ontwerp voor [Naam]`

Niet toegestaan: uitroeptekens, emoji, alles wat een claim doet die de body niet waarmaakt, "belangrijk", "actie vereist", vragen die nieuwsgierigheid forceren zonder inhoud.

Maximaal zes woorden.

---

## Regel 5 — Sequentie

Elke follow-up moet nieuwe waarde brengen. Een bump zonder inhoud verbrandt het recht om nog een keer te vragen.

- **Mail 1** — het frame (zie frames hieronder). Volledige ask.
- **Mail 2** — bewijs, geen herhaling. Eén concrete vondst uit de audit van hún site. Zelfde ask, ongewijzigd.
- **Mail 3** — krimp de ask. Niet het traject opnieuw aanbieden maar de drempel weghalen: "Ik hoef geen ja. Zeg 'stuur maar' en hij staat er, dan kijk je wanneer je wil."

Verboden in mail 2 en 3: "even een reminder", "wellicht gemist", "bovenop je inbox".

---

## Regel 6 — Schaarste met reason why

Schaarste zonder reden wordt doorgeprikt en kost vertrouwen. De geldige reden is capaciteit: vijf tegelijk omdat ze handmatig gebouwd worden. Die reden is waar en niet controleerbaar te weerleggen.

**De teller mag pas gerenderd worden vanaf twee vergeven plekken.** Bij nul getekende deals leest "5 van 5 beschikbaar" als: niemand wilde het. Onder de drempel: alleen capaciteitsframing, geen getal.

Nooit: een deadline verzinnen, "laatste kans", een teller die maandelijks reset.

---

## Regel 7 — Niches strikt gescheiden

| Niche | Frame | Aanbod in mail 1 |
|---|---|---|
| Cosmetische klinieken | Frictie of reviewgat. Draait om nieuwe aanvragen uit bestaand verkeer | Gratis conceptsite |
| Alternatieve zorg / chiro | Uitleg en verwachting. Draait om minder telefonische uitleg en betere no-shows, niet om groei | Gratis conceptsite |

Nooit een groei- of concurrentieframe naar alternatieve zorg. Die praktijken zitten vaak vol; het frame ketst af en beschadigt de afzender.

Eén gratis toegangspoort per niche. Als de conceptsite in de mail staat, komt de Praktijk Check-up er niet ook in.

---

## Regel 8 — Loom nooit koud

Vast, geldt overal:
- Loom wordt pas gemaakt en gestuurd na een positieve reply. Nooit als bijlage of link in mail 1.
- Gratis = concept plus Loom (idee getoond). Betaald = daadwerkelijke bouw.
- De Loom is een reveal, geen presentatie: hun huidige mobiele pad naast het nieuwe. Niet "wat ik in gedachten heb".

---

## Framebibliotheek

Vier frames. Kies er één per mail; nooit mengen.

**A — Frictie-reveal.** Observatie die zij zelf checken → consequentie in verkeer dat ze al betaald hebben → oplossing in één zin. Vereist: minimaal één harde frictievondst uit `conversion_checks`.

**B — Reviewgat.** Wat hun patiënten roemen staat niet op de site. Vertaalprobleem, geen designprobleem. Vereist: reviewsamenvatting met een consistent thema én een visuele analyse die aantoont dat dat thema ontbreekt.

**C — Kale ask.** Geen diagnose. Alleen aanbod, capaciteitsreden, risicoverwijdering. Vereist: niets. Dit is de controlevariant — als A en B het niet beter doen dan C, verzet de probleemstelling geen werk.

**D — Uitlegfrictie (alternatieve zorg).** Uitleg die zij nu telefonisch doen staat niet op de site. Vereist: reviewsamenvatting waarin uitleg/tijd nemen terugkomt.

---

## Zelfcontrole vóór verzenden

Loop deze lijst af per gegenereerde mail. Elk punt is pass/fail, geen oordeel.

1. Staat er een getal of bewering die niet uit een databaseveld komt? → **fail**
2. Kan de ontvanger de observatie binnen dertig seconden op zijn eigen telefoon verifiëren? → nee = **fail**
3. Is het probleem smaak in plaats van frictie? → **fail**
4. Staan alle vier waardevariabelen erin (uitkomst, geloof, tijd, inspanning)? → nee = **fail**
5. Komt er een zin of patroon uit de blacklist in voor? → **fail**
6. Meer dan 160 woorden in de body? → **fail**
7. Meer dan zeven keer "ik"? → **fail**
8. Zou deze zin standhouden bij iemand die zijn eigen cijfers kent? Per bewering. → nee = **fail**
9. Klopt het frame bij de niche van deze lead? → nee = **fail**
10. Wordt de Loom aangeboden vóór een reply? → **fail**

Rapporteer bij generatie op schaal het aantal fails per categorie. Een categorie die structureel faalt is een prompt- of dataprobleem, geen incident.

---

## Testdiscipline

Bij cohorts onder ~150 leads per variant: **geen A/B-test.** Stuur één variant en leer kwalitatief uit de replies. Bij 25 leads en een realistische positieve replyrate van 4–8% zijn dat één tot twee reacties — daar is geen conclusie uit te trekken.

Statistisch testen begint bij ongeveer 150–200 per variant, en dan op één variabele tegelijk. Onderwerpregel en body niet gelijktijdig wisselen.

Leg vóór verzending vast wat succes is. Zonder vooraf vastgelegde drempel wordt elk resultaat achteraf goedgepraat.
