# Audit: outreach-content — leest het zoals bedoeld?

Read-only (2026-07-18). Droog gerenderd met `render_step` (puur, deterministisch,
seed per lead+stap) op echte leads uit workspace aerys — **niets verstuurd, geen
dispatch-functie aangeraakt**. Strategie-referentie: de kernregels voor mail 1
(één feitelijke observatie, geen score/lijst/AVG/prijs, concreet getal, geen
oordeel), de Fase A-kernbeslissing van 2026-07-11 (koude mail = 1 observatie +
vraag; teardown/aanbod pas NA positieve reply) en het Loom-first-ontwerp.

**Overkoepelende vaststelling vooraf:** de v3.1-templates dateren van 2026-05-07
— **vóór** de Fase A-kernbeslissing van 2026-07-11. Ze implementeren de óúde
strategie (pitch + aanbod in mail 1) en zijn na die beslissing nooit herzien.
Veel van de drift hieronder is daarop terug te voeren.

---

## 1. Mail 1 tegen de strategie

Gerenderd voor echte leads per brug (steekproef 40 leads: 28 → ai_audit, 12 →
website, 0 → workflow¹). Voorbeeld (ai_audit, geanonimiseerd op contactnaam):

> **Onderwerp:** Alpha Haarkliniek — even kort
> Hoi daar,
> Ik keek naar ondernemers in Amsterdam met lokale impact en het viel me op dat
> Alpha Haarkliniek patiënten goed verder helpt. **49 reviews met een
> 4.9-rating.** Sterk gebouwd. […] Bij Aerys doen we daar een **gratis AI Audit**
> voor van 15 minuten. Of […] een korte Loom […] **Wat past beter, audit of Loom?**

Per regel getoetst:

| Regel | ai_audit | website | Oordeel |
|---|---|---|---|
| Eén feitelijke observatie met getal | ✅ via `{{signaal_blok}}` ("49 reviews met een 4.9-rating") | ✅ idem | conform |
| Geen score/lijst/AVG/prijs | ✅ alle vier schoon (regex-gecheckt op alle renders) | ✅ | conform |
| Geen oordeel / geen ongetoetste claim | ✅ | ❌ **"jullie homepage hangt *waarschijnlijk* niet helemaal in lijn"** — een ongetoetste website-aanname, verstuurd aan élke website-brug-lead. Nota bene: de opener-QA-gate (`validate_opener_sendable`) verbiedt exact dit ("ongetoetste-website-claim") in openers — het template zelf doet wat de gate verbiedt | **drift** |
| Eén vraag | ❌ 2 vragen | ❌ **3 vragen** | drift |
| Geen pitch vóór reply (beslissing 2026-07-11) | ❌ Aerys-pitch + audit/Loom-aanbod in mail 1 | ❌ idem | **drift** (templates ouder dan de beslissing) |
| Geen em-dash | ❌ **onderwerpregel bevat een em-dash** (`"{{bedrijfsnaam}} — even kort"`, sequence_templates `subj_base`) — in élke mail, alle bruggen | ❌ idem | **drift** |
| Aanhef | "Hoi daar," bij ontbrekende voornaam — **29,4% van de leads heeft geen `contact_first_name`** → bijna 1 op 3 koude mails opent onpersoonlijk | | zwak |

¹ *Workflow-brug: 0 in de steekproef — de reviews≥30/behandelingen≥3/locaties≥2-signalen halen zelden samen 70. De workflow-teksten zijn dus vrijwel dode copy bij de huidige data.*

**Belangrijkste structurele vondst:** mail 1 v3.1 rendert **niet** `{{opener}}`
maar `{{signaal_blok}}` (utils/signal_picker, 6-tier keten). De 954 opgeslagen,
QA-gated Haiku-openers (99,2% vulgraad) worden in het productie-pad **nergens
gerenderd** — ze reizen alleen als Warmr-custom-field mee. De opener-pijplijn
(generatie + QA + regeneratie-run + de harde launch-eis uit de reparatie) borgt
dus een veld dat de mails niet gebruiken. De observatie-rol is overgenomen door
signaal_blok — inhoudelijk overlappend (zelfde review-observatie), maar zonder
de Haiku-personalisatie én zonder de opener-QA-gate.

## 2. De opener-kwaliteit (gelezen)

15 gelezen, 954 geteld. Kwaliteit is overwegend **goed**: feitelijk, concreet
getal, geen "mooie site!"-complimenten. Voorbeelden:

> "Jullie staan op Google met 45 reviews en een 4.9 gemiddelde. Dat aantal
> vertrouwen bouw je niet op met standaardbehandelingen."

Afwijkingen:
- **Em-dash: 86/954 (9%)** bevatten een em-dash, terwijl de prompt 'm expliciet
  verbiedt ("Em-dashes (—) — gebruik komma of punt"). De QA-gate
  (`validate_opener_sendable`) **checkt er niet op** — de regel staat in de
  prompt, wordt niet afgedwongen op de output.
- Enkele vervallen in felicitatie/vleierij: *"Gefeliciteerd, Kirsten — de
  4,8-sterrenrating … spreekt volumes"* (em-dash én vleierij én anglicisme).
- Sommige eindigen met een vraag — zou stapelen met de template-vragen, maar
  omdat de opener niet gerenderd wordt is dat nu theoretisch.

## 3. De brug-routing-teksten

- **website-brug:** bevestigd — de tekst biedt een **Loom met 1-2 aanpassingen**,
  géén "gratis concept-website" zoals het ontwerp beloofde. Zelfde drift als de
  inventarisatie vond, nu in de gerenderde tekst gezien.
- **workflow-brug:** leest passend voor het bedoelde lead-type (druk, veel
  reviews/locaties): "Vragen die het team de honderdste keer beantwoordt…" —
  maar is bij de huidige data vrijwel onbereikbaar (0/40 in de steekproef).
- **ai_audit-brug (default):** leest het meest in lijn met de strategie: echte
  observatie, redelijk droge toon; "Sterk gebouwd." als fragmentzin werkt bij
  een goed signaal, maar met de tier-6-fallback ("het werk dat jullie leveren.
  Sterk gebouwd.") wordt 'ie hol.
- Extra drift: "gratis AI Audit van **15 minuten**" (ai_audit mail 1) — het
  interne aanbod is een Praktijk Check-up/audit; nergens prijs (conform), maar
  de 15-minuten-claim is een belofte die het ontwerp niet noemt.

## 4. Mail 2 en 3 in de thread

Threading klopt: "Re: {onderwerp}", zelfde thread, prijs-vrij (regex-schoon).
Ze bouwen voort (niet herhalen). **Maar de kern is kapot:**

> Mail 2 (gerenderd, zonder loom_link): *"…ik bleef er even over nadenken voor
> {bedrijf} en **heb een Loom van 3 minuten opgenomen**. Specifiek, niet
> generiek…"* — gevolgd door **niets**. Daarna: *"Wat denk je? Zinvol, of zit
> ik mis?"*

Het conditionele weglaten (reparatie punt 4) werkt technisch — er rendert géén
kale regel en geen `{{LOOM_LINK}}`-token. Maar de **copy claimt een opgenomen
Loom die niet bestaat**, en mail 3 idem ("Ik nam toch een persoonlijk videootje
op") zonder link. De mails 2 en 3 zijn *om de Loom heen geschreven*; zonder
Loom-mechanisme zijn ze niet leeg maar **onwaar**. Dit is per constructie zo in
alle drie de bruggen (6 van de 9 mails).

## 5. De lege-opener-rand

Bevestigd: `personalized_opener` zit in `HARD_REQUIRED_FIELDS`;
`filter_launchable_leads` blokkeert (unit-getest, 674 groen) en
`launch_readiness` geeft "blocked". Een lead zonder opener valt uit het cohort
vóór het renderen. Kanttekening uit §1: de eis borgt een veld dat v3.1 niet
rendert — het échte mail-1-gat zit bij `signaal_blok`-tier-6 (generieke
fallback), waar geen gate op staat.

---

## Slotoordeel: wat zou ik vandaag durven versturen?

**Geen van de negen mails zoals ze nu renderen.** Het dichtst bij verzendbaar is
**mail 1 van de ai_audit-brug**: echte observatie met getal, droge toon, schoon
op score/lijst/AVG/prijs. Maar ook die draagt de em-dash in het onderwerp, twee
vragen, en een pitch+aanbod in de eerste mail — precies wat de kernbeslissing
van 2026-07-11 ("één observatie + vraag; aanbod pas na reply") verbiedt.

Niet verzendbaar:
- **Website-brug mail 1** — de ongetoetste "jullie homepage hangt
  waarschijnlijk…"-aanname is het soort claim waarvoor de eigen QA-gate openers
  afkeurt; drie vragen; pitch.
- **Alle mails 2 en 3** — ze claimen een opgenomen Loom/video die niet bestaat.
  Zonder Loom-mechanisme (kolom + opname-flow) of herschreven copy zijn deze
  zes mails feitelijk onwaar.

De kernconclusie is niet dat de copy slecht geschreven is — hij is verzorgd —
maar dat hij een **strategie van vóór 2026-07-11** uitvoert en om een
**Loom-mechanisme heen is gebouwd dat niet bestaat**. Herschrijven van de
templates naar de Fase A-beslissing (observatie + vraag, aanbod na reply, geen
em-dash in het onderwerp, één vraag) is een aparte, bewuste content-taak — geen
plumbing.
