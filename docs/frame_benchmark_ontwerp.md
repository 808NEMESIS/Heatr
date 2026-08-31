# Frame B2 — Benchmark (ONTWERP, niet actief)

> Status: **plank-werk** (Sami 2026-08-29). Activeren pas ná de C-canary-baseline,
> één variabele tegelijk (testdiscipline heatr-copy). Dit document legt de claim-
> klasse, gronding en zelfcontrole vast zodat activatie een besluit is, geen bouw.

## Waarom dit frame de sterke positie heeft

Alle eerdere frames faalden op de epistemische asymmetrie: wij claimden iets over
hún site dat zij in 10 seconden konden checken en wij nauwelijks. Het benchmark-
frame draait dat om: **de claim komt uit onze eigen dataset** (1.400+ geldig
gemeten kliniek-sites, provenance-gedragen) — de ontvanger kán hem niet casual
falsificeren, en hij is waar omdat wíj hem gemeten hebben.

## De claim-vorm

> "Van de ruim {N} {sector-woord} die we in {stad} bekeken heeft ongeveer
> {X op de Y} online boeken. {haakje afhankelijk van hun eigen status}."

- **Nooit kale puntgetallen richting prospect** (kernregel 2026-08-04): geen
  "74,3%", wél "ongeveer 3 op de 4" / "9 van de 10".
- N wordt afgerond benoemd ("ruim 100", "ruim 50") en moet ≥ de cel-drempel zijn.
- Het haakje splitst op hun eigen (geldig gemeten!) status:
  - zij hébben boeking → compliment-variant: "jij zit aan de goede kant" →
    brug naar het volgende niveau (bv. wat er ná de boeking gebeurt).
  - zij hebben géén boeking → "jij nog niet" → frictie-brug. Vereist dezelfde
    checked_at-dag-verse verificatie als Frame A (het is óók een claim over hen).

## Gronding (Regel 0, mechanisch afdwingbaar)

1. **Celdrempel:** stad×sector-cel met **N ≥ 30 geldige metingen** (rijkheid ≥1,
   niet-onzeker, detector_version ≥ 3). Nu al haalbaar in A'dam/R'dam/DH/Utrecht;
   23 cellen op N≥10 en groeiend — de drempel groeit vanzelf vol.
2. **Versheid:** celstatistiek herberekend op verzenddag (goedkoop: stored data);
   de per-lead-status (welke kant van de streep zíj zitten) dag-vers via de
   bestaande reverify-gate.
3. **Afronding vastgelegd:** percentage → dichtstbijzijnde "X op de Y" met
   Y ∈ {2,3,4,5,10}; afwijking claim vs. werkelijk < 7 procentpunt, anders de
   voorzichtigere breuk kiezen.
4. **Zelfcontrole-uitbreiding:** benchmark-getallen (N, X, Y) komen als
   `allowed_numbers` mee (zelfde anti-fabricatiepatroon als review-cijfers);
   een benchmark-zin zonder cel-verwijzing in de gronding → fail.

## Nog te beslissen bij activatie (Sami)

- Welke as eerst: online-boeking (hardste data) of tel-klikbaar/WhatsApp.
- Compliment-variant ook sturen, of alleen de "jij nog niet"-kant.
- Of de benchmark in mail 1 komt (vervangt de omdat-regel?) of in mail 2 (bewijs).

## Voorwaarden voor activatie

1. C-canary-baseline binnen (≥14 verstuurd, replies geteld).
2. Eén variabele: benchmark-variant tegen C-controle, zelfde onderwerp-stijl.
3. Celdrempel N≥30 gehaald voor de steden in de batch.
4. Skill-zelfcontrole uitgebreid (punt 4 hierboven) vóór de eerste render.

---

## Fase-2-experiment: mail 2 als gecontroleerde variabele (ontwerp 2026-08-31)

Drie plan-items versmolten tot één experiment (verbeterplan Sami): de canary
draait mail 1+3 zónder mail 2 (frame-coherentie: een claimloze mail 1 kan geen
"nog iets wat me opviel"-vervolg dragen). Mail 2 wordt daarmee de natuurlijke
plek voor het éérste gecontroleerde experiment ná de baseline:

**Arm A — concept-tease (claimloos):** mail 2 maakt het aanbod concreter zonder
één woord over hun site: "De Loom laat je homepage zien met jullie eigen foto's
en een kop die in één zin zegt waar je goed in bent. Zelfde ask." Waarde = het
concept voorstelbaar maken.

**Arm B — benchmark-claim (eigen data):** mail 2 draagt de eigen-dataset-zin
("van de ruim {N} praktijken die we in {stad} bekeken heeft ongeveer {X op de Y}
online boeken — jij zit aan de goede kant; waar het verschil nu zit is wat de
site daarmee doet"). Waarde = exclusieve informatie; test meteen claim-klasse B2
op de veiligste positie (mail 2, ná een claimloze opening).

**Regels:** één variabele (mail 2-inhoud; mail 1/3 en onderwerpen identiek en
bevroren) · cel-drempel N≥30 voor arm B · beide armen door de zelfcontrole
(arm B met benchmark-getallen als allowed_numbers) · omdat-variatieset (3–4
formuleringen, deterministisch op lead-id) gaat in DEZELFDE release zodat
massa-uniformiteit vóór fase 2 is opgelost · succesmaat vooraf: reply-rate per
arm bij ≥35/arm, anders alleen kwalitatief lezen.
