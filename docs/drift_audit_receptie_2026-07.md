# Drift-audit receptie-machine — 2026-07-28

**Opdracht (Sami):** verificatie, geen fix. Leg per onderdeel de bedoelde spec naast de
feitelijke code; rapporteer dekt / afgeweken / twijfel mét geciteerde regel. Aanleiding:
Q7 was tussen mapping en code verschraald zonder dat iemand het merkte → is er bredere drift?

**Methode:** 5 lezers (één per onderdeel) → per verdict een onafhankelijke skepticus die de
geciteerde regel zelf herleest en probeert te weerleggen (39 agents, 0 errors). Waar finder en
tegentoets verschilden, staat hieronder het **hertoetste** verdict.

**Kernuitkomst:** niet alleen Q7. **3 afwijkingen + 1 twijfel + 1 Q7 copy↔detector-gap**,
verspreid over 3 van de 5 onderdelen. Ladder-beslisser en opener-logica zijn schoon.
Zwaarste vondst is géén haak, maar een teruggeslopen kopie van de 2026-07-22 "medisch"-bug,
één fase eerder in de funnel.

---

## Actiepunten (geprioriteerd) — NIETS hiervan is uitgevoerd

### ① ⚠️ afgeweken — subcat-disqualifier-reject in de kwalificatie-fase  (breedst, stil)
`enrichment/lead_qualifier.py:106-113` (`qualify_raw_company`). De 2026-07-22-fix is intact in
de *scorer* (`icp_matcher.py:86`, alleen sector-globale disqualifiers), maar dezelfde bug-klasse
leeft één stap eerder: `qualify_raw_company` plat **alle** subcategorie-disqualifiers samen met
de sector-globale en past ze via **substring-match** toe als **harde reject vóór lead-creatie**.
Voor `cosmetische_behandelaars` belanden `medisch`, `arts`, `huidtherapeut`, `schoonheidssalon`
in de globale reject-lijst. Zelf-tegensprekend (door tegentoets bevestigd):
- `arts` ⊂ `cosmetisch arts` (eigen lead_keyword) → reject
- `huidtherapeut` (schoonheidssalons-disq, `sectors.py:380`) rejecteert de medische_huidtherapie-ICP
- `schoonheidssalon` (huidtherapie-disq) rejecteert de schoonheidssalons-ICP

Richting: **te streng / over-blocking**. Stille verliezen aan de bovenkant van de funnel
(leads worden nooit lead → onzichtbaar in de lead-tabel). Zelfde klasse als de ~51%-bug, nu
op naam/categorie i.p.v. Claude-samenvatting, in kwalificatie i.p.v. scoring.

### ② Q7 copy↔detector-gap  (raakt de eerstvolgende mails)
Detector zelf **dekt** z'n eigen spec. Maar Q7 is in de héle codebase de *meten/tracking*-poot,
niet de receptionist-poot: `hook_detector.py:611` `_HOOK_THEME={"Q7":"meten"}`,
`receptie_sequence.py:114` "Q7 (niet meten) = het lek is onzichtbaar", `:138` "zonder dat het
ergens in je cijfers opduikt". De "niemand vangt ze op"-frame hoort structureel bij **Q4**
(`geen_dekking_buiten_uren`). De Q7-mail-1-copy die vorige sessie naar receptionist-taal
herschreven is (`hook_templates.py:133-139` "vangt niemand ze op … die contacten ben je kwijt")
zit nu bovenop een detector die **analytics-afwezigheid** meet. Een kliniek met prima
telefoon-opvolging maar zonder Google Analytics krijgt die receptionist-claim → **weerlegbaar**.
Dit is het omgekeerde van de oorspronkelijke diagnose ("detector dreef naar tracking"): de
detector was altijd tracking; de reframe pointde de copy op het verkeerde construct.
Keuze bij fix: óf de detector gaat gemiste-contacten meten, óf de copy terug naar een meten-frame.

### ③ ⚠️ afgeweken — Q4 vuurt zonder Q2-gate  (send-content)
`hook_detector.py:633-634` `if form_present: fired.append("Q4")`. Afgesproken: Q4 mag alleen als
mail-1-haak als Q2=='hit' (Q4 ⊆ Q2). Code gate't op `form_present`, nergens op `q2=='hit'`. Bij
`mechanism=='ambiguous'` → Q2='onbepaald' (`:680`) maar Q4 vuurt tóch. **Fail-open / te los.**
Risico: mail-1-claim "alleen een formulier achterlaten" op een site waar Q2 juist als onbepaald
is afgekeurd.

### ③ ⚠️ afgeweken — Q2 telt elk formulier als aanvraag  (send-content)
`hook_detector.py:683` + `:695` (`input[type=email], textarea >= 1`). De fallback-tak telt een
nieuwsbrief- of zoekveld als "alleen_aanvraag" → valse claim. De erkende Fairday-val; de
go-live-checklist quarantainet daarom 6 Q4/Q2-leads voor handmatige narekening (compensatie
buiten de code).

### ④ ❓ twijfel — post-send afmeld-sweep draait niet automatisch  (compliance-net latent)
`scripts/verify_unsubscribe_compliance.py:5` claimt "via cron", maar geen cron/launchd/n8n
verwijst ernaar (repo commit z'n andere schedulers wél: 2 plists + 10 n8n-workflows). De
drip-blok-pre-gate (`assert_no_open_flags`, fail-closed) checkt op vlaggen die niets automatisch
zet → het "verifiëren-niet-aannemen"-vangnet is latent dood na mail 1, tenzij een operator de
sweep handmatig draait. Nu afgedekt door de kill-switch; echt fail-open gat zodra er verzonden wordt.

---

## Wat dekt (geen actie)

- **Ladder-beslisser — volledig schoon (8/8).** Hoogste vuurt wint Q4>Q7>Q2>P1
  (`hook_detector.py:643-644`); alleen `=='hit'` telt; **SKIN Amsterdam gefixt** (Q4+Q2→Q4,
  in productie gewired + test `test_hook_detector.py:489`); combine_stable fail-closed; mail-2 uit
  ander thema; sequence_engine herberekent niets (`:484`, leest persistente `receptie_hook_code`).
  NB: regel 832 (`_decide_signal`) is de conceptsite-ladder 1–6, een ánder mechanisme — niet de Q-ladder.
- **Opener-logica — volledig schoon (5/5).** groet→positief→haak (`receptie_sequence.py:68-70`);
  tier behandeling→reputatie→feitelijk (`hook_templates.py:275-301`); last-resort F4-veilig;
  review-dedup werkt (`:223-226`). Doc-ruis: module-docstring `:8` noemt `{{positief}}` niet (stale).
- **Compliance-gates a/b(render)/b(post-send)/c/d/e — dekt & fail-closed.** Privacy hard eerst
  (`receptie_sequence.py:162-163`); afmeld render-zijde (`:164-165`); post-send verificatie
  (`warmr_unsubscribe.py:46` + `sequence_engine.py:526`); voornaam (`lead_naming.py:65`);
  rechtsvorm BV-door/zzp+onbepaald-block (`legal_form.py:81-85`); F4-tijdclaim (`:172-173`).
- **Scoring — dekt.** fit=int(icp*40)+bonus (`lead_scoring.py:45`); icp genormaliseerd over
  evalueerbare (`icp_matcher.py:107-116`); leest kvk_sbi_code (`:106`, dode phantom-fallback);
  saturatie op 5 (`:96`); gate 55/0.50 (`lead_scoring.py:187-189`, in-code default 65/0.6 = strenger);
  medisch-fix intact in de scorer (`:86`); makelaars/bouw→icp 0 (`:163-165`).
- **Q4-detector zelf (`:604`) en P1-detector (`:532`) — dekt & fail-closed.**

Kanttekeningen zonder eigen actie: voornaam- en F4-gate zijn denylists (onbekende junk/tijdclaim
glipt door, begrensd want templates zijn statisch); treatment-tier opent met "Ik zag dat jullie…"
(eerste-persoons-perceptie, niet door F4-regex gevangen — content-punt); keyword-saturatie 'distinct'
is overdreven (geen dedup, `filler`⊂`fillers` telt dubbel).

---

## Impact op de 32

- **Geen actief verzend-lek** — kill-switch dicht + go-live-checklist quarantainet Q4/Q2-leads.
- **Wel raakt drift de wáárheid van de copy**: Q4/Q2-fail-opens → mogelijk onware "alleen een
  formulier/aanvraag"-claim; Q7 copy↔detector-gap → receptionist-claim op een tracking-signaal.
  Relevant vóór Q7- of Q4/Q2-mails uitgaan.
- **lead_qualifier-bug raakt de 32 niet** (al geselecteerd), maar vernauwt stil de toekomstige funnel.

## Aanbevolen fix-volgorde (Sami beslist)

① lead_qualifier-disqualifier → ② Q7 copy↔detector uitlijnen → ③ Q4-gate + Q2-fallback →
④ unsubscribe-sweep inplannen. Elk klein, afzonderlijk getest, suite groen.

---

## Status 2026-07-29 — fixes toegepast (Sami's go: "alles, aanbevolen volgorde")

Volle suite **954 groen** (was 939 + 15 nieuwe tests), alleen de 2 pre-existing live-ERRORs
(e2e/google_maps). Nog NIET gecommit tot Sami's woord.

- **① `enrichment/lead_qualifier.py`** — subcat-flatten weg; alleen sector-globale disqualifiers
  (spiegel `icp_matcher`). +`tests/test_lead_qualifier_disqualifiers.py` (7).
- **② Q7 → meten-frame (optie A)** — `hook_templates.py:133-140` uitgelijnd met de detector +
  mail-3; label `SIGNAL_NAMES["Q7"]` → `geen_meting`. 2 tests geflipt (opener_q7 + sequence).
- **③ Q4-gate** (`decide_receptie_hook`: `form_present and q2=='hit'`) + **`_FORM_PRESENT_JS`**
  aangescherpt (nieuwsbrief/zoek telt niet). +2 unit-tests; JS syntax + gedrag geverifieerd via
  DOM-mock (browser-tier runtime nog los te bevestigen).
- **④ afmeld-sweep** — `verify_unsubscribe_compliance.py` env-configureerbaar
  (`RECEPTIE_SWEEP_CAMPAIGN_IDS`) + `deployment/launchd/nl.aerys.heatr.unsubscribe-compliance.plist`
  (interval 1800s, `--apply`). +`tests/test_unsubscribe_sweep_scheduling.py` (6). Sami: `launchctl
  load` + env vullen.

### Beide open items opgelost (2026-07-29)
1. **Duplicate `"disqualifiers"`-key — SAMENGEVOEGD.** Dode (292) + live (502) tot één lijst van 19,
   duplicate-key opgeheven. apotheek/huisartsenpraktijk/drogisterij/verpleeghuis/zorginstelling erin;
   `SEH` bewust weggelaten (⊂ namen, redundant met 'spoedeisende hulp'). Geen enkele term is substring
   van een ICP-keyword (getoetst). +2 structurele guard-tests (geen-substring-botsing, geen-duplicate-key).
2. **De 32 → 25, GEPERSISTEERD + geverifieerd.** `run_receptie_backfill.py --apply` op het cohort;
   DB onafhankelijk teruggelezen: 31/31 rijen, 25 met een haak, 6 op `None` (oude fout-haken gewist),
   alle 31 vers herschreven (18–20 min). De verzendlaag draagt nu de nieuwe haken.
