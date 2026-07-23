# Plan — Haakje-machine (Fase 2): Guardrail-1-proof signaal-ladder

> Vervolg op [haakje_mapping_mail1.md](haakje_mapping_mail1.md). Aanleiding: de live-
> verificatie (2026-07-22) bewees dat de opgeslagen `website_intelligence`-signalen niet
> te vertrouwen zijn — 4 van 5 Founding-Five-hooks klopten niet (Tajmeel=Salonkee,
> My Unique=Treatwell hébben boeking; de "alleen een telefoonnummer"-hook was daar
> aantoonbaar onwaar). Conclusie: geen haakje mag de deur uit zonder live, Guardrail-1-
> proof detectie. Dit plan bouwt die machine.

## Doel

Elke mail-1-hook is een **deterministische, live-geverifieerde** observatie. De ladder
(signaal 1→6) kiest het eerste signaal dat écht vuurt; vuurt niks → route naar
automatisering-angle (nooit een zwak haakje forceren). De Founding Five komt vervolgens
uit dít geverifieerde proces, niet uit ongetoetste data.

## Status 2026-07-22 — Fase 2a GEBOUWD ✅

Scope-keuzes (Sami akkoord, "plan uit en ga te werk"): 2a eerst · steekproef ~50 ·
KvK-naam uit · geen-signaal = uitsluiten.

- **A** ✅ `website_intelligence/hook_detector.py` — G1-proof detector signaal 1/2/3
  (mobiel render → cookie weg → scroll → networkidle → boeking/tel/CTA-positie/TTI).
  Fixte de root-cause van de vorige batch: `booking_detector` matchte "book" in
  **facebook** (elke kliniek met FB-link → vals has_booking) → woordgrens-fix +
  unit-test. Signaal 2 pas vanaf **2× de vouw** (anders overclaimt "paar schermen").
- **F** ✅ `config/hook_templates.py` (6 signalen × 2 varianten, em-dash-vrij) +
  `build_haakje`/`build_zonde_brug`; `_CS_MAIL1` herstructureerd (haakje + zonde-brug
  vervangt opener + site_observatie); wiring in `sequence_engine.render_faseA_marker`
  (leest `fired_signal` fail-soft, robuust vóór de migratie); migratie
  `040_hook_signals.sql`. Tests bijgewerkt; volledige suite **720 groen**.
- **B (rough)** ✅ TTI meegenomen als signaal 3 (domInteractive, geen 4G-throttle → low-conf).
- **M** ✅ steekproef 50 (valid email, domain, over de score-range) → ~30% vuurt een
  echt signaal. Kandidaten 2× her-gescand (stabiliteit) → **8 bevestigd**.
- **Selectie** ✅ Founding Five voorgesteld (3× sig 1, 2× sig 2) + 2 reserve;
  `skins.nl` uitgesloten (retailketen, buiten ICP). Review-Artifact met live-bewijs +
  gerenderde mails opgeleverd. **Niets verzonden, kill-switch dicht.**

**Wacht op Sami:** migratie 040 draaien · per-mail akkoord + telefoon-check · dan pas
arm/launch/activate. Daarna **Fase 2b** (signaal 4/5/6 + vision + routing, richting 40–50).

## Wat we hergebruiken

- `scratchpad/verify_hooks.py` — werkende prototype van Guardrail 1 (mobiel viewport,
  cookiebanner weg, scroll, networkidle, boekwidget/tel:/CTA-check). Kern van item A.
- Bestaand: `website_intelligence/{conversion_checker,technical_checker,visual_analyzer}.py`,
  `job_queue/website_analysis_queue.py` + `scripts/run_website_worker.py` (losgekoppelde
  worker), `utils/deduplicator.py`, unsub→`suppression_list`, de begroeting-confidence-gate
  (`utils/lead_naming.display_first_name`), Fase A-templates + render (`config/sequence_templates.py`,
  `campaigns/sequence_engine.py`).

## Werkpakketten

| # | Pakket | Status nu | Effort |
|---|--------|-----------|--------|
| A | **Guardrail-1 detector** (signaal 1+2, robuust) — productie-module `website_intelligence/hook_detector.py` uit het prototype. Fix: betrouwbare "primaire boek-CTA"-herkenning + bounding-box (vouw-positie), platform-lijst uitbreiden, per-signaal bewijs opslaan. | ◐ prototype | hoog |
| B | **Signaal 3** — echte TTI onder 4G-throttle (CDP-throttling + performance-timing), i.p.v. pagespeed-score. | ✗ | midden |
| C | **Signaal 4** — tap-targets <44px/overlap (DOM-meting, Google-criterium). | ✗ | midden |
| D | **Signaal 5** — footer-jaar (≥3 terug) / laatste blogdatum (≥18 mnd). | ✗ | laag |
| E | **Signaal 6 + Guardrail 2** — vision-prompt (ELEMENT+SCORE), score <60 → GEEN. Plug in bestaande vision-worker. | ✗ | midden |
| F | **Hook-compositie** — fired_signal + variant → template-hook (config), "zonde"-brug, aanbod. Vervangt `build_site_observatie`/Claude-opener voor mail 1. Variant seeded per lead. Slaat signaal+variant op. | ✗ | laag |
| G | **Routing** — geen signaal 1–6 → `website_fit=false` → uit de conceptsite-flow (automatisering-angle = aparte track, later). | ✗ | laag |
| H | **Guardrail 3** — signaal-6-mails door de bestaande review-gate (`_gate_leads_for_template`) vóór send; 1–5 auto na G1. | ◐ | laag |
| I | **Naam-waterfall** — KvK(opt-in/uit) → team-pagina → LinkedIn → "Hoi,". Team-extractie + confidence versterken; begroeting-gate blijft. LinkedIn = nieuw. | ◐ | midden |
| J | **Send-timing** (Warmr) — avond, di–do. Checken of Warmr send-windows kent; zo niet, toevoegen. | ✗ checken | midden |
| K | **Dedup + suppressie** — dedup op domein/KvK (deduplicator-sleutel verifiëren); suppressie op nee/unsub/**flow-voltooid-zonder-reactie**. | ◐ | laag |
| L | **Deliverability** — SPF/DKIM/DMARC op meet-aerys.nl verifiëren; warmup-status. Plain-text/geen-tracking = klaar. | ◐ checken | laag |
| M | **Re-scan + re-select** — detector over een **brede** pool draaien (niet alleen top-78; leads met een echt website-probleem zitten vaak niet bij de hoogste lead-score), fired-signals vullen, Founding Five kiezen op *geverifieerd signaal vuurt*. | ✗ | midden |

## Volgorde (kritiek pad eerst)

**Fase 2a — minimaal geverifieerde batch (de send-gate):**
1. **A** — G1-detector voor signaal 1+2 productie-klaar (hoogste waarde, sterkste + best detecteerbare hooks).
2. **F** — hook-template-systeem (config-copy staat al in de spec).
3. **B (rough)** — laadtijd als signaal 3 meenemen (prototype meet 'm al globaal).
4. **M** — brede re-scan → selecteer leads met een echt kloppend signaal 1/2/3.
5. **H** — 1–5 auto na G1; niks routen we (nog) niet weg, we selecteren gewoon de vuurders.
→ Resultaat: een Founding Five waar élke hook live geverifieerd is. Klaar om (na jurist + inbox) te lanceren.

**Fase 2b — de volle machine (richting 40–50 leads):**
6. **C, D, E** — signalen 4/5/6 + vision + G2.
7. **G** — routing naar automatisering-angle.
8. **I, K** — naam-waterfall + dedup/suppressie-harding.
9. **J, L** — send-windows + deliverability-verificatie.

## Open beslissingen (input nodig)

1. **Scope eerste build:** alleen Fase 2a (signaal 1–3 + verifieer + herselecteer) — of meteen door naar de volle 6-signaal-machine (2b)? Aanrader: 2a eerst, echte batch in handen, dan 2b.
2. **Brede scan:** hoe breed her-scannen we? Alle ~cosmetische leads met website (kost Playwright-tijd), of eerst een steekproef van ~50 om te zien hoeveel er überhaupt een kloppend signaal hebben?
3. **KvK-naam-enrichment:** aanzetten (betaald, ~€6.40/m + €0.02/call) voor betere begroetingen, of voorlopig uit laten en op team-pagina/LinkedIn leunen?
4. **Automatisering-angle:** leads zonder website-signaal nu alleen uitsluiten, of ook die tweede track bouwen? (Aanrader: nu uitsluiten, track later.)

## Definition of done (Fase 2a)

- `hook_detector.py` draait per lead: fired_signal ∈ {1,2,3,GEEN} + bewijs (platform/tel/CTA-positie/laadtijd), met G1 (cookie/scroll/networkidle) aan.
- Mail 1 rendert de template-hook uit fired_signal; geen Claude-opener meer voor mail 1.
- Re-scan over de pool; ≥5 leads met een geverifieerd signaal → nieuwe Founding Five.
- Elk van die 5 handmatig tegen de live site bevestigd (spec-eis) vóór send.
- Niets verzonden; kill-switch dicht.
