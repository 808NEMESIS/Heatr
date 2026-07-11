# Heatr — Enrichment Audit

**Datum:** 2026-07-11 · **Basis:** live productiedata (858 leads, workspace aerys) + de enrichment-code.
**Vraag:** wat verrijkt Heatr, hoe goed, en wat is bruikbaar?

---

## 1. Wat Heatr verrijkt — de 15 stappen

`run_enrichment_for_lead` draait per lead een vaste keten (job_queue/enrichment_queue.py):

| # | Stap | Produceert | Claude? |
|---|---|---|---|
| 1 | `website` | technische scan (SSL, CMS, mobiel, schema, sitemap) | nee |
| 2 | `contact_crawl` | contactpersonen van de site | nee (regex/scrape) |
| 3 | `owner_extract` | eigenaar/beslisser | **ja** (duurste) |
| 4 | `email_waterfall` | e-mail zoeken + verifiëren | nee |
| 5 | `review_recency` | recentheid Google-reviews | soms |
| 6 | `archetype` | lead-classificatie | **ja** |
| 7 | `company_enrichment` | `company_summary` + `industry` | **ja** |
| 8 | `website_intelligence` | 5-laags analyse (techniek/conversie/sector) | deels |
| 9 | `domain_age` | domeinleeftijd | nee |
| 10 | `treatment_focus` | behandelingen (cosmetisch) | **ja** |
| 11 | `meta_ads` | draait Meta-ads? | nee |
| 12 | `contact_discovery` | extra contacten | nee |
| 13 | `data_verification` | consistentiecheck | nee |
| 14 | `scoring` | lead-score 0-100 | nee |
| 15 | `inbox_selection` | Warmr-inbox koppelen | nee |

**Kosten:** €6,35 totaal sinds januari voor ~800 leads = **~€0,008/lead**. Verwaarloosbaar; kosten zijn géén probleem. Duurste: `owner_extract` (€0,52/220 calls).

---

## 2. Dekking (858 leads)

**Uitstekend (99-100%):** domain, company_name, phone, city, sector, score, google_rating, gdpr_safe.
**Goed:** email 85%, personalized_opener 100%, company_summary 99%, archetype 86%, website_intelligence 94%, contact_first_name 73%.
**Zwak:** contact_last_name 42%, linkedin_url 15%.
**Dood:** `industry` (leeg voor 98% — zie #4), kvk_number 0% (opt-in, bewust uit), visual/screenshot-laag (bestaat niet in prod).

> **Let op:** "100% dekking" is meerdere keren schijn — velden bevatten lege strings i.p.v. NULL. `industry` telt als "gevuld" maar is leeg.

---

## 3. De bruikbaarheids-trechter (het cijfer dat telt)

```
743  in-ICP leads (alternatieve_geneeskunde 390 + cosmetische 353)
625  + e-mail
508  + contactnaam
507  + archetype
492  + website-analyse   ← BRUIKBAAR voor de observatie-opener (A3)
  1  + score >= 65
```

**Twee conclusies:**
- Er staan **~492 direct bruikbare in-ICP leads** klaar voor de fase-A-opener. Dat is een prima startvoorraad — genoeg voor de eerste batches.
- **De score-drempel (MIN_SCORE_FOR_WARMR=65) is volstrekt misgekalibreerd:** exact **1** in-ICP lead haalt 65. De `scoring`-stap en de realiteit staan haaks op elkaar. Voor de observatie-opener maakt dat niet uit (die heeft geen score≥65 nodig), maar `/leads/send-to-warmr` zou 491 van de 492 bruikbare leads weigeren. De drempel of de score-formule moet herzien vóór dat pad gebruikt wordt.

---

## 4. De problemen, gerangschikt

### KRITIEK

**P1 · `personalized_opener` is voor 88% vervuild met markdown/meta-rommel.**
Het veld met "100% dekking" en 99% uniciteit is **niet plak-klaar**: 758/858 beginnen met `# Openingszin:`, `**Beste X,**` of bevatten Claude's scaffolding. De inhoud is inhoudelijk goed en specifiek (verwijst naar review-aantallen/ratings), maar de ruwe Claude-output wordt ongeschoond opgeslagen. Elke mail-merge die dit rechtstreeks plakt oogt kapot.
- *Nuance:* de nieuwe observatie-opener (A3) omzeilt dit — die gebruikt `personalized_opener` niet meer. Maar het is een symptoom: prompt-output wordt nergens genormaliseerd/gevalideerd.
- *Fix:* een strip/normaliseer-stap op elke Claude-tekstoutput (headers weg, meta-labels weg, eerste echte zin pakken), + een validatie die vervuilde output afkeurt i.p.v. opslaat.

**P2 · E-mailkwaliteit is feitelijk onbekend — 624/625 in-ICP "risky", slechts 1 "valid".**
De SMTP-verifier (`email_verifier.py`) draait échte MX+RCPT-checks, maar de doelgroep (behandelaren op Google Workspace/gangbare hosters) is grotendeels catch-all of weigert RCPT-probes → resultaat "risky/inconclusief" voor vrijwel iedereen. Met `HEATR_ALLOW_RISKY_EMAILS=true` sturen we tóch. **Dit is het grootste deliverability-risico** — ongeverifieerde adressen = bounces, en bounces beschadigen precies de warme inboxen die net met moeite ready zijn (zie Warmr-audit). Eén slechte batch kan de capaciteit weer nekken.
- *Fix-opties:* (a) een tweede verificatiebron naast SMTP (bv. een verify-API voor de "risky"-bucket vóór verzending); (b) hardere bounce-drempels per inbox (nu grotendeels dood — Warmr-plan W3); (c) conservatief starten: eerst kleine batches, bounce-rate meten vóór opschalen.

### HOOG

**P3 · Legacy-vervuiling: 113 out-of-ICP leads** (58 makelaars + 55 bouwbedrijven) uit de oude sector-pivot, met `archetype=None`. 13% van de base is ruis die niet verrijkt/gemaild hoort te worden. → filteren/archiveren.

**P4 · 856/858 leads zitten vast in `queued_no_inbox`.** Ze zijn gescoord maar nooit gepusht omdat er geen ready inbox was — het directe gevolg van de paused-inbox-fuik (nu gefixt). **Nu er 2 inboxen ready zijn, kan deze hele voorraad eindelijk bewegen.** Downstream-unblock, geen bug meer.

### MIDDEL

**P5 · `industry` is dode output** (leeg voor 98%). `company_enrichment` levert een goede `company_summary` maar het `industry`-veld blijft leeg. Niet kritiek (industry is nauwelijks nodig), maar het is een stap die deels niets produceert.

**P6 · De visuele laag ontbekt volledig.** Geen screenshots (bucket bestaat niet), `visual_score=0` overal, geen Claude-vision-proza. De website-intelligence is techniek+conversie+sector — de visuele beoordeling die een rijkere teardown zou voeden draait niet. Bewuste kosten/config-keuze, maar het beperkt de diepte van het teardown-product.

---

## 5. Wat gewoon goed is (niet aankomen)

- **Discovery + firmografie:** domain/naam/telefoon/stad/sector/rating op 99-100% — sterke, verse bottom-up data die Apollo c.s. voor dit segment niet hebben.
- **website_intelligence:** rijk en op schaal (492 in-ICP volledig). Dit is het echte wapen — de pass/fail-checks voeden zowel de opener als de teardown.
- **personalized_opener-*inhoud*** (los van de opmaak): genuinely specifiek, 99% uniek — geen templated slop.
- **Kosten:** ~€0,008/lead. De cost-guard + resumable enrichment (net gebouwd) houden dit beheerst.
- **Pipeline-robuustheid:** draait op schaal, 807 sites geanalyseerd, per-stap-hervat na crash.

---

## 6. Aanbevolen acties (in volgorde, gekoppeld aan fase A)

1. **Vóór de eerste echte batch (deliverability):** conservatief starten (kleine batch, bounce-rate meten), want de "risky"-emailrealiteit is een reëel bounce-risico voor de net-herstelde inboxen (P2).
2. **Legacy opschonen (P3):** de 113 makelaars/bouw uit de actieve set filteren — snel, verkleint ruis.
3. **Opener-normalisatie (P1):** strip-stap op Claude-tekstoutput — nodig zodra we `personalized_opener` of andere gegenereerde velden ergens tonen. (Voor A3 niet-blokkerend.)
4. **Score-drempel herzien (§3):** 65 is voor deze data onbruikbaar; ijk de formule of de drempel vóór het send-to-warmr-pad in gebruik gaat.
5. **Voorraad activeren (P4):** de 492 bruikbare leads uit `queued_no_inbox` zijn de fase-A-startvoorraad — nu de inboxen ready zijn.

**Kern:** de enrichment produceert op schaal, goedkoop en met echte diepte — de basis is sterker dan bij de meeste tools. De zwaktes zitten niet in "te weinig data" maar in **kwaliteitscontrole** (ongeschoonde AI-output, onbekende e-mailkwaliteit, misgekalibreerde score) en in **hygiëne** (legacy-ruis, dode velden). Geen van deze blokkeert fase A; P2 (bounce-risico) is de enige die je vóór de eerste batch bewust moet managen.
