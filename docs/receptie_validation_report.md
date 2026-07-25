# Receptie-detectoren — representativiteits-validatie (2026-07-25)

Doel: vaststellen of de receptie-ladder, gevalideerd op de eerste 100, betrouwbaar
blijft op een bredere/diversere set vóór volledige uitrol. Kill-switch dicht, niets
verzonden.

## Stap 1 — de base
- **963 leads** totaal. Receptie-doelgroep = **427 cosmetische_behandelaars** (alt-med
  421 = inactief legacy, geen target; makelaars/bouw 113 = uit ICP).
- Site-data-classifier + receptie-detectoren: **0 gepersist** (041 niet gedraaid; alleen
  de 100-scratchpad-cohort). Van de 427: 100 gescand, **327 beschikbaar**.
- Datakwaliteit-baseline (963): voornaam 48% onbruikbaar · SEO-title-naam 10% · derde/
  agency-email 17% · kvk_number 0%.

## Stap 2 — verificatie-batch (150: 43 non-Randstad + 107 verse Randstad, gecapt)
Vuur% + onbepaald% per haak, **batch vs eerste 100**:

| haak | hit batch | hit 100 | onbep. batch | onbep. 100 |
|---|---|---|---|---|
| Q4 | 18% | 12% | 5% | 0% |
| Q7 | 21%→**20%*** | 17% | 7% | 3% |
| Q2 | 12% | 7% | **34%** | 21% |
| P1 | 12% | 9% | 5% | 0% |

Mailbaar **42% (64/150)** vs 32% (eerste 100). *Q7 na de Jetpack-fix (2 vals-hits eruit).

- **Vuurpercentage ligt hoger, onbepaald ligt hoger.** De hits zijn in stap 3 als écht
  bevestigd → dit is een genuine populatie-verschil (bredere/regionale set = onvolwassener
  sites), niet een detector-artefact. De eerste 100 onder-representeerden de yield.
- Onbepaald hoger = fail-closed (detector onthoudt zich vaker, vuurt niet vals). Veilig.
- **Nieuwe platforms/boeksystemen: 0 echte gaten.** "semble" (= substring van "ensemble")
  en "boulevard" (= straatnaam) waren valse kandidaten, in context geverifieerd, niet
  toegevoegd.
- **Datakwaliteit non-Randstad vs Randstad: vergelijkbaar.** non-Randstad (43): voornaam
  ok 53% / junk 9% / leeg 37% · SEO 2% · agency 2%. Randstad (107): ok 56% / junk 4% /
  leeg 39% · SEO 1% · agency 8%. → non-Randstad niet structureel slechter; Randstad heeft
  meer agency-emails. Geen datakwaliteit-cliff buiten de Randstad.

## Stap 3 — blind httpx-kruischeck op de NIEUWE batch
5 hit + 5 geen per detector, andere methode dan de render. **31/32 harde agree** (P1 9/9,
Q4 8/8, Q2 6/6, Q7 8/9); 6 "weak" = statische methode mist JS-geladen kanalen (geen
detector-fout). De ene DISAGREE (dokterjan.nl, Q7) was een **echt gat**: WordPress/Jetpack
`stats.wp.com` die `_GA_RE` niet kende → 2/32 Q7-hits vals-positief. **Gedicht** (commit
`02292b1`, _GA_RE + Jetpack/fathom/umami/posthog/etc.). Na de fix: effectief **32/32**.

## GO / NO-GO
**Detector: GO — de volledige base (427 cosmetic) is veilig te scannen.** Het ene gevonden
gat (Jetpack) is gedicht; verificatie houdt 32/32 (≈ eerste 10/10 & 12/12); nul platform-
gaten. Het hogere vuurpercentage is genuine (geverifieerd), niet vals — de eerste 100
onderschatten de yield eerder dan overschatten.

**Outreach: de voornaam-enrichment is de blocker vóór brede uitrol** (niet vóór de eerste
32). ~43% van cosmetic heeft geen bruikbare naam; de gate vangt het af met "Hallo,", maar
handmatig invullen schaalt niet naar ~184 leads.

### Voornaam-recoverability (indicatief, steekproef 18 no-name cosmetic)
- **77% heeft een over-ons/team-bron** op de site (waar de eigenaarsnaam waarschijnlijk staat).
- **11% heeft direct een Dr./Drs.+naam** extraheerbaar (harde ondergrens).
- Realistisch recoverable = het merendeel van die 77%, maar het **precieze % is onbepaald**
  via snelle regex (NER te ruis; een quick-extract gaf junk als "opdown"/"ess"). Een echte
  meting vergt de verbeterde enrichment zelf.

## BACKLOG — blocker vóór brede uitrol (niet vóór de eerste 32)
**Voornaam-enrichment aan de bron verbeteren.** Nu pakt 'ie structureel het domein/een
initiaal i.p.v. de eigenaar. Bron: over-ons/team-pagina met rol-signaal (cosmetisch arts /
huidtherapeut / eigenaar / BIG) + LinkedIn-owner. Bron bestaat op ~77%. Meet ná implementatie
het echte recovery-% op een sample vóór de brede uitrol.
