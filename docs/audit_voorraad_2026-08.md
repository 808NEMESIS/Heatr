# Voorraad-kwaliteitsaudit (A2) — 2026-08-03

Meting: `scripts/voorraad_audit.py` (her-runbaar, read-only) · 1046 leads, workspace aerys.
Context: A1 (integraties) draaide dezelfde dag — 8×OK, 2×DEGRADED (Bouncer-tegoed 0,
KvK-abonnement 401). A4-DNS: meet-aerys.nl SPF ✅ DKIM(google) ✅ DMARC p=quarantine ✅
MX Google ✅ (gemeten via 1.1.1.1 + 8.8.8.8; de lokale resolver liet TXT vallen — altijd
publieke resolver gebruiken bij DNS-checks).

## Hoofdvondsten (op volgorde van gewicht)

### 1. 🔴 73% van de voorraad is STALE (>90 dagen niet bijgewerkt)
763/1046 leads zijn sinds ±april niet meer aangeraakt. Website-data, ratings en haakjes
van die leads zijn 4 maanden oud. **Gevolg**: elk haakje op een stale lead is een
onbetrouwbare claim tot her-check. Het 25-cohort is hierop de uitzondering (eind juli
her-gedetecteerd). **Actie**: re-enrichment-sweep vóór elke campagne buiten het cohort;
prioriteer de 289 launchbare.

### 2. 🔴 Cohort: 1 dode site + 7 naamloos + 25× rechtsvorm open
- **joostkroon.com is onbereikbaar** (ConnectError) — Q4-lead; claim onhoudbaar zolang
  de site plat ligt. → hercheck / uit cohort tot bewezen live. *(Was nota bene een
  "mag mee"-lead uit de eerdere Q4-triage.)*
- 7/25 zonder voornaam → 'Hallo,'-fallback (bekend, F3-curatie).
- 25/25 zonder `kvk_legal_form` → AVG-02 (bekend; KvK-abonnement activeren of CSV-route).

### 3. 🟠 41% van de voorraad is niet-actieve sector — en één groeit nog steeds
`ACTIVE_SECTORS` = {cosmetische_behandelaars, chiropractoren}, maar de voorraad bevat
426 alt-geneeskunde + 58 makelaars + 55 bouw (= 539/1046). Belangrijker: **de
tweewekelijkse alt/Utrecht-schedule scrapet een sector bij die niet benaderd wordt** —
`get_sector()` accepteert 'm (definitie bestaat) terwijl ACTIVE_SECTORS 'm uitsluit.
Dashboards tellen dit alles vrolijk mee. **Actie (F0.4, nu met cijfers)**: schedule
pauzeren + kiezen: alt archiveren of heractiveren. Plus 2 junk-rijen (sector None /
zakelijke_dienstverlening) opruimen.

### 4. 🟢 Gate-funnel is gezond en consistent
352 door 55/0.50 → 341 sendable → 289 gdpr_safe (matcht alle eerdere metingen).
0 icp/score-outliers. 5 leads door de gate zonder e-mail (score-model laat dat toe;
de sendability-gate vangt ze — geen lek, wel ruis).

### 5. 🟢 Duplicaat-rest is verwaarloosbaar
4 fuzzy domein-groepen: 1× echte dubbel (homeopathieamsterdam.nl 2× Amsterdam),
1× multi-vestiging (skin.nl Groningen/Amsterdam — legitiem), 2× eigen testdata
(aeryssolution.nl, makelaaramsterdam). Geen structureel dedup-probleem na 021.

### 6. Velden-dekking per sector (kern)
| sector | n | naam | sendable | site-score |
|---|---|---|---|---|
| cosmetisch | 505 | **62%** | 63% | 83% |
| alt-geneeskunde | 426 | 81% | 64% | 96% |

Naam-dekking cosmetisch (62%) blijft de zwakste schakel voor personalisatie — bevestigt
de bekende backlog (enrichment-aan-de-bron vóór brede uitrol).

## Reparatie-lijst
**Script-baar (Claude):** junk-rijen archiveren (2) · homeopathie-dubbel mergen ·
re-enrichment-sweep runner voor de 289 launchbare (bestaande queue, gedoseerd).
**Mensenwerk (Sami):** joostkroon.com herchecken · F0.4-besluit alt-geneeskunde ·
7 cohort-namen (F3) · Bouncer-tegoed · KvK-abonnement.

## Her-run
Na elke sweep-tranche: `python3 scripts/voorraad_audit.py` — secties 1/3/5 zijn de
sweep-stuurinstrumenten (dekking, versheid, sector-drift).
