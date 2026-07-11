# Heatr — Strategie & Optimaal Plan vs. HubSpot / Salesloft / Apollo / Instantly

**Datum:** 2026-07-11
**Kader:** Heatr = B2B mailtool + CRM-lite, verticaal op behandelaren NL/BE, motor achter de Aerys-ladder (review → website → AI-audit).
**Basis:** de audits + benchmark-research (lifecycle_audit v1/v2) + de feitelijke staat van de codebase na WP-A/fase 2-4.

---

## 1. De vier concurrenten — wat ze écht zijn

| | Wat het is | Sterktes | Zwaktes (de openingen) |
|---|---|---|---|
| **HubSpot** | Alles-in-één CRM-platform, inbound-DNA | Volledige suite (CRM+marketing+service), volwassen lifecycle-stages met progressie/regressie-criteria, ecosysteem, merkvertrouwen, rapportage | **Duur en zwaar voor MKB** (contact-based pricing, implementatie vergt consultants); cold outbound is niet hun DNA (geen sending-infra/warmup); generiek — nul sector-diepte; een 3-persoons kliniek-agency "beheert" HubSpot i.p.v. dat het klanten oplevert |
| **Salesloft** | Sales-engagement voor enterprise SDR-teams | Multi-channel cadences (call/mail/LinkedIn), dialer + conversation intelligence, per (lead×sequence)-state, team-governance, Salesforce-diepte | **Vereist een team + bestaande data**: geen discovery, geen enrichment, geen eigen deliverability (BYO); enterprise-pricing per seat; voor een 1-persoons agency letterlijk onbruikbaar; VS-centrisch |
| **Apollo** | B2B-database (270M+) + engagement, PLG | Gigantische database, goedkoop instappen, alles-in-één (data→sequence→dialer), intent-signalen, sterke product-led groei | **Data-kwaliteit voor NL/lokaal MKB is zwak**: praktijken zonder LinkedIn-voetafdruk staan er niet of verouderd in; shared-list-effect (iedereen mailt dezelfde adressen → inbox-moeheid); GDPR-grijs in de EU; personalisatie blijft mail-merge |
| **Instantly** | Cold-email-verzendmachine op volume | Spotgoedkoop, unlimited inboxes + warmup, dead-simple, deliverability-tooling | **Race to the bottom**: puur spray-and-pray, geen data/CRM/intelligentie; het volume-model wordt actief afgestraft door Google/Microsoft (2024-2026 crackdowns); geen sector-kennis; hoge churn — het is een wegwerptool |

### De structurele gap die alle vier laten liggen

Ze zijn allemaal **horizontaal** (elke sector, elke geografie) en beginnen allemaal bij **een lijst**. Geen van de vier doet:

1. **Bottom-up discovery van lokaal MKB** — bedrijven vinden via Google Maps/website/KvK die in géén database staan. Apollo's blinde vlek is precies Heatr's doelgroep: de fysiopraktijk en botox-kliniek zonder LinkedIn-profiel.
2. **Verticale intelligence** — wéten wat een goede cosmetische-kliniek-website is, welke behandelingen er zijn, wat de concurrent in dezelfde stad doet. Dat is per definitie niet horizontaal schaalbaar, dus doen ze het niet.
3. **Waarde vóór het gesprek** — alle vier sturen vragen om tijd; niemand levert een artefact dat op zichzelf waarde heeft.
4. **Timing op werkelijkheid** — Apollo heeft "intent data" (vaag, derde-partij); niemand detecteert dat een specifiek bedrijf zíjn website vernieuwde, reviews verloor of een vacature plaatste, en benadert exact dán.
5. **Eigen deliverability als geïntegreerde asset** — HubSpot/Salesloft hebben geen sending-infra, Apollo deelt reputatie, Instantly verhuurt volume. Heatr+Warmr bezit de hele keten.

**Heatr's positie in één zin:** niet "een goedkopere Apollo" maar een **klantenmachine voor lokale dienstverleners-verticals** — vind wie niet in databases staat, analyseer hun website dieper dan zij zelf ooit deden, geef die analyse wég op het moment dat er iets verandert, en begeleid het gesprek tot deal in een CRM dat één operator aankan.

---

## 2. Wat Heatr vandaag heeft vs. mist (eerlijk)

**Heeft (en de vier niet):** bottom-up discovery (Maps/KvK/website), 5-laags website-intelligence + vision + concurrentiebenchmark, eigen warmup/sending (Warmr), GDPR-verdedigbare eigen bronnen, sector-configs, en sinds deze week: een betrouwbare verzendlaag (idempotency, suppressie, enrollments, guards).

**Mist (en de vier wel):** volume-capaciteit (2 inboxen!), multi-channel, bewezen conversie-data, een lerende lus met echte uitkomsten, teamfuncties, en de lifecycle-intelligentie van fase 5 (nurture/recontact-vertakkingen).

**Mist (en niemand heeft):** de teardown-pagina als product, en markt-brede verandering-detectie. Dit zijn de twee revoluties — beide half aanwezig in de code.

---

## 3. Het optimale plan — vier fasen

### Fase A — "Bewijs" (nu → 4 weken)
Doel: **10 gevoerde review-gesprekken, ≥3 betaalde opdrachten**, elke uitkomst vastgelegd. Revolutie begint met bewijs, niet met architectuur.

1. **De teardown-pagina** (grootste hefboom, kleinste bouw): per lead een gegenereerde, deelbare pagina — score, screenshot, 3 verbeterpunten, concurrentvergelijking uit hun stad. Data bestaat al in `website_intelligence`; nodig: een render-endpoint + publieke token-URL + kliktracking (elke view = intent-event in de webhook-ledger).
2. **Mail wordt bezorger**: de v3.1-sequences herschrijven rond de link ("ik heb jullie site naast {concurrent} gelegd — hier staat wat ik zag"), niet rond de vraag om een gesprek.
3. **Capaciteit**: van 2 naar 6-10 inboxen (2-3 domeinen), warmup nu betrouwbaar dankzij W0/W1. Zonder dit is alles theorie.
4. **Intent → actie**: teardown bekeken → `crm_task` "bel binnen 24u" met de kijkdata erbij. De mens-in-de-loop is het product.

### Fase B — "De timing-motor" (1–3 maanden)
Doel: van lijst-gebaseerd naar trigger-gebaseerd — het structurele antwoord op de deliverability-crisis die Instantly's model sloopt.

1. **TAM-database**: alle behandelpraktijken NL/BE (~30-50k) via de bestaande discovery, continu ververst. Dit is de eigen "Apollo voor lokaal" — verse data die nergens te koop is.
2. **Markt-brede verandering-detectie**: de `recontact_signals`-motor generaliseren van "benaderde leads" naar de hele TAM (re-crawl-scheduler + signaal-taxonomie: site vernieuwd, rating-drop, vacature, KvK-mutatie, nieuwe vestiging). Eerste contact gebeurt alleen nog op een trigger.
3. **Fase 5-lifecycle** (uit het actieplan) nu bouwen — gevoed door echte events: recontact-ingangen A-E, nurture-baan met teardown-updates als waarde-touch, twee-assen state machine.
4. **Warmr W2/W3**: score-integriteit + placement-tests, zodat capaciteitsgroei niet op drijfzand staat.

### Fase C — "De lerende lus" (3–6 maanden)
Doel: elke uitkomst maakt de volgende 100 leads beter — de data-flywheel die geen van de vier voor deze niche kan bouwen.

1. **Outcome-feedback**: deal gewonnen/verloren → archetype/brug/trigger-gewichten → discovery-prioritering (de `feedback_processor` krijgt eindelijk echte voeding).
2. **Tweede vertical activeren** (tandartsen/mondhygiënisten — al in de sector-focus): bewijst dat het playbook kopieerbaar is.
3. **Beslispunt: intern wapen of product?** Multi-tenant-fundament ligt er (fase 4). Optie: Heatr als tool voor andere lokale agencies/verticals verkopen ("Apollo voor lokaal MKB, per vertical"). Pas beslissen mét fase-A/B-bewijs.

### Fase D — "De moat" (6–12 maanden)
1. **Heatr+Warmr als één platform** (W5-events, capacity, reputatie) — deliverability als eigendom, niet als huur.
2. **Data als content-moat**: "De staat van de kliniek-websites 2026" — benchmark-rapporten uit eigen data die niemand anders heeft; inbound-motor bovenop de outbound-machine.
3. **Multi-channel volwassen**: telefoon-orkestratie en WhatsApp Business — de kanalen die voor MKB-behandelaren écht werken en waar de VS-tools blind voor zijn.

---

## 4. Hoe dit elke concurrent counterd

| Tegen | Heatr's antwoord |
|---|---|
| HubSpot | Geen platform om te beheren maar een machine die klanten oplevert; MKB-prijs; sector-diepte die HubSpot nooit bouwt |
| Salesloft | Werkt voor een team van één; levert zelf de data én de verzending die Salesloft veronderstelt |
| Apollo | Verse bottom-up data van bedrijven die niet in hun database staan; GDPR-verdedigbaar; triggers i.p.v. stale intent |
| Instantly | Kwaliteit-over-volume overleeft de crackdowns: minder mails, betere timing, waarde vooraf — en eigen warmup-infra |

## 5. Wat we bewust NIET doen

Geen feature-pariteit najagen (dialer, LinkedIn-automation, dashboard-orgie). Niet horizontaal gaan vóór twee verticals bewezen zijn. Geen volume-spel — dat is Instantly's stervende heuvel. Geen fase-C/D-bouw vóór fase-A-bewijs.

## 6. Risico's

| Risico | Mitigatie |
|---|---|
| Capaciteit blijft mini (2 inboxen) | Fase A punt 3 is niet optioneel; Warmr-plan W2/W3 borgt de groei |
| Google/MS maken cold mail nóg moeilijker | Trigger-based + waarde-eerst = structureel lagere volumes met hogere relevantie — precies de richting die de filters belonen |
| GDPR | Eigen bronnen + documenteerbare grondslag (gerechtvaardigd belang B2B) + de suppressie-laag van fase 2; blijft dossier-onderhoud |
| Eén operator als bottleneck | Intent-gedreven taken (fase A punt 4) maximaliseren de waarde per uur; de machine filtert, de mens sluit |

---

*Kern: de vier grote spelers verkopen gereedschap aan mensen die al weten wie ze willen mailen. Heatr weet wíe, wéét waarom, en heeft iets te géven — voor een markt die voor de grote vier onzichtbaar is. Dat verdedig je niet met features maar met data die zij niet kunnen krijgen en timing die zij niet kunnen zien.*
