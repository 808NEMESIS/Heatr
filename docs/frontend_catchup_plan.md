# Frontend catch-up — plan (2026-07-20)

De backend-laag die deze sessie is gebouwd (audit-scorer Tier 1/2, Fase A-outreach
+ Founding-Five-teller, coördinaten/concurrent-selector, sector-poort/allowed_offers,
GDPR erase/export) heeft **vrijwel geen UI**. Alles is via API/CLI aangestuurd en
bewezen, maar een operator kan het niet klikken. Dit plan maakt de gaten expliciet,
scheidt **puur-frontend** van **frontend + nieuw endpoint**, en prioriteert op
"wat blokkeert veilig operationeel gebruik".

Niet-blokkerend nu (kill-switch dicht, niets gelanceerd), maar nodig vóór dagelijks
operationeel gebruik door iemand anders dan via de API.

## Grondslag (geverifieerd)

- Frontend = `frontend-next/` (React + Vite + TanStack Query, `/api`-proxy,
  `src/components/ui/*` incl. `Tabs`). 16 pagina's; **0 aangeraakt** deze sessie.
- `LeadDetail.tsx` is al **getabt** (`overview` + meer) en fetcht `/leads/{id}` →
  natuurlijke plek voor een Audit-tab + GDPR-acties.
- `CampagneLaunch.tsx` roept `/campaigns/preview` + `/campaigns/launch` aan — maar
  die draaien nog de **oude v3.1-flow** (Fase A is nog niet gewired).

## De gaten-matrix

| Feature | API-status | UI-thuis | Operator-actie | Prio | Omvang |
|---|---|---|---|---|---|
| **Founding-Five-teller** | ❌ **geen enkel endpoint** | nieuw paneel (Control of CampagneLaunch) | plek "vergeven" zetten + zien hoeveel vrij | **P0** | API M + UI M |
| **Fase A-launch** | preview/launch bestaan, maar v3.1 | CampagneLaunch (herzien) | juiste sequence/bruggen kiezen + lanceren | **P0** | wacht op backend-wiring + UI M |
| **Audit-rapport (Tier 1/2)** | POST bestaat, ❌ **geen GET** | Audit-tab in LeadDetail | audit triggeren + rapport bekijken | **P1** | API S + UI M |
| **GDPR forget/export** | ✅ endpoints bestaan | LeadDetail (actie-menu) | lead vergeten / data exporteren | **P2** | UI S (puur frontend) |
| **Sector-poort / allowed_offers** | backend, vloeit in WebsiteKansen-data | WebsiteKansen (badge) | — (automatisch na re-classify) | **P3** | UI XS |

## Benodigd backend-werk (endpoints die nog niet bestaan)

Deze moeten er zijn vóór de bijbehorende UI kan bestaan:

1. **`GET /leads/{id}/audit`** — haal het laatste opgeslagen `audit_report` op (per
   tier) zonder her-runnen/her-kosten. Nu geeft alleen de POST een rapport terug →
   je kunt een bestaand rapport niet tonen zonder opnieuw te scoren. Klein.
2. **Founding-Five-endpoints** (nieuw), workspace + niche-gescoped:
   - `GET /founding-five` → per niche: totaal, vergeven, vrij.
   - `POST /founding-five/{niche}` → markeer een plek vergeven (lead_id + deal_ref,
     drempel = getekende deal). Schrijft `heatr_founding_five_slots`.
   - `DELETE /founding-five/{slot_id}` → plek vrijgeven (deal afgeketst).
   Dit is de bron voor de live schaarste-teller in mail 3; zonder deze endpoints
   moet de operator handmatig in de DB, precies wat de eerlijke schaarste ondermijnt.

## Per item — wat de UI doet

### P0-a · Founding-Five-teller
Klein paneel: per niche (cosmetiek/chiro) "X van 5 vergeven, Y vrij", een knop
"plek vergeven" (kies lead + deal-referentie), en een lijst van vergeven plekken met
"vrijgeven". Plaats: **Control** (ops-overzicht) of bovenaan **CampagneLaunch**
(want de teller stuurt de mail-3-copy). Leest/schrijft de nieuwe endpoints.

### P0-b · Fase A-launch (CampagneLaunch herzien)
Hangt op de backend-wiring van Fase A in `/campaigns/launch` (aparte taak). Zodra
gewired: de preview toont de **twee bruggen** (conceptsite/workflow), de
mail 2-degradatievariant per lead, en de live plekken-teller in mail 3. De operator
kiest cohort + inboxen en lanceert. **Kritiek:** tot dit herzien is, lanceert de
pagina de oude v3.1-flow — dus deze UI + de wiring moeten sámen live.

### P1 · Audit-tab in LeadDetail
Nieuwe tab naast `overview`/website: knop "Audit draaien" (Tier 1) → toont
`score_normalized`, de 7 categorieën, findings (met `mail_zin`/`bewijs`),
knock-outs, en bij Tier 2 de reviews-vergelijking + benchmark. Vereist de nieuwe
`GET /leads/{id}/audit` om een bestaand rapport te laden; de POST voor (her)runnen.

### P2 · GDPR-knoppen in LeadDetail
Puur frontend — endpoints bestaan al. In het actie-/overflow-menu: "Exporteer data"
(`GET /gdpr/export/{id}` → download JSON) en "Vergeet lead" (`POST /gdpr/forget/{id}`,
met bevestigingsdialoog — het is onomkeerbaar). Toon de `gdpr_log`-historie op de
pagina zodat je ziet dát/wanneer het gebeurde.

### P3 · Sector-poort-badge (WebsiteKansen)
Grotendeels automatisch: na de re-classify tonen de dienst-tags op WebsiteKansen
vanzelf geen automatisering meer voor alt-zorg/chiro. Optioneel een klein label
"website-only (sector)" zodat de operator snapt waarom er geen AI-audit-tag staat.

## Voorgestelde volgorde

```
1. Backend-endpoints eerst (GET /leads/{id}/audit + founding-five CRUD)  ← deblokkeert UI
2. P0-a Founding-Five-teller-UI        (eerlijke schaarste = launch-kritisch)
3. P0-b Fase A-launch-herziening       (samen met de sequence-wiring)
4. P1  Audit-tab                        (het Trojan-Horse-product zichtbaar maken)
5. P2  GDPR-knoppen                     (compliance-hygiëne, puur frontend, snel)
6. P3  Sector-badge                     (cosmetisch, laagste prio)
```

## Conventies (volgen)
- TanStack Query + `api.get/post` op `/api`-proxy; `src/components/ui/*` hergebruiken
  (Tabs, dialog, badge). Design: Fraunces (headings) + Plus Jakarta Sans, lichtpaarse
  gradient-accenten — zelfde taal als de bestaande pagina's.
- Onomkeerbare acties (forget, launch) achter een bevestigingsdialoog.
- Alle writes workspace-gefilterd; auth via de bestaande Bearer-JWT-flow.

## Buiten scope van dit plan
Het live-wiren van Fase A in `/campaigns/launch` (aparte backend-taak, wacht op
jouw go) en de drempel-herijking (fase B, wacht op de eindstaat). Dit plan gaat
alleen over de UI-laag + de minimale endpoints die die UI nodig heeft.
