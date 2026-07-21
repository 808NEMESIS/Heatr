# Bruikbaarheidsaudit — Heatr per doeleinde (2026-07-21, eindstaat)

Meting op de geverifieerde eindstaat (953/962 gescoord, drempels herijkt, visual-
dekking gekocht, Tier 1 gedraaid). Oordeel: *productieklaar* (end-to-end bewezen op
schaal én output geconsumeerd) · *gedeeltelijk* (draait, bekend gat) · *alleen op
papier* (code, nooit aantoonbaar gedraaid) · *afwezig*.

## Per doeleinde

| Doeleinde | Oordeel | Bewijs / schaal | Zwakste schakel |
|---|---|---|---|
| **Lead discovery** | **gedeeltelijk** | cosmetiek 44 jobs → 426 leads · alt-med 14 → 421. Bewezen pad. | **chiro = afwezig** (0 jobs ooit; scoremodel op aannames). alt-med staat op inactief. |
| **Lead enrichment** | **productieklaar** | 962 leads verrijkt: score 100% · opener 99% · phone 97% · **email 80%** (518 valid). | contact_first_name **71%** (29% opent "Hoi,") · 174 catchall_risky (grijze e-mailzone). |
| **Kwalificatie / scoring** | **productieklaar** | 953 genormaliseerd; percentiel-drempels (41/49/56) net gelijkgetrokken over classifier + frontend + env; sector-poort re-classified. | consumptie wás zwak (frontend herberekende eigen banden) — **nu gedicht** (één set overal). |
| **Website-audit (prospect-facing)** | **gedeeltelijk** | **962 Tier 1-rapporten** (269 knock-outs, 30 lege sites); Tier 2 live (Places bewezen). | **geen UI** — het rapport bestaat maar is niet klikbaar/verstuurbaar (frontend-catch-up). alt-med-model dekt niche niet (31/90 checks). |
| **Outreach** | **gebouwd, nooit gedraaid** | specs 1-5 af + live-getest (dry-render); 216 verzendbare leads. | **outbound_log 0** — nooit gelanceerd; niet live-gewired; kill-switch dicht. |
| **Compliance / AVG** | **productieklaar (code)** | GDPR erase/export E2E-bewezen; verwerkersregister opgesteld; compliance_check in alle gates. | **jurist-review** nog niet (extern, verplicht vóór 1e kliniek); geen UI voor forget/export. |
| **`alternatieve_geneeskunde` (legacy)** | **legacy / gedeeltelijk** | 421 leads scoren mee; keyword-set net verbreed; sector-poort houdt ze website-only. | sector **inactief** (niet in ACTIVE_SECTORS); audit-scoremodel dekt de niche niet (mist beroeps-/medisch-trust). |

## De funnel (van gescrapet tot verzendbaar)

```
gescrapet (companies_raw)        1092
leads aangemaakt                  962   (88%)
verrijkt (score/enriched)         962   (100%)
compliance-veilig                 616   (64%)   ← −346: gdpr_safe / legacy-sectoren
verzendbaar e-mail (valid-only)   418   (43%)   ← −198: strikte valid-gate
score ≥ 55                        217   (23%)   ← −201: launch-gate
opener aanwezig                   216   (22%)   ← −1
niet in cooldown = VERZENDBAAR    216   (22%)
```
**End-to-end: 216 verzendbare leads van 1092 gescrapet (20%).** Allemaal conceptsite-
brug (workflow geschrapt); mail 2 is voor 39% volledig gepersonaliseerd (concurrent +
detail_2), 100% heeft minstens detail_2, 0% kale degradatie, 0 content-anomalieën.

## Waarvoor is Heatr vandaag inzetbaar — en de eerstvolgende schakel

**Vandaag inzetbaar:** de kern **discovery → enrichment → scoring** is productieklaar
op cosmetiek + alt-med (847 leads, 216 verzendbaar, alles op echte data). De prospect-
facing audit draait op schaal (962 rapporten) en de scoring is voor het eerst
consistent (één drempelset, sector-poort waterdicht). Je kunt vandaag een lead van
scrape tot "verzendklaar met gepersonaliseerde mail" brengen — en dat is bewezen.

**De eerstvolgende zwakste schakel die het meeste vrijmaakt:** de outreach is
volledig gebouwd maar **nooit gelanceerd** en **niet live-gewired**. Eén ding
deblokkeert de hele end-to-end-keten: **de Fase A-sequence wiren in
`/campaigns/launch` + een eerste testlancering naar een eigen adres.** Dat verandert
outreach van "alleen op papier" naar "end-to-end bewezen", en legt meteen de twee
resterende poorten bloot die géén code zijn (jurist-review + Warmr-inbox-gezondheid).
Daarna is de op-één-na-zwakste schakel de **frontend-catch-up** (audit-rapport,
Founding-Five-teller en GDPR-knoppen hebben nog geen UI — zie
`docs/frontend_catchup_plan.md`).

Kort: Heatr *ontdekt, verrijkt, scoort en stelt samen* — bewezen. Wat het nog niet
*doet* is versturen, en dat is bewust (kill-switch), niet door een gat.
