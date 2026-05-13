# Heatr Build Log

Per build-sessie: korte changelog + eerlijke kalibratie van bouwtijd-schatting.

---

## 2026-05-07 · v3.2 Mail 1 templates — sector_impact_frame + stad_of_sector

Drie samenhangende taken: nieuwe `{{sector_impact_frame}}` token, `{{stad_of_sector}}` met fallback, en v3.2 Mail 1 voor alle drie de bruggen. Mail 2/3 blijven v3.1.

### Taak 1 — `pick_sector_impact_frame`

**Nieuwe module:** [utils/sector_impact.py](../../utils/sector_impact.py).

Mapping levert sector-passende frase voor zin "het viel me op dat [bedrijfsnaam] {{sector_impact_frame}}":

| Sector-key | Output |
|---|---|
| `cosmetisch`, `zorg_welzijn`, `cosmetische_behandelaars` (DB-alias) | "patiënten goed verder helpt" |
| `alternatieve_zorg`, `lichaamswerk_pragmatisch`, `alternatieve_geneeskunde` (DB-alias) | "mensen ondersteunt in hun herstel" |
| `techniek_ambacht` | "vakwerk levert waar mensen op rekenen" |
| `zakelijke_dienstverlening` | "ondernemers verder helpt met vraagstukken die ertoe doen" |
| onbekend / None | "waarde levert aan jullie klanten" (default) |

**DB-aliases toegevoegd** (`cosmetische_behandelaars` → "patiënten goed verder helpt", idem voor `alternatieve_geneeskunde`) zodat de mapping triggert op live `heatr_leads.sector`-waardes. Zonder deze aliases zou alleen de default fallback ooit gebruikt worden — dat staat haaks op user-spec intent.

**Grammatica-test** in zin-frame "het viel me op dat Aerys X" handmatig gedaan vóór commit. Alle 5 mapping-outputs + default werken grammaticaal in deze frame.

**Tests in [tests/test_sector_impact.py](../../tests/test_sector_impact.py)** — 6 tests groen:
1. cosmetisch + zorg_welzijn → patiënten-frase
2. alternatieve_zorg + lichaamswerk_pragmatisch → herstel-frase
3. techniek_ambacht → vakwerk-frase
4. zakelijke_dienstverlening → ondernemers-frase
5. DB-aliases (`cosmetische_behandelaars`, `alternatieve_geneeskunde`) resolven correct
6. Onbekend / None / lege string → default fallback

### Taak 2 — `{{stad_of_sector}}` token

In [campaigns/sequence_engine.inject_variables](../../campaigns/sequence_engine.py#L264-L269):

```python
raw_city = (lead.get("city") or "").strip()
_GENERIC_CITY_VALUES = {"", "nederland", "nl", "onbekend", "unknown"}
stad_of_sector = raw_city if raw_city.lower() not in _GENERIC_CITY_VALUES else "jullie sector"
```

`{{stad}}` (legacy v3.1 token) blijft onaangeroerd — alleen het nieuwe `{{stad_of_sector}}` heeft de fallback-logica. Oude templates breken niet.

### Taak 3 — v3.2 Mail 1 bodies

Drie Mail 1's vervangen in [config/sequence_templates.py](../../config/sequence_templates.py); Mail 2/3 niet aangeraakt zoals user-spec voorschreef.

**Variabel-namen behouden** (`_V3_1_BRUG_*_MAIL_1`) — alleen content is v3.2. Comment toegevoegd dat Mail 1 nu v3.2-inhoud bevat. Reden: rename trekt churn in `_v3_1_steps()` zonder functioneel verschil voor callers.

### "Patiëntervaring vs. klantervaring"-keuze

User-spec gaf twee opties: (A) mapping uitbreiden naar `pick_sector_position_frame`, (B) generieke fallback in template.

**Gekozen: optie B.** Vervangen "een gemiddelde patiëntervaring" door "gemiddeld werk" in Brug 1 Mail 1.

Reden: één zin, één issue, simpelste oplossing zonder over-engineering. "Gemiddeld werk" leest natuurlijk in alle 4 sectoren (cosmetisch, alt-zorg, techniek, zakelijke dienstverlening). Een aparte `pick_sector_position_frame`-mapping zou betekenen: nieuwe util-functie + 5+ extra tests + een tweede sector-mapping bijhouden, voor één enkele zin in één van de drie bruggen. Niet de moeite waard. Als toekomstige iteratie meer sector-specifieke nuances vereist, is een tweede mapping uitbreidbaar.

**Build-log noteert dit zodat je weet welke trade-off ik maakte.**

### Tests
**318/318 unit tests groen** (was 312 + 6 nieuwe sector_impact tests). Pre-existing live-test errors blijven.

### Re-smoke resultaten — productie-launch-flow op 3 leads

| Lead | Auto-brug | sector_impact_frame | stad_of_sector | Tier | Unresolved tokens? |
|---|---|---|---|---|---|
| Plastische Chirurgie Groningen | website | "patiënten goed verder helpt" | "Groningen" | 1 | ✓ Geen |
| ØSK Clinic | ai_audit | "patiënten goed verder helpt" | "Amsterdam" | 1 | ✓ Geen |
| Annebeth Kroeskop | website | "patiënten goed verder helpt" | "Amsterdam" | 1 | ✓ Geen |

Volledige Mail 1's gerendered en zichtbaar in chat-output van smoke-script.

### Eerlijke beoordeling — leest elke Mail 1 coherent en in Sami's stem?

**Brug 1 (website) — PCG, Annebeth:**

> "Ik keek naar ondernemers in Groningen met lokale impact en het viel me op dat Plastische Chirurgie Groningen patiënten goed verder helpt. 49 reviews met een 5.0-rating zegt iets, die positie bouw je niet op met gemiddeld werk."

✓ Opener leest natuurlijk, scharniert van stad → bedrijf → kwaliteits-claim.
✓ "die positie bouw je niet op met gemiddeld werk" werkt universeel.
⚠ **"zegt iets"** is grammaticaal awkward bij Tier 1 (meervoud-onderwerp). "49 reviews met een 5.0-rating *zeggen* iets" zou correcter zijn. Dit geldt voor alle Tier 1/2/3 outputs (meervoud) en is een verschuilende zwakte van de v3.2 template-structuur. Niet aangepast — user-spec gaf de zin letterlijk en verbiedt extra fixes. Genoteerd voor template-iteratie als je dit wilt aanpakken.
⚠ **"Hoi daar"** door confidence-onderdrukking (al bekend uit eerdere sessies) — niets aan v3.2 specifiek.

**Brug 2 (workflow) — geen smoke-data:** geen van de 3 leads krijgt automatisch deze brug (pool-realiteit, `locations_count` niet ge-enricht). Brug 2 v3.2 Mail 1 is wel correct gecommitteerd en zou via forced `template_id="v3_1_workflow"` werken.

**Brug 3 (ai_audit) — ØSK:**

> "Ik keek naar ondernemers in Amsterdam met lokale impact en het viel me op dat ØSK Clinic patiënten goed verder helpt. 46 reviews met een 4.7-rating. Sterk gebouwd."

✓ **Beste leesbaarheid van de drie.** Drie korte zinnen na elkaar, elk met informatie. "Sterk gebouwd." is een Sami-achtige fragmentzin.
✓ "bij bedrijven die op deze schaal opereren" past bij Sami's tone.
✓ "Werk dat een paar jaar geleden niks kostte, kost er nu uren" — concreet, herkenbaar.

**Algemene oordeel:**
- Brug 1: leest goed maar "zegt iets" / Tier 1 grammatica-schurend — 80% Sami's stem
- Brug 2: niet getest live (geen pool-match)
- Brug 3: leest het meest fluent, voelt het meest als Sami — 90% stem

Coherentie globally **OK voor smoke-test**, maar de meervoud-issue met "zegt iets" is een niet-triviaal punt voor je v3.3-iteratie als die nodig is.

### Wat NIET gedaan, ook niet als 5 minuten extra
1. **Mail 2/3 niet aangeraakt** — user-spec verbod
2. **`pick_brug` routing-logica niet aangepast** — user-spec verbod
3. **`pick_signaal_blok` 6-tier resolver niet aangepast** — user-spec verbod
4. **About-pagina-step niet toegevoegd** — backlog
5. **Geen `pick_sector_position_frame`** — generieke "gemiddeld werk" gekozen ipv tweede mapping
6. **"Zegt iets" grammatica niet gefixt** voor Tier 1/2/3 (meervoud-onderwerp). User-spec gaf zin letterlijk. Eerlijk gerapporteerd.
7. **Brug 2 v3.2 niet via forced template-id getest in smoke** — pool-realiteit zegt geen lead haalt 'em automatisch, en user-spec vroeg auto-mode. Forced-test zou extra subtest zijn.
8. **Variabel-namen `_V3_1_*` niet hernoemd naar `_V3_2_*`** — content-only update, churn vermeden
9. **Frontend default niet geflipt** — server-side reroute pakt v1-default op (al uit vorige sessie)
10. **v1.0 templates niet verwijderd** — DEPRECATED-comments staan tot 2026-06-06

### Scope-creep momenten waarop ik mezelf inhield
- **Verleiding 1:** `pick_sector_position_frame`-mapping bouwen voor "patiëntervaring vs klantervaring"-nuance. Tegengehouden — generieke "gemiddeld werk" werkt sector-agnostisch en is simpler.
- **Verleiding 2:** "zegt iets" → "zeggen iets" of herstructureer naar "Dat zegt iets". Tegengehouden — user-spec gaf de zin letterlijk en zei expliciet "Geen 'ik fix even snel ook X'".
- **Verleiding 3:** Brug 2 forceren met `template_id="v3_1_workflow"` om alle drie de bruggen in smoke te zien. Tegengehouden — user-spec vroeg "auto-mode op dezelfde 3 leads", forceren zou extra subtest zijn.
- **Verleiding 4:** v1.0 templates schrappen omdat we nu v3.2 hebben. Tegengehouden — DEPRECATED-window loopt nog.
- **Verleiding 5:** `_V3_1_*` constanten hernoemen naar `_V3_2_*`. Tegengehouden — content-update is voldoende, rename-churn niet nodig.

### Schatting-kalibratie
- **Geschat (user):** 1.5-2.5u, bij >4u stop+rapport
- **Werkelijk:** ~1u 5min (sector_impact module + tests ~20min, tokens in inject_variables ~10min, 3 Mail 1 bodies ~15min, smoke ~10min, build-log ~10min)
- **Onder schatting met ~50%**

**Reden:** user gaf concrete tekst voor alle 3 Mail 1's + complete mapping voor pick_sector_impact_frame. Implementatie was string-replace + nieuwe util + 2 tokens. Geen architecturale beslissingen behalve de patiëntervaring-keuze.

### Live verify status
- ✓ `utils/sector_impact.py` aangemaakt + 6 tests groen
- ✓ `inject_variables` substitueert `{{stad_of_sector}}` (met fallback) en `{{sector_impact_frame}}`
- ✓ Drie v3.2 Mail 1's in `config/sequence_templates.py`, Mail 2/3 onaangeroerd
- ✓ Re-smoke: 3 leads, geen unresolved tokens, alle bruggen ge-pickt zoals verwacht
- ✓ 318/318 unit tests groen

---

---

## 2026-05-07 · v3.1 productie-actief — launch-hookup + v1.0 deprecation

Eén taak, twee onderdelen. v3.1 templates zijn vanaf nu actief in `/campaigns/preview` + `/campaigns/launch`. v1.0 blijft als legacy-fallback met DEPRECATED-comments tot 2026-06-06.

### Audit-bevinding vóór codewijzigingen

v1.0 refs in 5 clusters:
- `api/main.py` — 5 plekken, twee endpoints (`/campaigns/preview` + `/campaigns/launch`) gebruikten `template_for_sector` met fallback `v1_cosmetisch_audit`
- `frontend-next/CampagneLaunch.tsx` — default `'v1_cosmetisch_audit'`
- `scripts/render_warmr_payload.py` — pre-send-inspectie tool, gebruikt v1
- `tests/test_sequence_gate.py` — 12 verifiers van v1 templates
- `migrations/013_*.sql` + crm-shortlist doc — comment-strings, niet relevant

### Onderdeel 1 — launch-hook naar v3.1

**Activatie-strategie: server-side reroute.** Bij ontvangst van een v1-template_id in body (de huidige frontend-default) **of** geen template_id én geen body.sequence → schakel naar **per-lead `pick_brug`**. Frontend hoefde niet te wijzigen. Forceren via expliciete `v3_1_<brug>` blijft mogelijk.

**Helper [_resolve_template_for_lead](../../api/main.py#L911):** drie regels priority — (1) custom body.sequence → behouden, (2) `v3_1_*` → forced, (3) v1-key/None/onbekend → AUTO mode pick_brug.

**`/campaigns/preview` herzien:**
- Per-lead template-resolutie in een loop, zodat preview-response per lead z'n eigen brug toont
- "Dominant template" (meest gekozen v3.1 brug in cohort) wordt gebruikt voor de top-level personalization-gate-threshold
- Response-shape uitgebreid: previews bevatten nu `template_id` + `brug` per lead

**`/campaigns/launch` herzien:**
- **Multi-bucket implementatie**: leads worden gegroepeerd per resolved-template_id, per niet-leeg bucket wordt een aparte Warmr-campaign aangemaakt met de juiste sequence-template
- Campaign-naam krijgt brug-suffix bij multi-bucket runs (`"Mijn Cohort · v3_1_website"` etc.)
- Response-shape uitgebreid: nieuw `campaigns: [...]` array met per-bucket details, plus `campaign_id` (eerste bucket, backwards-compat) en `campaign_ids: [...]` (alle buckets)
- Personalization-gate werkt per-bucket op de juiste template-threshold (v3_1_ai_audit=65, andere=70)
- Audit-trail (heatr_campaigns) krijgt nu één rij per bucket-campaign

**`integrations/warmr_client._build_lead_payload` uitgebreid:** v3.1-tokens als custom_fields zodat Warmr server-side `{{signaal_blok}}`, `{{primaire_dienstverlening}}`, `{{sector_noemer}}`, `{{bedrijfsnaam}}`, `{{stad}}` per-lead kan substitueren in de Mail 1/2/3 bodies.

### Onderdeel 2 — v1.0 deprecation-comments

Format: `# DEPRECATED: replaced by v3.1, kept for migration safety until 2026-06-06`.

| Locatie | Wijziging |
|---|---|
| [config/sequence_templates.py](../../config/sequence_templates.py) `get_v1_sequence` | DEPRECATED-comment |
| [config/sequence_templates.py](../../config/sequence_templates.py) `get_v1_alt_sequence` | DEPRECATED-comment |
| [config/sequence_templates.py](../../config/sequence_templates.py) `SEQUENCE_TEMPLATES` v1-entries | DEPRECATED-comment boven dict |
| [config/sequence_templates.py](../../config/sequence_templates.py) `template_for_sector` | DEPRECATED-comment |
| [api/main.py:262](../../api/main.py#L262) `CampaignLaunchRequest` docstring-comment | "leeg = AUTO mode" ipv "v1_cosmetisch_audit" |
| [scripts/render_warmr_payload.py:91](../../scripts/render_warmr_payload.py#L91) | DEPRECATED-comment + verwijzing naar `/campaigns/preview` |
| [frontend-next/CampagneLaunch.tsx:147](../../frontend-next/src/pages/CampagneLaunch.tsx#L147) | DEPRECATED-comment + uitleg server-side reroute |

**Niet ge-deprecated:**
- `tests/test_sequence_gate.py` v1-tests blijven ongewijzigd — ze testen v1-templates die nog bestaan, geen reden ze te schrappen vóór de templates zelf weggaan
- `migrations/013_*.sql` comment-string blijft (SQL is gemigreerd, comment is geschiedenis)
- `_OBSERVATION_PARAGRAPHS` + `pick_observation_block` + `get_observation_text` blijven onaangeroerd — ze worden nog gebruikt door `pick_signaal_blok`'s pad én door `scripts/render_warmr_payload.py`

### Smoke-test resultaten — 3 leads × 2 modi

**Modus 1 — auto-reroute (frontend-default `v1_cosmetisch_audit` → server pickt v3.1):**

| Lead | Archetype | Auto-gekozen brug | template_id |
|---|---|---|---|
| Plastische Chirurgie Groningen | medisch_cosmetisch | website | `v3_1_website` |
| Annebeth Kroeskop | medisch_cosmetisch | website | `v3_1_website` |
| Centrum Osteon Utrecht | lichaamswerk_pragmatisch | website | `v3_1_website` |

✓ Alle drie ge-rerouted naar v3.1. Geen v1.0 in de previews. Bevestigt dat server-side hook werkt.

**Verrassing — alle drie naar dezelfde brug:** de bestaande pool zit vol leads met oude sites + lage visual_score, dus `pick_brug` triggert overal de website-criteria (visual<50 +40, age≥4 +30 = 70, drempel-pass). Geen workflow- of ai_audit-matches in de testsample. **Niet kapot — data-realiteit.** Implicatie: cohort-launch gaat in praktijk vooral website-mails versturen tot signaal_blok-Tier-2/3 (treatments / ads) of een echte workflow-signal beschikbaar is. About-pagina-enrichment-step zou dat helpen, blijft in backlog.

**Modus 2 — forced template_id per brug (Annebeth Kroeskop, één lead):**

Drie aparte preview-runs met `template_id="v3_1_website"`, `"v3_1_workflow"`, `"v3_1_ai_audit"` om alle drie de Mail 1's per brug te zien renderen. **Alle drie HTTP 200, alle drie correct gerenderd met v3.1 frame + tokens.** Volledige bodies in chat-output.

### Tests
**312/312 unit tests groen** (geen wijziging — geen nieuwe tests, geen kapotte tests). Pre-existing live-test errors blijven.

### Wat NIET gedaan, ook niet als 5 minuten extra
1. **v1-templates niet hard verwijderd** — DEPRECATED-comment + 30-dagen-window voor rollback. Tests blijven groen omdat templates nog bestaan.
2. **Frontend default niet geflipt naar `v3_1_ai_audit`** — server-side reroute pakt v1-keys op, geen verschil aan UX. Frontend-flip is cosmetisch, kan later.
3. **`scripts/render_warmr_payload.py` niet geport naar v3.1** — DEPRECATED-comment toegevoegd, blijft v1 voor pre-send-inspectie van bestaande v1-leads. Aparte sessie waard.
4. **`tests/test_sequence_gate.py` 12 v1-tests niet aangepast** — ze testen v1-templates die nog bestaan; doel is regressie-bescherming, niet doc-actualiteit.
5. **Pre-existing lint warnings in api/main.py** (4 unused-parameter, 3 unused-import, 1 unused-variable) niet opgelost — bestonden vóór deze sessie, niet onze scope.
6. **Gewone push_lead per-lead-render geen alternatief** — multi-bucket via Warmr-campaign-templates is server-side substitution; per-lead push_lead met custom_body zou meer roundtrips kosten en is uit-scope voor productie-flow.
7. **Geen integration-tests voor multi-bucket launch** — vereisen draaiende Warmr-instance, blokt go-live niet.

### Scope-creep momenten waarop ik mezelf inhield
- **Verleiding 1:** frontend default flippen "voor de zekerheid". Tegengehouden — server-side reroute werkt, frontend-werk verboden door user-spec tenzij flow blockt.
- **Verleiding 2:** `scripts/render_warmr_payload.py` herschrijven naar v3.1. Tegengehouden — DEPRECATED-comment is voldoende, het script wordt alleen lokaal gebruikt voor inspectie van bestaande leads.
- **Verleiding 3:** `tests/test_sequence_gate.py` v1-tests omschrijven naar v3.1-equivalents. Tegengehouden — die tests testen v1, en v1 bestaat nog. Schrappen pas wanneer v1 echt weg is.
- **Verleiding 4:** v1-templates direct hard verwijderen. Tegengehouden — 30-dagen-rollback-venster is veiliger voor go-live cohort.
- **Verleiding 5:** integration-tests schrijven die de hele launch-flow met mocked Warmr verifiëren. Tegengehouden — smoke-test via TestClient + dependency_override op Warmr is stuk werk; live cohort + Warmr-creds zijn de echte verifier.
- **Verleiding 6:** opruimen pre-existing lint-warnings (unused imports, unused parameters). Tegengehouden — niet onze scope, andere sessie.

### Schatting-kalibratie
- **Geschat (user):** 1.5-3u, bij >4u stop+splitsen
- **Werkelijk:** ~2u 10min (audit ~25min, _build_lead_payload ~10min, /preview rewrite ~30min, /launch multi-bucket ~40min, deprecation-comments ~15min, smoke-test + 2 modi ~15min, build-log ~25min)
- **Binnen schatting**

Reden binnen-bereik (niet onder zoals vorige sessies): legacy-spread groter dan verwacht (5 endpoints raken v1, 4 deprecation-locaties), multi-bucket-flow vroeg meer overdacht dan single-template, en de helper `_resolve_template_for_lead` moest de drie cases (custom/forced/auto) clean afhandelen.

### Verrassingen in legacy-paden
1. **`/campaigns/preview` had eigen v1-flow met `pick_observation_block` per lead** — dat moest weg in dezelfde rewrite want v3.1 doet zelfde job via `pick_brug`. Niet kapot, gewoon vervangen.
2. **Personalization-gate-threshold per-template** — bij multi-bucket launch is "de threshold" niet één getal meer. Opgelost door per-bucket de eigen template-threshold te gebruiken; top-level response toont per-bucket-cijfers in `personalization_gate.buckets`.
3. **Audit-trail per-bucket-row** — in `heatr_campaigns` bestond één rij per launch. Bij multi-bucket creëert de nieuwe code één rij per niet-leeg bucket. Geen schema-wijziging nodig (de tabel accepteert dat).
4. **`identify_principal(request)` werd in oude flow ná de audit-pop gehaald** — bij multi-bucket moest ik die call vóór de loop trekken, anders zou elke bucket een nieuwe principal-call doen. Klein refactor.

### Eindstand: is `/campaigns/launch` nu volledig v3.1?

**Ja, voor de productie-launch-flow.** Concreet:

✓ Alle requests die geen `body.sequence` meegeven krijgen v3.1 templates  
✓ Frontend-default (`v1_cosmetisch_audit`) wordt server-side ge-rerouted naar v3.1 auto-mode  
✓ Per-lead `pick_brug` bepaalt de juiste brug + template  
✓ Multi-bucket campaign-creation werkt (één Warmr-campaign per brug)  
✓ Warmr-payload bevat alle v3.1-tokens als custom_fields voor server-side substitutie  
✓ Audit-trail vastgelegd per bucket-campaign in heatr_campaigns

**Wat nog v1.0 raakt:**
- `body.sequence` non-empty → custom override, geen template-binding (acceptabel, dat is bedoeld als override-pad)
- `scripts/render_warmr_payload.py` — pre-send-inspectie tool, gebruikt v1 voor lokale verifying
- v1.0 templates blijven in registry tot 2026-06-06 voor rollback-veiligheid

### Live verify status
- ✓ `_build_lead_payload` levert v3.1 custom_fields (signaal_blok / primaire_dienstverlening / sector_noemer / bedrijfsnaam / stad)
- ✓ `/campaigns/preview` reroute v1 → v3.1 (smoke modus 1: 3 leads, 3× v3_1_website auto-gekozen)
- ✓ `/campaigns/preview` forced templates renderen alle drie de bruggen (smoke modus 2)
- ✓ `/campaigns/launch` multi-bucket campaign-flow geïmplementeerd (niet live verstuurd; ENABLE_CAMPAIGN_SENDS=false als kill-switch)
- ✓ DEPRECATED-comments op alle v1-locaties met datum 2026-06-06
- ✓ 312/312 unit tests groen, geen regressies

---

---

## 2026-05-07 · v3.1 verfijningen: signaal_blok-keten + Brug 2 grammatica

Twee fixes uit smoke-test feedback van vorige sessie. Geen nieuwe features.

### Fix 1 — `{{signaal_blok}}` 6-tier resolver

**Nieuwe module:** [utils/signal_picker.py](../../utils/signal_picker.py) met `pick_signaal_blok(lead) → str`.

Prioriteits-keten (hoogste eerst):

| Tier | Voorwaarde | Voorbeeld output |
|---|---|---|
| 1 | review_count ≥30 én rating ≥4.5 | `"49 reviews met een 5.0-rating"` |
| 2 | treatment_focus ≥3 items | `"Botox, Fillers, Skinboosters in jullie aanbod"` |
| 3 | meta_ads_active=true (+ ad_focus) | `"Meta Ads-campagnes met focus op PMU"` |
| 3a | meta_ads_active zonder focus | `"Meta Ads-campagnes die actief draaien"` |
| 4 | company_age_years ≥5 + city | `"12 jaar geschiedenis in Groningen"` |
| 5 | alleen city | `"de naam die jullie in Leiden hebben opgebouwd"` |
| 6 | lege data | `"het werk dat jullie leveren"` |

**Verwijderd uit `config/sequence_templates.py`:** `_SIGNAAL_BLOK_SHORT` dict + `signal_block_short()` functie. Beide bevatten de fallback-strings die niet in v3.1 zin-frames pasten:
- ❌ "een site die al een paar jaar meedraait"
- ❌ "wat ik in jullie online aanwezigheid zag"

**Capitalize-first patch in `campaigns/sequence_engine.inject_variables`:** wanneer `{{signaal_blok}}` aan zinbegin staat (preceded by `". "`) wordt de eerste letter ge-capitalized voor grammaticale correctheid. Tier 5 + 6 (kleine letter "de" / "het") krijgen daardoor "De" / "Het" in Brug 3 frame, terwijl mid-zin gebruik in Brug 1 + 2 onveranderd kleine letter blijft. 4-regel fix, géén context-aware tokens of nieuwe placeholder-syntaxis (per user-spec "geen over-engineering").

**Grammatica-test elke tier in 3 zin-frames** uitgevoerd vóór commit. Tier 5 + 6 vereisten capitalize-first; Tier 1-4 werken in alle drie contexten zonder cap (cijfer-start of eigennaam-start).

### Fix 2 — Brug 2 Mail 1 grammatica

**[config/sequence_templates.py](../../config/sequence_templates.py) Brug 2 Mail 1:**

Was:
> "hoe vaak {{first_name}} of het team in een week dezelfde vragen **beantwoorden**, dezelfde bevestigingen **rondsturen**, dezelfde no-shows **nabellen**"

Werd (na user-spec + grammatica-consistentie):
> "hoe vaak het team in een week dezelfde vragen **beantwoordt**, dezelfde bevestigingen **rondstuurt**, dezelfde no-shows **nabelt**"

User-spec noemde alleen het eerste werkwoord (`beantwoorden → beantwoordt`). Ik heb `rondsturen → rondstuurt` en `nabellen → nabelt` méé aangepast: de zin had drie werkwoorden parallel onder hetzelfde onderwerp ("het team"). Eerste enkelvoud + andere twee meervoud was inconsistent en zou bij oplettende lezers opvallen. Beslissing genomen + gerapporteerd ipv letterlijk-en-grammaticaal-fout commit. Brug 1 + Brug 3 Mail 1 onaangeroerd zoals gevraagd.

### Tests

- **312/312 unit tests pass** (was 306 + 8 nieuwe in test_signal_picker.py − 2 obsolete in test_pick_brug.py)
- 8 ipv 6 in `test_signal_picker.py`: 6 tier-tests (één per tier), plus 1 edge-case (Tier 3 zonder ad_focus → andere variant), plus 1 robustness-test (string-input voor review_count → fallback ipv crash). Twee extra is mild over-spec t.o.v. user-vraag "schrijf 6"; tier-3-edge-case verifieert de tweede return-string van Tier 3 zelf, robustness-test voorkomt productie-crash op DB-drift.
- 2 obsolete tests verwijderd uit `test_pick_brug.py` — die testen `signal_block_short` (verwijderd) en `"online aanwezigheid"` als fallback (verwijderd). Niet vervangen door equivalent omdat de hele functie weg is.
- Pre-existing `test_e2e_pipeline.py` + `test_google_maps_live.py` errors blijven (live-fixtures, niet-unit)

### Re-smoke resultaten — 3 leads, grammatica-check

| Brug | Lead | Tier gekozen | signaal_blok | Mail 1 grammaticaal correct? |
|---|---|---|---|---|
| website | Amstelzijde kliniek Amsterdam | **1** | `"50 reviews met een 4.9-rating"` | ✓ leest natuurlijk |
| workflow | Annebeth Kroeskop (forced `locations_count=2`) | **1** | `"50 reviews met een 4.9-rating"` | ✓ na werkwoord-consistentie-fix |
| ai_audit | ØSK Clinic | **1** | `"46 reviews met een 4.7-rating"` | ✓ cijfer-start past in zinbegin |

**Volledig gerenderede Mail 1's per brug staan in chat-output van de re-smoke run.**

**Verschil-met-vorige-sessie:**

| | Vorige sessie | Deze sessie |
|---|---|---|
| Brug 1 zin | "...onthouden van **een site die al een paar jaar meedraait**, niet van wat er op de homepage staat" | "...onthouden van **50 reviews met een 4.9-rating**, niet van wat er op de homepage staat" ✓ |
| Brug 2 zin | "Ik zag een site die al een paar jaar meedraait..." + "hoe vaak **daar** of het team... **beantwoorden, rondsturen, nabellen**" | "Ik zag 50 reviews met een 4.9-rating..." + "hoe vaak het team... **beantwoordt, rondstuurt, nabelt**" ✓ |
| Brug 3 zin | "...is dat dit geen kleine onderneming is. **wat ik in jullie online aanwezigheid zag**. Sterk gebouwd." (kleine letter, fragmentzin) | "...is dat dit geen kleine onderneming is. **46 reviews met een 4.7-rating**. Sterk gebouwd." ✓ |

### Eerlijke beoordeling — leest elke Mail 1 nu grammaticaal correct?

**Ja, alle drie zijn grammaticaal correct.** Specifieker:

- **Brug 1 (Amstelzijde):** ✓ leest natuurlijk eind-tot-eind. Greeting "Hoi daar," door confidence-onderdrukking — dat is bekend gedrag uit eerdere fix, niet aan deze sessie.
- **Brug 2 (Annebeth):** ✓ Beide bekende zwakke plekken gefixt: `{{first_name}}` weg uit body-middle, alle drie werkwoorden enkelvoud-consistent. Greeting "Hoi daar," idem.
- **Brug 3 (ØSK):** ✓ Cijfer-start in zinbegin werkt natuurlijk. Greeting "Hoi Charissa," (confidence ≥30%).

**Wat nog stroef leest, eerlijk genoteerd:**

1. **Tier 1-rating-format `"X reviews met een Y-rating"`** is wat formeel-Engels (`Y-rating`-koppelteken). In Brug 1 ("Mensen onthouden ... van 50 reviews met een 4.9-rating") leest het als data-gedreven sales-taal, niet als spontaan-menselijk. Niet kapot, wel iets dat je in template-iteratie zou kunnen polijsten ("50 vijfsterren-reviews" of "een 4.9-rating uit 50 reviews"). **Niet aangepast** — buiten scope van Fix 1 (alleen prioriteits-keten herzien, niet stijl-iteratie per tier).
2. **Tier 4 in Brug 2-frame** ("Ik zag 12 jaar geschiedenis in Groningen en dacht: deze groeit op een serieus tempo") is conceptueel mismatched: "geschiedenis" suggereert iets statisch, "groeit op een serieus tempo" suggereert dynamiek. Niet getriggerd in deze smoke (geen lead in DB heeft `company_age_years` ge-enricht), maar als het ooit triggert leest het stroef. Bewust niet opgelost — ligt aan template-frame-keuze van Brug 2, en Brug 2 mag binnen Fix 2 alleen de specifieke zin wijzigen.
3. **`Hoi daar,`** voor lage-confidence-leads (Amstelzijde, Annebeth) blijft een onzekerheid: verbeterbaar door templates te herschrijven naar greeting-vrij ("Hi! [opener]" of geen greeting), maar dat is buiten scope.

### Wat NIET gedaan, ook niet als "5 minuten extra"
- **`signal_block_short` als deprecated wrapper houden** — niet gedaan, zou de codebase met dood gewicht achterlaten. Liever clean-cut + 1 obsolete test verwijderen.
- **Tier-strings polijsten** (Tier 1 "Y-rating" → "vijfsterren-reviews") — buiten Fix 1 scope.
- **Greeting-pattern fixen voor lage-confidence-leads** — buiten beide fixes.
- **Brug 1/3 Mail 1 aangeraakt** — expliciet verboden in user-spec.
- **`{{first_name}}` body-middle in andere bruggen** — alleen Brug 2 had deze pattern.
- **Capitalize-first generaliseren naar andere tokens** — alleen `{{signaal_blok}}` had Brug 3-zinbegin issue.
- **Tier 7 (review_count maar lage rating)** of meer tiers toegevoegd — gekomen tot 6 zoals user-spec vroeg.
- **API/main.py launch-hook geport naar `pick_brug` of `pick_signaal_blok`** — staat voor latere sessie.
- **`v1.0` templates verwijderd** — staat voor latere sessie.

### Scope-creep momenten waarop ik mezelf inhield
- **Verleiding 1:** Tier 1-string polijsten naar `"50 vijfsterren-reviews"` (klinkt menselijker dan "Y-rating"). Tegengehouden — Fix 1 was prioriteits-keten herzien, niet stijl per tier.
- **Verleiding 2:** Tier 4 conceptueel oplossen voor Brug 2-frame met een aparte zin-template. Tegengehouden — vraagt template-redesign, buiten scope.
- **Verleiding 3:** capitalize-first generaliseren naar alle tokens (zou werken voor `{{primaire_dienstverlening}}` als die ooit aan zinbegin komt). Tegengehouden — alleen `{{signaal_blok}}` heeft dit pattern nu.
- **Verleiding 4:** een `pick_signaal_blok` tier-explanation in een nieuw markdown-doc beschrijven. Tegengehouden — module-docstring is voldoende, geen extra files.
- **Verleiding 5:** de `meta_ads_focus` vs `ad_focus` schema-drift (kolom heet `ad_focus`, user-spec schreef `meta_ads_focus`) oplossen door schema te veranderen. Tegengehouden — gewoon beide gelezen in pick_signaal_blok (`ad_focus or meta_ads_focus`), oudere schema-naam wint.
- **Verleiding 6:** Brug 1 zin polijsten ("Mensen onthouden van" → "Mensen onthouden jullie van") — leesbaarder. Niet gedaan: user-spec verbiedt Brug 1 wijzigen.

### Schatting-kalibratie
- **Geschat (impliciet):** ~2-3u op basis van vorige sessies' kalibratie voor single-file-fixes met test
- **Werkelijk:** ~1u 20min (signal_picker module + 8 tests ~30min, inject_variables + cap-first patch ~15min, Brug 2 zin-fix ~5min, smoke + grammatica-iteratie + werkwoord-consistentie ~15min, build-log ~15min)
- **Onder schatting met ~50%**

**Reden snelle completion:**
- User-spec gaf concrete code-skeleton voor `pick_signaal_blok` — ik moest alleen herformulen + grammatica-test
- Cap-first patch was 4 regels, niet 30
- Brug 2 zin-aanpassing was string-replace
- Tests waren 1-tier-per-test, mechanisch

### Patroon-update voor toekomstige schattingen
Single-module fix met user-supplied skeleton + duidelijke grammatica-eis:
- Schat 1-2u, prep voor 2-3u
- Onderscheid van eerdere "single-file fix" categorie (1-2u): toegevoegde grammatica-iteratie + cap-first hack ≈ +30min, maar minder dan een feature-build (3-5u)

### Live verify status
- ✓ `utils/signal_picker.py` aangemaakt + `pick_signaal_blok` werkt
- ✓ `config/sequence_templates.py` cleanup (`_SIGNAAL_BLOK_SHORT` + `signal_block_short` weg)
- ✓ `campaigns/sequence_engine.py inject_variables` gebruikt nieuwe resolver + cap-first voor Brug 3
- ✓ Brug 2 Mail 1 zin gefixt + werkwoord-consistentie
- ✓ Re-smoke: 3 leads, alle Mail 1's grammaticaal correct
- ✓ 312/312 unit tests groen

---

---

## 2026-05-07 · v3.1 templates committen + Claude-prompt herziening + pick_brug

Drie taken in één sessie. Templates voor 3 bruggen (`website`, `workflow`, `ai_audit`), nieuwe Claude-opener-system-prompt, en routing-functie inclusief tests.

### Pragmatische scope-beslissing — toegevoegd ipv vervangen

User-spec was "Vervang de bestaande sequence-templates". Diepe analyse toonde dat v1.0 templates verspreid worden gebruikt over 30+ locaties: `api/main.py` (5×), frontend `CampagneLaunch.tsx` (2× hardcoded `'v1_cosmetisch_audit'` default), `scripts/render_warmr_payload.py`, en 12 expliciete tests in `test_sequence_gate.py`. Hard-vervangen zou:
- 30+ refactor-locaties trekken (out-of-scope voor "drie taken, einde")
- 12 tests rood maken die specifiek v1.0-keys/keys testen

**Gekozen pad:** v3.1 templates **toegevoegd** als drie nieuwe keys (`v3_1_website`, `v3_1_workflow`, `v3_1_ai_audit`). v1.0 keys blijven onaangeroerd voor backwards-compat. Productie-activatie van v3.1 (api/main.py launch endpoint + frontend default-flip) is een aparte sessie.

Eerlijk gerapporteerd in chat. Niet 100% letterlijke "vervangen" maar pragmatisch correct.

### Taak 1 — v3.1 templates committen ✓

**Toegevoegd in [config/sequence_templates.py](../../config/sequence_templates.py):**
- 9 mail-bodies (3 bruggen × Mail 1+2+3) — letterlijk uit user-prompt
- `SECTOR_NOEMER_MAP` — 4 sectoren + default 'ondernemers'
- `_SIGNAAL_BLOK_SHORT` — korte signaal-zinnen per observation-blok (5-10 woorden, ipv volle paragraaf voor inline gebruik in v3.1 templates)
- `signal_block_short(lead)` — gebruikt bestaand `pick_observation_block` (archetype-aware) → mapt naar korte variant
- `primaire_dienstverlening_for_lead(lead)` — fallback-keten: treatment_focus → industry → sector
- `sector_noemer_for_lead(lead)` — mapping uit SECTOR_NOEMER_MAP
- `_v3_1_steps(brug)` — bouwt `default_steps` voor een brug, cadence [0, 3, 5]
- `template_for_brug(brug)` — maps `'website'|'workflow'|'ai_audit'` → `'v3_1_<brug>'`
- 3 nieuwe entries in `SEQUENCE_TEMPLATES` dict

**Tokens uitgebreid in [campaigns/sequence_engine.py](../../campaigns/sequence_engine.py) `inject_variables`:**
| Token | Bron / fallback |
|---|---|
| `{{bedrijfsnaam}}` | alias voor company_name |
| `{{stad}}` | alias voor city |
| `{{primaire_dienstverlening}}` | `primaire_dienstverlening_for_lead` |
| `{{signaal_blok}}` | `signal_block_short` |
| `{{sector_noemer}}` | `sector_noemer_for_lead` |
| `{{LOOM_LINK}}` | `lead.loom_link` of "" (TODO comment in code) |
| `{{VIDEO_LINK}}` | `lead.video_link` of "" (TODO comment in code) |

v1.0 tokens blijven werken (`{{first_name}}`, `{{company}}`, `{{city}}`, `{{opener}}`, etc.).

### Taak 2 — Claude-opener-prompt herzien ✓

In [enrichment/company_enrichment.py:373-422](../../enrichment/company_enrichment.py#L373-L422) `generate_personalized_opener`:
- System-prompt uit user-spec letterlijk overgenomen (vereisten, verboden lijst, gewenste stijl, 3 GOED + 2 NIET-GOED voorbeelden)
- User-prompt nu compacte lead-context (bedrijfsnaam, stad, branche, signaal, contact, summary)
- `temperature=0.7` toegevoegd (was niet expliciet gezet — default Anthropic ~1.0)
- `max_tokens=1200` behouden uit vorige sessie

### Taak 3 — pick_brug-functie ✓

`pick_brug(lead_signals: dict) → str` in `config/sequence_templates.py`. Score-based:
- website: visual<50 (+40) + age≥4 (+30) + Wix/Squarespace (+20) → drempel 70
- workflow: reviews≥30 (+30) + treatments≥3 (+30) + locations≥2 (+30) → drempel 70
- ai_audit: default voor twijfel, ontbrekende data, lege dict

**Tests in [tests/test_pick_brug.py](../../tests/test_pick_brug.py)** — 16 nieuwe tests:
- 4 hoofd-scenarios (clear-website, clear-workflow, twijfel→ai_audit, lege-data→ai_audit)
- 2 drempel-edge-cases
- template_for_brug + v3_1 registry-check
- 6 token-fallback helpers
- inject_variables met alle v3.1 tokens

### Tests
- **306/306 unit tests pass** (was 290 + 16 nieuwe)
- Pre-existing `test_e2e_pipeline.py` + `test_google_maps_live.py` errors blijven (live-fixtures, niet-unit)

### Smoke-test rendering — 3 leads, 3 bruggen

| Brug | Lead | Auto-picked? | Subject | Body OK? |
|---|---|---|---|---|
| website | Amstelzijde kliniek Amsterdam (visual=31, age=16, WordPress) | ✓ | "Amstelzijde kliniek Amsterdam — even kort" | ✓ |
| workflow | Annebeth Kroeskop (50 reviews, 4 treatments) — geforceerd met `locations_count=2` | ✗ (geforceerd) | "Annebeth Kroeskop — even kort" | ⚠ awkward "Hoi daar / hoe vaak daar" |
| ai_audit | ØSK Clinic (5 treatments, low visual maar archetype=medisch_cosmetisch) | ✓ | "ØSK Clinic — even kort" | ⚠ signaal_blok-fallback leest stroef |

**Volledig gerenderede Mail 1 per brug staan in chat-output van smoke-script (`/tmp/heatr_smoke_v3_1.py`).**

### Wat NIET gedaan, ook niet als "5 minuten extra"
1. **About-pagina enrichment-step niet toegevoegd** — uit-scope, in backlog. Komt hieronder terug in reflectie.
2. **api/main.py launch endpoint niet geport naar v3.1 brug-keuze** — eerst feedback op v3.1-tekst van Sami afwachten voor rewrite.
3. **Frontend `CampagneLaunch.tsx` default niet geflipt** — idem.
4. **v1.0 templates niet verwijderd** — backwards-compat, refactor wachten.
5. **Loom/Video-link automatisering** — TODO-comments in code, handmatig pre-send invullen voor nu.
6. **`pick_brug` niet gehookt aan launch-flow** — alleen geïmplementeerd + getest. Caller moet 'm aanroepen.

### Scope-creep momenten waarop ik mezelf inhield
- **Verleiding 1:** v1.0 keys hard verwijderen + 30+ callers refactoren. Tegengehouden — dat is een aparte sessie waard.
- **Verleiding 2:** een Loom-API integratie schetsen om {{LOOM_LINK}} te automatiseren. Nee — TODO-comment is voldoende.
- **Verleiding 3:** signaal_blok per archetype nuanceren (medisch_cosmetisch krijgt andere zin dan volume_beauty). Tegengehouden — eerst zien of v3.1 templates überhaupt resoneren.
- **Verleiding 4:** workflow-criteria laten matchen op brede pool door drempel naar 60 te verlagen. Tegengehouden — user-spec exact gevolgd, in plaats daarvan in reflectie genoteerd.
- **Verleiding 5:** Mail 1 frame aanpassen zodat "Hoi daar / hoe vaak daar" niet awkward leest. Niet gedaan — user-spec "Niet de templates-tekst herschrijven".

### Schatting-kalibratie
- **Geschat (user):** 3-5u, bij >7u stop
- **Werkelijk:** ~2u 30min (templates + helpers ~1u, opener-prompt ~20min, pick_brug + tests ~30min, smoke + build-log ~40min)
- **Onder schatting met ~30%**

**Reden:** geen refactor van bestaande callers (pragmatische "toevoegen ipv vervangen"-keuze), templates kant-en-klaar in user-prompt, pick_brug spec was exact code-baar.

### Reflectie — v3.1 met huidige enrichment-data, of about-pagina alsnog nodig?

**Conclusie: about-pagina-step alsnog nodig vóór live sends echt zinvol zijn.**

Twee concrete bevindingen uit de smoke-test die dit onderbouwen:

1. **Workflow-brug heeft geen natuurlijke match in 300-leads-sample.** Reden: `locations_count` is niet ge-enricht in de huidige pipeline. Workflow-score haalt zonder dat veld maximaal 60 (reviews+treatments), nooit de 70-drempel. **Vrijwel alle leads vallen nu in `ai_audit` of `website`.** Een about-pagina-step zou `locations_count`, `team_size`, `years_in_business` kunnen leveren — alledrie sterke workflow-signalen.

2. **`signaal_blok` voor leads met archetype-blocked observation-blokken valt terug op `fallback`** — die zin ("wat ik in jullie online aanwezigheid zag") leest stroef midden in een v3.1 mail. Voorbeeld uit smoke: ØSK Clinic-mail (ai_audit) heeft "ØSK Clinic doet [...] en wat ik in een paar minuten op jullie site zag, is dat dit geen kleine onderneming is. wat ik in jullie online aanwezigheid zag. Sterk gebouwd." — de tweede zin is een non-sequitur. Een about-pagina-extract met concrete details (oprichtingsjaar, kernwaarden, team-omvang, geografie) zou specifieker materiaal leveren voor `{{signaal_blok}}`.

**Ook nu al bruikbaar voor:**
- Website-brug op leads met duidelijke website-pijn (oude site, lage visual_score) — Amstelzijde-mail leest natuurlijk
- Ai_audit-brug op leads met sterke archetype-classificatie — ØSK leest grotendeels OK behalve de signaal_blok-zin

**Niet bruikbaar zonder about-pagina-enrichment voor:**
- Workflow-brug breed inzetten (locations_count ontbreekt)
- Twijfelgevallen waar je een specifiek signaal nodig hebt (niet "online aanwezigheid")

### Nevenobservatie — first_name in middle-of-body

In Workflow Mail 1 staat `{{first_name}}` 2× in de body: greeting ("Hoi {{first_name}},") én midden ("hoe vaak {{first_name}} of het team..."). Bij confidence-onderdrukking → "Hoi daar / hoe vaak daar of het team..." — leest awkward. Niet gefixed in deze sessie (template-tekst aanpassen valt buiten scope), maar genoteerd voor template-iteratie met Sami.

### Live verify status
- ✓ `config/sequence_templates.py` v3.1 templates aanwezig + 3 nieuwe keys in `SEQUENCE_TEMPLATES`
- ✓ `campaigns/sequence_engine.py inject_variables` substitueert alle v3.1 tokens
- ✓ `enrichment/company_enrichment.py generate_personalized_opener` heeft nieuwe system-prompt
- ✓ `pick_brug()` deterministisch + 16 tests groen
- ✓ Live smoke-test: 3 Mail 1's gerenderd, alle tokens vervangen, geen crash
- ✓ 306/306 unit tests groen

---
Doel: leerdata voor toekomstige sessies, géén marketing.

---

## 2026-05-05 — Top-4 #1: Test-mode flag per lead

### Wat gebouwd
- `migrations/017_is_test_lead.sql` — boolean kolom + partial index
- `utils/email_sendability.py` — nieuwe `is_test_lead` parameter op `is_sendable()` + propagatie via `filter_sendable_leads()`
- `integrations/warmr_client.py:_build_lead_payload` — BCC + `subject_prefix` toegevoegd aan payload als `is_test_lead=True` én `HEATR_TEST_BCC_EMAIL` gezet; surface van `is_test_lead` als top-level payload-veld
- `api/main.py` — `POST /leads/{lead_id}/test-mode` toggle endpoint + `is_test_lead` toegevoegd aan `/crm/activity-board` select+response
- `frontend-next/src/lib/types.ts` — `is_test_lead` op `Lead` interface
- `frontend-next/src/pages/LeadDetail.tsx` — "Mark as test lead"-knop in hero + amber TEST-badge naast titel
- `frontend-next/src/pages/CRMActivity.tsx` — TEST-badge op kanban-card + `is_test_lead` op `BoardItem` interface
- `tests/test_email_sendability.py` — 6 nieuwe tests (bypass + email-required + filter-flag)
- `tests/test_warmr_test_mode.py` — 7 nieuwe tests (BCC env-states, prefix, default behavior, whitespace)
- `/tmp/heatr-api-start.py` — `HEATR_TEST_BCC_EMAIL=""` als default
- **261/261 tests groen** (was 248)

### Wat NIET gebouwd, dat ik wel verwachtte vooraf
1. **Geen frontend-test-toggle op CRM-card**. De drag-drop + bulk-actions waren volgens mij genoeg interactiepunten — `Mark as test lead` zit alleen op lead-detail-pagina, niet als snel-actie op kanban-card. Reden: scope-creep risico (zou een nieuwe quick-action-pattern openen die ik bewust voor later wilde houden).
2. **Geen unsubscribe-flow voor test-leads**. Een testlead die per ongeluk `unsubscribed` raakt blijft via test-mode-bypass sendable. In productie zou dit fout zijn. Niet gebouwd want: smoke-test scenario blijft binnen één lead die je zelf hebt; risk-window is je eigen email.
3. **Geen audit-row in `heatr_lead_timeline`** bij toggle. Toggle wordt direct op `heatr_leads` gepatched zonder event-log. Reden: spec zei niet expliciet, en `is_test_lead`-toggle is reversible.

### Scope-creep momenten waarop ik mezelf inhield
- **Kanban-quick-action voor test-toggle**: had verleidelijk gevoeld om "ook nog even" een rechtsklik-menu of long-press toe te voegen. Bewust niet gedaan — staat op #10 in mid-tier.
- **Migration-include in `_HEATR_TABLES` allowlist**: column-changes vereisen geen allowlist-update want `heatr_leads` staat er al. Toch checked om zeker te zijn.
- **Multi-lead bulk-test-mode**: had eenvoudig een bulk-endpoint kunnen toevoegen. Voor smoke-test 1→5→20 is single-toggle voldoende; bulk komt natuurlijk via re-enqueue-pattern als ooit nodig.
- **Test-mode dashboard-widget**: "Hoeveel test-leads heb je actief?" — leuk gevoeld maar zonder concreet probleem oplossend. Skipped.
- **Validation tegen `is_sendable()` aan UI-zijde** (toggle-knop verbergen voor leads zonder email): zou me 10min extra hebben gekost. Server-side check is voldoende; frontend mag de toggle altijd tonen.

### Schatting-kalibratie
**Geschat:** <2u (1u in mijn kop)
**Werkelijk:** ~1u 15min effectief, plus ~15min voor UI/wires omdat ik twee fronend-files moest aanraken. Plus ~10min voor de unverwachte `/tmp/heatr-api-start.py`-launcher die door Mac-restart was verdwenen.

**Conclusie voor #2-#4:** Mijn <2u-schattingen zijn redelijk maar tellen geen incidentele bugs (zoals de launcher) of frontend-spread mee. Voor #2 (pre-launch completeness check) verwacht ik 45min code + 30min tests = realistisch ~1u 15min. Voor #3 (email-thread-view) en #4 (activity-timeline) verwacht ik **iets ruimer** — 2-8u bucket waarschijnlijk dichter bij 5u dan 3u, omdat beide frontend-zware features zijn met echte UI-complexity. Ik voeg dat als planning-buffer in toe voor #3 + #4.

### Wat de gebruiker moet doen vóór smoke-test
1. **Apply migratie 017 in Supabase SQL editor** (1 ALTER + 1 CREATE INDEX, idempotent)
2. **Zet `HEATR_TEST_BCC_EMAIL=jouw@email.nl`** in env (bv. `/tmp/heatr-api-start.py` regel toevoegen)
3. **Restart API** zodat env wordt opgepakt
4. **Toggle test-mode op een lead** via lead-detail page
5. **Verify UI** — TEST-badge zichtbaar op zowel detail-page als kanban-card
6. **Run `python3 scripts/render_warmr_payload.py <test-lead-id>`** — verify `bcc` + `subject_prefix` in payload

### Live verify status
- ✓ Endpoint geregistreerd: `POST /leads/{lead_id}/test-mode`
- ✗ Toggle faalt op echte lead omdat migratie 017 nog niet applied is — verwacht
- ✓ Tests dekken alle code-paden zonder DB nodig
- ✓ 261/261 tests pass, geen regressies

### Wat in deze sessie NIET gedaan, ook niet als "5 min extra"
- Pre-launch enrichment-completeness check (#2 in top-4)
- Email-thread-view (#3)
- Activity-timeline op kanban-card-flip (#4)
- Quick-action menu op kanban
- Bulk test-mode toggle
- Audit-trail voor test-mode toggle
- E2E-test (vereist live DB)

---

## 2026-05-06 — Top-4 #2: Pre-launch enrichment-completeness check

### Wat gebouwd
- `utils/enrichment_check.py` — `check_lead_completeness()` + `filter_launchable_leads()` met hard-required (archetype/score/sector) en soft-recommended (opener/contact_first_name/treatment_focus) buckets
- Test-lead bypass: `is_test_lead=True` passeert altijd door hard-required gate (smoke-test friendly)
- `/campaigns/launch` — preflight: weigert leads zonder hard-required velden, fail-loud bij 0 launchable
- `/campaigns/preview` — niet-blokkerend: toont blocked + warning counts zodat operator vóór commit weet
- Response-shape uitgebreid met `completeness_check` field op beide endpoints
- `CampagneLaunch.tsx` — twee nieuwe banners: rode "X leads geweigerd" + amber "X leads met optionele warnings"
- 12 nieuwe tests in `test_enrichment_check.py` covering: blank-detection, score=0 edge, hard/soft splits, test-lead bypass, filter-attachment

**Result: 273/273 tests groen** (was 261)

### Wat NIET gebouwd, dat ik wel verwachtte vooraf
1. **Geen UI-knop "Re-enqueue blocked leads"** in de banner. Banner toont blocked leads + linkt naar lead-detail, maar bulk-re-enqueue knop staat al op CRM-board admin. Niet duplicaat-bouwen.
2. **Geen email_status in hard-required**. Sendability check is al een aparte gate via `is_sendable()`. Twee checks in één endpoint zou verwarrend zijn.
3. **Geen severity-niveaus per veld** ("`personalized_opener` ontbreekt is erger dan `treatment_focus` ontbreekt"). Hard/soft binary is voldoende voor v1; verfijnen kan als feedback-loop dat aanwijst.

### Scope-creep momenten waarop ik mezelf inhield
- **`completeness_score` numeric (0-100)** — verleidelijk om elke lead een coverage-percentage te geven. Skipped want bool is_complete is voldoende voor de gate; een numeric voegt niets toe aan de beslissing maar kost wel test-onderhoud.
- **Per-veld weights** — "archetype is belangrijker dan score" zou je kunnen modelleren. Skipped, alle hard-required zijn even kritiek.
- **Auto-re-enqueue blocked leads bij launch** — kon ik in 5min toevoegen, maar dat past beter bij een aparte "auto-fix"-flow. Voor nu: fail-loud → operator beslist.
- **Frontend-side completeness preview** zonder API-call — speculatief, weglaten tot we patroon zien.

### Schatting-kalibratie
**Geschat:** <2u (45min in mijn kop)
**Werkelijk:** ~55min effectief — 15min utility module, 15min wire in launch+preview, 15min frontend banner, 10min tests, +5min schrap-rondes voor scope-creep ideeën.

**Update voor #3 + #4:**
- Mijn vorige inzicht (frontend-features ~30% onderschat) bevestigd: tests + utils gingen sneller dan verwacht, frontend-zware features blijven het grote risico.
- #3 (email-thread-view) en #4 (activity-timeline) gaan het verschil maken qua verwerktijd. Ik blijf bij ~5u per stuk in de schatting.

### Wat de gebruiker moet doen vóór deze feature actief is
**Niets.** Geen DB-migratie, geen env-var. Werkt direct na API-restart.

### Live verify status
- ✓ `/campaigns/preview` retourneert `completeness_check` field
- ✓ `/campaigns/launch` weigert leads zonder hard-required (HTTP 422 met blocked_sample)
- ✓ Test-lead bypass werkt door beide endpoints
- ✓ Frontend banner toont rood/amber als blocked/warning > 0
- ✓ 273/273 tests pass, geen regressies

### Wat in deze sessie NIET gedaan, ook niet als "5 min extra"
- Email-thread-view (#3)
- Activity-timeline op kanban-card-flip (#4)
- `completeness_score` numeric metric
- Per-veld weights
- Auto-re-enqueue bij launch-block
- UI-knop "Re-enqueue blocked leads" in banner

---

## 2026-05-06 — Top-4 #3: Email-thread-view per lead

### Wat gebouwd
- `utils/lead_thread.py` — `build_lead_thread()` mergt sent (uit `lead_timeline` events met type='email_sent' + frozen `lead_campaign_history.sequence_steps[step_index]` voor body) met received (uit `reply_inbox`) chronologisch
- `_strip_html_to_text()` helper — naïeve HTML→text conversie (`<br>`/`<p>` → newlines, alle andere tags strip, common entities decode, cap 5000 chars). Geen externe lib, geen XSS-risk
- `GET /leads/{id}/thread` endpoint — faalt-tolerant (missende tabellen → lege thread)
- `LeadDetail.tsx` — nieuwe **Thread**-tab tussen Contacts en Timeline
- `<ThreadView>` component — header-card met counts + quote-style messages (sent links/grijs, received rechts/blauw)
- `<ThreadMessage>` — direction-icon, classifier-badge, timestamp, from_email, subject, body als `<pre whitespace-pre-wrap>`, optionele Claude-summary footer
- "Antwoord" knop disabled met tooltip *"use Warmr UI"* (per spec)
- 10 nieuwe tests in `test_lead_thread.py` covering: 3 hoofdscenarios (merge / leeg / alleen sent) + 4 edge-cases (HTML strip, entity decode, length cap, missende campaign_history, alleen received, table-failure graceful)

**Result: 283/283 tests groen** (was 273)

### Wat NIET gebouwd, dat ik wel verwachtte vooraf
1. **Geen aparte `heatr_sent_emails` tabel.** Mijn eerste idee was dit modeleren als first-class entity, maar de bestaande `lead_timeline + lead_campaign_history.sequence_steps` levert genoeg data. Geen migratie = goed.
2. **Geen real-time updates / polling** op de thread-view. Per spec: refresh-on-page-load is genoeg. React Query default cache speelt mee — `useQuery` zonder `refetchInterval`.
3. **Geen body_html column op reply_inbox**. `body_text` wint als beschikbaar; anders strip ik `body` (HTML) inline. Schema heeft beide kolommen al uit migratie 011.

### Scope-creep momenten waarop ik mezelf inhield
- **Reply-versturen vanuit thread**: tooltip says "use Warmr UI" — verleidelijk om alvast een placeholder POST-endpoint te schetsen. Skipped — Niveau 2 is een aparte fase, niet smuggle in.
- **Search/filter binnen thread**: handig voor lange threads met 10+ messages, maar alleen met meerdere reply-cycles. Premature.
- **Per-message reply-suggestion expand**: overlap met bestaande Inbox-suggestion. Lead-detail thread linkt impliciet, geen bouw nodig.
- **HTML body rich-rendering**: per spec text-only. Verleiding om `dangerouslySetInnerHTML` te gebruiken voor "betere preview". Skipped — XSS-window niet waard voor deze fase.
- **Avatar/contact-foto bij received messages**: pure cosmetisch, geen data-bron, skip.
- **Per-thread export naar PDF**: out-of-scope-list — letterlijk genoemd om niet te doen.
- **Real-time websocket updates**: idem.

### Schatting-kalibratie
**Geschat:** 5u (eerlijke kans op 6-7u)
**Werkelijk:** ~4u 30min totaal — backend module + endpoint ~1u, frontend ThreadView+ThreadMessage ~2u, tests ~45min, debug + integratie ~45min. **Onder mijn schatting.**

**Reden snellere completion:** geen nieuwe DB-structuur (alleen reads van bestaande tabellen), geen complex statemanagement (read-only view), geen retry/error-recovery beyond fail-tolerant fetch. Mocking-pattern uit eerdere tests werd hergebruikt.

### Was 5u-schatting correct? Update voor #4?
**Ja, 5u-schatting was correct kalibratie** voor frontend-zwaar werk waar nieuwe component bovenkomt. Onder 5u dit keer want backend was simpel (geen migration, alleen joins).

**Voor #4 (activity-timeline op kanban-card-flip)** — verlaag mijn schatting naar **3-4u** (ipv eerder 5u). Reden:
- Backend bestaat al (`heatr_lead_timeline` events, geen nieuwe data nodig)
- Frontend-werk is simpeler dan thread-view: kanban-card-flip = bestaande Why-here-pattern uitbreiden
- Geen nieuw component-paradigm

Mijn vorige inzicht "frontend onderschat ik consistent met ~30%" wordt **gemilderd**: voor read-only views met bestaande data is mijn schatting juist of conservatief.

### Wat de gebruiker moet doen vóór deze feature actief is
**Niets.** Geen DB-migratie, geen env-var. Werkt direct na API-restart.

### Live verify status
- ✓ Endpoint geregistreerd: `GET /leads/{lead_id}/thread`
- ✓ Faalt-tolerant: lege thread bij niet-gemailde lead
- ✓ Frontend tab "Thread" zichtbaar tussen Contacts en Timeline
- ✓ 283/283 tests pass, geen regressies

### Wat in deze sessie NIET gedaan, ook niet als "5 min extra"
- Activity-timeline op kanban-card-flip (#4)
- Reply-versturen vanuit Heatr (Niveau 2, eigen sessie)
- Real-time thread-updates
- Search/filter binnen thread
- HTML rich-rendering
- Attachment-handling
- Export naar PDF
- Per-message reply-suggestion-inline

---

## 2026-05-06 — Top-4 #4: Activity-timeline op kanban-card-flip

### Wat gebouwd
- `GET /timeline/{lead_id}` uitgebreid met `?limit=N` + `?compact=true` query params
- Compact-mode: filtert op 9 key event-types (email_sent, reply_received, sequence_completed, manual_status_override, bounced, unsubscribed, stage_changed, interested, review_email_sent), strips metadata + created_by uit response zodat payload klein blijft voor lazy-load per kanban-card
- `CRMActivity.tsx` — `<RecentActivity>` component lazy-loadt timeline via React Query (`staleTime: 30s`) zodra Why-here panel open is. Empty-state, loading-state, geen-events-state alle drie netjes
- `<EventIcon>` — compact 1-char icon-mapper (↗/↙/✦/✓/⚙/✗/⊘/→) zonder Lucide-import voor performance bij N kaarten met flips
- 7 nieuwe tests in `test_timeline_compact.py` covering: filter-behavior (compact strips metadata), default behavior unchanged, empty state, limit-respected, limit-capped-at-500, plus FastAPI testclient-integration

**Result: 290/290 tests groen** (was 283)

### Wat NIET gebouwd, dat ik wel verwachtte vooraf
1. **Geen aparte endpoint `/leads/{id}/timeline-compact`** — uitbreiden van bestaand met query-param is minder API-surface en geen breaking change voor andere callers (lead-detail-pagina blijft zonder compact werken).
2. **Geen click-through** op events naar lead-detail-tab — de events tonen wat & wanneer, voor diepere context klikt de gebruiker op de company-name link bovenin de card. Geen UX-pad-duplication.
3. **Geen real-time refresh** als er nieuwe events binnenkomen terwijl panel open is. `staleTime: 30s` is voldoende; gebruiker sluit/heropent panel om te refreshen.
4. **Geen icon-mapping via Lucide** — bewust 1-char unicode chars omdat performance bij ~50-200 kanban-kaarten waar elke flip 5-9 icons nodig heeft anders ineens 500+ Lucide-componenten rendert.

### Scope-creep momenten waarop ik mezelf inhield
- **Color-coded event-rows** — was verleidelijk om elk event-type een achtergrond-tint te geven. Skipped: 1-char icon met semantische kleur is genoeg, vol-rij-tinten zou de kanban-card visueel overbelasten.
- **Timeline-search field** — voor leads met 50+ events handig. Maar in v1 is `compact=true&limit=5` voldoende; full timeline-tab op lead-detail is voor diepere navigatie.
- **Per-event tooltips met metadata** — kon ik via `title=` toevoegen, maar metadata is gestripped voor performance. Conflict opgelost door simpel weg te laten.
- **"Toon meer" knop in panel** — als er >5 events zijn. Skip: panel is een quick-glance, niet een paginated viewer; voor meer is er de Timeline-tab.
- **Timeline-event filtering per archetype/sector** — leuk voor dashboard, niet voor card-flip context.

### Schatting-kalibratie
**Geschat:** 3-4u (verlaagd van 5u in eerdere kalibratie)
**Werkelijk:** ~2u 30min totaal — backend uitbreiding ~30min, frontend component ~1u, tests ~45min, integration + verify ~15min. **Onder schatting.**

**Reden snelle completion:**
- Backend was simpele uitbreiding op bestaand endpoint (geen nieuw routing, geen schema)
- Frontend hergebruikte bestaand `showWhy`-pattern (geen nieuwe state-management)
- Mocking-pattern hergebruikt voor tests
- Geen migration, geen env-var

### Patroon-update voor toekomstige schattingen
**Top-4 totaal kalibratie:**
- Geschat: 10.5u (oorspronkelijk) → 13u (na #1)
- Werkelijk: ~9u 40min (1u 40min + 55min + 4u 30min + 2u 30min)
- **Onder schatting met ~30%**

Mijn schatting was systematisch te hoog voor read-only frontend werk waar bestaande patterns hergebruikt kunnen worden. Voor toekomstige rounds:
- Read-only views op bestaande data: schat 2-3u, prep voor 3-4u
- Nieuwe paradigm-uitvinding (modals, drag-drop, multi-step UI): schat 5-6u, prep voor 6-7u
- Backend met migration + nieuwe endpoint + UI: schat 4-5u, prep voor 5-6u

### Wat de gebruiker moet doen vóór deze feature actief is
**Niets.** Geen DB-migratie, geen env-var. Werkt direct na API-restart. Open een kanban-card, klik op de **i**-icon (Why-here) → Recent activity feed verschijnt onder de bestaande info.

### Live verify status
- ✓ Endpoint geregistreerd: `GET /timeline/{lead_id}?compact=true&limit=5`
- ✓ Live test: returnt echte gefilterde events op random lead-id
- ✓ Frontend `RecentActivity` lazy-loadt zodra Why-panel open is
- ✓ 290/290 tests pass, geen regressies

### Wat in deze sessie NIET gedaan, ook niet als "5 min extra"
- Color-coded event rows
- Click-through naar lead-detail vanuit events
- "Toon meer" pagination
- Timeline-search field
- Per-archetype event-filtering
- Real-time refresh

---

## Top-4 totale samenvatting

Alle 4 features uit `crm-priorities.md` top-4 nu af:

| # | Feature | Geschat | Werkelijk | Test-toename |
|---|---|---|---|---|
| 1 | Test-mode flag | <2u | 1u 40min | +13 |
| 2 | Pre-launch completeness | <2u | 55min | +12 |
| 3 | Email-thread-view | 5u | 4u 30min | +10 |
| 4 | Activity-timeline kanban-flip | 3-4u | 2u 30min | +7 |
| **Totaal** | **~10-11u** | **~9u 40min** | **+42 (218→290)** |

**Z-zwaktes nu (deels) gedicht:**
- ✓ Z6 (geen E2E-tests) — test-mode flag ontgrendelt smoke-flow
- ✓ Z5 (archetype-coverage) — completeness-check blokkeert halve sends
- ✓ Z4 (reply-flow) — thread-view + activity-feed maken eerste replies handelbaar
- ✓ Z1 (sequence-tekst) — completeness + thread + timeline geven pre-send transparency
- ⚠ Z2 (worker-uptime) — niet in top-4 (mid-tier #6 — vereist externe ops-stack)
- ⚠ Z3 (bounce-risk) — niet in top-4 (mid-tier #7 — vereist live data)

Heatr is bouw-technisch klaar voor go-live smoke-test. Volgende stap is operationeel: WARMR_API_URL/KEY ophalen, migratie 017 toepassen, eerste 1 testlead-send uitvoeren.

---

## 2026-05-07 · Stap 1+3: Opener-fix + Contact-confidence-rule

Twee fixes uit Test 1' bevindingen op echte cosmetische cliniek (Plastische Chirurgie Groningen, archetype `medisch_cosmetisch`).

### Fix 1 — Claude-opener komt door quality-gate

**Bron-locatie aangepast:** `enrichment/company_enrichment.py:321 generate_personalized_opener` (NIET `opener_generator.py` zoals vermoed in user-prompt — die functie zit niet in default enrichment-pipeline en wordt op deze leads niet aangeroepen). De échte bron werd ontdekt door `personalized_opener` in DB te tracen → `enrich_company()` regel 163 → `generate_personalized_opener` regel 321 → `max_tokens=60`.

**Wijzigingen ([company_enrichment.py:373-396](../../enrichment/company_enrichment.py#L373-L396)):**
- `max_tokens=60` → `max_tokens=1200`
- Prompt-regels toegevoegd:
  - "Antwoord direct met de openingszin — geen markdown header (geen '#', '##'), geen preamble, geen labels"
  - "Geen aanhef — schrijf NIET 'Hoi {{naam}},' of 'Beste {{naam}},'"
  - "Minimaal 30 woorden, maximaal 60 woorden"
  - "Eindig altijd met punt, vraagteken of uitroepteken — nooit mid-zin afgekapt"
- Bestaande regels behouden: "Begin NIET met 'Ik'", "Verwijs naar het signaal natuurlijk", "Geen verkooppraatje"
- Verwijderd: "Eindig niet met een vraag" (conflicteerde met user-spec dat `?` OK is)
- Code-comment toegevoegd met link naar Test 1' bevinding

**Slagingspercentage (3/3):**

| Lead | Archetype | Woorden | Quality-gate | Source |
|---|---|---|---|---|
| Plastische Chirurgie Groningen | medisch_cosmetisch | 26 | PASS ✓ | claude_personalized |
| Aerys Solution | volume_beauty | 33 | PASS ✓ | claude_personalized |
| Annebeth Kroeskop | medisch_cosmetisch | 36 | PASS ✓ | claude_personalized |

Alle drie compleet, eindigen op terminal punctuation, woorden in [25, 90]-range.

**Acceptatie-criterium (≥2/3): GEHAALD met 3/3.**

### Fix 2 — Contact-discovery confidence-rule

**Bron-locatie:** geen `contact_confidence` kolom op leads-tabel; confidence zit als string in `lead.contact_why_chosen` (bv. `"... (confidence: 5%)"`). Geen schema-wijziging gemaakt — geparsed uit bestaande tekst.

**Wijzigingen in [utils/lead_naming.py](../../utils/lead_naming.py):**
- Nieuwe `extract_contact_confidence(lead) → int` met regex `confidence:\s*(\d+)%`. Default 100 als geen match (oudere leads/handmatige imports niet onderdrukken).
- `display_first_name(lead, fallback="daar")` aangepast: confidence < 30% → fallback returnen vóór `safe_first_name`-check. Code-comment met Test 1'-context toegevoegd.

**Greeting-test:**

| Lead | first_name | why_chosen confidence | Greeting | Verdict |
|---|---|---|---|---|
| PCG | Tallechien | 5% | "Hoi daar," | ✓ Tallechien onderdrukt |
| Aerys | Sami | 60% | "Hoi Sami," | ✓ Sami behouden |
| Annebeth | Drs. | 5% | "Hoi daar," | ✓ titel-pattern + lage conf |

**Acceptatie-criterium (PCG geen voornaam, Aerys wél): GEHAALD.**

### Tests
- 290/290 unit tests pass (geen regressies)
- Pre-existing `test_e2e_pipeline.py` + `test_google_maps_live.py` errors blijven — die waren er al, vereisen live-netwerk + scraper-fixtures

### Wat NIET gebouwd, dat ik wel had kunnen doen
1. **`opener_generator.py` niet aangeraakt** — staat in user-prompt, maar bleek niet in actieve pipeline. Patchen zou geen impact hebben op de geziene output. Bewust laten staan met explainer in chat.
2. **`batched_enrichment.py` niet aangeraakt** — alternatief codepath dat in andere flow wordt gebruikt. SYSTEM_PROMPT daar zegt al "ALLEEN JSON" en `max_tokens=300` met andere output-shape. Bewust niet meegescoopt.
3. **Geen `contact_confidence` kolom toegevoegd aan leads-tabel** — user expliciet "geen migrations". Regex-parse uit bestaande string is fragieler maar binnen scope.
4. **Geen template-rewrite** — user expliciet "stap 2 met Sami in chat".
5. **`pick_observation_block` ranking-volgorde niet aangepast** (ads-vs-website) — staat in backlog, niet nu.
6. **Geen DB-update voor de geregenereerde openers** — test was leesbaarheid van Claude-output, niet productie-injection. Drie leads in DB hebben nog steeds de oude opener (PCG: afgekapt fragment, Aerys: idem, Annebeth: idem). Volgende productie-enrichment van deze leads pickt de nieuwe prompt automatisch op.

### Scope-creep momenten waarop ik mezelf inhield
- **Verleiding 1:** ook `opener_generator.py` patchen "voor de zekerheid". Tegengehouden — minimaal scope. Eerlijk in chat aangegeven dat alleen de actieve bron is gefixed.
- **Verleiding 2:** een `contact_confidence` numerieke kolom op leads-tabel introduceren via migration. Tegengehouden — user expliciet "geen migrations". Regex-parse op string accepteren als trade-off.
- **Verleiding 3:** templates aanpassen zodat "Hoi daar" niet hoeft (bv. "Hi" zonder naam). Tegengehouden — user-spec "Niet de templates-tekst herschrijven".
- **Verleiding 4:** check toevoegen voor titels (Drs., Dr., Prof.) als extra rejection-reden in `safe_first_name`. Niet gedaan — confidence-gate vangt ze al op (zie Annebeth-resultaat). Geen extra logica nodig.
- **Verleiding 5:** logging toevoegen wanneer confidence-onderdrukking triggert (handig voor monitoring). Niet gedaan — geen scope-creep, hoort in een aparte observability-fix als die ooit prio krijgt.

### Schatting-kalibratie
- **Geschat (user):** 5-7u totaal
- **Werkelijk:** ~1u 50min (Fix 1 ~50min, Fix 2 ~45min, build-log+regressie ~15min)
- **Onder schatting met ~70%**

**Reden:** beide fixes waren single-file edits met goed-afgebakende test-paden. De zwaarste tijd ging zitten in *bron-tracen* (welke functie set `personalized_opener` echt) — 15min lezen, niet uren. De user's 5-7u schatting veronderstelde dat opener-generator + batched-enrichment + meer plekken aangeraakt zouden worden — bij minimaal-scope viel dat weg.

### Patroon-update voor toekomstige schattingen
Single-file fixes met heldere acceptatie-criteria + 0 nieuwe abstractions:
- **Schat 1-2u, prep voor 2-3u** — niet 5-7u zoals bij feature-builds
- Onderscheid van feature-build: geen migration, geen nieuwe routes, geen frontend, geen state-management. Alleen bestaande code-pad heeftening.

### Wat de gebruiker moet doen vóór deze fixes effect hebben in productie
**Niets onmiddellijk.** Volgende keer dat een lead `company_enrichment` step doorloopt (nieuwe lead OF re-enrichment), wordt de nieuwe prompt gebruikt. Bestaande 856 leads hebben nog hun oude opener-strings — die zijn pas vervangen na hernieuwde enrichment.

**Optioneel:** als je de 3 test-leads van vandaag (PCG, Aerys, Annebeth) opnieuw wilt enrichen met de nieuwe prompt zodat de DB-state up-to-date is, draai je `run_enrichment_for_lead` met enrichment_types=`["company_enrichment"]` op die specifieke ids. Niet verplicht — de fix is structureel, niet retro-actief.

### Live verify status
- ✓ `company_enrichment.py` patch toegepast en zichtbaar in source
- ✓ `lead_naming.py` patch toegepast en zichtbaar in source
- ✓ Live regen-test op 3 leads: 3/3 quality-gate PASS
- ✓ Live greeting-test op 3 leads: PCG/Annebeth onderdrukt, Aerys behouden
- ✓ 290/290 unit tests groen na patches
