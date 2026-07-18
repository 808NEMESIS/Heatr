# Audit-scorer — technisch ontwerp

Code-verankerde naslag voor de prospect-facing website-audit (`audit/`). Beschrijft
het **model en de codestructuur**, geen operationele cijfers. De Notion-log "Heatr —
Audit-laag" is het beslissings-/bouwnarratief; dit document is de "hoe werkt het".
Bij tegenspraak wint de code.

---

## 1. Doel en scheiding

`website_intelligence` scoort een site op **koopwaardigheid voor Aerys** (is deze
lead het benaderen waard). De audit-laag scoort dezelfde site op **gebreken voor de
prospect** (hier verliest u klanten). Zelfde signalen, andere weging, andere taal.

**Harde scheiding:** twee scorers, gedeelde data, gescheiden logica. De audit leest
`website_intelligence` maar schrijft **nooit** `total_score`. Zo blijft de interne
classifier tunebaar zonder verstuurde rapporten te breken.

- Interne scorer: `website_intelligence/analyzer.py` → `heatr_website_intelligence`.
- Audit-scorer: `audit/scorer.py` → `heatr_audit_reports`.

## 2. Datamodel

`heatr_audit_reports` (migratie `035_audit_reports.sql`), geregistreerd in
`config/database.py _HEATR_TABLES`. **Append-only, `unique(lead_id, version)`** —
v1 (tier 1) en v2 (tier 2) blijven allebei staan; nooit upsert. Een verstuurd
rapport verandert niet omdat de dataset eromheen groeide.

Aparte tabel, **geen uitbreiding van `heatr_teardown_pages`** (029): dat modelleert
de gepubliceerde, deelbare pagina (token, html_path, draft→published→revoked) +
`page_views` — de render/bezorg-laag. `audit_reports` is de scoringdata (de bron);
een teardown-pagina zou later een audit_report *renderen*.

Kernkolommen: `version, tier, score_total, score_normalized, score_capped_by,
scored_layers, categories jsonb, findings jsonb, benchmark jsonb,
screenshot_desktop_url, screenshot_mobile_url, content_hash`.

Het report-dict dat `score_lead` teruggeeft is het contract (zie §7); `persist_audit_report`
schrijft het met een oplopende `version`.

## 3. Tiering

| | Tier 1 | Tier 2 |
|---|---|---|
| Wanneer | bij elke lead / batch | op verzoek (bij leadreactie) |
| Kosten | DB-reads + 1 lichte httpx-fetch; geen Claude/Places | + Places API + benchmark |
| Crawl | geen nieuwe crawl (leest bestaande capture-data) | **geen nieuwe crawl** |
| Doel | vult de benchmark-dataset | volledige score + benchmark |

Rescan-skip: als `content_hash` (`dom_text_hash`) gelijk is aan de vorige run,
hoeft niet herberekend. Vision wordt nooit opnieuw aangeroepen — `visual_score` ligt
er al in `website_intelligence`.

## 4. Scoremodel

**Normaliseren over de BEHAALDE noemer.** Een check waarvan de databron ontbreekt is
`not_measurable` en telt niet mee in teller én noemer — geen valse fail. Formule
(`audit/scorer.py`):

```
achieved = som van punten_behaald over de MEETBARE checks
denom    = som van punten_max    over de MEETBARE checks
score_normalized = round(achieved / denom * 100)
```

`scored_layers` legt vast welke categorieën meededen. `score_denominator` maakt de
noemer expliciet — een 77/70 is iets anders dan een 77/95.

**Maximum-som per niche (bron = de checks, niet de config-header):**
cosmetiek **108**, chiro **107**. Het gat t.o.v. 110 zit volledig in Lead Conversion
(items 32/31 vs header 34); het 1-punt-verschil tussen de niches is by-design
(`consult_intake` 3 → `vergoeding` 2 voor chiro). **De twee niches zitten op een net
andere absolute schaal — alleen `score_normalized` (0-100) is vergelijkbaar, nooit
`score_total`.**

**Knock-outs** (cappen de eindscore, reden in `score_capped_by`):
- geen enkele afspraakmogelijkheid (agenda/formulier/WhatsApp) → max **70**
- geen `tel:`-link én geen zichtbaar nummer → max **80**

**Lege-site-detectie:** DOM-tekst < 150 tekens → aparte high-severity finding
(`empty_site`, `mail_safe=true`), **geen** lage score. Een verlaten placeholder is
geen slechte site (avichi.nl: status 200, geldige titel, 25 tekens body).

**Broken links & pages:** buiten de score, apart geteld/gerapporteerd (Tier 2).

## 5. Categorieën en rapportlabels

Rapportlabels staan los van interne namen (`config/audit_weights.CATEGORY_LABELS`) —
de prospect leest geen jargon.

| Intern | Rapportlabel | Max (cos/chi) |
|---|---|---|
| lead_conversion | Komen bezoekers tot actie | 32 / 31 |
| social_proof | Waarom zouden ze u kiezen | 18 / 18 |
| local_trust | Bent u vindbaar | 13 / 13 |
| professional_trust | Bent u geloofwaardig | 9 / 9 |
| privacy | Voldoet u aan de regels | 10 / 10 |
| seo_visibility | Wordt u gevonden | 10 / 10 |
| technical | Werkt uw site | 8 / 8 |
| visual | Hoe komt u over | 8 / 8 |

## 6. Check-catalogus (43 checks)

`config/audit_weights.CHECKS` is de bron van waarheid (punten + sectoren);
`audit/checks.CHECK_FUNCS` de detectie. `[cos]`/`[chi]` = sector-specifiek.

**Lead Conversion:** online_afspraak_widget 10 · whatsapp_klikbaar 5 · tel_above_fold 3 ·
contactform_max5 4 · cta_above_fold 3 · consult_intake_aanbod 3 `[cos]` /
vergoeding_aanvullend 2 `[chi]` · responstijd_belofte 2 · live_chat 2

**Social Proof:** reviews_zichtbaar 5 · google_rating_min 4 · voor_na_galerij 4 `[cos]` /
patientverhalen 4 `[chi]` · behandelaars_naam_foto_kwal 3 · echte_praktijkfotos 2
(*altijd not_measurable*, zie §9)

**Local Trust:** maps_embed_contact 4 · adres_footer_volledig 4 · gbp_link 2 ·
openingstijden_structured 3

**Beroeps- & Medisch Trust:** `[cos]` big_nummer 4 · keurmerk_cosmetiek 3 · wkkgz_klachten 1 ·
voor_na_toestemming 1 — `[chi]` scn_nca_registratie 4 · erkende_opleiding_chiro 3 ·
wkkgz_klachten 1 · geschilleninstantie 1

**AVG (privacy):** privacyverklaring_bereikbaar 3 · cookiebanner_weigeren 3 ·
geen_tracking_pre_consent 3 · verwerkersregister_dpo 1 — *alle `mail_safe=false`*

**SEO:** unieke_title_meta 2 · een_h1_per_pagina 1 · schema_medical_valide 2 ·
behandelpaginas_3plus 3 `[cos]` / klachtenpaginas_3plus 3 `[chi]` · interne_links_key 1 ·
sitemap_robots 1

**Technical:** psi_mobile_50 3 · lcp_onder_2_5 2 (*nu not_measurable*, §11) ·
https_geldig_cert 1 · geen_mixed_content 1 · security_headers 1

**Visual:** visuele_indruk 8 — `visual_score` (0-25) → 0-8. **Eén getal, geen
dimensie-uitsplitsing** (§9).

## 7. Findings-contract

Elke check geeft één finding-object (`audit/checks._f`):

```
check_id, categorie, status (pass|warn|fail|not_measurable),
punten_behaald, punten_max,
bewijs,        # DOM-selector, netwerk-request of screenshot-referentie — VERPLICHT
severity, mail_safe (bool), mail_zin, uitleg
```

- `bewijs` is verplicht: je vertelt een ondernemer dat er iets mis is — zonder bewijs
  is elke claim aanvechtbaar.
- `mail_safe=false` voor alles wat naar wetsovertreding ruikt (tracking, privacy). Juist
  en waardevol, maar ongevraagd verstuurd leest het als dreigement — nooit in een
  geautomatiseerde eerste mail. Bewaren voor de call.
- `mail_zin` is één feitelijke zin, geen oordeel, concreet getal waar mogelijk.

## 8. Databronnen — hergebruikt vs. nieuw

**Hergebruikt (geen her-detectie):**
- `leads`: has_whatsapp, has_online_booking, has_instagram, phone, google_rating,
  google_review_count, google_maps_url, has_cookie_banner.
- `website_intelligence.conversion_details`: CTA, tel, WhatsApp, booking, chatbot, form.
- `website_intelligence.technical_details`: has_ssl, pagespeed_mobile, has_sitemap.
- `website_intelligence`: visual_score, screenshot-urls, dom_text_hash.
- `heatr_website_network_log`: requests met fase (pre/post_freeze).
- `enrichment_data` (contact_crawl_v2): schema_org, page_text.

**Nieuw:**
- `audit/tracking.py`: tracker-blocklist + neutraal-allowlist over de netwerk-log.
- `audit/nl_trust.py`: BIG (mét context), keurmerken, Wkkgz, geschil, chiro-opleiding.
- Lichte homepage-fetch in `scorer._fetch_html` → `(html, response_headers)` voor de
  DOM-structuur-checks (H1/title/meta/embed/mixed-content) én `security_headers`
  (zonder re-enrichment).
- `audit/places.py`: reviews via Google Places (Tier 2).

## 9. Beslissingen (verankerd in code)

- **`echte_praktijkfotos` altijd `not_measurable`** — niet dichten via de Vision-
  fotografie-dimensie: die correleert bijna volledig met de andere drie en meet "ziet
  er goed uit", niet "echte foto's". Wacht op een echte signaalbron.
- **Eén visuele score, geen dimensie-uitsplitsing** — Vision vormt één indruk en
  verdeelt die over de labels; claim geen precisie die de meting niet heeft.
- **`is_third_party` is geen tracker** — expliciete blocklist (facebook.net,
  google-analytics.com, googletagmanager.com, doubleclick, hotjar, clarity.ms,
  linkedin/px, tiktok pixel) + neutraal-allowlist (fonts, CDN, maps, recaptcha).
  Alleen `pre_freeze`-matches zijn hard. Conservatief: false positives duurder.
- **Netwerk-cutoff op networkidle-of-load+3s**, alles doorloggen met fase — trackers
  vuren vaak ná `load`.
- **Benchmark cohort-gebonden voorlopig** — data-gaten zijn cohort-gebonden (oud
  cohort mist has_cookie_banner/response_headers/schema_org). Per-lead-score prima;
  benchmark `provisional: true` tot een re-enrichment-sweep gelijke meetbaarheid brengt.

## 10. Pipeline

- **`audit/scorer.score_lead(lead, sb, tier, places)`** → report-dict (schrijft niet).
- **`persist_audit_report(report, sb, benchmark)`** → append-only insert met versie.
- **`scripts/run_audit_tier1.py`** — batch, resume-safe (skip leads met bestaand tier-1
  report tenzij `--all`), `--limit`/`--dry-run`.
- **`POST /leads/{id}/audit`** — per lead (Tier 1).
- **`scripts/sample_audit_tier1.py`** — read-only steekproef met not_measurable-detail.
- **Tier 2** (`audit/places.py` + `audit/benchmark.py`): live-call wacht op
  `GOOGLE_PLACES_API_KEY`; de `google_places` rate-limiter-bucket moet nog aan
  `rate_limiter.RATE_LIMITS` (bestaand bestand) worden toegevoegd, samen met de key.

## 11. Bekende schuld & incrementele verbeteringen

Schuld (raakt de audit, uit de bouwlog): Pillow `--user`-install op de worker-host
(fragiel); migratie 029 niet toegepast (teardown-tabellen ontbreken);
`website_intelligence.lead_id` zonder unique-constraint terwijl code
`on_conflict="lead_id"` doet; kolomnaam-drift `*_details` vs `*_data`.

Incrementeel (na de eerste run): **LCP uitlezen** — zit al in de PageSpeed-respons
(`technical_checker.py:117-122`), key aanwezig, geen re-crawl; eerste op de lijst.
**Re-enrichment-sweep** — vult has_cookie_banner + schema_org voor het oude cohort en
brengt de benchmark op gelijke meetbaarheid.
