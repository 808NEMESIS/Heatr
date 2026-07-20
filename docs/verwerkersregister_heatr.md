# Verwerkersregister — Heatr (concept voor jurist-review)

> **Technisch/organisatorisch register, geen juridisch advies.** Dit stuk beschrijft
> wat de Heatr-code wérkelijk verwerkt en naar welke subverwerkers data gaat,
> geverifieerd tegen de codebase (2026-07-19/20). Het vult het gat dat
> `docs/audit_compliance_verwerker.md` vond: het bestaande Notion-register
> "Verwerkersovereenkomsten & Subverwerkers (AVG)" beschrijft **Aerys OS**
> (patiëntproduct), niet **Heatr** (outbound lead-intelligence). Vóór de eerste
> échte kliniek-mail moet een jurist de rolverdeling, grondslag en dit register
> bevestigen.

## 1. Rolverdeling — Heatr wijkt af van Aerys OS

Bij Aerys OS is Aerys **verwerker** namens de kliniek (patiëntdata). Bij **Heatr**
is dat niet zo: Heatr verzamelt en verwerkt **prospect**-persoonsgegevens
(contactpersonen van klinieken) waarbij Aerys zélf doel en middelen bepaalt →
Aerys is hier vermoedelijk **verwerkingsverantwoordelijke**, niet verwerker.
Grondslag in de code: gerechtvaardigd belang, B2B cold outreach (Art. 6(1)(f)) —
zie `utils/gdpr_manager.generate_processing_register()`. **Jurist bevestigt de rol
en de grondslag.** Deze afwijking is de kern: twee producten, twee rollen, één
register mag ze niet door elkaar halen.

## 2. Subverwerkers-register (Heatr, code-geverifieerd)

| Subverwerker | Doel | Persoonsgegevens erheen | Regio | Vindplaats in code | Art. 9? |
|---|---|---|---|---|---|
| **Supabase** (DB + Storage) | Opslag: leads, transcripten, screenshots, alle tabellen | Volledige lead-records, gesprekstranscripten, website-screenshots | EU (Frankfurt) | `config/database.py` | Nee (B2B) |
| **Anthropic** (Claude) | AI-enrichment: openers, sector-classificatie, Vision, gesprekssamenvatting | Contactnamen, vrije sitetekst, teamnamen/bio's, **gesprekstranscripten** in prompts | US (SCC's) | `company_enrichment.py`, `sector_checker.py`, `visual_analyzer.py`, `personalization_extractor.py`, `calls/*` | Nee |
| **Bouncer** (usebouncer.com) | E-mailverificatie | E-mailadressen (persoonsgebonden bij naam@) | EU/GDPR | `enrichment/verify_api.py` | Nee |
| **Warmr** | E-mailverzending + inbox-warming | E-mail, voornaam, bedrijfsnaam, opener (custom_fields) | eigen infra | `integrations/warmr_client.py` | Nee |
| **Resend** | Operator-alerts + dagbriefing (géén prospect-mail) | Aggregaten/coverage; te verifiëren of regel-PII meegaat | US (DPF) | `utils/alert_manager.py`, briefing in `api/main.py` | Nee |
| **Google PageSpeed** | Technische site-check | Alleen bedrijfs-URL | US | `technical_checker.py:117` | Nee |
| **ip-api.com** | Server-geolocatie | Domein → geo | US | `technical_checker.py:99` | Nee |
| **RDAP** (rdap.org/SIDN) | Domein-leeftijd | Domeinnamen | EU/US | `domain_age_scraper.py` | Nee |
| **Meta Ad Library** | Ads-signaal | Bedrijfsnamen | US | `meta_ads_scraper.py` | Nee |
| **Google Places** ⏳ | Reviews als audit-bron (Tier 2, wacht op key) | Bedrijfsnaam + stad | US | `audit/places.py` | Nee |

> **Vóór productie te tekenen/verifiëren:** DPA + regio/zero-retention voor
> **Anthropic** (US, transcripten!), **Bouncer**, **Warmr**, **Resend**. De
> grensgevallen (PageSpeed, ip-api, RDAP, Meta) verwerken alleen bedrijfs-/
> domeindata — documenteren, risico laag. **Google Places toevoegen vóór de key
> in productie gaat.**
>
> Het Notion-register noemt Anthropic nog als "toekomstig / nu alleen FAKE" — dat
> is **onjuist voor Heatr**: Heatr doet vandaag duizenden echte Claude-calls.

## 3. Gegevenscategorieën die Heatr werkelijk opslaat

Naast bedrijfsdata (naam, adres, KvK, SBI, URL, Google-rating):

- **Contactpersonen**: `contact_name/first/last/tussenvoegsel`, `contact_title`,
  `contact_linkedin_url`, `contact_why_chosen`; plus `heatr_lead_contacts`
  (full_name, title, confidence) — ~1.700 rijen.
- **Vrije AI-tekst**: `personalization_hooks/observations`, `company_positioning`,
  `review_best_quote` (kan klant-/patiëntnamen uit reviews bevatten),
  `website_intelligence.team_contacts` + `.personalization`.
- **Gesprekstranscripten**: `heatr_call_records.transcript` + deelnemer-e-mails —
  het meest privacy-zware artefact.
- **Screenshots**: website-captures (kunnen gezichten/teamfoto's bevatten).
- **Verificatie-audit + netwerk-logs**: `heatr_email_verifications` (append-only),
  `heatr_website_network_log`.

Minimalisatie in code: `GDPR_MODE=strict` filtert persoonlijke e-mails bij scraping
(`website_scraper.py`); alleen role-/zakelijke adressen worden bij voorkeur
verwerkt.

## 4. Bewaartermijn en betrokkenenrechten (code-status)

- **Bewaartermijn**: er is **geen** geautomatiseerde purge-/anonimiseringsjob. De
  eerder gegenereerde belofte ("2 jaar, daarna automatisch geanonimiseerd") is per
  2026-07-19 **verwijderd** uit de gegenereerde teksten (`gdpr_manager.py`) omdat
  geen job die waarmaakte. Huidige status: *handmatig beheerd, verwijdering op
  verzoek*. Een echte retentie-purge is een aparte, nog te bouwen taak.
- **Recht op vergetelheid (Art. 17)**: `POST /gdpr/forget/{lead_id}` →
  `forget_lead`. Sinds 2026-07-19 kaart-gedreven (`GDPR_DATA_MAP`), dekt het
  volledige actuele schema (transcripten, contacten, snapshots, alle Storage-paden);
  E2E-bewezen op synthetische lead (marker nergens meer). Audittrail in
  `heatr_gdpr_log` (migratie 036, gedraaid). Origineel adres blijft in de
  suppressielijst zodat her-scrapen nooit heraanschrijfbaar maakt.
- **Recht op inzage (Art. 15)**: `POST /gdpr/export/{lead_id}` → `export_lead_data`,
  spiegelt exact dezelfde tabellen als de erase (één bron: `GDPR_DATA_MAP`).

## 5. Poort naar productie (Heatr-specifiek)

- [ ] Jurist bevestigt rolverdeling (verantwoordelijke vs. verwerker) + grondslag.
- [ ] DPA's: Anthropic (US, transcripten), Bouncer, Warmr, Resend — tekenen/regio.
- [ ] Google Places-DPA vóór de key in productie.
- [ ] Besluit retentie: purge-job bouwen óf bewaartermijn expliciet "onbepaald,
      verwijdering op verzoek" houden (huidige code-realiteit).
- [ ] Privacyverklaring afstemmen op wat sectie 3 werkelijk opslaat.
- [ ] Eigen-site-check: laadt aeryssolution.nl trackers vóór consent? (het
      terugkaats-risico uit de audit — Heatr flagt prospects hier zelf op).

---

*Bron: code-verificatie in `docs/audit_compliance_verwerker.md` (2026-07-19).
Spiegelt en corrigeert het Notion-register voor het Heatr-product. Jurist-review
verplicht vóór de eerste kliniek.*
