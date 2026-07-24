# Receptie-campagne — go-live checklist (wat wacht op Sami)

Alles hieronder is van jou (juridisch, infra-verificatie, data, of een
autorisatie/verzendbeslissing). Per item: **precies één handeling** + **waar**.
Gesorteerd op wat de rest deblokkeert. Kill-switch dicht, code 100% klaar.

Status van de vier sloten die elke send blokkeren:
`kill-switch (dicht) · AVG-02 rechtsvorm (blok: onbepaald) · privacy-token (blok: leeg) · afmeld-token (blok: leeg)`

---

### 1. DDL 041 + 042 — deblokkeert alles hierna
- **Handeling:** plak `migrations/RUN_041_042_paste.sql` in de **Supabase SQL-editor** (project `zomdrygdcaenjnrrpcpw`) en run. Idempotent → veilig her-draaibaar (de eerdere `suppressions`-fout is gefixt: 042 mikt nu op `heatr_suppressions` + voegt enkel `kvk_number` toe).
- **Waar:** Supabase → SQL Editor.
- **Verifieer:** de sanity-`SELECT` onderaan het bestand toont de `receptie_*`-kolommen.

### 2. Backfill — DIT DOE IK, zodra 041 er is
- **Handeling:** zeg "041 staat", dan draai ik `python3 scripts/run_receptie_backfill.py --apply` (service-key mag rijen schrijven). Vult `receptie_hook_code` op de leads → de enrollment kan renderen.
- **Waar:** ik, hier. Geen send.

### 3. AVG-02 oplossen — GROOTSTE blocker (blokkeert alle 32)
Nu blokkeert de rechtsvorm-gate iedereen, want `heatr_leads.kvk_legal_form` bestaat niet in prod → alles is `onbepaald`. Eén van twee:
- **3a. Juridisch advies** dat gerechtvaardigd belang (AVG 6(1)(f)) volstaat voor deze koude B2B-zorgdoelgroep → ik versoepel de gate naar de geadviseerde policy. **Waar:** jouw jurist → dan één regel in `utils/legal_form.py`.
- **3b. OF** `kvk_legal_form`-kolom toevoegen + vullen (KvK opt-in aan) zodat rechtspersonen als `rechtspersoon` classificeren. **Waar:** migratie + `KVK_API_KEY` aan.

### 4. Privacyzin — env `RECEPTIE_PRIVACY_NOTICE`
- **Handeling:** zet de herschreven **AVG art.14-privacyparagraaf + link** (dekt scraping / Google-reviews / afgeleide eigenaarsnaam; en de "geen profilering"-claim in de huidige privacyverklaring klopt niet meer met de haakje-machine).
- **Waar:** `.env` op de server + de privacyverklaring op www.aeryssolution.nl/privacy.

### 5. Afmeldlink — env `RECEPTIE_UNSUBSCRIBE_TEMPLATE`
- **Handeling:** zet de **geverifieerde afmeldregel** met `{email}`/`{id}`-placeholders (nadat je de link zelf getest hebt).
- **Waar:** `.env` op de server.

### 6. Seed-test plain-text-MIME — go/no-go op de wire
- **Handeling:** volg `docs/receptie_seed_test_plaintext.md` (één mail naar je eigen adres, hard vergrendeld met `HEATR_SEND_ALLOWLIST`); check `Content-Type: text/plain`.
- **Waar:** jij, met je eigen adres. Geen prospect-send.

### 7. Handmatige narekening vóór launch — twee lijsten (Sami)

**7a. De 6 Q4-leads met ONbevestigd formulier — apart houden tot je ze zelf op de
site checkt** (de Fairday-val: "alleen een formulier" terwijl het een nieuwsbrief
of zoekveld kan zijn). Hun Q4-claim rust op een DOM-form dat niet als aanvraag-
formulier bevestigd is. Sluit ze uit van de eerste launch-cohort tot je bevestigt:

| kliniek | site — narekenen: is er een écht contact/afspraak-formulier? |
|---|---|
| Dr. liem clinic | https://drliemclinic.nl |
| Huidarsenaal | https://huidarsenaal.nl |
| Kliniek Dokter Frodo | https://dokterfrodo.nl |
| Kliniek Vrijdag | https://kliniekvrijdag.nl |
| Piuralift | https://piuralift.nl |
| laserskin kliniek | https://laserskinkliniek.nl |

De **4 Q4-leads mét bevestigd formulier (Q2 hit)** mogen wél mee (mits de andere
gates): DC Klinieken Groningen · Joost Kroon · SKIN Amsterdam Centrum · SR Clinic.

**7b. De 14 leads zonder bruikbare voornaam — namen handmatig opzoeken** (de
enrichment pakt structureel het domein/een initiaal i.p.v. de eigenaar; de gate
vangt het af met "Hallo,", maar voor deze founding-cohort wil je de echte naam):

| kliniek | site — voornaam eigenaar opzoeken | huidige (junk/leeg) |
|---|---|---|
| Huidarsenaal | https://huidarsenaal.nl | "C." |
| Kliniek Vrijdag | https://kliniekvrijdag.nl | "Afspraak" |
| Piuralift | https://piuralift.nl | "F." |
| SR Clinic | https://srclinic.nl | "A." |
| laserskin kliniek | https://laserskinkliniek.nl | leeg |
| Allure Laser Clinic | https://allure-laser-clinics.salonized.com | leeg |
| Beauty Clinic Nederland | https://beautyclinic.global | leeg |
| Clinic LuxaSkin | https://clinic-luxaskin.nl | leeg |
| Glow Clinic Utrecht | https://glowclinicutrecht.nl | "Glowclinicnl" |
| LaserQueens | https://laserqueens.nl | leeg |
| Knapste | https://knapste.nl | leeg |
| Mourits huidtherapie | https://mouritshuidtherapie.nl | leeg |
| HyperHidrosis Kliniek | https://hyperhidrosis-kliniek.nl | leeg |
| Skin Studios | https://skinstudios.nl | leeg |

**7c. KVEG-voornaam** — vul de voornaam uit Slack in op de KVEG-lead. **Waar:** DB.

**7d. Bedrijfsnaam-check** — `empclinics.com` had een SEO-title als naam; die is nu
geschoond naar "EMPCLINICS Emphair Haartransplantatie" (geen pipes meer, ≤4 woorden
→ komt door de gate). Eyeball 'm even; wil je 'm strakker ("EMPCLINICS"), corrigeer
de company_name in de DB. Namen die na schoonmaak nog vervuild zijn, blokkeert de
render zelf (`company_name_needs_review`).

### 8. Kill-switch aan — JOUW expliciete beslissing, als laatste
- **Handeling:** `ENABLE_PROSPECT_SENDS=true`. Dit is de enige schakel die "niets verstuurt" omzet in "sends kunnen". Ik zet 'm niet autonoom.
- **Waar:** `.env` op de server.

### 9. Launch de cohort
- **Handeling:** `POST /campaigns/launch` met `{"template_id":"faseA_receptie","lead_ids":[…de 32…]}`.
- **Waar:** jij (of ik, op jouw expliciete go, ná 1–8).

---

**Volgorde-logica:** 1 → 2 (ik) → dan 3/4/5/6 parallel → 7 → 8 → 9. Zonder 3 blijft alles geblokkeerd, ook al staan 4/5 goed. Vraag mij op elk punt om de dry-run/preview opnieuw te draaien zodra een slot dichtvalt.
