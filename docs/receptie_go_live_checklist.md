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

### 7. KVEG-voornaam — niet blokkerend
- **Handeling:** vul de voornaam uit Slack in op de KVEG-lead (anders krijgt KVEG "Hallo,"). 14/32 leads hebben sowieso geen bruikbare voornaam → "Hallo,"; dit verbetert er één.
- **Waar:** DB / enrichment.

### 8. Kill-switch aan — JOUW expliciete beslissing, als laatste
- **Handeling:** `ENABLE_PROSPECT_SENDS=true`. Dit is de enige schakel die "niets verstuurt" omzet in "sends kunnen". Ik zet 'm niet autonoom.
- **Waar:** `.env` op de server.

### 9. Launch de cohort
- **Handeling:** `POST /campaigns/launch` met `{"template_id":"faseA_receptie","lead_ids":[…de 32…]}`.
- **Waar:** jij (of ik, op jouw expliciete go, ná 1–8).

---

**Volgorde-logica:** 1 → 2 (ik) → dan 3/4/5/6 parallel → 7 → 8 → 9. Zonder 3 blijft alles geblokkeerd, ook al staan 4/5 goed. Vraag mij op elk punt om de dry-run/preview opnieuw te draaien zodra een slot dichtvalt.
