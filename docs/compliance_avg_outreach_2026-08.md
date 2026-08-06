# Compliance-dossier — koude B2B-e-mail (Heatr / Aerys)

**Versie:** 2026-08-06 · **Status:** intern werkdocument, geen juridisch advies ·
**Kanaal:** uitsluitend e-mail (geen post, telefoon of aan-de-deur) ·
**Doelgroep:** zakelijke ontvangers in NL — cosmetische behandelaars, alternatieve
geneeskunde, chiropractoren (`config/sectors.ACTIVE_SECTORS`).

Dit document legt vast waaróm elke send is toegestaan en welke waarborgen in code
zijn afgedwongen. Het dient tegelijk als verdedigingsdossier bij een eventuele
klacht (ACM / Autoriteit Persoonsgegevens).

---

## 1. Rechtsgrond per lead (Telecommunicatiewet art. 11.7)

Een lead mag alléén koud gemaild worden als er een expliciete grond is. Twee
zelfstandige gronden, afgedwongen in `utils/legal_form.receptie_avg_safe`:

| Grond | Wie | Wettelijke basis |
|---|---|---|
| **Rechtspersoon** | BV, NV, stichting, coöperatie, vereniging | Opt-in-regime geldt niet voor rechtspersonen (art. 11.7). |
| **Gepubliceerd zakelijk adres** | óók eenmanszaak/VOF/onbepaald | Art. 11.7 **lid 3**: adres dat de ontvanger zelf "heeft bestemd en openbaar gemaakt voor dit doel", gebruikt overeenkomstig dat doel. |

Zonder één van beide → **geblokkeerd** (fail-closed). De AVG-verwerkingsgrond is
gerechtvaardigd belang, B2B commercieel contact (Art. 6(1)(f) AVG) —
`utils/gdpr_manager.generate_processing_register`.

### Bewijs van publicatie
Voor de tweede grond geldt niet een aanname maar een **verificatie**:
`scripts/confirm_published_email.py` haalt de eigen site van de lead op en bevestigt
dat het adres er letterlijk op staat, en legt dan `email_discovery_source='website'`
vast. Per bevestigde lead is er bewijs (URL + adres) in een lokaal bewijsbestand
(`docs/avg_publication_evidence_*.json`, bewust buiten git-historie i.v.m. Art. 17).
Run 2026-08-04: 217 leads geverifieerd.

---

## 2. Waarborgen (afgedwongen in code)

| # | Waarborg | Waar |
|---|---|---|
| 1 | Grond-gate (rechtspersoon of gepubliceerd adres) | `utils/legal_form.receptie_avg_safe` |
| 2 | Afzender herkenbaar in elke mail | `config/receptie_sequence.receptie_compliance_tokens` |
| 3 | Afmeldlink in elke mail; drip stopt bij missende link | idem + `sequence_engine` compliance-hold |
| 4 | Afmelding/bounce/reply stopt direct de héle sequence | `warmr_webhook` → `stop_all_sequences_for_lead` |
| 5 | Platformbrede suppressie (e-mail/domein), org-breed | `utils/suppression`, fail-closed in de send-gate |
| 6 | Verwijderrecht (Art. 17) + inzage (Art. 15) | `utils/gdpr_manager` |
| 7 | Relevante, niet-massale outreach (canary-aanpak) | `scripts/canary_preview.py` |
| 8 | Kill-switch — geen sends tot expliciet aan | `ENABLE_CAMPAIGN_SENDS` / `ENABLE_PROSPECT_SENDS=false` |

---

## 3. Non-Mailing Indicator (NMI) — waarom niet van toepassing op e-mail

De KvK-NMI beperkt volgens de **KVK API-overeenkomst §5** het gebruik van
Handelsregistergegevens voor direct marketing **"by post or door-to-door
activities"** — dus post en aan-de-deur, niet e-mail. Ons kanaal is uitsluitend
e-mail, dat onder art. 11.7 valt (zie §1). Een NMI-filter is daarom **niet vereist**
voor de huidige opzet.

> **Voorwaarde:** dit geldt zolang het kanaal e-mail blijft. Zodra er post- of
> telefonische/aan-de-deur-benadering bijkomt, moet de NMI (en het bel-me-niet-
> regime) wél worden gerespecteerd — dan pas een NMI-gate bouwen op KvK-data.

---

## 4. Restrisico + mitigatie

- **"Overeenkomstig het doel" (art. 11.7 lid 3)** — de ACM leest dit streng: een
  contactadres gepubliceerd voor klantvragen, gebruikt voor koude sales, is
  **pleitbaar maar niet risicovrij**. Mitigatie: alleen sturen bij een aanbod dat
  relevant is voor hun dienstverlening; laag volume; per-lead bewijs bewaren.
- **Handhavingsprofiel** — ACM richt zich in de praktijk op volume-spammers,
  consumenten-targeting en NMI-negeerders. Realistische worst case voor een
  zorgvuldige B2B-operator met correcte afmelding: klacht → waarschuwing.
- **Escalatiepad** — bij een klacht óf vóór harde opschaling: juridische review
  laten bevestigen (Spoor J). Tot dan: canary + monitoren.

---

## 5. Monitoring (bij start van sends)

- Slack-melding bij elke **afmelding** en **reactie** (`utils/slack_notify`) →
  directe zichtbaarheid van signalen.
- Dagelijkse drempels: unsubscribe-rate > 2% (waarschuwing) / > 5% (kritiek),
  bounce-rate > 3% blokkeert alle sends (`utils/alert_manager.check_metric_alerts`).
- Bij een klacht: sequence stopt automatisch (§2.4); adres wordt gesuppressed;
  dossier + bewijsbestand overleggen.

---

*Onderhoud: werk dit document bij zodra de rechtsgrond-gate, het kanaal of de
KvK-integratie verandert. Bij twijfel over §1/§4: leg voor aan een jurist.*
