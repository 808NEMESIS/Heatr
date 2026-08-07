# Runbook — de eerste canary (2026-08)

Alles staat klaar. Dit is de exacte volgorde om de eerste echte mails veilig te
sturen — **eerst naar jezelf, dan pas naar leads.** De kill-switch openen is de
enige "go" en blijft jouw handeling.

---

## De canary-batch (8 leads)

Uit de 24 die AVG-veilig + launchbaar zijn én een gedetecteerde haak hebben.
Verse Vision-data (web-score), gepersonaliseerde opener, mail 1/3 al `sendable`.
**AVG-grond bij alle 8 = gepubliceerd zakelijk site-adres (art. 11.7 lid 3).**

| # | Bedrijf | Stad | Lead | Web | Haak | E-mail |
|---|---|---|---|---|---|---|
| 1 | Face Institute | Amsterdam | 66 | 54 | P1 | info@faceinstitute.nl |
| 2 | Natuurgeneeskunde Leidsche Rijn | Utrecht | 65 | 39 | Q7 | info@natuurgeneeskundeleidscherijn.nl |
| 3 | Avoria Huidtherapie | Amsterdam | 64 | 65 | P1 | info@avoriahuidtherapie.nl |
| 4 | Skin8 | Breda | 61 | 54 | Q7 | info@skin8.nl |
| 5 | Nova Huidverbetering | Breda | 60 | 61 | P1 | info@novahuidverbetering.nl |
| 6 | Huidarsenaal | Rotterdam | 60 | 52 | Q7 | info@huidarsenaal.nl |
| 7 | Perfect Skin Clinic | Leeuwarden | 59 | 63 | Q7 | info@perfectskinclinic.nl |
| 8 | Skincosmediq | Den Haag | 58 | 67 | P1 | info@skincosmediq.nl |

Mails per lead bekijken: `python3 scripts/canary_preview.py --n 8` en de receptie-
preview (mail 1/2/3) via de LeadDetail-Receptie-tab of `build_receptie_preview`.

---

## Vóór arming — checklist
- [x] Warmr-API up (`localhost:8000`, daemon `nl.aerys.warmr.api`, reboot-proof).
- [x] Warmr-capaciteit: 2 ready inboxen, `daily_campaign_target`=12 elk (24/dag).
- [x] AVG-grond geverifieerd per lead (bewijsbestand voor Spoor J).
- [ ] **Slack-workflow actief** — toggle `Heatr → Slack (#heatr-outreach)` in n8n Active uit/aan; ik doe dan de end-to-end test.
- [ ] Compliance-dossier gelezen (`docs/compliance_avg_outreach_2026-08.md`).

## Arming — stappen (in volgorde)

**1. Zelf-test eerst (niets gaat naar leads).**
In `.env`:
```
HEATR_SEND_ALLOWLIST=<jouw-eigen-adres>      # ALLEEN jij ontvangt
ENABLE_PROSPECT_SENDS=true                    # de gate die activate checkt
ENABLE_CAMPAIGN_SENDS=true                    # master kill-switch
```
Herstart de API + worker zodat de env laadt.

**2. Enroll de 8 in een draft-campagne** — via de frontend (CampagneLaunch: leads
kiezen → receptie-sequence → launch). Dit maakt een Warmr-**draft**; er gaat niks uit.

**3. Preview controleren** — de gerenderde mails + `sendable=true` per lead.

**4. Activeer (HET verzendmoment):**
```
curl -X POST https://<heatr-host>/campaigns/{campaign_id}/activate \
     -H "X-API-Key: $HEATR_API_KEY"
```
De 4 muren draaien (service-key, kill-switch, per-ontvanger her-verificatie, allowlist).
Omdat de allowlist alleen jouw adres bevat → **alleen jij krijgt de mail**. Faalt er
één ontvanger → 409, niets geactiveerd.

**5. Klopt de zelf-mail?** Zet dan `HEATR_SEND_ALLOWLIST` = de 8 canary-adressen
(of leeg = geen allowlist) en activeer opnieuw → de echte canary gaat de deur uit.

**6. Monitoren** — Slack #heatr-outreach (reactie/afmelding) + `/analytics`
(bounce >3% blokkeert automatisch, unsub >2% waarschuwt).

## Terugdraaien
- Kill-switch dicht: `ENABLE_PROSPECT_SENDS=false` (+ `ENABLE_CAMPAIGN_SENDS=false`).
- Warmr-campagne pauzeren.
- Adres op suppressielijst = nooit meer benaderd (automatisch bij afmelding/bounce).

---

*Na een geslaagde canary: Fase 3 uit `plan_naar_eerste_sends_2026-08.md`
(campaign_target rampen, allowlist eraf, receptie volledig aan, sweep-cron, KvK-backfill).*
