# Heatr — Smoke-test prompts

Vier opeenvolgende tests die het volledige outbound-pad valideren (lead → enrichment
→ Warmr-send → reply-classifier → sequence-progressie). Test 1 is render-only en
draait zonder Warmr-verbinding. Test 2/3/4 vereisen werkende Warmr-credentials +
`ENABLE_CAMPAIGN_SENDS=true`.

Volgorde: **1 → 2 → 3 → 4.** Niet door elkaar draaien.

---

## Test 2 — Live send zonder reactie

> Setup live send-test voor sequence-progressie zonder reply.
>
> **Voorwaarden controleren:**
> - `WARMR_API_URL`, `WARMR_API_KEY`, `WARMR_WEBHOOK_SECRET` gezet (anders stop)
> - Warmr campaign-template heeft `{{first_name}}` + `{{opener}}` placeholders
> - Warmr inbox warmup ≥70, cap ≥10
> - `ENABLE_CAMPAIGN_SENDS=true`
>
> **Lead:**
> - Bedrijf: gebruik een tweede test-domein dat ik nog moet aanleveren
> - Email: tweede mailadres (geef ik door)
> - `is_test_lead=true`
>
> **Wat ik doe:** ik *negeer* alle mails die binnenkomen. Niet antwoorden.
>
> **Wat jij doet:**
> 1. Trigger enrichment + push naar Warmr via `/campaigns/launch`
> 2. Confirm Mail 1 verzonden via Warmr-API status-check
> 3. Geef mij een tijdlijn: wanneer komt Mail 2 verwacht, wanneer Mail 3, wanneer status-flip naar `geen_interesse`
> 4. Na 3-4 dagen automatisch checken: is Mail 2 verzonden? Welke status? Update `docs/sessions/test-2-log.md`
> 5. Idem na Mail 3
> 6. Eindrapport: klopte de timing? Klopte de status-progressie? Werd `lead_campaign_history` correct bijgewerkt?
>
> **Test-criterium:** sequence loopt van Mail 1 → Mail 2 → Mail 3 → cold zonder menselijke interventie en zonder errors in worker-logs.

---

## Test 3 — Live send met reactie

> Setup live send-test voor reply-pad.
>
> **Voorwaarden:** zelfde als Test 2.
>
> **Lead:**
> - Bedrijf: Aerys Solution (mijn eigen)
> - Email: sami-jansema@hotmail.com
> - `is_test_lead=true`
>
> **Wat ik doe:** ik antwoord op Mail 1 binnen 24u met "Interessant, kun je meer vertellen?"
>
> **Wat jij doet:**
> 1. Trigger enrichment + send via `/campaigns/launch`
> 2. Confirm Mail 1 verzonden
> 3. Wacht op webhook van Warmr met reply
> 4. Toon mij in chat:
>    - HMAC-validatie status
>    - Classifier-tag (verwacht: `interested` of `positive`)
>    - Drafter-suggestie volledige tekst
>    - Lead-status-flip (verwacht: → `actief_gesprek`)
>    - Sequence-stop confirmation (Mail 2 mag NIET verstuurd worden)
> 5. Update `docs/sessions/test-3-log.md` met alle bovenstaande
>
> **Test-criteria:**
> - Reply binnen webhook ✓
> - Classifier-tag matcht intent
> - Drafter-suggestie is bruikbaar (niet generiek "Bedankt voor uw reactie")
> - Sequence stopt — Mail 2 wordt niet verstuurd ondanks 3-4 dagen wachten
> - Status correct bijgewerkt

---

## Test 4 — Out-of-office detection

> Setup OOO-detection-test.
>
> **Wat ik doe:** stel auto-reply in op sami-jansema@hotmail.com met "Ik ben afwezig tot [datum + 5 dagen]". Wacht op Mail 1.
>
> **Wat jij doet:**
> 1. Verstuur Mail 1 (`is_test_lead=true`)
> 2. OOO-reply komt binnen → Warmr-webhook
> 3. Toon mij:
>    - Classifier-tag (verwacht: `auto_reply` of `ooo`, NIET `interested`)
>    - Werd `recontact_after` veld gezet met return_date?
>    - Werd lead-status `wachten` of `gepauzeerd` (niet `actief_gesprek`, niet `geen_interesse`)
>    - Wordt Mail 2 onderdrukt tot na return_date
>
> **Test-criteria:**
> - OOO niet als interested geclassificeerd
> - `recontact_after` correct geparseerd uit OOO-tekst (datum-extractie werkt)
> - Sequence pauseert ipv stopt
> - Mail 2 verstuurt op return_date + 1, niet eerder
