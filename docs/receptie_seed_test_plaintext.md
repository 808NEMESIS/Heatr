# Seed-test: plain-text-MIME van de receptie-mail (DELIV-02)

Doel: bevestigen dat een receptie-mail als **`Content-Type: text/plain`** de deur
uitgaat (geen HTML, geen tracking-pixel). Dit is **geen prospect-send** — één mail
naar je eigen adres, hard vergrendeld met `HEATR_SEND_ALLOWLIST` zodat niemand
anders kan ontvangen, ook niet als je je vergist in de cohort.

## Waarom dit een aparte check is
Heatr zet `content_type=text/plain` in de Warmr-push-payload (commit `302163a`),
maar of Warmr dat honoreert in de uitgaande MIME weet alleen Warmr. Deze test
bewijst het aan de ontvangstkant.

## Stappen

1. **Vergrendel op je eigen adres** (dit is het slot — alléén dit adres kan
   ontvangen, ongeacht kill-switch of cohort):
   ```bash
   export HEATR_SEND_ALLOWLIST=jouw@aeryssolution.nl
   ```

2. **Zet de compliance-tokens** (voor de test mogen het dummy-teksten zijn; de
   render-gate eist alleen dat ze niet leeg zijn):
   ```bash
   export RECEPTIE_PRIVACY_NOTICE="TEST — herkomst/privacy: aeryssolution.nl/privacy"
   export RECEPTIE_UNSUBSCRIBE_TEMPLATE="TEST — afmelden: aeryssolution.nl/uit?e={email}"
   ```

3. **Kies één test-lead met jouw eigen e-mailadres** die een `receptie_hook_code`
   heeft (draai eerst 041 + `run_receptie_backfill.py --apply`). Snelste weg: zet
   tijdelijk je eigen adres op een bestaande mailbare lead, of maak een test-lead
   met `email=jouw@aeryssolution.nl` + een domein uit de 32-cohort.

4. **Zet de kill-switch tijdelijk aan** (de allowlist blijft het echte slot):
   ```bash
   export ENABLE_PROSPECT_SENDS=true
   ```

5. **Launch de test-cohort van één** (de receptie-brug):
   ```bash
   curl -X POST "$HEATR_API_URL/campaigns/launch" \
     -H "X-API-Key: $HEATR_API_KEY" -H "Content-Type: application/json" \
     -d '{"template_id":"faseA_receptie","lead_ids":["<test_lead_id>"]}'
   ```
   De dispatcher rendert mail 1 live (gate-stack + plain-text) en pusht 'm naar
   Warmr; alléén jouw allowlist-adres ontvangt.

6. **Controleer de MIME.** Open de ontvangen mail → *Toon origineel* (Gmail) /
   *Berichtbron weergeven* (Outlook) / open de `.eml` → zoek de header:
   ```
   Content-Type: text/plain; charset="UTF-8"
   ```
   - **text/plain** → goed, DELIV-02 bevestigd.
   - **text/html** of **multipart/alternative met HTML** → Warmr honoreert de
     flag niet; dan is een Warmr-side aanpassing nodig (meld het, ik pas de
     payload-sleutel aan zodra we Warmrs contract kennen).
   Check meteen dat er **geen `<img>`-tracking-pixel** in de body zit.

7. **Draai alles terug** (belangrijk — laat de kill-switch niet aan staan):
   ```bash
   unset HEATR_SEND_ALLOWLIST RECEPTIE_PRIVACY_NOTICE RECEPTIE_UNSUBSCRIBE_TEMPLATE
   export ENABLE_PROSPECT_SENDS=false
   ```

## Veiligheid
- `HEATR_SEND_ALLOWLIST` is een harde lijst: staat een ontvanger er niet in, dan
  weigert de dispatcher de send — óók bij kill-switch aan. Zolang alleen jouw
  adres erin staat, kan deze test niets naar een prospect sturen.
- Zet je stap 7 niet terug, dan blijft de kill-switch open. Doe stap 7 altijd.
