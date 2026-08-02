# Stappenplan — van "lead-machine" naar "outbound in gebruik" (2026-08-02)

Bron: gebruiksklaar-audit 2026-08-02 (live gemeten). Kern: discovery→enrich→score draait
hands-off; verzenden is 0% in gebruik door vier stapels — AVG-02-data (0/986), het
afmeld-vangnet dat niet draait, onbewezen mail-2/3-pad, en 5/dag Warmr-capaciteit.
Echte bruikbare voorraad: 113 cosmetisch (25 met receptie-haak), niet "316".

Kritieke pad = **Spoor J (juridisch)** — start dat vandaag, de rest kan er parallel aan.

---

## FASE 0 — Sloten & ruis (vandaag, ±30 min, Sami)

| # | Stap | Hoe | Verificatie |
|---|------|-----|-------------|
| 0.1 | Afmeld-sweep-plist laden | `launchctl load ~/Heatr/deployment/launchd/nl.aerys.heatr.unsubscribe-compliance.plist` — zonder `RECEPTIE_SWEEP_CAMPAIGN_IDS` is 'ie een luide no-op (geverifieerd), dus veilig om nu te laden; IDs vullen bij launch (F5.3) | `launchctl list \| grep unsubscribe` toont de job |
| 0.2 | Dode schedules verwijderen | Zoeken → Herhaal-scrapes → × bij makelaars/Amsterdam en bouwbedrijven/Rotterdam (of Claude via `DELETE /discovery-schedules/{id}` met go) | volgende ochtend: `logs/discovery.log` zonder "Onbekende sector"-errors |
| 0.3 | n8n: mail-2/3-pad bevestigen | Open http://localhost:5678 → check dat **01-sequence-due-sends** geïmporteerd én actief is (schedule /15min). Zo niet: importeer `deployment/n8n-workflows/01-sequence-due-sends.json` en activeer | workflow toont "Active" + executions-lijst vult |
| 0.4 | Beslissing: alternatieve_geneeskunde | Live scrapet de pijplijn alt/Utrecht tweewekelijks terwijl CLAUDE.md zegt "gedeactiveerd". Kies: (a) heractiveren is bewust → CLAUDE.md updaten; (b) drift → schedule pauzeren + sector uit ACTIVE_SECTORS | één antwoord, vastgelegd (F6.4 voert door) |

## FASE 1 — AVG-02 open (rechtsvorm-data; 1-2 dagen, twee routes parallel)

**Route A — handmatig, voor het 32/25-cohort (snelst):**
1. `docs/rechtsvorm_32.csv` afmaken (KvK-zoekregister is gratis; `domain,rechtsvorm` per regel).
2. `python3 scripts/set_legal_form.py docs/rechtsvorm_32.csv` (dry-run) → output nalopen.
3. Zelfde met `--apply`.

**Route B — automatisch, voor de rest van de 113 (Claude bouwt, Sami geeft go):**
1. Claude schrijft `scripts/backfill_legal_form_kvk.py`: per lead zonder `kvk_legal_form`
   → `search_kvk` (KVK_API_KEY staat al) → `kvk_legal_form` wegschrijven. Rate-limited,
   dry-run default, alleen workspace aerys, alleen de 113-selectie. Kosten ±€0,02/call ≈ €2,50.
2. Dry-run → steekproef → `--apply` → hercheck.

**Verificatie (beide):** legal-form-gate hercheck op het cohort — `GET /leads/{id}/receptie`
blijft `sendable`, en de teller `kvk_legal_form gevuld` > 0 voor alle cohort-leads;
eenmanszaak/vof-leads worden zichtbaar geblokkeerd (dat is de bedoeling).

*Parallel optioneel:* jurist bevestigt 6(1)(f) als vangnet (Spoor J.5) — niet meer blokkerend
zodra de data er is, wél netter.

## FASE 2 — Send-pad end-to-end bewijzen (1 dagdeel; geen klinieken, alles onder allowlist)

Volgorde is dwingend:
1. **Warmr-API herstart** (leest nieuwe env/Route-C pas dan): kill + start op :8000.
2. **Seed-test plain-text (DELIV-02)** — volg `docs/receptie_seed_test_plaintext.md` exact:
   `HEATR_SEND_ALLOWLIST=info@aeryssolution.nl`, dummy compliance-tokens, tijdelijk
   `ENABLE_PROSPECT_SENDS=true`, cohort-van-één → Gmail "Toon origineel" →
   `Content-Type: text/plain` + geen tracking-`<img>` + **afmeldfooter aanwezig** (Warmr
   plakt 'm — dit bewijst keuze B echt). Daarna stap 7: alles terug op slot.
3. **Q7 test-send** naar plus-adres met verse `WARMR_TEST_CAMPAIGN_ID` — bewijst de
   meten-frame-copy én Route-C/enrollment door de echte verzendlaag.
4. **Vangnet-test**: `python3 scripts/verify_unsubscribe_compliance.py --campaign-id <testcampagne>`
   → hoort de token-rij te vinden (of luid te flaggen). Dan is F0.1 ook inhoudelijk bewezen.

**Gate:** alle vier groen vóór F5. Elke afwijking = stop + fix.

## FASE 3 — Cohort-curatie (1-2 uur mensenwerk, parallel aan F2)

1. **7a** — de 6 Q4-leads met onbevestigd formulier handmatig checken (drliemclinic,
   huidarsenaal, dokterfrodo, kliniekvrijdag, piuralift, laserskinkliniek): echt
   aanvraagformulier → mag mee; nieuwsbrief/zoekveld → uit cohort.
2. **7b/c/d** — 14 eigenaarsnamen opzoeken + in DB; KVEG-voornaam (Slack); empclinics-naam.
3. **Haarlem-lead** (`cf2c84ca`) eigen e-mailadres geven (de …biltstraat-inbox is Utrecht).

**Deliverable:** definitieve cohort-lijst met lead_ids — de input voor F5.3.

## SPOOR J — Juridisch (START NU — langste doorlooptijd, moet landen vóór F5)

1. **Privacy-paragraaf** op aeryssolution.nl/privacy herschrijven: dekt scraping/Google-
   reviews/afgeleide eigenaarsnaam; corrigeer "geen profilering". De live
   `RECEPTIE_PRIVACY_NOTICE` verwijst er al naar — de pagina moet 'm waarmaken.
2. **Afmeld-copy** juridisch checken + afmeld-URL end-to-end testen (GET-bevestiging →
   POST-suppress) op aeryssolution.nl/Vercel.
3. **DPA's**: Anthropic (verwerkt nu al echte namen/transcripten — hoogste prioriteit),
   Bouncer, Warmr, Resend. Google-Places pas bij die key.
4. **Verwerkersregister**: Heatr-sectie toevoegen (concept: `docs/verwerkersregister_heatr.md`).
5. Optioneel: 6(1)(f)-bevestiging (vangnet naast F1).

## FASE 4 — Capaciteit & infra (parallel; vóór vólume, niet vóór het cohort)

1. **Warmr-migratie** `~/warmr/migrations/2026-07-11_capaciteit_terug.sql` in de
   Warmr-Supabase (status-log/backfill draait nu fail-soft).
2. **Targets omhoog + W2-W5** in de warmr-repo (aparte sessie): 5/dag → realistisch
   verzendtempo; ready/reputatie-detectie die niet zichzelf meet.
3. **Hosting-beslissing 24/7** (Warmr + Heatr-API + publieke HEATR_BASE_URL): mag ná het
   eerste cohort @5/dag, moet vóór echte volumes. Zoom/check-up-webhook hangt hier ook aan.

## FASE 5 — Arming & eerste cohort (pas als F1+F2+F3+Spoor J groen; ±1 uur + 5 dagen verzenden)

**Pre-flight (alles aantoonbaar groen):**
- [ ] cohort-leads hebben rechtsvorm ≠ geblokkeerd (F1)
- [ ] seed + Q7 + vangnet-test groen (F2)
- [ ] cohort-lijst definitief (F3)
- [ ] privacy-pagina live + afmeld-URL getest (J1/J2)
- [ ] n8n 01 actief (F0.3)
- [ ] `WARMR_API_KEY` heeft trigger_campaigns-permissie (pre-flight-call)

**Dan, in deze volgorde:**
1. `.env`: `ENABLE_PROSPECT_SENDS=true` + `HEATR_SEND_ALLOWLIST=<cohort-adressen>` → API herstart.
2. `POST /campaigns/launch` (X-API-Key) met template `faseA_receptie` + de cohort-ids → **draft**.
3. `RECEPTIE_SWEEP_CAMPAIGN_IDS=<campaign_id>` in de sweep-plist-env → plist herladen.
4. `POST /campaigns/{id}/activate` — **de ene muur**. Per-ontvanger her-verificatie moet
   groen zijn, anders 409 met redenen.
5. **Monitoring & abort-drempel (afgesproken vóóraf):** dagelijkse check (5/dag-tempo);
   abort = pauzeer campagne bij ≥2 bounces, één spam-klacht, of afmeld-vangnet-vlag.
   Sweep-output + `GET /compliance/flags` + Warmr-stats zijn de drie meters.

## FASE 6 — Hygiëne & robuustheid (niet-blokkerend; Claude kan nu al, met go)

1. **Log-timestamps**: cron-regels voorzien van `date`-prefix + newline (nu write-only blobs).
2. **Replies-classifier ISE's** debuggen (intermitterend "Internal Server Error" — onverklaard).
3. **Leads-payload**: refetch 8s→30s of kolommen-select smaller (4,3 MB per refresh).
4. **CLAUDE.md** sector-sectie actualiseren na F0.4 (alt-geneeskunde + chiropractoren-status).
5. Optioneel: FK's `crm_tasks→leads` / `lead_timeline→leads` als migratie, zodat
   PostgREST-embeds weer kunnen (nu batch-merges — werkt, maar dit is netter).

---

## Volgorde-samenvatting (wat kan vandaag)

```
VANDAAG:   F0 (30 min) + Spoor J starten + F1-route-A CSV afmaken
           + go voor Claude: F1-route-B script + F6.1-6.3
DAG 2-3:   F1 apply + verify · F2 seed/Q7/vangnet · F3 curatie
DAARNA:    F5 pre-flight → arming → eerste cohort @5/dag
PARALLEL:  F4 (Warmr-capaciteit) — klaar vóór opschaling na het cohort
```

Eigenaarschap: F0/F1a/F3/F5-beslissingen/Spoor J = Sami · F1b/F6 = Claude (met go) ·
F2 = samen (Sami drukt, Claude leest mee) · F4 = warmr-repo-sessie.
