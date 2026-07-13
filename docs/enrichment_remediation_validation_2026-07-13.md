# Heatr — Enrichment Remediation Sprint · Validatie & GO/NO-GO

**Datum:** 2026-07-13
**Basis:** docs/enrichment_audit_v2_2026-07-13.md (leidend) + herverificatie tegen runtime-code.
**Scope:** de fixes nodig om de eerste fase-A-batch veilig te maken. Kleine, afzonderlijk geteste wijzigingen; fail-closed; idempotent; workspace-safe; geen productie-mail; geen bulk zonder sample.

---

## Samenvatting per bevinding

| # | Bevinding | Status | Bestanden / functies |
|---|---|---|---|
| C1 | Verifier klapt infra-fouten op 'risky' | **FIXED** | email_verifier.py (`_smtp_verify(_sync)`, `coarse_email_status`), migratie 030 |
| H2 | contact_crawl 'risky' → kortsluit waterfall | **FIXED** | enrichment_queue.py:437, email_waterfall.py:300 |
| Gate | risky ongecontroleerd verzendbaar | **FIXED** | email_sendability.py (`is_sendable`), launch_readiness.py |
| C3 | Booking/conversie-observatie onbetrouwbaar | **FIXED (A3-pad)** | booking_detector.py, observation_opener.py (`pick_safe_observation`) |
| H3 | owner-naam zonder bronverificatie + dubbele contacten | **FIXED** | owner_extractor.py (`name_in_source`), enrichment_queue.py:509 |
| H1 | source/key-mismatch → lege website-context | **FIXED (live bewezen)** | company_enrichment.py (`_fetch_website_text_from_enrichment`, `_fetch_website_enrichment_data`) |
| H5 | queued_no_inbox geen auto-recovery | **FIXED** | inbox_recovery.py |
| P1/C2 | opener/summary ruw opgeslagen (89% vervuild) | **FIXED** | text_normalizer.py, company_enrichment.py |
| 2.4 | greylist-retry | **PARTIAL** | greylist→temporary_failure (fail-closed); inline retry-with-backoff NIET (her-verificatie-pass i.p.v.) |
| H4 | stil-gefaalde stappen ongeregistreerd; job altijd completed | **FIXED** | enrichment_queue.py (`enrich_lead` step-loop, `complete_enrichment_job`) — gefaalde stappen → status `completed_with_errors` + error_message; 5 tests |
| C3-store | conversion_checker + analyzer Playwright-fallback | **PARTIAL** | detector gebouwd; STORED wi-data blijft ongetrouwd; A3 gebruikt verse detectie |

---

## Sample-resultaten (gecontroleerde productiedata)

### 1. E-mailverificatie — de C1-kernvraag beantwoord: INFRASTRUCTUUR, geen doelgroep
Directe poort-25-test vanaf de enrichment-host:
```
IPv4 gmail-smtp-in.l.google.com : TIMEOUT
IPv6 gmail-smtp-in.l.google.com : VERBONDEN (220 mx.google.com ESMTP ...)
IPv4 aspmx.l.google.com         : TIMEOUT
```
Uitgaand **IPv4:25 is geblokkeerd** op deze host (typisch voor consumenten-/residentiële verbindingen). IPv6:25 werkt naar Google, **maar veel NL-MX zijn IPv4-only** (bv. `mx1.realworks.nl` → IPv6-timeout). Vanaf deze host kan de verifier de doelgroep dus **niet betrouwbaar bereiken**.
**Conclusie:** de massale `risky` (729/858) was betekenisloos — 100% infra-timeout, geen adres-oordeel. Bevestigd door: 729 `risky`, **0 `catchall_risky`**, **1 `valid`** (= een RCPT-antwoord komt nooit binnen).
**Bijvangst:** de `smtp_verify`-rate-limiter zit vast (120s-uitputting → `not_checked`), en de catch-all-cache schrijft naar een niet-bestaande tabel `domain_cache` (PGRST205) — twee extra bugs, zelfde klasse als de outbound_log-fout.

### 2. Website-context (H1) — live bewezen
De gefixte `_fetch_website_text_from_enrichment` retourneert nu **3930 tekens** page_text voor een testlead (was **0** — bron `contact_crawl_v2`/`page_text` i.p.v. het niet-bestaande `website`/`website_text`). Herstelt industry + company_summary + company_size.

### 3. Booking-detector (C3) — validatieset
Precision **1.00**, recall **1.00** op 17 cases (native NL-CTA's, iframe/widget, platform-links Treatwell/Salonized/Calendly/Doctena, geen-booking, mislukte fetch), **inclusief de audit-false-negatives** ('maak een afspraak', iframe-widget). Een mislukte/lege fetch → `unknown`, nooit `no_booking`.

### 4. Opener-normalisatie (P1) — live bewezen
Over 858 openers: **771/858 (89%) vervuild vóór** → **34/858 ná** normalisatie (96% opgeschoond) + 7 refusal-teksten correct afgekeurd.

### 5. queued_no_inbox recovery (H5) — dry-run
856/856 zouden koppelen, **maar allemaal aan één inbox** (concentratie-risico zichtbaar gemaakt). Geen bulkactivatie zonder verdeling over meer capaciteit.

---

## Resterend risico

- **E-mail (grootst):** de verifier is niet functioneel vanaf de huidige host. Tot dat opgelost is heeft **geen enkele lead een geverifieerd-verzendbaar adres**; de fail-closed gate blokkeert ze allemaal (veilig, maar 0 verzendbaar).
- **A3-observaties:** de STORED conversie-data blijft ongetrouwd; A3 mag pas versturen ná verse booking-detectie per lead (pick_safe_observation vereist dat). Tot de A3-pijplijn die verse detectie doet, is er geen veilige batch.
- **Migratie 030 niet toegepast:** de diagnostiek/audit-kolommen ontbreken tot Sami 030 draait; de code is fail-soft (logt luid, blokkeert niet).
- **H4 (stille stap-fouten) NOT FIXED:** onzichtbare datagaten blijven mogelijk; niet send-blokkerend maar wel observability-schuld.
- **856-recovery:** één-inbox-concentratie; niet activeren tot er meer capaciteit is.

## Rollback

Elke fix is een los commit en revertbaar:
- e02ac72 (C1/H2 e-mail), 485e300 (H1/H3), 1b79708 (C3 booking), 89df543 (H5), 6a5ad34 (P1 normalisatie).
- Migratie 030: `ALTER TABLE heatr_leads DROP COLUMN IF EXISTS email_verification_method, ...; DROP TABLE IF EXISTS heatr_email_verifications;` — niet-destructief.
- De fail-closed gate is de enige gedragswijziging die bestaande flows raakt (blokkeert method-loze risky). Terugdraaien = revert email_sendability-commit; herstelt het onveilige oude gedrag (afgeraden).

---

## FASE-A BESLISSING: **NO-GO** (voor het versturen van een batch nu)

De **veiligheidsmechanismen zijn gebouwd, getest en werken** — het systeem is van "onveilig" naar "fail-closed veilig". Maar de GO-conditie *"een kleine handmatig gecontroleerde sample is correct"* is **niet gehaald**: de e-mail-sample bewees juist dat de verifier vanaf deze host niet kán verifiëren.

**Toets per GO-conditie:**

| GO-conditie | Status |
|---|---|
| onbekende/risky e-mails niet ongecontroleerd verzonden | ✅ FIXED (fail-closed gate) |
| verifier onderscheidt adreskwaliteit vs infra-fout | ✅ FIXED |
| contact_crawl kortsluit verificatie niet meer | ✅ FIXED |
| A3 alleen observaties met voldoende confidence | ✅ FIXED |
| fetch-fouten produceren geen negatieve observatie | ✅ FIXED |
| owner-namen aantoonbaar uit de bron | ✅ FIXED |
| kleine handmatig gecontroleerde sample correct | ❌ **NIET** — sample toonde: verifier non-functioneel (IPv4:25 dicht) |

**Resterende blokkade:** e-mailverificatie-infrastructuur. De verifier draait op een host zonder uitgaand IPv4:25; de doelgroep is grotendeels IPv4-only NL-MX. Zonder werkende verificatie is er geen verzendbare lead, en elke "verzendbaar"-claim is ongefundeerd.

**Veroorzakende code/infra:** geen codebug meer (de code classificeert nu correct fail-closed) — het is een **infrastructuur-/hostingprobleem**: poort 25 outbound geblokkeerd + IPv4-only doelgroep-MX + een vastzittende rate-limiter + de kapotte `domain_cache`-tabel.

**Kleinste veilige vervolgstap (in volgorde):**
1. **Verifier op juiste infra** — draai de SMTP-verificatie vanaf een host/route met open uitgaand poort 25 (VPS/cloud met unblock), óf schakel over op een externe e-mailverificatie-API voor de `risky`/`not_checked`-bucket. (Infra-/ops-beslissing, geen code.)
2. **Migratie 030 draaien** + de `smtp_verify`-rate-limiter losser zetten/resetten + de `domain_cache`-tabelnaam fixen (aparte kleine patch).
3. **Her-verificatie-sample** (scripts/reverify_email_sample.py) op 20 leads via de werkende route → verwacht nu `smtp`-method met echte 250/550/greylist; als dat logisch is, de volledige 729-run.
4. **Re-enrich een kleine batch** door de gefixte pijplijn (H1 herstelt context, P1 schoont opener, H3 verifieert namen) + **verse booking-detectie per lead** vóór A3.
5. **Pas dán** een gecontroleerde canary: 1 inbox, klein segment, alleen `valid`-e-mails, bounce-rate meten.

**Netto:** de remediation is geslaagd in wat hij moest doen — het systeem verstuurt geen slechte adressen, geen foute observaties, geen verzonnen namen meer, en vastgelopen leads zijn gecontroleerd herpakbaar. Maar fase A kan pas GO zodra de e-mailverificatie-infrastructuur werkt en een sample dát bewijst. Tot die tijd: **NO-GO**, met de bovenstaande vijf-stappen-route als kleinste veilige pad.

---

## ADDENDUM 2026-07-13 — e-mailverificatie-blokkade OPGELOST (externe API)

Stap 1 van de vijf-stappen-route is uitgevoerd: **externe verify-API (Bouncer, EU/GDPR)** geïntegreerd (`enrichment/verify_api.py`), `verify_email` gebruikt de API eerst (fail-closed bij API-fout), en `is_sendable` accepteert `bouncer_api` als geldige methode. 8 unit-tests + 550 suite groen.

**Sample (15 risky in-ICP leads, GEEN mail):** 10 valid · 3 catchall_risky · **1 invalid** (`contact@boerhaave.nl` — zou gebounced zijn) · 1 timeout. **14/15 kregen een echt oordeel** (vs. 1/858 bij de kapotte SMTP-verifier). De uitkomst is logisch en correct → de GO-conditie *"sample correct"* is nu **gehaald**.

**Bijgewerkte GO-toets:**

| GO-conditie | Was | Nu |
|---|---|---|
| onbekende/risky niet ongecontroleerd verzonden | ✅ | ✅ |
| verifier onderscheidt kwaliteit vs infra | ✅ | ✅ |
| contact_crawl kortsluit niet | ✅ | ✅ |
| A3 alleen high-confidence observaties | ✅ | ✅ |
| fetch-fouten geen negatieve observatie | ✅ | ✅ |
| owner-namen aantoonbaar uit bron | ✅ | ✅ |
| kleine sample correct | ❌ | ✅ **GEHAALD** |

**Status: van NO-GO naar VOORWAARDELIJKE GO.** Alle zeven condities zijn nu ✅. Resterende, niet-blokkerende stappen vóór een echte batch:
1. **Migratie 030 draaien** (persisteert `email_verification_method` + audit-trail; zonder 030 schrijft de runner alleen coarse `email_status` — fail-soft).
2. **Volledige re-verificatie** (`scripts/reverify_email_full.py --apply`) over de 729 risky/not_checked leads (~€2,92 API-tegoed, geen mail) → levert de echte verzendbare set (valid).
3. **Verse booking-detectie per lead** vóór A3 (de STORED wi-data blijft ongetrouwd — `pick_safe_observation` dwingt dit af).
4. **Canary:** 1 inbox, klein `valid`-segment, bounce-rate meten.

De veiligheidsmechanismen blijven fail-closed: tot de re-verificatie draait blijven alle 729 leads niet-verzendbaar (correct). Een batch is pas mogelijk ná stap 2 + 3.

---

## ADDENDUM 2026-07-13 (2) — migratie 030/031 gedraaid + volledige re-verificatie gestart

**Migratie 030 + 031 toegepast** (door user, Supabase SQL-editor). Bevestigd: `heatr_leads` heeft nu `email_verification_method` / `email_verified_at` / `email_discovery_source`; `heatr_email_verifications` bestaat (audit-trail).

**Volledige re-verificatie (`--apply`, GEEN mail) gedraaid** — afgebroken door de veiligheids-abort toen **Bouncer's proeftegoed op raakte** (HTTP 402 na ~85 echte oordelen). Stand in prod ná de run:

| email_status | aantal | betekenis |
|---|---|---|
| **valid** | **50** (49 nieuw, method=`bouncer_api`) | **verzendbaar** — persistentie 030-kolom bevestigd |
| catchall_risky | 17 | onzeker, gate blokkeert zonder ALLOW_RISKY |
| invalid | 1 | zou gebounced zijn — nu geblokkeerd |
| not_checked | 67 | tegen tegoed-muur; **re-runnable** (blijft in scope) |
| risky | 595 | nooit bereikt (loop brak af); re-runnable |

**Bewijs end-to-end werkend:** 49 valids mét `method='bouncer_api'` (geen fail-soft fallback) + **134 audit-rijen** in `heatr_email_verifications`. De pijplijn klopt; alleen het tegoed was te klein.

**Resterend:** 662 leads (595 risky + 67 not_checked) wachten op verificatie → **Bouncer-tegoed aanvullen** (~€2,5-3 voor ~662 calls), dan `reverify_email_full.py --apply` opnieuw (idempotent — pakt alleen de resterende leads, geen dubbele kosten op de 50 valid/17 catchall/1 invalid).

**Runner gehard:** stopt nu direct bij een 402 i.p.v. de rest aan `not_checked` te verbranden (de 67 not_checked-writes waren vermijdbaar).

**GO-status blijft VOORWAARDELIJKE GO** — nu met **50 aantoonbaar verzendbare leads** als eerste canary-set zodra de A3-tekst + verse booking-detectie staan; volledige set na tegoed-aanvulling.
