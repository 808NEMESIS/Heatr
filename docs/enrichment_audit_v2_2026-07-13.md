# Heatr — Adversariële Enrichment Audit V2

**Datum:** 2026-07-13
**Type:** onafhankelijke tweede audit die enrichment_audit_2026-07-11.md (V1) probeert te falsifiëren.
**Methode:** 4 parallelle adversariële code-tracers (pipeline/queue · Claude-output · e-mail · website+scoring+datakwaliteit) + eigen verificatie op de live productie-DB (858 leads).
**Legenda:** [CONFIRMED] = tegen runtime-code/DB geverifieerd · [PARTIAL] = deels · [REJECTED] = weerlegd.

---

## Kernconclusie vooraf

De V1-audit had de **feiten** grotendeels goed (dekking, vervuiling, 856 vast) maar de **oorzaken** op de twee belangrijkste punten fout — en miste de bevindingen die er voor fase A het meest toe doen. Twee dingen waar de hele fase-A-funnel op leunt zijn **onbetrouwbaar op een manier die schade doet, niet alleen data mist**:

1. **De e-mail-laag verzendt betekenisloos "risky"** — waarschijnlijk een kapotte verifier/omgeving, niet de doelgroep. Verzenden verbrandt de net-herstelde inboxen.
2. **De conversie-detectie is onbetrouwbaar** — de A3-observatie "ik zag geen online boeken" zou aantoonbaar fout naar een deel van de klinieken gaan.

Beide **blokkeren de A3-launch** in de huidige staat.

---

## CRITICAL

### C1 · E-mailverificatie produceert betekenisloos "risky" voor ~iedereen — [CONFIRMED, oorzaak V1 REJECTED]
- *Bestanden:* `enrichment/email_verifier.py`, `enrichment/email_waterfall.py`, `utils/email_sendability.py`
- *Functies:* `_smtp_verify_sync:336-393`, `_smtp_verify:301-333`, `get_best_email:145-181`, `is_sendable`
- *Feit:* 729/858 `risky`, 127 `not_found`, **1 `valid`**, **0 `catchall_risky`**.
- *Technisch:* élke connectie-fout (timeout/refused/exception) op poort 25 → `("risky","timeout"/"exception")`. Een catch-all-doelgroep zou de aparte status `catchall_risky` produceren — die is **0×**. Direct-antwoordende Google/MS-domeinen (het gros) zouden `valid` geven. 1/858 valid = de RCPT-antwoorden komen nooit binnen = **poort 25 egress geblokkeerd op de prod-host** (of IPv6/IPv4-route-mismatch), met greylisting-zonder-retry als tweede factor. In deze code betekent `risky` letterlijk "verifier kon het niet bevestigen" — niet "twijfelachtig adres".
- *V1 zei:* "doelgroep is catch-all → inconclusief → risky." → **REJECTED.** De signatuur (0 catchall) weerlegt dat.
- *Verergerend:* het diagnostische veld `verification_method` (smtp/timeout/exception) dat dit onderscheid draagt, wordt **overal weggegooid** (`get_best_email` retourneert alleen `(email,status)`; nergens gepersisteerd) → niemand kán bug-vs-realiteit nu bewijzen.
- *Impact:* met `HEATR_ALLOW_RISKY_EMAILS=true` (default) gaat de hele ongeverifieerde emmer de deur uit → bounce-rate ver boven de 2-5% provider-drempel → **directe reputatievernietiging van de net-herstelde warme inboxen** (één batch).
- *Fix:* (1) persisteer `verification_method` en her-verifieer de 729 risky → beslist bug-vs-realiteit; (2) `ALLOW_RISKY=false` tot dat bekend is; (3) verifier op een host/route met open poort 25 (of externe verify-API) + greylist-retry (451 → herprobeer).

### C2 · `personalized_opener` ruw opgeslagen, 88% markdown/meta-vervuild — [CONFIRMED]
- *Bestand:* `enrichment/company_enrichment.py` · *functie:* `generate_personalized_opener:386-509`, opslag `:506` (`return response.content[0].text.strip()`) → `:240-246`.
- *Technisch:* de "schone" paden (`opener_generator.generate_openers` met JSON-parse, `batched_enrichment`) draaien **NIET in productie** — alleen in test-/scriptbestanden. De queue draait `enrich_company`, dat de ruwe Claude-tekst met alleen `.strip()` opslaat. De prompt vráágt geen markdown (verbiedt het zelfs, `:448`) maar dwingt niets af → `# Openingszin:` / `**Beste X,**` gaat 1-op-1 de DB in. Bereikt de mail via `warmr_client.py:494` (`{{opener}}`).
- *Impact:* 758/858 openers niet plak-klaar. (A3 omzeilt dit — de observatie-opener gebruikt `personalized_opener` niet — maar het veld wordt elders/UI wél gebruikt.)
- *Fix:* normalisatie-helper na `:506` (strip headers/bold/aanhef-preamble, cap op 2 zinnen), of JSON-output afdwingen zoals de test-paden.

### C3 · Conversie-detectie te onbetrouwbaar voor A3-observaties — [NEW, CONFIRMED] ⚠️ blokkeert A3
- *Bestanden:* `website_intelligence/analyzer.py:74-93`, `website_intelligence/conversion_checker.py:16`, `scrapers/website_scraper.py:97`
- *Technisch:* de analyzer haalt HTML op met **één kale httpx-GET, zonder Playwright/JS-fallback** (anders dan `website_scraper`). Steekproef 30 in-ICP behandelaar-sites met de exacte analyzer-logica: **3/30 (10%) lege fetch** (SSL/block/timeout) → alle checks op nul bewijs; van 15 als "geen booking" bestempelde sites hadden er **12 wél een booking-indicator** die de smalle keyword-set mist (iframe/widget/Treatwell/Salonized, of "maak een afspraak" vs "afspraak maken"). Twee verschillende booking-detectors met verschillende fingerprints.
- *Impact:* de A3-mail "op jullie site zag ik zo snel geen manier om online een afspraak in te plannen" zou **feitelijk fout** naar een groot deel van de klinieken gaan — precies het geval waarin geen mail beter is dan de mail. De hedge ("zag ik zo snel geen…") verzacht maar redt het niet: als ze wél boeken, lees je als iemand die niet goed keek.
- *Fix vóór A3:* (a) conversie-detectie op de Playwright-fetch draaien (zoals website_scraper) i.p.v. kale httpx; (b) booking-keyword-set verbreden + iframe/widget-detectie; (c) A3 alleen observaties gebruiken met hoog vertrouwen, of de observatie verschuiven naar signalen die betrouwbaarder zijn (bv. "geen klikbaar telefoonnummer op mobiel" is robuuster dan booking); (d) tot dan: A3-observatie handmatig steekproeven vóór verzenden.

### C4 · Geen enkele organische lead haalt score ≥65 — [CONFIRMED mechanisme; V1-oorzaak PARTIAL]
- *Bestanden:* `scoring/lead_scoring.py:179-185,121,192-194`, `scoring/icp_matcher.py`, `config/scoring_weights.py`
- *Feit:* score-verdeling (n=858): mediaan 40, max 85. ≥65: **1 lead — en dat is een `is_test_lead` met een handmatig gezette 85**, niet door `score_lead` berekend. Organische bovengrens ~50-52.
- *Technisch:* de draaiende formule = `fit(0-40)+data_quality(0-20)+reachability(0-25)+personalization(0-15)`. De `LEAD_SCORING_FACTORS`-dict in `scoring_weights.py` (has_valid_email=25 etc.) is **dode code** — nergens geïmporteerd. Elke dimensie is structureel afgekapt: fit≤20 (icp_match mediaan 0.38 — `sbi_code` NULL voor alle 858 want KvK uit, `company_size` onbekend voor alle, `industry` leeg), reachability≤19 (`valid` email 1/858 → iedereen 6 i.p.v. 10 pt), data_quality≤12.8, personalization≈2 (hooks gevuld voor 6/858). Tweede poort `MIN_ICP_MATCH_FOR_WARMR=0.6` blokkeert los daarvan 857/858.
- *V1 zei:* "drempel volstrekt misgekalibreerd." → **PARTIAL.** Geen optel-bug; de drempel ligt boven het bereikbare max, maar de dieper liggende oorzaak is dat **vier score-inputs systematisch leeg/laag zijn** (KvK uit, industry leeg via C-bug H1, hooks leeg, valid-email onbereikbaar via C1) + een tweede icp≥0.6-poort. Drempel verlagen alleen lost niets op.
- *Impact:* het send-to-warmr-pad (score≥65 + icp≥0.6) weigert de facto élke lead. Voor A3 niet-blokkerend (die heeft geen score≥65 nodig), maar het scoremodel is nu betekenisloos als selectiepoort.
- *Fix:* herkalibreer beide drempels (~45-48 / ~0.4) én repareer de icp-inputs (industry via H1, size), óf herweeg de dimensies. Niet vóór A3 nodig.

---

## HIGH

### H1 · Source/key-mismatch breekt industry + company_summary + company_size in één bug — [NEW, CONFIRMED; V1-oorzaak REJECTED]
- *Bestand:* `enrichment/company_enrichment.py` · *functies:* `_fetch_website_text_from_enrichment:607-635`, `_fetch_website_enrichment_data:638-665`
- *Technisch:* deze halen website-tekst op met `source="website"` + key `website_text`, maar de crawler slaat op onder `source="contact_crawl_v2"` + key `page_text` (`enrichment_queue.py:461,468`). **Beide kloppen niet → retourneert structureel `""`.** Daardoor krijgen `industry` (Claude-fallback uitgehongerd), `company_summary` en `company_size` allemaal lege website-context.
- *V1 zei:* "industry dood door kapotte JSON-parsing." → **REJECTED.** De parse werkt (strikte allowlist, anti-hallucinatie); de oorzaak is de fetch-mismatch.
- *Impact:* industry leeg 98%, summary uitgehongerd, size onbekend (→ voedt C4's lage icp_match).
- *Fix:* `source="website"`→`"contact_crawl_v2"` en `"website_text"`→`"page_text"` in beide helpers. Herstelt drie velden in één klap.

### H2 · `contact_crawl` hardcodeert `risky` + kortsluit de verificatie-waterval — [NEW, CONFIRMED]
- *Bestanden:* `job_queue/enrichment_queue.py:436-437`, `enrichment/email_waterfall.py:300-308`
- *Technisch:* contact_crawl zet bij een gevonden adres `email_status="risky"` **zonder verificatie**. `email_waterfall` (latere stap) slaat zichzelf dan volledig over: `if existing_status in ("valid","risky") → skip`. SMTP/pattern/Google-fallback draaien nooit; `email_discovery_method` wordt `pre_existing`.
- *Impact:* een ongeverifieerd gecrawld adres wordt de definitieve lead-email — dit is een grote bron van de C1-"risky"-massa, en het schendt de "verified data"-eis.
- *Fix:* contact_crawl niet hardcoderen op `risky` (bv. `not_checked`), of de waterval niet laten short-circuiten op een onopgevraagd/onnageverifieerd adres.

### H3 · owner_extract: non-idempotent (dubbele contacten) + geen naam-verificatie (hallucinatie) — [NEW, CONFIRMED]
- *Bestand:* `job_queue/enrichment_queue.py:511-535`, `enrichment/owner_extractor.py`
- *Technisch:* (a) `contacts.insert(...)` per teamlid zónder dedup → bij een resume/re-run dubbele rijen. Live: **77 leads met >3 contactrijen** — de duplicatie is al gebeurd. (b) Geen check dat de geëxtraheerde `name` daadwerkelijk in `page_text` voorkomt → een gehallucineerde naam passeert alle filters en wordt direct `contact_name`/`contact_first_name`, en stroomt de mail-aanhef in ("Hoi {verzonnen naam},").
- *Impact:* dubbele CRM-contacten + risico op mail met een verzonnen persoonsnaam.
- *Fix:* dedup op (lead_id, naam/email) vóór insert; substring-verificatie van de naam tegen page_text + confidence-floor vóór het overschrijven van contact_name.

### H4 · Stil-gefaalde enrichment-stappen worden nergens geregistreerd; job altijd `completed` — [NEW, CONFIRMED]
- *Bestand:* `job_queue/enrichment_queue.py:328-368`
- *Technisch:* de per-stap try/except logt alleen een warning; een gefaalde stap wordt niet aan `steps_done` toegevoegd, nergens als "failed" vastgelegd, en de job draait daarna **altijd** `complete_enrichment_job` → `completed`. Geen `steps_failed`, geen gate op `data_verification`. Een stil-gefaalde stap wordt nooit herprobeerd; de data ontbreekt permanent zonder signaal.
- *Impact:* onzichtbare datagaten (dit is precies hoe C1/H1 lang onopgemerkt bleven).
- *Fix:* registreer gefaalde stappen (bv. `steps_failed`-kolom); markeer een job met kritieke-stap-fout niet als volledig `completed`.

### H5 · `queued_no_inbox` heeft geen automatisch herstelpad — [V1 P4 mechanisme CONFIRMED; "geen bug" PARTIAL]
- *Bestand:* `job_queue/enrichment_queue.py:818-826,137-139,361-364`
- *Technisch:* `inbox_selection` (laatste stap) zet `status='queued_no_inbox'` bij 0 ready inboxes (856/858 leads). `company_enrichment` heeft ondertussen `enrichment_version=1` gezet. `queue_all_unenriched_leads` selecteert `status='discovered' AND enrichment_version=0` → deze leads zijn op **beide** criteria uitgesloten. Niets re-runt `inbox_selection`. Herstel enkel via handmatig re-enqueue-endpoint met confirm-phrase. (Terzijde: de slot-status-bump op `:361-364` is dode code — `inbox_selection` heeft de status al van `discovered` weggehaald.)
- *V1 zei:* "puur inbox-fuik-gevolg, geen bug." → **PARTIAL.** Directe oorzaak = inbox-schaarste (by design), maar het permanent stranden zonder auto-recovery is een missing-recovery-gat.
- *Impact:* nu de inboxen ready zijn (Warmr-fix), bewegen de 856 leads **niet automatisch** — handmatige re-enqueue nodig.
- *Fix:* re-driver die `queued_no_inbox`-leads opnieuw door `inbox_selection` haalt zodra er capaciteit is.

---

## MEDIUM

- **M1 · Cache-vergiftiging + geen retry** [NEW]. `utils/claude_cache.py:199` cachet een non-lege refusal ("Ik kan niet helpen…") 7-30 dagen als geldig; `company_enrichment` heeft geen `anthropic_retry` → een 529 landt als `""` opgeslagen. *Fix:* refusal-detectie vóór cache; company_enrichment door de retry-wrapper.
- **M2 · Prompt-injectie via `page_text`** [NEW]. Gescrapte tekst gaat ongesanitiseerd in de prompts (owner/summary/opener) binnen `"""`-delimiters — een site kan de delimiter breken of instructies injecteren. Enum-velden (treatment/archetype) vangen het af; vrije-tekst-velden (owner-naam, summary, opener) niet. *Fix:* `page_text` saniteren/`"""` neutraliseren.
- **M3 · Catch-all token-mismatch** [NEW]. Drie spellingen: `catchall_risky` (verifier) vs `catchall` (sendability) vs `catch_all` (scoring/main). Een `catchall_risky`-lead valt door naar niet-sendable (veilig, maar de sendability-config liegt). *Fix:* één token.
- **M4 · Vision silent fallback→5** [NEW, latent]. `visual_analyzer.py:227,245-246`: een regex-parse-fout wordt een plausibele middenscore 5 i.p.v. een gemarkeerde fout. Nu moot (vision draait niet), latent bij aanzetten. *Fix:* parse-fout scheiden van echte 5.
- **M5 · Twee niet-identiek geordende default-stap-lijsten** [NEW]. `enrichment_queue.py:78-94` vs `254-261` — divergentie-valstrik. *Fix:* één bron.
- **M6 · Legacy + duplicaten + stale** [V1 P3 CONFIRMED + nieuw]. 113 out-of-ICP (58 makelaars + 55 bouw) exact bevestigd; **7 dubbele e-mailadressen** (unique dekt domein, niet email); enrichment tot ~11 weken oud (oudste `analyzed_at` 2026-04-27). *Fix:* legacy archiveren, email-unique overwegen, re-enrich-TTL.

---

## Wat VEILIG en GOED is (niet aankomen)

- **`treatment_focus` + `archetype`** — sterkste guardrails: JSON-parse + `temperature=0.0` + allowlist/enum-validatie + parse-vóór-cache. Hallucinatie wordt gefilterd. [CONFIRMED VEILIG]
- **Queue CAS-claim** race-veilig; **workspace-filter** (fase 4) sluit cross-tenant claims uit. [CONFIRMED]
- **Reaper-cron draait** — elke 10 min via crontab (niet in repo, wél live) → permanente `running`-stranders worden opgeruimd. [CONFIRMED, corrigeert Agent A's zorg]
- **De gevaarlijke ongebonden worker-loop is dode code** — live draait de veilige één-job-per-call variant. [CONFIRMED — P2-2 uit lifecycle-audit afgewaardeerd]
- **Kosten verwaarloosbaar** (~€0,008/lead, €6,35 totaal). [CONFIRMED]

---

## Slotconclusies

### 1. Welke V1-conclusies volledig overeind blijven
- Opener-vervuiling 88% (P1) — CONFIRMED, oorzaak nu exact.
- Dekkingscijfers + "100% is soms schijn (lege strings)" — CONFIRMED.
- 113 legacy out-of-ICP (P3) — CONFIRMED exact.
- Vision-laag draait niet (P6) — CONFIRMED (visual_score NULL voor alle 807).
- 856 in `queued_no_inbox` (P4) — mechanisme CONFIRMED.
- Kosten verwaarloosbaar — CONFIRMED.

### 2. Welke V1-conclusies onjuist of onvolledig zijn
- **P2-oorzaak "doelgroep catch-all → risky" — REJECTED.** Het is een kapotte verifier/omgeving (0 catchall, 1 valid = poort 25/route). Ander fix-pad.
- **"Score-drempel misgekalibreerd" — PARTIAL.** Geen optel-bug; de score is legitiem laag door vier uitgehongerde inputs + een tweede icp≥0.6-poort; de config-dict waar V1 impliciet op leunt is dode code; de 1 lead ≥65 is een testlead.
- **"Industry dood door JSON-parsing" — REJECTED.** Het is de source/key-mismatch (H1) die óók summary + size breekt.
- **"PageSpeed 0 want key ontbreekt" (P6-subclaim) — REJECTED.** Key werkt; technical_score haalt 25.
- **"492 leads bruikbaar voor A3" — ONDERMIJND.** De conversie-data waarop A3-observaties leunen is onbetrouwbaar (C3).
- **"856 queued = geen bug" — PARTIAL.** Missing-recovery-gat (H5).

### 3. Nieuwe bevindingen (niet in V1)
Source/key-mismatch (H1) · contact_crawl risky-kortsluiting (H2) · owner-hallucinatie + dubbele contacten (H3) · stil-gefaalde stappen ongeregistreerd (H4) · geen auto-recovery queued_no_inbox (H5) · cache-vergiftiging + geen retry (M1) · prompt-injectie (M2) · catch-all token-mismatch (M3) · vision silent-5 (M4) · dubbele default-lijsten (M5) · 7 dubbele emails · geen organische lead ≥65 (testlead) · `verification_method` overal weggegooid · A3-conversie-observaties onbetrouwbaar (C3).

### 4. Wat VÓÓR productie (verzenden) opgelost moet
1. **C1 — e-mailverificatie:** persisteer `verification_method`, her-verifieer, zet `ALLOW_RISKY=false` tot bekend is of risky bug of realiteit is. **Zonder dit verbrandt de eerste batch de inboxen.** #1 blokker.
2. **C3 — conversie-detectie op Playwright + bredere booking-detectie**, of A3-observaties beperken tot robuuste signalen (klikbaar nummer) + handmatige steekproef. **Blokkeert de A3-mailinhoud.**
3. **H2 — contact_crawl risky-kortsluiting** (voedt C1).
4. **H3 — owner-naam-verificatie** (verzin geen aanhef-naam) als contact_name in de mail komt.
5. **C2 — opener-normalisatie** als `personalized_opener` ergens verzonden wordt (A3 gebruikt het niet).

### 5. Wat veilig kan wachten
- C4 scoring-herkalibratie (A3 heeft geen score≥65 nodig; wél nodig vóór send-to-warmr-pad).
- H1 source/key-fix (kwaliteit, geen blokker voor A3-observaties die uit checks komen — maar wel snel & hoog rendement).
- H4/H5 (observability/recovery — belangrijk, niet send-blokkerend).
- M1-M6 (cache, injectie, tokens, legacy, dubbele lijsten) — hygiëne.
- M6 legacy-opschoning (snel, verkleint ruis).

---

*Kern: de enrichment produceert goedkoop en op schaal, en de discovery/firmografie + treatment/archetype-classificatie zijn solide. Maar de twee pijlers onder fase A — betrouwbare e-mail-deliverability en betrouwbare conversie-observaties — zijn beide op dit moment onbetrouwbaar op een manier die actief schaadt. Dat gate't de A3-launch tot C1 en C3 zijn geadresseerd.*
