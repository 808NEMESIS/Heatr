# DEPLOY-CHECKLIST — Heatr Outbound Safety (2026-07-10)

Alles wat JIJ moet doen staat in dit bestand, met de volledige SQL er al in.
Open het Supabase-dashboard van het Heatr-project, ga naar **SQL Editor**,
en werk de stappen VAN BOVEN NAAR BENEDEN af. Per stap: plak het SQL-blok,
klik Run, en vergelijk met "VERWACHT".

FAALT ER IETS? Stop bij die stap, ga NIET verder, en stuur mij de foutmelding.

Na stap 9 zeg je "klaar" tegen Claude — die herstart dan de services,
verifieert alles en pusht de commits naar GitHub.

═══════════════════════════════════════════════════════════════════════════
STAP 1 — NULMETING (baseline, vóór alles)
═══════════════════════════════════════════════════════════════════════════
Run de 7 queries hieronder (mag in één keer; de editor toont dan alleen het
laatste resultaat — run ze los als je alle getallen wilt noteren).
Belangrijkste check: query 1 hoort **0** te geven (dat bevestigt de
ledger-bug die we fixen). Noteer de uitkomsten in docs/pre_fix_baseline_2026-07.md.

```sql
-- 1. Ledger-omvang (verwacht: 0)
SELECT COUNT(*) AS outbound_log_rows FROM heatr_outbound_log;

-- 2. Enrollment-tabel-omvang (verwacht: 0)
SELECT COUNT(*) AS lch_rows FROM heatr_lead_campaign_history;

-- 3. Unsubscribed op email_status maar status niet geblokkeerd
SELECT COUNT(*) AS unsub_desync
FROM heatr_leads
WHERE email_status = 'unsubscribed'
  AND status NOT IN ('unsubscribed', 'forgotten', 'disqualified');

-- 4. Gebounced maar nog campaign-eligible
SELECT COUNT(*) AS bounce_eligible
FROM heatr_leads
WHERE email_status = 'bounced'
  AND status NOT IN ('unsubscribed', 'forgotten', 'disqualified');

-- 5. Reply-classificaties laatste 30 dagen
SELECT classification, COUNT(*)
FROM heatr_reply_inbox
WHERE received_at > now() - interval '30 days'
GROUP BY classification ORDER BY 2 DESC;

-- 6. Sends per dag, laatste 14 dagen
SELECT date_trunc('day', created_at) AS dag, COUNT(*)
FROM heatr_lead_timeline
WHERE event_type = 'email_sent' AND created_at > now() - interval '14 days'
GROUP BY 1 ORDER BY 1 DESC;

-- 7. Vergeten leads met de oude gedeelde placeholder
SELECT COUNT(*) AS forgotten_placeholder
FROM heatr_leads WHERE email = 'verwijderd@anoniem.nl';
```

═══════════════════════════════════════════════════════════════════════════
STAP 2 — MIGRATIE 022: outbound-ledger UNIQUE
═══════════════════════════════════════════════════════════════════════════
STAP 2a — eerst ALLEEN deze preflight. VERWACHT: 0 rijen ("Success. No rows returned").

```sql
SELECT workspace_id, idempotency_key, COUNT(*) AS n
FROM heatr_outbound_log
WHERE status IN ('in_flight', 'completed')
GROUP BY workspace_id, idempotency_key
HAVING COUNT(*) > 1;
```

STAP 2b — gaf 2a écht 0 rijen? Plak dan het volledige bestand:

```sql
-- 022_outbound_ledger_unique.sql — Werkpakket A: Outbound Safety Foundation.
--
-- Context: de dispatcher schreef tot nu naar de ONGEPREFIXTE tabelnaam
-- "outbound_log" (die niet bestaat) — elke ledger-write faalde stil en
-- idempotency was fail-open (audit v2, P0-1). De code-fix voegt
-- "outbound_log" toe aan _HEATR_TABLES; deze migratie levert de
-- database-garantie die de code-fix pas veilig maakt:
--
--   1. een PARTIAL UNIQUE index op (workspace_id, idempotency_key) voor
--      actieve records (in_flight/completed) — een gewone index is géén
--      idempotency-garantie;
--   2. een CHECK op de toegestane statussen (incl. het nieuwe
--      reserveringsmodel: in_flight → completed | failed_retryable |
--      failed_terminal);
--   3. een bijgewerkte tabel-COMMENT: reserveringsrijen worden in-place
--      gefinaliseerd; alle overige rijen blijven append-only.
--
-- Draai dit VÓÓR de code-deploy. De code-fix mag niet live zonder deze
-- index (naam repareren zonder UNIQUE = zichtbaar werkend maar nog
-- steeds racegevoelig).

-- ── A. Preflight: bestaande duplicaten? ─────────────────────────────────────
-- Verwacht: 0 rijen. De tabel is door de prefix-bug nooit succesvol
-- beschreven; een niet-lege uitkomst betekent dat er via een ander pad
-- geschreven is — dan NIET doorgaan met sectie B maar eerst opschonen.
SELECT workspace_id, idempotency_key, COUNT(*) AS n
FROM heatr_outbound_log
WHERE status IN ('in_flight', 'completed')
GROUP BY workspace_id, idempotency_key
HAVING COUNT(*) > 1;

-- ── B. Partial UNIQUE op actieve records ────────────────────────────────────
-- Alleen in_flight/completed tellen mee: een failed_retryable-rij valt uit
-- het predicaat zodat een bewuste retry een nieuwe reservering kan inserten.
CREATE UNIQUE INDEX IF NOT EXISTS uq_outbound_log_active_key
  ON heatr_outbound_log (workspace_id, idempotency_key)
  WHERE status IN ('in_flight', 'completed');

-- ── C. Status-CHECK (tabel is leeg → direct valide) ─────────────────────────
-- 'failed' blijft toegestaan als legacy-waarde (pre-022 code schreef die);
-- nieuwe code schrijft failed_retryable/failed_terminal.
ALTER TABLE heatr_outbound_log
  DROP CONSTRAINT IF EXISTS heatr_outbound_log_status_check;
ALTER TABLE heatr_outbound_log
  ADD CONSTRAINT heatr_outbound_log_status_check CHECK (status IN (
    'in_flight', 'completed', 'failed', 'failed_retryable', 'failed_terminal',
    'blocked_compliance', 'blocked_killswitch', 'skipped_duplicate'
  ));

-- ── D. Semantiek-comment bijwerken ──────────────────────────────────────────
COMMENT ON TABLE heatr_outbound_log IS
  'Outbound side-effect-ledger + idempotency-reservering. Een dispatch INSERT eerst een in_flight-rij (eigenaarstoewijzing via uq_outbound_log_active_key); die ene rij wordt in-place gefinaliseerd naar completed/failed_retryable/failed_terminal. Alle overige rijen (blocked_*, skipped_duplicate) zijn append-only en worden nooit gemuteerd of verwijderd.';

-- ── E. Verificatie (draai NA A-D) ───────────────────────────────────────────
-- Verwacht: 1 rij met indexdef die het WHERE-predicaat bevat.
SELECT indexname, indexdef FROM pg_indexes
WHERE schemaname = 'public' AND indexname = 'uq_outbound_log_active_key';
```

VERWACHT: laatste resultaat toont 1 rij met indexname `uq_outbound_log_active_key`.

═══════════════════════════════════════════════════════════════════════════
STAP 3 — MIGRATIE 023: email_status-CHECK-fix
═══════════════════════════════════════════════════════════════════════════
```sql
-- 023_email_status_check_fix.sql — Fase 2 (suppression/GDPR), hotfix-deel.
--
-- PROBLEEM (actief in productie sinds 021 sectie D): de CHECK
-- chk_heatr_leads_email_status staat als NOT VALID — dat slaat alleen de
-- validatie van BESTAANDE rijen over; NIEUWE writes worden WEL afgedwongen.
-- De toegestane set ('valid','risky','catchall_risky','invalid',
-- 'not_checked','not_found','verified','catch_all') mist waarden die de
-- code daadwerkelijk schrijft:
--
--   - 'bounced'       ← bounce-webhook (api/main.py) — write FAALT nu
--   - 'unsubscribed'  ← unsubscribe-webhook — write FAALT nu
--   - 'pending'       ← verifier-tussenstand
--   - 'catchall'      ← verifier (naast 'catch_all' — beide komen voor)
--   - 'role_email'    ← sendability-vocabulaire
--   - 'blocked'       ← sendability-vocabulaire
--
-- Gevolg: elke bounce/unsubscribe via de Warmr-webhook werd op de
-- leads-update afgewezen door de constraint, de except-tak logde en de
-- suppressie belandde NERGENS. Dit repareert de set naar het volledige
-- code-vocabulaire (bron: utils/email_sendability.py + webhook-handlers).
--
-- Draai dit VÓÓR de fase-2 code-deploy (de code gaat 'bounced'/'unsubscribed'
-- juist bewuster schrijven).

-- ── A. Preflight: welke waarden staan er nu écht in? ────────────────────────
SELECT email_status, COUNT(*) FROM heatr_leads
WHERE email_status IS NOT NULL
GROUP BY email_status ORDER BY 2 DESC;

-- ── B. CHECK vervangen door het volledige code-vocabulaire ──────────────────
ALTER TABLE heatr_leads
  DROP CONSTRAINT IF EXISTS chk_heatr_leads_email_status;
ALTER TABLE heatr_leads
  ADD CONSTRAINT chk_heatr_leads_email_status
  CHECK (email_status IS NULL OR email_status IN (
    'valid', 'verified',                      -- sendable
    'risky', 'catchall', 'catch_all', 'catchall_risky', 'role_email',
    'pending', 'not_checked',                 -- tussenstand
    'invalid', 'not_found',                   -- niet-sendable
    'bounced', 'unsubscribed', 'blocked'      -- suppressie
  )) NOT VALID;

-- ── C. Valideren zodra A geen onbekende waarden meer toont ──────────────────
-- ALTER TABLE heatr_leads VALIDATE CONSTRAINT chk_heatr_leads_email_status;

-- ── D. Verificatie: mag 'bounced' er nu in? (verwacht: geen fout) ───────────
-- Test in een transactie die je terugdraait:
-- BEGIN;
--   UPDATE heatr_leads SET email_status='bounced'
--   WHERE id = (SELECT id FROM heatr_leads LIMIT 1);
-- ROLLBACK;
```

VERWACHT: geen fout. (Sectie A toont onderweg de huidige email_status-waarden.)

═══════════════════════════════════════════════════════════════════════════
STAP 4 — MIGRATIE 024: suppressielijst
═══════════════════════════════════════════════════════════════════════════
```sql
-- 024_suppressions.sql — Fase 2 (PR 7): centrale cross-workspace suppressie.
--
-- WAAROM: suppressie leefde als mutabele velden op de lead-rij, per
-- workspace. Dezelfde persoon in workspace B is een andere rij → een
-- unsubscribe in A onderdrukte niets in B (audit v2 P0-2). Deze tabel is
-- de platformbrede, append-only suppressielijst op genormaliseerd
-- e-mailadres, geraadpleegd door de dispatcher vóór élke prospect-send.
--
-- PRIVACY: de tabel bevat e-mailadressen (grondslag: wettelijke plicht om
-- unsubscribe/erasure na te leven — een suppressielijst is daarvoor de
-- standaard). De gate antwoordt callers alleen "globally_suppressed",
-- NOOIT welke tenant/campagne de suppressie aanmaakte.
--
-- Draai VÓÓR de fase-2 code-deploy (de dispatcher gaat deze tabel
-- fail-closed bevragen: tabel afwezig = prospect-sends geblokkeerd).

CREATE TABLE IF NOT EXISTS heatr_suppressions (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    normalized_email     TEXT NOT NULL,      -- lower(trim(email))
    domain               TEXT,               -- alleen bij domain_block
    suppression_type     TEXT NOT NULL CHECK (suppression_type IN (
        'unsubscribe', 'hard_bounce', 'complaint', 'forgotten',
        'manual_global', 'domain_block'
    )),
    reason               TEXT,
    source               TEXT NOT NULL,      -- warmr_webhook | reply_classifier | gdpr_forget | operator
    source_workspace_id  TEXT,               -- herkomst — NOOIT terug naar callers lekken
    lead_id              UUID,
    campaign_id          TEXT,
    event_id             TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at           TIMESTAMPTZ,        -- NULL = actief; alleen operator mag revoken
    created_by           TEXT
);

-- Eén ACTIEVE suppressie per adres (platformbreed). Een revoke + nieuwe
-- suppressie geeft een nieuwe rij → historie blijft bewaard (append-only).
CREATE UNIQUE INDEX IF NOT EXISTS uq_suppressions_active_email
  ON heatr_suppressions (normalized_email)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_suppressions_domain
  ON heatr_suppressions (domain) WHERE domain IS NOT NULL AND revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_suppressions_recent
  ON heatr_suppressions (created_at DESC);

COMMENT ON TABLE heatr_suppressions IS
  'Platformbrede (cross-workspace) suppressielijst op genormaliseerd e-mailadres. Append-only: revoken = revoked_at zetten, nooit deleten. Geraadpleegd door utils/suppression.check_suppressed via de outbound-dispatcher (fail-closed).';

-- ── outbound_log status-CHECK uitbreiden met blocked_suppression ────────────
ALTER TABLE heatr_outbound_log
  DROP CONSTRAINT IF EXISTS heatr_outbound_log_status_check;
ALTER TABLE heatr_outbound_log
  ADD CONSTRAINT heatr_outbound_log_status_check CHECK (status IN (
    'in_flight', 'completed', 'failed', 'failed_retryable', 'failed_terminal',
    'blocked_compliance', 'blocked_killswitch', 'blocked_suppression',
    'skipped_duplicate'
  ));

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT indexname FROM pg_indexes
WHERE schemaname='public' AND indexname='uq_suppressions_active_email';
```

VERWACHT: laatste resultaat toont `uq_suppressions_active_email`.

═══════════════════════════════════════════════════════════════════════════
STAP 5 — MIGRATIE 025: tracking-enrollments
═══════════════════════════════════════════════════════════════════════════
```sql
-- 025_tracking_enrollments.sql — Fase 3 (PR 9): tracking-only enrollments.
--
-- ADR-001: Warmr dript, Heatr trackt. Launch gaat per gepushte lead één
-- rij schrijven met send_owner='warmr' en status='active'. Deze kolommen
-- + constraints maken dat veilig:
--
--   - send_owner: de harde grens tegen dubbele drip — get_due_sends
--     (Model B) selecteert uitsluitend send_owner='heatr';
--   - UNIQUE (workspace_id, lead_id, campaign_id): her-enrollment van
--     dezelfde lead in dezelfde campagne is een no-op, nooit een stille
--     overschrijving. Een bewuste her-benadering = nieuwe campagne
--     (launch maakt per bucket een nieuwe Warmr-campaign_id);
--   - status-CHECK: legt het volledige runtime-vocabulaire vast en stopt
--     de vocabulaire-drift tussen supabase_schema.sql en migratie 009
--     (audit v2 §6: twee onverenigbare definities).
--
-- Tabel is vandaag LEEG (audit v2 F2: geen enkele insert bestond) — alle
-- constraints zijn direct veilig. Draai VÓÓR de fase-3 code-deploy.

ALTER TABLE heatr_lead_campaign_history
  ADD COLUMN IF NOT EXISTS send_owner    TEXT NOT NULL DEFAULT 'heatr',
  ADD COLUMN IF NOT EXISTS service_type  TEXT,
  ADD COLUMN IF NOT EXISTS template_id   TEXT,
  ADD COLUMN IF NOT EXISTS enrolled_at   TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS completed_at  TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS stopped_at    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMPTZ;

ALTER TABLE heatr_lead_campaign_history
  DROP CONSTRAINT IF EXISTS chk_lch_send_owner;
ALTER TABLE heatr_lead_campaign_history
  ADD CONSTRAINT chk_lch_send_owner CHECK (send_owner IN ('heatr', 'warmr'));

-- Eén enrollment per (workspace × lead × campagne).
CREATE UNIQUE INDEX IF NOT EXISTS uq_lch_enrollment
  ON heatr_lead_campaign_history (workspace_id, lead_id, campaign_id);

-- Volledig runtime-status-vocabulaire (bronnen: sequence_engine, webhook
-- status_map, control-endpoints, reply_classifier, ADR-001 'active').
ALTER TABLE heatr_lead_campaign_history
  DROP CONSTRAINT IF EXISTS chk_lch_status;
ALTER TABLE heatr_lead_campaign_history
  ADD CONSTRAINT chk_lch_status CHECK (status IN (
    'active',                            -- warmr-owned tracking, in flight
    'pending', 'paused',                 -- heatr-owned (Model B, toekomst)
    'sequence_complete', 'no_response',  -- afgerond zonder reply
    'replied', 'bounced', 'unsubscribed',-- terminale events
    'stopped', 'blocked', 'error'        -- operator/compliance/fout
  ));

-- Dedup-lookups op de nieuwe as
CREATE INDEX IF NOT EXISTS idx_lch_owner_active
  ON heatr_lead_campaign_history (workspace_id, send_owner)
  WHERE is_active = true;

COMMENT ON COLUMN heatr_lead_campaign_history.send_owner IS
  'ADR-001: warmr = tracking-only rij (Warmr dript zelf; get_due_sends negeert deze). heatr = Model B stap-dispatch (toekomstige migratie).';

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT indexname FROM pg_indexes
WHERE schemaname='public' AND indexname IN ('uq_lch_enrollment','idx_lch_owner_active');
```

VERWACHT: laatste resultaat toont `uq_lch_enrollment` en `idx_lch_owner_active` (2 rijen).

═══════════════════════════════════════════════════════════════════════════
STAP 6 — MIGRATIE 026: webhook-eventledger
═══════════════════════════════════════════════════════════════════════════
```sql
-- 026_webhook_events.sql — Fase 3 (PR 10): inbound webhook-eventledger.
--
-- WAAROM (audit v2, scenario's 3 en 14): de Warmr-webhook had geen enkele
-- delivery-deduplicatie — een geredeliverd event maakte dubbele
-- reply_inbox-rijen en dubbele crm_tasks. En een event voor een onbekende
-- lead verdween stil achter {"ok": true} — geen dead-letter, geen signaal.
--
-- Deze tabel is de at-least-once → exactly-once brug:
--   1. elk binnenkomend event wordt EERST hier geregistreerd;
--   2. duplicate event_id (UNIQUE) → verwerking geskipt, eerlijke response;
--   3. onbekende lead → processing_status='dead_letter' i.p.v. stil succes;
--   4. verwerkingsfout → 'error' + reden, zichtbaar voor reconciliatie.
--
-- Warmr levert (nog) geen event_id: dan synthetiseert Heatr er één als
-- sha256 over de canonieke payload — exacte redeliveries dedupen daarmee
-- ook zonder contract-wijziging. occurred_at/sequence_no zijn alvast
-- gemodelleerd voor het toekomstige Warmr-contract (audit v2 §7).
--
-- Draai VÓÓR de fase-3-PR10 code-deploy. Gedrag bij ontbrekende tabel:
-- INBOUND faalt SOFT (events verwerken > dedup — een geblokkeerde inbound
-- zou replies/unsubscribes VERLIEZEN), met luide log.

CREATE TABLE IF NOT EXISTS heatr_webhook_events (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id           TEXT NOT NULL,       -- Warmr event_id of synth:sha256(payload)
    workspace_id       TEXT NOT NULL,
    event_type         TEXT,
    occurred_at        TIMESTAMPTZ,         -- bron-timestamp (contract, later)
    sequence_no        BIGINT,              -- monotone teller (contract, later)
    lead_id            UUID,
    campaign_id        TEXT,
    message_id         TEXT,
    payload            JSONB,
    processing_status  TEXT NOT NULL DEFAULT 'received' CHECK (processing_status IN (
        'received', 'processed', 'duplicate', 'dead_letter', 'error'
    )),
    processed_at       TIMESTAMPTZ,
    error              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- De dedup-garantie: één rij per event, platformbreed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_webhook_events_event_id
  ON heatr_webhook_events (event_id);

-- Reconciliatie-queries (dead-letters en errors bovenaan de operatorlijst)
CREATE INDEX IF NOT EXISTS idx_webhook_events_attention
  ON heatr_webhook_events (processing_status, created_at DESC)
  WHERE processing_status IN ('dead_letter', 'error');
CREATE INDEX IF NOT EXISTS idx_webhook_events_lead
  ON heatr_webhook_events (lead_id) WHERE lead_id IS NOT NULL;

COMMENT ON TABLE heatr_webhook_events IS
  'Inbound Warmr-eventledger: dedup op event_id (UNIQUE), dead-letter voor onbekende leads, error-status voor mislukte verwerking. Append-only; processing_status/processed_at/error zijn de enige mutaties.';

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT indexname FROM pg_indexes
WHERE schemaname='public' AND indexname='uq_webhook_events_event_id';
```

VERWACHT: laatste resultaat toont `uq_webhook_events_event_id`.

═══════════════════════════════════════════════════════════════════════════
STAP 7 — MIGRATIE 027: cost-guard op schaal
═══════════════════════════════════════════════════════════════════════════
```sql
-- 027_cost_guard_scale.sql — Fase 4 (PR 15/16/17): schaalbare kostenbewaking.
--
-- PROBLEEM (audit v2 P1-2): guarded_call haalde bij ELKE AI-call alle
-- api_cost_log-rijen van de maand op en somde in Python — O(n) per call,
-- O(n²) over een batch — en het range-filter op created_at was
-- ongeïndexeerd (de enige index stond op de kolom `date`). Bij 100k leads
-- × ~10 calls = 1M+ rijen/maand die elke enrichment-stap opnieuw scant.
--
-- Dit levert: (A) een samengestelde index op de échte query-kolommen,
-- (B) één STABLE aggregatie-functie (workspace-som, of platform-som bij
-- NULL — de basis voor het globale platformbudget uit PR 16), en
-- (C) de spent_eur-kolom op enrichment_jobs voor resumable enrichment
-- (PR 17: het per-lead-plafond overleeft nu een worker-restart).
--
-- Draai VÓÓR de fase-4 code-deploy: de code roept heatr_cost_sum aan en
-- is fail-closed — zonder deze functie blokkeren alle AI-calls (veilig,
-- maar disruptief).

-- ── A. Index op de query-kolommen ───────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_heatr_api_cost_log_ws_created
  ON heatr_api_cost_log (workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_heatr_api_cost_log_created
  ON heatr_api_cost_log (created_at DESC);

-- ── B. Aggregatie-functie ───────────────────────────────────────────────────
-- p_workspace = NULL → platformbrede som (globaal budget, PR 16).
CREATE OR REPLACE FUNCTION heatr_cost_sum(p_workspace text, p_since timestamptz)
RETURNS numeric
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(SUM(cost_eur), 0)::numeric
  FROM heatr_api_cost_log
  WHERE created_at >= p_since
    AND (p_workspace IS NULL OR workspace_id = p_workspace);
$$;

-- ── C. Resumable enrichment (PR 17) ─────────────────────────────────────────
-- spent_eur: het per-lead/job-plafond was een in-memory accumulator die op
-- restart naar 0 reset — een crashende job kreeg het budget 3× vers.
ALTER TABLE heatr_enrichment_jobs
  ADD COLUMN IF NOT EXISTS spent_eur NUMERIC NOT NULL DEFAULT 0;

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT heatr_cost_sum('aerys', now() - interval '30 days') AS aerys_30d,
       heatr_cost_sum(NULL,    now() - interval '30 days') AS platform_30d;
```

VERWACHT: laatste resultaat toont twee bedragen (jouw AI-kosten van 30 dagen,
2x hetzelfde getal — aerys is de enige workspace).

═══════════════════════════════════════════════════════════════════════════
STAP 8 — MIGRATIE 028: resumable enrichment
═══════════════════════════════════════════════════════════════════════════
```sql
-- 028_resumable_steps.sql — Fase 4 (PR 17): resumable enrichment, schema-deel.
--
-- steps_completed bestond als kolom maar werd na de init ([]) nooit meer
-- geschreven (audit v2 P1-3 / scenario 9): een job die crashte op stap 14
-- van 15 draaide bij retry ALLE Claude-stappen opnieuw. De code gaat nu
-- stap-namen bijschrijven; dit normaliseert het kolomtype naar jsonb
-- (het dode supabase_schema.sql beloofde int[]; namen zijn robuuster dan
-- indexposities omdat enrichment_types per job kan verschillen).
--
-- spent_eur is al toegevoegd in migratie 027. Draai 027 EERST.

-- Kolom garanderen (no-op als hij bestaat) …
ALTER TABLE heatr_enrichment_jobs
  ADD COLUMN IF NOT EXISTS steps_completed JSONB NOT NULL DEFAULT '[]'::jsonb;

-- … en het type normaliseren naar jsonb (werkt voor int[] én jsonb).
ALTER TABLE heatr_enrichment_jobs
  ALTER COLUMN steps_completed DROP DEFAULT;
ALTER TABLE heatr_enrichment_jobs
  ALTER COLUMN steps_completed TYPE JSONB USING to_jsonb(steps_completed);
ALTER TABLE heatr_enrichment_jobs
  ALTER COLUMN steps_completed SET DEFAULT '[]'::jsonb;

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_name = 'heatr_enrichment_jobs'
  AND column_name IN ('steps_completed', 'spent_eur');
```

VERWACHT: 2 rijen — `steps_completed` met data_type `jsonb` en `spent_eur`
met data_type `numeric`.

═══════════════════════════════════════════════════════════════════════════
STAP 9 — AUTH-PROVISIONING (workspace-claims voor bestaande users)
═══════════════════════════════════════════════════════════════════════════
Zonder deze stap sluit de frontend zichzelf buiten na de code-herstart.

```sql
UPDATE auth.users
SET raw_app_meta_data = COALESCE(raw_app_meta_data, '{}'::jsonb)
                        || '{"workspace_id": "aerys"}'::jsonb
WHERE raw_app_meta_data->>'workspace_id' IS NULL;

SELECT count(*) FROM auth.users WHERE raw_app_meta_data->>'workspace_id' IS NULL;
```

VERWACHT: tweede query geeft **0**.

═══════════════════════════════════════════════════════════════════════════
STAP 10 — ZEG "KLAAR" TEGEN CLAUDE
═══════════════════════════════════════════════════════════════════════════
Claude doet dan:
  1. launchctl kickstart van nl.aerys.heatr.api + nl.aerys.heatr.scraping-worker
     (de services laden dan pas de nieuwe code)
  2. verificatie: /healthz, logs, eerste ledger-writes
  3. git push origin main (backup van de 19 commits)

DAARNA (jij, eenmalig): log in de Heatr-frontend UIT en weer IN — de oude
browser-sessie draagt de workspace-claim nog niet.

.env HOEFT NIET AANGEPAST: ENABLE_CAMPAIGN_SENDS=true staat er al en de
nieuwe code valt daarop terug — sends blijven gewoon werken.
