-- 030_email_verification_diagnostics.sql — Remediation-sprint C1/H2.
--
-- De verifier klapte infra-fouten (timeout/refused/exception) samen op
-- email_status='risky' — niet te onderscheiden van échte adres-kwaliteit
-- (4xx). Enrichment audit V2: 729 'risky', 0 'catchall_risky', 1 'valid' =
-- signatuur van een kapotte verifier/omgeving, niet de doelgroep.
--
-- Deze migratie voegt de DIAGNOSTIEK toe zonder de bestaande email_status-
-- CHECK (migratie 023) te schenden: infra-fouten worden voortaan
-- email_status='not_checked' (al toegestaan + al fail-closed), met de
-- precieze reden in de nieuwe kolom email_verification_method. Echte 4xx
-- blijft 'risky' mét method='smtp' → alleen díe is (met ALLOW_RISKY) nog
-- verzendbaar.
--
-- Draai VÓÓR de C1-code-deploy. Idempotent (ADD COLUMN IF NOT EXISTS).

ALTER TABLE heatr_leads
  ADD COLUMN IF NOT EXISTS email_verification_method TEXT,   -- smtp | timeout | connection_error | temporary_failure | mx_check | catchall_detection | cache | rate_limited | format_check | pre_existing | crawl_unverified | error
  ADD COLUMN IF NOT EXISTS email_verified_at         TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS email_discovery_source    TEXT;   -- website | pattern | google | kvk | contact_crawl | pre_existing

-- Append-only audit-trail per verificatiepoging (spec 2.1). Losstaand van
-- de lead-rij zodat her-verificatie (2.5) niets overschrijft.
CREATE TABLE IF NOT EXISTS heatr_email_verifications (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id        TEXT NOT NULL,
    lead_id             UUID,
    email               TEXT NOT NULL,
    verification_status TEXT NOT NULL CHECK (verification_status IN (
        'valid', 'invalid', 'catchall_risky', 'risky',
        'timeout', 'connection_error', 'temporary_failure',
        'not_checked', 'not_found'
    )),
    verification_method TEXT,          -- smtp | mx_check | catchall_detection | timeout | connection_error | temporary_failure | cache | rate_limited | format_check | error
    smtp_code           INT,           -- 250 | 550 | 451 | ... | NULL
    discovery_source    TEXT,
    checked_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_label           TEXT           -- bv. 'sample_2026-07-13', 're_verify_batch_1'
);
CREATE INDEX IF NOT EXISTS idx_email_verifications_lead
  ON heatr_email_verifications (lead_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_email_verifications_run
  ON heatr_email_verifications (run_label, verification_status);

COMMENT ON TABLE heatr_email_verifications IS
  'Append-only: één rij per verificatiepoging. verification_status onderscheidt adres-kwaliteit (valid/invalid/risky/catchall) van infra-fout (timeout/connection_error/temporary_failure). Nooit overschrijven.';

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT column_name FROM information_schema.columns
WHERE table_name='heatr_leads' AND column_name LIKE 'email_verif%';
