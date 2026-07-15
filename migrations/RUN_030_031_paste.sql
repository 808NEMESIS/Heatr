-- ─────────────────────────────────────────────────────────────────────────────
-- PLAK-DIT in de Supabase SQL-editor (project zomdrygdcaenjnrrpcpw → SQL Editor).
-- Bevat migratie 030 + 031. Beide idempotent — veilig meerdere keren te draaien.
-- Draait GEEN mail, verwijdert niets. Verwachte output onderaan: de nieuwe kolommen.
-- ─────────────────────────────────────────────────────────────────────────────

-- 030 — e-mailverificatie-diagnostiek
ALTER TABLE heatr_leads
  ADD COLUMN IF NOT EXISTS email_verification_method TEXT,
  ADD COLUMN IF NOT EXISTS email_verified_at         TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS email_discovery_source    TEXT;

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
    verification_method TEXT,
    smtp_code           INT,
    discovery_source    TEXT,
    checked_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_label           TEXT
);
CREATE INDEX IF NOT EXISTS idx_email_verifications_lead
  ON heatr_email_verifications (lead_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_email_verifications_run
  ON heatr_email_verifications (run_label, verification_status);

-- 031 — domain_cache-bugfix (catch-all-cache schreef naar niet-bestaande tabel)
CREATE TABLE IF NOT EXISTS heatr_domain_cache (
    domain      TEXT PRIMARY KEY,
    is_catchall BOOLEAN NOT NULL,
    has_mx      BOOLEAN,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_domain_cache_expires
  ON heatr_domain_cache (expires_at);

-- Verificatie — moet 3 rijen tonen (email_verification_method / _verified_at / _discovery_source):
SELECT column_name FROM information_schema.columns
WHERE table_name = 'heatr_leads' AND column_name LIKE 'email_%'
ORDER BY column_name;
