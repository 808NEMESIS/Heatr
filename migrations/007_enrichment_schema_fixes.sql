-- 007_enrichment_schema_fixes.sql
-- Schema-gaps dichtgetimmerd die de enrichment worker silently skipt:
--   - heatr_leads.kvk_employee_count_range  (company_enrichment leest dit)
--   - heatr_enrichment_data.email_candidate  (waterfall logt hier naar)
--   - heatr_enrichment_data.email_status     (idem)
--   - system_state table                     (inbox cache)
--
-- Alles idempotent (IF NOT EXISTS). Niet-breaking.

ALTER TABLE heatr_leads
    ADD COLUMN IF NOT EXISTS kvk_employee_count_range text;

ALTER TABLE heatr_enrichment_data
    ADD COLUMN IF NOT EXISTS email_candidate text,
    ADD COLUMN IF NOT EXISTS email_status text;

-- system_state cache (used by enrichment_queue voor warmr inbox cache)
CREATE TABLE IF NOT EXISTS heatr_system_state (
    key text PRIMARY KEY,
    value jsonb,
    expires_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_system_state_expires
    ON heatr_system_state (expires_at);

COMMENT ON TABLE heatr_system_state IS
    'Key-value cache voor short-lived system state (warmr_inboxes_cache, etc.).';
