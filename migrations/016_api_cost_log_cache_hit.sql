-- ============================================================
-- 016_api_cost_log_cache_hit.sql
-- Migration: voeg cache_hit kolom toe aan heatr_api_cost_log.
--
-- Reden: Fix 1/3 van cost-audit (zie docs/cost_audit_2026-05-11.md) vereist
-- dat cache-hits registreerbaar zijn voor analytics + verify-scripts. De
-- kolom werd in eerder schema-design genoemd maar niet geforceerd doorgevoerd.
--
-- Idempotent — meermaals uitvoeren is veilig.
-- ============================================================

ALTER TABLE heatr_api_cost_log
    ADD COLUMN IF NOT EXISTS cache_hit BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_heatr_api_cost_log_cache_hit
    ON heatr_api_cost_log(cache_hit)
    WHERE cache_hit = TRUE;
