-- 016_import_runs.sql
-- Idempotency tracking voor /leads/import.
-- Client genereert per import-batch een UUID en stuurt mee als import_run_id.
-- Server bewaart 24u; herhaalde calls met dezelfde id → return cached result
-- ipv opnieuw inserten. Voorkomt dubbel-klikken-corruption.

CREATE TABLE IF NOT EXISTS public.heatr_import_runs (
    id           UUID PRIMARY KEY,                    -- = import_run_id van client
    workspace_id TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    result       JSONB NOT NULL DEFAULT '{}',         -- volledige import-response
    row_count    INT,
    imported_by  TEXT
);

CREATE INDEX IF NOT EXISTS idx_import_runs_workspace_recent
  ON public.heatr_import_runs (workspace_id, started_at DESC);

COMMENT ON TABLE public.heatr_import_runs IS
  'Idempotency-cache voor /leads/import. TTL: 24h (cleanup via cron). Client moet UUID v4 in import_run_id meegeven.';
