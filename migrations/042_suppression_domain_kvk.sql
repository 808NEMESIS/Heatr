-- 042_suppression_kvk.sql — org-brede suppressie op KvK-nummer (AVG-05).
--
-- Audit-bevinding (2026-07-24): suppressie matchte alleen op exact e-mailadres,
-- dus een afgemelde of 'vergeten' praktijk kon onder een ander adres bij hetzelfde
-- domein/KvK opnieuw benaderd worden. De `domein`-sleutel bestaat AL sinds migratie
-- 024 (heatr_suppressions.domain, met index idx_suppressions_domain) — die vullen
-- we nu ook bij elke suppressie. Alleen het KvK-nummer is nieuw.
--
-- LET OP: de tabel heet `heatr_suppressions` (heatr_-prefix; de code's
-- .table("suppressions") wordt door de wrapper geprefixt, maar RAW SQL niet).
--
-- Idempotent (IF NOT EXISTS). Draai in de Supabase SQL-editor (MCP heeft geen
-- Heatr-prod-toegang).

ALTER TABLE heatr_suppressions ADD COLUMN IF NOT EXISTS kvk_number text;

CREATE INDEX IF NOT EXISTS idx_suppressions_kvk
  ON heatr_suppressions (kvk_number) WHERE kvk_number IS NOT NULL AND revoked_at IS NULL;
