-- 042_suppression_domain_kvk.sql — org-brede suppressie op domein + KvK (AVG-05).
--
-- Audit-bevinding (2026-07-24): suppressie matchte alleen op exact e-mailadres,
-- dus een afgemelde of 'vergeten' praktijk kon onder een ander adres bij hetzelfde
-- domein/KvK-nummer opnieuw benaderd worden. Voor deze ICP (kleine, eigenaar-
-- geleide praktijken: org = persoon) is org-brede suppressie de juiste, veilige
-- richting: één afmelding/forget suppresst de hele praktijk.
--
-- normalized_domain: het deel na @ (of de kale host), lower/trim.
-- kvk_number:        KvK-nummer van de gesuppresste praktijk (indien bekend).
--
-- Idempotent (IF NOT EXISTS). Niet automatisch uitgevoerd — draai in de Supabase
-- SQL-editor (MCP heeft geen prod-toegang).

ALTER TABLE suppressions ADD COLUMN IF NOT EXISTS normalized_domain text;
ALTER TABLE suppressions ADD COLUMN IF NOT EXISTS kvk_number        text;

-- Snel matchen op domein/KvK bij de send-gate.
CREATE INDEX IF NOT EXISTS idx_suppressions_domain
  ON suppressions(normalized_domain) WHERE normalized_domain IS NOT NULL AND revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_suppressions_kvk
  ON suppressions(kvk_number) WHERE kvk_number IS NOT NULL AND revoked_at IS NULL;
