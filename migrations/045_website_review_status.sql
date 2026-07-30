-- Migratie 045 — review_status op website_intelligence (Fase 2, review-triage)
--
-- PATCH /leads/{id}/website-review schrijft `review_status`, maar die kolom bestond
-- NIET op de live tabel (geverifieerd 2026-07-29) → de PATCH gaf een PGRST-fout.
-- Deze kolom completeert de triage-knoppen (ok / opportunity / urgent) op de
-- Website-kansen + de LeadDetail Website-tab.
--
-- Draai dit in de Supabase SQL-editor (MCP heeft geen prod-toegang).

ALTER TABLE heatr_website_intelligence
    ADD COLUMN IF NOT EXISTS review_status text;

-- Optioneel, voor snel filteren op getrieerde kansen:
CREATE INDEX IF NOT EXISTS idx_wi_review_status
    ON heatr_website_intelligence (workspace_id, review_status);
