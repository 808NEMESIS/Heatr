-- APPLY_ME_008.sql
-- Extra schema-gaps ontdekt in enrichment worker logs.
-- Idempotent (IF NOT EXISTS). Plak in Supabase SQL editor, druk Run.

ALTER TABLE heatr_leads
    ADD COLUMN IF NOT EXISTS has_whatsapp boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS industry text,
    ADD COLUMN IF NOT EXISTS company_size_estimate text,
    ADD COLUMN IF NOT EXISTS personalized_opener text,
    ADD COLUMN IF NOT EXISTS company_summary text,
    ADD COLUMN IF NOT EXISTS tracking_tools text[];

ALTER TABLE heatr_enrichment_data
    ADD COLUMN IF NOT EXISTS enrichment_step int DEFAULT 0,
    ADD COLUMN IF NOT EXISTS catch_all boolean,
    ADD COLUMN IF NOT EXISTS source text,
    ADD COLUMN IF NOT EXISTS succeeded boolean DEFAULT false,
    ADD COLUMN IF NOT EXISTS raw_result jsonb;
