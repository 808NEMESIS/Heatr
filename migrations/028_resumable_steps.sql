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
