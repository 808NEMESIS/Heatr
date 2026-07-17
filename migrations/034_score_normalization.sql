-- 034_score_normalization.sql — schaal-herstel van de website-score.
--
-- analyze_website telde technical+visual+conversion+sector op en cappte op 100,
-- terwijl visual altijd None was (Vision stond gated-off). Elke website_score is
-- daardoor op een 70-schaal berekend maar als 100-schaal opgeslagen. De score
-- normaliseert nu over de BEHAALDE noemer (som van max-punten van de lagen die
-- meededen). Deze twee kolommen maken die noemer expliciet en queryable — een
-- 77/70 is iets anders dan een 77/95.
--
-- LET OP: verandert op zichzelf geen bestaande waarden. De backfill (apart script,
-- draait pas na akkoord) herberekent de bestaande rijen.
--
-- Idempotent. Niet automatisch uitgevoerd — draai in de Supabase SQL-editor.

ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS score_denominator integer;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS visual_included    boolean;

COMMENT ON COLUMN heatr_website_intelligence.score_denominator IS
  'Som van max-punten van de lagen die aan total_score bijdroegen (70 zonder visual, 95 met). NULL = oude, niet-genormaliseerde rij.';
COMMENT ON COLUMN heatr_website_intelligence.visual_included IS
  'Of Laag 2 (Vision) meetelde in total_score. False/NULL => score over 70 genormaliseerd.';
