-- 014_archetype_classification.sql
-- Adds koop-archetype classification to leads. Archetype is een meta-laag
-- bovenop sector + subcategorie — bepaalt de Mail 1 tone, niet wat ze doen.
--
-- Mogelijke waarden (zie enrichment/archetype_classifier.py):
--   cosmetisch:    medisch_cosmetisch | esthetisch_medisch | premium_beauty | volume_beauty
--   alternatief:   lichaamswerk_pragmatisch | holistisch_spiritueel | therapeutisch_mentaal | welzijn_praktisch

ALTER TABLE public.heatr_leads
    ADD COLUMN IF NOT EXISTS archetype             TEXT,
    ADD COLUMN IF NOT EXISTS archetype_reason      TEXT,
    ADD COLUMN IF NOT EXISTS archetype_confidence  NUMERIC(3,2),
    ADD COLUMN IF NOT EXISTS archetype_classified_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_leads_archetype_workspace
  ON public.heatr_leads (workspace_id, archetype)
  WHERE archetype IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_leads_archetype_unclassified
  ON public.heatr_leads (archetype_classified_at)
  WHERE archetype IS NULL AND status IN ('discovered', 'enriched');

COMMENT ON COLUMN public.heatr_leads.archetype IS
  'Koop-archetype: hoe deze lead beslist. Bepaalt Mail 1 tone. NULL = nog niet geclassificeerd.';
COMMENT ON COLUMN public.heatr_leads.archetype_reason IS
  'Korte uitleg (Claude-gegenereerd) waarom dit archetype gekozen is. Voor audit + UI tooltip.';
COMMENT ON COLUMN public.heatr_leads.archetype_confidence IS
  'Claude self-assessed confidence 0.00-1.00. <0.7 = handmatige review aangeraden.';
