-- 010_review_recency.sql
-- Adds review_recency_checked_at to track when google_reviews_scraper last ran.
-- latest_review_date already exists (006). NULL means: never checked or no review found.
--
-- Used by enrichment/google_reviews_scraper.py (writes) and
-- config/sequence_templates.pick_observation_block (reads → BLOK B selectie).

ALTER TABLE public.heatr_leads
    ADD COLUMN IF NOT EXISTS review_recency_checked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_leads_review_recency_unchecked
  ON public.heatr_leads (review_recency_checked_at)
  WHERE review_recency_checked_at IS NULL AND google_maps_url IS NOT NULL;

COMMENT ON COLUMN public.heatr_leads.review_recency_checked_at IS
  'Last run of google_reviews_scraper. NULL = never checked. latest_review_date can be NULL even when checked (no parseable review found).';
