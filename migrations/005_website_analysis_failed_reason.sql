-- 005_website_analysis_failed_reason.sql
-- Adds two optional columns to heatr_leads for logging website analysis failures.
-- Only written by job_queue/website_analysis_queue.py when a lead's analysis fails
-- (prescreen fail, Playwright error, Claude error).
--
-- Runnable idempotently. Non-breaking: existing code paths don't read these columns.

ALTER TABLE heatr_leads
    ADD COLUMN IF NOT EXISTS website_analysis_failed_reason text,
    ADD COLUMN IF NOT EXISTS website_analysis_failed_at timestamptz;

COMMENT ON COLUMN heatr_leads.website_analysis_failed_reason IS
    'Short reason string (<=300 chars) from process_next_website_analysis. Null if last analysis succeeded or no attempt yet.';

COMMENT ON COLUMN heatr_leads.website_analysis_failed_at IS
    'Timestamp of the last failed analysis attempt. Null if never failed.';
