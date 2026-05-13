-- 017_is_test_lead.sql
-- Test-mode flag per lead voor smoke-test pipeline.
--
-- is_test_lead=true triggers:
--   1. is_sendable() bypass — gate accepteert deze lead ongeacht email_status
--   2. _build_lead_payload — voegt BCC toe naar HEATR_TEST_BCC_EMAIL + [TEST]
--      prefix in mail-subject zodat duidelijk is dat het een testflow is
--
-- Use-case: Sami markeert zichzelf of een collega als test-lead, draait de
-- complete pipeline (enrichment → preview → launch → reply → drafter), zonder
-- risk dat een echte prospect een halve send krijgt.

ALTER TABLE public.heatr_leads
    ADD COLUMN IF NOT EXISTS is_test_lead BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_leads_test_mode
  ON public.heatr_leads (workspace_id, is_test_lead)
  WHERE is_test_lead = true;

COMMENT ON COLUMN public.heatr_leads.is_test_lead IS
  'Test-mode flag. True = gate-bypass + BCC naar HEATR_TEST_BCC_EMAIL + [TEST] prefix in subject.';
