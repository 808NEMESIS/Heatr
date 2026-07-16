-- 032_checkup_followup.sql — Check-up follow-up.
--
-- Een gesprek dat niet op een 'ja' eindigt wordt afgesloten met een check-up
-- rapport; dat rapport is later de aanleiding om terug te komen. Hangt achter
-- crm_stage='gereageerd', tussen reply en gewonnen/verloren.
--
-- Vier harde regels (bepalen het ontwerp):
--   1. Geen uitgaande claim onverifieerbaar — retarget verwijst alleen naar het
--      rapport als report_status='sent' in de DB staat.
--   2. Twee permanente menselijke gates: uitkomst kiezen + rapport vrijgeven.
--   3. Fail-closed matching: precies één e-mailmatch of 'unmatched' (geen fuzzy).
--   4. Geen rapport zonder cijfers: checkup_data verplicht, anders 'skipped'.
--
-- Idempotent. Draai VÓÓR de calls/-module-deploy. Plak in de Supabase SQL-editor.
--
-- checkup_data-vorm (leeft op heatr_leads.checkup_data, uit het gesprek zelf —
-- LOS van website intelligence, dat gaat over hun site en is er al vóór het
-- gesprek):
-- {
--   "unanswered_inbound_per_week": 14,
--   "response_time_hours": 11,
--   "value_per_new_patient": 800,
--   "conversion_estimate": 0.25,
--   "no_shows_per_month": 6,
--   "hours_reception_per_week": 5,
--   "reviews_per_month": 2,
--   "treatments_per_month": 80,
--   "source": "call",
--   "captured_at": "2026-07-15"
-- }

CREATE TABLE IF NOT EXISTS heatr_call_records (
  id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id         text NOT NULL,
  lead_id              uuid REFERENCES heatr_leads(id),

  -- bron
  zoom_meeting_id      text UNIQUE,
  call_date            timestamptz NOT NULL,
  duration_minutes     int,
  transcript           text NOT NULL,
  participants         jsonb,
  transcript_source    text NOT NULL DEFAULT 'manual',    -- manual | zoom

  -- matching (fail-closed: precies één e-mailmatch of unmatched)
  match_status         text NOT NULL DEFAULT 'unmatched', -- unmatched | matched | manually_matched

  -- uitkomst (gate 1, altijd menselijk)
  outcome              text,                              -- won | timing | no_value | stalled | hard_no
  outcome_note         text,
  outcome_set_at       timestamptz,
  timing_target_date   date,                              -- als prospect zelf een moment noemde

  -- rapport
  report_findings      jsonb,
  report_html          text,
  report_status        text NOT NULL DEFAULT 'pending',   -- pending | generated | approved | sent | skipped
  report_sent_at       timestamptz,

  -- cadans
  retarget_due_at      timestamptz,
  retarget_status      text NOT NULL DEFAULT 'none',      -- none | scheduled | sent | replied | exhausted
  retarget_attempt     int NOT NULL DEFAULT 0,
  retarget_last_sent_at timestamptz,

  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_call_records_workspace ON heatr_call_records(workspace_id);
CREATE INDEX IF NOT EXISTS idx_call_records_lead ON heatr_call_records(lead_id);
CREATE INDEX IF NOT EXISTS idx_call_records_retarget ON heatr_call_records(retarget_due_at)
  WHERE retarget_status = 'scheduled';
CREATE INDEX IF NOT EXISTS idx_call_records_unmatched ON heatr_call_records(workspace_id)
  WHERE match_status = 'unmatched';

ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS last_call_at timestamptz;
ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS last_call_outcome text;
ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS checkup_data jsonb;

-- Verificatie — moet de nieuwe tabel + kolommen tonen:
SELECT 'heatr_call_records' AS created;
SELECT column_name FROM information_schema.columns
WHERE table_name = 'heatr_leads' AND column_name IN ('last_call_at', 'last_call_outcome', 'checkup_data')
ORDER BY column_name;
