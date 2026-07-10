-- 025_tracking_enrollments.sql — Fase 3 (PR 9): tracking-only enrollments.
--
-- ADR-001: Warmr dript, Heatr trackt. Launch gaat per gepushte lead één
-- rij schrijven met send_owner='warmr' en status='active'. Deze kolommen
-- + constraints maken dat veilig:
--
--   - send_owner: de harde grens tegen dubbele drip — get_due_sends
--     (Model B) selecteert uitsluitend send_owner='heatr';
--   - UNIQUE (workspace_id, lead_id, campaign_id): her-enrollment van
--     dezelfde lead in dezelfde campagne is een no-op, nooit een stille
--     overschrijving. Een bewuste her-benadering = nieuwe campagne
--     (launch maakt per bucket een nieuwe Warmr-campaign_id);
--   - status-CHECK: legt het volledige runtime-vocabulaire vast en stopt
--     de vocabulaire-drift tussen supabase_schema.sql en migratie 009
--     (audit v2 §6: twee onverenigbare definities).
--
-- Tabel is vandaag LEEG (audit v2 F2: geen enkele insert bestond) — alle
-- constraints zijn direct veilig. Draai VÓÓR de fase-3 code-deploy.

ALTER TABLE heatr_lead_campaign_history
  ADD COLUMN IF NOT EXISTS send_owner    TEXT NOT NULL DEFAULT 'heatr',
  ADD COLUMN IF NOT EXISTS service_type  TEXT,
  ADD COLUMN IF NOT EXISTS template_id   TEXT,
  ADD COLUMN IF NOT EXISTS enrolled_at   TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS completed_at  TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS stopped_at    TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_event_at TIMESTAMPTZ;

ALTER TABLE heatr_lead_campaign_history
  DROP CONSTRAINT IF EXISTS chk_lch_send_owner;
ALTER TABLE heatr_lead_campaign_history
  ADD CONSTRAINT chk_lch_send_owner CHECK (send_owner IN ('heatr', 'warmr'));

-- Eén enrollment per (workspace × lead × campagne).
CREATE UNIQUE INDEX IF NOT EXISTS uq_lch_enrollment
  ON heatr_lead_campaign_history (workspace_id, lead_id, campaign_id);

-- Volledig runtime-status-vocabulaire (bronnen: sequence_engine, webhook
-- status_map, control-endpoints, reply_classifier, ADR-001 'active').
ALTER TABLE heatr_lead_campaign_history
  DROP CONSTRAINT IF EXISTS chk_lch_status;
ALTER TABLE heatr_lead_campaign_history
  ADD CONSTRAINT chk_lch_status CHECK (status IN (
    'active',                            -- warmr-owned tracking, in flight
    'pending', 'paused',                 -- heatr-owned (Model B, toekomst)
    'sequence_complete', 'no_response',  -- afgerond zonder reply
    'replied', 'bounced', 'unsubscribed',-- terminale events
    'stopped', 'blocked', 'error'        -- operator/compliance/fout
  ));

-- Dedup-lookups op de nieuwe as
CREATE INDEX IF NOT EXISTS idx_lch_owner_active
  ON heatr_lead_campaign_history (workspace_id, send_owner)
  WHERE is_active = true;

COMMENT ON COLUMN heatr_lead_campaign_history.send_owner IS
  'ADR-001: warmr = tracking-only rij (Warmr dript zelf; get_due_sends negeert deze). heatr = Model B stap-dispatch (toekomstige migratie).';

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT indexname FROM pg_indexes
WHERE schemaname='public' AND indexname IN ('uq_lch_enrollment','idx_lch_owner_active');
