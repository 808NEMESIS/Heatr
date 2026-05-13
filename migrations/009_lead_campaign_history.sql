-- 009_lead_campaign_history.sql
-- Tracks per-lead-per-campaign send progress + stop-trigger state.
-- Used by campaigns/sequence_engine.py and api/main.py webhook handler.
--
-- One row per (lead × campaign). status transitions:
--   pending → pending (advance step) → sequence_complete | unsubscribed | bounced | blocked | error
-- is_active=true means still in flight; webhook reply/bounce/unsub flips it to false.

CREATE TABLE IF NOT EXISTS public.heatr_lead_campaign_history (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    TEXT NOT NULL,
    lead_id         UUID NOT NULL,
    campaign_id     TEXT NOT NULL,                 -- Warmr campaign_id (string from Warmr API)
    inbox_id        TEXT,                          -- preferred Warmr inbox for sends
    sequence_steps  JSONB NOT NULL DEFAULT '[]',   -- frozen copy of sequence at launch time
    step_index      INT  NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending',
    is_active       BOOLEAN NOT NULL DEFAULT true,
    next_send_at    TIMESTAMPTZ,                   -- when next step is due
    sent_at         TIMESTAMPTZ,                   -- last successful send
    block_reason    TEXT,                          -- populated on blocked/error/unsubscribed
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Lookups by webhook handler (stop sequences for lead)
CREATE INDEX IF NOT EXISTS idx_lead_campaign_history_lead_active
  ON public.heatr_lead_campaign_history (lead_id, workspace_id)
  WHERE is_active = true;

-- Sequence-engine due-send poller
CREATE INDEX IF NOT EXISTS idx_lead_campaign_history_due
  ON public.heatr_lead_campaign_history (workspace_id, next_send_at)
  WHERE status = 'pending' AND is_active = true;

-- Audit/analytics
CREATE INDEX IF NOT EXISTS idx_lead_campaign_history_workspace_status
  ON public.heatr_lead_campaign_history (workspace_id, status, created_at DESC);

COMMENT ON TABLE  public.heatr_lead_campaign_history IS 'Per-lead per-campaign send progression. Reply/bounce/unsub flips is_active=false (sequence stop-trigger).';
COMMENT ON COLUMN public.heatr_lead_campaign_history.status IS 'pending | sequence_complete | unsubscribed | bounced | blocked | error';
COMMENT ON COLUMN public.heatr_lead_campaign_history.is_active IS 'false after stop-trigger or sequence_complete — webhook handler queries WHERE is_active=true';
