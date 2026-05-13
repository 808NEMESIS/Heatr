-- 012_feedback_runs.sql
-- Persisted history of feedback_processor runs.
-- Each row = one analysis window (default 30 days lookback). Stores aggregate
-- counts + insight/adjustment payload as JSONB for retrieval via
-- GET /scoring/feedback-history.
--
-- Adjustments are SUGGESTIONS, not auto-applied. Operator reviews and
-- updates config/scoring_weights.py manually.

CREATE TABLE IF NOT EXISTS public.heatr_feedback_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    TEXT NOT NULL,
    period_days     INT  NOT NULL DEFAULT 30,
    leads_analyzed  INT  NOT NULL DEFAULT 0,
    replied         INT  NOT NULL DEFAULT 0,
    bounced         INT  NOT NULL DEFAULT 0,
    reply_rate      NUMERIC(5,3),
    bounce_rate     NUMERIC(5,3),
    insights        JSONB NOT NULL DEFAULT '[]',
    adjustments     JSONB NOT NULL DEFAULT '[]',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feedback_runs_workspace_recent
  ON public.heatr_feedback_runs (workspace_id, created_at DESC);

COMMENT ON TABLE public.heatr_feedback_runs IS
  'Per-run snapshot of feedback_processor output. Operator reviews insights/adjustments to manually tune scoring_weights.py.';
