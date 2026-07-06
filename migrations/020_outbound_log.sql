-- 020_outbound_log.sql — Sprint 2 Control Plane: dispatcher-fundament.
--
-- heatr_outbound_log: APPEND-ONLY ledger van elke outbound side-effect die
-- door utils/outbound_dispatcher.dispatch_outbound loopt. Eén rij per
-- dispatch-poging (ook geblokkeerde en geskipte pogingen) — dit is het
-- I7-fundament (reproduceerbaarheid) én het I6-keuzepunt (idempotency).
--
-- restart_epoch op lead_campaign_history: onderdeel van de idempotency-key
-- voor sequence-sends (seq-send:{record}:{step}:{epoch}). Een bewuste
-- operator-restart bumpt de epoch → nieuwe key → herzendbaar; een
-- accidentele dubbele dispatch van dezelfde stap houdt dezelfde key → block.

CREATE TABLE IF NOT EXISTS heatr_outbound_log (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id     TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    kind             TEXT NOT NULL,      -- warmr_push | warmr_bulk_push | warmr_campaign_create | operator_email
    lead_id          UUID,               -- NULL bij bulk/operator-mail
    lead_ids         JSONB,              -- gevuld bij bulk
    actor            TEXT NOT NULL,      -- bv. user:sami@..., service:n8n, scheduler:sequence-dispatch
    status           TEXT NOT NULL,      -- completed | failed | blocked_compliance | skipped_duplicate
    result           JSONB,
    error            TEXT,
    metadata         JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbound_log_key
  ON heatr_outbound_log (workspace_id, idempotency_key);
CREATE INDEX IF NOT EXISTS idx_outbound_log_recent
  ON heatr_outbound_log (workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_outbound_log_lead
  ON heatr_outbound_log (lead_id) WHERE lead_id IS NOT NULL;

ALTER TABLE heatr_lead_campaign_history
  ADD COLUMN IF NOT EXISTS restart_epoch INT NOT NULL DEFAULT 0;

COMMENT ON TABLE heatr_outbound_log IS
  'Append-only side-effect-ledger. Elke outbound-dispatch (ook blocked/skipped) is één rij. Nooit updaten of deleten — I7-fundament.';
