-- 026_webhook_events.sql — Fase 3 (PR 10): inbound webhook-eventledger.
--
-- WAAROM (audit v2, scenario's 3 en 14): de Warmr-webhook had geen enkele
-- delivery-deduplicatie — een geredeliverd event maakte dubbele
-- reply_inbox-rijen en dubbele crm_tasks. En een event voor een onbekende
-- lead verdween stil achter {"ok": true} — geen dead-letter, geen signaal.
--
-- Deze tabel is de at-least-once → exactly-once brug:
--   1. elk binnenkomend event wordt EERST hier geregistreerd;
--   2. duplicate event_id (UNIQUE) → verwerking geskipt, eerlijke response;
--   3. onbekende lead → processing_status='dead_letter' i.p.v. stil succes;
--   4. verwerkingsfout → 'error' + reden, zichtbaar voor reconciliatie.
--
-- Warmr levert (nog) geen event_id: dan synthetiseert Heatr er één als
-- sha256 over de canonieke payload — exacte redeliveries dedupen daarmee
-- ook zonder contract-wijziging. occurred_at/sequence_no zijn alvast
-- gemodelleerd voor het toekomstige Warmr-contract (audit v2 §7).
--
-- Draai VÓÓR de fase-3-PR10 code-deploy. Gedrag bij ontbrekende tabel:
-- INBOUND faalt SOFT (events verwerken > dedup — een geblokkeerde inbound
-- zou replies/unsubscribes VERLIEZEN), met luide log.

CREATE TABLE IF NOT EXISTS heatr_webhook_events (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id           TEXT NOT NULL,       -- Warmr event_id of synth:sha256(payload)
    workspace_id       TEXT NOT NULL,
    event_type         TEXT,
    occurred_at        TIMESTAMPTZ,         -- bron-timestamp (contract, later)
    sequence_no        BIGINT,              -- monotone teller (contract, later)
    lead_id            UUID,
    campaign_id        TEXT,
    message_id         TEXT,
    payload            JSONB,
    processing_status  TEXT NOT NULL DEFAULT 'received' CHECK (processing_status IN (
        'received', 'processed', 'duplicate', 'dead_letter', 'error'
    )),
    processed_at       TIMESTAMPTZ,
    error              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- De dedup-garantie: één rij per event, platformbreed.
CREATE UNIQUE INDEX IF NOT EXISTS uq_webhook_events_event_id
  ON heatr_webhook_events (event_id);

-- Reconciliatie-queries (dead-letters en errors bovenaan de operatorlijst)
CREATE INDEX IF NOT EXISTS idx_webhook_events_attention
  ON heatr_webhook_events (processing_status, created_at DESC)
  WHERE processing_status IN ('dead_letter', 'error');
CREATE INDEX IF NOT EXISTS idx_webhook_events_lead
  ON heatr_webhook_events (lead_id) WHERE lead_id IS NOT NULL;

COMMENT ON TABLE heatr_webhook_events IS
  'Inbound Warmr-eventledger: dedup op event_id (UNIQUE), dead-letter voor onbekende leads, error-status voor mislukte verwerking. Append-only; processing_status/processed_at/error zijn de enige mutaties.';

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT indexname FROM pg_indexes
WHERE schemaname='public' AND indexname='uq_webhook_events_event_id';
