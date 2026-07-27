-- 044_compliance_flags.sql — persistente, zichtbare compliance-vlaggen (Sami 2026-07-27).
--
-- Aanleiding: Warmr's afmeld-footer is best-effort (campaign_scheduler try/except →
-- warning + mail zonder footer). Heatr's post-send-check detecteert een missende
-- unsubscribe_tokens-rij, maar detectie mag geen logregel blijven: een missende
-- afmeldlink is een AVG-verplichting die faalt. Deze tabel maakt de vondst hard en
-- zichtbaar, met een acknowledged-state zodat een volgende drip-batch niet doorgaat
-- tot de vlag bekeken is (utils/compliance_flags.assert_no_open_flags).
--
-- Tabel = heatr_compliance_flags (heatr_-prefix). Idempotent. Draai in de Supabase
-- SQL-editor.

CREATE TABLE IF NOT EXISTS heatr_compliance_flags (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text NOT NULL,
    flag_type       text NOT NULL,              -- bv. 'missing_unsubscribe'
    lead_id         text,
    campaign_id     text,
    detail          text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    acknowledged_at timestamptz,                -- NULL = open (blokkeert de drip)
    acknowledged_by text
);

-- Open vlaggen snel vindbaar (de drip-pre-gate query't hierop).
CREATE INDEX IF NOT EXISTS idx_compliance_flags_open
  ON heatr_compliance_flags (workspace_id, flag_type)
  WHERE acknowledged_at IS NULL;

-- Eén open vlag per (workspace, type, lead, campagne): een herhaalde sweep mag
-- geen dubbele vlaggen spammen.
CREATE UNIQUE INDEX IF NOT EXISTS uq_compliance_flags_open
  ON heatr_compliance_flags (workspace_id, flag_type, lead_id, campaign_id)
  WHERE acknowledged_at IS NULL;
