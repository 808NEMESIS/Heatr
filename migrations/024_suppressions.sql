-- 024_suppressions.sql — Fase 2 (PR 7): centrale cross-workspace suppressie.
--
-- WAAROM: suppressie leefde als mutabele velden op de lead-rij, per
-- workspace. Dezelfde persoon in workspace B is een andere rij → een
-- unsubscribe in A onderdrukte niets in B (audit v2 P0-2). Deze tabel is
-- de platformbrede, append-only suppressielijst op genormaliseerd
-- e-mailadres, geraadpleegd door de dispatcher vóór élke prospect-send.
--
-- PRIVACY: de tabel bevat e-mailadressen (grondslag: wettelijke plicht om
-- unsubscribe/erasure na te leven — een suppressielijst is daarvoor de
-- standaard). De gate antwoordt callers alleen "globally_suppressed",
-- NOOIT welke tenant/campagne de suppressie aanmaakte.
--
-- Draai VÓÓR de fase-2 code-deploy (de dispatcher gaat deze tabel
-- fail-closed bevragen: tabel afwezig = prospect-sends geblokkeerd).

CREATE TABLE IF NOT EXISTS heatr_suppressions (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    normalized_email     TEXT NOT NULL,      -- lower(trim(email))
    domain               TEXT,               -- alleen bij domain_block
    suppression_type     TEXT NOT NULL CHECK (suppression_type IN (
        'unsubscribe', 'hard_bounce', 'complaint', 'forgotten',
        'manual_global', 'domain_block'
    )),
    reason               TEXT,
    source               TEXT NOT NULL,      -- warmr_webhook | reply_classifier | gdpr_forget | operator
    source_workspace_id  TEXT,               -- herkomst — NOOIT terug naar callers lekken
    lead_id              UUID,
    campaign_id          TEXT,
    event_id             TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at           TIMESTAMPTZ,        -- NULL = actief; alleen operator mag revoken
    created_by           TEXT
);

-- Eén ACTIEVE suppressie per adres (platformbreed). Een revoke + nieuwe
-- suppressie geeft een nieuwe rij → historie blijft bewaard (append-only).
CREATE UNIQUE INDEX IF NOT EXISTS uq_suppressions_active_email
  ON heatr_suppressions (normalized_email)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_suppressions_domain
  ON heatr_suppressions (domain) WHERE domain IS NOT NULL AND revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_suppressions_recent
  ON heatr_suppressions (created_at DESC);

COMMENT ON TABLE heatr_suppressions IS
  'Platformbrede (cross-workspace) suppressielijst op genormaliseerd e-mailadres. Append-only: revoken = revoked_at zetten, nooit deleten. Geraadpleegd door utils/suppression.check_suppressed via de outbound-dispatcher (fail-closed).';

-- ── outbound_log status-CHECK uitbreiden met blocked_suppression ────────────
ALTER TABLE heatr_outbound_log
  DROP CONSTRAINT IF EXISTS heatr_outbound_log_status_check;
ALTER TABLE heatr_outbound_log
  ADD CONSTRAINT heatr_outbound_log_status_check CHECK (status IN (
    'in_flight', 'completed', 'failed', 'failed_retryable', 'failed_terminal',
    'blocked_compliance', 'blocked_killswitch', 'blocked_suppression',
    'skipped_duplicate'
  ));

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT indexname FROM pg_indexes
WHERE schemaname='public' AND indexname='uq_suppressions_active_email';
