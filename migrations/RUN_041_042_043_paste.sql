-- RUN_041_042_paste.sql — plak-blok voor de Supabase SQL-editor (Sami draait dit).
-- MCP heeft geen Heatr-prod-toegang (project zomdrygdcaenjnrrpcpw niet bereikbaar),
-- dus deze DDL draai jij handmatig. Idempotent (IF NOT EXISTS) → veilig her-draaibaar.
-- Na afloop kan ik scripts/run_receptie_backfill.py --apply draaien (vult hook_code).

-- ── 041: receptie-haakje-ladder Q4/Q7/Q2/P1 op heatr_website_intelligence ──────
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_hook_code     text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_hook_ladder   jsonb;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_second_hook   text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_q4            text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_q7            text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_q2            text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_p1            text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_q4_gated      boolean;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_form_present  boolean;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_hook_variant   text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_second_variant text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_evidence      jsonb;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_detected_at   timestamptz;

CREATE INDEX IF NOT EXISTS idx_website_intel_receptie_hook
  ON heatr_website_intelligence(workspace_id, receptie_hook_code)
  WHERE receptie_hook_code IS NOT NULL;

-- ── 042: org-brede suppressie op KvK ──────────────────────────────────────────
-- Tabel = heatr_suppressions (heatr_-prefix!). `domain` bestaat al sinds 024,
-- dus alleen kvk_number is nieuw.
ALTER TABLE heatr_suppressions ADD COLUMN IF NOT EXISTS kvk_number text;

CREATE INDEX IF NOT EXISTS idx_suppressions_kvk
  ON heatr_suppressions (kvk_number) WHERE kvk_number IS NOT NULL AND revoked_at IS NULL;

-- ── 043: rechtsvorm-veld op leads (AVG-02, handmatig + gratis B.V.-afleiding) ──
ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS kvk_legal_form text;

-- Sanity na afloop:
--   SELECT column_name FROM information_schema.columns
--    WHERE table_name='heatr_website_intelligence' AND column_name LIKE 'receptie_%';
--   SELECT column_name FROM information_schema.columns
--    WHERE table_name='heatr_leads' AND column_name='kvk_legal_form';
