-- ============================================================
-- 015_claude_cache.sql
-- Migration: heatr_claude_cache tabel aanmaken voor prompt-response caching.
--
-- Reden: utils/claude_cache.py:cached_claude_call() doet SELECT/UPSERT op
-- heatr_claude_cache. Tabel ontbrak in productie → PGRST205 silent fails →
-- 0 cache-hits in 6.772 calls (zie docs/cost_audit_2026-05-11.md).
--
-- Schema: matchet huidige code (response TEXT, niet JSONB) + uitgebreid met
-- context-kolom + gesplitste tokens voor per-context TTL-logica (Fix 1/3
-- van cost-audit follow-up).
-- Kopieer dit bestand, plak in Supabase SQL editor, druk Run. Idempotent.
-- ============================================================

CREATE TABLE IF NOT EXISTS heatr_claude_cache (
  cache_key      TEXT          PRIMARY KEY,
  context        TEXT          NOT NULL DEFAULT '',          -- caller-naam, bepaalt TTL
  model          TEXT          NOT NULL,
  prompt_hash    TEXT          NOT NULL,                     -- eerste 16 chars van cache_key (invalidate-by-domain)
  response       TEXT          NOT NULL,                     -- text content only, niet full Anthropic response object
  input_tokens   INT           NOT NULL DEFAULT 0,
  output_tokens  INT           NOT NULL DEFAULT 0,
  tokens_used    INT           GENERATED ALWAYS AS (input_tokens + output_tokens) STORED,
  created_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  expires_at     TIMESTAMPTZ   NOT NULL,
  hit_count      INT           NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_heatr_claude_cache_expires ON heatr_claude_cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_heatr_claude_cache_context ON heatr_claude_cache(context);

-- Optionele cleanup (handmatig of via pg_cron later):
--   DELETE FROM heatr_claude_cache WHERE expires_at < NOW();
