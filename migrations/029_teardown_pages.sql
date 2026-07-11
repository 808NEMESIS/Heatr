-- 029_teardown_pages.sql — Fase A PR A1: de teardown-pagina als product.
--
-- Per lead een deelbare, statische website-analyse (uit heatr_website_
-- intelligence). De mail wordt bezorger van deze pagina i.p.v. een vraag
-- om tijd; elke view is een intent-signaal (→ beltaak, PR A2).
--
-- Twee tabellen:
--   heatr_teardown_pages : één pagina per lead (draft→published→revoked),
--     token = niet-raadbare publieke sleutel, content_hash voor idempotente
--     her-generatie.
--   heatr_page_views     : append-only view-log (het intent-fundament).
--
-- Draai VÓÓR de PR A1-code-deploy.

CREATE TABLE IF NOT EXISTS heatr_teardown_pages (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id  TEXT NOT NULL,
    lead_id       UUID NOT NULL,
    token         TEXT NOT NULL,                 -- 128-bit random, publieke URL-sleutel
    status        TEXT NOT NULL DEFAULT 'draft'  -- draft | published | revoked
                    CHECK (status IN ('draft', 'published', 'revoked')),
    html_path     TEXT,                          -- Storage-pad na publish (teardowns/{token}.html)
    content_hash  TEXT,                          -- sha256 van de bron-analyse → idempotente re-gen
    summary       JSONB,                         -- {score, top_findings[], has_competitor} voor mail/CRM
    generated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ,
    approved_by   TEXT,                           -- operator die publiceerde (B4 review-queue)
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Eén actieve pagina per lead (draft of published). Een revoke geeft een
-- nieuwe rij → historie blijft (append-only voor terminale rijen).
CREATE UNIQUE INDEX IF NOT EXISTS uq_teardown_active_lead
  ON heatr_teardown_pages (workspace_id, lead_id)
  WHERE status IN ('draft', 'published');
CREATE UNIQUE INDEX IF NOT EXISTS uq_teardown_token
  ON heatr_teardown_pages (token);
CREATE INDEX IF NOT EXISTS idx_teardown_status
  ON heatr_teardown_pages (workspace_id, status, generated_at DESC);

CREATE TABLE IF NOT EXISTS heatr_page_views (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token       TEXT NOT NULL,
    lead_id     UUID,
    workspace_id TEXT,
    viewed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ua_hash     TEXT,                             -- sha1(user-agent)[:16] — geen ruwe UA/PII
    referer     TEXT,
    is_bot      BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_page_views_lead
  ON heatr_page_views (lead_id, viewed_at DESC) WHERE is_bot = FALSE;
CREATE INDEX IF NOT EXISTS idx_page_views_token
  ON heatr_page_views (token, viewed_at DESC);

COMMENT ON TABLE heatr_teardown_pages IS
  'Fase A: per-lead deelbare website-analyse. status draft→published→revoked; token = publieke sleutel; content_hash voor idempotente re-gen.';
COMMENT ON TABLE heatr_page_views IS
  'Append-only view-log van teardown-paginas — het intent-fundament (view → beltaak). Bots gefilterd via is_bot.';

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT tablename FROM pg_tables
WHERE schemaname='public' AND tablename IN ('heatr_teardown_pages','heatr_page_views');
