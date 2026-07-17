-- 035_audit_reports.sql — prospect-facing website-audit (tweede scoringslaag).
--
-- APART van heatr_website_intelligence (de interne classifier). De twee scorers
-- delen data, geen logica: website_intelligence.total_score blijft ongemoeid.
-- APART van heatr_teardown_pages (029): dat is de gepubliceerde/gerenderde pagina
-- + view-log (bezorg-laag); dit is de scoring-data (analytische laag). Een
-- teardown-pagina zou later een audit_report renderen.
--
-- APPEND-ONLY, versioned per lead: unique(lead_id, version). v1 = tier 1 (gratis
-- checks), v2 = tier 2 (+ Places + benchmark) blijven allebei staan. Nooit upsert
-- — een verstuurd rapport mag niet veranderen omdat de dataset eromheen groeide.
--
-- Idempotent. Niet automatisch uitgevoerd — draai in de Supabase SQL-editor.

CREATE TABLE IF NOT EXISTS heatr_audit_reports (
    id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id           text NOT NULL,
    lead_id                uuid NOT NULL,
    version                integer NOT NULL,
    tier                   integer NOT NULL DEFAULT 1,        -- 1 = gratis, 2 = + Places/benchmark

    score_total            integer,                           -- behaalde punten (over de meegetelde noemer)
    score_normalized       integer,                           -- 0-100
    score_capped_by        text,                              -- knock-out-reden of NULL
    scored_layers          jsonb,                             -- welke categorieen/lagen meededen (noemer-transparantie)

    categories             jsonb,                             -- per categorie: behaald/max/label
    findings               jsonb,                             -- lijst finding-objecten (check_id, bewijs, mail_safe, ...)
    benchmark              jsonb,                             -- BEVROREN op moment van generatie (geen live query)

    screenshot_desktop_url text,                              -- referentie (bron: website_intelligence / Storage)
    screenshot_mobile_url  text,
    content_hash           text,                              -- dom_text_hash op moment van scan -> rescan-skip
    report_url             text,                              -- gevuld door de renderer (buiten scope)

    created_at             timestamptz NOT NULL DEFAULT now(),
    UNIQUE (lead_id, version)
);

CREATE INDEX IF NOT EXISTS idx_audit_reports_lead
  ON heatr_audit_reports (workspace_id, lead_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_audit_reports_tier
  ON heatr_audit_reports (workspace_id, tier, created_at DESC);

COMMENT ON TABLE heatr_audit_reports IS
  'Prospect-facing website-audit (0-100, 7 categorieen). Append-only, versioned per lead. Los van website_intelligence (interne classifier) en teardown_pages (render/bezorg-laag). benchmark is bevroren op generatie-moment.';
