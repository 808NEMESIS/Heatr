-- 033_crawl_instrumentation.sql — crawl-instrumentatie voor de audit-scorer.
--
-- Legt data vast die de bestaande crawl niet opsloeg (zie
-- docs/inventarisatie_crawl_voor_audit.md): screenshots (desktop+mobiel),
-- pre/post-consent netwerkgedrag, content-hashes. Dit is ALLEEN de opslag; de
-- audit-scorer zelf en het scoringgedrag blijven ongewijzigd.
--
-- Idempotent (IF NOT EXISTS). Niet automatisch uitgevoerd — draai in de Supabase
-- SQL-editor.

-- --- website_intelligence: screenshots (2x), content-hashes, timing --------
-- De bestaande kolommen screenshot_url / screenshot_local_path blijven staan
-- (ongebruikt) — niet droppen om niets te breken. Twee screenshots passen daar
-- niet in; daarom aparte desktop/mobile-kolommen.
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS screenshot_desktop_url  text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS screenshot_mobile_url   text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS dom_text_hash           text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS screenshot_desktop_hash text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS screenshot_mobile_hash  text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS capture_timings         jsonb;

-- --- leads: cookie-banner-vlag (naast has_whatsapp / has_online_booking) ----
-- Werd al berekend in de crawl maar nergens opgeslagen. Expliciet true/false:
-- de AFWEZIGHEID van een banner is zelf een signaal (tracking zonder consent).
ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS has_cookie_banner boolean;

-- --- netwerk-log: APPEND-ONLY -----------------------------------------------
-- Bewust NIET unique op lead_id: overschrijven maakt before/after op de
-- pre-consent tracking-check onmogelijk, en dat is de belangrijkste check in het
-- model. Elke scan voegt een rij toe; unique op (lead_id, captured_at).
-- 'requests' = lijst van {url, resource_type, method, status, timing_ms,
-- is_third_party, phase(pre_freeze|post_freeze)}. 'summary' = tellingen.
CREATE TABLE IF NOT EXISTS heatr_website_network_log (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  workspace_id   text NOT NULL,
  lead_id        uuid NOT NULL,
  domain         text,
  captured_at    timestamptz NOT NULL DEFAULT now(),
  dom_text_hash  text,
  requests       jsonb NOT NULL,
  summary        jsonb,
  created_at     timestamptz NOT NULL DEFAULT now(),
  UNIQUE (lead_id, captured_at)
);

CREATE INDEX IF NOT EXISTS idx_website_network_log_lead
  ON heatr_website_network_log(workspace_id, lead_id, captured_at DESC);
