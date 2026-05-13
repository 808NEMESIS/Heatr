-- 006_warmr_sequence_fields.sql — Datapunten voor Warmr Sequence v1.0.
-- Allemaal nullable + idempotent. Niet-breaking.

ALTER TABLE heatr_leads
    ADD COLUMN IF NOT EXISTS booking_system text,
    ADD COLUMN IF NOT EXISTS latest_review_date timestamptz,
    ADD COLUMN IF NOT EXISTS treatment_focus text[],
    ADD COLUMN IF NOT EXISTS domain_registered_at timestamptz,
    ADD COLUMN IF NOT EXISTS website_age_years int,
    ADD COLUMN IF NOT EXISTS meta_ads_active boolean,
    ADD COLUMN IF NOT EXISTS ad_focus text,
    ADD COLUMN IF NOT EXISTS meta_ads_checked_at timestamptz,
    ADD COLUMN IF NOT EXISTS enrichment_blocked_reason text,
    ADD COLUMN IF NOT EXISTS enrichment_partial boolean DEFAULT false,
    -- Socials discovered via crawler_v2
    ADD COLUMN IF NOT EXISTS instagram_url text,
    ADD COLUMN IF NOT EXISTS facebook_url text,
    ADD COLUMN IF NOT EXISTS linkedin_url text,
    -- Full owner name (eerdere kolom contact_first_name was voornaam only)
    ADD COLUMN IF NOT EXISTS contact_name text;

ALTER TABLE heatr_website_intelligence
    ADD COLUMN IF NOT EXISTS local_competitors_in_db int,
    ADD COLUMN IF NOT EXISTS local_competitors_higher_rating int;

-- Domain-age cache: shared across workspaces (domain ownership is domain-global)
CREATE TABLE IF NOT EXISTS heatr_domain_age_cache (
    domain text PRIMARY KEY,
    registered_at timestamptz,
    years_old int,
    checked_at timestamptz NOT NULL DEFAULT now(),
    source text NOT NULL DEFAULT 'rdap'
);

CREATE INDEX IF NOT EXISTS idx_domain_age_cache_checked_at
    ON heatr_domain_age_cache (checked_at);

-- Meta Ads cache: shared across workspaces (company ads are globally visible)
CREATE TABLE IF NOT EXISTS heatr_meta_ads_cache (
    cache_key text PRIMARY KEY,        -- sha256(company_name + domain)
    company_name text,
    domain text,
    meta_ads_active boolean,
    ad_focus text,
    raw_snippet jsonb,
    checked_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_meta_ads_cache_checked_at
    ON heatr_meta_ads_cache (checked_at);

-- Vision screenshot cache: shared across leads with identical website-screenshots.
-- Same kliniek = same site = same hash → reuse Sonnet Vision result (save ~€0.014).
CREATE TABLE IF NOT EXISTS heatr_vision_cache (
    screenshot_hash text PRIMARY KEY,   -- sha256(png_bytes)
    domain text,
    result jsonb NOT NULL,
    input_tokens int,
    output_tokens int,
    cost_saved_count int NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_hit_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_vision_cache_created_at
    ON heatr_vision_cache (created_at);

-- Comments for clarity
COMMENT ON COLUMN heatr_leads.booking_system IS
    'online | contact-form-only | phone-only | unknown. Set by conversion_checker.';

COMMENT ON COLUMN heatr_leads.treatment_focus IS
    'Array van 1-5 behandeling-labels, hybrid (Claude vrije tekst met allowlist sanity).';

COMMENT ON COLUMN heatr_leads.enrichment_blocked_reason IS
    'Reden als enrichment halt wordt door cost_guard (daily_budget, per_lead_ceiling).';

COMMENT ON TABLE heatr_vision_cache IS
    'Cache voor Claude Sonnet Vision analyse. Sleutel = sha256 van screenshot PNG bytes. '
    'TTL 90 dagen via periodieke cleanup. cost_saved_count voor telemetry.';
