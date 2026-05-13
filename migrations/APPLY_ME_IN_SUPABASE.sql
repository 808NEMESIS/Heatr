-- ============================================================
-- APPLY_ME_IN_SUPABASE.sql
-- Gecombineerde migration: 006 + 007 + crawler v2 extensions.
-- Kopieer dit hele bestand, plak in Supabase SQL editor, druk Run.
-- Alles is idempotent (IF NOT EXISTS) — meermaals uitvoeren is veilig.
-- ============================================================

-- ── leads: Warmr v1.0 datapunten + crawler socials + owner naam ──
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
    ADD COLUMN IF NOT EXISTS instagram_url text,
    ADD COLUMN IF NOT EXISTS facebook_url text,
    ADD COLUMN IF NOT EXISTS linkedin_url text,
    ADD COLUMN IF NOT EXISTS contact_name text,
    ADD COLUMN IF NOT EXISTS kvk_employee_count_range text;

-- ── website_intelligence: competitor aggregaten ──
ALTER TABLE heatr_website_intelligence
    ADD COLUMN IF NOT EXISTS local_competitors_in_db int,
    ADD COLUMN IF NOT EXISTS local_competitors_higher_rating int;

-- ── enrichment_data: email waterfall kolommen ──
ALTER TABLE heatr_enrichment_data
    ADD COLUMN IF NOT EXISTS email_candidate text,
    ADD COLUMN IF NOT EXISTS email_status text;

-- ── Domain-age cache (gratis RDAP lookups, 365d TTL) ──
CREATE TABLE IF NOT EXISTS heatr_domain_age_cache (
    domain text PRIMARY KEY,
    registered_at timestamptz,
    years_old int,
    checked_at timestamptz NOT NULL DEFAULT now(),
    source text NOT NULL DEFAULT 'rdap'
);
CREATE INDEX IF NOT EXISTS idx_domain_age_cache_checked_at
    ON heatr_domain_age_cache (checked_at);

-- ── Meta Ads cache (7d TTL) ──
CREATE TABLE IF NOT EXISTS heatr_meta_ads_cache (
    cache_key text PRIMARY KEY,
    company_name text,
    domain text,
    meta_ads_active boolean,
    ad_focus text,
    raw_snippet jsonb,
    checked_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_meta_ads_cache_checked_at
    ON heatr_meta_ads_cache (checked_at);

-- ── Claude Sonnet Vision cache (screenshot hash → response, 90d TTL) ──
CREATE TABLE IF NOT EXISTS heatr_vision_cache (
    screenshot_hash text PRIMARY KEY,
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

-- ── System-state cache (Warmr inbox cache, etc.) ──
CREATE TABLE IF NOT EXISTS heatr_system_state (
    key text PRIMARY KEY,
    value jsonb,
    expires_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_system_state_expires
    ON heatr_system_state (expires_at);

-- ── Comments (documentation only) ──
COMMENT ON COLUMN heatr_leads.booking_system IS
    'online | contact-form-only | phone-only | unknown. Gezet door conversion_checker.';
COMMENT ON COLUMN heatr_leads.treatment_focus IS
    'Array van 1-5 behandeling-labels (Claude Haiku classifier output).';
COMMENT ON COLUMN heatr_leads.enrichment_blocked_reason IS
    'Reden als enrichment halt wordt door cost_guard (daily_budget, per_lead_ceiling).';
COMMENT ON COLUMN heatr_leads.contact_name IS
    'Volledige eigenaar/beslisser-naam (Claude Haiku via owner_extractor).';
COMMENT ON TABLE heatr_vision_cache IS
    'Cache voor Claude Sonnet Vision analyse. Sleutel = sha256 van screenshot PNG bytes.';

-- Klaar. Verwachte output: ~20 "ALTER TABLE" / "CREATE" statements, allemaal zonder errors.
