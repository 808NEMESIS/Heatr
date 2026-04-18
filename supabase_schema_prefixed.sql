-- =============================================================================
-- Heatr — Prefixed schema for shared Warmr database
-- All tables prefixed with heatr_ to avoid conflicts with Warmr tables.
-- When Heatr gets its own Supabase project, remove the prefix.
-- =============================================================================

-- heatr_workspaces
CREATE TABLE IF NOT EXISTS heatr_workspaces (
    id          text        PRIMARY KEY,
    name        text        NOT NULL,
    plan        text        NOT NULL DEFAULT 'starter',
    settings    jsonb       NOT NULL DEFAULT '{}',
    created_at  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO heatr_workspaces (id, name, plan)
VALUES ('aerys', 'Aerys', 'internal')
ON CONFLICT (id) DO NOTHING;

-- heatr_companies_raw
-- Drop the old unprefixed one we created earlier (it has wrong schema)
DROP TABLE IF EXISTS companies_raw CASCADE;

CREATE TABLE IF NOT EXISTS heatr_companies_raw (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id            text        NOT NULL,
    domain                  text,
    company_name            text        NOT NULL,
    sector                  text,
    city                    text,
    country                 text        NOT NULL DEFAULT 'NL',
    phone                   text,
    address                 text,
    google_category         text,
    google_rating           numeric(3,1),
    google_review_count     int,
    google_maps_url         text,
    source                  text        NOT NULL DEFAULT 'google_maps',
    source_url              text,
    business_status         text,
    qualification_status    text,
    disqualification_reason text,
    raw_data                jsonb       NOT NULL DEFAULT '{}',
    created_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS heatr_companies_raw_workspace_domain
    ON heatr_companies_raw (workspace_id, domain);
CREATE INDEX IF NOT EXISTS heatr_companies_raw_sector_city
    ON heatr_companies_raw (workspace_id, sector, city);

-- heatr_leads
CREATE TABLE IF NOT EXISTS heatr_leads (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id            text        NOT NULL,
    company_name            text        NOT NULL,
    domain                  text,
    city                    text,
    sector                  text,
    country                 text        NOT NULL DEFAULT 'NL',

    -- Contact
    contact_name            text,
    contact_first_name      text,
    contact_tussenvoegsel   text,
    contact_last_name       text,
    contact_title           text,
    contact_linkedin_url    text,
    contact_source          text,
    contact_why_chosen      text,
    email                   text,
    email_status            text,
    email_type              text,
    gdpr_safe               boolean     NOT NULL DEFAULT false,
    phone                   text,

    -- Company signals
    google_rating           numeric(3,1),
    google_review_count     int,
    google_maps_url         text,
    google_category         text,
    kvk_number              text,
    kvk_sbi_code            text,
    sbi_code                text,
    instagram_url           text,
    has_instagram           boolean     NOT NULL DEFAULT false,
    has_online_booking      boolean     NOT NULL DEFAULT false,
    has_booking             boolean     NOT NULL DEFAULT false,
    cms_detected            text,
    tracking_tools          jsonb       NOT NULL DEFAULT '[]',

    -- Enrichment
    industry                text,
    company_summary         text,
    company_size_estimate   text,
    employee_count          int,
    personalized_opener     text,
    enrichment_version      int         NOT NULL DEFAULT 0,
    company_positioning     text,
    personalization_hooks   jsonb       NOT NULL DEFAULT '[]',
    personalization_observations jsonb   NOT NULL DEFAULT '[]',

    -- Verification
    confidence_scores       jsonb       NOT NULL DEFAULT '{}',
    data_quality_score      numeric(4,3) NOT NULL DEFAULT 0,
    inconsistency_flags     text[]      NOT NULL DEFAULT '{}',
    source_attribution      jsonb       NOT NULL DEFAULT '{}',

    -- Scoring
    score                   int         NOT NULL DEFAULT 0,
    icp_match               numeric(4,3) NOT NULL DEFAULT 0,
    website_score           int,
    fit_score               int         NOT NULL DEFAULT 0,
    reachability_score      int         NOT NULL DEFAULT 0,
    personalization_potential int        NOT NULL DEFAULT 0,

    -- Status
    status                  text        NOT NULL DEFAULT 'discovered',
    disqualification_reason text,
    source                  text,
    warmr_lead_id           text,
    preferred_inbox_id      text,

    -- CRM
    crm_stage               text        NOT NULL DEFAULT 'ontdekt',
    crm_notes               text,
    snoozed_until           timestamptz,
    next_contact_after      timestamptz,
    contact_attempt_count   int         NOT NULL DEFAULT 0,
    unsubscribed_at         timestamptz,
    unsubscribe_source      text,

    -- Timestamps
    enriched_at             timestamptz,
    scored_at               timestamptz,
    pushed_to_warmr_at      timestamptz,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS heatr_leads_workspace_status ON heatr_leads(workspace_id, status);
CREATE INDEX IF NOT EXISTS heatr_leads_workspace_sector ON heatr_leads(workspace_id, sector);
CREATE INDEX IF NOT EXISTS heatr_leads_domain ON heatr_leads(workspace_id, domain);
CREATE INDEX IF NOT EXISTS heatr_leads_email ON heatr_leads(workspace_id, email);
CREATE INDEX IF NOT EXISTS heatr_leads_score ON heatr_leads(workspace_id, score DESC);
CREATE INDEX IF NOT EXISTS heatr_leads_crm_stage ON heatr_leads(workspace_id, crm_stage);

-- heatr_lead_contacts
-- Drop the old unprefixed one
DROP TABLE IF EXISTS lead_contacts CASCADE;

CREATE TABLE IF NOT EXISTS heatr_lead_contacts (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text        NOT NULL,
    lead_id         uuid        NOT NULL,
    full_name       text        NOT NULL,
    first_name      text,
    tussenvoegsel   text,
    last_name       text,
    title           text,
    seniority_score int         NOT NULL DEFAULT 0,
    linkedin_url    text,
    email_pattern   text,
    source          text        NOT NULL,
    confidence      numeric(4,3) NOT NULL DEFAULT 0,
    is_primary      boolean     NOT NULL DEFAULT false,
    why_chosen      text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS heatr_lead_contacts_lead ON heatr_lead_contacts(lead_id);
CREATE INDEX IF NOT EXISTS heatr_lead_contacts_workspace ON heatr_lead_contacts(workspace_id, lead_id);

-- heatr_website_intelligence
CREATE TABLE IF NOT EXISTS heatr_website_intelligence (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id            text        NOT NULL,
    lead_id                 uuid        NOT NULL UNIQUE,
    domain                  text        NOT NULL,
    total_score             int,
    technical_score         int,
    visual_score            int,
    conversion_score        int,
    sector_score            int,
    technical_details       jsonb       DEFAULT '{}',
    conversion_details      jsonb       DEFAULT '{}',
    sector_details          jsonb       DEFAULT '{}',
    personalization         jsonb       DEFAULT '{}',
    team_contacts           jsonb       DEFAULT '[]',
    opportunity_types       text[]      DEFAULT '{}',
    priority                text,
    opportunity_reasons     jsonb       DEFAULT '{}',
    analyzed_at             timestamptz
);

CREATE INDEX IF NOT EXISTS heatr_wi_workspace ON heatr_website_intelligence(workspace_id);
CREATE INDEX IF NOT EXISTS heatr_wi_lead ON heatr_website_intelligence(lead_id);

-- heatr_enrichment_data
CREATE TABLE IF NOT EXISTS heatr_enrichment_data (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    lead_id         uuid        NOT NULL,
    workspace_id    text        NOT NULL,
    step            text        NOT NULL,
    source          text,
    data            jsonb       NOT NULL DEFAULT '{}',
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS heatr_enrichment_data_lead ON heatr_enrichment_data(lead_id);

-- heatr_scraping_jobs
CREATE TABLE IF NOT EXISTS heatr_scraping_jobs (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text        NOT NULL,
    sector          text        NOT NULL,
    search_query    text        NOT NULL,
    city            text        NOT NULL,
    country         text        NOT NULL DEFAULT 'NL',
    source          text        NOT NULL DEFAULT 'google_maps',
    status          text        NOT NULL DEFAULT 'pending',
    results_found   int         DEFAULT 0,
    results_new     int         DEFAULT 0,
    retry_count     int         DEFAULT 0,
    error_message   text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    started_at      timestamptz,
    completed_at    timestamptz
);

CREATE INDEX IF NOT EXISTS heatr_scraping_jobs_status ON heatr_scraping_jobs(workspace_id, status);

-- heatr_enrichment_jobs
CREATE TABLE IF NOT EXISTS heatr_enrichment_jobs (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id        text        NOT NULL,
    lead_id             uuid        NOT NULL,
    status              text        NOT NULL DEFAULT 'pending',
    current_step        int         DEFAULT 1,
    steps_completed     text[]      DEFAULT '{}',
    enrichment_types    text[]      DEFAULT '{}',
    priority            int         DEFAULT 5,
    retry_count         int         DEFAULT 0,
    error_message       text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    started_at          timestamptz,
    completed_at        timestamptz
);

CREATE INDEX IF NOT EXISTS heatr_enrichment_jobs_status ON heatr_enrichment_jobs(workspace_id, status, priority);
CREATE INDEX IF NOT EXISTS heatr_enrichment_jobs_lead ON heatr_enrichment_jobs(lead_id);

-- heatr_lead_timeline
CREATE TABLE IF NOT EXISTS heatr_lead_timeline (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text        NOT NULL,
    lead_id         uuid,
    event_type      text        NOT NULL,
    title           text        NOT NULL,
    body            text,
    metadata        jsonb       DEFAULT '{}',
    created_by      text        DEFAULT 'system',
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS heatr_timeline_lead ON heatr_lead_timeline(lead_id, created_at DESC);

-- heatr_api_cost_log (Heatr's own cost tracking, separate from Warmr's)
CREATE TABLE IF NOT EXISTS heatr_api_cost_log (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text,
    date            date        NOT NULL DEFAULT current_date,
    model           text        NOT NULL,
    prompt_tokens   int         NOT NULL DEFAULT 0,
    response_tokens int         NOT NULL DEFAULT 0,
    cost_eur        numeric(10,6) NOT NULL DEFAULT 0,
    context         text,
    lead_id         uuid,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS heatr_api_cost_log_date ON heatr_api_cost_log(workspace_id, date);
