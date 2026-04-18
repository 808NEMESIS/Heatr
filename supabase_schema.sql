-- =============================================================================
-- Heatr Supabase Schema
-- Platform: B2B lead generation + website intelligence for BENELUX
-- All tables with workspace_id have Row Level Security enabled.
-- Run against Supabase SQL editor or psql to migrate.
-- =============================================================================

-- Enable UUID generation
create extension if not exists "pgcrypto";

-- =============================================================================
-- workspaces
-- Tenant table — every other table is scoped to a workspace_id.
-- =============================================================================
create table if not exists workspaces (
    id          text        primary key,               -- human-readable slug (e.g. 'aerys')
    name        text        not null,
    plan        text        not null default 'starter', -- internal | starter | pro
    settings    jsonb       not null default '{}',
    created_at  timestamptz not null default now()
);

-- Seed: internal Aerys workspace
insert into workspaces (id, name, plan)
values ('aerys', 'Aerys', 'internal')
on conflict (id) do nothing;

-- =============================================================================
-- sector_configs
-- Per-workspace sector overrides stored as JSONB (base configs live in config/sectors.py).
-- =============================================================================
create table if not exists sector_configs (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        not null references workspaces(id) on delete cascade,
    sector_key      text        not null,
    config          jsonb       not null default '{}',
    active          boolean     not null default true,
    created_at      timestamptz not null default now(),
    unique (workspace_id, sector_key)
);

alter table sector_configs enable row level security;

create policy "workspace members see own sector_configs"
    on sector_configs for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

-- =============================================================================
-- companies_raw
-- Raw company records as scraped from Google Maps, directories, KvK, etc.
-- One row per unique domain per workspace — deduplication target for all scrapers.
-- =============================================================================
create table if not exists companies_raw (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        not null references workspaces(id) on delete cascade,
    domain          text,                              -- cleaned domain (no protocol, no trailing slash)
    company_name    text        not null,
    sector          text        not null,
    city            text,
    country         text        not null default 'NL',
    phone           text,
    address         text,
    google_rating   numeric(3,1),
    google_review_count int,
    google_maps_url text,
    source          text        not null,              -- google_maps | directory | kvk | manual
    source_url      text,
    scraping_job_id uuid,
    raw_data        jsonb       not null default '{}', -- full scraped payload
    created_at      timestamptz not null default now()
);

alter table companies_raw enable row level security;

create policy "workspace members see own companies_raw"
    on companies_raw for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create index if not exists companies_raw_workspace_domain
    on companies_raw (workspace_id, domain);

create index if not exists companies_raw_sector_city
    on companies_raw (workspace_id, sector, city);

-- =============================================================================
-- leads
-- Enriched, scored, GDPR-checked leads ready for review and Warmr push.
-- Central entity of Heatr — everything relates back to this table.
-- =============================================================================
create table if not exists leads (
    id                      uuid        primary key default gen_random_uuid(),
    workspace_id            text        not null references workspaces(id) on delete cascade,
    company_raw_id          uuid        references companies_raw(id) on delete set null,
    company_name            text        not null,
    domain                  text,
    city                    text,
    sector                  text        not null,
    country                 text        not null default 'NL',

    -- Contact
    contact_name            text,
    contact_first_name      text,
    contact_tussenvoegsel   text,
    contact_last_name       text,
    email                   text,
    email_status            text,       -- valid | risky | invalid | not_found
    email_type              text,       -- role | personal | unknown
    gdpr_safe               boolean     not null default false,
    phone                   text,

    -- Company signals
    google_rating           numeric(3,1),
    google_review_count     int,
    kvk_number              text,
    kvk_sbi_code            text,
    instagram_url           text,
    has_instagram           boolean     not null default false,
    has_online_booking      boolean     not null default false,
    cms_detected            text,
    tracking_tools          jsonb       not null default '[]',

    -- Scoring
    score                   int         not null default 0,  -- lead score 0-100
    icp_match               numeric(4,3) not null default 0, -- 0.0 to 1.0
    website_score           int,                             -- 0-100 from website_intelligence

    -- Status
    status                  text        not null default 'discovered',
    -- discovered | enriched | scored | qualified | pushed_to_warmr | disqualified
    disqualify_reason       text,
    warmr_lead_id           text,       -- Warmr's internal ID after push

    -- Timestamps
    enriched_at             timestamptz,
    scored_at               timestamptz,
    pushed_to_warmr_at      timestamptz,
    created_at              timestamptz not null default now(),
    updated_at              timestamptz not null default now()
);

alter table leads enable row level security;

create policy "workspace members see own leads"
    on leads for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create index if not exists leads_workspace_status
    on leads (workspace_id, status);

create index if not exists leads_workspace_sector
    on leads (workspace_id, sector);

create index if not exists leads_domain
    on leads (workspace_id, domain);

create index if not exists leads_email
    on leads (workspace_id, email);

-- Auto-update updated_at
create or replace function update_updated_at()
returns trigger language plpgsql as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

create trigger leads_updated_at
    before update on leads
    for each row execute function update_updated_at();

-- =============================================================================
-- website_intelligence
-- Full 5-layer website analysis result per lead/domain.
-- Drives the website opportunities view and the Aerys sales pitch.
-- =============================================================================
create table if not exists website_intelligence (
    id                      uuid        primary key default gen_random_uuid(),
    workspace_id            text        not null references workspaces(id) on delete cascade,
    lead_id                 uuid        not null references leads(id) on delete cascade,
    domain                  text        not null,

    -- Aggregate scores (0-100 total, layers summed)
    total_score             int,        -- sum of all layers, max 100
    technical_score         int,        -- layer 1, max 25
    visual_score            int,        -- layer 2, max 25 (Claude Vision)
    conversion_score        int,        -- layer 3, max 30
    sector_score            int,        -- layer 4, max 15
    score_vs_market         int,        -- delta vs top-3 competitor average (can be negative)

    -- Layer 1 — Technical
    has_ssl                 boolean,
    is_mobile_friendly      boolean,
    pagespeed_mobile        int,
    pagespeed_desktop       int,
    cms_detected            text,
    server_country          text,
    has_sitemap             boolean,
    has_schema_markup       boolean,
    technical_data          jsonb       not null default '{}',

    -- Layer 2 — Visual (Claude Sonnet Vision)
    screenshot_url          text,       -- Supabase Storage public URL
    screenshot_local_path   text,       -- /tmp/screenshots/{domain}.png
    claude_vision_analysis  jsonb       not null default '{}',
    -- e.g. { "overall": 7, "typography": 6, "color": 8, "whitespace": 5,
    --        "images": 6, "trust_signals": 4, "mobile": 7, "sector_fit": 8,
    --        "strengths": [...], "improvements": [...], "inspiration": [...] }

    -- Layer 3 — Conversion
    has_primary_cta         boolean,
    cta_above_fold          boolean,
    cta_text                text,
    cta_strength_score      int,        -- Claude-rated 1-5
    phone_clickable         boolean,
    has_whatsapp            boolean,
    has_online_booking      boolean,
    has_chatbot             boolean,
    chatbot_platform        text,       -- Intercom | Drift | Tidio | Landbot | Trengo | WhatsApp Business
    chatbot_response_time   int,        -- seconds, null if no chatbot
    contact_form_fields     int,        -- number of fields in contact form
    conversion_data         jsonb       not null default '{}',

    -- Layer 4 — Sector specific
    sector_data             jsonb       not null default '{}',

    -- Layer 5 — Competitor comparison
    competitor_data         jsonb       not null default '{}',
    -- e.g. { "competitors": [...], "market_avg_score": 67, "our_rank": 3 }

    -- Opportunity classification
    opportunity_types       text[]      not null default '{}',
    -- website_rebuild | conversion_optimization | chatbot | ai_audit
    opportunity_priority    text,       -- urgent | high | medium | low
    chatbot_opportunity     boolean     not null default false,

    analyzed_at             timestamptz,
    created_at              timestamptz not null default now(),
    updated_at              timestamptz not null default now()
);

alter table website_intelligence enable row level security;

create policy "workspace members see own website_intelligence"
    on website_intelligence for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create index if not exists website_intelligence_lead
    on website_intelligence (workspace_id, lead_id);

create index if not exists website_intelligence_opportunity
    on website_intelligence (workspace_id, opportunity_priority);

create trigger website_intelligence_updated_at
    before update on website_intelligence
    for each row execute function update_updated_at();

-- =============================================================================
-- enrichment_data
-- Audit log of every email discovery attempt per lead, one row per waterfall step.
-- Allows replay and debugging of the 4-step email waterfall.
-- =============================================================================
create table if not exists enrichment_data (
    id                  uuid        primary key default gen_random_uuid(),
    workspace_id        text        not null references workspaces(id) on delete cascade,
    lead_id             uuid        not null references leads(id) on delete cascade,
    enrichment_step     int         not null, -- 1=website 2=pattern 3=google_search 4=kvk
    source              text        not null, -- website | pattern | google_search | kvk
    email_candidate     text,
    email_verified      boolean,
    email_status        text,                 -- valid | risky | invalid | catch_all
    catch_all           boolean     not null default false,
    mx_records          jsonb       not null default '[]',
    raw_result          jsonb       not null default '{}',
    succeeded           boolean     not null default false,
    created_at          timestamptz not null default now()
);

alter table enrichment_data enable row level security;

create policy "workspace members see own enrichment_data"
    on enrichment_data for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create index if not exists enrichment_data_lead
    on enrichment_data (workspace_id, lead_id);

-- =============================================================================
-- scraping_jobs
-- Tracks each Google Maps / directory / KvK scraping run with its status and results.
-- =============================================================================
create table if not exists scraping_jobs (
    id                  uuid        primary key default gen_random_uuid(),
    workspace_id        text        not null references workspaces(id) on delete cascade,
    sector              text        not null,
    city                text,
    country             text        not null default 'NL',
    source              text        not null, -- google_maps | directory | kvk | google_search
    search_query        text,
    status              text        not null default 'pending',
    -- pending | running | completed | failed | paused
    total_found         int         not null default 0,
    total_new           int         not null default 0, -- after dedup
    total_enriched      int         not null default 0,
    error_message       text,
    worker_id           text,
    started_at          timestamptz,
    completed_at        timestamptz,
    created_at          timestamptz not null default now()
);

alter table scraping_jobs enable row level security;

create policy "workspace members see own scraping_jobs"
    on scraping_jobs for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create index if not exists scraping_jobs_status
    on scraping_jobs (workspace_id, status);

-- =============================================================================
-- enrichment_jobs
-- Async queue for the 4-step email waterfall per lead.
-- =============================================================================
create table if not exists enrichment_jobs (
    id                  uuid        primary key default gen_random_uuid(),
    workspace_id        text        not null references workspaces(id) on delete cascade,
    lead_id             uuid        not null references leads(id) on delete cascade,
    status              text        not null default 'pending',
    -- pending | running | completed | failed
    current_step        int         not null default 1,
    steps_completed     int[]       not null default '{}',
    error_message       text,
    worker_id           text,
    started_at          timestamptz,
    completed_at        timestamptz,
    created_at          timestamptz not null default now()
);

alter table enrichment_jobs enable row level security;

create policy "workspace members see own enrichment_jobs"
    on enrichment_jobs for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create index if not exists enrichment_jobs_status
    on enrichment_jobs (workspace_id, status);

create index if not exists enrichment_jobs_lead
    on enrichment_jobs (workspace_id, lead_id);

-- =============================================================================
-- icp_definitions
-- Ideal Customer Profile definitions that drive the ICP matcher and auto-scoring.
-- =============================================================================
create table if not exists icp_definitions (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        not null references workspaces(id) on delete cascade,
    name            text        not null,
    sector          text,       -- null = applies to all sectors
    criteria        jsonb       not null default '{}',
    -- e.g. { "min_google_rating": 4.0, "max_employees": 15, "requires_email": true,
    --        "preferred_cities": ["Amsterdam", "Utrecht"], "keywords": [...] }
    weights         jsonb       not null default '{}',
    active          boolean     not null default true,
    created_at      timestamptz not null default now(),
    updated_at      timestamptz not null default now()
);

alter table icp_definitions enable row level security;

create policy "workspace members see own icp_definitions"
    on icp_definitions for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create trigger icp_definitions_updated_at
    before update on icp_definitions
    for each row execute function update_updated_at();

-- =============================================================================
-- icp_feedback
-- Warmr reply events fed back into Heatr — drives automatic ICP score improvements.
-- =============================================================================
create table if not exists icp_feedback (
    id                  uuid        primary key default gen_random_uuid(),
    workspace_id        text        not null references workspaces(id) on delete cascade,
    lead_id             uuid        not null references leads(id) on delete cascade,
    icp_definition_id   uuid        references icp_definitions(id) on delete set null,
    feedback_type       text        not null,
    -- interested | replied | meeting_booked | bounced | not_interested | disqualified
    warmr_event         text,       -- raw event name from Warmr webhook
    warmr_campaign_id   text,
    notes               text,
    created_at          timestamptz not null default now()
);

alter table icp_feedback enable row level security;

create policy "workspace members see own icp_feedback"
    on icp_feedback for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create index if not exists icp_feedback_lead
    on icp_feedback (workspace_id, lead_id);

-- =============================================================================
-- lead_campaign_history
-- Tracks every Warmr campaign a lead has been part of — used for 90-day cooldown.
-- =============================================================================
create table if not exists lead_campaign_history (
    id                  uuid        primary key default gen_random_uuid(),
    workspace_id        text        not null references workspaces(id) on delete cascade,
    lead_id             uuid        not null references leads(id) on delete cascade,
    warmr_campaign_id   text        not null,
    warmr_inbox_id      text,
    status              text        not null default 'active',
    -- active | completed | stopped | bounced
    sent_at             timestamptz not null default now(),
    last_event          text,       -- interested | replied | bounced | unsubscribed
    last_event_at       timestamptz,
    created_at          timestamptz not null default now()
);

alter table lead_campaign_history enable row level security;

create policy "workspace members see own lead_campaign_history"
    on lead_campaign_history for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create index if not exists lead_campaign_history_lead
    on lead_campaign_history (workspace_id, lead_id);

create index if not exists lead_campaign_history_active
    on lead_campaign_history (workspace_id, lead_id, status)
    where status = 'active';

-- =============================================================================
-- reply_inbox
-- Inbound Warmr webhook events (replies, bounces, interests) stored for the unified inbox.
-- =============================================================================
create table if not exists reply_inbox (
    id                  uuid        primary key default gen_random_uuid(),
    workspace_id        text        not null references workspaces(id) on delete cascade,
    lead_id             uuid        references leads(id) on delete set null,
    warmr_message_id    text        unique,
    warmr_inbox_id      text,
    warmr_campaign_id   text,
    from_email          text,
    subject             text,
    body                text,
    reply_type          text,
    -- interested | not_interested | auto_reply | bounce | unsubscribe | other
    processed           boolean     not null default false,
    processed_at        timestamptz,
    created_at          timestamptz not null default now()
);

alter table reply_inbox enable row level security;

create policy "workspace members see own reply_inbox"
    on reply_inbox for all
    using (workspace_id = current_setting('app.current_workspace_id', true));

create index if not exists reply_inbox_unprocessed
    on reply_inbox (workspace_id, processed)
    where processed = false;

create index if not exists reply_inbox_lead
    on reply_inbox (workspace_id, lead_id);

-- =============================================================================
-- rate_limit_state
-- Shared token bucket state for all workers (multi-worker safe via Supabase).
-- No workspace_id — rate limits are global per service.
-- =============================================================================
create table if not exists rate_limit_state (
    service         text        primary key,   -- google_maps | google_search | kvk_api | etc.
    tokens          numeric(10,4) not null,    -- current token count (float for partial refills)
    max_tokens      numeric(10,4) not null,    -- bucket capacity
    refill_rate     numeric(10,6) not null,    -- tokens added per second
    last_refill     timestamptz not null default now(),
    updated_at      timestamptz not null default now()
);

-- Seed initial rate limit buckets
insert into rate_limit_state (service, tokens, max_tokens, refill_rate) values
    ('google_maps',   10,   10,   0.016667),  -- 10 req/min (60/hr spread)
    ('google_search',  2,    2,   0.002778),  -- 10 req/hr max
    ('kvk_api',        5,    5,   0.027778),  -- 100 req/hr
    ('warmr_api',     20,   20,   0.333333),  -- 1200 req/hr
    ('pagespeed_api', 10,   10,   0.111111),  -- 400 req/hr
    ('claude_haiku',  10,   10,   0.833333),  -- 50 req/min
    ('claude_sonnet',  5,    5,   0.333333),  -- 20 req/min
    ('smtp_verify',    3,    3,   0.000833)   -- 3 per domain per hour
on conflict (service) do nothing;

-- =============================================================================
-- Session 2 addendum: retry_count on scraping_jobs
-- =============================================================================
alter table scraping_jobs add column if not exists retry_count int not null default 0;

-- =============================================================================
-- Session 3 addendum: new columns on leads
-- =============================================================================
alter table leads add column if not exists preferred_inbox_id     text;
alter table leads add column if not exists industry               text;
alter table leads add column if not exists company_summary        text;
alter table leads add column if not exists personalized_opener    text;
alter table leads add column if not exists company_size_estimate  text;
alter table leads add column if not exists enrichment_version     int  not null default 0;
alter table leads add column if not exists google_category        text;
alter table leads add column if not exists kvk_sbi_code           text;
alter table leads add column if not exists kvk_founding_year      int;
alter table leads add column if not exists kvk_employee_count_range text;
alter table leads add column if not exists kvk_legal_form         text;
alter table leads add column if not exists has_whatsapp           boolean not null default false;

-- enrichment_jobs: priority + enrichment_types + retry_count (not in session 1 schema)
alter table enrichment_jobs add column if not exists priority          int  not null default 5;
alter table enrichment_jobs add column if not exists enrichment_types  text[];
alter table enrichment_jobs add column if not exists retry_count       int  not null default 0;

-- =============================================================================
-- domain_cache
-- Stores MX + catch-all detection results with a 7-day TTL.
-- Prevents re-checking the same domain repeatedly during bulk enrichment.
-- =============================================================================
create table if not exists domain_cache (
    domain      text        primary key,
    has_mx      boolean     not null default false,
    is_catchall boolean     not null default false,
    checked_at  timestamptz not null default now(),
    expires_at  timestamptz not null
);

-- =============================================================================
-- system_state
-- Key-value store for ephemeral system state with TTL expiry.
-- Used by: Google Search CAPTCHA blocks, Warmr inbox cache.
-- =============================================================================
create table if not exists system_state (
    key         text        primary key,
    value       text        not null default '',
    expires_at  timestamptz not null
);

-- =============================================================================
-- crm_tasks
-- Sales tasks tied to leads. Supports due dates, snoozing, and priority.
-- =============================================================================
create table if not exists crm_tasks (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        references workspaces(id),
    lead_id         uuid        references leads(id) on delete cascade,
    title           text        not null,
    description     text,
    task_type       text,
    -- call | email | linkedin | offerte | follow_up | onboarding | other
    priority        text        not null default 'medium',
    -- urgent | high | medium | low
    status          text        not null default 'open',
    -- open | completed | snoozed | cancelled
    due_date        timestamptz,
    snoozed_until   timestamptz,
    completed_at    timestamptz,
    created_by      text        not null default 'system',
    created_at      timestamptz not null default now()
);

create index if not exists crm_tasks_workspace_idx  on crm_tasks(workspace_id);
create index if not exists crm_tasks_lead_idx       on crm_tasks(lead_id);
create index if not exists crm_tasks_status_idx     on crm_tasks(status);
create index if not exists crm_tasks_due_date_idx   on crm_tasks(due_date);

alter table crm_tasks enable row level security;
create policy "workspace_isolation_crm_tasks"
    on crm_tasks for all
    using (workspace_id = current_setting('app.workspace_id', true));

-- =============================================================================
-- lead_timeline
-- Immutable append-only log of every event for a lead.
-- Used by the CRM slide panel as the primary activity feed.
-- =============================================================================
create table if not exists lead_timeline (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        references workspaces(id),
    lead_id         uuid        references leads(id) on delete cascade,
    event_type      text        not null,
    -- discovered | email_verified | website_analysed | email_sent |
    -- reply_received | stage_changed | task_created | task_completed |
    -- note_added | call_logged | meeting_logged | deal_won | deal_lost |
    -- review_email_sent | warmr_event | sequence_completed | snoozed
    title           text        not null,
    body            text,
    metadata        jsonb       not null default '{}',
    created_by      text        not null default 'system',
    created_at      timestamptz not null default now()
);

create index if not exists lead_timeline_workspace_idx  on lead_timeline(workspace_id);
create index if not exists lead_timeline_lead_idx       on lead_timeline(lead_id);
create index if not exists lead_timeline_created_idx    on lead_timeline(created_at desc);
create index if not exists lead_timeline_type_idx       on lead_timeline(event_type);

alter table lead_timeline enable row level security;
create policy "workspace_isolation_lead_timeline"
    on lead_timeline for all
    using (workspace_id = current_setting('app.workspace_id', true));

-- =============================================================================
-- crm_deals
-- Closed/won deals with deal value for revenue tracking.
-- =============================================================================
create table if not exists crm_deals (
    id                  uuid        primary key default gen_random_uuid(),
    workspace_id        text        references workspaces(id),
    lead_id             uuid        references leads(id) on delete cascade,
    dienst_type         text,
    -- website | conversie_optimalisatie | chatbot | ai_audit | combinatie
    value               float,
    currency            text        not null default 'EUR',
    project_start_date  date,
    notes               text,
    created_at          timestamptz not null default now()
);

create index if not exists crm_deals_workspace_idx  on crm_deals(workspace_id);
create index if not exists crm_deals_lead_idx       on crm_deals(lead_id);

alter table crm_deals enable row level security;
create policy "workspace_isolation_crm_deals"
    on crm_deals for all
    using (workspace_id = current_setting('app.workspace_id', true));

-- =============================================================================
-- leads: add crm_stage + snoozed_until columns if not already present
-- =============================================================================
alter table leads add column if not exists crm_stage      text not null default 'ontdekt';
alter table leads add column if not exists snoozed_until  timestamptz;
alter table leads add column if not exists crm_notes      text;

-- =============================================================================
-- leads: additional columns for Session 7 hardening
-- =============================================================================
alter table leads add column if not exists disqualification_reason  text;
alter table leads add column if not exists unsubscribed_at          timestamptz;
alter table leads add column if not exists unsubscribe_source       text;   -- warmr_webhook | manual | gdpr
alter table leads add column if not exists next_contact_after       timestamptz;
alter table leads add column if not exists contact_attempt_count    int  not null default 0;

-- =============================================================================
-- leads: data verification + contact discovery + personalization columns
-- =============================================================================
alter table leads add column if not exists confidence_scores          jsonb not null default '{}';
alter table leads add column if not exists data_quality_score         numeric(4,3) not null default 0;
alter table leads add column if not exists inconsistency_flags        text[] not null default '{}';
alter table leads add column if not exists source_attribution         jsonb not null default '{}';
alter table leads add column if not exists contact_title              text;
alter table leads add column if not exists contact_linkedin_url       text;
alter table leads add column if not exists contact_source             text;
alter table leads add column if not exists contact_why_chosen         text;
alter table leads add column if not exists fit_score                  int not null default 0;
alter table leads add column if not exists reachability_score         int not null default 0;
alter table leads add column if not exists personalization_potential   int not null default 0;
alter table leads add column if not exists personalization_hooks      jsonb not null default '[]';
alter table leads add column if not exists personalization_observations jsonb not null default '[]';
alter table leads add column if not exists company_positioning        text;

-- =============================================================================
-- lead_contacts: discovered contact persons per lead
-- =============================================================================
create table if not exists lead_contacts (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        not null,
    lead_id         uuid        not null,
    full_name       text        not null,
    first_name      text,
    tussenvoegsel   text,
    last_name       text,
    title           text,
    seniority_score int         not null default 0,
    linkedin_url    text,
    email_pattern   text,
    source          text        not null,
    confidence      numeric(4,3) not null default 0,
    is_primary      boolean     not null default false,
    why_chosen      text,
    created_at      timestamptz not null default now()
);

create index if not exists lead_contacts_lead_idx on lead_contacts(lead_id);
create index if not exists lead_contacts_workspace_idx on lead_contacts(workspace_id, lead_id);

alter table lead_contacts enable row level security;
create policy "workspace_isolation_lead_contacts"
    on lead_contacts for all
    using (workspace_id = current_setting('app.workspace_id', true));

-- status column: extend possible values to include 'forgotten', 'unsubscribed'
-- (no DDL change needed — values are text; just documenting allowed values)

-- =============================================================================
-- lead_campaign_history: add sequence engine columns
-- =============================================================================
alter table lead_campaign_history add column if not exists step_index      int  not null default 0;
alter table lead_campaign_history add column if not exists sequence_steps  jsonb;
alter table lead_campaign_history add column if not exists next_send_at    timestamptz;
alter table lead_campaign_history add column if not exists sent_at         timestamptz;
alter table lead_campaign_history add column if not exists block_reason    text;
alter table lead_campaign_history add column if not exists sending_domain  text;
alter table lead_campaign_history add column if not exists inbox_id        text;
alter table lead_campaign_history add column if not exists campaign_id     uuid;
alter table lead_campaign_history add column if not exists is_active       boolean not null default true;

-- =============================================================================
-- indexes — from MVP_GAPS.md audit
-- =============================================================================
create index if not exists leads_crm_stage_idx          on leads(workspace_id, crm_stage);
create index if not exists leads_score_idx              on leads(workspace_id, score desc);
create index if not exists leads_email_status_idx       on leads(workspace_id, email_status);
create index if not exists leads_snoozed_until_idx      on leads(snoozed_until) where crm_stage = 'later';
create index if not exists leads_next_contact_idx       on leads(next_contact_after) where status = 'no_response';
create index if not exists tasks_due_open_idx           on crm_tasks(workspace_id, due_date) where status = 'open';
create index if not exists lch_next_send_idx            on lead_campaign_history(workspace_id, next_send_at) where status = 'pending' and is_active = true;
create index if not exists lch_lead_idx                 on lead_campaign_history(lead_id);
create index if not exists timeline_lead_created_idx    on lead_timeline(lead_id, created_at desc);

-- =============================================================================
-- startup_log — startup validation results
-- =============================================================================
create table if not exists startup_log (
    id              uuid        primary key default gen_random_uuid(),
    started_at      timestamptz not null default now(),
    success         boolean     not null,
    checks          jsonb,       -- array of {name, passed, detail}
    warnings        jsonb,
    errors          jsonb,
    duration_ms     int
);

-- =============================================================================
-- claude_cache — LLM response cache (7-day TTL)
-- =============================================================================
create table if not exists claude_cache (
    cache_key       text        primary key,   -- SHA-256 hex
    workspace_id    text        not null,
    model           text        not null,
    response_text   text        not null,
    prompt_tokens   int,
    response_tokens int,
    hit_count       int         not null default 0,
    created_at      timestamptz not null default now(),
    expires_at      timestamptz not null,
    last_hit_at     timestamptz
);

create index if not exists claude_cache_expires_idx on claude_cache(expires_at);

-- =============================================================================
-- api_cost_log — per-call Claude cost tracking
-- =============================================================================
create table if not exists api_cost_log (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        not null,
    date            date        not null default current_date,
    model           text        not null,
    prompt_tokens   int         not null default 0,
    response_tokens int         not null default 0,
    cost_eur        numeric(10, 6) not null default 0,
    cache_hit       boolean     not null default false,
    context         text,       -- e.g. 'company_enrichment', 'vision_analysis'
    lead_id         uuid,
    created_at      timestamptz not null default now()
);

create index if not exists api_cost_log_workspace_date_idx on api_cost_log(workspace_id, date);

-- =============================================================================
-- blocked_sends — log of all SendingGuard blocks
-- =============================================================================
create table if not exists blocked_sends (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        not null,
    lead_id         uuid,
    inbox_id        text,
    block_reason    text        not null,
    check_name      text,       -- which guard check triggered
    blocked_at      timestamptz not null default now()
);

create index if not exists blocked_sends_workspace_idx on blocked_sends(workspace_id, blocked_at desc);

-- =============================================================================
-- system_alerts — in-app alert notifications
-- =============================================================================
create table if not exists system_alerts (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        not null,
    alert_type      text        not null,
    message         text        not null,
    severity        text        not null check (severity in ('info', 'warning', 'critical')),
    is_read         boolean     not null default false,
    created_at      timestamptz not null default now()
);

create index if not exists system_alerts_workspace_unread_idx on system_alerts(workspace_id, is_read, created_at desc);

alter table system_alerts enable row level security;
create policy "workspace_isolation_system_alerts"
    on system_alerts for all
    using (workspace_id = current_setting('app.workspace_id', true));

-- =============================================================================
-- gdpr_log — audit trail for forget + export requests
-- =============================================================================
create table if not exists gdpr_log (
    id              uuid        primary key default gen_random_uuid(),
    workspace_id    text        not null,
    action          text        not null check (action in ('forget', 'export', 'register_view')),
    lead_id         uuid,
    lead_email      text,
    performed_by    text        not null default 'user',
    completed_at    timestamptz not null default now(),
    metadata        jsonb
);

create index if not exists gdpr_log_workspace_idx on gdpr_log(workspace_id, completed_at desc);

-- =============================================================================
-- daily_metrics — KPI snapshot per workspace per day
-- =============================================================================
create table if not exists daily_metrics (
    id                      uuid        primary key default gen_random_uuid(),
    workspace_id            text        not null,
    date                    date        not null,

    -- Discovery
    companies_discovered    int         not null default 0,
    leads_enriched          int         not null default 0,
    leads_qualified         int         not null default 0,
    email_coverage_rate     numeric(6,4),

    -- Sending
    emails_sent             int         not null default 0,
    emails_blocked          int         not null default 0,
    bounce_count            int         not null default 0,
    bounce_rate             numeric(6,4),
    spam_complaint_count    int         not null default 0,
    unsubscribe_count       int         not null default 0,
    unsubscribe_rate        numeric(6,4),
    open_count              int         not null default 0,
    open_rate               numeric(6,4),
    reply_count             int         not null default 0,
    reply_rate              numeric(6,4),
    interested_count        int         not null default 0,
    meeting_rate            numeric(6,4),

    -- Website intelligence
    websites_analysed       int         not null default 0,
    avg_website_score       numeric(5,1),
    urgent_opportunities    int         not null default 0,
    review_emails_sent      int         not null default 0,

    -- CRM
    deals_won               int         not null default 0,
    revenue_won             numeric(12,2) not null default 0,
    tasks_completed         int         not null default 0,
    tasks_created           int         not null default 0,

    -- Costs
    estimated_cost_eur      numeric(10,4) not null default 0,

    created_at              timestamptz not null default now(),

    unique (workspace_id, date)
);

create index if not exists daily_metrics_workspace_date_idx on daily_metrics(workspace_id, date desc);
