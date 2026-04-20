-- ============================================================
-- Migration 004: Discovery schedules + Recontact signals + Reply classifier
-- Run in Supabase SQL Editor.
-- ============================================================

-- 1. Lead discovery schedules (recurring auto-scrapes)
CREATE TABLE IF NOT EXISTS heatr_lead_discovery_schedules (
    id               uuid         PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id     text         NOT NULL,
    sector           text         NOT NULL,
    city             text         NOT NULL,
    country          text         NOT NULL DEFAULT 'NL',
    frequency_days   int          NOT NULL DEFAULT 14,
    target_new_leads int          NOT NULL DEFAULT 20,
    max_results      int          NOT NULL DEFAULT 40,
    last_run_at      timestamptz,
    next_run_at      timestamptz  NOT NULL DEFAULT now(),
    active           boolean      NOT NULL DEFAULT true,
    created_at       timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS hds_ws_next ON heatr_lead_discovery_schedules(workspace_id, next_run_at) WHERE active = true;
CREATE UNIQUE INDEX IF NOT EXISTS hds_unique ON heatr_lead_discovery_schedules(workspace_id, sector, city) WHERE active = true;

-- 2. Outreach snapshots (baseline voor change-signal detection)
CREATE TABLE IF NOT EXISTS heatr_lead_outreach_snapshots (
    id                    uuid         PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id          text         NOT NULL,
    lead_id               uuid         NOT NULL,
    snapshot_at           timestamptz  NOT NULL DEFAULT now(),
    website_hash          text,
    google_rating         numeric(3,1),
    google_review_count   int,
    score_vs_market       int,
    kvk_bestuurder_name   text,
    complaint_count       int          DEFAULT 0,
    has_jobs_page         boolean      DEFAULT false
);

CREATE INDEX IF NOT EXISTS hlos_lead_idx ON heatr_lead_outreach_snapshots(lead_id, snapshot_at DESC);

-- 3. Leads: extra column for KvK bestuurder (used in recontact signal)
ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS kvk_bestuurder_name text;

-- 4. Reply inbox: classification columns
-- Note: reply_inbox might not exist yet — create if missing
CREATE TABLE IF NOT EXISTS heatr_reply_inbox (
    id                        uuid         PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id              text         NOT NULL,
    lead_id                   uuid,
    campaign_id               uuid,
    inbox_id                  text,
    from_email                text,
    subject                   text,
    body                      text,
    received_at               timestamptz  DEFAULT now(),
    classification            text,
    classification_summary    text,
    classification_sentiment  text,
    is_read                   boolean      NOT NULL DEFAULT false
);

ALTER TABLE heatr_reply_inbox ADD COLUMN IF NOT EXISTS classification            text;
ALTER TABLE heatr_reply_inbox ADD COLUMN IF NOT EXISTS classification_summary    text;
ALTER TABLE heatr_reply_inbox ADD COLUMN IF NOT EXISTS classification_sentiment  text;

CREATE INDEX IF NOT EXISTS hri_ws_unclassified ON heatr_reply_inbox(workspace_id) WHERE classification IS NULL;
CREATE INDEX IF NOT EXISTS hri_lead_idx ON heatr_reply_inbox(lead_id, received_at DESC);
