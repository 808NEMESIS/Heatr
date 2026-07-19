-- 037_missing_base_tables.sql — de 9 basis-schema-tabellen die nooit in prod kwamen.
--
-- Gevonden door de basis-schema-uitbreiding van verify_migrations (2026-07-19):
-- deze tabellen staan in supabase_schema.sql (ongeprefixt) maar bestaan niet in
-- prod (heatr_-prefix-conventie). Alle schrijvers ernaar faalden al die tijd
-- fail-soft: CRM-taken/deals, ICP-feedback-loop, dagmetrics, blocked-sends-log,
-- systeem-alerts en de startup-log deden dus stil niets.
-- (heatr_gdpr_log = aparte migratie 036, vanwege de GDPR-urgentie.)
--
-- Definities 1-op-1 uit supabase_schema.sql, alleen namen + FK-referenties naar
-- de heatr_-prefix gebracht. Idempotent. Draai in de Supabase SQL-editor.

CREATE TABLE IF NOT EXISTS heatr_sector_configs (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text        NOT NULL REFERENCES heatr_workspaces(id) ON DELETE CASCADE,
    sector_key      text        NOT NULL,
    config          jsonb       NOT NULL DEFAULT '{}',
    active          boolean     NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, sector_key)
);

CREATE TABLE IF NOT EXISTS heatr_blocked_sends (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text        NOT NULL,
    lead_id         uuid,
    inbox_id        text,
    block_reason    text        NOT NULL,
    check_name      text,
    blocked_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS heatr_blocked_sends_workspace_idx
  ON heatr_blocked_sends(workspace_id, blocked_at DESC);

CREATE TABLE IF NOT EXISTS heatr_system_alerts (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text        NOT NULL,
    alert_type      text        NOT NULL,
    message         text        NOT NULL,
    severity        text        NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    is_read         boolean     NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS heatr_system_alerts_workspace_unread_idx
  ON heatr_system_alerts(workspace_id, is_read, created_at DESC);

CREATE TABLE IF NOT EXISTS heatr_startup_log (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at      timestamptz NOT NULL DEFAULT now(),
    success         boolean     NOT NULL,
    checks          jsonb,
    warnings        jsonb,
    errors          jsonb,
    duration_ms     int
);

CREATE TABLE IF NOT EXISTS heatr_daily_metrics (
    id                      uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id            text        NOT NULL,
    date                    date        NOT NULL,
    companies_discovered    int         NOT NULL DEFAULT 0,
    leads_enriched          int         NOT NULL DEFAULT 0,
    leads_qualified         int         NOT NULL DEFAULT 0,
    email_coverage_rate     numeric(6,4),
    emails_sent             int         NOT NULL DEFAULT 0,
    emails_blocked          int         NOT NULL DEFAULT 0,
    bounce_count            int         NOT NULL DEFAULT 0,
    bounce_rate             numeric(6,4),
    spam_complaint_count    int         NOT NULL DEFAULT 0,
    unsubscribe_count       int         NOT NULL DEFAULT 0,
    unsubscribe_rate        numeric(6,4),
    open_count              int         NOT NULL DEFAULT 0,
    open_rate               numeric(6,4),
    reply_count             int         NOT NULL DEFAULT 0,
    reply_rate              numeric(6,4),
    interested_count        int         NOT NULL DEFAULT 0,
    meeting_rate            numeric(6,4),
    websites_analysed       int         NOT NULL DEFAULT 0,
    avg_website_score       numeric(5,1),
    urgent_opportunities    int         NOT NULL DEFAULT 0,
    deals_won               int         NOT NULL DEFAULT 0,
    revenue_won             numeric(12,2) NOT NULL DEFAULT 0,
    tasks_completed         int         NOT NULL DEFAULT 0,
    tasks_created           int         NOT NULL DEFAULT 0,
    estimated_cost_eur      numeric(10,4) NOT NULL DEFAULT 0,
    created_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, date)
);
CREATE INDEX IF NOT EXISTS heatr_daily_metrics_workspace_date_idx
  ON heatr_daily_metrics(workspace_id, date DESC);

CREATE TABLE IF NOT EXISTS heatr_icp_definitions (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text        NOT NULL REFERENCES heatr_workspaces(id) ON DELETE CASCADE,
    name            text        NOT NULL,
    sector          text,
    criteria        jsonb       NOT NULL DEFAULT '{}',
    weights         jsonb       NOT NULL DEFAULT '{}',
    active          boolean     NOT NULL DEFAULT true,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS heatr_icp_feedback (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id        text        NOT NULL REFERENCES heatr_workspaces(id) ON DELETE CASCADE,
    lead_id             uuid        NOT NULL REFERENCES heatr_leads(id) ON DELETE CASCADE,
    icp_definition_id   uuid        REFERENCES heatr_icp_definitions(id) ON DELETE SET NULL,
    feedback_type       text        NOT NULL,
    warmr_event         text,
    warmr_campaign_id   text,
    notes               text,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS heatr_icp_feedback_lead
  ON heatr_icp_feedback(lead_id);

CREATE TABLE IF NOT EXISTS heatr_crm_tasks (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text        REFERENCES heatr_workspaces(id),
    lead_id         uuid        REFERENCES heatr_leads(id) ON DELETE CASCADE,
    title           text        NOT NULL,
    description     text,
    task_type       text,
    priority        text        NOT NULL DEFAULT 'medium',
    status          text        NOT NULL DEFAULT 'open',
    due_date        timestamptz,
    snoozed_until   timestamptz,
    completed_at    timestamptz,
    created_by      text        NOT NULL DEFAULT 'system',
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS heatr_crm_tasks_workspace_idx ON heatr_crm_tasks(workspace_id);
CREATE INDEX IF NOT EXISTS heatr_crm_tasks_lead_idx      ON heatr_crm_tasks(lead_id);
CREATE INDEX IF NOT EXISTS heatr_crm_tasks_status_idx    ON heatr_crm_tasks(status);
CREATE INDEX IF NOT EXISTS heatr_crm_tasks_due_date_idx  ON heatr_crm_tasks(due_date);
CREATE INDEX IF NOT EXISTS heatr_tasks_due_open_idx
  ON heatr_crm_tasks(workspace_id, due_date) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS heatr_crm_deals (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id        text        REFERENCES heatr_workspaces(id),
    lead_id             uuid        REFERENCES heatr_leads(id) ON DELETE CASCADE,
    dienst_type         text,
    value               float,
    currency            text        NOT NULL DEFAULT 'EUR',
    project_start_date  date,
    notes               text,
    created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS heatr_crm_deals_workspace_idx ON heatr_crm_deals(workspace_id);
CREATE INDEX IF NOT EXISTS heatr_crm_deals_lead_idx      ON heatr_crm_deals(lead_id);

-- ── Verificatie ──────────────────────────────────────────────────────────────
SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename IN
  ('heatr_sector_configs','heatr_blocked_sends','heatr_system_alerts',
   'heatr_startup_log','heatr_daily_metrics','heatr_icp_definitions',
   'heatr_icp_feedback','heatr_crm_tasks','heatr_crm_deals');
