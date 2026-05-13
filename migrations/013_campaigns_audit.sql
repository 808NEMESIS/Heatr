-- 013_campaigns_audit.sql
-- Creates heatr_campaigns met volledige audit-trail.
-- Today: api/main.py:1005 doet `db.table("campaigns").insert(...)` maar de table
-- bestaat niet en de mapping ontbreekt — alle campagnes verdwijnen in /dev/null.
-- Migration creates de table + voegt audit-fields toe (created_by, template_id,
-- sequence_snapshot, lead_ids, gate-results, status-transitions).

CREATE TABLE IF NOT EXISTS public.heatr_campaigns (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id             TEXT NOT NULL,
    warmr_campaign_id        TEXT,                          -- ID at Warmr side
    name                     TEXT NOT NULL,
    template_id              TEXT,                          -- 'v1_cosmetisch_audit' etc.
    status                   TEXT NOT NULL DEFAULT 'draft', -- draft | active | paused | completed | cancelled
    lead_count               INT  NOT NULL DEFAULT 0,
    lead_ids                 JSONB NOT NULL DEFAULT '[]',   -- frozen list at launch time
    inbox_ids                JSONB NOT NULL DEFAULT '[]',
    sequence_snapshot        JSONB,                         -- frozen sequence steps used
    personalization_gate     JSONB,                         -- {auto_count, review_count, skip_count, threshold}
    created_by               TEXT,                          -- 'service:<name>' or 'user:<email>' or 'unknown'
    created_via              TEXT,                          -- 'service_key' | 'user_jwt' | 'legacy_dev'
    request_ip               TEXT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    launched_at              TIMESTAMPTZ,
    completed_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_campaigns_workspace_recent
  ON public.heatr_campaigns (workspace_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_campaigns_warmr_id
  ON public.heatr_campaigns (warmr_campaign_id)
  WHERE warmr_campaign_id IS NOT NULL;

COMMENT ON TABLE  public.heatr_campaigns IS 'Audit-trail van alle campagne-launches. Bevat lead_ids en sequence_snapshot zodat we precies kunnen reproduceren wat er is verstuurd.';
COMMENT ON COLUMN public.heatr_campaigns.created_by  IS 'Authenticated principal: ''service:<name>'' voor X-API-Key, ''user:<email>'' voor JWT, ''legacy_dev'' voor cutover-token.';
COMMENT ON COLUMN public.heatr_campaigns.created_via IS 'service_key | user_jwt | legacy_dev';
