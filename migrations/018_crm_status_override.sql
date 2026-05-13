-- 015_crm_status_override.sql
-- Adds CRM manual-override fields. Activity-endpoint berekent normaal de
-- status uit timeline + classifier output, MAAR Sami moet dit kunnen
-- overrulen (bv. lead zei mondeling "geen interesse" maar mailde nooit
-- terug, of recontact-datum die niet in een classifier-reply zat).
--
-- Override wint van derived-status. NULL = geen override → derived gebruikt.

ALTER TABLE public.heatr_leads
    ADD COLUMN IF NOT EXISTS manual_status_override        TEXT,
    ADD COLUMN IF NOT EXISTS manual_status_override_reason TEXT,
    ADD COLUMN IF NOT EXISTS manual_status_override_at     TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS manual_status_override_by     TEXT,
    -- recontact_later: expliciete datum (bv. "kom terug in september")
    ADD COLUMN IF NOT EXISTS recontact_after               TIMESTAMPTZ,
    -- Source-tagging voor handmatige imports
    ADD COLUMN IF NOT EXISTS imported_at                   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS imported_by                   TEXT,
    ADD COLUMN IF NOT EXISTS imported_source               TEXT;  -- 'csv', 'manual', etc.

-- Filter index voor CRM kanban (recontact-due-deze-week query)
CREATE INDEX IF NOT EXISTS idx_leads_recontact_after
  ON public.heatr_leads (workspace_id, recontact_after)
  WHERE recontact_after IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_leads_manual_override
  ON public.heatr_leads (workspace_id, manual_status_override)
  WHERE manual_status_override IS NOT NULL;

COMMENT ON COLUMN public.heatr_leads.manual_status_override IS
  'CRM status forced door Sami: afgemeld | geen_interesse | recontact_later | actief_gesprek | verkeerde_contact. NULL = derived.';
COMMENT ON COLUMN public.heatr_leads.recontact_after IS
  'Voor recontact_later status: datum waarop lead opnieuw aangeschreven mag.';
