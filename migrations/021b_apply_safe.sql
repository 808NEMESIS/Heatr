-- 021b_apply_safe.sql — Het VEILIGE minimum. Draai NA 021a.
--
-- Bevat sectie C (RLS + anon-revoke — hoogste waarde, laagste risico; backend
-- draait op service_role en blijft werken) en sectie B (unique dedup — alleen
-- als 021a A1/A2 LEEG waren; anders faalt 't luid, geen corruptie).
--
-- De risicovollere CHECK/FK-onderdelen staan in de hoofd-021-file en pas je
-- pas toe na opschoning van A3/A4.

-- ── C. RLS + REVOKE over alle heatr_-tabellen (sluit de publieke anon-lek) ──
DO $$
DECLARE t text;
BEGIN
  FOR t IN
    SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='public' AND c.relkind='r' AND c.relname LIKE 'heatr\_%'
  LOOP
    EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY;', t);
    EXECUTE format('REVOKE ALL ON public.%I FROM anon;', t);
    EXECUTE format('REVOKE ALL ON public.%I FROM authenticated;', t);
  END LOOP;
END $$;

-- ── B. Unique dedup (alleen draaien als 021a A1 én A2 leeg waren) ───────────
CREATE UNIQUE INDEX IF NOT EXISTS uq_heatr_companies_ws_domain
  ON heatr_companies_raw (workspace_id, lower(domain))
  WHERE domain IS NOT NULL AND domain <> '';

CREATE UNIQUE INDEX IF NOT EXISTS uq_heatr_leads_ws_email
  ON heatr_leads (workspace_id, lower(email))
  WHERE email IS NOT NULL AND email <> '';
