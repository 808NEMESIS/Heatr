-- 021d_rollback.sql — Alleen als je 021b wilt terugdraaien.

DROP INDEX IF EXISTS uq_heatr_companies_ws_domain;
DROP INDEX IF EXISTS uq_heatr_leads_ws_email;

-- RLS terugdraaien HEROPENT de anon-blootstelling — alleen als iets brak dat
-- écht directe anon/authenticated-toegang nodig had.
-- DO $$ DECLARE t text; BEGIN
--   FOR t IN SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
--            WHERE n.nspname='public' AND relkind='r' AND relname LIKE 'heatr\_%' LOOP
--     EXECUTE format('ALTER TABLE public.%I DISABLE ROW LEVEL SECURITY;', t);
--     EXECUTE format('GRANT SELECT ON public.%I TO authenticated;', t);
--   END LOOP; END $$;
