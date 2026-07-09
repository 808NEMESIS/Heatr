-- 021c_verify.sql — Draai NA 021b. Bevestigt dat de fix zit.

-- Unique indexen aanwezig? (verwacht: 2 rijen)
SELECT indexname FROM pg_indexes WHERE schemaname='public'
  AND indexname IN ('uq_heatr_companies_ws_domain','uq_heatr_leads_ws_email');

-- RLS aan op alle heatr_-tabellen? (verwacht: LEEG = overal aan)
SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public' AND c.relkind='r' AND c.relname LIKE 'heatr\_%'
  AND c.relrowsecurity = false;

-- Anon/authenticated geen toegang meer? (beide false verwacht)
SELECT has_table_privilege('anon','public.heatr_leads','SELECT')          AS anon_select,
       has_table_privilege('authenticated','public.heatr_leads','SELECT') AS auth_select;
