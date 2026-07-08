-- verify_rls_anon.sql — Recovery Patch 1 deliverable.
-- Verificatieplan voor de #1 auditblootstelling: is lead-PII in heatr_* tabellen
-- publiek leesbaar via de anon-rol (PostgREST) omdat RLS ontbreekt?
--
-- ALLEEN-LEZEN. Voer uit in de Supabase SQL editor. Wijzigt niets.
-- De remediatie (RLS aanzetten + anon-grants intrekken) staat als Patch 6-plan
-- in migrations/ en wordt pas ná deze verificatie + impactanalyse toegepast.

-- ============================================================================
-- 1) RLS-status per runtime-tabel. relrowsecurity=false = RLS UIT = risico.
-- ============================================================================
SELECT n.nspname            AS schema,
       c.relname            AS table_name,
       c.relrowsecurity     AS rls_enabled,
       c.relforcerowsecurity AS rls_forced
FROM   pg_class c
JOIN   pg_namespace n ON n.oid = c.relnamespace
WHERE  n.nspname = 'public'
  AND  c.relkind = 'r'
  AND  c.relname LIKE 'heatr\_%'
ORDER  BY c.relrowsecurity ASC, c.relname;   -- RLS-uit bovenaan

-- ============================================================================
-- 2) Directe grants aan de anon-rol op heatr_* tabellen.
--    Elke rij hier = de anon-key kan die operatie via PostgREST uitvoeren.
--    Verwacht/gewenst: LEEG (geen anon-grants).
-- ============================================================================
SELECT grantee, table_name, privilege_type
FROM   information_schema.role_table_grants
WHERE  table_schema = 'public'
  AND  table_name LIKE 'heatr\_%'
  AND  grantee IN ('anon', 'PUBLIC')
ORDER  BY table_name, privilege_type;

-- ============================================================================
-- 3) Grants die de anon-rol ERFT (bv. via PUBLIC of role-membership).
--    has_table_privilege('anon', ...) is de sluitende test: TRUE = leesbaar.
-- ============================================================================
SELECT c.relname AS table_name,
       has_table_privilege('anon', format('public.%I', c.relname), 'SELECT') AS anon_can_select
FROM   pg_class c
JOIN   pg_namespace n ON n.oid = c.relnamespace
WHERE  n.nspname = 'public'
  AND  c.relkind = 'r'
  AND  c.relname LIKE 'heatr\_%'
  AND  has_table_privilege('anon', format('public.%I', c.relname), 'SELECT') = true
ORDER  BY c.relname;   -- ELKE rij hier is een PII-blootstelling

-- ============================================================================
-- 4) Bevestig dat de gevoeligste tabel PII bevat (context voor de blootstelling).
-- ============================================================================
SELECT column_name, data_type
FROM   information_schema.columns
WHERE  table_schema = 'public' AND table_name = 'heatr_leads'
  AND  column_name IN ('email','contact_name','contact_first_name','phone',
                       'contact_linkedin_url','company_name','domain')
ORDER  BY column_name;

-- ----------------------------------------------------------------------------
-- OVER-DE-LIJN SMOKE TEST (buiten SQL — bevestigt de PostgREST-blootstelling):
--
--   curl -s "https://zomdrygdcaenjnrrpcpw.supabase.co/rest/v1/heatr_leads?select=email,company_name&limit=1" \
--        -H "apikey: <ANON_KEY>" -H "Authorization: Bearer <ANON_KEY>"
--
--   → Krijg je rijen terug, dan is lead-PII publiek. Remediatie = Patch 6:
--     ALTER TABLE heatr_leads ENABLE ROW LEVEL SECURITY;  (+ policies)
--     REVOKE ALL ON heatr_leads FROM anon;                (+ per tabel)
--   Doe dit pas ná query 1-3 + impactanalyse (welke policies de service-role
--   en de frontend-JWT nodig hebben) — zie migrations/021 (Patch 6).
-- ----------------------------------------------------------------------------
