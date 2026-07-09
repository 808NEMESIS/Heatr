-- ============================================================================
-- 021_datamodel_integrity.sql — Recovery Patch 6 (datamodel-integriteit)
--
-- Dekt: DB-niveau dedup (unique), RLS + anon-revoke (de #1 PII-blootstelling),
-- CHECK-enums op statusvelden. GEEN blinde migratie: draai eerst de PREFLIGHT,
-- pas de FORWARD alleen toe als de preflight schoon is, VERIFY daarna, ROLLBACK
-- staat onderaan. Alles idempotent (IF NOT EXISTS) waar mogelijk.
--
-- Belangrijk: de runtime-backend gebruikt de SERVICE_ROLE-key, die RLS bypasst.
-- RLS + revoke aan anon/authenticated breekt de app dus NIET — het sluit alleen
-- de publieke anon-key (browser-bundle) af. Zie ook deploy/sql/verify_rls_anon.sql.
-- ============================================================================


-- ============================================================================
-- SECTIE A — PREFLIGHT (ALLEEN-LEZEN). Draai dit eerst; alles moet 0/leeg zijn
-- vóór de FORWARD, anders eerst opschonen.
-- ============================================================================

-- A1. Duplicaat-domeinen per workspace (blokkeert de unique index op companies).
SELECT workspace_id, lower(domain) AS domain, count(*) AS n
FROM   heatr_companies_raw
WHERE  domain IS NOT NULL AND domain <> ''
GROUP  BY workspace_id, lower(domain)
HAVING count(*) > 1
ORDER  BY n DESC;

-- A2. Duplicaat-e-mails per workspace (blokkeert de unique index op leads).
SELECT workspace_id, lower(email) AS email, count(*) AS n
FROM   heatr_leads
WHERE  email IS NOT NULL AND email <> ''
GROUP  BY workspace_id, lower(email)
HAVING count(*) > 1
ORDER  BY n DESC;

-- A3. Statuswaarden die BUITEN de beoogde enum vallen (blokkeert de CHECK).
--     Elke rij hier moet eerst genormaliseerd/opgeruimd worden.
SELECT 'leads.status' AS col, status AS val, count(*) n FROM heatr_leads
  WHERE status IS NOT NULL
    AND status NOT IN ('discovered','enriched','scored','qualified','queued',
                       'contacted','replied','bounced','no_response',
                       'unsubscribed','disqualified','forgotten')
  GROUP BY status
UNION ALL
SELECT 'leads.email_status', email_status, count(*) FROM heatr_leads
  WHERE email_status IS NOT NULL
    AND email_status NOT IN ('valid','risky','catchall_risky','invalid',
                             'not_checked','not_found','verified','catch_all')
  GROUP BY email_status
ORDER BY n DESC;

-- A4. Wees-rijen (zouden een FK breken). Informational — FK's zijn SECTIE E.
SELECT 'website_intelligence' AS tbl, count(*) AS orphans
FROM   heatr_website_intelligence w
LEFT   JOIN heatr_leads l ON l.id = w.lead_id
WHERE  l.id IS NULL
UNION ALL
SELECT 'lead_contacts', count(*)
FROM   heatr_lead_contacts c
LEFT   JOIN heatr_leads l ON l.id = c.lead_id
WHERE  l.id IS NULL;

-- A5. Heeft anon/authenticated nu leesrechten op heatr_leads? (moet weg)
SELECT has_table_privilege('anon', 'public.heatr_leads', 'SELECT')          AS anon_select,
       has_table_privilege('authenticated', 'public.heatr_leads', 'SELECT') AS auth_select;


-- ============================================================================
-- SECTIE B — DB-NIVEAU DEDUP (unique indexen). Voorwaarde: A1 en A2 leeg.
-- Dit maakt de app-niveau read-then-write dedup race-proof.
-- Risico: als er (nog) duplicaten zijn faalt CREATE UNIQUE INDEX — dat is
-- gewenst (fail-closed), NIET stil. Eerst A1/A2 opschonen.
-- ============================================================================

CREATE UNIQUE INDEX IF NOT EXISTS uq_heatr_companies_ws_domain
  ON heatr_companies_raw (workspace_id, lower(domain))
  WHERE domain IS NOT NULL AND domain <> '';

CREATE UNIQUE INDEX IF NOT EXISTS uq_heatr_leads_ws_email
  ON heatr_leads (workspace_id, lower(email))
  WHERE email IS NOT NULL AND email <> '';

-- NB: bewust GEEN unique op heatr_lead_campaign_history(lead_id) — die tabel is
-- per lead×campagne. De webhook is in Patch 7 losgekoppeld van die (foutieve)
-- on_conflict=lead_id, dus deze unique is niet nodig en zou legitieme rijen breken.


-- ============================================================================
-- SECTIE C — RLS + REVOKE (sluit de publieke anon-blootstelling). Laag risico:
-- de backend draait op service_role (bypasst RLS). Deny-by-default voor
-- anon/authenticated; geen policies nodig omdat niets buiten de backend om
-- deze tabellen hoort te lezen.
-- Pas toe op ALLE heatr_-tabellen. (Herhaal per tabel; hier de gevoeligste +
-- een generiek DO-blok voor de rest.)
-- ============================================================================

-- Generiek over alle heatr_-tabellen:
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


-- ============================================================================
-- SECTIE D — CHECK-enums op statusvelden. Voorwaarde: A3 leeg (of opgeruimd).
-- Voorkomt de status-vocabulaire-drift die de cooldown-bug veroorzaakte.
-- Risico: bestaande afwijkende waarden laten de ADD CONSTRAINT falen — eerst A3.
-- NOT VALID + VALIDATE apart zodat de lock kort is en bestaande data eerst
-- gecontroleerd wordt.
-- ============================================================================

ALTER TABLE heatr_leads
  ADD CONSTRAINT chk_heatr_leads_email_status
  CHECK (email_status IS NULL OR email_status IN
    ('valid','risky','catchall_risky','invalid','not_checked','not_found',
     'verified','catch_all')) NOT VALID;
-- Na controle: ALTER TABLE heatr_leads VALIDATE CONSTRAINT chk_heatr_leads_email_status;


-- ============================================================================
-- SECTIE E — FOREIGN KEYS (optioneel, hoogste risico). Voorwaarde: A4 = 0.
-- Wees-rijen laten de ADD CONSTRAINT falen. NOT VALID zodat bestaande data
-- niet meteen gevalideerd wordt; VALIDATE apart na opschoning.
-- ============================================================================

-- ALTER TABLE heatr_website_intelligence
--   ADD CONSTRAINT fk_hwi_lead FOREIGN KEY (lead_id)
--   REFERENCES heatr_leads(id) ON DELETE CASCADE NOT VALID;
-- ALTER TABLE heatr_lead_contacts
--   ADD CONSTRAINT fk_hlc_lead FOREIGN KEY (lead_id)
--   REFERENCES heatr_leads(id) ON DELETE CASCADE NOT VALID;
-- (uitgecommentarieerd — pas aan na A4=0; ON DELETE CASCADE bewust gekozen zodat
--  het verwijderen van een lead zijn afgeleide data meeneemt.)


-- ============================================================================
-- SECTIE F — VERIFY (draai ná de FORWARD).
-- ============================================================================

-- F1. Unique indexen aanwezig?
SELECT indexname FROM pg_indexes
WHERE  schemaname='public' AND indexname IN
  ('uq_heatr_companies_ws_domain','uq_heatr_leads_ws_email');

-- F2. RLS aan op alle heatr_-tabellen? (rls_enabled moet overal true zijn)
SELECT c.relname, c.relrowsecurity
FROM   pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE  n.nspname='public' AND c.relkind='r' AND c.relname LIKE 'heatr\_%'
  AND  c.relrowsecurity = false;   -- verwacht: LEEG

-- F3. Anon/authenticated GEEN toegang meer? (beide false verwacht)
SELECT has_table_privilege('anon','public.heatr_leads','SELECT')          AS anon_select,
       has_table_privilege('authenticated','public.heatr_leads','SELECT') AS auth_select;


-- ============================================================================
-- SECTIE G — ROLLBACK (indien nodig).
-- ============================================================================

-- DROP INDEX IF EXISTS uq_heatr_companies_ws_domain;
-- DROP INDEX IF EXISTS uq_heatr_leads_ws_email;
-- ALTER TABLE heatr_leads DROP CONSTRAINT IF EXISTS chk_heatr_leads_email_status;
-- RLS terugdraaien (LET OP: heropent de anon-blootstelling — alleen als nodig):
-- DO $$ DECLARE t text; BEGIN
--   FOR t IN SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
--            WHERE n.nspname='public' AND relkind='r' AND relname LIKE 'heatr\_%' LOOP
--     EXECUTE format('ALTER TABLE public.%I DISABLE ROW LEVEL SECURITY;', t);
--   END LOOP; END $$;


-- ============================================================================
-- RISICO'S PER CONSTRAINT (samenvatting)
-- ----------------------------------------------------------------------------
-- B unique-indexen : LAAG. Falen (i.p.v. corrumperen) bij bestaande dups → A1/A2
--                    eerst schoon. Partial (WHERE domain/email not null) zodat
--                    website/email-loze leads niet geblokkeerd worden.
-- C RLS + revoke   : LAAG voor de app (service_role bypasst RLS); HOOG in waarde
--                    (sluit publieke PII-lek). Controleer wél dat geen enkele
--                    directe PostgREST-consument op anon/authenticated leunt.
-- D CHECK email_st.: MIDDEL. Bestaande vocab-drift (A3) blokkeert VALIDATE →
--                    eerst normaliseren. NOT VALID houdt de lock kort.
-- E FK's           : HOOG. Wees-rijen (A4) blokkeren. ON DELETE CASCADE — bewust,
--                    maar controleer dat geen proces op wees-rijen rekent.
-- ============================================================================
