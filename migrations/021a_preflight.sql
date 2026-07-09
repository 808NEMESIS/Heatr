-- 021a_preflight.sql — ALLEEN-LEZEN. Draai dit EERST in de SQL-editor van
-- project zomdrygdcaenjnrrpcpw. Wijzigt niets. De uitkomst bepaalt wat je
-- veilig kunt toepassen met 021b.
--
-- Verwacht/gewenst: A1, A2, A3, A4 leeg. A5 = false (anon heeft geen toegang).

-- A1. Dubbele domeinen per workspace (blokkeert de unique index).
SELECT workspace_id, lower(domain) AS domain, count(*) AS n
FROM   heatr_companies_raw
WHERE  domain IS NOT NULL AND domain <> ''
GROUP  BY workspace_id, lower(domain) HAVING count(*) > 1 ORDER BY n DESC;

-- A2. Dubbele e-mails per workspace (blokkeert de unique index).
SELECT workspace_id, lower(email) AS email, count(*) AS n
FROM   heatr_leads
WHERE  email IS NOT NULL AND email <> ''
GROUP  BY workspace_id, lower(email) HAVING count(*) > 1 ORDER BY n DESC;

-- A3. email_status buiten de beoogde enum (blokkeert de CHECK-validatie).
SELECT email_status, count(*) n FROM heatr_leads
WHERE email_status IS NOT NULL
  AND email_status NOT IN ('valid','risky','catchall_risky','invalid',
                           'not_checked','not_found','verified','catch_all')
GROUP BY email_status ORDER BY n DESC;

-- A4. Wees-rijen (zouden FK's breken).
SELECT 'website_intelligence' tbl, count(*) orphans FROM heatr_website_intelligence w
  LEFT JOIN heatr_leads l ON l.id=w.lead_id WHERE l.id IS NULL
UNION ALL
SELECT 'lead_contacts', count(*) FROM heatr_lead_contacts c
  LEFT JOIN heatr_leads l ON l.id=c.lead_id WHERE l.id IS NULL;

-- A5. Kan anon/authenticated nu heatr_leads lezen? (moet false worden)
SELECT has_table_privilege('anon','public.heatr_leads','SELECT')          AS anon_select,
       has_table_privilege('authenticated','public.heatr_leads','SELECT') AS auth_select;
