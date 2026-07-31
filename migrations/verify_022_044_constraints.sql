-- verify_022_044_constraints.sql — ALLEEN-LEZEN. Draai in de Supabase SQL-editor
-- (project zomdrygdcaenjnrrpcpw). Wijzigt niets.
--
-- WAAROM: scripts/verify_migrations.py checkt alleen of KOLOMMEN/TABELLEN bestaan,
-- niet of de partial-UNIQUE indexen en CHECK-constraints van 022/023/024/028/044
-- er echt staan. Les uit migratie 033: een selectieve paste kan één CREATE INDEX
-- stil overslaan → de code slikt daarna dubbele in_flight-reserveringen /
-- dubbele compliance-vlaggen ZONDER foutmelding (idempotency-/dedup-lek).
--
-- VERWACHT: elke regel status = 'OK'. status = 'ONTBREEKT' → draai die specifieke
-- CREATE-regel uit de betreffende migratie alsnog.

-- ── 1. Partial-UNIQUE + gewone indexen (pg_indexes) ─────────────────────────
WITH expected_idx(mig, indexname) AS (
    VALUES
        ('022', 'uq_outbound_log_active_key'),
        ('024', 'uq_suppressions_active_email'),
        ('024', 'idx_suppressions_domain'),
        ('024', 'idx_suppressions_recent'),
        ('044', 'idx_compliance_flags_open'),
        ('044', 'uq_compliance_flags_open'),
        ('045', 'idx_wi_review_status')
)
SELECT e.mig,
       e.indexname,
       CASE WHEN i.indexname IS NULL THEN 'ONTBREEKT' ELSE 'OK' END AS status
FROM   expected_idx e
LEFT JOIN pg_indexes i
       ON i.schemaname = 'public'
      AND i.indexname  = e.indexname
ORDER  BY e.mig, e.indexname;

-- ── 2. CHECK-constraints (pg_constraint) ────────────────────────────────────
WITH expected_con(mig, conname) AS (
    VALUES
        ('022', 'heatr_outbound_log_status_check'),
        ('023', 'chk_heatr_leads_email_status')
)
SELECT e.mig,
       e.conname,
       CASE WHEN c.conname IS NULL THEN 'ONTBREEKT' ELSE 'OK' END AS status
FROM   expected_con e
LEFT JOIN pg_constraint c
       ON c.conname      = e.conname
      AND c.connamespace = 'public'::regnamespace
ORDER  BY e.mig, e.conname;

-- ── 3. 028 — steps_completed genormaliseerd naar jsonb ──────────────────────
SELECT 'heatr_enrichment_jobs.steps_completed' AS kolom,
       COALESCE(data_type, '(kolom ontbreekt)')  AS data_type,
       CASE WHEN data_type = 'jsonb' THEN 'OK'
            ELSE 'CONTROLEER (verwacht jsonb)' END AS status
FROM   information_schema.columns
WHERE  table_schema = 'public'
  AND  table_name   = 'heatr_enrichment_jobs'
  AND  column_name  = 'steps_completed';
