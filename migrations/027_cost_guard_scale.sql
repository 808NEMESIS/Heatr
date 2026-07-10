-- 027_cost_guard_scale.sql — Fase 4 (PR 15/16/17): schaalbare kostenbewaking.
--
-- PROBLEEM (audit v2 P1-2): guarded_call haalde bij ELKE AI-call alle
-- api_cost_log-rijen van de maand op en somde in Python — O(n) per call,
-- O(n²) over een batch — en het range-filter op created_at was
-- ongeïndexeerd (de enige index stond op de kolom `date`). Bij 100k leads
-- × ~10 calls = 1M+ rijen/maand die elke enrichment-stap opnieuw scant.
--
-- Dit levert: (A) een samengestelde index op de échte query-kolommen,
-- (B) één STABLE aggregatie-functie (workspace-som, of platform-som bij
-- NULL — de basis voor het globale platformbudget uit PR 16), en
-- (C) de spent_eur-kolom op enrichment_jobs voor resumable enrichment
-- (PR 17: het per-lead-plafond overleeft nu een worker-restart).
--
-- Draai VÓÓR de fase-4 code-deploy: de code roept heatr_cost_sum aan en
-- is fail-closed — zonder deze functie blokkeren alle AI-calls (veilig,
-- maar disruptief).

-- ── A. Index op de query-kolommen ───────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_heatr_api_cost_log_ws_created
  ON heatr_api_cost_log (workspace_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_heatr_api_cost_log_created
  ON heatr_api_cost_log (created_at DESC);

-- ── B. Aggregatie-functie ───────────────────────────────────────────────────
-- p_workspace = NULL → platformbrede som (globaal budget, PR 16).
CREATE OR REPLACE FUNCTION heatr_cost_sum(p_workspace text, p_since timestamptz)
RETURNS numeric
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(SUM(cost_eur), 0)::numeric
  FROM heatr_api_cost_log
  WHERE created_at >= p_since
    AND (p_workspace IS NULL OR workspace_id = p_workspace);
$$;

-- ── C. Resumable enrichment (PR 17) ─────────────────────────────────────────
-- spent_eur: het per-lead/job-plafond was een in-memory accumulator die op
-- restart naar 0 reset — een crashende job kreeg het budget 3× vers.
ALTER TABLE heatr_enrichment_jobs
  ADD COLUMN IF NOT EXISTS spent_eur NUMERIC NOT NULL DEFAULT 0;

-- ── Verificatie ─────────────────────────────────────────────────────────────
SELECT heatr_cost_sum('aerys', now() - interval '30 days') AS aerys_30d,
       heatr_cost_sum(NULL,    now() - interval '30 days') AS platform_30d;
