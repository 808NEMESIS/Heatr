-- 023_email_status_check_fix.sql — Fase 2 (suppression/GDPR), hotfix-deel.
--
-- PROBLEEM (actief in productie sinds 021 sectie D): de CHECK
-- chk_heatr_leads_email_status staat als NOT VALID — dat slaat alleen de
-- validatie van BESTAANDE rijen over; NIEUWE writes worden WEL afgedwongen.
-- De toegestane set ('valid','risky','catchall_risky','invalid',
-- 'not_checked','not_found','verified','catch_all') mist waarden die de
-- code daadwerkelijk schrijft:
--
--   - 'bounced'       ← bounce-webhook (api/main.py) — write FAALT nu
--   - 'unsubscribed'  ← unsubscribe-webhook — write FAALT nu
--   - 'pending'       ← verifier-tussenstand
--   - 'catchall'      ← verifier (naast 'catch_all' — beide komen voor)
--   - 'role_email'    ← sendability-vocabulaire
--   - 'blocked'       ← sendability-vocabulaire
--
-- Gevolg: elke bounce/unsubscribe via de Warmr-webhook werd op de
-- leads-update afgewezen door de constraint, de except-tak logde en de
-- suppressie belandde NERGENS. Dit repareert de set naar het volledige
-- code-vocabulaire (bron: utils/email_sendability.py + webhook-handlers).
--
-- Draai dit VÓÓR de fase-2 code-deploy (de code gaat 'bounced'/'unsubscribed'
-- juist bewuster schrijven).

-- ── A. Preflight: welke waarden staan er nu écht in? ────────────────────────
SELECT email_status, COUNT(*) FROM heatr_leads
WHERE email_status IS NOT NULL
GROUP BY email_status ORDER BY 2 DESC;

-- ── B. CHECK vervangen door het volledige code-vocabulaire ──────────────────
ALTER TABLE heatr_leads
  DROP CONSTRAINT IF EXISTS chk_heatr_leads_email_status;
ALTER TABLE heatr_leads
  ADD CONSTRAINT chk_heatr_leads_email_status
  CHECK (email_status IS NULL OR email_status IN (
    'valid', 'verified',                      -- sendable
    'risky', 'catchall', 'catch_all', 'catchall_risky', 'role_email',
    'pending', 'not_checked',                 -- tussenstand
    'invalid', 'not_found',                   -- niet-sendable
    'bounced', 'unsubscribed', 'blocked'      -- suppressie
  )) NOT VALID;

-- ── C. Valideren zodra A geen onbekende waarden meer toont ──────────────────
-- ALTER TABLE heatr_leads VALIDATE CONSTRAINT chk_heatr_leads_email_status;

-- ── D. Verificatie: mag 'bounced' er nu in? (verwacht: geen fout) ───────────
-- Test in een transactie die je terugdraait:
-- BEGIN;
--   UPDATE heatr_leads SET email_status='bounced'
--   WHERE id = (SELECT id FROM heatr_leads LIMIT 1);
-- ROLLBACK;
