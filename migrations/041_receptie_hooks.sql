-- 041_receptie_hooks.sql — receptie-haakje-ladder Q4/Q7/Q2/P1 per lead (Fase 2d).
--
-- De receptie-ladder (Sami 2026-07-24) vervangt voor deze campagne de conceptsite/
-- Founding-Five-funnel (040's fired_signal 1..6). Aparte kolommen, zodat beide
-- ladders naast elkaar kunnen bestaan; render_faseA_marker kiest op brug-type.
--
-- Bron van waarheid: website_intelligence/hook_detector.detect_receptie — één
-- mobiele render x2, per as fail-closed 2-run-gecombineerd (combine_stable), dan
-- decide_receptie_hook. Waarden zijn 2-run-stabiel en fail-closed geverifieerd
-- (Q7 10/10, P1 12/12, Q4 13/14 blind bevestigd; routing 32/100 mailbaar).
--
-- receptie_hook_code   : 'Q4'|'Q7'|'Q2'|'P1' (mail-1 haak) of NULL = niet mailbaar.
-- receptie_hook_ladder : jsonb, alle gevuurde haken in ladder-volgorde.
-- receptie_second_hook : 'Q4'|'Q7'|'Q2'|'P1' of NULL — mail-2 haak (ander thema);
--                        NULL → geen mail 2, direct mail 3.
-- receptie_q4/q7/q2/p1 : 'hit'|'geen'|'onbepaald' per as (transparantie/debug).
-- receptie_q4_gated    : Q4 vuurde maar viel door bij gebrek aan formulier.
-- receptie_form_present: formulier op de pagina (Q4/Q2-copy claimt 'een formulier').
-- receptie_*_variant   : 'A'|'B' (NULL → render kiest deterministisch).
-- receptie_evidence    : jsonb bewijs per as.
--
-- Idempotent (IF NOT EXISTS). Niet automatisch uitgevoerd — draai in de Supabase
-- SQL-editor (MCP heeft geen prod-toegang).

ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_hook_code     text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_hook_ladder   jsonb;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_second_hook   text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_q4            text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_q7            text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_q2            text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_p1            text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_q4_gated      boolean;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_form_present  boolean;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_hook_variant   text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_second_variant text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_evidence      jsonb;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS receptie_detected_at   timestamptz;

-- Snel de mailbare receptie-leads vinden bij campagne-selectie.
CREATE INDEX IF NOT EXISTS idx_website_intel_receptie_hook
  ON heatr_website_intelligence(workspace_id, receptie_hook_code)
  WHERE receptie_hook_code IS NOT NULL;
