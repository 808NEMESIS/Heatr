-- 040_hook_signals.sql — haakje-ladder-signaal per lead (Fase 2a).
--
-- Slaat het LIVE-gevuurde website-signaal op dat de mail-1 haakje bepaalt
-- (website_intelligence/hook_detector.py → config/hook_templates.build_haakje).
-- Zie docs/plan_haakje_machine_fase2.md (pakket A/F) + docs/haakje_mapping_mail1.md.
--
-- Aanleiding: de opgeslagen conversion/technical-signalen bleken niet te
-- vertrouwen voor haakje-selectie (Tajmeel=Salonkee / My Unique=Treatwell HADDEN
-- boeking, maar het "alleen telefoon"-haakje vuurde toch — live-verificatie
-- 2026-07-22). Dit veld is een APARTE, Guardrail-1-proof detectie-uitkomst.
--
-- fired_signal: 1..6 (ladder), NULL = geen signaal → lead hoort niet in de
--   conceptsite-flow. 1=geen online boeking, 2=CTA onder de vouw, 3=trage TTI,
--   4=tap-targets, 5=verouderde footer, 6=vision-element.
-- hook_variant: 'A'|'B' (deterministisch geseed als NULL → render kiest).
-- hook_evidence: bewijs-array uit de detector (platform/tel/CTA-positie/...).
-- hook_jaar / hook_vision_element: veld-invulling voor signaal 5 resp. 6.
--
-- Idempotent (IF NOT EXISTS). Niet automatisch uitgevoerd — draai in de Supabase
-- SQL-editor (MCP heeft geen prod-toegang).

ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS fired_signal        smallint;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS hook_signal_name    text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS hook_variant        text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS hook_confidence     text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS hook_evidence       jsonb;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS hook_jaar           smallint;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS hook_vision_element text;
ALTER TABLE heatr_website_intelligence ADD COLUMN IF NOT EXISTS hook_detected_at    timestamptz;

-- Snel de vurende leads vinden bij Founding-Five-selectie.
CREATE INDEX IF NOT EXISTS idx_website_intel_fired_signal
  ON heatr_website_intelligence(workspace_id, fired_signal)
  WHERE fired_signal IS NOT NULL;
