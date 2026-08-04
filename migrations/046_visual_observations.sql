-- Migratie 046 — visual_observations op website_intelligence (verse Vision-laag)
--
-- analyze_visual (Claude Sonnet Vision) levert al top_improvements/top_strengths, maar
-- die tekst werd nooit opgeslagen — alleen visual_score. Zonder de tekst kan de pitch
-- alleen een percentiel geven ("oogt verouderder dan X%"); mét de tekst kan 'ie een
-- CONCRETE observatie noemen ("gedateerde stockfoto's, weinig witruimte").
--
-- Vorm: {"improvements": [...], "strengths": [...], "overall": N, "at": "iso"}.
-- Wordt gevuld door (a) de website-worker (analyze_website, in één analyse-pass)
-- en (b) scripts/run_vision_refresh.py --apply (backfill; ~€0,02/lead). Beide
-- fail-soft: zolang deze kolom ontbreekt schrijven ze alleen visual_score.
--
-- Draai dit in de Supabase SQL-editor (MCP heeft geen prod-toegang). Idempotent, geen mail.

ALTER TABLE heatr_website_intelligence
    ADD COLUMN IF NOT EXISTS visual_observations jsonb;
