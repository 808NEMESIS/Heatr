-- Migratie 046 — visual_observations op website_intelligence (verse Vision-laag)
--
-- analyze_visual (Claude Sonnet Vision) levert al top_improvements/top_strengths, maar
-- die tekst werd nooit opgeslagen — alleen visual_score. Zonder de tekst kan de pitch
-- alleen een percentiel geven ("oogt verouderder dan X%"); mét de tekst kan 'ie een
-- CONCRETE observatie noemen ("gedateerde stockfoto's, weinig witruimte").
--
-- Vorm: {"improvements": [...], "strengths": [...], "overall": N, "at": "iso"}.
-- scripts/run_vision_refresh.py --apply vult 'm (gedoseerd; kost ~€0,02/lead).
--
-- Draai dit in de Supabase SQL-editor (MCP heeft geen prod-toegang). Idempotent, geen mail.

ALTER TABLE heatr_website_intelligence
    ADD COLUMN IF NOT EXISTS visual_observations jsonb;
