-- 038_founding_five_slots.sql — de Founding-Five plekken-teller (outreach spec 3).
--
-- Eén rij = één VERGEVEN plek (drempel = getekende deal, niet een verzonden
-- concept). Per niche (= lead.sector) geteld; Founding Five geldt per niche.
-- De sequence-engine leest bij elke verzending:
--     vrije_plekken = 5 - COUNT(* WHERE workspace_id=… AND niche=…)
-- zodat de "vijf plekken"-claim in mail 3 nooit meer belooft dan waar is.
--
-- Raakt heatr_website_intelligence NIET (backfill ongemoeid). Idempotent.
-- Draai in de Supabase SQL-editor.

CREATE TABLE IF NOT EXISTS heatr_founding_five_slots (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id  text        NOT NULL,
    niche         text        NOT NULL,                 -- = lead.sector
    lead_id       uuid,                                 -- welke lead de plek nam (nullable)
    deal_ref      text,                                 -- vrije referentie naar de getekende deal
    signed_at     timestamptz NOT NULL DEFAULT now(),   -- drempel: getekende deal
    notes         text,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS heatr_founding_five_slots_niche_idx
  ON heatr_founding_five_slots (workspace_id, niche);

-- Verificatie
SELECT tablename FROM pg_tables WHERE schemaname='public'
  AND tablename = 'heatr_founding_five_slots';
