-- 043_kvk_legal_form.sql — rechtsvorm-veld op leads (AVG-02, zonder KvK-API).
--
-- Sami 2026-07-25: KvK-API valt af (kost geld). Rechtsvorm komt nu uit (1) handmatige
-- invoer via het openbare KvK-zoekregister, (2) gratis B.V.-afleiding uit naam/footer.
-- Beide schrijven naar deze kolom; classify_legal_form (utils/legal_form.py) leest 'm.
-- Stond in supabase_schema.sql maar was nooit op prod toegepast.
--
-- Waarden: vrije tekst, bv. 'BV' / 'Besloten Vennootschap' / 'Eenmanszaak' / 'ZZP'.
-- De gate mapt BV/NV/stichting → rechtspersoon (door), eenmanszaak/zzp/vof →
-- natuurlijk persoon (geblokkeerd), leeg/onbekend → onbepaald (geblokkeerd).
--
-- Tabel = heatr_leads (heatr_-prefix). Idempotent. Draai in de Supabase SQL-editor.

ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS kvk_legal_form text;
