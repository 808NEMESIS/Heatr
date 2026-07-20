-- 039_lead_coordinates.sql — lat/lng op leads (outreach spec 4, radius-selector).
--
-- Nul geocoding-API: de coördinaten zitten al in leads.google_maps_url
-- (!3d<lat>!4d<lng> = echte plek, 100% dekking geverifieerd 2026-07-20).
-- scripts/backfill_coordinates.py vult deze kolommen uit de bestaande URL;
-- toekomstige scrapes vullen ze direct (google_maps_scraper).
--
-- Nullable double precision. Raakt heatr_website_intelligence NIET — de lopende
-- website-backfill schrijft niet naar leads.lat/lng, dus deze kolom-toevoeging
-- (metadata-only, geen table-rewrite) is veilig tijdens de run. Idempotent.

ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS lat double precision;
ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS lng double precision;

-- Verificatie
SELECT column_name FROM information_schema.columns
  WHERE table_name = 'heatr_leads' AND column_name IN ('lat', 'lng');
