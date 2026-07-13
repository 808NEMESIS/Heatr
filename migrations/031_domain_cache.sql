-- 031_domain_cache.sql — bugfix: catch-all-cache schreef naar niet-bestaande tabel.
--
-- email_verifier.py schrijft naar 'domain_cache' (catch-all-detectie, 7d TTL),
-- maar die tabel bestaat niet én stond niet in _HEATR_TABLES → elke write
-- faalde (PGRST205), catch-all werd elke keer opnieuw gedetecteerd (traag).
-- Zelfde klasse als de outbound_log-prefixbug.
--
-- Code-fix: 'domain_cache' toegevoegd aan _HEATR_TABLES (→ heatr_domain_cache).
-- Draai deze migratie zodat de cache echt werkt. Idempotent.

CREATE TABLE IF NOT EXISTS heatr_domain_cache (
    domain      TEXT PRIMARY KEY,
    is_catchall BOOLEAN NOT NULL,
    has_mx      BOOLEAN,
    checked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_domain_cache_expires
  ON heatr_domain_cache (expires_at);

COMMENT ON TABLE heatr_domain_cache IS
  'Catch-all-detectie per domein met TTL (email_verifier). Vervangt de dode niet-geprefixte domain_cache-write.';

SELECT 'heatr_domain_cache' AS created;
