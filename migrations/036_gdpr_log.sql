-- 036_gdpr_log.sql — de GDPR-audittrail-tabel die nooit is toegepast.
--
-- heatr_gdpr_log staat in het basis-supabase_schema.sql maar bestaat niet in
-- prod (gevonden in audit_compliance_verwerker.md): elke forget/export-actie
-- probeerde fail-soft te loggen naar een niet-bestaande tabel — er was dus GEEN
-- bewijs dat aan een verwijderverzoek is voldaan. Dat bewijs is juridisch nodig
-- (Art. 5(2) accountability): je moet kunnen aantonen dát en wanneer je aan een
-- verzoek voldeed. lead_email bevat bewust het originele adres als dat bewijs.
--
-- (verify_migrations miste dit omdat het alleen migrations/*.sql parseerde;
-- die check dekt nu ook het basis-schema.)
--
-- Idempotent. Niet automatisch uitgevoerd — draai in de Supabase SQL-editor.

CREATE TABLE IF NOT EXISTS heatr_gdpr_log (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id    text        NOT NULL,
    action          text        NOT NULL CHECK (action IN ('forget', 'export', 'register_view')),
    lead_id         uuid,
    lead_email      text,
    performed_by    text        NOT NULL DEFAULT 'user',
    completed_at    timestamptz NOT NULL DEFAULT now(),
    metadata        jsonb
);

CREATE INDEX IF NOT EXISTS heatr_gdpr_log_workspace_idx
  ON heatr_gdpr_log(workspace_id, completed_at DESC);
