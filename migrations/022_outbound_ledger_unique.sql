-- 022_outbound_ledger_unique.sql — Werkpakket A: Outbound Safety Foundation.
--
-- Context: de dispatcher schreef tot nu naar de ONGEPREFIXTE tabelnaam
-- "outbound_log" (die niet bestaat) — elke ledger-write faalde stil en
-- idempotency was fail-open (audit v2, P0-1). De code-fix voegt
-- "outbound_log" toe aan _HEATR_TABLES; deze migratie levert de
-- database-garantie die de code-fix pas veilig maakt:
--
--   1. een PARTIAL UNIQUE index op (workspace_id, idempotency_key) voor
--      actieve records (in_flight/completed) — een gewone index is géén
--      idempotency-garantie;
--   2. een CHECK op de toegestane statussen (incl. het nieuwe
--      reserveringsmodel: in_flight → completed | failed_retryable |
--      failed_terminal);
--   3. een bijgewerkte tabel-COMMENT: reserveringsrijen worden in-place
--      gefinaliseerd; alle overige rijen blijven append-only.
--
-- Draai dit VÓÓR de code-deploy. De code-fix mag niet live zonder deze
-- index (naam repareren zonder UNIQUE = zichtbaar werkend maar nog
-- steeds racegevoelig).

-- ── A. Preflight: bestaande duplicaten? ─────────────────────────────────────
-- Verwacht: 0 rijen. De tabel is door de prefix-bug nooit succesvol
-- beschreven; een niet-lege uitkomst betekent dat er via een ander pad
-- geschreven is — dan NIET doorgaan met sectie B maar eerst opschonen.
SELECT workspace_id, idempotency_key, COUNT(*) AS n
FROM heatr_outbound_log
WHERE status IN ('in_flight', 'completed')
GROUP BY workspace_id, idempotency_key
HAVING COUNT(*) > 1;

-- ── B. Partial UNIQUE op actieve records ────────────────────────────────────
-- Alleen in_flight/completed tellen mee: een failed_retryable-rij valt uit
-- het predicaat zodat een bewuste retry een nieuwe reservering kan inserten.
CREATE UNIQUE INDEX IF NOT EXISTS uq_outbound_log_active_key
  ON heatr_outbound_log (workspace_id, idempotency_key)
  WHERE status IN ('in_flight', 'completed');

-- ── C. Status-CHECK (tabel is leeg → direct valide) ─────────────────────────
-- 'failed' blijft toegestaan als legacy-waarde (pre-022 code schreef die);
-- nieuwe code schrijft failed_retryable/failed_terminal.
ALTER TABLE heatr_outbound_log
  DROP CONSTRAINT IF EXISTS heatr_outbound_log_status_check;
ALTER TABLE heatr_outbound_log
  ADD CONSTRAINT heatr_outbound_log_status_check CHECK (status IN (
    'in_flight', 'completed', 'failed', 'failed_retryable', 'failed_terminal',
    'blocked_compliance', 'blocked_killswitch', 'skipped_duplicate'
  ));

-- ── D. Semantiek-comment bijwerken ──────────────────────────────────────────
COMMENT ON TABLE heatr_outbound_log IS
  'Outbound side-effect-ledger + idempotency-reservering. Een dispatch INSERT eerst een in_flight-rij (eigenaarstoewijzing via uq_outbound_log_active_key); die ene rij wordt in-place gefinaliseerd naar completed/failed_retryable/failed_terminal. Alle overige rijen (blocked_*, skipped_duplicate) zijn append-only en worden nooit gemuteerd of verwijderd.';

-- ── E. Verificatie (draai NA A-D) ───────────────────────────────────────────
-- Verwacht: 1 rij met indexdef die het WHERE-predicaat bevat.
SELECT indexname, indexdef FROM pg_indexes
WHERE schemaname = 'public' AND indexname = 'uq_outbound_log_active_key';
