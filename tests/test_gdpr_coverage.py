"""
tests/test_gdpr_coverage.py — de GDPR-gegevenskaart kan niet meer stil
achterlopen op het schema.

De fout die dit voorkomt: forget_lead was gebouwd voor het schema van vóór
migratie 029-035 — transcripten, lead_contacts, snapshots e.d. bleven staan bij
een verwijderverzoek. Nu is GDPR_DATA_MAP de ene bron voor erase én export, en
faalt deze suite zodra:
  - een tabel aan config.database._HEATR_TABLES wordt toegevoegd zonder
    GDPR-classificatie;
  - een kritieke leads-PII-kolom uit de redactielijst valt;
  - een map-entry een ongeldige vorm heeft.
"""
from config.database import _HEATR_TABLES
from utils.gdpr_manager import GDPR_DATA_MAP, _LEADS_REDACTION, _STORAGE_PATHS_DOMAIN


def test_every_allowlisted_table_is_classified():
    """Nieuwe tabel in _HEATR_TABLES zonder GDPR-classificatie -> rood."""
    unclassified = set(_HEATR_TABLES) - set(GDPR_DATA_MAP)
    assert not unclassified, (
        f"Tabellen zonder GDPR-classificatie: {sorted(unclassified)} — "
        f"voeg ze toe aan GDPR_DATA_MAP (delete/redact/none/special + reden)"
    )


def test_no_stale_map_entries():
    """Map-entries voor tabellen die niet (meer) in de allowlist staan -> rood."""
    stale = set(GDPR_DATA_MAP) - set(_HEATR_TABLES)
    assert not stale, f"GDPR_DATA_MAP bevat onbekende tabellen: {sorted(stale)}"


def test_map_entries_are_well_formed():
    valid_actions = {"delete", "redact", "none", "special"}
    for table, spec in GDPR_DATA_MAP.items():
        assert spec.get("action") in valid_actions, table
        assert spec.get("reason"), f"{table}: reden verplicht"
        if spec["action"] == "redact":
            assert spec.get("redact_columns"), f"{table}: redact zonder kolommen"
        if spec["action"] in ("delete", "redact"):
            assert spec.get("by", "lead_id") in ("lead_id", "domain"), table


def test_privacy_heavy_tables_are_erased():
    """De zwaarste artefacten moeten delete zijn, niet redact/none."""
    assert GDPR_DATA_MAP["call_records"]["action"] == "delete"       # transcripten
    assert GDPR_DATA_MAP["lead_contacts"]["action"] == "delete"      # namen/bio's
    assert GDPR_DATA_MAP["reply_inbox"]["action"] == "delete"        # reply-teksten
    assert GDPR_DATA_MAP["enrichment_data"]["action"] == "delete"    # page_text/emails


def test_append_only_tension_resolved_as_redaction():
    """email_verifications: verwijderrecht wint, uitkomst+timestamp blijven."""
    spec = GDPR_DATA_MAP["email_verifications"]
    assert spec["action"] == "redact"
    assert "email" in spec["redact_columns"]


def test_leads_redaction_covers_the_audit_gaps():
    """De kolommen die audit_compliance_verwerker.md als gemist aanwees."""
    required = {
        "contact_name", "contact_title", "contact_linkedin_url",
        "contact_why_chosen", "review_best_quote", "personalization_hooks",
        "personalization_observations", "company_positioning",
        "kvk_bestuurder_name", "checkup_data", "crm_notes",
        "personalized_opener", "phone",
    }
    missing = required - set(_LEADS_REDACTION)
    assert not missing, f"leads-redactie mist: {sorted(missing)}"


def test_storage_covers_old_and_new_paths():
    joined = " ".join(_STORAGE_PATHS_DOMAIN)
    assert "{domain}.png" in joined                      # oude pad
    assert "captures/{domain}/desktop.webp" in joined    # nieuwe paden
    assert "captures/{domain}/mobile.webp" in joined


def test_erase_and_export_run_from_same_map():
    """Elke delete/redact-tabel met persoonsgegevens is ook exporteerbaar
    (inzage spiegelt verwijdering)."""
    for table, spec in GDPR_DATA_MAP.items():
        if spec["action"] in ("delete", "redact") and spec.get("by", "lead_id") == "lead_id":
            assert spec.get("export") is True, (
                f"{table}: wél gewist maar niet in de export — inzagerecht incompleet"
            )
