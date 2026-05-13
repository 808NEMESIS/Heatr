"""
tests/test_warmr_test_mode.py — _build_lead_payload BCC + [TEST] prefix logic.

Verifieert dat:
  - is_test_lead=true + HEATR_TEST_BCC_EMAIL gezet → BCC + subject_prefix in payload
  - is_test_lead=true zonder env-var → geen BCC (graceful — kun je de feature uit hebben)
  - is_test_lead=false → geen BCC, geen prefix (default behavior)
  - is_test_lead surface in payload zelf (top-level + nestable in custom_fields)
"""
from unittest.mock import MagicMock

from integrations.warmr_client import WarmrClient


def _build(lead: dict) -> dict:
    """Direct _build_lead_payload aanroepen zonder live WarmrClient init."""
    client = WarmrClient.__new__(WarmrClient)
    client._sb = None
    client.workspace_id = "aerys"
    return client._build_lead_payload(lead, campaign_id="test-campaign", preferred_inbox_id=None)


def test_test_mode_off_no_bcc_in_payload(monkeypatch):
    monkeypatch.setenv("HEATR_TEST_BCC_EMAIL", "sami@aerys.nl")
    lead = {
        "id": "lead-1",
        "email": "real@prospect.nl",
        "contact_first_name": "Mark",
        "is_test_lead": False,
    }
    payload = _build(lead)
    assert "bcc" not in payload
    assert "subject_prefix" not in payload
    assert payload["is_test_lead"] is False


def test_test_mode_on_with_env_adds_bcc_and_prefix(monkeypatch):
    monkeypatch.setenv("HEATR_TEST_BCC_EMAIL", "sami@aerys.nl")
    lead = {
        "id": "lead-1",
        "email": "real@prospect.nl",
        "contact_first_name": "Mark",
        "is_test_lead": True,
    }
    payload = _build(lead)
    assert payload["bcc"] == "sami@aerys.nl"
    assert payload["subject_prefix"] == "[TEST] "
    assert payload["is_test_lead"] is True


def test_test_mode_on_without_env_skips_bcc_gracefully(monkeypatch):
    """Zonder HEATR_TEST_BCC_EMAIL: lead is wél is_test_lead, maar geen BCC.

    Dit voorkomt dat een ongeplande env-config zorgt dat BCC=None of
    een crash triggert. Test-mode flag blijft surfacen voor logging.
    """
    monkeypatch.delenv("HEATR_TEST_BCC_EMAIL", raising=False)
    lead = {
        "id": "lead-1",
        "email": "real@prospect.nl",
        "contact_first_name": "Mark",
        "is_test_lead": True,
    }
    payload = _build(lead)
    assert "bcc" not in payload
    assert "subject_prefix" not in payload
    assert payload["is_test_lead"] is True


def test_test_mode_off_ignores_env_var(monkeypatch):
    """is_test_lead=False mag NIET BCC krijgen ook al staat env-var."""
    monkeypatch.setenv("HEATR_TEST_BCC_EMAIL", "sami@aerys.nl")
    lead = {
        "id": "lead-1",
        "email": "real@prospect.nl",
        "contact_first_name": "Mark",
        "is_test_lead": False,
    }
    payload = _build(lead)
    assert "bcc" not in payload


def test_test_mode_default_false_when_field_missing(monkeypatch):
    """Lead zonder is_test_lead-veld krijgt default False-gedrag."""
    monkeypatch.setenv("HEATR_TEST_BCC_EMAIL", "sami@aerys.nl")
    lead = {
        "id": "lead-1",
        "email": "real@prospect.nl",
        "contact_first_name": "Mark",
        # Geen is_test_lead-key
    }
    payload = _build(lead)
    assert payload["is_test_lead"] is False
    assert "bcc" not in payload


def test_test_mode_strips_whitespace_in_env(monkeypatch):
    """Env-var met trailing whitespace mag niet leiden tot BCC=' sami@aerys.nl '."""
    monkeypatch.setenv("HEATR_TEST_BCC_EMAIL", "  sami@aerys.nl  ")
    lead = {"id": "lead-1", "email": "real@prospect.nl", "is_test_lead": True}
    payload = _build(lead)
    assert payload["bcc"] == "sami@aerys.nl"


def test_test_mode_empty_env_treated_as_unset(monkeypatch):
    """HEATR_TEST_BCC_EMAIL=' ' (alleen whitespace) → behandel als unset."""
    monkeypatch.setenv("HEATR_TEST_BCC_EMAIL", "   ")
    lead = {"id": "lead-1", "email": "real@prospect.nl", "is_test_lead": True}
    payload = _build(lead)
    assert "bcc" not in payload
