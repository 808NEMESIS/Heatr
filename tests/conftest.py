"""tests/conftest.py — gedeelde test-fixtures.

Hermetische receptie-env: enkele integratie-testmodules roepen load_dotenv() aan bij
import, waardoor .env-waarden in de hele pytest-sessie belanden. De receptie-render-
gate leest env-vars (RECEPTIE_UNSUBSCRIBE_VIA_WARMR, privacy, template), dus zonder
isolatie zou een .env-waarde het gate-gedrag in unit-tests stilletjes veranderen.

Deze autouse-fixture wist die drie vars vóór elke test. Tests die een specifieke modus
willen, zetten die expliciet via monkeypatch (dat overschrijft de gewiste default).
Alleen RECEPTIE_*-vars worden aangeraakt — niet-receptie-tests blijven ongemoeid.
"""
import pytest


@pytest.fixture(autouse=True)
def _hermetic_receptie_env(monkeypatch):
    for key in (
        "RECEPTIE_UNSUBSCRIBE_VIA_WARMR",
        "RECEPTIE_PRIVACY_NOTICE",
        "RECEPTIE_UNSUBSCRIBE_TEMPLATE",
    ):
        monkeypatch.delenv(key, raising=False)
    yield
