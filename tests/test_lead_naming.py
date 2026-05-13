"""
tests/test_lead_naming.py — safe_first_name + display_first_name.
"""
from utils.lead_naming import display_first_name, safe_first_name


def test_safe_first_name_returns_real_name():
    lead = {"contact_first_name": "Mark", "email": "mark.jansen@kliniek.nl"}
    assert safe_first_name(lead) == "Mark"


def test_safe_first_name_strips_email_local_part_match():
    """Live bug: gmail-inference produceerde first_name='Ceciledebooij'."""
    lead = {"contact_first_name": "Ceciledebooij", "email": "ceciledebooij@gmail.com"}
    assert safe_first_name(lead) == ""


def test_safe_first_name_case_insensitive_match():
    lead = {"contact_first_name": "ceciledebooij", "email": "Ceciledebooij@gmail.com"}
    assert safe_first_name(lead) == ""


def test_safe_first_name_strips_long_unspaced():
    """Geen spatie + >12 chars = waarschijnlijk samengevoegde naam."""
    lead = {"contact_first_name": "JanenAnnaPraktijk", "email": "info@kliniek.nl"}
    assert safe_first_name(lead) == ""


def test_safe_first_name_keeps_short_name_with_consumer_email():
    """Mark is een echte naam, ook al is email gmail."""
    lead = {"contact_first_name": "Mark", "email": "markvandenbos@gmail.com"}
    # Mark is geen prefix-match dus mag blijven
    assert safe_first_name(lead) == "Mark"


def test_safe_first_name_empty_returns_empty():
    assert safe_first_name({}) == ""
    assert safe_first_name({"contact_first_name": None}) == ""
    assert safe_first_name({"contact_first_name": "  "}) == ""


def test_display_first_name_uses_fallback():
    assert display_first_name({}, fallback="daar") == "daar"
    assert display_first_name({"contact_first_name": "Mark", "email": "x@y.nl"}, fallback="daar") == "Mark"


def test_display_first_name_empty_string_fallback():
    """Als caller geen fallback wil, kan ze "" als fallback geven."""
    assert display_first_name({}, fallback="") == ""


def test_inject_variables_uses_safe_first_name():
    """End-to-end: render_step / inject_variables moet safe_first_name gebruiken."""
    from campaigns.sequence_engine import inject_variables
    lead = {"contact_first_name": "Ceciledebooij", "email": "ceciledebooij@gmail.com"}
    text = "Hoi {{first_name}}, leuk je te zien."
    out = inject_variables(text, lead)
    # Niet 'Hoi Ceciledebooij' — fallback 'daar' wordt gebruikt
    assert "Ceciledebooij" not in out
    assert "Hoi daar" in out
