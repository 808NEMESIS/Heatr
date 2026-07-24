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


# ── Junk-'namen' afvangen (sample-bevinding 2026-07-24) ──────────────────────
def test_safe_first_name_rejects_generic_words():
    for junk in ("Afspraak", "Contact", "Info", "Receptie", "Team", "Boeking"):
        assert safe_first_name({"contact_first_name": junk}) == "", junk


def test_safe_first_name_rejects_business_fragment():
    # domein-/merkfragment als naam ('Glowclinicnl' = glowclinic + nl).
    assert safe_first_name({"contact_first_name": "Glowclinicnl"}) == ""
    assert safe_first_name({"contact_first_name": "Skinstudio"}) == ""


def test_safe_first_name_rejects_initial_and_digits():
    assert safe_first_name({"contact_first_name": "A."}) == ""
    assert safe_first_name({"contact_first_name": "J"}) == ""
    assert safe_first_name({"contact_first_name": "Team2"}) == ""


def test_safe_first_name_keeps_eponymous_owner_names():
    # echte voornamen die ook in de klinieknaam zitten mogen NIET sneuvelen.
    assert safe_first_name({"contact_first_name": "Joost", "company_name": "Joost Kroon"}) == "Joost"
    assert safe_first_name({"contact_first_name": "Frodo", "company_name": "Kliniek Dokter Frodo"}) == "Frodo"
    assert safe_first_name({"contact_first_name": "Dunya"}) == "Dunya"
    assert safe_first_name({"contact_first_name": "Bo"}) == "Bo"  # korte echte naam


# ── SEO-title-vervuiling in company_name (2026-07-24) ────────────────────────
from utils.lead_naming import clean_company_name  # noqa: E402


def test_clean_company_name_strips_seo_title():
    n, rv = clean_company_name("EMPCLINICS Emphair Haartransplantatie | Esthetische Chirurgie | X")
    assert n == "EMPCLINICS Emphair Haartransplantatie" and rv is False   # geen pipe meer, <=4 woorden
    assert clean_company_name("Beauty | Clinic")[0] == "Beauty"
    assert clean_company_name("Glow Clinic Utrecht - de beste van de stad")[0] == "Glow Clinic Utrecht"


def test_clean_company_name_keeps_clean_names():
    for name in ("SKIN Amsterdam Centrum", "Kliniek Dokter Frodo", "Joost Kroon"):
        n, rv = clean_company_name(name)
        assert n == name and rv is False


def test_clean_company_name_flags_still_polluted():
    # nog steeds title-tag-achtig na knippen (>4 woorden) → handmatige correctie.
    assert clean_company_name("Kliniek voor Esthetiek Botox Filler Laser Rotterdam")[1] is True
    assert clean_company_name("")[1] is True
    assert clean_company_name(None)[1] is True
