"""
tests/test_contact_inference.py — _infer_contact_from_email guard-rails.

Live bug ontdekt: 'ceciledebooij@gmail.com' → first_name='Ceciledebooij'
→ Mail 1 begon met 'Hoi Ceciledebooij' = onmiddellijke 'dit is bot'-trigger.
"""
from enrichment.contact_discovery import _infer_contact_from_email


def test_consumer_domain_gmail_rejected():
    """Gmail/Hotmail/etc. local-part is geen echte voornaam — weiger inference."""
    assert _infer_contact_from_email("ceciledebooij@gmail.com", "ceciledebooij.nl") is None


def test_consumer_domain_outlook_rejected():
    assert _infer_contact_from_email("praktijk@outlook.com", "kliniek.nl") is None


def test_consumer_domain_ziggo_rejected():
    """NL-ISP consumer mailen ook weigeren."""
    assert _infer_contact_from_email("jan@ziggo.nl", "jan.nl") is None


def test_role_email_rejected():
    assert _infer_contact_from_email("info@kliniek.nl", "kliniek.nl") is None
    assert _infer_contact_from_email("contact@kliniek.nl", "kliniek.nl") is None


def test_long_unsplittable_local_rejected():
    """Geen splitter + >12 chars = waarschijnlijk samengevoegde naam, te onveilig."""
    assert _infer_contact_from_email("ceciledebooij@kliniek.nl", "kliniek.nl") is None
    assert _infer_contact_from_email("dirkenmariakliniek@samen.nl", "samen.nl") is None


def test_short_local_on_business_domain_accepted():
    """jan@kliniek.nl → first_name=Jan ok (kort, eigen domein)."""
    out = _infer_contact_from_email("jan@kliniek.nl", "kliniek.nl")
    assert out is not None
    assert out["first_name"] == "Jan"


def test_dotted_local_on_business_domain_accepted():
    out = _infer_contact_from_email("jan.devries@kliniek.nl", "kliniek.nl")
    assert out is not None
    assert out["first_name"] == "Jan"
    assert out["last_name"] == "Devries"


def test_single_letter_initial_rejected():
    """Single-letter first name (j.devries) wordt afgekeurd door min_length-check."""
    out = _infer_contact_from_email("j.devries@kliniek.nl", "kliniek.nl")
    assert out is None  # eerste deel "j" < 2 chars → weigert


def test_too_short_first_name_rejected():
    """Single letter local zonder dot → reject."""
    assert _infer_contact_from_email("a@kliniek.nl", "kliniek.nl") is None
