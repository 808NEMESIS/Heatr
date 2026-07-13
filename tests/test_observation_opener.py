"""
tests/test_observation_opener.py — de koude-mail-opener (fase A PR A3).

Valideert: prioriteit op herkenbaarheid, fail-closed zonder signaal, geen
zwakke/pitch-signalen als opener, deterministische variant per lead, en de
mail-regels (begint niet met 'Ik', geen link/cijfer, lichte vs volledige
ondertekening).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from campaigns.observation_opener import (
    SIGNATURE_FULL,
    SIGNATURE_LIGHT,
    pick_observation,
    render_mail1,
)


def _lead(**ov):
    base = {"id": "lead-abc", "contact_first_name": "Wendy",
            "domain": "praktijk.nl", "company_name": "Praktijk X"}
    base.update(ov)
    return base


def _wi(failed=("online_booking",), passed=()):
    details = [{"check": c, "passed": False} for c in failed]
    details += [{"check": c, "passed": True} for c in passed]
    return {"conversion_details": {"details": details}}


def test_priority_online_booking_first():
    wi = _wi(failed=("whatsapp", "online_booking", "phone_clickable"))
    p = pick_observation(_lead(), wi)
    assert p["signal"] == "online_booking"


def test_priority_falls_through_to_phone():
    wi = _wi(failed=("whatsapp", "phone_clickable"), passed=("online_booking",))
    p = pick_observation(_lead(), wi)
    assert p["signal"] == "phone_clickable"


def test_chatbot_and_contact_form_never_chosen():
    """Zwakke/pitch-achtige signalen mogen nooit de opener worden."""
    wi = _wi(failed=("chatbot", "contact_form"))
    assert pick_observation(_lead(), wi) is None


def test_no_signal_returns_none_fail_closed():
    wi = _wi(failed=(), passed=("online_booking", "phone_clickable", "whatsapp"))
    assert pick_observation(_lead(), wi) is None
    assert render_mail1(_lead(), wi) is None


def test_deterministic_variant_per_lead():
    """Zelfde lead → zelfde variant (reproduceerbaar, geen randomness)."""
    wi = _wi(failed=("online_booking",))
    a = pick_observation(_lead(id="lead-1"), wi)
    b = pick_observation(_lead(id="lead-1"), wi)
    assert a == b


def test_different_leads_can_get_different_variants():
    wi = _wi(failed=("online_booking",))
    seen = {pick_observation(_lead(id=f"lead-{i}"), wi)["observation"] for i in range(20)}
    assert len(seen) >= 2  # rotatie doet iets


def test_observation_hedged_not_assertive():
    """Geen stellige bewering die fout kan zijn — altijd een hedge."""
    hedges = ("zag ik", "kon ik", "leek", "viel me", "niet meteen", "zo snel")
    for sig in ("online_booking", "phone_clickable", "whatsapp", "cta_above_fold"):
        p = pick_observation(_lead(), _wi(failed=(sig,)))
        assert any(h in p["observation"].lower() for h in hedges), p["observation"]


def test_mail_does_not_start_with_ik():
    """CLAUDE.md-regel: de body begint niet met 'Ik'."""
    for sig in ("online_booking", "phone_clickable", "whatsapp"):
        mail = render_mail1(_lead(), _wi(failed=(sig,)))
        # eerste inhoudsregel na de groet
        first_content = mail.split("\n\n")[1]
        assert not first_content.startswith("Ik ")


def test_mail_has_no_link_or_score():
    mail = render_mail1(_lead(), _wi(failed=("online_booking",)))
    assert "http" not in mail
    assert "/100" not in mail
    assert "€" not in mail


def test_light_signature_default():
    mail = render_mail1(_lead(), _wi(failed=("online_booking",)))
    assert "Aerys' Solution · 06 20761632" in mail
    # geen links in de koude ondertekening
    assert "aeryssolution.nl" not in mail
    assert "Plan een gesprek" not in mail


def test_full_signature_on_request():
    mail = render_mail1(_lead(), _wi(failed=("online_booking",)),
                        signature=SIGNATURE_FULL.replace("{calendar_url}", "https://x"))
    assert "CEO · Aerys' Solution" in mail
    assert "Van interactie naar impact" in mail


def test_greeting_without_name():
    mail = render_mail1(_lead(contact_first_name=None), _wi(failed=("online_booking",)))
    assert mail.startswith("Hoi,")


def test_question_capitalized():
    mail = render_mail1(_lead(), _wi(failed=("online_booking",)))
    # de vraag is een eigen zin met hoofdletter, geen ". bewust"
    assert ". Bewust" in mail or ". Regelen" in mail
    assert ". bewust" not in mail


# ── C3: GO-veilige observatie (alleen vers + hoge confidence) ────────────────

def test_safe_observation_only_on_high_confidence_no_booking():
    from campaigns.observation_opener import pick_safe_observation
    lead = _lead()
    # geslaagde fetch, geen booking → observatie mag
    ok = pick_safe_observation(lead, {"value": "no_booking", "confidence": "high", "evidence": []})
    assert ok is not None and ok["signal"] == "online_booking"
    assert ok["confidence"] == "high"


def test_safe_observation_blocked_on_unknown_fetch():
    from campaigns.observation_opener import pick_safe_observation
    # mislukte/twijfelachtige fetch → GEEN observatie (fail-closed)
    assert pick_safe_observation(_lead(), {"value": "unknown", "confidence": "low"}) is None
    assert pick_safe_observation(_lead(), None) is None


def test_safe_observation_blocked_when_booking_exists():
    from campaigns.observation_opener import pick_safe_observation
    # site HEEFT booking → nooit 'geen booking' claimen
    assert pick_safe_observation(_lead(), {"value": "has_booking", "confidence": "high"}) is None


def test_safe_observation_low_confidence_no_booking_blocked():
    from campaigns.observation_opener import pick_safe_observation
    assert pick_safe_observation(_lead(), {"value": "no_booking", "confidence": "low"}) is None
