"""
tests/test_lead_activity.py — derived CRM status beslissingsboom.

Tests pure derive_status() — geen DB. Run: pytest tests/test_lead_activity.py -v
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from utils.lead_activity import COOLDOWN_DAYS, CRM_STATUSES, derive_status


def _now() -> datetime:
    return datetime.now(timezone.utc)


def test_manual_override_wins_over_everything():
    lead = {"manual_status_override": "geen_interesse", "manual_status_override_reason": "telefonisch", "email_status": "verified"}
    classification = {"category": "interested"}
    seq = [{"is_active": True}]
    status, reason, _ = derive_status(lead, classification, _now(), seq)
    assert status == "geen_interesse"
    assert "telefonisch" in reason


def test_email_status_unsubscribed_wins_over_classifier():
    lead = {"email_status": "unsubscribed"}
    classification = {"category": "interested"}
    status, _, _ = derive_status(lead, classification, _now(), [])
    assert status == "afgemeld"


def test_classifier_unsubscribe_returns_afgemeld():
    lead = {}
    status, _, _ = derive_status(lead, {"category": "unsubscribe_request"}, _now(), [])
    assert status == "afgemeld"


def test_classifier_not_interested():
    status, _, _ = derive_status({}, {"category": "not_interested"}, _now(), [])
    assert status == "geen_interesse"


def test_classifier_not_now_returns_recontact_later():
    status, _, _ = derive_status({}, {"category": "not_now"}, _now(), [])
    assert status == "recontact_later"


def test_classifier_interested_returns_actief_gesprek():
    status, _, _ = derive_status({}, {"category": "interested"}, _now(), [])
    assert status == "actief_gesprek"


def test_classifier_question_returns_actief_gesprek():
    status, _, _ = derive_status({}, {"category": "question"}, _now(), [])
    assert status == "actief_gesprek"


def test_classifier_wrong_person():
    status, _, _ = derive_status({}, {"category": "wrong_person"}, _now(), [])
    assert status == "verkeerde_contact"


def test_active_sequence_no_reply_returns_in_sequence():
    lead = {}
    seq = [{"is_active": True, "status": "pending", "created_at": _now().isoformat()}]
    status, _, _ = derive_status(lead, None, None, seq)
    assert status == "in_sequence"


def test_completed_sequence_within_cooldown_returns_cooldown():
    lead = {}
    sent = (_now() - timedelta(days=10)).isoformat()
    seq = [{"is_active": False, "status": "sequence_complete", "sent_at": sent}]
    status, _, _ = derive_status(lead, None, None, seq)
    assert status == "cooldown"


def test_completed_sequence_after_cooldown_returns_klaar_voor_recontact():
    lead = {}
    sent = (_now() - timedelta(days=COOLDOWN_DAYS + 5)).isoformat()
    seq = [{"is_active": False, "status": "sequence_complete", "sent_at": sent}]
    status, _, _ = derive_status(lead, None, None, seq)
    assert status == "klaar_voor_recontact"


def test_no_sequence_returns_niet_aangeschreven():
    status, _, _ = derive_status({}, None, None, [])
    assert status == "niet_aangeschreven"


def test_invalid_manual_override_falls_through_to_derived():
    """Onbekende override-waarde moet niet de derived-logica blokkeren."""
    lead = {"manual_status_override": "onzin_status"}
    classification = {"category": "interested"}
    status, _, _ = derive_status(lead, classification, _now(), [])
    assert status == "actief_gesprek"


def test_all_returned_statuses_in_known_set():
    """Alle paden van derive_status moeten een geldige CRM_STATUS retourneren."""
    cases = [
        ({"manual_status_override": "afgemeld"}, None, None, []),
        ({"email_status": "unsubscribed"}, None, None, []),
        ({}, {"category": "not_interested"}, _now(), []),
        ({}, {"category": "not_now"}, _now(), []),
        ({}, {"category": "interested"}, _now(), []),
        ({}, {"category": "wrong_person"}, _now(), []),
        ({}, None, None, [{"is_active": True, "created_at": _now().isoformat()}]),
        ({}, None, None, [{"is_active": False, "status": "sequence_complete", "sent_at": _now().isoformat()}]),
        ({}, None, None, []),
    ]
    for lead, cls, inb, seq in cases:
        status, _, _ = derive_status(lead, cls, inb, seq)
        assert status in CRM_STATUSES, f"got unknown status {status}"
