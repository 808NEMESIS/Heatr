"""
tests/test_launch_readiness.py — per-lead launch readiness verdict.

Valideert de compositie van bestaande gates tot ready/blocked/needs_review
met concrete redenen. Pure-function tests, fixed-time fixtures.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.launch_readiness import assess_launch_readiness

NOW = datetime(2026, 7, 5, 12, 0, tzinfo=timezone.utc)


def _ready_lead(**overrides):
    """Lead die alle checks groen heeft — verdict 'ready' als baseline."""
    lead = {
        "id": "l1",
        "company_name": "Kliniek X",
        "gdpr_safe": True,
        "status": "enriched",
        "email": "info@kliniek.nl",
        "email_status": "verified",
        "archetype": "medisch_cosmetisch",
        "score": 80,
        "icp_match": 0.8,
        "sector": "cosmetische_behandelaars",
        "contact_first_name": "Anna",
        "personalized_opener": "Hi Anna...",
        "treatment_focus": ["botox"],
        "website_score": 42,
        "pushed_to_warmr_at": None,
        "contact_attempt_count": 0,
        "next_contact_after": None,
        "snoozed_until": None,
    }
    lead.update(overrides)
    return lead


def _verdict(lead):
    return assess_launch_readiness(lead, now=NOW)


def test_fully_ready_lead():
    out = _verdict(_ready_lead())
    assert out["verdict"] == "ready"
    assert out["blockers"] == []
    assert out["reviews"] == []


def test_gdpr_unsafe_blocks():
    out = _verdict(_ready_lead(gdpr_safe=False))
    assert out["verdict"] == "blocked"
    assert any("gdpr_safe" in b for b in out["blockers"])


def test_unsubscribed_status_blocks():
    out = _verdict(_ready_lead(status="unsubscribed"))
    assert out["verdict"] == "blocked"


def test_missing_email_blocks():
    out = _verdict(_ready_lead(email=None))
    assert out["verdict"] == "blocked"
    assert any("Email niet sendable" in b for b in out["blockers"])


def test_low_score_blocks():
    out = _verdict(_ready_lead(score=40))
    assert out["verdict"] == "blocked"
    assert any("score=40" in b for b in out["blockers"])


def test_low_icp_blocks():
    out = _verdict(_ready_lead(icp_match=0.3))
    assert out["verdict"] == "blocked"


def test_missing_archetype_blocks():
    out = _verdict(_ready_lead(archetype=None))
    assert out["verdict"] == "blocked"
    assert any("archetype" in b for b in out["blockers"])


def test_active_cooldown_blocks():
    future = (NOW + timedelta(days=30)).isoformat()
    out = _verdict(_ready_lead(next_contact_after=future))
    assert out["verdict"] == "blocked"
    assert any("Cooldown" in b for b in out["blockers"])


def test_expired_cooldown_does_not_block():
    past = (NOW - timedelta(days=5)).isoformat()
    out = _verdict(_ready_lead(next_contact_after=past))
    assert out["verdict"] == "ready"


def test_risky_email_needs_review():
    out = _verdict(_ready_lead(email_status="risky"))
    assert out["verdict"] == "needs_review"
    assert any("bounce-risico" in r for r in out["reviews"])


def test_missing_personalisation_needs_review():
    out = _verdict(_ready_lead(personalized_opener=None, contact_first_name=None))
    assert out["verdict"] == "needs_review"
    assert any("personalisatie" in r for r in out["reviews"])


def test_previously_contacted_needs_review():
    out = _verdict(_ready_lead(pushed_to_warmr_at="2026-06-01T10:00:00+00:00"))
    assert out["verdict"] == "needs_review"
    assert any("eerder benaderd" in r for r in out["reviews"])


def test_missing_website_analysis_needs_review():
    out = _verdict(_ready_lead(website_score=None))
    assert out["verdict"] == "needs_review"


def test_test_lead_bypasses_score_and_completeness_not_compliance():
    # Test-lead met lage score + missing archetype → geen block daarop
    out = _verdict(_ready_lead(is_test_lead=True, score=0, icp_match=0, archetype=None))
    assert out["verdict"] != "blocked"
    # Maar compliance blijft absoluut
    out2 = _verdict(_ready_lead(is_test_lead=True, status="unsubscribed"))
    assert out2["verdict"] == "blocked"


def test_blocked_wins_over_review():
    """Lead met zowel block- als review-issues → verdict blocked."""
    out = _verdict(_ready_lead(gdpr_safe=False, email_status="risky"))
    assert out["verdict"] == "blocked"
    assert len(out["reviews"]) >= 1  # review-redenen blijven zichtbaar
