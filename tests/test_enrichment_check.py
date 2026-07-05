"""
tests/test_enrichment_check.py — Pre-launch completeness checker.

Verifies hard-required enforcement, soft-recommended warnings, de test-lead
bypass voor smoke-test pipeline, en de compliance-gate (GDPR + status) die
GEEN test-lead-bypass kent.
"""
from utils.enrichment_check import (
    BLOCKED_STATUSES,
    HARD_REQUIRED_FIELDS,
    SOFT_RECOMMENDED_FIELDS,
    check_lead_completeness,
    compliance_check,
    filter_launchable_leads,
    _is_filled,
)


def _base_lead(**overrides):
    """Compleet, compliant lead — alle checks groen tenzij override."""
    lead = {
        "id": "l1",
        "company_name": "Praktijk X",
        "archetype": "welzijn_praktisch",
        "score": 75,
        "sector": "cosmetische_behandelaars",
        "contact_first_name": "Mark",
        "personalized_opener": "Hi Mark...",
        "treatment_focus": ["botox"],
        "gdpr_safe": True,
        "status": "enriched",
    }
    lead.update(overrides)
    return lead


# ---------------------------------------------------------------------------
# _is_filled helper
# ---------------------------------------------------------------------------

def test_is_filled_rejects_none_and_empty():
    assert _is_filled({"score": None}, "score") is False
    assert _is_filled({"archetype": ""}, "archetype") is False
    assert _is_filled({"archetype": "  "}, "archetype") is False
    assert _is_filled({"treatment_focus": []}, "treatment_focus") is False


def test_is_filled_accepts_real_values():
    assert _is_filled({"archetype": "lichaamswerk_pragmatisch"}, "archetype") is True
    assert _is_filled({"score": 75}, "score") is True
    assert _is_filled({"treatment_focus": ["chiropractie"]}, "treatment_focus") is True


def test_is_filled_score_zero_is_unfilled():
    """Score=0 is niet enriched — scoring-step zou >0 moeten produceren."""
    assert _is_filled({"score": 0}, "score") is False


# ---------------------------------------------------------------------------
# compliance_check — GDPR + status gate (geen enkele bypass)
# ---------------------------------------------------------------------------

def test_compliance_valid_lead_passes():
    ok, reason = compliance_check(_base_lead())
    assert ok is True
    assert reason is None


def test_compliance_blocks_unsubscribed():
    ok, reason = compliance_check(_base_lead(status="unsubscribed"))
    assert ok is False
    assert "unsubscribed" in reason


def test_compliance_blocks_forgotten():
    ok, reason = compliance_check(_base_lead(status="forgotten"))
    assert ok is False
    assert "forgotten" in reason


def test_compliance_blocks_disqualified():
    ok, reason = compliance_check(_base_lead(status="disqualified"))
    assert ok is False
    assert "disqualified" in reason


def test_compliance_blocks_gdpr_unsafe():
    ok, reason = compliance_check(_base_lead(gdpr_safe=False))
    assert ok is False
    assert "gdpr_safe" in reason


def test_compliance_blocks_gdpr_missing():
    """Ontbrekende gdpr_safe = niet vastgesteld = conservatief blocken."""
    lead = _base_lead()
    del lead["gdpr_safe"]
    ok, _ = compliance_check(lead)
    assert ok is False


def test_compliance_no_test_lead_bypass():
    """Unsubscribe-belofte geldt ook voor test-leads — geen bypass."""
    ok, _ = compliance_check(_base_lead(is_test_lead=True, status="unsubscribed"))
    assert ok is False


# ---------------------------------------------------------------------------
# check_lead_completeness
# ---------------------------------------------------------------------------

def test_complete_lead_passes():
    out = check_lead_completeness(_base_lead())
    assert out["is_complete"] is True
    assert out["compliance_blocked"] is False
    assert out["blocked_reason"] is None
    assert out["missing_required"] == []
    assert out["missing_recommended"] == []


def test_lead_missing_archetype_blocked():
    lead = _base_lead(archetype=None)
    out = check_lead_completeness(lead)
    assert out["is_complete"] is False
    assert "archetype" in out["missing_required"]
    assert "Re-enqueue" in out["blocked_reason"]


def test_lead_missing_score_blocked():
    out = check_lead_completeness(_base_lead(score=None))
    assert out["is_complete"] is False
    assert "score" in out["missing_required"]


def test_lead_missing_sector_blocked():
    out = check_lead_completeness(_base_lead(sector=None))
    assert out["is_complete"] is False
    assert "sector" in out["missing_required"]


def test_test_lead_bypasses_blocked_required_fields():
    """is_test_lead=True → geen blocked_reason ondanks missing required —
    MITS compliance groen is (gdpr_safe=True + status toegestaan)."""
    lead = {
        "id": "l1",
        "is_test_lead": True,
        "gdpr_safe": True,
        "status": "enriched",
        # Geen archetype, score, sector
    }
    out = check_lead_completeness(lead)
    assert out["is_test_lead"] is True
    assert out["blocked_reason"] is None
    assert "archetype" in out["missing_required"]  # blijft tonen voor zichtbaarheid


def test_test_lead_does_not_bypass_compliance():
    """Test-lead met unsubscribed status blijft geblokkeerd."""
    out = check_lead_completeness(_base_lead(is_test_lead=True, status="unsubscribed"))
    assert out["compliance_blocked"] is True
    assert "GDPR/status-block" in out["blocked_reason"]


def test_lead_complete_but_missing_recommended_warns():
    """Hard-required ok, maar contact_first_name ontbreekt → warning."""
    lead = _base_lead(contact_first_name=None, personalized_opener=None, treatment_focus=None)
    out = check_lead_completeness(lead)
    assert out["is_complete"] is True
    assert out["blocked_reason"] is None
    assert "contact_first_name" in out["missing_recommended"]
    assert "personalized_opener" in out["missing_recommended"]


# ---------------------------------------------------------------------------
# filter_launchable_leads
# ---------------------------------------------------------------------------

def test_filter_splits_complete_blocked_warnings():
    leads = [
        # Complete + alle recommended = launchable, no warning
        _base_lead(id="1"),
        # Complete + missing recommended = launchable + warning
        _base_lead(id="2", contact_first_name=None, personalized_opener=None, treatment_focus=None),
        # Missing required = blocked
        _base_lead(id="3", archetype=None),
        # Test-lead = launchable ondanks missing required (compliance groen)
        {"id": "4", "is_test_lead": True, "gdpr_safe": True, "status": "enriched"},
    ]
    launchable, blocked, warnings = filter_launchable_leads(leads)
    assert {l["id"] for l in launchable} == {"1", "2", "4"}
    assert {l["id"] for l in blocked} == {"3"}
    assert {w["lead_id"] for w in warnings} == {"2", "4"}


def test_filter_blocks_all_unsafe_statuses():
    """Elke BLOCKED_STATUS + gdpr_safe=False → blocked, ook met complete data."""
    leads = [
        _base_lead(id="u", status="unsubscribed"),
        _base_lead(id="f", status="forgotten"),
        _base_lead(id="d", status="disqualified"),
        _base_lead(id="g", gdpr_safe=False),
        _base_lead(id="ok"),
    ]
    launchable, blocked, _ = filter_launchable_leads(leads)
    assert {l["id"] for l in launchable} == {"ok"}
    assert {l["id"] for l in blocked} == {"u", "f", "d", "g"}


def test_filter_blocks_unsafe_test_lead():
    """Compliance-block wint van de test-lead-bypass."""
    leads = [_base_lead(id="t", is_test_lead=True, status="unsubscribed")]
    launchable, blocked, _ = filter_launchable_leads(leads)
    assert launchable == []
    assert blocked[0]["id"] == "t"
    assert blocked[0]["_completeness"]["compliance_blocked"] is True


def test_filter_attaches_completeness_to_lead():
    leads = [_base_lead(id="1")]
    launchable, _, _ = filter_launchable_leads(leads)
    assert "_completeness" in launchable[0]
    assert launchable[0]["_completeness"]["is_complete"] is True


def test_constants_match_expected_field_names():
    """Sanity: hard/soft sets + blocked statuses niet per ongeluk veranderd."""
    assert HARD_REQUIRED_FIELDS == ("archetype", "score", "sector")
    assert "personalized_opener" in SOFT_RECOMMENDED_FIELDS
    assert "contact_first_name" in SOFT_RECOMMENDED_FIELDS
    assert BLOCKED_STATUSES == ("unsubscribed", "forgotten", "disqualified")
