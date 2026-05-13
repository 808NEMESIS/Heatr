"""
tests/test_enrichment_check.py — Pre-launch completeness checker.

Verifies hard-required enforcement, soft-recommended warnings, en de
test-lead bypass voor smoke-test pipeline.
"""
from utils.enrichment_check import (
    HARD_REQUIRED_FIELDS,
    SOFT_RECOMMENDED_FIELDS,
    check_lead_completeness,
    filter_launchable_leads,
    _is_filled,
)


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
# check_lead_completeness
# ---------------------------------------------------------------------------

def test_complete_lead_passes():
    lead = {
        "id": "l1",
        "company_name": "Praktijk X",
        "archetype": "welzijn_praktisch",
        "score": 75,
        "sector": "alternatieve_geneeskunde",
        "contact_first_name": "Mark",
        "personalized_opener": "Hi Mark...",
        "treatment_focus": ["chiropractie"],
    }
    out = check_lead_completeness(lead)
    assert out["is_complete"] is True
    assert out["blocked_reason"] is None
    assert out["missing_required"] == []
    assert out["missing_recommended"] == []


def test_lead_missing_archetype_blocked():
    lead = {
        "id": "l1",
        "score": 75,
        "sector": "cosmetische_behandelaars",
    }
    out = check_lead_completeness(lead)
    assert out["is_complete"] is False
    assert "archetype" in out["missing_required"]
    assert "Re-enqueue" in out["blocked_reason"]


def test_lead_missing_score_blocked():
    lead = {"id": "l1", "archetype": "premium_beauty", "sector": "cosmetische_behandelaars"}
    out = check_lead_completeness(lead)
    assert out["is_complete"] is False
    assert "score" in out["missing_required"]


def test_lead_missing_sector_blocked():
    lead = {"id": "l1", "archetype": "volume_beauty", "score": 60}
    out = check_lead_completeness(lead)
    assert out["is_complete"] is False
    assert "sector" in out["missing_required"]


def test_test_lead_bypasses_blocked_required_fields():
    """is_test_lead=True → geen blocked_reason ondanks missing required."""
    lead = {
        "id": "l1",
        "is_test_lead": True,
        # Geen archetype, score, sector
    }
    out = check_lead_completeness(lead)
    assert out["is_test_lead"] is True
    assert out["blocked_reason"] is None
    assert "archetype" in out["missing_required"]  # blijft tonen voor zichtbaarheid


def test_lead_complete_but_missing_recommended_warns():
    """Hard-required ok, maar contact_first_name ontbreekt → warning."""
    lead = {
        "id": "l1",
        "archetype": "lichaamswerk_pragmatisch",
        "score": 50,
        "sector": "alternatieve_geneeskunde",
        # Geen contact_first_name / personalized_opener / treatment_focus
    }
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
        {"id": "1", "archetype": "premium_beauty", "score": 80, "sector": "cosmetische_behandelaars",
         "contact_first_name": "Anna", "personalized_opener": "Hi Anna...",
         "treatment_focus": ["botox"]},
        # Complete + missing recommended = launchable + warning
        {"id": "2", "archetype": "volume_beauty", "score": 50, "sector": "cosmetische_behandelaars"},
        # Missing required = blocked
        {"id": "3", "score": 70, "sector": "cosmetische_behandelaars"},  # geen archetype
        # Test-lead = launchable ondanks missing required
        {"id": "4", "is_test_lead": True},
    ]
    launchable, blocked, warnings = filter_launchable_leads(leads)
    assert {l["id"] for l in launchable} == {"1", "2", "4"}
    assert {l["id"] for l in blocked} == {"3"}
    # Warnings = launchable EN missing_recommended. Zowel #2 (regulier) als #4
    # (test-lead) hebben missing recommended, dus beide krijgen warning.
    assert {w["lead_id"] for w in warnings} == {"2", "4"}


def test_filter_attaches_completeness_to_lead():
    leads = [{"id": "1", "archetype": "premium_beauty", "score": 80, "sector": "cosmetische_behandelaars"}]
    launchable, _, _ = filter_launchable_leads(leads)
    assert "_completeness" in launchable[0]
    assert launchable[0]["_completeness"]["is_complete"] is True


def test_constants_match_expected_field_names():
    """Sanity: hard/soft sets niet per ongeluk veranderd."""
    assert HARD_REQUIRED_FIELDS == ("archetype", "score", "sector")
    assert "personalized_opener" in SOFT_RECOMMENDED_FIELDS
    assert "contact_first_name" in SOFT_RECOMMENDED_FIELDS
