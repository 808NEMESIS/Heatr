"""tests/test_owner_verification.py — H3: owner-naam-bronverificatie."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from enrichment.owner_extractor import name_in_source, OWNER_NAME_CONFIDENCE_FLOOR

PAGE = "Onze praktijk. Eigenaar: Mieke Kuipers is al 12 jaar hoofdbehandelaar. Team: dr. Jansen."

def test_full_name_in_source():
    assert name_in_source("Mieke Kuipers", PAGE) is True

def test_surname_match_when_claude_adds_title():
    # Claude geeft 'Dr. J. Jansen', site toont alleen 'Jansen'
    assert name_in_source("Dr. J. Jansen", PAGE) is True

def test_hallucinated_name_rejected():
    assert name_in_source("Pieter van Verzinsel", PAGE) is False

def test_short_firstname_not_matched_loosely():
    assert name_in_source("An", PAGE) is False  # <3 tekens, geen los token-match

def test_empty_inputs():
    assert name_in_source(None, PAGE) is False
    assert name_in_source("Mieke", "") is False

def test_confidence_floor_is_reasonable():
    assert 0.0 < OWNER_NAME_CONFIDENCE_FLOOR <= 0.7

def test_case_and_whitespace_insensitive():
    assert name_in_source("  mieke   kuipers ", PAGE) is True
