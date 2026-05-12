"""
tests/test_treatment_from_google.py — Google-category → treatment_focus mapping.

Run: pytest tests/test_treatment_from_google.py -v
"""
from enrichment.treatment_from_google import map_google_category, merge_treatment_focus


def test_chiropractor_maps_to_chiro_manueel():
    assert map_google_category("Chiropractor") == ["chiropractie", "manueel"]


def test_acupuncturist_maps():
    assert map_google_category("Acupuncturist") == ["acupunctuur"]


def test_skin_care_maps_to_huidverzorging():
    tags = map_google_category("Skin care clinic")
    assert "huidverzorging" in tags
    assert "huidtherapie" in tags


def test_dutch_label_works_too():
    assert map_google_category("Schoonheidssalon") == ["beauty_salon", "huidverzorging"]


def test_unknown_category_returns_empty():
    assert map_google_category("Random foobar") == []


def test_empty_input_returns_empty():
    assert map_google_category(None) == []
    assert map_google_category("") == []


def test_partial_match_still_works():
    """Header met extra prefix moet nog matchen op keyword."""
    tags = map_google_category("Premium Beauty Salon Amsterdam")
    assert "beauty_salon" in tags


def test_merge_dedupes_and_keeps_order():
    existing = ["chiropractie", "manueel"]
    inferred = ["manueel", "wim_hof"]
    out = merge_treatment_focus(existing, inferred)
    assert out == ["chiropractie", "manueel", "wim_hof"]


def test_merge_handles_none_existing():
    assert merge_treatment_focus(None, ["a", "b"]) == ["a", "b"]


def test_merge_filters_empty_strings():
    assert merge_treatment_focus(["", "a"], ["", "b"]) == ["a", "b"]
