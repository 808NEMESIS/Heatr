"""
tests/test_regio_selector.py — outreach spec 4 (coords) + spec 5 (selector).

Pure logica, geen DB/API. De live pool-query (compute_regio_signals) wacht op
migratie 039 + de coord-backfill; hier testen we de bouwstenen synthetisch.
"""
from utils.geo import extract_place_coords, haversine_km
from website_intelligence.regio_selector import (
    select_concurrent_gap, pick_detail_2, radius_pool,
)
from campaigns.sequence_engine import inject_variables
from config.sequence_templates import _mail2_variant_key, resolve_faseA_step


# ── Spec 4: coord-extractie ─────────────────────────────────────────────────
def test_extract_place_prefers_3d4d_over_viewport():
    url = "https://google.nl/maps/place/X/@52.3479658,4.7493863,12z/data=!3d52.3636714!4d4.8884071!16s"
    lat, lng = extract_place_coords(url)
    assert (lat, lng) == (52.3636714, 4.8884071)   # place, niet het @-kaartbeeld


def test_extract_falls_back_to_viewport():
    lat, lng = extract_place_coords("https://google.nl/maps/place/X/@52.37,4.89,15z/")
    assert (lat, lng) == (52.37, 4.89)


def test_extract_none_when_absent():
    assert extract_place_coords("https://example.com") is None
    assert extract_place_coords(None) is None


def test_haversine_known_distance():
    # Amsterdam Dam ~ Rotterdam centrum ≈ 57-58 km
    d = haversine_km(52.3730, 4.8930, 51.9225, 4.4792)
    assert 55 < d < 62


# ── Spec 5: radius-pool ─────────────────────────────────────────────────────
def test_radius_pool_filters_and_excludes_self():
    lead = {"id": "me", "lat": 52.37, "lng": 4.89}
    cand = [
        {"id": "me", "lat": 52.37, "lng": 4.89},        # self → uit
        {"id": "near", "lat": 52.38, "lng": 4.90},      # ~1.3 km → in
        {"id": "far", "lat": 51.92, "lng": 4.48},       # ~57 km → uit
        {"id": "nocoord", "lat": None, "lng": None},    # geen coords → uit
    ]
    pool = radius_pool(lead, cand, radius_km=15)
    assert {c["id"] for c in pool} == {"near"}


def test_radius_pool_empty_without_own_coords():
    assert radius_pool({"id": "x", "lat": None, "lng": None}, [{"id": "y", "lat": 52.0, "lng": 4.0}]) == []


# ── Spec 5: concurrent-gap-selectie ─────────────────────────────────────────
def test_gap_reviews_picked_with_numbers():
    lead = {"google_review_count": 20, "google_rating": 4.7}
    pool = [{"google_review_count": 120, "google_rating": 4.7}]
    s = select_concurrent_gap(lead, pool, brug="conceptsite")
    assert s and "120 reviews" in s and "20" in s


def test_gap_rating_picked_nl_comma():
    lead = {"google_review_count": 60, "google_rating": 4.4}
    pool = [{"google_review_count": 60, "google_rating": 4.9}]
    s = select_concurrent_gap(lead, pool, brug="conceptsite")
    assert s and "4,9" in s and "4,4" in s  # NL-komma


def test_gap_booking_only_workflow():
    lead = {"google_review_count": 50, "google_rating": 4.7, "has_online_booking": False}
    pool = [{"google_review_count": 50, "google_rating": 4.7, "has_online_booking": True}]
    assert select_concurrent_gap(lead, pool, brug="workflow") is not None
    assert select_concurrent_gap(lead, pool, brug="conceptsite") is None  # geen kloof voor conceptsite


def test_gap_none_when_no_defensible_gap():
    lead = {"google_review_count": 100, "google_rating": 4.9}
    pool = [{"google_review_count": 40, "google_rating": 4.6}]  # lead is beter
    assert select_concurrent_gap(lead, pool, brug="conceptsite") is None


# ── Spec 5b: detail_2 laag-uitsluiting ──────────────────────────────────────
def test_detail_2_prefers_conversion_not_reviews():
    assert "afspraakknop" in pick_detail_2({"has_online_booking": False})
    assert "WhatsApp" in pick_detail_2({"has_online_booking": True, "has_whatsapp": False})
    assert "laadtijd" in pick_detail_2({"has_online_booking": True, "has_whatsapp": True, "website_score": 30})
    assert pick_detail_2({"has_online_booking": True, "has_whatsapp": True, "website_score": 80}) is None


def test_detail_2_flows_grammatically_in_mail2_frame():
    """Naamwoord-frase → geen botsende werkwoorden vóór 'liet me niet los'."""
    d = pick_detail_2({"has_online_booking": False})
    sentence = f"{d} liet me niet los."
    assert "staat liet" not in sentence and "heeft liet" not in sentence
    assert sentence.startswith("De ontbrekende afspraakknop op je site liet me niet los")


# ── Integratie: tokens vullen → mail 2 "both"-variant rendert schoon ────────
def test_signals_feed_mail2_both_variant():
    lead = {
        "company_name": "Kliniek X", "contact_first_name": "Jan",
        "sector": "cosmetische_behandelaars",
        "detail_2": "Dat er nog geen directe afspraakknop op je site staat",
        "concurrent_signaal": "een praktijk op een paar kilometer met 120 reviews, waar jullie er 20 hebben",
    }
    assert _mail2_variant_key(lead) == "both"
    step = resolve_faseA_step("conceptsite", 1, lead, free_slots=5)
    body = inject_variables(step["body"], lead)
    assert "{{" not in body and "—" not in body
    assert "120 reviews" in body and "afspraakknop" in body
    assert body.count("?") == 1
