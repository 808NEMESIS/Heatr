"""tests/test_signal_picker.py — 6-tier prioriteits-keten voor {{signaal_blok}}.

Eén test per tier (1-6) met data die de exact-die-tier triggert (lower tiers
hebben ook signal-data uitgesloten zodat duidelijk is dat de hogere tier wint).
"""
from __future__ import annotations

from utils.signal_picker import pick_signaal_blok


# ---------------------------------------------------------------------------
# Tier 1 — review-quality (hoogste prioriteit)
# ---------------------------------------------------------------------------

def test_tier_1_review_quality_wins_when_count_high_and_rating_high():
    lead = {
        "google_review_count": 49,
        "google_rating": 5.0,
        # Lower tiers also satisfied — Tier 1 should still win
        "treatment_focus": ["Botox", "Fillers", "Skinboosters"],
        "meta_ads_active": True,
        "ad_focus": "PMU",
        "company_age_years": 10,
        "city": "Amsterdam",
    }
    assert pick_signaal_blok(lead) == "49 reviews met een 5.0-rating"


# ---------------------------------------------------------------------------
# Tier 2 — treatment-spread
# ---------------------------------------------------------------------------

def test_tier_2_treatment_spread_when_no_review_quality():
    lead = {
        # Tier 1 NOT satisfied (rating onder 4.5)
        "google_review_count": 50,
        "google_rating": 4.2,
        # Tier 2 satisfied
        "treatment_focus": ["Botox", "Fillers", "Skinboosters", "Microneedling"],
        # Lower tiers also satisfied
        "meta_ads_active": True,
        "company_age_years": 8,
        "city": "Utrecht",
    }
    assert pick_signaal_blok(lead) == "Botox, Fillers, Skinboosters in jullie aanbod"


# ---------------------------------------------------------------------------
# Tier 3 — ad-investering
# ---------------------------------------------------------------------------

def test_tier_3_ad_investering_when_no_higher_tiers():
    lead = {
        # Tier 1 NOT satisfied (count te laag)
        "google_review_count": 10,
        "google_rating": 5.0,
        # Tier 2 NOT satisfied (te weinig treatments)
        "treatment_focus": ["Botox"],
        # Tier 3 satisfied
        "meta_ads_active": True,
        "ad_focus": "PMU",
        # Lower tiers also satisfied
        "company_age_years": 7,
        "city": "Rotterdam",
    }
    assert pick_signaal_blok(lead) == "Meta Ads-campagnes met focus op PMU"


def test_tier_3_ad_investering_without_focus_falls_to_generic_string():
    """Edge case: meta_ads_active=true maar ad_focus leeg → andere variant."""
    lead = {
        "google_review_count": 5,
        "google_rating": 4.0,
        "treatment_focus": [],
        "meta_ads_active": True,
        "ad_focus": None,  # Geen specifieke focus
    }
    assert pick_signaal_blok(lead) == "Meta Ads-campagnes die actief draaien"


# ---------------------------------------------------------------------------
# Tier 4 — bedrijfsleeftijd (positief frame, NIET website-leeftijd)
# ---------------------------------------------------------------------------

def test_tier_4_bedrijfsleeftijd_when_no_higher_tiers():
    lead = {
        # Tier 1-3 NOT satisfied
        "google_review_count": 5,
        "google_rating": 4.0,
        "treatment_focus": ["X"],
        "meta_ads_active": False,
        # Tier 4 satisfied
        "company_age_years": 12,
        "city": "Groningen",
    }
    assert pick_signaal_blok(lead) == "12 jaar geschiedenis in Groningen"


# ---------------------------------------------------------------------------
# Tier 5 — lokale-zichtbaarheid
# ---------------------------------------------------------------------------

def test_tier_5_lokale_zichtbaarheid_when_only_city_known():
    lead = {
        # Tier 1-4 NOT satisfied
        "google_review_count": 5,
        "google_rating": 3.0,
        "treatment_focus": [],
        "meta_ads_active": False,
        "company_age_years": None,
        # Tier 5: city alleen
        "city": "Leiden",
    }
    assert pick_signaal_blok(lead) == "de naam die jullie in Leiden hebben opgebouwd"


# ---------------------------------------------------------------------------
# Tier 6 — generieke fallback
# ---------------------------------------------------------------------------

def test_tier_6_generic_fallback_when_lead_data_empty():
    """Compleet lege lead → fallback naar generieke string."""
    assert pick_signaal_blok({}) == "het werk dat jullie leveren"

    # Ook expliciet None-velden moet niet crashen
    lead_with_nones = {
        "google_review_count": None,
        "google_rating": None,
        "treatment_focus": None,
        "meta_ads_active": False,
        "company_age_years": None,
        "city": None,
    }
    assert pick_signaal_blok(lead_with_nones) == "het werk dat jullie leveren"


# ---------------------------------------------------------------------------
# Robustness — geen crashes op rare data
# ---------------------------------------------------------------------------

def test_handles_string_review_count_gracefully():
    """Als review_count een string is (rare DB-state) → niet crashen."""
    lead = {
        "google_review_count": "not_a_number",
        "google_rating": 5.0,
        "city": "Den Haag",
    }
    # Tier 1 faalt op TypeError, valt door naar Tier 5
    assert pick_signaal_blok(lead) == "de naam die jullie in Den Haag hebben opgebouwd"
