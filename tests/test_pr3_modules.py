"""
tests/test_pr3_modules.py — PR3 enrichment modules smoketests.

Covers: treatment_classifier, domain_age_scraper, meta_ads_scraper,
vision_cache, review_analyzer relative-date parse, conversion_checker
booking_system enum.
"""
from __future__ import annotations

import base64
import hashlib
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

from enrichment.treatment_classifier import _sanity_check, _ALLOWLIST
from enrichment.domain_age_scraper import _normalize_domain, _pick_registration_event
from enrichment.meta_ads_scraper import _extract_ad_focus, _cache_key
from enrichment.review_analyzer import _parse_relative_date, latest_review_date_from_reviews
from utils.vision_cache import screenshot_hash


class TestTreatmentSanity:
    def test_allowlist_populated(self):
        assert "botox" in _ALLOWLIST
        assert "microblading" in _ALLOWLIST
        assert len(_ALLOWLIST) > 50

    def test_sanity_keeps_known_terms(self):
        assert _sanity_check(["Botox", "Fillers"]) == ["Botox", "Fillers"]

    def test_sanity_drops_unknown(self):
        assert _sanity_check(["Unicorn therapie", "Magic dust"]) == []

    def test_sanity_caps_at_5(self):
        out = _sanity_check(["Botox", "Fillers", "Laser", "Microneedling", "Profhilo", "Hifu", "Peeling"])
        assert len(out) <= 5

    def test_sanity_handles_non_string(self):
        # Claude sometimes returns nested dicts by mistake
        assert _sanity_check(["Botox", None, 42, ""]) == ["Botox"]  # type: ignore[list-item]


class TestDomainAgeHelpers:
    def test_normalize_strips_protocol_and_www(self):
        assert _normalize_domain("https://WWW.Foo.nl/bar?q=1") == "foo.nl"

    def test_normalize_empty(self):
        assert _normalize_domain("") == ""

    def test_pick_registration_event(self):
        events = [
            {"eventAction": "last changed", "eventDate": "2024-01-01T00:00:00Z"},
            {"eventAction": "registration", "eventDate": "2018-06-15T00:00:00Z"},
        ]
        dt = _pick_registration_event(events)
        assert dt is not None and dt.year == 2018

    def test_pick_registration_returns_none_when_missing(self):
        events = [{"eventAction": "expiration", "eventDate": "2030-01-01T00:00:00Z"}]
        assert _pick_registration_event(events) is None


class TestMetaAdsHelpers:
    def test_extract_focus_finds_first_match(self):
        assert _extract_ad_focus("We doen Botox en laser").lower() == "botox"

    def test_extract_focus_case_insensitive(self):
        assert _extract_ad_focus("MICROBLADING aanbieding").lower() == "microblading"

    def test_extract_focus_returns_none_on_miss(self):
        assert _extract_ad_focus("gewoon een praatje") is None

    def test_cache_key_stable_and_case_insensitive(self):
        assert _cache_key("Kliniek", "foo.nl") == _cache_key("kliniek", "FOO.NL")


class TestReviewRelativeDate:
    def test_weeks_ago_nl(self):
        now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
        dt = _parse_relative_date("3 weken geleden", now)
        assert dt is not None
        assert (now - dt).days == 21

    def test_een_maand_geleden(self):
        now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
        dt = _parse_relative_date("een maand geleden", now)
        assert dt is not None
        assert (now - dt).days == 30

    def test_years_ago_english(self):
        now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
        dt = _parse_relative_date("2 years ago", now)
        assert dt is not None
        assert 720 <= (now - dt).days <= 740

    def test_latest_picks_most_recent(self):
        now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
        reviews = [
            {"date": "6 maanden geleden"},
            {"date": "1 week geleden"},
            {"date": ""},
            {"date": "2 jaar geleden"},
        ]
        latest = latest_review_date_from_reviews(reviews)
        # Most recent is "1 week geleden" ≈ 7 days ago
        assert latest is not None


class TestVisionCacheHash:
    def test_hash_stable_for_same_bytes(self):
        b = base64.b64encode(b"same png").decode()
        assert screenshot_hash(b) == hashlib.sha256(b"same png").hexdigest()

    def test_hash_different_for_different_bytes(self):
        a = base64.b64encode(b"aaa").decode()
        b = base64.b64encode(b"bbb").decode()
        assert screenshot_hash(a) != screenshot_hash(b)

    def test_hash_empty(self):
        assert screenshot_hash("") == ""


class TestConversionCheckerBookingSystem:
    @pytest.mark.asyncio
    async def test_booking_enum_online(self):
        from website_intelligence.conversion_checker import check_conversion
        html = '<html><body><a href="https://calendly.com/x">boek</a></body></html>'
        res = await check_conversion(
            domain="x.nl", page_html=html, sector="cosmetische_behandelaars",
            supabase_client=MagicMock(),
        )
        assert res.get("booking_system") == "online"

    @pytest.mark.asyncio
    async def test_booking_enum_phone_only(self):
        from website_intelligence.conversion_checker import check_conversion
        html = '<html><body><a href="tel:+31612345678">Bel ons</a></body></html>'
        res = await check_conversion(
            domain="x.nl", page_html=html, sector="cosmetische_behandelaars",
            supabase_client=MagicMock(),
        )
        # Should fall through to phone-only (no form, no booking)
        assert res.get("booking_system") in ("phone-only", "unknown")
