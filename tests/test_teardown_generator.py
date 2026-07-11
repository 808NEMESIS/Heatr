"""
tests/test_teardown_generator.py — fase A PR A1.

Valideert findings-extractie en HTML-render op de ECHTE datavorm van
heatr_website_intelligence (checklist, geen screenshots/vision):
- onze-config-gap-checks (pagespeed zonder key) worden NIET als prospect-fout
  getoond;
- lege/deels-lege analyse crasht niet en produceert geen kale pagina;
- alle lead-tekst is ge-escaped (geen HTML-injectie via bedrijfsnaam);
- concurrentie-sectie standaard UIT + alleen bij bruikbare data;
- content_hash is stabiel en verandert mee met de analyse.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from campaigns.teardown_generator import (
    compute_content_hash,
    extract_findings,
    new_token,
    render_teardown_html,
)


# ── Echte datavorm (osteopathieherengracht.nl, uit prod) ────────────────────

def _lead(**ov):
    base = {
        "company_name": "Osteopathie Herengracht Amsterdam",
        "city": "Amsterdam",
        "contact_first_name": "Wendy",
        "domain": "osteopathieherengracht.nl",
    }
    base.update(ov)
    return base


def _wi(**ov):
    base = {
        "total_score": 18,
        "technical_details": {"details": [
            {"check": "ssl", "passed": True},
            {"check": "cms", "value": "WordPress", "passed": True},
            {"check": "schema_markup", "passed": True},
            {"check": "sitemap", "passed": True},
            {"check": "server_location", "value": "NL", "passed": True},
            {"check": "pagespeed", "note": "PAGESPEED_API_KEY not set", "passed": False},
        ]},
        "conversion_details": {"details": [
            {"check": "cta_above_fold", "passed": False},
            {"check": "phone_clickable", "passed": False},
            {"check": "whatsapp", "passed": False},
            {"check": "online_booking", "passed": False},
            {"check": "chatbot", "passed": False},
            {"check": "contact_form", "passed": False},
        ]},
        "sector_details": {"tier": "C", "checks": [
            {"key": "rbcz", "label": "RBCZ", "passed": False, "points": 0},
            {"key": "avg", "label": "AVG", "passed": False, "points": 0},
            {"key": "privacyverklaring", "label": "privacyverklaring", "passed": True, "points": 2},
        ]},
        "opportunity_reasons": {
            "chatbot": "Geen chatbot of live chat gedetecteerd",
            "ai_audit": "Standaard aanbeveling na websitegesprek",
            "website_rebuild": "Totaal score 18/100 — onder 40",
            "conversie_optimalisatie": "Geen WhatsApp integratie",
        },
        "competitor_data": {},
        "score_vs_market": None,
    }
    base.update(ov)
    return base


# ── findings ────────────────────────────────────────────────────────────────

def test_pagespeed_config_gap_not_shown_as_prospect_failure():
    """De pagespeed-check faalt door ONZE ontbrekende key — mag de prospect
    niet aangerekend worden."""
    f = extract_findings(_lead(), _wi())
    labels = [c["label"] for c in f["tech_checks"]]
    assert not any("pagespeed" in l.lower() or "snelheid" in l.lower() for l in labels)
    # de wél-passende checks staan er wel
    assert any("SSL" in l for l in labels)


def test_findings_uses_dutch_opportunity_reasons():
    f = extract_findings(_lead(), _wi())
    assert "Totaal score 18/100 — onder 40" in f["top_findings"]
    assert len(f["top_findings"]) <= 5
    assert f["score"] == 18


def test_conversion_gaps_captured():
    f = extract_findings(_lead(), _wi())
    conv_failed = [c["label"] for c in f["conv_checks"] if not c["passed"]]
    assert "WhatsApp-contact" in conv_failed
    assert "Online afspraken maken" in conv_failed


def test_competitor_unusable_when_no_data():
    f = extract_findings(_lead(), _wi())
    assert f["competitor_usable"] is False
    assert f["score_vs_market"] is None


def test_competitor_usable_with_two_analyzed():
    wi = _wi(score_vs_market=-7,
             competitor_data={"total_analyzed": 2, "market_avg_score": 25})
    f = extract_findings(_lead(), wi)
    assert f["competitor_usable"] is True
    assert f["score_vs_market"] == -7


# ── render ──────────────────────────────────────────────────────────────────

def test_render_produces_full_page():
    html = render_teardown_html(_lead(), _wi(), cta_url="https://cal.com/aerys")
    assert html.startswith("<!doctype html>")
    assert "Osteopathie Herengracht Amsterdam" in html
    assert "18<small>/100</small>" in html
    assert "https://cal.com/aerys" in html
    assert 'name="robots" content="noindex' in html


def test_render_escapes_company_name():
    """Bedrijfsnaam met HTML mag niet injecteren."""
    lead = _lead(company_name='<script>alert(1)</script> & Zn')
    html = render_teardown_html(lead, _wi(), cta_url="https://x")
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html
    assert "&amp; Zn" in html


def test_render_survives_empty_analysis():
    """Deels-lege analyse (geen reasons, lege details) → geen crash, geen
    kale pagina."""
    lead = _lead(contact_first_name=None, city=None)
    wi = {"total_score": None, "technical_details": {}, "conversion_details": {},
          "sector_details": {}, "opportunity_reasons": {}}
    html = render_teardown_html(lead, wi, cta_url="https://x")
    assert "<!doctype html>" in html
    assert "0<small>/100</small>" in html  # None → 0
    assert "Hoi," in html                  # geen "Hoi None,"
    # fallback-finding i.p.v. lege lijst
    assert "meer bezoekers klant" in html


def test_competitor_block_off_by_default():
    wi = _wi(score_vs_market=-7,
             competitor_data={"total_analyzed": 2, "market_avg_score": 25})
    html_off = render_teardown_html(_lead(), wi, cta_url="https://x")
    assert "t.o.v. de buurt" not in html_off
    html_on = render_teardown_html(_lead(), wi, cta_url="https://x", include_competitor=True)
    assert "t.o.v. de buurt" in html_on
    assert "7 punten onder" in html_on


def test_unsubscribe_link_optional():
    assert "geen analyses meer" not in render_teardown_html(_lead(), _wi(), cta_url="https://x")
    assert "geen analyses meer" in render_teardown_html(
        _lead(), _wi(), cta_url="https://x", unsubscribe_url="https://x/opt-out")


# ── hash + token ────────────────────────────────────────────────────────────

def test_content_hash_stable_and_sensitive():
    h1 = compute_content_hash(_lead(), _wi())
    h2 = compute_content_hash(_lead(), _wi())
    assert h1 == h2
    h3 = compute_content_hash(_lead(), _wi(total_score=42))
    assert h3 != h1


def test_token_is_unguessable_and_unique():
    tokens = {new_token() for _ in range(100)}
    assert len(tokens) == 100
    assert all(len(t) >= 24 for t in tokens)
