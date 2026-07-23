"""
tests/test_hook_detector.py — signaal-ladder-beslissing + header/nav-regel.

Regressie op Sami's live-check (2026-07-22): Fairday en CadanCe vuurden
signaal 2 ("boeking X schermen diep") terwijl er een boekingang in de
header/hoofdnavigatie zat (boven de vouw, of in het hamburger-menu). De
scherm-diepte-conclusie was daardoor aantoonbaar onwaar. Harde regel:
boekingang in header/nav/menu → signaal 2 vervalt, ongeacht de body-widget.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from website_intelligence.hook_detector import (
    SIGNAL2_MIN_Y,
    _decide_signal,
    evaluate_booking_entries,
)


def _entry(text="", href="", y=None, visible=True, in_nav=False, aria=""):
    return {"text": text, "href": href, "aria": aria, "y": y,
            "visible": visible, "in_nav": in_nav}


# ── Fixture: Fairday Clinics — header-"boek nu" + hero-knop + diepe widget ──
FAIRDAY_ENTRIES = [
    _entry(text="boek nu", y=52.0, visible=True, in_nav=True),      # header
    _entry(text="boek nu", y=430.0, visible=True, in_nav=False),    # hero, boven vouw
    _entry(text="book now", y=3014.8, visible=True, in_nav=False),  # body-widget diep
    _entry(text="menu", y=20.0, visible=True, in_nav=True),         # ruis
]

# ── Fixture: CadanCe — "afspraak maken" in dichtgeklapt hoofdmenu + diepe widget ──
CADANCE_ENTRIES = [
    _entry(text="afspraak maken", y=None, visible=False, in_nav=True),          # hamburger
    _entry(text="maak online een afspraak", href="/afspraak-maken#boek",
           y=4909.2, visible=True, in_nav=False),                               # body diep
]

# ── Positieve controle: alléén een diepe zichtbare widget, nav schoon ──
DEEP_ONLY_ENTRIES = [
    _entry(text="boek een afspraak", y=2200.0, visible=True, in_nav=False),
]


def test_fairday_fixture_no_signal2():
    r = evaluate_booking_entries(FAIRDAY_ENTRIES)
    assert r["found"] and r["nav_booking"] and not r["sig2_allowed"]
    d = _decide_signal(
        fetch_ok=True,
        booking={"value": "has_booking", "confidence": "high", "evidence": []},
        tel_present=False, cta=r, dom_interactive_ms=400,
    )
    assert d["fired_signal"] is None, d


def test_cadance_fixture_no_signal2():
    r = evaluate_booking_entries(CADANCE_ENTRIES)
    assert r["found"] and r["nav_booking"] and not r["sig2_allowed"]
    d = _decide_signal(
        fetch_ok=True,
        booking={"value": "has_booking", "confidence": "high",
                 "evidence": ["platform:salonized.com"]},
        tel_present=True, cta=r, dom_interactive_ms=440,
    )
    assert d["fired_signal"] is None, d


def test_deep_widget_clean_nav_fires_signal2():
    r = evaluate_booking_entries(DEEP_ONLY_ENTRIES)
    assert r["sig2_allowed"] and r["min_visible_y"] == 2200.0
    d = _decide_signal(
        fetch_ok=True,
        booking={"value": "has_booking", "confidence": "high", "evidence": []},
        tel_present=True, cta=r, dom_interactive_ms=400,
    )
    assert d["fired_signal"] == 2
    assert any("nav_check=schoon" in e for e in d["evidence"])


def test_hidden_non_nav_booking_blocks_signal2():
    # verborgen boek-element buiten de nav (overlay/popup) → bereikbaar zonder
    # scrollen → bij twijfel niet vuren
    entries = DEEP_ONLY_ENTRIES + [_entry(text="boek nu", y=None, visible=False)]
    r = evaluate_booking_entries(entries)
    assert r["hidden_booking"] and not r["sig2_allowed"]


def test_shallow_widget_blocks_signal2():
    # zichtbare boekingang boven de 2-schermen-grens → "paar schermen scrollen" onwaar
    entries = [_entry(text="boek nu", y=SIGNAL2_MIN_Y - 100, visible=True)]
    r = evaluate_booking_entries(entries)
    assert not r["sig2_allowed"]


def test_normalized_book_now_variants_match():
    # JS-scan normaliseert "book-now"/"Book_now" → "book now"; de evaluatie moet
    # die als boekingang zien (Fairday's hero-knop heette "book-now.at2")
    r = evaluate_booking_entries([_entry(text="book now.at2", y=300.0, visible=True)])
    assert r["found"]


def test_nav_contact_link_does_not_count_as_booking():
    """Punt 2 (2026-07-22): een generieke 'Contact'-link in de nav mag het
    signaal NIET doden — alleen afspraak-intentie telt. Anders ruil je false
    positives in voor false negatives."""
    r = evaluate_booking_entries([
        _entry(text="contact", href="/contact", y=30.0, visible=True, in_nav=True),
        _entry(text="boek een afspraak", y=2300.0, visible=True, in_nav=False),
    ])
    assert not r["nav_booking"]        # 'contact' telt niet
    assert r["sig2_allowed"]           # diepe echte boekingang blijft geldig


def test_nav_afspraak_link_does_count_as_booking():
    r = evaluate_booking_entries([
        _entry(text="afspraak maken", href="/afspraak", y=30.0, visible=True, in_nav=True),
        _entry(text="boek een afspraak", y=2300.0, visible=True, in_nav=False),
    ])
    assert r["nav_booking"] and not r["sig2_allowed"]


def test_sig1_unaffected_by_nav_rule():
    d = _decide_signal(
        fetch_ok=True,
        booking={"value": "no_booking", "confidence": "high", "evidence": []},
        tel_present=True,
        cta=evaluate_booking_entries([]), dom_interactive_ms=500,
    )
    assert d["fired_signal"] == 1
