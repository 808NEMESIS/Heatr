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
    classify_booking_mechanism,
    combine_stable,
    decide_receptie_hook,
    evaluate_booking_entries,
    evaluate_coverage,
    evaluate_footer_year,
    evaluate_price_anchor,
    evaluate_selfbooking,
    evaluate_tap_targets,
    evaluate_tracking,
    price_link_in_html,
)


def _cov(**kw):
    base = dict(chat_platform=False, wa=False, messenger=False,
                self_booking=False, ambiguous_chat=False, fetch_ok=True)
    base.update(kw)
    return evaluate_coverage(**base)


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


_SELF = "self_booking"


def test_fairday_fixture_no_signal2():
    r = evaluate_booking_entries(FAIRDAY_ENTRIES)
    assert r["found"] and r["nav_booking"] and not r["sig2_allowed"]
    d = _decide_signal(
        fetch_ok=True,
        booking={"value": "has_booking", "confidence": "high", "evidence": [], "kind": _SELF},
        tel_present=False, cta=r, mechanism=_SELF, fcp_ms=400,
    )
    assert d["fired_signal"] is None, d


def test_cadance_fixture_no_signal2():
    r = evaluate_booking_entries(CADANCE_ENTRIES)
    assert r["found"] and r["nav_booking"] and not r["sig2_allowed"]
    d = _decide_signal(
        fetch_ok=True,
        booking={"value": "has_booking", "confidence": "high",
                 "evidence": ["platform:salonized.com"], "kind": _SELF},
        tel_present=True, cta=r, mechanism=_SELF, fcp_ms=440,
    )
    assert d["fired_signal"] is None, d


def test_deep_widget_clean_nav_fires_signal2():
    r = evaluate_booking_entries(DEEP_ONLY_ENTRIES)
    assert r["sig2_allowed"] and r["min_visible_y"] == 2200.0
    d = _decide_signal(
        fetch_ok=True,
        booking={"value": "has_booking", "confidence": "high", "evidence": [], "kind": _SELF},
        tel_present=True, cta=r, mechanism=_SELF, fcp_ms=400,
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
        booking={"value": "no_booking", "confidence": "high", "evidence": [], "kind": None},
        tel_present=True,
        cta=evaluate_booking_entries([]), mechanism=None, fcp_ms=500,
    )
    assert d["fired_signal"] == 1


# ── Signaal 1b — consult-only (mechanisme-classificatie) ────────────────────
def _booking(kind, val="has_booking", conf="high"):
    return {"value": val, "confidence": conf, "kind": kind, "evidence": []}


def test_mechanism_kveg_bel_mij_terug_is_request_form():
    m = classify_booking_mechanism(_booking("request_form"), [], "kveg.nl",
                                   request_only_present=True)
    assert m == "request_form"


def test_mechanism_amstelzijde_form_path_is_request_form():
    entries = [_entry(text="consult plannen", href="/consultatie-form/", y=40.0, in_nav=True)]
    m = classify_booking_mechanism(_booking("request_form"), entries, "amstelzijdekliniek.nl",
                                   request_only_present=False)
    assert m == "request_form"


def test_mechanism_fairday_hash_widget_is_ambiguous():
    # 'boek nu' met href='#' (JS-widget), geen request-only, geen form-path → ambiguous
    entries = [_entry(text="boek nu", href="#", y=52.0, in_nav=True)]
    m = classify_booking_mechanism(_booking("request_form"), entries, "fairdayclinics.com",
                                   request_only_present=False)
    assert m == "ambiguous"


def test_mechanism_external_booking_href_is_self_booking():
    entries = [_entry(text="boek nu", href="https://x.zenoti.com/webstore", y=52.0)]
    m = classify_booking_mechanism(_booking("request_form"), entries, "aeverclinics.com",
                                   request_only_present=False)
    assert m == "self_booking"


def test_signal_1b_fires_on_request_form_and_needs_review():
    d = _decide_signal(
        fetch_ok=True, booking=_booking("request_form"), tel_present=True,
        cta=evaluate_booking_entries([]), mechanism="request_form", fcp_ms=400,
    )
    assert d["fired_signal"] == "1b" and d["needs_review"] is True


def test_ambiguous_mechanism_does_not_fire_1b():
    d = _decide_signal(
        fetch_ok=True, booking=_booking("request_form"), tel_present=True,
        cta=evaluate_booking_entries([]), mechanism="ambiguous", fcp_ms=400,
    )
    assert d["fired_signal"] is None


def test_1b_suppressed_when_consult_is_proposition():
    # Amstelzijde-fixture: consult is expliciet propositie → 1b mag NIET vuren
    from website_intelligence.hook_detector import _CONSULT_PROPOSITION_RE
    amstel = "na het consult stellen we een persoonlijk behandelplan voor je op"
    assert _CONSULT_PROPOSITION_RE.search(amstel)
    d = _decide_signal(
        fetch_ok=True, booking=_booking("request_form"), tel_present=True,
        cta=evaluate_booking_entries([]), mechanism="request_form",
        consult_proposition=True, fcp_ms=400,
    )
    assert d["fired_signal"] is None
    # zonder de propositie-copy vuurt 1b wél
    d2 = _decide_signal(
        fetch_ok=True, booking=_booking("request_form"), tel_present=True,
        cta=evaluate_booking_entries([]), mechanism="request_form",
        consult_proposition=False, fcp_ms=400,
    )
    assert d2["fired_signal"] == "1b"


# ── Signaal 3/4/5 ────────────────────────────────────────────────────────────
def test_signal_3_slow_fcp():
    d = _decide_signal(fetch_ok=True, booking={"value": "no_booking", "confidence": "low", "kind": None},
                       tel_present=False, cta=evaluate_booking_entries([]),
                       mechanism=None, fcp_ms=5200)
    assert d["fired_signal"] == 3


def test_signal_4_overlapping_taps():
    tap = {"count": 20, "small": 6, "overlaps": 5}
    d = _decide_signal(fetch_ok=True, booking={"value": "no_booking", "confidence": "low", "kind": None},
                       tel_present=False, cta=evaluate_booking_entries([]),
                       mechanism=None, fcp_ms=800, tap=tap)
    assert d["fired_signal"] == 4


def test_tap_targets_social_icons_no_false_positive():
    # een paar losse kleine icoontjes, geen overlap → NIET vuren
    assert not evaluate_tap_targets({"count": 30, "small": 4, "overlaps": 0})["fires"]


def test_tap_targets_sparse_icon_menu_no_false_positive():
    # CadanCe-geval: 9/9 klein maar 1 overlap en te weinig elementen → NIET vuren
    assert not evaluate_tap_targets({"count": 9, "small": 9, "overlaps": 1})["fires"]
    # wél vuren bij een pagina dicht vol met te kleine targets
    assert evaluate_tap_targets({"count": 20, "small": 14, "overlaps": 1})["fires"]


def test_signal_5_stale_footer():
    assert evaluate_footer_year(2021, 2026)["fires"]
    assert not evaluate_footer_year(2024, 2026)["fires"]
    assert not evaluate_footer_year(None, 2026)["fires"]
    d = _decide_signal(fetch_ok=True, booking={"value": "no_booking", "confidence": "low", "kind": None},
                       tel_present=False, cta=evaluate_booking_entries([]),
                       mechanism=None, fcp_ms=800, tap={"count": 5, "small": 0, "overlaps": 0},
                       footer=evaluate_footer_year(2020, 2026))
    assert d["fired_signal"] == 5 and d.get("jaar") == 2020


def test_ladder_order_sig1_beats_all():
    # geen boeking + tel → signaal 1, ook al is de site traag
    d = _decide_signal(fetch_ok=True, booking={"value": "no_booking", "confidence": "high", "kind": None},
                       tel_present=True, cta=evaluate_booking_entries([]),
                       mechanism=None, fcp_ms=9000, footer=evaluate_footer_year(2019, 2026))
    assert d["fired_signal"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Q7 — meting/tracking-detector (tri-state, fail-closed). Bekende hit + geen-hit
# + onbepaald als fixtures; de onbepaald-regel is de C9-les die Sami eiste.
# ─────────────────────────────────────────────────────────────────────────────

def test_q7_geen_hit_when_analytics_loads():
    # bekende NIET-hit: GA/GTM laadt → de site meet wél.
    r = evaluate_tracking(["https://www.googletagmanager.com/gtm.js?id=GTM-XYZ"],
                          "<html><body>kliniek</body></html>",
                          consent_accepted=True, fetch_ok=True)
    assert r["state"] == "geen" and "analytics" in r["found"]


def test_q7_geen_hit_when_meta_pixel_loads():
    r = evaluate_tracking(["https://connect.facebook.net/en_US/fbevents.js"], "",
                          consent_accepted=True, fetch_ok=True)
    assert r["state"] == "geen" and "pixel" in r["found"]


def test_q7_hit_when_no_tracking_and_no_cmp():
    # bekende HIT: geen analytics, geen pixel, geen consent-manager → site meet niets.
    r = evaluate_tracking(["https://kliniek.nl/style.css", "https://kliniek.nl/logo.png"],
                          "<html><body>welkom</body></html>",
                          consent_accepted=False, fetch_ok=True)
    assert r["state"] == "hit"


def test_q7_onbepaald_when_cmp_present_and_not_accepted():
    # consent-manager aanwezig + niet geaccepteerd → tracking kan post-consent
    # laden → nooit als hit, altijd onbepaald (fail-closed, F2).
    r = evaluate_tracking(["https://kliniek.nl/app.js"],
                          "<script src='https://consent.cookiebot.com/uc.js'></script>",
                          consent_accepted=False, fetch_ok=True)
    assert r["state"] == "onbepaald"


def test_q7_hit_when_cmp_present_but_accepted_and_still_nothing():
    # wél geaccepteerd én daarna nog steeds geen tracking → hit mag (sterk bewijs).
    r = evaluate_tracking(["https://kliniek.nl/app.js"],
                          "<script src='https://consent.cookiebot.com/uc.js'></script>",
                          consent_accepted=True, fetch_ok=True)
    assert r["state"] == "hit"


def test_q7_onbepaald_when_fetch_failed():
    r = evaluate_tracking([], "", consent_accepted=True, fetch_ok=False)
    assert r["state"] == "onbepaald"


# ─────────────────────────────────────────────────────────────────────────────
# P1 — prijsanker-detector (tri-state, fail-closed).
# ─────────────────────────────────────────────────────────────────────────────

def test_p1_geen_hit_when_euro_amount_on_page():
    r = evaluate_price_anchor({"euro_amount": True}, fetch_ok=True)
    assert r["state"] == "geen"


def test_p1_geen_hit_when_price_page_linked():
    # tarieven-/prijzen-/prices-pagina gelinkt (waar dan ook) → prijsanker bestaat.
    r = evaluate_price_anchor({"euro_amount": False, "price_page_link": True}, fetch_ok=True)
    assert r["state"] == "geen"


def test_p1_hit_when_no_price_anywhere():
    r = evaluate_price_anchor({"euro_amount": False, "price_page_link": False,
                               "service_unchecked": False, "price_teaser_no_amount": False},
                              fetch_ok=True)
    assert r["state"] == "hit"


def test_p1_onbepaald_when_service_page_unchecked():
    # steekproef-les (allure/salonized): prijs stond op de /services-pagina, niet op
    # de home. Een ongecontroleerde diensten-/behandelpagina → onbepaald, nooit hit.
    r = evaluate_price_anchor({"euro_amount": False, "price_page_link": False,
                               "service_unchecked": True}, fetch_ok=True)
    assert r["state"] == "onbepaald"


def test_p1_onbepaald_when_price_teaser_without_amount():
    # 'Bekijk tarieven'-accordion zonder zichtbaar bedrag → prijs mogelijk achter
    # interactie → onbepaald, nooit hit.
    r = evaluate_price_anchor({"euro_amount": False, "price_page_link": False,
                               "service_unchecked": False, "price_teaser_no_amount": True},
                              fetch_ok=True)
    assert r["state"] == "onbepaald"


def test_p1_onbepaald_when_fetch_failed():
    r = evaluate_price_anchor({"euro_amount": False, "price_page_link": False}, fetch_ok=False)
    assert r["state"] == "onbepaald"


def test_price_link_in_html_raw_fallback():
    # steekproef-les (thebodyshaper): /tarieven/-link zat in de GESERVEERDE HTML
    # maar niet in de mobiele render → raw-HTML-vangnet moet 'm alsnog vinden.
    assert price_link_in_html('<a href="https://x.nl/tarieven/">Tarieven</a>')
    assert price_link_in_html('<nav><a href="/prijzen">Prijzen</a></nav>')
    assert price_link_in_html('<a href="/en/pricing">Prices</a>')
    assert not price_link_in_html('<a href="/contact">Contact</a>')
    assert not price_link_in_html("")


# ─────────────────────────────────────────────────────────────────────────────
# Q4 — dekking buiten kantooruren (async-kanaal). Elk gevonden kanaal = geen hit.
# ─────────────────────────────────────────────────────────────────────────────

def test_q4_hit_when_no_channel_at_all():
    assert _cov()["state"] == "hit"


def test_q4_geen_when_self_booking():
    assert _cov(self_booking=True)["state"] == "geen"


def test_q4_geen_when_chat_platform():
    assert _cov(chat_platform=True)["state"] == "geen"


def test_q4_geen_when_whatsapp():
    assert _cov(wa=True)["state"] == "geen"


def test_q4_geen_when_messenger():
    assert _cov(messenger=True)["state"] == "geen"


def test_q4_onbepaald_when_ambiguous_chat_only():
    # chat-marker zonder herkend platform (laadt mogelijk na interactie) → onbepaald.
    assert _cov(ambiguous_chat=True)["state"] == "onbepaald"


def test_q4_geen_beats_ambiguous_when_real_channel_present():
    # een herkend kanaal wint van de ambigue marker → geen, niet onbepaald.
    assert _cov(wa=True, ambiguous_chat=True)["state"] == "geen"


def test_q4_onbepaald_when_fetch_failed():
    assert _cov(ambiguous_chat=False, fetch_ok=False)["state"] == "onbepaald"


# ─────────────────────────────────────────────────────────────────────────────
# Receptie-ladder-beslissing Q4→Q7→Q2→P1 + Q4-formulier-gate + mail-2 tweede haak.
# ─────────────────────────────────────────────────────────────────────────────

def test_ladder_q4_wins_when_form_present():
    # Q4 vuurt (⊆ Q2, dus Q2 ook), formulier aanwezig → mail-1-haak = Q4.
    r = decide_receptie_hook("hit", "hit", "hit", "hit", form_present=True)
    assert r["hook_code"] == "Q4"
    assert r["hook_ladder"] == ["Q4", "Q7", "Q2", "P1"]
    # mail-2 = eerstvolgende haak uit ÁNDER thema (niet Q2=boeken) → Q7 (meten).
    assert r["second_hook"] == "Q7"
    assert r["q4_gated"] is False


def test_ladder_q4_gated_without_form_falls_through():
    # Q4 vuurt maar geen formulier → q4_gated, val door naar Q7.
    r = decide_receptie_hook("hit", "hit", "hit", "hit", form_present=False)
    assert r["hook_code"] == "Q7" and r["q4_gated"] is True
    assert "Q4" not in r["hook_ladder"]


def test_ladder_skips_geen_and_onbepaald():
    # alleen Q2 is een hit; Q7 onbepaald, P1 geen → mail-1 = Q2, geen tweede haak.
    r = decide_receptie_hook("geen", "onbepaald", "hit", "geen", form_present=True)
    assert r["hook_code"] == "Q2" and r["hook_ladder"] == ["Q2"]
    assert r["second_hook"] is None


def test_ladder_no_hits_is_not_mailable():
    r = decide_receptie_hook("geen", "geen", "geen", "geen", form_present=True)
    assert r["hook_code"] is None and r["hook_ladder"] == []


def test_ladder_second_hook_must_be_distinct_theme():
    # mail-1 = Q7 (meten); Q2 (boeken) en P1 (prijs) vuren ook → tweede haak = Q2
    # (eerste uit ander thema in ladder-volgorde: Q2 vóór P1).
    r = decide_receptie_hook("geen", "hit", "hit", "hit", form_present=True)
    assert r["hook_code"] == "Q7"
    assert r["second_hook"] == "Q2"


# ─────────────────────────────────────────────────────────────────────────────
# Q2 — 'alleen aanvraag, geen zelf-boeken' op het canonieke booking-mechanisme.
# ─────────────────────────────────────────────────────────────────────────────

def test_q2_geen_when_self_booking():
    assert evaluate_selfbooking("self_booking", form_present=True, fetch_ok=True)["state"] == "geen"


def test_q2_hit_when_request_form():
    assert evaluate_selfbooking("request_form", form_present=True, fetch_ok=True)["state"] == "hit"


def test_q2_onbepaald_when_ambiguous():
    assert evaluate_selfbooking("ambiguous", form_present=True, fetch_ok=True)["state"] == "onbepaald"


def test_q2_hit_when_no_booking_but_form_present():
    assert evaluate_selfbooking(None, form_present=True, fetch_ok=True)["state"] == "hit"


def test_q2_onbepaald_when_no_booking_and_no_form():
    # geen boeking én geen formulier → 'alleen aanvraag' houdt geen stand → onbepaald.
    assert evaluate_selfbooking(None, form_present=False, fetch_ok=True)["state"] == "onbepaald"


def test_q2_onbepaald_when_fetch_failed():
    assert evaluate_selfbooking("request_form", form_present=True, fetch_ok=False)["state"] == "onbepaald"


# ─────────────────────────────────────────────────────────────────────────────
# 2-run-stabiliteit: alleen identieke hit/geen over beide runs telt (fail-closed).
# ─────────────────────────────────────────────────────────────────────────────

def test_combine_stable_agreeing_hit():
    assert combine_stable("hit", "hit") == "hit"


def test_combine_stable_agreeing_geen():
    assert combine_stable("geen", "geen") == "geen"


def test_combine_stable_disagreement_becomes_onbepaald():
    assert combine_stable("hit", "geen") == "onbepaald"


def test_combine_stable_any_onbepaald_becomes_onbepaald():
    assert combine_stable("hit", "onbepaald") == "onbepaald"
    assert combine_stable("onbepaald", "geen") == "onbepaald"
