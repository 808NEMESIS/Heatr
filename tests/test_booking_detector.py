"""
tests/test_booking_detector.py — C3-validatieset + precision/recall.

Vaste testset: native booking, externe widget/iframe, platform-links (NL),
JS-zware sites, geen booking, mislukte/lege fetch, NL-tekstvarianten.
Extra nadruk op de false-negatives uit audit V2 ('geen booking' terwijl er
wél geboekt kan worden).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from website_intelligence.booking_detector import detect_booking

_FILLER = "<p>" + ("lorem ipsum dolor sit amet " * 40) + "</p>"  # >500 chars

# ── Validatieset: (naam, html, fetch_ok, verwachte value) ───────────────────
CASES = [
    # native booking-CTA (NL)
    ("native_maak_afspraak", f'<html>{_FILLER}<a href="/afspraak">Maak een afspraak</a></html>', True, "has_booking"),
    ("native_boek_afspraak", f'<html>{_FILLER}<button>Boek een afspraak</button></html>', True, "has_booking"),
    ("native_online_boeken", f'<html>{_FILLER}<a href="/online">Online boeken</a></html>', True, "has_booking"),
    ("native_plan_consult", f'<html>{_FILLER}<a href="/c">Plan een consult</a></html>', True, "has_booking"),
    # de audit-false-negative: 'maak een afspraak' terwijl checker 'afspraak maken' zocht
    ("audit_fn_maak_afspraak", f'<html>{_FILLER}<a href="/x">maak een afspraak</a></html>', True, "has_booking"),
    # externe widget / iframe
    ("iframe_widget", f'<html>{_FILLER}<iframe src="https://widget.example/booking?id=3"></iframe></html>', True, "has_booking"),
    ("iframe_afspraak", f'<html>{_FILLER}<iframe src="https://x.nl/afspraak-plannen"></iframe></html>', True, "has_booking"),
    # platform-links (NL + internationaal)
    ("treatwell", f'<html>{_FILLER}<a href="https://treatwell.nl/salon/x">Boek</a></html>', True, "has_booking"),
    ("salonized", f'<html>{_FILLER}<script src="https://widget.salonized.com/w.js"></script></html>', True, "has_booking"),
    ("calendly", f'<html>{_FILLER}<div class="calendly-inline-widget" data-url="https://calendly.com/x"></div></html>', True, "has_booking"),
    ("doctena", f'<html>{_FILLER}<a href="https://www.doctena.nl/arts/x">Afspraak</a></html>', True, "has_booking"),
    ("onlineafspraken", f'<html>{_FILLER}<a href="https://onlineafspraken.nl/x">plan</a></html>', True, "has_booking"),
    # geen booking (echte negatieve)
    ("no_booking_contact_only", f'<html>{_FILLER}<a href="mailto:info@x.nl">Mail ons</a><a href="tel:+31">Bel</a></html>', True, "no_booking"),
    ("no_booking_plain", f'<html>{_FILLER}<h1>Welkom bij onze praktijk</h1></html>', True, "no_booking"),
    # regressie 2026-07-22: 'facebook' bevat 'book' → bare-substring gaf vals
    # has_booking op elke kliniek met een Facebook-link (hasci.nl / echthaar.nl).
    ("no_booking_facebook_link", f'<html>{_FILLER}<a href="https://www.facebook.com/kliniek">Facebook</a><a href="tel:+31">Bel</a></html>', True, "no_booking"),
    # mislukte/lege fetch → NOOIT no_booking
    ("empty_fetch", "", False, "unknown"),
    ("failed_fetch_flag", f'<html>{_FILLER}</html>', False, "unknown"),
    ("too_short_html", "<html></html>", True, "unknown"),
]


def test_validation_set_precision_recall():
    tp = fp = tn = fn = 0
    fn_details = []
    for name, html, fetch_ok, expected in CASES:
        got = detect_booking(html, fetch_ok=fetch_ok)["value"]
        # positieve klasse = 'has_booking'
        if expected == "has_booking" and got == "has_booking":
            tp += 1
        elif expected == "has_booking" and got != "has_booking":
            fn += 1
            fn_details.append(f"{name}: verwacht has_booking, kreeg {got}")
        elif expected != "has_booking" and got == "has_booking":
            fp += 1
        else:
            tn += 1
    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    print(f"\nBooking-detector: precision={precision:.2f} recall={recall:.2f} "
          f"(tp={tp} fp={fp} tn={tn} fn={fn})")
    if fn_details:
        print("FALSE NEGATIVES:", fn_details)
    # eis: geen enkele false-negative op has_booking (de audit-schade),
    # en geen false-positive (geen booking → nooit has_booking).
    assert fn == 0, f"false negatives op has_booking: {fn_details}"
    assert fp == 0, "false positive: 'geen booking' als has_booking gedetecteerd"


def test_fetch_failure_never_negative():
    """C3-kern: een mislukte fetch mag nooit 'no_booking' worden."""
    for html, ok in (("", False), ("<html>iets korts</html>", False), ("", True)):
        assert detect_booking(html, fetch_ok=ok)["value"] != "no_booking"


def test_negative_requires_successful_full_fetch():
    r = detect_booking("<html>" + ("x " * 300) + "</html>", fetch_ok=True)
    assert r["value"] == "no_booking" and r["confidence"] == "high"


def test_platform_gives_high_confidence():
    r = detect_booking('<a href="https://calendly.com/x">boek</a>' + "x" * 600, fetch_ok=True)
    assert r["value"] == "has_booking" and r["confidence"] == "high"
    assert r["platform"] == "calendly.com"


def test_social_links_not_booking():
    """Regressie: social-domeinen met 'book' in de host (facebook) mogen NOOIT
    als online-boeking tellen. Bewezen false-positive op hasci.nl/echthaar.nl."""
    for host in (
        "https://www.facebook.com/kliniek",
        "https://facebook.com/echt.haar.kliniek/",
        "https://m.facebook.com/x",
    ):
        html = f'<html>{_FILLER}<a href="{host}">Volg ons</a></html>'
        r = detect_booking(html, fetch_ok=True)
        assert r["value"] == "no_booking", f"{host} → {r}"
    # maar een echte boek-href met woordgrens moet nog steeds vuren
    for href in ("/online-booking", "/afspraak-maken", "https://booksy.com/x", "/reserveren"):
        html = f'<html>{_FILLER}<a href="{href}">Plan</a></html>'
        assert detect_booking(html, fetch_ok=True)["value"] == "has_booking", href


def test_kveg_amstelzijde_same_verdict_on_form():
    """Definitie-besluit 2026-07-22: een aanvraag-/terugbelformulier IS een online
    afspraak-ingang (request_form). KVEG (bel-mij-terug) en Amstelzijde (consult
    plannen) moeten HETZELFDE oordeel krijgen op het formulier-punt: beide
    has_booking, geen van beide no_booking. Eerder spraken ze elkaar tegen."""
    kveg = detect_booking(f'<html>{_FILLER}<p>Liever gebeld worden? Bel mij terug via het formulier.</p><a href="tel:+31">Bel</a></html>', fetch_ok=True)
    amstel = detect_booking(f'<html>{_FILLER}<a href="/consultatie-form/">Consult plannen</a><a href="tel:+31">Bel</a></html>', fetch_ok=True)
    assert kveg["value"] == "has_booking" and kveg["kind"] == "request_form"
    assert amstel["value"] == "has_booking" and amstel["kind"] == "request_form"
    assert kveg["value"] == amstel["value"]  # geen tegenspraak meer


def test_generic_contact_is_not_booking():
    """Punt 2: een GENERIEKE contactpagina/link (geen afspraak-intentie) mag NIET
    als boekingang tellen — anders vervalt elke lead."""
    for html in (
        f'<html>{_FILLER}<a href="/contact">Contact</a><a href="tel:+31">Bel ons</a></html>',
        f'<html>{_FILLER}<a href="/contact-opnemen">Neem contact op</a></html>',
        f'<html>{_FILLER}<nav><a href="/over-ons">Over ons</a><a href="/contact">Contact</a></nav></html>',
    ):
        r = detect_booking(html, fetch_ok=True)
        assert r["value"] == "no_booking", (html[:60], r)


def test_platform_is_self_booking_kind():
    r = detect_booking(f'<a href="https://salonized.com/x">Boek</a>{_FILLER}', fetch_ok=True)
    assert r["value"] == "has_booking" and r["kind"] == "self_booking"


def test_testrun_gaps_2026_07_22():
    """Regressie testrun 2026-07-22: drie bewezen detectie-gaten die elk een
    aantoonbaar ONWARE signaal-1-mail hadden opgeleverd."""
    # 1. Zenoti-boekplatform (Aever Clinics)
    r = detect_booking(f'<html>{_FILLER}<a href="https://x.zenoti.com/webstorenew/services">Boek nu</a></html>', fetch_ok=True)
    assert r["value"] == "has_booking" and r["platform"] == "zenoti.com"
    # 2. "nu boeken"-knop zonder href (Monalisa)
    r = detect_booking(f'<html>{_FILLER}<button>Nu boeken</button></html>', fetch_ok=True)
    assert r["value"] == "has_booking"
    # 3. consult-aanvraag-paden (Hairworld: /consult-aanvragen/, Amstelzijde: /consultatie-form/)
    for href, txt in (("/consult-aanvragen/", "Vrijblijvend consult aanvragen"),
                      ("/consultatie-form/", "Consult plannen")):
        r = detect_booking(f'<html>{_FILLER}<a href="{href}">{txt}</a></html>', fetch_ok=True)
        assert r["value"] == "has_booking", (href, r)
