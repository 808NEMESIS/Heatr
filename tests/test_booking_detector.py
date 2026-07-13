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
