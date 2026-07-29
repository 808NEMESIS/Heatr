"""tests/test_receptie_opener_q7.py — Q7-herframe + verplichte positieve opener.

Q7 volgt het meten-frame (de site meet/houdt niet bij wat bezoekers doen), consistent
met de detector en mail-3, NIET het receptionist-frame (dat is Q4). Mail-1 opent met één
WAAR, site-specifiek positief detail uit bestaande leaddata, nooit een verzonnen of
generieke complimentzin. Alles em-dash-vrij (QA-gate bant ze).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.hook_templates import HOOK_VARIANTS, SIGNAL_NAMES, build_positive_opener

_RECEPTIONIST_WORDS = ("niemand", "opvangt", "bereikbaar", "vangt")   # Q4's verhaal, niet Q7
_METEN_MARKERS = ("bijgehouden", "bijhoudt", "onzichtbaar", "nergens terug", "misloopt")
_GENERIC_COMPLIMENTS = ("mooie", "moderne", "prachtig", "strak", "professioneel", "fraai")


# ── Q7-uitlijning (drift-audit 2026-07-28): copy volgt de detector (meten-frame),
# NIET het receptionist-frame (dat is Q4's verhaal) ──────────────────────────
def test_q7_carries_meten_frame_not_receptionist():
    assert SIGNAL_NAMES["Q7"] == "geen_meting"
    variants = HOOK_VARIANTS["Q7"]
    assert len(variants) == 2
    for i, v in enumerate(variants):
        low = v.lower()
        assert any(w in low for w in _METEN_MARKERS), f"Q7[{i}] mist het meten-frame: {v!r}"
        assert not any(w in low for w in _RECEPTIONIST_WORDS), f"Q7[{i}] draagt nog receptionist-frame: {v!r}"
        assert "—" not in v and "–" not in v, f"Q7[{i}] bevat een em-dash"


# ── Positieve opener: ladder ─────────────────────────────────────────────────
def _lead(**kw):
    base = {"company_name": "Testkliniek", "city": "Amsterdam"}
    base.update(kw)
    return base


def test_opener_prefers_site_specific_treatment():
    op = build_positive_opener(_lead(treatment_focus=["Laserontharing", "Hydrafacial"],
                                     google_review_count=47, google_rating=4.8))
    assert op.startswith("Ik zag dat jullie")
    assert "Laserontharing" in op and "Hydrafacial" in op    # site-specifiek, geen reviews


def test_opener_falls_back_to_reputation_without_treatment():
    op = build_positive_opener(_lead(treatment_focus=None, google_review_count=49, google_rating=5.0))
    assert "49 reviews" in op and "5,0" in op and "Amsterdam" in op   # echte cijfers, niet verzonnen


def test_opener_last_resort_is_factual_not_a_compliment():
    op = build_positive_opener(_lead(treatment_focus=None, google_review_count=0, google_rating=0))
    assert "Testkliniek" in op and "Amsterdam" in op         # dichtstbijzijnd feitelijk gegeven
    assert not any(w in op.lower() for w in _GENERIC_COMPLIMENTS)


def test_opener_never_generic_compliment_in_any_tier():
    for lead in (_lead(treatment_focus=["Botox en fillers"]),
                 _lead(google_review_count=40, google_rating=4.6),
                 _lead(google_review_count=0, google_rating=0)):
        assert not any(w in build_positive_opener(lead).lower() for w in _GENERIC_COMPLIMENTS)


def test_opener_em_dash_free_across_all_tiers():
    for lead in (_lead(treatment_focus=["Botox"]),
                 _lead(google_review_count=40, google_rating=4.6),
                 _lead(google_review_count=0, google_rating=0)):
        op = build_positive_opener(lead)
        assert "—" not in op and "–" not in op


def test_opener_skips_em_dash_treatment_and_uses_reputation():
    # een behandelnaam met em-dash wordt overgeslagen → valt terug op reputatie
    op = build_positive_opener(_lead(treatment_focus=["Laser — premium"],
                                     google_review_count=47, google_rating=4.8))
    assert "—" not in op
    assert "47 reviews" in op            # overgeslagen behandeling → reputatie-fallback


def test_opener_no_numbers_invented_when_data_missing():
    # geen reviews/rating → nooit een verzonnen getal in de opener
    op = build_positive_opener(_lead(treatment_focus=None))
    assert not any(ch.isdigit() for ch in op)   # last-resort (naam+stad) draagt geen cijfers


# ── Review-ontdubbeling: hetzelfde getal nooit twee keer ─────────────────────
def test_review_number_appears_once_when_opener_cites_it():
    from config.receptie_sequence import render_receptie_mail
    lead = _lead(city="Groningen", contact_first_name="Sanne", treatment_focus=None,
                 google_review_count=49, google_rating=5.0)
    m = render_receptie_mail(1, lead, hook_code="Q7", privacy_notice="privacy",
                             unsubscribe="afmelden", seed="x")
    assert m["body"].count("49 reviews") == 1      # niet in opener én zonde-brug
    assert "En dat is zonde" in m["body"]          # zonde-brug zonder cijfers


def test_review_number_stays_in_zonde_when_opener_uses_treatment():
    from config.receptie_sequence import render_receptie_mail
    lead = _lead(city="Amsterdam", contact_first_name="Sam",
                 treatment_focus=["Laserontharing", "Hydrafacial"],
                 google_review_count=47, google_rating=4.8)
    m = render_receptie_mail(1, lead, hook_code="Q7", privacy_notice="privacy",
                             unsubscribe="afmelden", seed="y")
    assert "Laserontharing" in m["body"]           # opener = behandeling (geen reviews)
    assert "47 reviews" in m["body"]               # zonde-brug draagt de cijfers, geen overlap
