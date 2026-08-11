"""tests/test_frictie_copywriter.py — Frame A/C copy-engine + geautomatiseerde zelfcontrole.

Bewijst dat de engine (heatr-copy skill) gegronde, frictie-gebaseerde mails bouwt die
de 10-punts zelfcontrole passeren, dat de vers-gate een oude frictieclaim tegenhoudt,
en dat de woordteller telt wat we denken (transparantie-eis Sami 2026-08-11)."""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from campaigns.frictie_copywriter import (
    _pitch, build_frictie_mail1, build_frictie_mail2, build_frictie_mail3, build_kale_ask_mail1,
    copy_selfcheck, friction_reverified_today, is_friction_fresh, niche_for_sector,
    render_frictie_mail, select_leak, select_second_finding,
)

PRIV = "Je ontvangt deze mail omdat je praktijk openbaar vindbaar is; zie aeryssolution.nl/privacy"
NOW = dt.date(2026, 8, 11)
FRESH = "2026-08-01"      # 10 dagen oud
STALE = "2026-01-01"      # ruim over 45 dagen
COSM = {"id": "l1", "company_name": "Skin8", "domain": "https://skin8.nl",
        "sector": "cosmetische_behandelaars", "contact_first_name": "Chris"}
ALT = {"id": "l2", "company_name": "Natuurpraktijk Jacobs", "domain": "natuurpraktijk-jacobs.nl",
       "sector": "alternatieve_geneeskunde", "contact_first_name": "Najat"}
BOOK_LEAK = {"has_online_booking": False}


def _kw(**over):
    base = dict(privacy_notice=PRIV, unsubscribe="", warmr_owns_unsubscribe=True)
    base.update(over)
    return base


# ── select_leak: alleen geen-boeking ─────────────────────────────────────────
def test_select_leak_booking_only():
    assert select_leak({"has_online_booking": False})[0] == "has_online_booking"
    # telefoon/CTA-frictie geeft bewust None (eigen frame nodig — logische dekking)
    assert select_leak({"has_online_booking": True, "has_phone_clickable": False}) is None
    assert select_leak({"has_online_booking": True, "has_cta_above_fold": False}) is None
    assert select_leak({}) is None


def test_select_leak_none_when_reverify_uncertain():
    # Onzekere herverificatie (twee fetches oneens/fout) → geen frictieclaim.
    assert select_leak({"has_online_booking": False, "reverify_uncertain": True}) is None


def test_niche_mapping():
    assert niche_for_sector("cosmetische_behandelaars") == "cosmetisch"
    assert niche_for_sector("alternatieve_geneeskunde") == "alt"
    assert niche_for_sector("chiropractoren") == "alt"


# ── vers-gate ────────────────────────────────────────────────────────────────
def test_is_friction_fresh():
    assert is_friction_fresh(FRESH, max_age_days=45, now=NOW) is True
    assert is_friction_fresh(STALE, max_age_days=45, now=NOW) is False
    assert is_friction_fresh(None, max_age_days=45, now=NOW) is False        # onbekend = niet vers
    assert is_friction_fresh("rommel", max_age_days=45, now=NOW) is False


# ── Frame A ──────────────────────────────────────────────────────────────────
def test_frame_a_cosmetic():
    out = build_frictie_mail1(COSM, niche="cosmetisch", leak=select_leak(BOOK_LEAK), **_kw())
    assert out is not None and out["frame"] == "A" and out["subject"] == "Skin8 op mobiel"
    b = out["body"]
    assert b.startswith("Hoi Chris,")
    assert "Wie wil boeken, moet bellen." in b                 # frictie-leak
    assert "die níet bellen" in b                              # tegenwerping vooraf weg
    assert "hoeft op te nemen" in b                            # uitkomst = concreet beeld
    assert 'Antwoord met "ja"' in b                            # CTA = exacte handeling
    assert "één tik" not in b                                  # geen feature-uitkomst meer
    sc = copy_selfcheck(b, subject=out["subject"], niche="cosmetisch", domain="skin8.nl",
                        name="Skin8", frame="A", privacy_notice=PRIV, unsubscribe="")
    assert sc["passed"], sc["detail"]
    assert sc["words"] <= 160


def test_frame_a_alt_logic_and_no_growth():
    out = build_frictie_mail1(ALT, niche="alt", leak=select_leak(BOOK_LEAK), **_kw())
    assert out is not None
    b = out["body"]
    assert "hoogste drempel" in b and "op de pagina" in b
    assert "\n\nJij ziet dat niet gebeuren.\n\n" in b       # punchline staat alleen
    low = b.lower()
    assert not any(w in low for w in ("buurpraktijk", "concurrent", "groei", "meer aanvragen"))
    sc = copy_selfcheck(b, subject=out["subject"], niche="alt", domain="natuurpraktijk-jacobs.nl",
                        name="Natuurpraktijk Jacobs", frame="A", privacy_notice=PRIV, unsubscribe="")
    assert sc["passed"], sc["detail"]
    assert sc["words"] <= 160


def test_frame_a_blocks_on_missing_privacy():
    assert build_frictie_mail1(COSM, niche="cosmetisch", leak=select_leak(BOOK_LEAK),
                               **_kw(privacy_notice="")) is None


def test_frame_a_blocks_dirty_company_name():
    lead = {**COSM, "company_name": "Skin8 de allerbeste botox kliniek van Nederland"}
    assert build_frictie_mail1(lead, niche="cosmetisch", leak=select_leak(BOOK_LEAK), **_kw()) is None


def test_frame_a_blocks_without_domain():
    lead = {"id": "x", "company_name": "Skin8", "sector": "cosmetische_behandelaars"}
    assert build_frictie_mail1(lead, niche="cosmetisch", leak=select_leak(BOOK_LEAK), **_kw()) is None


# ── Frame C ──────────────────────────────────────────────────────────────────
def test_frame_c_passes_selfcheck():
    out = build_kale_ask_mail1(COSM, niche="cosmetisch", **_kw())
    assert out is not None and out["frame"] == "C"
    assert out["subject"] == "gratis ontwerp voor Skin8"
    sc = copy_selfcheck(out["body"], subject=out["subject"], niche="cosmetisch", domain="skin8.nl",
                        name="Skin8", frame="C", privacy_notice=PRIV, unsubscribe="")
    assert sc["passed"], sc["detail"]


# ── render_frictie_mail: frame-keuze + vers-gate ─────────────────────────────
def test_render_picks_a_with_fresh_friction():
    out = render_frictie_mail(COSM, sector="cosmetische_behandelaars", conversion_details=BOOK_LEAK,
                              analyzed_at=FRESH, now=NOW, **_kw())
    assert out["frame"] == "A" and out["stale_friction"] is False and out["selfcheck"]["passed"]


def test_render_downgrades_to_c_when_friction_stale():
    out = render_frictie_mail(COSM, sector="cosmetische_behandelaars", conversion_details=BOOK_LEAK,
                              analyzed_at=STALE, now=NOW, **_kw())
    assert out["frame"] == "C" and out["stale_friction"] is True and out["selfcheck"]["passed"]


def test_render_downgrades_to_c_when_analyzed_at_unknown():
    out = render_frictie_mail(COSM, sector="cosmetische_behandelaars", conversion_details=BOOK_LEAK,
                              analyzed_at=None, now=NOW, **_kw())
    assert out["frame"] == "C" and out["stale_friction"] is True


def test_render_falls_back_to_c_without_friction():
    out = render_frictie_mail(COSM, sector="cosmetische_behandelaars",
                              conversion_details={"has_online_booking": True},
                              analyzed_at=FRESH, now=NOW, **_kw())
    assert out["frame"] == "C" and out["stale_friction"] is False


# ── woordteller-transparantie (Sami's calibratie-eis) ────────────────────────
def test_pitch_counts_greeting_through_cta_excluding_boilerplate():
    out = build_frictie_mail1(COSM, niche="cosmetisch", leak=select_leak(BOOK_LEAK), **_kw())
    pitch = _pitch(out["body"], PRIV, "")
    assert pitch.startswith("Hoi Chris,")                      # begroeting telt mee
    assert 'werkdagen.' in pitch and pitch.rstrip().endswith("werkdagen.")  # t/m CTA
    assert "Sami Jansema" not in pitch and "aeryssolution.nl/privacy" not in pitch  # boilerplate eruit
    sc = copy_selfcheck(out["body"], subject=out["subject"], niche="cosmetisch", domain="skin8.nl",
                        name="Skin8", frame="A", privacy_notice=PRIV, unsubscribe="")
    assert sc["words"] == len(pitch.split())                   # gerapporteerd getal == echte telling


# ── zelfcontrole vangt drift ─────────────────────────────────────────────────
def test_selfcheck_flags_fabricated_number():
    body = "Hallo,\n\n40% haakt af.\n\nZal ik 'm maken?"
    assert 1 in copy_selfcheck(body, subject="x", niche="cosmetisch", frame="C")["fails"]


def test_selfcheck_flags_taste():
    body = "Hallo,\n\nJe site ziet er uit als 2010 met generieke stockfoto's.\n\nklaar."
    assert 3 in copy_selfcheck(body, subject="x", niche="cosmetisch", frame="C")["fails"]


def test_selfcheck_flags_blacklist():
    body = "Hallo,\n\nDe basis zit goed. Nergens aan vast.\n\nklaar."
    assert 5 in copy_selfcheck(body, subject="x", niche="cosmetisch", frame="C")["fails"]


def test_selfcheck_flags_growth_to_alt():
    body = "Hallo,\n\nJe wint 'm van de buurpraktijk en krijgt meer aanvragen.\n\nklaar."
    assert 9 in copy_selfcheck(body, subject="x", niche="alt", frame="C")["fails"]


def test_selfcheck_flags_cold_loom():
    body = "Hallo,\n\nIn de bijlage een Loom: https://loom.com/abc.\n\nklaar."
    assert 10 in copy_selfcheck(body, subject="x", niche="cosmetisch", frame="C")["fails"]


def test_selfcheck_flags_subject_too_long():
    out = build_kale_ask_mail1(COSM, niche="cosmetisch", **_kw())
    sc = copy_selfcheck(out["body"], subject="dit onderwerp is echt veel te lang geworden nu",
                        niche="cosmetisch", domain="skin8.nl", name="Skin8", frame="C",
                        privacy_notice=PRIV, unsubscribe="")
    assert 4 in sc["fails"]


# ── vandaag-gate + tweede vondst + mail 2/3 ──────────────────────────────────
def test_friction_reverified_today():
    assert friction_reverified_today({"checked_at": "2026-08-11T09:00:00+00:00"}, now=NOW) is True
    assert friction_reverified_today({"checked_at": "2026-08-01"}, now=NOW) is False
    assert friction_reverified_today({}, now=NOW) is False


def test_select_second_finding():
    assert select_second_finding({"has_phone_clickable": False})[0] == "has_phone_clickable"
    assert select_second_finding({"has_phone_clickable": True, "form_field_count": 7})[0] == "form_field_count"
    # alleen generieke vondsten (CTA/WhatsApp/chatbot) → None
    assert select_second_finding({"has_cta_above_fold": False, "has_whatsapp": False}) is None
    assert select_second_finding({"has_phone_clickable": False, "reverify_uncertain": True}) is None


def test_build_frictie_mail2_proof():
    second = select_second_finding({"has_phone_clickable": False})
    out = build_frictie_mail2(ALT, niche="alt", second=second, **_kw())
    assert out is not None and out["frame"] == "2"
    assert "niet aantikken" in out["body"]                    # concrete tweede vondst
    assert 'Antwoord met "ja"' in out["body"]                 # zelfde ask
    assert out["selfcheck"]["passed"], out["selfcheck"]["detail"]


def test_build_frictie_mail3_shrink_ask():
    out = build_frictie_mail3(COSM, niche="cosmetisch", **_kw())
    assert out is not None and out["frame"] == "3"
    assert "stuur maar" in out["body"] and out["body"].count("?") == 0
    assert out["selfcheck"]["passed"], out["selfcheck"]["detail"]
