"""
tests/test_legal_form.py — rechtsvorm-risico-gate (AVG-02).

Kernpunt: twee zelfstandige gronden voor veilig (Sami 2026-08-04):
  1. bevestigde rechtspersoon (BV/NV/stichting) — vrij benaderbaar; of
  2. een op de EIGEN SITE gepubliceerd zakelijk adres (art. 11.7 lid 3 Tw) —
     geldt óók voor eenmanszaak/VOF/onbepaald.
Zonder één van beide → geblokkeerd (fail-closed). Signaal voor (2) =
leads.email_discovery_source == 'website' (waterval-stap 1, hun eigen site).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.legal_form import classify_legal_form, receptie_avg_safe


def test_bv_is_rechtspersoon_and_safe():
    for lf in ("Besloten Vennootschap", "BV", "B.V.", "Stichting", "Coöperatie"):
        assert classify_legal_form({"kvk_legal_form": lf}) == "rechtspersoon", lf
        assert receptie_avg_safe({"kvk_legal_form": lf})[0] is True


def test_eenmanszaak_is_natuurlijk_persoon_and_blocked_without_published_addr():
    # Natuurlijk persoon ZONDER op eigen site gepubliceerd adres → geblokkeerd.
    for lf in ("Eenmanszaak", "ZZP", "VOF", "Maatschap"):
        assert classify_legal_form({"kvk_legal_form": lf}) == "natuurlijk_persoon", lf
        ok, reason = receptie_avg_safe({"kvk_legal_form": lf})
        assert ok is False and reason == "natuurlijk_persoon_geen_gepubliceerd_adres"


def test_empty_legal_form_is_onbepaald_and_blocked_without_published_addr():
    # KvK opt-in uit → geen rechtsvorm → onbepaald → geblokkeerd zonder site-adres.
    for lead in ({}, {"kvk_legal_form": None}, {"kvk_legal_form": "  "}):
        assert classify_legal_form(lead) == "onbepaald"
        ok, reason = receptie_avg_safe(lead)
        assert ok is False and reason == "rechtsvorm_onbepaald_geen_gepubliceerd_adres"


# ── Art. 11.7 lid 3: op eigen site gepubliceerd zakelijk adres (2026-08-04) ────

def test_eenmanszaak_with_published_site_email_is_safe():
    # Eenmanszaak MÉT adres van hun eigen website → art. 11.7 lid 3 → veilig.
    lead = {"kvk_legal_form": "Eenmanszaak", "email_discovery_source": "website"}
    ok, reason = receptie_avg_safe(lead)
    assert ok is True and reason == "gepubliceerd_zakelijk_adres_art_11_7_lid_3"


def test_onbepaald_with_published_site_email_is_safe():
    # Onbepaalde rechtsvorm + site-gepubliceerd adres → veilig (grond werkt
    # ongeacht rechtsvorm). Dit deblokkeert de grote onbepaald-bucket.
    lead = {"email_discovery_source": "website"}
    ok, reason = receptie_avg_safe(lead)
    assert ok is True and reason == "gepubliceerd_zakelijk_adres_art_11_7_lid_3"


def test_non_site_email_sources_do_not_unlock():
    # Alleen 'website' telt; gegokt patroon / google / kvk / leeg = GEEN grond.
    for src in ("google", "kvk", "contact_crawl", "pre_existing", "", None):
        lead = {"kvk_legal_form": "Eenmanszaak", "email_discovery_source": src}
        ok, _ = receptie_avg_safe(lead)
        assert ok is False, src


def test_rechtspersoon_safe_regardless_of_email_source():
    # Rechtspersoon is toch al vrij; reason blijft 'rechtspersoon' (niet lid 3).
    lead = {"kvk_legal_form": "BV", "email_discovery_source": "google"}
    ok, reason = receptie_avg_safe(lead)
    assert ok is True and reason == "rechtspersoon"


def test_unknown_form_string_is_onbepaald_not_optimistic():
    # onbekende string → nooit optimistisch 'rechtspersoon'.
    assert classify_legal_form({"kvk_legal_form": "iets vaags"}) == "onbepaald"
    assert receptie_avg_safe({"kvk_legal_form": "iets vaags"})[0] is False


# ── Gratis B.V.-afleiding uit naam/footer (2026-07-25, zonder KvK-API) ────────
from utils.legal_form import derive_rechtspersoon  # noqa: E402


def test_derive_rechtspersoon_from_bv_signal():
    for t in ("Skin Clinic B.V.", "Kliniek Amsterdam BV", "Beauty N.V.",
              "... besloten vennootschap ...", "Huidkliniek Holding B.V."):
        assert derive_rechtspersoon(t) == "rechtspersoon", t


def test_derive_rechtspersoon_none_when_no_signal():
    for t in ("Kliniek Dokter Frodo", "Glow Clinic Utrecht", "", None):
        assert derive_rechtspersoon(t) is None, t


def test_classify_uses_name_bv_when_no_kvk_field():
    # geen kvk_legal_form, maar 'BV' in de naam → rechtspersoon (gratis tier).
    assert classify_legal_form({"company_name": "Laser Kliniek BV"}) == "rechtspersoon"
    assert receptie_avg_safe({"company_name": "Laser Kliniek BV"})[0] is True


def test_classify_uses_footer_page_text():
    lead = {"company_name": "Mooi Kliniek"}  # geen BV in naam
    assert classify_legal_form(lead) == "onbepaald"
    assert classify_legal_form(lead, page_text="KvK 12345678 | Mooi Kliniek B.V.") == "rechtspersoon"


def test_manual_kvk_field_still_wins():
    # handmatige invoer blijft leidend: 'eenmanszaak' → geblokkeerd, ook met BV-naam.
    assert classify_legal_form({"company_name": "X BV", "kvk_legal_form": "Eenmanszaak"}) == "natuurlijk_persoon"
    assert receptie_avg_safe({"company_name": "X BV", "kvk_legal_form": "Eenmanszaak"})[0] is False
