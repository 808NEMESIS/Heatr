"""tests/test_booking_detection.py — has_online_booking op echte NL-boekpatronen.

Fixtures = de echte opgehaalde HTML van de vier foutgevallen uit het meetrapport
(detector gaf False terwijl er aantoonbaar een boekoptie in de HTML staat) plus één
negatieve controle die False MOET blijven. TDD: vóór de detector-uitbreiding falen de
vier True-verwachtingen; de controle slaagt al.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from website_intelligence.conversion_checker import check_conversion

_FIX = Path(__file__).resolve().parent / "fixtures" / "booking"


def _html(slug: str) -> str:
    return (_FIX / f"{slug}.html").read_text()


async def _booking(slug: str, domain: str):
    r = await check_conversion(domain, _html(slug), "alternatieve_geneeskunde")
    return r.get("has_online_booking"), r.get("booking_platform")


# ── de vier foutgevallen: boekoptie staat in de HTML → moet True worden ──────────
@pytest.mark.asyncio
async def test_yosay_clientomgeving_subdomain_booking():
    # href naar {praktijk}.clientomgeving.nl/afspraak-maken
    ok, platform = await _booking("yosay", "yosayacupunctuur.nl")
    assert ok is True and platform


@pytest.mark.asyncio
async def test_osteovisie_crossuite_platform():
    # agenda.crossuite.com — NL osteo/paramedisch boekplatform
    ok, platform = await _booking("osteovisie", "osteovisie.nl")
    assert ok is True and platform and "crossuite" in platform.lower()


@pytest.mark.asyncio
async def test_kliniekvinci_selfhosted_boeken_page():
    # href="/boeken/" — self-hosted boekpagina (geen platform, wél boekoptie)
    ok, platform = await _booking("kliniekvinci", "kliniekvinci.nl")
    assert ok is True and platform


@pytest.mark.asyncio
async def test_artsenpraktijkleef_selfhosted_afspraak_inplannen():
    # href="/direct-afspraak-inplannen/"
    ok, platform = await _booking("artsenpraktijkleef", "artsenpraktijkleef.nl")
    assert ok is True and platform


# ── regressie (stap 3): href="/nl/online-boeken" — patroon, geen exacte "/boeken" ─
@pytest.mark.asyncio
async def test_doctors_at_soap_online_boeken_url():
    ok, platform = await _booking("doctors_at_soap", "atsoap.com")
    assert ok is True and platform


# ── handcheck-gaten (Sami 2026-08-13): drie gemiste boekopties ───────────────────
@pytest.mark.asyncio
async def test_jeannette_hessels_clinicminds_platform():
    # schedule.clinicminds.com 3× op de homepage + "Boek online een afspraak"
    ok, platform = await _booking("jeannette_hessels", "jeannettehessels.nl")
    assert ok is True and platform and "clinicminds" in platform.lower()


@pytest.mark.asyncio
async def test_midi_maak_afspraak_without_article():
    # <a class="menu-link">Maak Afspraak</a> — zonder lidwoord
    ok, platform = await _booking("midi", "midi-acupunctuur.nl")
    assert ok is True and platform


@pytest.mark.asyncio
async def test_beautyslim_maak_een_afspraak_url():
    # href="/maak-een-afspraak/" + knoptekst (fusion-theme, mogelijk geneste markup)
    ok, platform = await _booking("beautyslim", "beautyslim.nl")
    assert ok is True and platform


# ── wa.me als boekpad-VELD (apart van has_online_booking; Osteopathie Anna) ──────
@pytest.mark.asyncio
async def test_osteopathie_anna_whatsapp_link_field():
    r = await check_conversion("osteopathie-anna.nl", _html("osteopathie_anna"),
                               "alternatieve_geneeskunde")
    assert r.get("has_whatsapp_link") is True          # wa.me/31616534508 onder de hero
    # wa.me is een boekPAD maar geen online-boeksysteem: flipt has_online_booking NIET
    assert "wa.me" not in str(r.get("booking_platform") or "").lower()


# ── negatieve controles: mogen NIET omslaan als de detector verruimt ─────────────
@pytest.mark.asyncio
async def test_centrum_osteopathie_no_booking_stays_false():
    ok, _platform = await _booking("centrum_osteopathie", "osteodenhaag.nl")
    assert ok is False


@pytest.mark.asyncio
async def test_laarman_button_to_contact_counts_as_booking():
    # <a class="ui builder_button">Maak een afspraak</a> → /contact/ — structureel
    # identiek aan MIDI die Sami hand-fout verklaarde: de claim gaat over "een knop
    # om een afspraak te maken", ongeacht waar die heen leidt. Dus True. (Correctie
    # 2026-08-14: eerder onterecht als oude-vals-positief geklasseerd.)
    ok, platform = await _booking("laarman", "osteopathielaarman.nl")
    assert ok is True and platform


@pytest.mark.asyncio
async def test_loose_sentence_text_stays_false():
    # Zuivere guardrail: "afspraak maken" in lopende tekst zonder knop → False.
    html = ("<html><body><p>U kunt telefonisch een afspraak maken via 010-1234567. "
            "Wij zijn bereikbaar op werkdagen.</p><a href='/contact/'>Contact</a>"
            "</body></html>")
    r = await check_conversion("voorbeeld.nl", html, "alternatieve_geneeskunde")
    assert r.get("has_online_booking") is False


# ── telefoon-klikbaar: 0a-recall op patroonvarianten (2026-08-29) ────────────────
@pytest.mark.asyncio
async def test_phone_clickable_pattern_variants():
    base = "<html><body>" + "x" * 400 + "{link}<form><input name='n'></form></body></html>"
    for link in ('<a href="tel:0101234567">bel</a>',
                 "<a href='tel:+31612345678'>bel</a>",
                 '<a href = "tel:010-123">bel</a>',          # spaties rond =
                 '<a href=tel:0101234567>bel</a>',            # ongequote
                 '<a href="callto:0101234567">bel</a>',       # callto-schema
                 '<a HREF="TEL:0101234567">bel</a>'):         # hoofdletters
        r = await check_conversion("x.nl", base.format(link=link), "alternatieve_geneeskunde")
        assert r["has_phone_clickable"] is True, link


@pytest.mark.asyncio
async def test_phone_text_only_is_not_clickable():
    # nummer als kale tekst (of 'tel:' in lopende tekst) is GEEN klikbare link —
    # dit is de afwezigheid waarop de mail-2-claim mag leunen.
    html = ("<html><body>" + "x" * 400 +
            "<p>Bel ons op 010-1234567 of via tel: 0612345678</p>"
            "<a href='/contact'>contact</a></body></html>")
    r = await check_conversion("x.nl", html, "alternatieve_geneeskunde")
    assert r["has_phone_clickable"] is False
