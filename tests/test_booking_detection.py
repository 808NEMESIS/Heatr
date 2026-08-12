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


# ── negatieve controle: geen boekoptie → moet False BLIJVEN na de fix ────────────
@pytest.mark.asyncio
async def test_centrum_osteopathie_no_booking_stays_false():
    ok, _platform = await _booking("centrum_osteopathie", "osteodenhaag.nl")
    assert ok is False
