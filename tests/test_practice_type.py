"""tests/test_practice_type.py — praktijktype-indicatie (fit-weging, geen gate).

Fixtures = de echte testrun-gevallen van 2026-07-22 waar het onderscheid
zichtbaar had moeten zijn (Sami's live-check)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from website_intelligence.practice_type import (
    assess_practice_size,
    classify_practice_type,
    suggest_aanhef_register,
)


AMSTEL_TEXT = ("de kliniek, opgericht in 2009 door dokter hayri hortoglu. "
               "ons team van specialisten heeft jarenlange ervaring; dankzij ons "
               "multidisciplinaire team stellen we een persoonlijk behandelplan op.")


def test_amstelzijde_is_established_player():
    r = assess_practice_size(AMSTEL_TEXT, current_year=2026)
    assert r["likely_established_player"]
    assert "team_specialisten" in r["signals"] and "lang_bestaand" in r["signals"]
    assert r["established_year"] == 2009


def test_small_solo_practice_not_flagged_established():
    r = assess_practice_size("kleine praktijk voor huidverbetering in utrecht", 2026)
    assert not r["likely_established_player"]


def test_register_formeel_on_title():
    r = suggest_aanhef_register("opgericht door dokter Hayri Hortoglu")
    assert r["register"] == "formeel" and r["title"]
    r2 = suggest_aanhef_register("Petra, eigenaresse")
    assert r2["register"] == "informeel"


def test_cadance_is_huidinstituut():
    r = classify_practice_type(
        "CadanCe Huidinstituut Den Haag",
        "CadanCe Huidinstituut Den Haag biedt professionele huidverzorgingsbehandelingen",
        "info@cadancehuidinstituut.nl")
    assert r["practice_type"] == "salon_huidinstituut"
    assert not r["flags"]


def test_natural_beauty_is_salon_op_gmail():
    r = classify_practice_type(
        "Natural Beauty Salon",
        "Natural Beauty Salon is een schoonheidssalon in Den Haag die "
        "verzorgingsbehandelingen aanbiedt",
        "petranederhoed@gmail.com")
    assert r["practice_type"] == "salon_huidinstituut"
    assert any("freemail" in f for f in r["flags"])


def test_kveg_is_medisch_esthetisch():
    r = classify_practice_type(
        "Kliniek voor Esthetische Geneeskunde",
        "Deze kliniek biedt esthetische medische behandelingen voor patiënten",
        "info@kveg.nl")
    assert r["practice_type"] == "medisch_esthetisch"
    assert not r["flags"]


def test_gemengd_profiel_zichtbaar():
    # kliniek-naam met salon-aanbod → gemengd, niet stil "kliniek"
    r = classify_practice_type(
        "Be Clinic", "Be Clinic biedt medische en cosmetische behandelingen en "
        "gezichtsbehandelingen", "info@beclnc.com")
    assert r["practice_type"] == "gemengd"


def test_salonized_not_matched_as_salon():
    r = classify_practice_type(
        "X Kliniek", "boekingen lopen via salonized.com widget met botox aanbod",
        "info@xkliniek.nl")
    assert r["practice_type"] == "medisch_esthetisch", r
