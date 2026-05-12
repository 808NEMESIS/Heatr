"""tests/test_sector_impact.py — pick_sector_impact_frame mapping + default.

Eén test per unieke return-string + default fallback + DB-sector-alias verificatie.
"""
from __future__ import annotations

from utils.sector_impact import pick_sector_impact_frame


def test_cosmetisch_returns_patient_frase():
    assert pick_sector_impact_frame("cosmetisch") == "patiënten goed verder helpt"
    assert pick_sector_impact_frame("zorg_welzijn") == "patiënten goed verder helpt"


def test_alternatieve_zorg_returns_herstel_frase():
    assert pick_sector_impact_frame("alternatieve_zorg") == "mensen ondersteunt in hun herstel"
    assert pick_sector_impact_frame("lichaamswerk_pragmatisch") == "mensen ondersteunt in hun herstel"


def test_techniek_ambacht_returns_vakwerk_frase():
    assert pick_sector_impact_frame("techniek_ambacht") == "vakwerk levert waar mensen op rekenen"


def test_zakelijke_dienstverlening_returns_ondernemers_frase():
    assert pick_sector_impact_frame("zakelijke_dienstverlening") == (
        "ondernemers verder helpt met vraagstukken die ertoe doen"
    )


def test_db_sector_keys_resolve_correctly():
    """Live heatr_leads.sector-waardes (niet de generieke user-spec keys) moeten
    matchen via toegevoegde DB-aliases. Anders triggert mapping nooit op echte data.
    """
    assert pick_sector_impact_frame("cosmetische_behandelaars") == "patiënten goed verder helpt"
    assert pick_sector_impact_frame("alternatieve_geneeskunde") == "mensen ondersteunt in hun herstel"


def test_unknown_or_none_returns_default():
    expected = "waarde levert aan jullie klanten"
    assert pick_sector_impact_frame(None) == expected
    assert pick_sector_impact_frame("") == expected
    assert pick_sector_impact_frame("does_not_exist") == expected
    assert pick_sector_impact_frame("makelaars") == expected  # bestaat in DB maar niet in mapping
