"""tests/test_lead_selection.py — selectie-uitsluiting (beroepsvereniging / afgeleide naam)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.lead_selection import selection_exclusion


def test_generic_association_keyword():
    assert selection_exclusion({"company_name": "Koninklijke Vereniging Homeopathie Nederland"}) == "beroepsvereniging"
    assert selection_exclusion({"company_name": "Koepel Alternatieve Zorg"}) == "beroepsvereniging"


def test_explicit_acronym_association():
    # 'De AVIG' bevat geen verenigings-woord → moet expliciet worden gevangen.
    assert selection_exclusion({"company_name": "De AVIG"}) == "beroepsvereniging"


def test_explicit_derived_names():
    assert selection_exclusion({"company_name": "Ping Shu Yuan"}) == "voornaam_uit_bedrijfsnaam"
    assert selection_exclusion({"company_name": "Cosmetic Heroes"}) == "voornaam_uit_bedrijfsnaam"
    assert selection_exclusion({"company_name": "Dokter Jan"}) == "voornaam_uit_bedrijfsnaam"


def test_explicit_entity_undetermined():
    assert selection_exclusion({"company_name": "VoedingBewegingPsyche"}) == "entiteit_onbepaald"
    assert selection_exclusion({"company_name": "Praktijk Chanti Ohm"}) == "entiteit_onbepaald"


def test_legal_form_association():
    assert selection_exclusion({"company_name": "Iets", "kvk_legal_form": "Vereniging"}) == "beroepsvereniging"


def test_eponymous_practice_not_excluded():
    # eponieme praktijken met een echte persoonsnaam blijven in de pool
    for nm in ("Jeannette Hessels", "Osteopathie Anna", "Homeopathie Joyce Bosman Jansen",
               "Acupunctuur Vianney Barneveld"):
        assert selection_exclusion({"company_name": nm}) is None


def test_normal_clinic_not_excluded():
    assert selection_exclusion({"company_name": "Kliniek Vinci"}) is None
    assert selection_exclusion({"company_name": "Yosay Acupunctuur"}) is None
