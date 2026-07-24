"""
tests/test_legal_form.py — rechtsvorm-risico-gate (AVG-02).

Kernpunt: conservatief. Alleen een bevestigde rechtspersoon (BV/NV/stichting) is
veilig voor koude mail; eenmanszaak/zzp = natuurlijk persoon → opt-in vereist;
leeg/onbekend (KvK opt-in uit) → 'onbepaald' → geblokkeerd tot rechtsvorm bekend.
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


def test_eenmanszaak_is_natuurlijk_persoon_and_blocked():
    for lf in ("Eenmanszaak", "ZZP", "VOF", "Maatschap"):
        assert classify_legal_form({"kvk_legal_form": lf}) == "natuurlijk_persoon", lf
        ok, reason = receptie_avg_safe({"kvk_legal_form": lf})
        assert ok is False and reason == "natuurlijk_persoon_opt_in_vereist"


def test_empty_legal_form_is_onbepaald_and_blocked():
    # KvK opt-in uit → geen rechtsvorm → onbepaald → geblokkeerd (Sami's default).
    for lead in ({}, {"kvk_legal_form": None}, {"kvk_legal_form": "  "}):
        assert classify_legal_form(lead) == "onbepaald"
        ok, reason = receptie_avg_safe(lead)
        assert ok is False and reason == "rechtsvorm_onbepaald"


def test_unknown_form_string_is_onbepaald_not_optimistic():
    # onbekende string → nooit optimistisch 'rechtspersoon'.
    assert classify_legal_form({"kvk_legal_form": "iets vaags"}) == "onbepaald"
    assert receptie_avg_safe({"kvk_legal_form": "iets vaags"})[0] is False
