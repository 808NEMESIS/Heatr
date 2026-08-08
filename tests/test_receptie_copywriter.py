"""tests/test_receptie_copywriter.py — value-first mail-1 assemblage (canonieke boog)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from campaigns.receptie_copywriter import build_value_first_mail1, _themes_phrase

PRIV = "Je ontvangt deze mail omdat je praktijk openbaar vindbaar is; zie aeryssolution.nl/privacy"
LEAD = {"id": "l1", "company_name": "Face Institute"}
THEMES = ["hoe op hun gemak mensen zich voelen", "het nette eindresultaat"]
LEAK = "Wat me wél opviel: op de site kan een bezoeker nergens direct een moment vastleggen."


def _kw(**over):
    base = dict(bridge="conceptsite", themes=THEMES, privacy_notice=PRIV,
               unsubscribe="", leak_line=LEAK, warmr_owns_unsubscribe=True)
    base.update(over)
    return base


def test_conceptsite_full_mail():
    out = build_value_first_mail1(LEAD, **_kw())
    assert out is not None
    assert out["subject"] == "Face Institute, iets wat me opviel"
    b = out["body"]
    assert b.count("Face Institute") >= 2                 # naam verweven
    assert "hoe op hun gemak" in b and "eindresultaat" in b
    assert "gratis een concept" in b and "Loom" in b       # Founding Five + Loom
    assert b.count("?") == 1                                # één vraag
    assert "—" not in b


def test_workflow_needs_no_leak():
    out = build_value_first_mail1(LEAD, **_kw(bridge="workflow", leak_line=None))
    assert out is not None and out["bridge"] == "workflow"
    assert "gratis in kaart" in out["body"] and "Loom" in out["body"]


def test_conceptsite_without_leak_returns_none():
    assert build_value_first_mail1(LEAD, **_kw(leak_line=None)) is None


def test_fewer_than_two_themes_returns_none():
    assert build_value_first_mail1(LEAD, **_kw(themes=["maar één thema"])) is None
    assert build_value_first_mail1(LEAD, **_kw(themes=[])) is None


def test_missing_privacy_blocks_via_gate():
    assert build_value_first_mail1(LEAD, **_kw(privacy_notice="")) is None


def test_em_dash_theme_blocked_by_gate():
    assert build_value_first_mail1(LEAD, **_kw(themes=["iets — met em-dash", "tweede thema"])) is None


def test_unknown_bridge_returns_none():
    assert build_value_first_mail1(LEAD, **_kw(bridge="onzin")) is None


def test_themes_phrase_joins_two():
    assert _themes_phrase(["a", "b", "c"]) == "a en b"
