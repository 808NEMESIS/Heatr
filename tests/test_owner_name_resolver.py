"""tests/test_owner_name_resolver.py — eigenaarsvoornaam via bron-prioriteit (Fase 1).

Kern: NOOIT domein/e-mail-local-part/initiaal als voornaam; bron-prioriteit
company_name -> over_ons_role -> linkedin; bij twijfel onbepaald, niet gokken.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from enrichment.owner_name_resolver import (
    first_name_from_company,
    pick_owner,
    resolve_owner_first_name,
)


# ── Source 1: persoonsnaam in de bedrijfsnaam ────────────────────────────────
def test_source1_person_name_in_company():
    assert first_name_from_company("Joost Kroon") == "Joost"
    assert first_name_from_company("Dr. Liem Clinic") == "Liem"      # titel gestript, voornaam over
    assert first_name_from_company("Anna Beauty") == "Anna"


def test_source1_rejects_brand_and_place():
    for brand in ("Glow Clinic Utrecht", "LaserQueens", "Allure Laser Clinics",
                  "Beauty Clinic Nederland", "Nielsen Cosmetics"):
        assert first_name_from_company(brand) is None, brand      # merk/plaats/achternaam → geen voornaam


# ── Fixture 1 — company-name-match ───────────────────────────────────────────
def test_resolve_company_name_match():
    fn, source, _ = resolve_owner_first_name({"company_name": "Joost Kroon"}, team=None)
    assert fn == "Joost" and source == "company_name"


# ── Fixture 2 — over-ons met rolsignaal ──────────────────────────────────────
def test_resolve_over_ons_role():
    team = [{"first_name": "Mieke", "role": "eigenaar", "is_owner": True, "confidence": 0.9}]
    fn, source, _ = resolve_owner_first_name({"company_name": "Glow Clinic"}, team=team)
    assert fn == "Mieke" and source == "over_ons_role"


# ── Fixture 3 — geen betrouwbare naam → onbepaald ────────────────────────────
def test_resolve_no_reliable_name_is_onbepaald():
    fn, source, _ = resolve_owner_first_name({"company_name": "Glow Clinic"}, team=[])
    assert fn is None and source == "none"


# ── Harde regels: nooit domein/e-mail-local-part/initiaal ────────────────────
def test_never_domain_or_brand_fragment():
    # domeinfragment als "naam" in een team-item → safe_first_name-gate weigert 'm
    team = [{"first_name": "Glowclinicnl", "role": "eigenaar", "is_owner": True, "confidence": 0.9}]
    fn, source, _ = resolve_owner_first_name({"company_name": "Glow Clinic"}, team=team)
    assert fn is None and source == "none"


def test_never_email_local_part():
    # de resolver gebruikt het e-mailadres NIET als bron; een naam die toevallig het
    # local-part is, komt niet uit deze module (structureel uitgesloten).
    fn, source, _ = resolve_owner_first_name(
        {"company_name": "Glow Clinic", "email": "info@glow.nl"}, team=[])
    assert fn is None and source == "none"


def test_never_initial():
    team = [{"first_name": "A.", "role": "eigenaar", "is_owner": True, "confidence": 0.9}]
    fn, _, _ = resolve_owner_first_name({"company_name": "X Clinic"}, team=team)
    assert fn is None                                             # initiaal geweigerd


# ── Rol-hiërarchie: eigenaar > arts > behandelaar ───────────────────────────
def test_role_hierarchy_owner_beats_doctor():
    team = [
        {"first_name": "Sanne", "role": "huidtherapeut", "is_owner": False, "confidence": 0.8},
        {"first_name": "Peter", "role": "eigenaar", "is_owner": True, "confidence": 0.7},
        {"first_name": "Karim", "role": "cosmetisch arts", "is_owner": False, "confidence": 0.9},
    ]
    owner, reason = pick_owner(team)
    assert owner["first_name"] == "Peter" and reason == "ok"      # eigenaar wint van arts/therapeut


def test_multiple_equal_top_role_is_onbepaald():
    team = [
        {"first_name": "Peter", "role": "eigenaar", "is_owner": True, "confidence": 0.8},
        {"first_name": "Anna", "role": "mede-eigenaar", "is_owner": True, "confidence": 0.8},
    ]
    owner, reason = pick_owner(team)
    assert owner is None and reason == "meerdere_gelijk"          # gelijke top-rol → onbepaald


def test_no_role_signal_is_onbepaald():
    team = [{"first_name": "Jan", "role": "receptie", "is_owner": False, "confidence": 0.9}]
    owner, reason = pick_owner(team)
    assert owner is None and reason == "geen_rolsignaal"          # naam zonder eigenaar/behandelaar-rol
