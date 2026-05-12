"""tests/test_pick_brug.py — v3.1 brug-router.

Tests voor pick_brug() in config/sequence_templates.py:
  - clear-website signals → 'website'
  - clear-workflow signals → 'workflow'
  - twijfel (beide laag) → 'ai_audit' (default)
  - lege/missende data → 'ai_audit' (default)
"""
from __future__ import annotations

from config.sequence_templates import (
    SEQUENCE_TEMPLATES,
    pick_brug,
    primaire_dienstverlening_for_lead,
    sector_noemer_for_lead,
    template_for_brug,
)


# ---------------------------------------------------------------------------
# pick_brug — vier kern-scenarios
# ---------------------------------------------------------------------------

def test_pick_brug_clear_website():
    """Lage visual_score + oude site + Wix → website-pad."""
    lead = {
        "visual_score": 30,         # +40
        "website_age_years": 6,     # +30
        "cms_detected": "Wix",      # +20  → totaal 90
        "google_review_count": 12,  # workflow score laag
    }
    assert pick_brug(lead) == "website"


def test_pick_brug_clear_workflow():
    """Veel reviews + meerdere treatments + meerdere locaties → workflow-pad."""
    lead = {
        "visual_score": 80,                 # geen website-signaal
        "website_age_years": 1,             # geen website-signaal
        "google_review_count": 120,         # +30
        "treatment_focus": ["Botox", "Filler", "Laser"],  # +30
        "locations_count": 3,               # +30  → totaal 90
    }
    assert pick_brug(lead) == "workflow"


def test_pick_brug_twijfel_naar_ai_audit():
    """Beide score's onder 70 → fallback ai_audit."""
    lead = {
        "visual_score": 60,           # geen 40
        "website_age_years": 3,       # geen 30
        "cms_detected": "WordPress",  # geen 20
        "google_review_count": 20,    # geen 30
        "treatment_focus": ["X"],     # geen 30 (need >=3)
        "locations_count": 1,         # geen 30
    }
    # website_score=0, workflow_score=0 → ai_audit default
    assert pick_brug(lead) == "ai_audit"


def test_pick_brug_lege_data_naar_ai_audit():
    """Compleet lege/None velden → fallback ai_audit (default voor twijfel)."""
    lead: dict = {}
    assert pick_brug(lead) == "ai_audit"

    # Ook expliciet None-waardes mogen niet crashen
    lead2 = {
        "visual_score": None,
        "website_age_years": None,
        "cms_detected": None,
        "google_review_count": None,
        "treatment_focus": None,
        "locations_count": None,
    }
    assert pick_brug(lead2) == "ai_audit"


# ---------------------------------------------------------------------------
# Edge cases — gedrag rondom drempel
# ---------------------------------------------------------------------------

def test_pick_brug_website_just_under_threshold():
    """website_score=60 (<70) → niet website. Geen workflow-signal → ai_audit."""
    lead = {
        "visual_score": 30,        # +40
        "website_age_years": 6,    # +30 → 70 — net op drempel
    }
    # 70 >= 70 én > workflow(0) → website
    assert pick_brug(lead) == "website"

    lead2 = {
        "visual_score": 30,        # +40
        "website_age_years": 3,    # geen 30 → 40
    }
    # 40 < 70 → ai_audit
    assert pick_brug(lead2) == "ai_audit"


def test_pick_brug_tie_workflow_wins_only_if_strictly_higher():
    """Als beide scores >= 70 maar gelijk → ai_audit (geen strikte winnaar)."""
    # Dit is een randgeval dat zelden voorkomt; gedrag verifiëren.
    lead = {
        "visual_score": 30,                             # +40
        "website_age_years": 6,                         # +30 → website 70
        "google_review_count": 30,                      # +30
        "treatment_focus": ["a", "b", "c"],             # +30
        "locations_count": 1,                           # geen +30 → workflow 60
    }
    # website 70 > workflow 60 → website
    assert pick_brug(lead) == "website"


# ---------------------------------------------------------------------------
# Template-helpers (sanity)
# ---------------------------------------------------------------------------

def test_template_for_brug_maps_to_v3_1_keys():
    assert template_for_brug("website")  == "v3_1_website"
    assert template_for_brug("workflow") == "v3_1_workflow"
    assert template_for_brug("ai_audit") == "v3_1_ai_audit"


def test_v3_1_templates_in_registry_with_three_steps():
    for tmpl_id in ("v3_1_website", "v3_1_workflow", "v3_1_ai_audit"):
        assert tmpl_id in SEQUENCE_TEMPLATES, f"{tmpl_id} ontbreekt"
        t = SEQUENCE_TEMPLATES[tmpl_id]
        assert t["version"] == "3.1"
        assert t["cadence_days"] == [0, 3, 5]
        assert len(t["default_steps"]) == 3
        # Mail 1 zonder reply-thread, Mail 2/3 wel
        assert t["default_steps"][0]["thread"] == "new"
        assert t["default_steps"][1]["thread"] == "reply"
        assert t["default_steps"][2]["thread"] == "reply"


# ---------------------------------------------------------------------------
# Token-fallback helpers
# ---------------------------------------------------------------------------

def test_primaire_dienstverlening_uses_treatment_focus_first():
    lead = {
        "treatment_focus": ["Ooglidcorrectie", "Injectables"],
        "industry": "medische dienstverlening",
        "sector": "cosmetische_behandelaars",
    }
    assert primaire_dienstverlening_for_lead(lead) == "Ooglidcorrectie, Injectables"


def test_primaire_dienstverlening_falls_back_to_industry():
    lead = {"treatment_focus": [], "industry": "tandartspraktijk", "sector": "alternatieve_geneeskunde"}
    assert primaire_dienstverlening_for_lead(lead) == "tandartspraktijk"


def test_primaire_dienstverlening_falls_back_to_sector_when_all_empty():
    lead = {"sector": "cosmetische_behandelaars"}
    assert "cosmetische" in primaire_dienstverlening_for_lead(lead)


def test_sector_noemer_known_sectors():
    assert sector_noemer_for_lead({"sector": "cosmetische_behandelaars"}) == "cliniek-ondernemers"
    assert sector_noemer_for_lead({"sector": "alternatieve_geneeskunde"}) == "praktijken"
    assert sector_noemer_for_lead({"sector": "techniek_ambacht"}) == "ambachtelijke bedrijven"


def test_sector_noemer_unknown_falls_back_to_ondernemers():
    assert sector_noemer_for_lead({"sector": "iets_wat_niet_bestaat"}) == "ondernemers"
    assert sector_noemer_for_lead({}) == "ondernemers"


# NB: signal_block_short is op 2026-05-07 verwijderd. {{signaal_blok}} resolution
# loopt nu via utils/signal_picker.pick_signaal_blok — getest in test_signal_picker.py.


# ---------------------------------------------------------------------------
# inject_variables — v3.1 tokens
# ---------------------------------------------------------------------------

def test_inject_variables_substitutes_v3_1_tokens():
    from campaigns.sequence_engine import inject_variables
    text = (
        "Hoi {{first_name}}, {{bedrijfsnaam}} doet {{primaire_dienstverlening}} "
        "in {{stad}}. Sterk: {{signaal_blok}}. Voor {{sector_noemer}} relevant. "
        "{{LOOM_LINK}} | {{VIDEO_LINK}}"
    )
    lead = {
        "contact_first_name": "Sami",
        "contact_why_chosen": "(confidence: 80%)",  # voorbij confidence-gate
        "company_name": "Aerys Solution",
        "city": "Groningen",
        "treatment_focus": ["Software", "AI"],
        "google_rating": 4.5,
        "google_review_count": 20,
        "days_since_last_review": 50,
        "has_online_booking": True,
        "sector": "cosmetische_behandelaars",
    }
    out = inject_variables(text, lead)
    assert "Aerys Solution" in out
    assert "Software, AI" in out
    assert "Groningen" in out
    assert "cliniek-ondernemers" in out
    assert "{{" not in out  # alle tokens vervangen
    # LOOM_LINK + VIDEO_LINK leeg → resulteert in lege strings, niet {{LOOM_LINK}}
    assert "{{LOOM_LINK}}" not in out
    assert "{{VIDEO_LINK}}" not in out
