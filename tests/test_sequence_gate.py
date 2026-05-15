"""
tests/test_sequence_gate.py — Personalization gate + observation-block selector.

Pure-function tests. No DB, no API. Verifies:
  - _personalization_score_0_100 schaal
  - _gate_leads_for_template buckets (auto / review / skip)
  - pick_observation_block decision tree
  - get_observation_text variable substitution
  - render_step volgorde (variables eerst, spintax daarna)

Run: pytest tests/test_sequence_gate.py -v
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from config.sequence_templates import (
    SEQUENCE_TEMPLATES,
    get_observation_text,
    get_v1_sequence,
    pick_observation_block,
    template_for_sector,
)


# ---------------------------------------------------------------------------
# Personalization score scaling
# ---------------------------------------------------------------------------

def _import_gate():
    from api.main import _personalization_score_0_100, _gate_leads_for_template
    return _personalization_score_0_100, _gate_leads_for_template


def test_score_scales_15_to_100():
    score_fn, _ = _import_gate()
    assert score_fn({"personalization_potential": 15}) == 100
    assert score_fn({"personalization_potential": 0}) == 0
    assert score_fn({"personalization_potential": 7.5}) == 50


def test_score_handles_missing():
    score_fn, _ = _import_gate()
    assert score_fn({}) == 0
    assert score_fn({"personalization_potential": None}) == 0
    assert score_fn({"personalization_potential": "garbage"}) == 0


# ---------------------------------------------------------------------------
# Gate buckets
# ---------------------------------------------------------------------------

def test_gate_splits_correctly_at_threshold_70():
    _, gate_fn = _import_gate()
    template = {"min_personalization_score": 70}
    leads = [
        {"id": "high", "personalization_potential": 12},   # 80 — auto
        {"id": "edge", "personalization_potential": 10.5}, # 70 — auto
        {"id": "review", "personalization_potential": 9},  # 60 — review (≥50)
        {"id": "low_review", "personalization_potential": 7.5},  # 50 — review (≥50, <70)
        {"id": "skip", "personalization_potential": 5},    # 33 — skip
    ]
    auto, review, skip = gate_fn(leads, template)
    assert {l["id"] for l in auto} == {"high", "edge"}
    assert {l["id"] for l in review} == {"review", "low_review"}
    assert {l["id"] for l in skip} == {"skip"}


def test_gate_no_template_passes_all_through():
    _, gate_fn = _import_gate()
    leads = [{"id": "a"}, {"id": "b"}]
    auto, review, skip = gate_fn(leads, None)
    assert len(auto) == 2 and not review and not skip


def test_gate_template_without_threshold_passes_all_through():
    _, gate_fn = _import_gate()
    leads = [{"id": "x", "personalization_potential": 0}]
    auto, _, _ = gate_fn(leads, {"min_personalization_score": None})
    assert len(auto) == 1


def test_is_test_lead_bypasses_personalization_gate():
    """is_test_lead=true MUST bypass the personalization-score threshold,
    not just the completeness check (faf8abd only covered completeness).
    Test-lead with score 0 → would normally skip (0 < 50 review_floor at
    threshold 70), but must land in auto bucket."""
    _, gate_fn = _import_gate()
    template = {"min_personalization_score": 70}
    test_lead = {
        "id": "test-1",
        "is_test_lead": True,
        "personalization_potential": 0,   # would otherwise skip
    }
    real_skip_lead = {
        "id": "real-low",
        "personalization_potential": 0,   # not a test lead → still skip
    }
    auto, review, skip = gate_fn([test_lead, real_skip_lead], template)
    assert test_lead in auto, "test-lead must bypass gate, land in auto"
    assert test_lead not in skip
    assert test_lead not in review
    assert real_skip_lead in skip, "non-test low-score lead must still skip"
    # _pers_score_0_100 is gehydrateerd ook voor test-leads (debug-output)
    assert test_lead["_pers_score_0_100"] == 0


# ---------------------------------------------------------------------------
# pick_observation_block — order of precedence
# ---------------------------------------------------------------------------

def test_pick_block_website_wins_over_reviews():
    """Old-website signal should win even if reviews-signal also matches."""
    lead = {
        "website_age_years": 5,
        "visual_score": 50,
        "google_rating": 4.2,
        "days_since_last_review": 60,
    }
    assert pick_observation_block(lead) == "website"


def test_pick_block_reviews_when_only_reviews_match():
    lead = {
        "google_rating": 4.3,
        "days_since_last_review": 45,
        "has_online_booking": True,
    }
    assert pick_observation_block(lead) == "reviews"


def test_pick_block_reviews_skips_when_days_unknown():
    """Strict gate: zonder echte days_since_last_review → geen reviews block."""
    lead = {"google_rating": 4.2, "has_online_booking": True}
    # Met deze data zou reviews matchen ALS we vals positief waren —
    # maar pick_observation_block hoort in dit geval 'fallback' te kiezen.
    assert pick_observation_block(lead) == "fallback"


def test_pick_block_ads_when_active():
    lead = {"meta_ads_active": True, "ad_focus": "filler", "google_rating": 5.0}
    assert pick_observation_block(lead) == "ads"


def test_pick_block_operational_when_no_booking():
    lead = {"google_rating": 5.0, "has_online_booking": False}
    assert pick_observation_block(lead) == "operational"


def test_pick_block_fallback_when_nothing_matches():
    lead = {"has_online_booking": True, "google_rating": 5.0}
    assert pick_observation_block(lead) == "fallback"


def test_pick_block_reviews_uses_latest_review_date_fallback():
    """Als days_since_last_review None is maar latest_review_date oud → moet alsnog reviews kiezen."""
    old = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    lead = {
        "google_rating": 4.1,
        "days_since_last_review": None,
        "latest_review_date": old,
        "has_online_booking": True,
    }
    assert pick_observation_block(lead) == "reviews"


# ---------------------------------------------------------------------------
# get_observation_text — substitution
# ---------------------------------------------------------------------------

def test_observation_text_substitutes_company_city():
    text = get_observation_text("website", {"company_name": "FooKliniek", "city": "Utrecht"})
    assert "FooKliniek" in text
    assert "Utrecht" in text
    assert "{{company}}" not in text
    assert "{{city}}" not in text


def test_observation_text_unknown_block_falls_back():
    text = get_observation_text("does-not-exist", {"company_name": "X", "city": "Y"})
    # Fallback paragraaf bevat altijd {{city}} → "Y"
    assert "Y" in text


def test_observation_text_no_lead_keeps_placeholders():
    text = get_observation_text("website", None)
    assert "{{company}}" in text


def test_observation_text_blok_b_weaves_competitor_data():
    """When local_competitors_* are present on lead, BLOK B paragraph should
    include a concrete sentence about higher-rated competitors."""
    lead = {
        "company_name": "Praktijk X",
        "city": "Utrecht",
        "google_rating": 4.2,
        "local_competitors_higher_rating": 3,
        "local_competitors_in_db": 5,
    }
    text = get_observation_text("reviews", lead)
    assert "3 van de 5" in text
    assert "Utrecht" in text
    assert "{{city}}" not in text


def test_observation_text_blok_b_skips_competitor_when_missing():
    """No competitor data → no competitor sentence (keep paragraph clean)."""
    lead = {"company_name": "Praktijk X", "city": "Utrecht", "google_rating": 4.2}
    text = get_observation_text("reviews", lead)
    assert "vergelijkbare klinieken" not in text


# ---------------------------------------------------------------------------
# get_v1_sequence — structure + Mail 1 uniformity
# ---------------------------------------------------------------------------

def test_v1_sequence_has_three_steps():
    seq = get_v1_sequence("website")
    assert len(seq) == 3
    assert seq[0]["delay_days"] == 0
    assert seq[1]["delay_days"] == 3
    assert seq[2]["delay_days"] == 5


def test_v1_sequence_mail_1_is_uniform_across_blocks():
    """Per-lead variatie loopt via custom_fields.opener — Mail 1 body moet voor alle
    blokken identiek zijn (uniform `{{opener}}` frame)."""
    bodies = {b: get_v1_sequence(b)[0]["body"] for b in ("website", "reviews", "ads", "fallback")}
    assert len(set(bodies.values())) == 1, "Mail 1 body should be uniform — variatie via {{opener}}"
    assert "{{opener}}" in bodies["website"]


def test_v1_template_in_registry():
    assert "v1_cosmetisch_audit" in SEQUENCE_TEMPLATES
    t = SEQUENCE_TEMPLATES["v1_cosmetisch_audit"]
    assert t["min_personalization_score"] == 70
    assert t["cadence_days"] == [0, 3, 5]
    assert t["sector"] == "cosmetische_behandelaars"


# ---------------------------------------------------------------------------
# Alt-zorg template
# ---------------------------------------------------------------------------

def test_alt_zorg_template_in_registry():
    assert "v1_alternatieve_zorg" in SEQUENCE_TEMPLATES
    t = SEQUENCE_TEMPLATES["v1_alternatieve_zorg"]
    assert t["sector"] == "alternatieve_geneeskunde"
    # alt-zorg = niet-haastig → langere cadans dan cosmetisch
    assert t["cadence_days"] == [0, 4, 6]
    # iets lagere drempel — alt-zorg leads hebben vaak dunner data
    assert t["min_personalization_score"] == 65


def test_alt_zorg_observation_text_no_cosmetisch_jargon():
    """BLOK A van alt-zorg mag niet de cosmetische zinnen bevatten."""
    lead = {"sector": "alternatieve_geneeskunde", "company_name": "X", "city": "Utrecht"}
    text = get_observation_text("website", lead)
    assert "in de cosmetische hoek" not in text.lower()
    assert "klinieken in" not in text.lower()
    # Wel: alt-zorg eigen taal
    assert "praktijken" in text.lower() or "spreekkamer" in text.lower()


def test_cosmetic_paragraph_unchanged_for_cosmetic_lead():
    """Bestaand cosmetisch gedrag mag niet kapot."""
    lead = {"sector": "cosmetische_behandelaars", "company_name": "X", "city": "Utrecht"}
    text = get_observation_text("website", lead)
    assert "cosmetische hoek" in text


def test_get_observation_text_no_sector_defaults_to_cosmetisch():
    """Geen sector op lead → cosmetisch (default), geen crash."""
    text = get_observation_text("website", {"company_name": "X", "city": "Y"})
    assert "X" in text


# ---------------------------------------------------------------------------
# template_for_sector routing
# ---------------------------------------------------------------------------

def test_template_for_sector_routes_correctly():
    assert template_for_sector("cosmetische_behandelaars") == "v1_cosmetisch_audit"
    assert template_for_sector("alternatieve_geneeskunde") == "v1_alternatieve_zorg"


def test_template_for_sector_unknown_returns_none():
    assert template_for_sector("makelaars") is None
    assert template_for_sector(None) is None
    assert template_for_sector("") is None


# ---------------------------------------------------------------------------
# is_quality_opener — Claude-output validator
# ---------------------------------------------------------------------------

def test_is_quality_opener_accepts_real_opener():
    from campaigns.sequence_engine import is_quality_opener
    opener = (
        "Ik kwam Praktijk X tegen tijdens onderzoek naar chiro's in Utrecht. "
        "Wat me opviel: de Wim Hof methode in jullie aanbod sinds 2021. Hoe kies "
        "je daar als enige in de stad voor? Open om er kort over te sparren?"
    )
    ok, reason = is_quality_opener(opener)
    assert ok, f"expected accept, got reject: {reason}"


def test_is_quality_opener_rejects_too_short():
    from campaigns.sequence_engine import is_quality_opener
    ok, reason = is_quality_opener("Hoi, even een vraag.")
    assert not ok
    assert "too_short" in reason


def test_is_quality_opener_rejects_banned_pattern():
    from campaigns.sequence_engine import is_quality_opener
    text = (
        "Snelle vraag voor je: ik kwam jullie praktijk tegen via Google en "
        "ben benieuwd of jullie open staan voor een kort gesprek over het "
        "verbeteren van jullie online aanwezigheid en conversie ratio's. Open?"
    )
    ok, reason = is_quality_opener(text)
    assert not ok
    assert "banned_pattern" in reason


def test_is_quality_opener_rejects_markdown_header():
    from campaigns.sequence_engine import is_quality_opener
    text = (
        "# Voorstel openingszin:\n\n"
        "Mark, jullie tagline rust in je rug spreekt boekdelen. Wat me opviel: jullie "
        "Wim Hof methode sinds 2021. Hoe combineer je dat met klassieke chiropractie?"
    )
    ok, reason = is_quality_opener(text)
    # Na clean → header weg → moet OK zijn
    assert ok is True or "header" in reason


def test_is_quality_opener_rejects_truncated_mid_sentence():
    """Live bug: opener eindigde op '...over de kwaliteit van uw werk en de waar'"""
    from campaigns.sequence_engine import is_quality_opener
    text = (
        "Mark, jullie tagline rust in je rug spreekt boekdelen. Wat me opviel: "
        "jullie Wim Hof methode sinds 2021. Hoe combineer je dat met klassieke chiropractie en de waar"
    )
    ok, reason = is_quality_opener(text)
    assert ok is False
    assert "truncated" in reason


def test_clean_claude_opener_strips_markdown_prefix():
    from campaigns.sequence_engine import clean_claude_opener
    raw = (
        "# Voorstel openingszin:\n\n"
        "Mark, dit is de echte tekst die we willen behouden."
    )
    cleaned = clean_claude_opener(raw)
    assert cleaned.startswith("Mark, dit is de echte")
    assert "#" not in cleaned[:5]


def test_clean_claude_opener_strips_label_prefix():
    from campaigns.sequence_engine import clean_claude_opener
    cases = [
        "Opener: Mark, prima.",
        "Openingszin — Mark, prima.",
        "Optie 1: Mark, prima.",
    ]
    for raw in cases:
        cleaned = clean_claude_opener(raw)
        assert cleaned.startswith("Mark, prima"), f"Failed for {raw!r} → {cleaned!r}"


def test_clean_claude_opener_strips_code_fence():
    from campaigns.sequence_engine import clean_claude_opener
    raw = "```\nMark, prima reactie.\n```"
    cleaned = clean_claude_opener(raw)
    assert "```" not in cleaned
    assert cleaned.startswith("Mark")


def test_is_quality_opener_rejects_unresolved_placeholder():
    from campaigns.sequence_engine import is_quality_opener
    text = (
        "Ik kwam {{company}} tegen — niet veel praktijken in {{city}} doen wat "
        "jullie doen. Wat me opviel is dat de site nog niet goed mobiel werkt. "
        "Bewuste keuze of staat het nog op de lijst?"
    )
    ok, reason = is_quality_opener(text)
    assert not ok
    assert reason == "unresolved_placeholder"


def test_is_quality_opener_rejects_empty():
    from campaigns.sequence_engine import is_quality_opener
    assert is_quality_opener(None) == (False, "empty")
    assert is_quality_opener("") == (False, "empty")
    assert is_quality_opener("   ") == (False, "empty")


# ---------------------------------------------------------------------------
# Archetype-aware block selection
# ---------------------------------------------------------------------------

def test_pick_block_holistisch_spiritueel_skips_operational():
    """Holistisch-spirituele praktijken kiezen vaak bewust voor 'bel ons' —
    'geen online booking' is GEEN pijn voor hen."""
    lead = {
        "archetype": "holistisch_spiritueel",
        "has_online_booking": False,
        "google_rating": 5.0,
    }
    # Should NOT pick 'operational', should fall through to 'fallback'
    assert pick_observation_block(lead) == "fallback"


def test_pick_block_medisch_cosmetisch_skips_ads():
    """Plastisch chirurgen vinden ads-tone te commercieel — skip dat blok."""
    lead = {
        "archetype": "medisch_cosmetisch",
        "meta_ads_active": True,
        "ad_focus": "blepharoplastie",
        "has_online_booking": True,
        "google_rating": 5.0,
    }
    assert pick_observation_block(lead) == "fallback"


def test_pick_block_volume_beauty_uses_all_blocks():
    """Volume-beauty mag alle blokken gebruiken inclusief ads."""
    lead = {
        "archetype": "volume_beauty",
        "meta_ads_active": True,
        "ad_focus": "wimperextensions",
        "has_online_booking": True,
        "google_rating": 5.0,
    }
    assert pick_observation_block(lead) == "ads"


def test_pick_block_no_archetype_legacy_behavior():
    """Geen archetype bekend → alle blokken toegestaan (backwards compat)."""
    lead = {
        "meta_ads_active": True,
        "ad_focus": "filler",
        "has_online_booking": True,
        "google_rating": 5.0,
    }
    assert pick_observation_block(lead) == "ads"


# ---------------------------------------------------------------------------
# render_step — variable injection BEFORE spintax
# ---------------------------------------------------------------------------

def test_render_step_substitutes_double_brace_then_spintax():
    """{{opener}} and {a|b} both in same body — must not interfere."""
    from campaigns.sequence_engine import render_step

    step = {
        "subject": "{{company}} — {Hoi|Goedemiddag}",
        "body": "Hoi {{first_name}},\n\n{{opener}}\n\n— Sami",
        "delay_days": 0,
    }
    lead = {
        "company_name": "Mijn Praktijk",
        "contact_first_name": "Annelijn",
        "personalized_opener": "Ik kwam jullie tegen via Google.",
    }
    out = render_step(step, lead)
    # Subject: company injected, spintax resolved (one of the two)
    assert "Mijn Praktijk" in out["subject"]
    assert any(g in out["subject"] for g in ("Hoi", "Goedemiddag"))
    assert "{Hoi|Goedemiddag}" not in out["subject"]
    # Body: both first_name AND opener fully substituted (no leftover braces)
    assert "Annelijn" in out["body"]
    assert "Ik kwam jullie tegen via Google." in out["body"]
    assert "{{opener}}" not in out["body"]
    assert "{opener}" not in out["body"]
    assert "{first_name}" not in out["body"]
