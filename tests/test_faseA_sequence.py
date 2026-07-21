"""
tests/test_faseA_sequence.py — Fase A Founding-Five sequence (specs 1-3).

Droog-render-asserts (geen sends, kill-switch dicht). Bewijst per gegenereerde
mail: geen em-/en-dash, precies één vraag, geen onopgeloste tokens, geen
Loom-claim-als-bestaand, concept/betaald-scheiding intact, mail 2-degradatie in
alle vier token-toestanden, mail 3 valide bij plekken 5/2/1/0.
"""
from campaigns.sequence_engine import inject_variables
from config.sequence_templates import (
    resolve_faseA_step, faseA_brug_for, _mail2_variant_key, _mail3_plekken,
    _FASE_A_BRUGGEN, FOUNDING_FIVE_TOTAL,
)
from utils.text_normalizer import validate_opener_sendable


def _render(step, lead):
    """Render zonder spintax-randomness (geen {a|b} in Fase A-copy)."""
    return {
        "subject": inject_variables(step["subject"], lead),
        "body": inject_variables(step["body"], lead),
    }


def _base_lead(**kw):
    lead = {
        "company_name": "Testkliniek", "city": "Amsterdam",
        "sector": "cosmetische_behandelaars", "domain": "test.nl",
        "contact_first_name": "Jan",
        "personalized_opener": "Jullie 68 reviews met een 4.9 vallen op in Amsterdam.",
        "google_review_count": 68, "google_rating": 4.9,
    }
    lead.update(kw)
    return lead


def _assert_clean_mail(rendered, *, expect_offer_word=True):
    for part in (rendered["subject"], rendered["body"]):
        assert "—" not in part and "–" not in part, f"em/en-dash in: {part!r}"
        assert "{{" not in part and "}}" not in part, f"onopgeloste token in: {part!r}"
    body = rendered["body"]
    # nooit MEER dan één ask (v3.1-fout was 2-3 vragen); een break-up mag er 0.
    assert body.count("?") <= 1, f"meer dan 1 vraag ({body.count('?')}): {body!r}"
    # geen Loom-claim-als-bestaand
    for phrase in ("ik heb opgenomen", "heb ik opgenomen", "nam ik op", "heb een Loom opgenomen",
                   "videootje op", "video opgenomen"):
        assert phrase.lower() not in body.lower(), f"Loom-claim-als-bestaand: {phrase}"


# ── Spec 1: opener-render + em-dash-gate ────────────────────────────────────
def test_opener_renders_in_mail1():
    lead = _base_lead()
    r = _render(resolve_faseA_step("conceptsite", 0, lead, free_slots=5), lead)
    assert "68 reviews met een 4.9" in r["body"]


def test_opener_falls_back_to_signaal_when_missing():
    lead = _base_lead(personalized_opener=None)
    r = _render(resolve_faseA_step("conceptsite", 0, lead, free_slots=5), lead)
    # geen leeg gat waar de opener hoort; signaal_blok vulde het (review-getal)
    assert "68 reviews" in r["body"]
    assert "\n\n\n" not in r["body"]


def test_stale_emdash_opener_falls_back_not_rendered():
    lead = _base_lead(personalized_opener="Sterk werk in Amsterdam — echt indrukwekkend.")
    r = _render(resolve_faseA_step("conceptsite", 0, lead, free_slots=5), lead)
    assert "—" not in r["body"]  # em-dash-opener mag nooit uitgaan


def test_validate_opener_rejects_emdash():
    ok, reason = validate_opener_sendable("Jullie site oogt sterk — modern en clean.")
    assert not ok and reason == "em_dash"
    ok2, _ = validate_opener_sendable("Jullie site oogt sterk, modern en clean.")
    assert ok2


# ── Spec 2: voornaam-fallback ───────────────────────────────────────────────
def test_greeting_with_name():
    lead = _base_lead(contact_first_name="Jan")
    r = _render(resolve_faseA_step("conceptsite", 0, lead, free_slots=5), lead)
    assert r["body"].startswith("Hoi Jan,")


def test_greeting_without_name_no_daar_gap():
    lead = _base_lead(contact_first_name=None)
    r = _render(resolve_faseA_step("conceptsite", 0, lead, free_slots=5), lead)
    assert r["body"].startswith("Hoi,")
    assert "daar" not in r["body"].split("\n")[0]


# ── Spec 3: mail 2-degradatie (vier toestanden) ─────────────────────────────
def test_mail2_variant_selection():
    assert _mail2_variant_key(_base_lead(detail_2="X", concurrent_signaal="Y")) == "both"
    assert _mail2_variant_key(_base_lead(detail_2="X")) == "detail_2"
    assert _mail2_variant_key(_base_lead(concurrent_signaal="Y")) == "concurrent"
    assert _mail2_variant_key(_base_lead()) == "none"


def test_mail2_all_four_render_clean_both_bruggen():
    combos = [
        {},                                              # none
        {"detail_2": "de laadtijd van je homepage"},     # detail_2
        {"concurrent_signaal": "een concurrent met 120 reviews"},  # concurrent
        {"detail_2": "de laadtijd", "concurrent_signaal": "een concurrent met 120 reviews"},  # both
    ]
    for brug in _FASE_A_BRUGGEN:
        for extra in combos:
            lead = _base_lead(**extra)
            r = _render(resolve_faseA_step(brug, 1, lead, free_slots=5), lead)
            _assert_clean_mail(r)
            assert r["body"].count("?") == 1  # mail 2 heeft altijd precies één ask


# ── Spec 3: mail 3 plekken-teller (5/2/1/0) ─────────────────────────────────
def test_mail3_plekken_states():
    assert "{{vrije_plekken}}" in _mail3_plekken(5)   # multi toont het getal
    assert "{{vrije_plekken}}" in _mail3_plekken(2)
    assert "laatste plek" in _mail3_plekken(1)
    assert "plek" not in _mail3_plekken(0).lower() or "gereduceerd" not in _mail3_plekken(0)


def test_mail3_renders_clean_all_slot_counts_both_bruggen():
    for brug in _FASE_A_BRUGGEN:
        for free in (5, 2, 1, 0):
            lead = _base_lead(vrije_plekken=free)
            r = _render(resolve_faseA_step(brug, 2, lead, free_slots=free), lead)
            _assert_clean_mail(r)
            if free >= 2:
                assert str(free) in r["body"]          # live getal
            if free == 0:
                assert "gereduceerd tarief" not in r["body"]  # aanbod-alinea gevallen


# ── concept/betaald-scheiding + bruggen ─────────────────────────────────────
def test_offer_free_paid_separation_mail1():
    for brug in _FASE_A_BRUGGEN:
        lead = _base_lead(sector="cosmetische_behandelaars")
        r = _render(resolve_faseA_step(brug, 0, lead, free_slots=5), lead)
        b = r["body"].lower()
        assert "gratis" in b                     # concept/analyse = gratis
        assert "gereduceerd tarief" in b         # de bouw = betaald (reduced)
        assert "kosteloos" in b


def test_faseA_brug_mapping():
    # Workflow-brug geschrapt (2026-07-21): alles → conceptsite.
    assert faseA_brug_for("workflow") == "conceptsite"
    assert faseA_brug_for("website") == "conceptsite"
    assert faseA_brug_for("ai_audit") == "conceptsite"


def test_render_faseA_marker_resolves_plekken_live():
    """De marker leest de plekken-teller LIVE (via free_founding_five_slots) en
    resolvet mail 3 op het verzendmoment — niet de launch-tijd."""
    import asyncio
    from campaigns.sequence_engine import render_faseA_marker

    class _Res:  # 2 vergeven → 5-2 = 3 vrij
        count = 2
    class _Tbl:
        def select(self, *a, **k): return self
        def eq(self, *a, **k): return self
        def execute(self): return _Res()
    class _SB:
        def table(self, *a, **k): return _Tbl()

    lead = {"company_name": "Kliniek X", "contact_first_name": "Jan",
            "sector": "cosmetische_behandelaars",
            "personalized_opener": "Jullie 68 reviews met een 4.9 vallen op."}
    marker = {"faseA_brug": "conceptsite", "faseA_step": 2, "delay_days": 5}
    out = asyncio.get_event_loop().run_until_complete(
        render_faseA_marker(marker, lead, _SB(), "aerys"))
    assert "3 plekken" in out["body"]          # live: 5 − 2 vergeven
    assert "—" not in out["body"] and "{{" not in out["body"]
    # mail 1 (marker) rendert de opener + begroeting
    m1 = asyncio.get_event_loop().run_until_complete(
        render_faseA_marker({"faseA_brug": "conceptsite", "faseA_step": 0}, lead, _SB(), "aerys"))
    assert m1["body"].startswith("Hoi Jan,") and "68 reviews" in m1["body"]


def test_three_mails_have_distinct_subjects_threaded():
    lead = _base_lead()
    s0 = resolve_faseA_step("conceptsite", 0, lead, free_slots=5)
    s1 = resolve_faseA_step("conceptsite", 1, lead, free_slots=5)
    assert s0["thread"] == "new" and s1["thread"] == "reply"
    assert s1["subject"].startswith("Re: ")
    assert FOUNDING_FIVE_TOTAL == 5
