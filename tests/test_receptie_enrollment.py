"""
tests/test_receptie_enrollment.py — receptie-cohort-enrollment (Fase-A-markers).

receptie_faseA_steps bouwt de 3 markers (faseA_brug='receptie', cadence 0/3/5) met
subject/body-shells; de echte body + gates komen live via render_faseA_marker.
_resolve_template_for_lead kiest die steps bij template_id='faseA_receptie'.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.sequence_templates import receptie_faseA_steps


def test_receptie_faseA_steps_structure():
    steps = receptie_faseA_steps()
    assert len(steps) == 3
    for i, s in enumerate(steps):
        assert s["faseA_brug"] == "receptie" and s["faseA_step"] == i
        assert s["subject"] and s["body"]           # shell aanwezig voor campaign-create
    assert [s["delay_days"] for s in steps] == [0, 3, 5]
    assert steps[0]["thread"] == "new" and steps[1]["thread"] == "reply"


def test_receptie_faseA_steps_carry_hook_tokens_not_resolved():
    # de shell is ONgerenderd: {{begroeting}}/{{haakje}} worden pas live vervangen.
    m1 = receptie_faseA_steps()[0]
    assert "{{begroeting}}" in m1["body"] and "{{haakje}}" in m1["body"]


def test_resolver_selects_receptie_for_explicit_template_id():
    try:
        from api.main import _resolve_template_for_lead
    except Exception:
        import pytest
        pytest.skip("api.main niet importeerbaar in deze omgeving")
    t_id, t_obj, steps = _resolve_template_for_lead(
        {"id": "l1"}, "faseA_receptie", [])
    assert t_id == "faseA_receptie" and t_obj is None
    assert len(steps) == 3 and all(s["faseA_brug"] == "receptie" for s in steps)


def test_resolver_ignores_receptie_when_other_template():
    try:
        from api.main import _resolve_template_for_lead
    except Exception:
        import pytest
        pytest.skip("api.main niet importeerbaar in deze omgeving")
    # AUTO-mode (geen faseA_receptie) → conceptsite, niet receptie.
    _, _, steps = _resolve_template_for_lead({"id": "l1"}, None, [])
    assert all(s.get("faseA_brug") != "receptie" for s in steps)
