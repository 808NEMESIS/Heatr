"""tests/test_receptie_preview.py — pure receptie-preview-assembler (LeadDetail-tab).

build_receptie_preview neemt een lead-rij + WI-rij (migratie 041) en levert de haak + axis-
states + de gerenderde mail 1/2/3, zonder DB/Playwright/send. Pint: hook doorgegeven, mail-1
draagt de meten-frame Q7-copy (niet het receptionist-frame), geen haak → lege mails/geen crash,
Q4 + second_hook → mail-2 aanwezig en niet geskipt.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.receptie_sequence import build_receptie_preview


def _lead(**kw):
    base = {
        "id": "d7e57e57-7e57-4e57-8e57-000000000001",
        "company_name": "Testkliniek Amsterdam", "city": "Amsterdam",
        "contact_first_name": "Sami", "google_review_count": 47, "google_rating": 4.6,
        "treatment_focus": ["Laserontharing", "Hydrafacial"],
    }
    base.update(kw)
    return base


def _wi(hook, second=None, **kw):
    base = {
        "receptie_hook_code": hook, "receptie_second_hook": second,
        "receptie_q4": "geen", "receptie_q7": "hit", "receptie_q2": "geen", "receptie_p1": "geen",
        "receptie_q4_gated": False, "receptie_form_present": False,
        "receptie_detected_at": "2026-07-29T11:41:00+00:00",
        "receptie_evidence": ["geen_analytics_geen_pixel_client_side"],
    }
    base.update(kw)
    return base


def test_q7_preview_carries_hook_axes_and_meten_copy():
    out = build_receptie_preview(_lead(), _wi("Q7"))
    assert out["hook_code"] == "Q7"
    assert out["axes"] == {"q4": "geen", "q7": "hit", "q2": "geen", "p1": "geen"}
    assert out["detected_at"] and out["evidence"]
    mail1 = next(m for m in out["mails"] if m["step"] == 1)
    low = (mail1["body"] or "").lower()
    assert any(w in low for w in ("bijgehouden", "nergens terug", "misloopt")), mail1["body"]
    assert "niemand" not in low                       # receptionist-frame is weg (dat is Q4)
    assert "sendable" in mail1 and "block_reason" in mail1   # de gate draait echt mee


def test_no_hook_is_empty_and_no_crash():
    for wi in (None, _wi(None)):
        out = build_receptie_preview(_lead(), wi)
        assert out["hook_code"] is None
        assert out["mails"] == []


def test_q4_with_second_hook_includes_unskipped_mail2():
    out = build_receptie_preview(
        _lead(), _wi("Q4", second="Q7", receptie_q4="hit", receptie_form_present=True))
    steps = [m["step"] for m in out["mails"]]
    assert 1 in steps and 2 in steps and 3 in steps
    mail2 = next(m for m in out["mails"] if m["step"] == 2)
    assert mail2["skipped"] is False


def test_single_hook_omits_mail2():
    out = build_receptie_preview(_lead(), _wi("P1"))
    steps = [m["step"] for m in out["mails"]]
    assert steps == [1, 3]                             # geen second_hook → geen mail-2
