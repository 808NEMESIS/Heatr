"""
tests/test_hook_writer.py — receptie-persistentie (writer, migratie-041-velden).

Test de veld-mapping (puur) + de fail-soft update-keten met een nep-supabase, en
dat run_receptie_for_lead de detector aanroept en persist.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

def _run(coro):
    """Private event-loop: raakt de gedeelde pytest-asyncio-loop niet aan."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


from website_intelligence.hook_writer import (
    persist_receptie,
    receptie_row,
    run_receptie_for_lead,
)

RESULT = {
    "domain": "kliniek.nl", "hook_code": "Q4", "hook_ladder": ["Q4", "Q7"],
    "second_hook": "Q7", "q4": "hit", "q7": "hit", "q2": "hit", "p1": "geen",
    "q4_gated": False, "form_present": True,
    "runs": [{"evidence": {"q4": ["geen_chat_wa_messenger_zelfboeken"]}}],
}


# ── nep-supabase: legt de update-payload + filters vast ──────────────────────
class _FakeQuery:
    def __init__(self, store):
        self.store = store

    def update(self, row):
        self.store["row"] = row
        return self

    def eq(self, col, val):
        self.store.setdefault("eq", {})[col] = val
        return self

    def execute(self):
        return type("R", (), {"data": self.store.get("_data", [{"lead_id": 1}])})()


class _FakeSupabase:
    def __init__(self, data=None):
        self.store = {"_data": data if data is not None else [{"lead_id": 1}]}

    def table(self, name):
        self.store["table"] = name
        return _FakeQuery(self.store)


def test_receptie_row_maps_all_fields():
    row = receptie_row(RESULT, detected_at="2026-07-24T12:00:00Z")
    assert row["receptie_hook_code"] == "Q4"
    assert row["receptie_hook_ladder"] == ["Q4", "Q7"]
    assert row["receptie_second_hook"] == "Q7"
    assert row["receptie_q4"] == "hit" and row["receptie_p1"] == "geen"
    assert row["receptie_q4_gated"] is False and row["receptie_form_present"] is True
    assert row["receptie_evidence"] == {"q4": ["geen_chat_wa_messenger_zelfboeken"]}
    assert row["receptie_detected_at"] == "2026-07-24T12:00:00Z"


def test_receptie_row_none_hook_when_not_mailable():
    row = receptie_row({"hook_code": None, "hook_ladder": [], "q4": "geen",
                        "q7": "geen", "q2": "geen", "p1": "geen"})
    assert row["receptie_hook_code"] is None and row["receptie_hook_ladder"] == []


def test_persist_receptie_writes_to_wi_row():
    sb = _FakeSupabase()
    ok = persist_receptie(sb, lead_id=1, workspace_id="aerys", result=RESULT,
                          detected_at="2026-07-24T12:00:00Z")
    assert ok is True
    assert sb.store["table"] == "website_intelligence"
    assert sb.store["eq"] == {"workspace_id": "aerys", "lead_id": 1}
    assert sb.store["row"]["receptie_hook_code"] == "Q4"


def test_persist_receptie_fail_soft_when_no_row():
    # geen WI-rij geraakt (lege data) → False, geen exception.
    sb = _FakeSupabase(data=[])
    assert persist_receptie(sb, 99, "aerys", RESULT) is False


def test_persist_receptie_fail_soft_on_exception():
    class _Boom:
        def table(self, *_):
            raise RuntimeError("kolom bestaat niet (migratie 041 niet gedraaid)")
    assert persist_receptie(_Boom(), 1, "aerys", RESULT) is False


def test_run_receptie_for_lead_calls_detector_and_persists(monkeypatch):
    calls = {}

    async def _fake_detect(domain, playwright=None, *, browser=None, **kw):
        calls["domain"] = domain
        return RESULT

    import website_intelligence.hook_detector as hd
    monkeypatch.setattr(hd, "detect_receptie", _fake_detect)
    sb = _FakeSupabase()
    out = _run(
        run_receptie_for_lead(sb, {"id": 1, "workspace_id": "aerys", "domain": "kliniek.nl"}))
    assert calls["domain"] == "kliniek.nl"
    assert out["hook_code"] == "Q4"
    assert sb.store["row"]["receptie_hook_code"] == "Q4"


def test_run_receptie_for_lead_skips_empty_domain():
    sb = _FakeSupabase()
    out = _run(
        run_receptie_for_lead(sb, {"id": 1, "workspace_id": "aerys", "domain": ""}))
    assert out is None
