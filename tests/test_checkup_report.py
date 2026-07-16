"""
tests/test_checkup_report.py — de fail-closed QA-gate op het check-up rapport.

validate_report_sendable is het vangnet naast het menselijke vrijgeef-gate:
verzonnen getal / pitch / gedachtestreepje / ≠3 bevindingen / >400 woorden → niet
verzendbaar. Rekensommen op bron-getallen blijven wél toegestaan.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from calls.report_generator import validate_report_sendable as V, generate_checkup_report


_CHECKUP = {
    "unanswered_inbound_per_week": 14,
    "value_per_new_patient": 800,
    "conversion_estimate": 0.25,
    "response_time_hours": 11,
    "no_shows_per_month": 6,
}


def _report(findings_html: str) -> str:
    return f'<article class="checkup-report"><header><h1>Kliniek X</h1></header>{findings_html}</article>'


_GOOD = _report(
    '<section class="finding"><h2>1. Gemiste inbound</h2>'
    '<p class="fact">14 gemiste aanvragen per week.</p>'
    '<p class="cost">Conservatief gerekend: 14 keer 800 euro keer 25 procent is 2800 euro per week.</p></section>'
    '<section class="finding"><h2>2. Reactietijd</h2>'
    '<p class="fact">11 uur gemiddelde reactietijd op een aanvraag.</p>'
    '<p class="cost">Conservatief: bij 14 aanvragen is dat veel wachttijd.</p></section>'
    '<section class="finding"><h2>3. No shows</h2>'
    '<p class="fact">6 no shows per maand.</p>'
    '<p class="cost">6 keer 800 euro is 4800 euro per maand, conservatief.</p></section>'
)


class TestQaGate:
    def test_accepts_valid_report(self):
        ok, r = V(_GOOD, _CHECKUP)
        assert ok, r

    def test_calculation_result_is_grounded(self):
        # 2800 = 14*800*0.25 en 4800 = 6*800 — afgeleid van bron-getallen, toegestaan.
        ok, r = V(_GOOD, _CHECKUP)
        assert ok and r == "ok"

    def test_rejects_ungrounded_number(self):
        bad = _GOOD.replace("6 no shows per maand.", "99 no shows per maand.")
        ok, r = V(bad, _CHECKUP)
        assert not ok and r.startswith("ungrounded_number")

    def test_rejects_em_dash(self):
        bad = _GOOD.replace("6 no shows per maand.", "6 no shows per maand — te veel.")
        ok, r = V(bad, _CHECKUP)
        assert not ok and r == "dash"

    def test_rejects_pitch(self):
        bad = _GOOD.replace("Conservatief: bij 14 aanvragen is dat veel wachttijd.",
                            "Wij kunnen dit voor je oplossen.")
        ok, r = V(bad, _CHECKUP)
        assert not ok and r == "pitch"

    def test_rejects_wrong_finding_count(self):
        two = _report(
            '<section class="finding"><p class="fact">14 per week.</p></section>'
            '<section class="finding"><p class="fact">6 per maand.</p></section>')
        ok, r = V(two, _CHECKUP)
        assert not ok and r == "finding_count"

    def test_rejects_too_long(self):
        long_html = _GOOD.replace("</article>", "<p>" + "woord " * 420 + "</p></article>")
        ok, r = V(long_html, _CHECKUP)
        assert not ok and r == "too_long"

    def test_empty_rejected(self):
        assert V("", _CHECKUP)[1] == "empty"

    def test_findings_param_overrides_count(self):
        # als de caller de findings-lijst meegeeft, telt die (niet de HTML-regex)
        ok, r = V(_GOOD, _CHECKUP, findings=[1, 2, 3])
        assert ok


class TestGeneratorGuards:
    @pytest.mark.asyncio
    async def test_no_checkup_data_errors(self):
        out = await generate_checkup_report(lead={"company_name": "X"}, checkup_data={}, transcript="t")
        assert out["error"] == "no_checkup_data"

    @pytest.mark.asyncio
    async def test_missing_company_errors(self):
        out = await generate_checkup_report(lead={}, checkup_data=_CHECKUP, transcript="t")
        assert "incomplete_lead_data" in out["error"]
