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

    def test_rejects_ungrounded_fact(self):
        # Een verzonnen FEIT-getal (99 komt niet uit de bron) → afgekeurd.
        findings = [
            {"fact": "99 no shows per maand.", "cost": "x"},
            {"fact": "14 aanvragen per week.", "cost": "y"},
            {"fact": "800 euro per patient.", "cost": "z"},
        ]
        ok, r = V(_GOOD, _CHECKUP, findings=findings)
        assert not ok and r == "ungrounded_fact:99"

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


def test_qa_accepts_four_operand_annual_calc():
    # 12/week × 20% × 700 × 52 weken = 87.360 → afgerond 87.000/jaar, moet passen.
    checkup = {"unanswered_inbound_per_week": 12, "value_per_new_patient": 700, "conversion_estimate": 0.20}
    html = _report(
        '<section class="finding"><p class="fact">12 aanvragen per week blijven liggen.</p>'
        '<p class="cost">12 keer 20 procent keer 700 euro keer 52 weken is ruim 87.000 euro per jaar, conservatief.</p></section>'
        '<section class="finding"><p class="fact">700 euro per nieuwe patient.</p><p class="cost">Conservatief gerekend.</p></section>'
        '<section class="finding"><p class="fact">Nog een feit hier.</p><p class="cost">Netjes doorgerekend.</p></section>')
    ok, r = V(html, checkup)
    assert ok, r


def test_qa_still_rejects_invented_benchmark_in_fact():
    # Een verzonnen FEIT ('47% van klinieken') is een claim → moet een bron-getal
    # zijn; is het niet → afgekeurd, ook al lijkt 47 op een rekensom-afleiding.
    checkup = {"unanswered_inbound_per_week": 12, "value_per_new_patient": 700}
    findings = [
        {"fact": "Gemiddeld verliezen klinieken 47 procent van hun leads.", "cost": "x"},
        {"fact": "12 aanvragen per week.", "cost": "y"},
        {"fact": "700 euro per patient.", "cost": "z"},
    ]
    html = _report('<section class="finding"></section>' * 3)
    ok, r = V(html, checkup, findings=findings)
    assert not ok and r == "ungrounded_fact:47"


def test_qa_calc_result_allowed_in_cost():
    # Hetzelfde 47-achtige getal in een REKENSOM (cost) is toegestaan als afleiding.
    checkup = {"unanswered_inbound_per_week": 12, "value_per_new_patient": 700}
    findings = [
        {"fact": "12 aanvragen per week blijven liggen.",
         "cost": "12 keer 700 euro keer 52 weken is ruim 400.000 euro per jaar, conservatief."},
        {"fact": "700 euro per patient.", "cost": "conservatief."},
        {"fact": "Nog een feit.", "cost": "netjes."},
    ]
    html = _report('<section class="finding"></section>' * 3)
    ok, r = V(html, checkup, findings=findings)
    assert ok, r
