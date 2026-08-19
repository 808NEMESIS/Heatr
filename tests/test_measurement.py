"""tests/test_measurement.py — het ene meetpad: bruikbaarheid, fallback, provenance.

Borgt de drie lessen van 2026-08: (1) een geblokkeerde/lege respons is GEEN meting
(403-met-body, JS-shell), (2) de Playwright-fallback neemt het over als httpx faalt,
(3) elke meting draagt een herkomst-contract met detector-versie."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from website_intelligence.measurement import (
    DETECTOR_VERSION, measure_conversion, provenance, richness, usable_measurement,
)

# Minimale pagina met échte conversie-signalen (tel-link + boekknop + formulierveld).
RICH_HTML = (
    "<html><body>" + "x" * 400 +
    '<a href="tel:0101234567">010-1234567</a>'
    '<a href="/afspraak-maken">Maak een afspraak</a>'
    '<form><input name="naam"><input name="email"></form>'
    "</body></html>"
)
SHELL_HTML = "<html><body>" + "<div class='app'></div>" * 100 + "</body></html>"


# ── usable_measurement: de drie faalklassen ──────────────────────────────────
@pytest.mark.asyncio
async def test_non_2xx_is_no_measurement():
    usable, result, rich, reason = await usable_measurement("x.nl", 403, RICH_HTML, "alternatieve_geneeskunde")
    assert usable is False and result is None and reason == "http_403"


@pytest.mark.asyncio
async def test_wall_is_no_measurement():
    html = "<html>Checking your browser before accessing" + "x" * 500
    usable, _r, _n, reason = await usable_measurement("x.nl", 200, html, "alternatieve_geneeskunde")
    assert usable is False and reason == "challenge_wall"


@pytest.mark.asyncio
async def test_signal_less_shell_is_no_measurement():
    usable, _r, rich, reason = await usable_measurement("x.nl", 200, SHELL_HTML, "alternatieve_geneeskunde")
    assert usable is False and rich == 0 and reason == "no_content_signals"


@pytest.mark.asyncio
async def test_rich_page_is_measurement():
    usable, result, rich, reason = await usable_measurement("x.nl", 200, RICH_HTML, "alternatieve_geneeskunde")
    assert usable is True and rich >= 1 and reason == "ok"
    assert result.get("has_online_booking") is True     # /afspraak-maken herkend


# ── measure_conversion: fallback + drie-waarden-uitkomst ─────────────────────
class _FakeRenderer:
    def __init__(self, status, html):
        self._s, self._h = status, html
        self.called = 0

    async def fetch(self, _dom):
        self.called += 1
        return self._s, self._h


@pytest.mark.asyncio
async def test_fallback_takes_over_when_httpx_blocked(monkeypatch):
    import website_intelligence.measurement as m
    async def fake_httpx(_d):
        return 403, "<html>gestylede foutpagina" + "x" * 500
    monkeypatch.setattr(m, "fetch_httpx", fake_httpx)
    rend = _FakeRenderer(200, RICH_HTML)
    out = await measure_conversion("x.nl", "alternatieve_geneeskunde", renderer=rend)
    assert rend.called == 1
    assert out["usable"] is True and out["provenance"]["method"] == "playwright"
    assert out["result"]["has_online_booking"] is True


@pytest.mark.asyncio
async def test_unusable_without_renderer_yields_no_result(monkeypatch):
    import website_intelligence.measurement as m
    async def fake_httpx(_d):
        return 403, "<html>blok" + "x" * 500
    monkeypatch.setattr(m, "fetch_httpx", fake_httpx)
    out = await measure_conversion("x.nl", "alternatieve_geneeskunde")
    assert out["usable"] is False and out["result"] is None   # drie-waarden: ONBEKEND
    p = out["provenance"]
    assert p["reason"] == "http_403" and p["content_seen"] is False
    assert p["detector_version"] == DETECTOR_VERSION and p["measured_at"]


@pytest.mark.asyncio
async def test_still_unusable_after_render_stays_unknown(monkeypatch):
    import website_intelligence.measurement as m
    async def fake_httpx(_d):
        return 200, SHELL_HTML                              # JS-shell via httpx
    monkeypatch.setattr(m, "fetch_httpx", fake_httpx)
    rend = _FakeRenderer(200, SHELL_HTML)                   # render helpt ook niet
    out = await measure_conversion("x.nl", "alternatieve_geneeskunde", renderer=rend)
    assert out["usable"] is False and out["result"] is None
    assert out["provenance"]["reason"] == "no_content_signals"


def test_provenance_contract_fields():
    p = provenance(method="httpx", status=200, body_size=1234, content_seen=True, reason="ok")
    assert set(p) == {"method", "status", "body_size", "content_seen", "reason",
                      "detector_version", "measured_at"}


def test_richness_zero_on_empty():
    assert richness(None) == 0 and richness({}) == 0
