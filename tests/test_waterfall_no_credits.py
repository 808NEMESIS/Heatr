"""tests/test_waterfall_no_credits.py — gevonden e-mail overleeft een Bouncer-tegoed-op.

Regressie 2026-08-02: Bouncer 402 → verify_api gaf (correct) status 'not_checked',
maar get_best_email kende 'not_checked' niet in _INFRA_STATUSES → een GEVONDEN adres
werd weggegooid en de lead kreeg 'not_found' (43/43 op de Breda-sweep, terwijl de
sites gewoon info@ tonen). Pint: (1) not_checked-adressen worden teruggegeven als
infra-onbekend; (2) de 402-circuit-breaker slaat vervolg-calls over.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.mark.asyncio
async def test_get_best_email_keeps_not_checked(monkeypatch):
    from enrichment import email_verifier as ev

    async def fake_verify_list(candidates, sb):
        return [{"email": e, "status": "not_checked", "method": "error"} for e in candidates]

    monkeypatch.setattr(ev, "verify_email_list", fake_verify_list)
    email, status, method = await ev.get_best_email(["info@kliniek.nl"], None)
    assert email == "info@kliniek.nl", "gevonden adres mag NIET weggegooid worden bij API-fout"
    assert status == "not_checked"           # fail-closed: niet sendable, wél bewaard


@pytest.mark.asyncio
async def test_verify_api_402_circuit_breaker(monkeypatch):
    import enrichment.verify_api as va

    monkeypatch.setenv("EMAIL_VERIFY_PROVIDER", "bouncer")
    monkeypatch.setenv("BOUNCER_API_KEY", "test-key")
    va._no_credits_until = 0.0

    calls = {"n": 0}

    class _Resp:
        status_code = 402
        def raise_for_status(self): ...
        def json(self): return {}

    class _Client:
        def __init__(self, *a, **k): ...
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def get(self, *a, **k):
            calls["n"] += 1
            return _Resp()

    monkeypatch.setattr(va.httpx, "AsyncClient", _Client)

    r1 = await va.verify_via_api("a@b.nl")
    r2 = await va.verify_via_api("c@d.nl")     # moet de API overslaan
    assert r1["raw_reason"] == "no_credits_402"
    assert r2["raw_reason"] == "no_credits_402_cached"
    assert calls["n"] == 1, "na een 402 mag er geen tweede API-call volgen binnen de TTL"
    va._no_credits_until = 0.0                 # test-hygiëne
