"""
tests/test_email_verification_recovery.py — Recovery Patch 2.

Bewijst dat SMTP/MX-e-mailverificatie niet langer 100% 'not_checked'/'not_found'
oplevert. Het exacte defect: de rate-limit-key 'smtp_verify' ontbrak →
wait_for_token() raiste ValueError → gevangen → elke e-mail 'rate_limited'.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.rate_limiter import RATE_LIMITS, consume_token
import enrichment.email_verifier as ev


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# --- Het exacte defect: de key moet bestaan en geldig zijn -------------------

def test_smtp_verify_key_registered():
    assert "smtp_verify" in RATE_LIMITS, "smtp_verify ontbreekt → verificatie is dood"
    cfg = RATE_LIMITS["smtp_verify"]
    assert cfg["max_tokens"] > 0 and cfg["refill_rate"] > 0


class _SeedDB:
    """rate_limit_state-mock: geen bestaande rij → consume_token seed't + True."""
    def table(self, name):
        assert name == "rate_limit_state"
        return self

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def single(self):
        return self

    def upsert(self, *a, **k):
        return self

    def update(self, *a, **k):
        return self

    def execute(self):
        class _R:
            data = None
        return _R()


def test_rate_limiter_accepts_smtp_verify_without_valueerror():
    # Pre-fix raiste dit ValueError('Unknown service smtp_verify').
    assert _run(consume_token("smtp_verify", _SeedDB())) is True


def test_unknown_service_still_raises():
    with pytest.raises(ValueError):
        _run(consume_token("definitely_not_a_service", _SeedDB()))


# --- Verificatie levert weer ECHTE, onderscheiden statussen ------------------

def _patch_pipeline(monkeypatch, *, mx, smtp_result, catchall=False):
    async def _wait(*a, **k):
        return None

    async def _mx(_domain):
        return mx

    async def _cached_catchall(_domain, _db):
        return catchall  # False = geen catchall, ga door naar SMTP

    async def _smtp(_email, _mx, _timeout):
        return smtp_result

    monkeypatch.setattr(ev, "wait_for_token", _wait)
    monkeypatch.setattr(ev, "_get_mx_records", _mx)
    monkeypatch.setattr(ev, "_get_cached_catchall", _cached_catchall)
    monkeypatch.setattr(ev, "_smtp_verify", _smtp)


def test_verify_email_returns_valid(monkeypatch):
    _patch_pipeline(monkeypatch, mx=["mx.test"], smtp_result=("valid", "smtp"))
    status, method = _run(ev.verify_email("info@kliniek.nl", object()))
    assert status == "valid" and method == "smtp"


def test_verify_email_returns_risky(monkeypatch):
    _patch_pipeline(monkeypatch, mx=["mx.test"], smtp_result=("risky", "smtp"))
    status, _ = _run(ev.verify_email("info@kliniek.nl", object()))
    assert status == "risky"


def test_verify_email_invalid_on_no_mx(monkeypatch):
    _patch_pipeline(monkeypatch, mx=[], smtp_result=("valid", "smtp"))
    status, method = _run(ev.verify_email("info@kliniek.nl", object()))
    assert status == "invalid" and method == "mx_check"


def test_verify_email_catchall_flagged(monkeypatch):
    _patch_pipeline(monkeypatch, mx=["mx.test"], smtp_result=("valid", "smtp"), catchall=True)
    status, _ = _run(ev.verify_email("info@kliniek.nl", object()))
    assert status == "catchall_risky"


def test_statuses_stay_distinct(monkeypatch):
    """valid / risky / invalid / catchall_risky komen niet meer allemaal als
    not_checked uit — het defect dat coverage naar 0 bracht."""
    outcomes = set()
    for smtp, mx, catchall in [
        (("valid", "smtp"), ["mx"], False),
        (("risky", "smtp"), ["mx"], False),
        (("valid", "smtp"), [], False),        # → invalid
        (("valid", "smtp"), ["mx"], True),     # → catchall_risky
    ]:
        _patch_pipeline(monkeypatch, mx=mx, smtp_result=smtp, catchall=catchall)
        status, _ = _run(ev.verify_email("info@kliniek.nl", object()))
        outcomes.add(status)
    assert outcomes == {"valid", "risky", "invalid", "catchall_risky"}
    assert "not_checked" not in outcomes
