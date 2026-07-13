"""
tests/test_email_verifier_status.py — remediation C1.

Valideert dat de verifier INFRA-fouten (timeout/connection/exception)
onderscheidt van ADRES-kwaliteit (valid/invalid/risky), en dat de
coarse-mapping infra-statussen fail-closed naar 'not_checked' zet.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from enrichment.email_verifier import (
    _smtp_verify_sync,
    coarse_email_status,
    get_best_email,
)


class _FakeSMTP:
    """Context-manager mock die een configureerbare rcpt-code of exception geeft."""
    def __init__(self, code=None, exc=None):
        self._code, self._exc = code, exc

    def __enter__(self):
        if self._exc:
            raise self._exc
        return self

    def __exit__(self, *a):
        return False

    def connect(self, *a): pass
    def ehlo(self, *a): pass
    def mail(self, *a): pass
    def quit(self): pass
    def rcpt(self, email): return (self._code, b"")


def _run_sync(code=None, exc=None):
    with patch("enrichment.email_verifier.smtplib.SMTP",
               return_value=_FakeSMTP(code=code, exc=exc)):
        return _smtp_verify_sync("a@b.nl", ["mx.b.nl"], timeout=1)


# ── SMTP-code → status (adres-kwaliteit) ────────────────────────────────────

def test_250_is_valid():
    assert _run_sync(code=250) == ("valid", "smtp")


def test_550_is_invalid():
    assert _run_sync(code=550) == ("invalid", "smtp")


def test_greylist_451_is_temporary_not_risky():
    """451 = greylist/tijdelijk → temporary_failure (INFRA), NIET risky."""
    assert _run_sync(code=451) == ("temporary_failure", "smtp")


def test_unknown_code_with_answer_is_risky():
    """Onbekende code MAAR we kregen een RCPT-antwoord → infra werkt → risky."""
    assert _run_sync(code=442) == ("risky", "smtp")


# ── Connectie-fouten → INFRA-status (nooit risky) ───────────────────────────

def test_connection_refused_is_connection_error_not_risky():
    import socket
    st, _ = _run_sync(exc=ConnectionRefusedError())
    assert st == "connection_error"


def test_socket_timeout_is_timeout_not_risky():
    import socket
    st, method = _run_sync(exc=socket.timeout())
    assert st == "timeout"


def test_os_error_is_connection_error():
    st, _ = _run_sync(exc=OSError("no route"))
    assert st == "connection_error"


# ── Coarse-mapping: infra → not_checked (fail-closed) ───────────────────────

def test_coarse_maps_infra_to_not_checked():
    for infra in ("timeout", "connection_error", "temporary_failure"):
        assert coarse_email_status(infra) == "not_checked"


def test_coarse_preserves_address_statuses():
    assert coarse_email_status("valid") == "valid"
    assert coarse_email_status("invalid") == "invalid"
    assert coarse_email_status("risky") == "risky"
    assert coarse_email_status("catchall_risky") == "catchall_risky"


def test_coarse_unknown_fails_closed():
    assert coarse_email_status("iets_geks") == "not_checked"


# ── get_best_email: infra-adres wordt niet als 'bevestigd' gepresenteerd ─────

@pytest.mark.asyncio
async def test_get_best_email_prefers_valid_over_infra():
    async def fake_verify_list(candidates, sb):
        return [
            {"email": "info@b.nl", "status": "connection_error", "method": "connection_error"},
            {"email": "mark@b.nl", "status": "valid", "method": "smtp"},
        ]
    with patch("enrichment.email_verifier.verify_email_list", side_effect=fake_verify_list):
        email, status, method = await get_best_email(["info@b.nl", "mark@b.nl"], None)
    assert (email, status, method) == ("mark@b.nl", "valid", "smtp")


@pytest.mark.asyncio
async def test_get_best_email_returns_infra_status_not_risky():
    """Een gegokt adres dat alleen infra-fouten gaf, komt terug MET zijn
    infra-status (→ coarse not_checked → niet verzendbaar), niet als risky."""
    async def fake_verify_list(candidates, sb):
        return [{"email": "info@b.nl", "status": "timeout", "method": "timeout"}]
    with patch("enrichment.email_verifier.verify_email_list", side_effect=fake_verify_list):
        email, status, method = await get_best_email(["info@b.nl"], None)
    assert status == "timeout"
    assert coarse_email_status(status) == "not_checked"
