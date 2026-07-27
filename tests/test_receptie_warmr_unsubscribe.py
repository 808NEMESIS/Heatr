"""tests/test_receptie_warmr_unsubscribe.py — keuze B: Warmr bezit de afmeldlink.

Twee lagen:
  1. render-gate: privacy blijft HARD vereist; de afmeld-eis vervalt niet stil maar
     wordt Warmr-geleverd (alleen mét de bewuste RECEPTIE_UNSUBSCRIBE_VIA_WARMR-vlag).
  2. post-send-verifier: bestaat de unsubscribe_tokens-rij? Fail-closed bij leesfout.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.receptie_sequence import (
    receptie_compliance_tokens,
    receptie_mail_sendable,
    receptie_unsubscribe_via_warmr,
)
from utils.warmr_unsubscribe import unsubscribe_token_present

_CLEAN_BODY = "Hoi Sami,\n\nEen observatie zonder verboden tekens.\n\nZal ik iets laten zien?\n\nGroet"
_PRIVACY = "Je ontvangt deze mail omdat je praktijk openbaar vindbaar is; zie https://x.nl/privacy"


# ── Laag 1: render-gate ───────────────────────────────────────────────────────

def test_privacy_still_hard_required_even_with_warmr(monkeypatch):
    monkeypatch.setenv("RECEPTIE_UNSUBSCRIBE_VIA_WARMR", "true")
    ok, reason = receptie_mail_sendable(_CLEAN_BODY, privacy_notice="", unsubscribe="",
                                        warmr_owns_unsubscribe=True)
    assert ok is False and reason == "privacy_notice_missing"


def test_warmr_owned_unsubscribe_allows_empty_unsub(monkeypatch):
    # Met de Warmr-vlag mag de Heatr-afmeldlink leeg zijn (Warmr plakt 'm).
    ok, reason = receptie_mail_sendable(_CLEAN_BODY, privacy_notice=_PRIVACY, unsubscribe="",
                                        warmr_owns_unsubscribe=True)
    assert ok is True and reason == "ok"


def test_without_flag_empty_unsub_still_blocks():
    # Zonder de vlag blijft het oude gedrag: lege afmeldlink → geblokkeerd.
    ok, reason = receptie_mail_sendable(_CLEAN_BODY, privacy_notice=_PRIVACY, unsubscribe="",
                                        warmr_owns_unsubscribe=False)
    assert ok is False and reason == "unsubscribe_missing"


@pytest.mark.parametrize("val,expected", [
    ("true", True), ("1", True), ("on", True), ("YES", True),
    ("false", False), ("", False), ("0", False),
])
def test_via_warmr_flag_parsing(monkeypatch, val, expected):
    monkeypatch.setenv("RECEPTIE_UNSUBSCRIBE_VIA_WARMR", val)
    assert receptie_unsubscribe_via_warmr() is expected


def test_compliance_tokens_warmr_mode_yields_no_heatr_link(monkeypatch):
    monkeypatch.setenv("RECEPTIE_PRIVACY_NOTICE", _PRIVACY)
    monkeypatch.setenv("RECEPTIE_UNSUBSCRIBE_VIA_WARMR", "true")
    monkeypatch.setenv("RECEPTIE_UNSUBSCRIBE_TEMPLATE", "Afmelden: https://x.nl/u/{id}")  # moet genegeerd worden
    privacy, unsub = receptie_compliance_tokens({"id": "L1", "email": "a@b.nl"})
    assert privacy == _PRIVACY
    assert unsub == ""                      # Heatr levert GEEN link in Warmr-modus


def test_compliance_tokens_heatr_mode_still_builds_link(monkeypatch):
    monkeypatch.delenv("RECEPTIE_UNSUBSCRIBE_VIA_WARMR", raising=False)
    monkeypatch.setenv("RECEPTIE_PRIVACY_NOTICE", _PRIVACY)
    monkeypatch.setenv("RECEPTIE_UNSUBSCRIBE_TEMPLATE", "Afmelden: https://x.nl/u/{id}")
    _, unsub = receptie_compliance_tokens({"id": "L1", "email": "a@b.nl"})
    assert unsub == "Afmelden: https://x.nl/u/L1"


# ── Laag 2: post-send-verifier ────────────────────────────────────────────────

class _FakeQuery:
    def __init__(self, rows, raise_=False):
        self._rows, self._raise = rows, raise_

    def select(self, *a, **k): return self
    def eq(self, *a, **k): return self
    def order(self, *a, **k): return self
    def limit(self, *a, **k): return self

    def execute(self):
        if self._raise:
            raise RuntimeError("REST timeout")
        class _R:
            data = self._rows
        _R.data = self._rows
        return _R()


class _FakeSB:
    def __init__(self, rows, raise_=False):
        self._rows, self._raise = rows, raise_

    def table(self, _name):
        return _FakeQuery(self._rows, self._raise)


def test_verifier_present():
    sb = _FakeSB([{"id": "t1", "token": "abcdef123456", "used": False, "campaign_id": "C1"}])
    ok, detail = unsubscribe_token_present(sb, "L1", "C1")
    assert ok is True and "token aanwezig" in detail


def test_verifier_absent_flags():
    sb = _FakeSB([])
    ok, detail = unsubscribe_token_present(sb, "L1", "C1")
    assert ok is False and "geen unsubscribe_tokens-rij" in detail


def test_verifier_read_error_is_fail_closed():
    sb = _FakeSB([], raise_=True)
    ok, detail = unsubscribe_token_present(sb, "L1", "C1")
    assert ok is False and "niet leesbaar" in detail


def test_verifier_requires_lead_id():
    ok, detail = unsubscribe_token_present(_FakeSB([]), "", "C1")
    assert ok is False and "geen lead_id" in detail
