"""tests/test_test_send_guard.py — harde end-to-end-verzendtest-guard.

Twee lagen:
  1. de pure guard (utils/testmail_guard): batch-guard, reroute, confirm, no-op.
  2. de dispatcher-integratie: bewijst dat de reroute de send-callable ECHT
     bereikt (het rerouted adres gaat de deur uit) en dat een gemengde batch de
     hele dispatch aborteert vóór er iets verstuurd wordt.

Veiligheidsinvariant onder test: NO-OP als TEST_MODE uit → nul gedragsverandering
in productie; en de guard kan een send alleen blokkeren/herschrijven, nooit
verruimen.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Alias de guard-functies: hun echte namen beginnen met 'test_' (domeinterm
# "test-send"), en pytest zou ze anders als test-functies verzamelen.
from utils.testmail_guard import (  # noqa: E402
    TestSendBlocked,
    enforce_and_reroute,
    test_mode_active as _mode_active,
    test_recipient as _recipient,
)


# ── Laag 1: pure guard ────────────────────────────────────────────────────────

def test_noop_when_test_mode_off(monkeypatch):
    monkeypatch.delenv("TEST_MODE", raising=False)
    targets = [{"id": "1", "email": "echt@kliniek.nl", "is_test_lead": False}]
    assert enforce_and_reroute(targets, confirm_test=False) == []
    assert targets[0]["email"] == "echt@kliniek.nl"   # ongewijzigd
    assert _mode_active() is False


@pytest.mark.parametrize("val,expected", [
    ("1", True), ("true", True), ("TRUE", True), ("yes", True), ("on", True),
    ("0", False), ("false", False), ("", False), ("nee", False),
])
def test_test_mode_active_parsing(monkeypatch, val, expected):
    monkeypatch.setenv("TEST_MODE", val)
    assert _mode_active() is expected


def test_confirm_required_in_test_mode(monkeypatch):
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.setenv("TEST_RECIPIENT", "sami@example.com")
    targets = [{"id": "t", "email": "x@test-aerys.local", "is_test_lead": True}]
    with pytest.raises(TestSendBlocked, match="confirm-test"):
        enforce_and_reroute(targets, confirm_test=False)
    assert targets[0]["email"] == "x@test-aerys.local"   # geen reroute bij block


def test_recipient_required_in_test_mode(monkeypatch):
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.delenv("TEST_RECIPIENT", raising=False)
    targets = [{"id": "t", "email": "x@test-aerys.local", "is_test_lead": True}]
    with pytest.raises(TestSendBlocked, match="TEST_RECIPIENT"):
        enforce_and_reroute(targets, confirm_test=True)


def test_mixed_batch_aborts_whole_batch(monkeypatch):
    # Één echt record tussen de testrecords → de HELE batch wordt geweigerd,
    # en er wordt NIETS gererouteerd (geen halve mutatie).
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.setenv("TEST_RECIPIENT", "sami@example.com")
    targets = [
        {"id": "t1", "email": "a@test-aerys.local", "is_test_lead": True},
        {"id": "echt", "email": "prospect@kliniek.nl", "is_test_lead": False},
    ]
    with pytest.raises(TestSendBlocked, match="hele batch geabort"):
        enforce_and_reroute(targets, confirm_test=True)
    assert targets[0]["email"] == "a@test-aerys.local"      # niets aangeraakt
    assert targets[1]["email"] == "prospect@kliniek.nl"


def test_reroute_rewrites_all_test_targets(monkeypatch):
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.setenv("TEST_RECIPIENT", "sami-jansema@hotmail.com")
    targets = [
        {"id": "t1", "email": "a@test-aerys.local", "is_test_lead": True},
        {"id": "t2", "email": "b@test-aerys.local", "is_test_lead": True},
    ]
    originals = enforce_and_reroute(targets, confirm_test=True)
    assert originals == ["a@test-aerys.local", "b@test-aerys.local"]
    assert all(t["email"] == "sami-jansema@hotmail.com" for t in targets)


def test_recipient_getter(monkeypatch):
    monkeypatch.setenv("TEST_RECIPIENT", "  sami@example.com  ")
    assert _recipient() == "sami@example.com"
    monkeypatch.delenv("TEST_RECIPIENT", raising=False)
    assert _recipient() == ""


# ── Laag 2: dispatcher-integratie ─────────────────────────────────────────────

def _run(coro):
    """Privé event-loop: raakt de gedeelde pytest-asyncio-loop niet."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _FakeTable:
    """Minimale supabase-tabel-stub: insert geeft een id terug, update no-op."""
    def __init__(self):
        self._payload = None

    def insert(self, row):
        self._payload = row
        return self

    def update(self, _row):
        return self

    def eq(self, *_a, **_k):
        return self

    def in_(self, *_a, **_k):
        return self

    def select(self, *_a, **_k):
        return self

    def limit(self, *_a, **_k):
        return self

    def execute(self):
        class _R:
            data = [{"id": "rec-1", "status": "in_flight"}]
        return _R()


class _FakeSB:
    def table(self, _name):
        return _FakeTable()


def _patch_gates(monkeypatch):
    """Compliance + suppressie hebben eigen tests; hier no-op zodat we puur de
    guard meten (niet die gates opnieuw nabootsen)."""
    import utils.outbound_dispatcher as od
    monkeypatch.setattr(od, "compliance_check", lambda _t: (True, "ok"))
    monkeypatch.setattr(od, "check_suppressed", lambda _sb, _emails: set())


def _dispatch(targets, *, confirm_test, sent_box):
    from utils.outbound_dispatcher import dispatch_outbound

    async def _send():
        # Leest de (mogelijk gererouteerde) e-mail van het eerste target op het
        # verzendmoment — precies zoals push_lead(lead) dat doet.
        sent_box.append(targets[0]["email"])
        return {"ok": True}

    return _run(dispatch_outbound(
        kind="warmr_push",
        idempotency_key="test-key-1",
        actor="test:guard",
        leads=targets,
        send=_send,
        supabase_client=_FakeSB(),
        workspace_id="aerys",
        confirm_test=confirm_test,
    ))


def test_dispatch_reroutes_actual_recipient(monkeypatch):
    # TEST_MODE + allowlist op het testadres + kill-switch aan (ephemeer) →
    # de send-callable stuurt naar het GEREROUTEERDE adres, niet het origineel.
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.setenv("TEST_RECIPIENT", "sami-jansema@hotmail.com")
    monkeypatch.setenv("HEATR_SEND_ALLOWLIST", "sami-jansema@hotmail.com")
    monkeypatch.setenv("ENABLE_PROSPECT_SENDS", "true")
    _patch_gates(monkeypatch)
    targets = [{"id": "t", "email": "iemand@test-aerys.local", "is_test_lead": True,
                "email_status": "valid", "gdpr_safe": True}]
    sent_box: list[str] = []
    disp = _dispatch(targets, confirm_test=True, sent_box=sent_box)
    assert disp.executed is True
    assert sent_box == ["sami-jansema@hotmail.com"]   # rerouted adres ging de deur uit


def test_dispatch_mixed_batch_blocks_before_send(monkeypatch):
    # Gemengde batch in TEST_MODE → DispatchHalted, en de send-callable draait NOOIT.
    from utils.outbound_dispatcher import DispatchHalted
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.setenv("TEST_RECIPIENT", "sami-jansema@hotmail.com")
    monkeypatch.setenv("ENABLE_PROSPECT_SENDS", "true")
    targets = [
        {"id": "t", "email": "a@test-aerys.local", "is_test_lead": True},
        {"id": "echt", "email": "prospect@kliniek.nl", "is_test_lead": False},
    ]
    sent_box: list[str] = []
    with pytest.raises(DispatchHalted, match="hele batch geabort"):
        _dispatch(targets, confirm_test=True, sent_box=sent_box)
    assert sent_box == []   # niets verstuurd
    assert targets[1]["email"] == "prospect@kliniek.nl"   # echt record onaangeraakt


def _build_payload(lead: dict) -> dict:
    """Direct _build_lead_payload (zonder live WarmrClient-init), als test_warmr_test_mode."""
    from integrations.warmr_client import WarmrClient
    client = WarmrClient.__new__(WarmrClient)
    client._sb = None
    client.workspace_id = "aerys"
    return client._build_lead_payload(lead, campaign_id="test-campaign", preferred_inbox_id=None)


def test_reroute_wins_over_017_bcc(monkeypatch):
    """KERN (Sami keuze A): een record dat ZOWEL de 017-BCC-flow ALS de reroute-
    guard triggert, mag NOOIT resulteren in mail naar het originele to: of naar de
    017-BCC-bestemming — alleen naar TEST_RECIPIENT.

    Reroute overschrijft to: én zet _testmail_rerouted, waardoor _build_lead_payload
    de 017-BCC (en de [TEST]-prefix) onderdrukt. Bewijs: payload.email == TEST_RECIPIENT,
    geen bcc, geen subject_prefix, en het originele adres komt nergens in de payload voor.
    """
    monkeypatch.setenv("TEST_MODE", "1")
    monkeypatch.setenv("TEST_RECIPIENT", "info@aeryssolution.nl")
    monkeypatch.setenv("HEATR_TEST_BCC_EMAIL", "intern-bcc@aerys.nl")   # 017-trigger AAN
    lead = {
        "id": "test-both", "email": "origineel@test-aerys.local",
        "is_test_lead": True,   # triggert 017-BCC
        "contact_first_name": "Sami", "company_name": "Testkliniek",
    }
    # 1. reroute-guard draait (zoals in de dispatcher) → to: herschreven + vlag gezet.
    originals = enforce_and_reroute([lead], confirm_test=True)
    assert originals == ["origineel@test-aerys.local"]
    assert lead["email"] == "info@aeryssolution.nl"
    assert lead["_testmail_rerouted"] is True

    # 2. Warmr-payload gebouwd → BCC + prefix onderdrukt, alleen TEST_RECIPIENT.
    payload = _build_payload(lead)
    assert payload["email"] == "info@aeryssolution.nl"     # reroute won voor to:
    assert "bcc" not in payload                            # 017-BCC onderdrukt
    assert "subject_prefix" not in payload                 # [TEST]-prefix mee-onderdrukt
    # Het originele adres én de BCC-bestemming komen NERGENS in de payload voor:
    blob = repr(payload)
    assert "origineel@test-aerys.local" not in blob
    assert "intern-bcc@aerys.nl" not in blob


def test_017_bcc_still_works_without_reroute(monkeypatch):
    """Regressiewacht: buiten TEST_MODE + zonder reroute-vlag blijft 017-BCC intact."""
    monkeypatch.delenv("TEST_MODE", raising=False)
    monkeypatch.setenv("HEATR_TEST_BCC_EMAIL", "intern-bcc@aerys.nl")
    lead = {"id": "x", "email": "real@prospect.nl", "is_test_lead": True}
    payload = _build_payload(lead)
    assert payload["bcc"] == "intern-bcc@aerys.nl"         # 017 ongemoeid
    assert payload["subject_prefix"] == "[TEST] "


def test_dispatch_unchanged_when_test_mode_off(monkeypatch):
    # TEST_MODE uit → de guard is no-op; een niet-allowlisted echt adres volgt
    # het normale pad (hier: kill-switch aan, geen allowlist → gewoon versturen).
    monkeypatch.delenv("TEST_MODE", raising=False)
    monkeypatch.delenv("HEATR_SEND_ALLOWLIST", raising=False)
    monkeypatch.setenv("ENABLE_PROSPECT_SENDS", "true")
    _patch_gates(monkeypatch)
    targets = [{"id": "p", "email": "prospect@kliniek.nl", "is_test_lead": False,
                "email_status": "valid", "gdpr_safe": True}]
    sent_box: list[str] = []
    disp = _dispatch(targets, confirm_test=False, sent_box=sent_box)
    assert disp.executed is True
    assert sent_box == ["prospect@kliniek.nl"]   # geen reroute in productie
