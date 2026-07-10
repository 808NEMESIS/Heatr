"""
tests/test_email_sendability.py — sendability matrix.
"""
from utils.email_sendability import filter_sendable_leads, is_sendable


def test_verified_is_sendable():
    ok, reason = is_sendable("a@b.nl", "verified")
    assert ok is True
    assert reason == "verified"


def test_valid_alias_is_sendable():
    ok, _ = is_sendable("a@b.nl", "valid")
    assert ok


def test_bounced_never_sendable():
    ok, reason = is_sendable("a@b.nl", "bounced")
    assert ok is False
    assert "bounced" in reason


def test_unsubscribed_never_sendable():
    ok, _ = is_sendable("a@b.nl", "unsubscribed")
    assert ok is False


def test_not_found_never_sendable():
    ok, _ = is_sendable("a@b.nl", "not_found")
    assert ok is False


def test_risky_accepted_by_default():
    """Pad A: HEATR_ALLOW_RISKY_EMAILS default true."""
    ok, reason = is_sendable("a@b.nl", "risky")
    assert ok is True
    assert "risky" in reason


def test_risky_rejected_when_disabled():
    ok, reason = is_sendable("a@b.nl", "risky", allow_risky=False)
    assert ok is False
    assert "risky_rejected" in reason


def test_catchall_treated_like_risky():
    ok, _ = is_sendable("a@b.nl", "catchall", allow_risky=True)
    assert ok
    ok, _ = is_sendable("a@b.nl", "catchall", allow_risky=False)
    assert not ok


def test_role_email_accepted_when_status_missing():
    """info@/contact@ zonder status → sendable (role_email)."""
    ok, reason = is_sendable("info@kliniek.nl", None)
    assert ok is True
    assert "role_email" in reason


def test_role_email_rejected_when_disabled():
    ok, _ = is_sendable("info@kliniek.nl", None, allow_role_emails=False)
    assert ok is False


def test_personal_email_no_status_rejected():
    """Persoonlijk email zonder verifier-status → niet sendable."""
    ok, reason = is_sendable("mark@kliniek.nl", None)
    assert ok is False
    assert reason == "not_verified"


def test_no_email_returns_false():
    assert is_sendable(None, "verified") == (False, "no_email")
    assert is_sendable("", "verified") == (False, "no_email")
    assert is_sendable("garbage", "verified") == (False, "no_email")


def test_env_var_disables_risky(monkeypatch):
    monkeypatch.setenv("HEATR_ALLOW_RISKY_EMAILS", "false")
    ok, _ = is_sendable("a@b.nl", "risky")
    assert ok is False


def test_filter_splits_correctly():
    leads = [
        {"id": "1", "email": "a@b.nl", "email_status": "verified"},
        {"id": "2", "email": "b@b.nl", "email_status": "bounced"},
        {"id": "3", "email": "info@b.nl", "email_status": None},
        {"id": "4", "email": "c@b.nl", "email_status": "risky"},
    ]
    sendable, unsendable = filter_sendable_leads(leads)
    assert {l["id"] for l in sendable} == {"1", "3", "4"}
    assert {l["id"] for l in unsendable} == {"2"}
    # _sendability_reason is gezet op alle
    assert all("_sendability_reason" in l for l in sendable + unsendable)


# ---------------------------------------------------------------------------
# Test-mode bypass (feature: is_test_lead)
# ---------------------------------------------------------------------------

def test_test_lead_does_NOT_bypass_bounced_status():
    """OMGEKEERD in fase 2 (audit v2 P0-2): suppressie wint van de
    test-bypass. Een bounce is een deliverability-feit — geen testvlag
    mag daaroverheen."""
    ok, reason = is_sendable("a@b.nl", "bounced", is_test_lead=True)
    assert ok is False
    assert reason == "status:bounced"


def test_test_lead_does_NOT_bypass_unsubscribed_status():
    """OMGEKEERD in fase 2: unsubscribe is een belofte aan de ontvanger —
    geldt ook voor test-gemarkeerde leads."""
    ok, reason = is_sendable("a@b.nl", "unsubscribed", is_test_lead=True)
    assert ok is False
    assert reason == "status:unsubscribed"


def test_test_lead_does_NOT_bypass_blocked_status():
    ok, reason = is_sendable("a@b.nl", "blocked", is_test_lead=True)
    assert ok is False


def test_test_lead_bypasses_not_found_status():
    """Verificatie-statussen (not_found/risky/pending) mag de test-bypass
    WEL passeren — dat is het doel van smoke-tests op een eigen adres."""
    ok, reason = is_sendable("a@b.nl", "not_found", is_test_lead=True)
    assert ok is True


def test_test_lead_bypasses_pending_status():
    ok, reason = is_sendable("a@b.nl", "pending", is_test_lead=True)
    assert ok is True
    assert reason == "test_lead_bypass"


def test_test_lead_still_requires_email():
    """Zelfs test-leads moeten een geldig email hebben."""
    ok, reason = is_sendable(None, "verified", is_test_lead=True)
    assert ok is False
    assert reason == "no_email"
    ok, reason = is_sendable("", "verified", is_test_lead=True)
    assert ok is False
    ok, reason = is_sendable("garbage", "verified", is_test_lead=True)
    assert ok is False


def test_test_lead_default_false_does_not_bypass():
    """Zonder expliciet is_test_lead=True moet bounced gewoon afgewezen worden."""
    ok, reason = is_sendable("a@b.nl", "bounced")
    assert ok is False


def test_filter_honors_test_lead_flag():
    """Fase 2: de vlag bypasst verificatie-statussen, maar suppressie
    (bounced) blokkeert óók test-leads."""
    leads = [
        {"id": "1", "email": "a@b.nl", "email_status": "not_found"},
        {"id": "2", "email": "b@b.nl", "email_status": "not_found", "is_test_lead": True},
        {"id": "3", "email": "c@b.nl", "email_status": "bounced", "is_test_lead": True},
    ]
    sendable, unsendable = filter_sendable_leads(leads)
    assert {l["id"] for l in sendable} == {"2"}
    assert {l["id"] for l in unsendable} == {"1", "3"}
    assert sendable[0]["_sendability_reason"] == "test_lead_bypass"
