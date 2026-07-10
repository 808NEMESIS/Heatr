"""
tests/test_auth.py — Auth dependencies (X-API-Key, Supabase JWT, legacy).

Verifies get_workspace + require_service_key reject/accept matrix without spinning
up the full FastAPI app. Test against the dependency functions directly.

Run: pytest tests/test_auth.py -v
"""
from __future__ import annotations

import os
import secrets
from unittest.mock import MagicMock

import jwt as _jwt
import pytest
from fastapi import HTTPException


@pytest.fixture
def api_key(monkeypatch):
    key = secrets.token_urlsafe(48)
    monkeypatch.setenv("HEATR_API_KEY", key)
    return key


@pytest.fixture
def jwt_secret(monkeypatch):
    secret = "test-jwt-secret-" + secrets.token_urlsafe(16)
    monkeypatch.setenv("SUPABASE_JWT_SECRET", secret)
    return secret


def _make_request(headers: dict[str, str] | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = headers or {}
    return req


# ---------------------------------------------------------------------------
# X-API-Key path
# ---------------------------------------------------------------------------

def test_service_key_correct_match_returns_workspace(api_key, monkeypatch):
    monkeypatch.delenv("LEGACY_DEV_TOKEN_ALLOWED", raising=False)
    from api.main import _service_key_workspace
    req = _make_request({"X-API-Key": api_key})
    assert _service_key_workspace(req) == "aerys"


def test_service_key_wrong_value_returns_none(api_key):
    from api.main import _service_key_workspace
    req = _make_request({"X-API-Key": "totally-wrong-key"})
    assert _service_key_workspace(req) is None


def test_service_key_too_short_env_rejected(monkeypatch):
    monkeypatch.setenv("HEATR_API_KEY", "tooshort")
    from api.main import _service_key_workspace
    req = _make_request({"X-API-Key": "tooshort"})
    assert _service_key_workspace(req) is None


def test_service_key_missing_header_returns_none(api_key):
    from api.main import _service_key_workspace
    assert _service_key_workspace(_make_request()) is None


# ---------------------------------------------------------------------------
# Supabase JWT path
# ---------------------------------------------------------------------------

def test_jwt_valid_returns_workspace(jwt_secret):
    from api.main import _jwt_workspace
    token = _jwt.encode(
        {"sub": "user-id", "aud": "authenticated", "app_metadata": {"workspace_id": "tenant-x"}},
        jwt_secret, algorithm="HS256",
    )
    req = _make_request({"Authorization": f"Bearer {token}"})
    assert _jwt_workspace(req) == "tenant-x"


def test_jwt_no_workspace_claim_rejected_fail_closed(jwt_secret, monkeypatch):
    """OMGEKEERD in fase 4 PR 12 (audit v2 scenario 11): een geldige JWT
    zonder workspace_id-claim kreeg voorheen stilzwijgend DEFAULT_WORKSPACE
    en daarmee volledige toegang tot de aerys-data. Nu: geweigerd."""
    monkeypatch.delenv("HEATR_JWT_WORKSPACE_FALLBACK", raising=False)
    from api.main import _jwt_workspace
    token = _jwt.encode(
        {"sub": "user-id", "aud": "authenticated"},
        jwt_secret, algorithm="HS256",
    )
    req = _make_request({"Authorization": f"Bearer {token}"})
    assert _jwt_workspace(req) is None


def test_jwt_no_claim_allowed_via_explicit_cutover_flag(jwt_secret, monkeypatch):
    """De oude fallback bestaat alleen nog achter de expliciete flag —
    voor de frontend-cutover tot de claims geprovisioned zijn."""
    monkeypatch.setenv("HEATR_JWT_WORKSPACE_FALLBACK", "true")
    from api.main import _jwt_workspace
    token = _jwt.encode(
        {"sub": "user-id", "aud": "authenticated"},
        jwt_secret, algorithm="HS256",
    )
    req = _make_request({"Authorization": f"Bearer {token}"})
    assert _jwt_workspace(req) == "aerys"


def test_jwt_wrong_secret_rejected(jwt_secret):
    from api.main import _jwt_workspace
    token = _jwt.encode(
        {"sub": "x", "aud": "authenticated"}, "DIFFERENT-secret", algorithm="HS256",
    )
    req = _make_request({"Authorization": f"Bearer {token}"})
    assert _jwt_workspace(req) is None


def test_jwt_wrong_audience_rejected(jwt_secret):
    from api.main import _jwt_workspace
    token = _jwt.encode(
        {"sub": "x", "aud": "anon"}, jwt_secret, algorithm="HS256",
    )
    req = _make_request({"Authorization": f"Bearer {token}"})
    assert _jwt_workspace(req) is None


def test_jwt_missing_secret_env_rejects_all(monkeypatch):
    monkeypatch.delenv("SUPABASE_JWT_SECRET", raising=False)
    from api.main import _jwt_workspace
    req = _make_request({"Authorization": "Bearer anything.at.all"})
    assert _jwt_workspace(req) is None


# ---------------------------------------------------------------------------
# Legacy dev-token path
# ---------------------------------------------------------------------------

def test_legacy_off_by_default(monkeypatch):
    monkeypatch.delenv("LEGACY_DEV_TOKEN_ALLOWED", raising=False)
    from api.main import _legacy_dev_token
    req = _make_request({"Authorization": "Bearer dev-token"})
    assert _legacy_dev_token(req) is None


def test_legacy_on_accepts_literal_dev_token(monkeypatch):
    monkeypatch.setenv("LEGACY_DEV_TOKEN_ALLOWED", "true")
    from api.main import _legacy_dev_token
    req = _make_request({"Authorization": "Bearer dev-token"})
    assert _legacy_dev_token(req) == "aerys"


def test_legacy_on_rejects_arbitrary_bearer(monkeypatch):
    """Recovery security-fix: met de flag AAN mag alléén het letterlijke
    'dev-token' door — geen willekeurige Bearer-string meer (voorheen een
    volledige auth-bypass)."""
    monkeypatch.setenv("LEGACY_DEV_TOKEN_ALLOWED", "true")
    from api.main import _legacy_dev_token
    for bogus in ("hunter2xx", "letmein", "Bearer", "dev-token2", "xxxxxxxx"):
        req = _make_request({"Authorization": f"Bearer {bogus}"})
        assert _legacy_dev_token(req) is None, f"{bogus!r} zou geweigerd moeten worden"


# ---------------------------------------------------------------------------
# Combined get_workspace flow
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_workspace_no_auth_raises_401(monkeypatch):
    monkeypatch.delenv("LEGACY_DEV_TOKEN_ALLOWED", raising=False)
    from api.main import get_workspace
    with pytest.raises(HTTPException) as exc:
        await get_workspace(_make_request())
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_get_workspace_service_key_wins(api_key, monkeypatch):
    monkeypatch.delenv("LEGACY_DEV_TOKEN_ALLOWED", raising=False)
    from api.main import get_workspace
    ws = await get_workspace(_make_request({"X-API-Key": api_key}))
    assert ws == "aerys"


# ---------------------------------------------------------------------------
# require_service_key (stricter)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_require_service_key_rejects_jwt_only(jwt_secret):
    from api.main import require_service_key
    token = _jwt.encode({"sub": "x", "aud": "authenticated"}, jwt_secret, algorithm="HS256")
    with pytest.raises(HTTPException) as exc:
        await require_service_key(_make_request({"Authorization": f"Bearer {token}"}))
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_require_service_key_rejects_legacy(monkeypatch):
    monkeypatch.setenv("LEGACY_DEV_TOKEN_ALLOWED", "true")
    from api.main import require_service_key
    with pytest.raises(HTTPException) as exc:
        await require_service_key(_make_request({"Authorization": "Bearer dev-token"}))
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_require_service_key_accepts_correct_key(api_key):
    from api.main import require_service_key
    ws = await require_service_key(_make_request({"X-API-Key": api_key}))
    assert ws == "aerys"
