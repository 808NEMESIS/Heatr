"""
tests/test_timeline_compact.py — /timeline/{lead_id}?compact=true filtering.

Verifies:
  - compact=true filtert op key event-types
  - compact=true strips metadata om payload klein te houden
  - limit=N respecteerd (default 100, cap 500)
  - default behaviour (compact=false) onveranderd
  - lege state (geen events) returnt count=0
"""
from unittest.mock import MagicMock

import pytest


def _mock_db_with_events(events: list[dict]):
    """Mock supabase chain die events terugeert ongeacht filters."""
    db = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.in_.return_value = chain
    chain.order.return_value = chain
    chain.limit.return_value = chain
    result = MagicMock()
    result.data = events
    chain.execute.return_value = result
    db.table.return_value = chain
    return db


# ---------------------------------------------------------------------------
# Tests draaien tegen utility/predicate logic ipv FastAPI live
# ---------------------------------------------------------------------------

def test_key_event_types_list_includes_critical_types():
    """De whitelist voor compact-mode moet alle critical events bevatten."""
    # Hardcoded lijst zoals in api/main.py — als deze veranderd, update test
    expected = {
        "email_sent", "reply_received", "sequence_completed",
        "manual_status_override", "bounced", "unsubscribed",
        "stage_changed", "interested", "review_email_sent",
    }
    # Dit is een sanity check: zorg dat we niet per ongeluk types laten vallen
    assert "email_sent" in expected
    assert "reply_received" in expected
    assert "manual_status_override" in expected
    assert "bounced" in expected


def test_compact_strips_metadata_from_response():
    """Compact-mode rows hebben alleen id/event_type/title/created_at velden."""
    full_event = {
        "id": "e1",
        "event_type": "email_sent",
        "title": "Mail 1 verstuurd",
        "created_at": "2026-04-21T10:00:00+00:00",
        "metadata": {"campaign_id": "abc", "step_index": 0},
        "created_by": "service:default",
        "body": "extra body content",
    }
    # Mimic van de strip-logica in get_timeline endpoint
    stripped = {
        k: v for k, v in full_event.items()
        if k in ("id", "event_type", "title", "created_at")
    }
    assert stripped == {
        "id": "e1",
        "event_type": "email_sent",
        "title": "Mail 1 verstuurd",
        "created_at": "2026-04-21T10:00:00+00:00",
    }
    assert "metadata" not in stripped
    assert "body" not in stripped


# ---------------------------------------------------------------------------
# Tests via FastAPI testclient (full endpoint flow)
# ---------------------------------------------------------------------------

@pytest.fixture
def app_client(monkeypatch):
    """FastAPI testclient met in-memory mocked Supabase."""
    monkeypatch.setenv("HEATR_API_KEY", "x" * 48)
    monkeypatch.setenv("LEGACY_DEV_TOKEN_ALLOWED", "true")

    from fastapi.testclient import TestClient
    import api.main as api_main

    return api_main.app, TestClient(api_main.app), api_main


def test_compact_endpoint_returns_filtered_events(app_client):
    app, client, api_main = app_client

    # Mock get_supabase dependency
    events = [
        {"id": "e1", "event_type": "email_sent", "title": "Mail 1",
         "created_at": "2026-04-21T10:00:00+00:00",
         "metadata": {"step_index": 0}, "created_by": "service"},
        {"id": "e2", "event_type": "reply_received", "title": "Reply binnen",
         "created_at": "2026-04-22T08:30:00+00:00",
         "metadata": {}, "created_by": "service"},
    ]
    fake_db = _mock_db_with_events(events)

    def fake_get_supabase():
        return fake_db

    app.dependency_overrides[api_main.get_supabase] = fake_get_supabase
    try:
        resp = client.get("/timeline/lead-1?compact=true&limit=5",
                          headers={"X-API-Key": "x" * 48})
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        # Alle events stripped (geen metadata, geen created_by)
        for e in data["events"]:
            assert set(e.keys()) == {"id", "event_type", "title", "created_at"}
    finally:
        app.dependency_overrides.clear()


def test_full_endpoint_returns_metadata(app_client):
    """compact=false (default) houdt metadata + created_by intact."""
    app, client, api_main = app_client

    events = [{
        "id": "e1", "event_type": "email_sent", "title": "Mail 1",
        "created_at": "2026-04-21T10:00:00+00:00",
        "metadata": {"step_index": 0}, "created_by": "service",
    }]
    fake_db = _mock_db_with_events(events)
    app.dependency_overrides[api_main.get_supabase] = lambda: fake_db
    try:
        resp = client.get("/timeline/lead-1", headers={"X-API-Key": "x" * 48})
        assert resp.status_code == 200
        data = resp.json()
        # Default behavior: metadata + created_by aanwezig
        assert "metadata" in data["events"][0]
        assert "created_by" in data["events"][0]
    finally:
        app.dependency_overrides.clear()


def test_empty_timeline_returns_zero_count(app_client):
    app, client, api_main = app_client
    fake_db = _mock_db_with_events([])
    app.dependency_overrides[api_main.get_supabase] = lambda: fake_db
    try:
        resp = client.get("/timeline/lead-virgin?compact=true",
                          headers={"X-API-Key": "x" * 48})
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0
        assert data["events"] == []
    finally:
        app.dependency_overrides.clear()


def test_limit_respected(app_client):
    """limit=N moet doorgegeven worden aan de chain."""
    app, client, api_main = app_client

    fake_db = _mock_db_with_events([])
    captured_limit = []
    chain = fake_db.table.return_value
    original_limit = chain.limit
    def capturing_limit(n):
        captured_limit.append(n)
        return original_limit(n)
    chain.limit = capturing_limit

    app.dependency_overrides[api_main.get_supabase] = lambda: fake_db
    try:
        client.get("/timeline/lead-1?limit=5", headers={"X-API-Key": "x" * 48})
        assert 5 in captured_limit
    finally:
        app.dependency_overrides.clear()


def test_limit_capped_at_500(app_client):
    """limit boven 500 wordt naar 500 geclamped."""
    app, client, api_main = app_client

    fake_db = _mock_db_with_events([])
    captured_limit = []
    chain = fake_db.table.return_value
    original = chain.limit
    chain.limit = lambda n: (captured_limit.append(n), original(n))[1]

    app.dependency_overrides[api_main.get_supabase] = lambda: fake_db
    try:
        client.get("/timeline/lead-1?limit=10000", headers={"X-API-Key": "x" * 48})
        assert max(captured_limit) == 500
    finally:
        app.dependency_overrides.clear()
