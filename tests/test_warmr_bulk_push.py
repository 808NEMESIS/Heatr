"""
tests/test_warmr_bulk_push.py — push_leads_bulk payload + bookkeeping.

Verifieert dat:
  - campaign_id wordt top-level meegestuurd in /leads/bulk body (Warmr-side
    BulkLeadIn schema vereist dit, zonder geeft Warmr silent skip op
    campaign_leads.insert).
  - Na succesvolle push wordt heatr_leads bookkeeping bijgewerkt
    (warmr_lead_id, pushed_to_warmr_at, status, preferred_inbox_id).
  - Empty/missing `inserted` array in Warmr-response → graceful fallback,
    log warning, geen crash.
  - Lead met email die niet in Warmr's `inserted` voorkomt (duplicate of
    suppressed lead-side) → geen update, geen exception.
  - Email-matching is case-insensitive.

Run:
    pytest tests/test_warmr_bulk_push.py -v
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import MagicMock

import pytest

from integrations.warmr_client import WarmrClient


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _make_client(supabase=None) -> WarmrClient:
    """Bouw WarmrClient zonder het auth-checks pad."""
    client = WarmrClient.__new__(WarmrClient)
    client.api_url = "http://localhost:8000/api/v1"
    client.api_key = "wrmr_test_key"
    client._sb = supabase
    client.workspace_id = "aerys"
    return client


class _CapturingRequest:
    """Mock for _request(): captures call args + returns a configured response."""

    def __init__(self, response: dict):
        self.response = response
        self.calls: list[dict] = []

    async def __call__(self, method: str, path: str, **kwargs):
        self.calls.append({"method": method, "path": path, **kwargs})
        return self.response


class _FakeSb:
    """Records every .table().update().eq().execute() chain made on it."""

    def __init__(self):
        self.updates: list[dict] = []

    def table(self, name: str):
        self._table = name
        return self

    def update(self, patch: dict):
        self._patch = patch
        return self

    def eq(self, col: str, val):
        self._filter = (col, val)
        return self

    def execute(self):
        self.updates.append({
            "table":  self._table,
            "patch":  self._patch,
            "filter": self._filter,
        })

        class _Resp:
            data = [{}]
        return _Resp()


# ─────────────────────────────────────────────────────────────────────
# 1. campaign_id MUST be top-level in /leads/bulk body
# ─────────────────────────────────────────────────────────────────────

def test_bulk_push_sends_campaign_id_at_top_level():
    """Without top-level campaign_id, Warmr silently skips campaign_leads insert."""
    client = _make_client(supabase=_FakeSb())
    capture = _CapturingRequest({
        "pushed": 1, "failed": 0, "duplicates": 0,
        "inserted": [{"email": "a@x.nl", "lead_id": "wid-1"}],
    })
    client._request = capture

    leads = [{"id": "h1", "email": "a@x.nl"}]
    asyncio.run(client.push_leads_bulk(leads, campaign_id="campaign-uuid-123"))

    assert len(capture.calls) == 1
    call = capture.calls[0]
    assert call["path"] == "/leads/bulk"
    body = call["json"]
    assert body["campaign_id"] == "campaign-uuid-123", (
        "campaign_id MUST be at top level — Warmr's BulkLeadIn reads it there"
    )
    assert isinstance(body["leads"], list)
    assert len(body["leads"]) == 1


# ─────────────────────────────────────────────────────────────────────
# 2. Bookkeeping written from response.inserted
# ─────────────────────────────────────────────────────────────────────

def test_bulk_push_writes_back_bookkeeping_per_lead():
    sb = _FakeSb()
    client = _make_client(supabase=sb)
    client._request = _CapturingRequest({
        "pushed": 2, "failed": 0, "duplicates": 0,
        "inserted": [
            {"email": "alice@example.nl", "lead_id": "wid-a"},
            {"email": "bob@example.nl",   "lead_id": "wid-b"},
        ],
    })

    leads = [
        {"id": "h-alice", "email": "alice@example.nl", "preferred_inbox_id": "ib-1"},
        {"id": "h-bob",   "email": "bob@example.nl",   "preferred_inbox_id": "ib-2"},
    ]
    asyncio.run(client.push_leads_bulk(leads, campaign_id="c-1"))

    assert len(sb.updates) == 2
    by_id = {u["filter"][1]: u["patch"] for u in sb.updates}
    assert by_id["h-alice"]["warmr_lead_id"] == "wid-a"
    assert by_id["h-alice"]["status"] == "pushed_to_warmr"
    assert by_id["h-alice"]["preferred_inbox_id"] == "ib-1"
    assert "pushed_to_warmr_at" in by_id["h-alice"]
    assert by_id["h-bob"]["warmr_lead_id"] == "wid-b"
    assert by_id["h-bob"]["preferred_inbox_id"] == "ib-2"


# ─────────────────────────────────────────────────────────────────────
# 3. Email matching is case-insensitive
# ─────────────────────────────────────────────────────────────────────

def test_bulk_push_email_match_is_case_insensitive():
    sb = _FakeSb()
    client = _make_client(supabase=sb)
    client._request = _CapturingRequest({
        "pushed": 1, "failed": 0, "duplicates": 0,
        "inserted": [{"email": "alice@example.nl", "lead_id": "wid-a"}],
    })

    leads = [{"id": "h-alice", "email": "Alice@Example.NL"}]
    asyncio.run(client.push_leads_bulk(leads, campaign_id="c-1"))

    assert len(sb.updates) == 1
    assert sb.updates[0]["patch"]["warmr_lead_id"] == "wid-a"


# ─────────────────────────────────────────────────────────────────────
# 4. Empty inserted (older Warmr without the field) — graceful + log
# ─────────────────────────────────────────────────────────────────────

def test_bulk_push_missing_inserted_field_logs_warning_and_continues(caplog):
    sb = _FakeSb()
    client = _make_client(supabase=sb)
    # Pre-May-2026 Warmr response shape: no 'inserted' key at all.
    client._request = _CapturingRequest({
        "pushed": 1, "failed": 0, "duplicates": 0,
    })

    leads = [{"id": "h1", "email": "x@y.nl"}]
    with caplog.at_level(logging.WARNING, logger="integrations.warmr_client"):
        asyncio.run(client.push_leads_bulk(leads, campaign_id="c-1"))

    # No crash, no bookkeeping written
    assert len(sb.updates) == 0
    # Warning surfaced
    assert any("inserted" in rec.message.lower() for rec in caplog.records)


def test_bulk_push_empty_inserted_list_silent_no_writeback():
    """Empty list (vs missing key) = all dups/suppressed. No warning needed."""
    sb = _FakeSb()
    client = _make_client(supabase=sb)
    client._request = _CapturingRequest({
        "pushed":     0,
        "failed":     0,
        "duplicates": 2,
        "inserted":   [],
    })

    leads = [{"id": "h1", "email": "x@y.nl"}, {"id": "h2", "email": "z@y.nl"}]
    asyncio.run(client.push_leads_bulk(leads, campaign_id="c-1"))

    assert len(sb.updates) == 0


# ─────────────────────────────────────────────────────────────────────
# 5. Lead absent from Warmr's inserted list (duplicate/suppressed) — skip
# ─────────────────────────────────────────────────────────────────────

def test_bulk_push_lead_absent_from_inserted_is_skipped():
    """A lead in the input chunk but not in Warmr's inserted = likely
    duplicate or suppressed. Should be silently skipped (no exception,
    no bookkeeping write)."""
    sb = _FakeSb()
    client = _make_client(supabase=sb)
    client._request = _CapturingRequest({
        "pushed": 1, "failed": 0, "duplicates": 1,
        "inserted": [{"email": "alice@example.nl", "lead_id": "wid-a"}],
    })

    leads = [
        {"id": "h-alice", "email": "alice@example.nl"},
        {"id": "h-bob",   "email": "bob@example.nl"},   # duplicate at Warmr-side
    ]
    asyncio.run(client.push_leads_bulk(leads, campaign_id="c-1"))

    # Only Alice gets bookkeeping; Bob is silently skipped.
    assert len(sb.updates) == 1
    assert sb.updates[0]["filter"] == ("id", "h-alice")


# ─────────────────────────────────────────────────────────────────────
# 6. No-supabase mode (test invocation) — no crash
# ─────────────────────────────────────────────────────────────────────

def test_bulk_push_without_supabase_client_does_not_crash():
    """WarmrClient initialized without _sb (test mode) must skip writeback
    without raising."""
    client = _make_client(supabase=None)
    client._request = _CapturingRequest({
        "pushed": 1, "failed": 0, "duplicates": 0,
        "inserted": [{"email": "a@x.nl", "lead_id": "wid-1"}],
    })

    leads = [{"id": "h1", "email": "a@x.nl"}]
    # Must not raise
    result = asyncio.run(client.push_leads_bulk(leads, campaign_id="c-1"))
    assert result["pushed"] == 1
