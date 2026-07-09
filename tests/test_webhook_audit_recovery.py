"""
tests/test_webhook_audit_recovery.py — Recovery Patch 7.

Bewijst dat de Warmr-webhook de inbound-reply-audit niet meer stil verliest:
  - reply_inbox-insert gebruikt de ECHTE kolommen (body, from_email) en NIET
    de niet-bestaande (event_type/from_name/body_text) → geen PGRST204 meer;
  - lead_campaign_history wordt ge-UPDATE (geen upsert on_conflict=lead_id
    zonder unique);
  - de response is EERLIJK: {"ok": false} als een audit-insert faalt.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class _Chain:
    """Generieke supabase-chain: alle ops chainbaar, execute geeft leeg terug.
    reply_inbox-insert mag optioneel falen (fail_reply_inbox)."""
    def __init__(self, sink, table, fail_reply_inbox):
        self._sink, self._table, self._fail = sink, table, fail_reply_inbox
        self._op = None
        self._payload = None

    def insert(self, payload):
        self._op, self._payload = "insert", payload
        return self

    def update(self, payload):
        self._op, self._payload = "update", payload
        return self

    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def order(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def maybe_single(self):
        return self

    def single(self):
        return self

    def execute(self):
        if self._op in ("insert", "update"):
            self._sink.setdefault(self._table, []).append((self._op, self._payload))
            if self._table == "reply_inbox" and self._op == "insert" and self._fail:
                raise RuntimeError("PGRST204: column not found")

        class _R:
            data = []
            count = 0
        return _R()


class _FakeDB:
    def __init__(self, fail_reply_inbox=False):
        self.sink: dict = {}
        self._fail = fail_reply_inbox

    def table(self, name):
        return _Chain(self.sink, name, self._fail)


def _client_and_post(monkeypatch, *, fail_reply_inbox=False):
    monkeypatch.setenv("WARMR_WEBHOOK_SECRET", "testsecret")
    from fastapi.testclient import TestClient
    import api.main as m

    db = _FakeDB(fail_reply_inbox=fail_reply_inbox)
    m.app.dependency_overrides[m.get_supabase] = lambda: db

    body = json.dumps({
        "event": "replied",
        "custom_fields": {"heatr_lead_id": "L1", "workspace_id": "aerys"},
        "from_email": "prospect@kliniek.nl",
        "subject": "Re: jullie review",
        "body_text": "Ja, interessant.",
    }).encode()
    sig = hmac.new(b"testsecret", body, hashlib.sha256).hexdigest()

    client = TestClient(m.app)  # geen `with` → lifespan/startup-validator draait niet
    resp = client.post("/webhooks/warmr", content=body,
                       headers={"X-Warmr-Signature": sig, "Content-Type": "application/json"})
    m.app.dependency_overrides.clear()
    return resp, db


def test_reply_inbox_uses_real_columns(monkeypatch):
    resp, db = _client_and_post(monkeypatch)
    assert resp.status_code == 200
    inserts = [p for (op, p) in db.sink.get("reply_inbox", []) if op == "insert"]
    assert inserts, "geen reply_inbox-insert"
    payload = inserts[0]
    assert payload.get("body") == "Ja, interessant."       # body, niet body_text
    assert payload.get("from_email") == "prospect@kliniek.nl"
    for dood in ("event_type", "from_name", "body_text"):
        assert dood not in payload, f"niet-bestaande kolom {dood} nog in insert"


def test_lead_campaign_history_updated_not_upserted(monkeypatch):
    _resp, db = _client_and_post(monkeypatch)
    lch = db.sink.get("lead_campaign_history", [])
    assert any(op == "update" and p.get("status") == "replied" for (op, p) in lch)
    # geen niet-bestaande kolommen in de update
    for (op, p) in lch:
        assert "event_type" not in p and "updated_at" not in p


def test_response_honest_when_audit_fails(monkeypatch):
    resp, _db = _client_and_post(monkeypatch, fail_reply_inbox=True)
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False and body["audit_logged"] is False


def test_response_ok_when_audit_succeeds(monkeypatch):
    resp, _db = _client_and_post(monkeypatch)
    assert resp.json()["ok"] is True


def test_webhook_rejects_bad_signature(monkeypatch):
    monkeypatch.setenv("WARMR_WEBHOOK_SECRET", "testsecret")
    from fastapi.testclient import TestClient
    import api.main as m
    client = TestClient(m.app)
    resp = client.post("/webhooks/warmr", content=b'{"event":"replied"}',
                       headers={"X-Warmr-Signature": "wrong", "Content-Type": "application/json"})
    assert resp.status_code == 401
