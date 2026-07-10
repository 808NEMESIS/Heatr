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
    """Generieke supabase-chain: alle ops chainbaar. leads-select kent lead L1
    (PR 10: de webhook verifieert lead-bestaan); webhook_events emuleert de
    UNIQUE op event_id; reply_inbox-insert mag optioneel falen."""
    def __init__(self, sink, table, fail_reply_inbox, db):
        self._sink, self._table, self._fail = sink, table, fail_reply_inbox
        self._db = db
        self._op = None
        self._payload = None
        self._eq: dict = {}

    def insert(self, payload):
        self._op, self._payload = "insert", payload
        return self

    def update(self, payload):
        self._op, self._payload = "update", payload
        return self

    def select(self, *a, **k):
        return self

    def eq(self, col, val):
        self._eq[col] = val
        return self

    def in_(self, *a, **k):
        return self

    def is_(self, *a, **k):
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
        if self._op == "insert" and self._table == "webhook_events":
            eid = self._payload.get("event_id")
            if eid in self._db.seen_events:
                raise RuntimeError(
                    'duplicate key value violates unique constraint '
                    '"uq_webhook_events_event_id" (23505)')
            self._db.seen_events.add(eid)
        if self._op in ("insert", "update"):
            self._sink.setdefault(self._table, []).append((self._op, self._payload))
            if self._table == "reply_inbox" and self._op == "insert" and self._fail:
                raise RuntimeError("PGRST204: column not found")

        class _R:
            data = []
            count = 0
        r = _R()
        # PR 10: de webhook verifieert lead-bestaan; alleen L1 bestaat.
        if self._op is None and self._table == "leads":
            if self._eq.get("id") == "L1" or self._eq.get("warmr_lead_id") == "W-L1":
                r.data = {"id": "L1", "email": "prospect@kliniek.nl"}
        return r


class _FakeDB:
    def __init__(self, fail_reply_inbox=False):
        self.sink: dict = {}
        self._fail = fail_reply_inbox
        self.seen_events: set = set()

    def table(self, name):
        return _Chain(self.sink, name, self._fail, self)


def _post(monkeypatch, db, body_dict):
    monkeypatch.setenv("WARMR_WEBHOOK_SECRET", "testsecret")
    from fastapi.testclient import TestClient
    import api.main as m

    m.app.dependency_overrides[m.get_supabase] = lambda: db
    body = json.dumps(body_dict).encode()
    sig = hmac.new(b"testsecret", body, hashlib.sha256).hexdigest()
    client = TestClient(m.app)  # geen `with` → lifespan/startup-validator draait niet
    resp = client.post("/webhooks/warmr", content=body,
                       headers={"X-Warmr-Signature": sig, "Content-Type": "application/json"})
    m.app.dependency_overrides.clear()
    return resp


_DEFAULT_BODY = {
    "event": "replied",
    "custom_fields": {"heatr_lead_id": "L1", "workspace_id": "aerys"},
    "from_email": "prospect@kliniek.nl",
    "subject": "Re: jullie review",
    "body_text": "Ja, interessant.",
}


def _client_and_post(monkeypatch, *, fail_reply_inbox=False):
    db = _FakeDB(fail_reply_inbox=fail_reply_inbox)
    resp = _post(monkeypatch, db, _DEFAULT_BODY)
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

# ---------------------------------------------------------------------------
# Fase 3 PR 10 — eventledger: dedup, dead-letter, finalisatie
# ---------------------------------------------------------------------------

def test_duplicate_delivery_short_circuits(monkeypatch):
    """Scenario 3 (audit v2): een geredeliverd event (zelfde payload) mag geen
    tweede reply_inbox-rij of statuswissel veroorzaken."""
    db = _FakeDB()
    r1 = _post(monkeypatch, db, _DEFAULT_BODY)
    inbox_after_first = len(db.sink.get("reply_inbox", []))
    r2 = _post(monkeypatch, db, _DEFAULT_BODY)  # exacte redelivery
    assert r1.json()["ok"] is True
    assert r2.json() == {"ok": True, "duplicate": True, "event_id": r1.json()["event_id"]}
    assert len(db.sink.get("reply_inbox", [])) == inbox_after_first  # geen 2e rij


def test_unknown_lead_dead_letters(monkeypatch):
    """Scenario 14 (audit v2): lead-gebonden event voor een onbekende lead →
    dead_letter + eerlijke response, geen stil {"ok": true}."""
    db = _FakeDB()
    body = dict(_DEFAULT_BODY, custom_fields={"heatr_lead_id": "GHOST", "workspace_id": "aerys"})
    resp = _post(monkeypatch, db, body)
    out = resp.json()
    assert out["ok"] is False
    assert out["reason"] == "unknown_lead" and out["dead_letter"] is True
    # dead_letter is gefinaliseerd op het event
    updates = [p for (op, p) in db.sink.get("webhook_events", []) if op == "update"]
    assert any(u.get("processing_status") == "dead_letter" for u in updates)
    # géén side-effects gedaan
    assert not db.sink.get("reply_inbox")


def test_unknown_heatr_id_falls_back_to_warmr_correlation(monkeypatch):
    """Fallback: heatr_lead_id onbekend maar payload draagt Warmr's lead-id →
    correleren i.p.v. dead-letteren."""
    db = _FakeDB()
    body = dict(_DEFAULT_BODY,
                custom_fields={"heatr_lead_id": "GHOST", "workspace_id": "aerys"},
                lead_id="W-L1")
    resp = _post(monkeypatch, db, body)
    assert resp.json()["ok"] is True
    assert db.sink.get("reply_inbox"), "correlatie had moeten slagen → side-effects"


def test_processed_event_finalized(monkeypatch):
    db = _FakeDB()
    resp = _post(monkeypatch, db, _DEFAULT_BODY)
    assert resp.json()["ok"] is True
    updates = [p for (op, p) in db.sink.get("webhook_events", []) if op == "update"]
    assert any(u.get("processing_status") == "processed" for u in updates)
