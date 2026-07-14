"""
tests/test_reply_inbox_endpoint.py — GET /reply-inbox (2026-07-14).

De Inbox-pagina (frontend-next) riep GET /reply-inbox al aan, maar het endpoint
bestond niet — alleen GET /inbox met een ander contract. Deze tests bewijzen:
  - het response-contract dat Inbox.tsx verwacht ({replies: [...]} met
    body_preview/sender_email/company_name-mapping);
  - workspace-scoping;
  - het ?classification=-filter (incl. 'unclassified' en 422 op onzin);
  - de limit-cap;
  - webhook-vervolg: unsubscribed/bounced krijgen deterministische
    classification, en crm_stage-writes gebruiken de CRM-enum
    ('gereageerd'/'verloren' — niet het onzichtbare 'beantwoord'/'afgesloten').
"""
from __future__ import annotations

import hashlib
import hmac
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ==============================================================================
# Fakes — chainbare supabase-mock (patroon: test_webhook_audit_recovery.py)
# ==============================================================================

class _SelectChain:
    """Vangt de query-bouw per tabel en levert canned rows.

    GET /reply-inbox doet twee queries: reply_inbox (hoofdlijst) en daarna
    leads (batch company_name-lookup via .in_) — er is geen FK dus geen
    embedded join mogelijk (PGRST200, live smoke 2026-07-14)."""
    def __init__(self, calls: dict, rows: list):
        self._calls, self._rows = calls, rows

    def select(self, cols):
        self._calls["select"] = cols
        return self

    def eq(self, col, val):
        self._calls.setdefault("eq", []).append((col, val))
        return self

    def is_(self, col, val):
        self._calls.setdefault("is_", []).append((col, val))
        return self

    def in_(self, col, vals):
        self._calls.setdefault("in_", []).append((col, list(vals)))
        return self

    def order(self, col, desc=False):
        self._calls["order"] = (col, desc)
        return self

    def limit(self, n):
        self._calls["limit"] = n
        return self

    def execute(self):
        class _R:
            pass
        r = _R()
        r.data = self._rows
        return r


class _FakeDB:
    def __init__(self, rows: list | None = None, lead_rows: list | None = None):
        self.calls: dict = {}          # query-bouw van de reply_inbox-query
        self.lead_calls: dict = {}     # query-bouw van de leads-lookup
        self.rows = rows or []
        self.lead_rows = lead_rows if lead_rows is not None else [
            {"id": "L1", "company_name": "Kliniek Zonneveld"},
        ]

    def table(self, name):
        if name == "leads":
            self.lead_calls["table"] = name
            return _SelectChain(self.lead_calls, self.lead_rows)
        self.calls["table"] = name
        return _SelectChain(self.calls, self.rows)


def _get(db, path):
    from fastapi.testclient import TestClient
    import api.main as m

    m.app.dependency_overrides[m.get_supabase] = lambda: db
    m.app.dependency_overrides[m.get_workspace] = lambda: "aerys"
    client = TestClient(m.app)  # geen `with` → lifespan draait niet
    resp = client.get(path)
    m.app.dependency_overrides.clear()
    return resp


_ROW = {
    "id": "R1",
    "lead_id": "L1",
    "subject": "Re: jullie review",
    "body": None,
    "body_html": "<p>Bedankt voor je bericht!<br>Bel me <b>morgen</b> even.</p>" + ("x" * 500),
    "from_email": "prospect@kliniek.nl",
    "classification": "interested",
    "received_at": "2026-07-14T10:00:00+00:00",
}


# ==============================================================================
# Contract
# ==============================================================================

class TestContract:
    def test_response_shape_and_field_mapping(self):
        db = _FakeDB(rows=[_ROW])
        resp = _get(db, "/reply-inbox?limit=100")
        assert resp.status_code == 200
        body = resp.json()
        assert "replies" in body and len(body["replies"]) == 1
        r = body["replies"][0]
        # exact de velden die Inbox.tsx verwacht
        assert set(r.keys()) == {
            "id", "lead_id", "subject", "body_preview", "sender_email",
            "classification", "received_at", "company_name",
        }
        assert r["sender_email"] == "prospect@kliniek.nl"     # ← from_email
        assert r["company_name"] == "Kliniek Zonneveld"       # ← leads-lookup
        assert r["classification"] == "interested"

    def test_body_preview_stripped_and_truncated(self):
        db = _FakeDB(rows=[_ROW])
        r = _get(db, "/reply-inbox").json()["replies"][0]
        assert "<p>" not in r["body_preview"] and "<b>" not in r["body_preview"]
        assert "Bedankt voor je bericht!" in r["body_preview"]
        assert len(r["body_preview"]) <= 200

    def test_body_fallback_when_no_html(self):
        row = {**_ROW, "body_html": None, "body": "plain tekst reply"}
        db = _FakeDB(rows=[row])
        r = _get(db, "/reply-inbox").json()["replies"][0]
        assert r["body_preview"] == "plain tekst reply"

    def test_missing_lead_lookup_is_none_safe(self):
        db = _FakeDB(rows=[_ROW], lead_rows=[])  # lead niet (meer) vindbaar
        r = _get(db, "/reply-inbox").json()["replies"][0]
        assert r["company_name"] is None

    def test_empty_inbox_returns_empty_list(self):
        db = _FakeDB(rows=[])
        body = _get(db, "/reply-inbox").json()
        assert body == {"replies": []}


# ==============================================================================
# Scoping, filters, limit
# ==============================================================================

class TestQueryBuilding:
    def test_workspace_scoped(self):
        db = _FakeDB()
        _get(db, "/reply-inbox")
        assert db.calls["table"] == "reply_inbox"
        assert ("workspace_id", "aerys") in db.calls["eq"]

    def test_classification_filter(self):
        db = _FakeDB()
        _get(db, "/reply-inbox?classification=unsubscribe_request")
        assert ("classification", "unsubscribe_request") in db.calls["eq"]

    def test_unclassified_filter_uses_is_null(self):
        db = _FakeDB()
        _get(db, "/reply-inbox?classification=unclassified")
        assert ("classification", "null") in db.calls["is_"]

    def test_unknown_classification_is_422(self):
        db = _FakeDB()
        resp = _get(db, "/reply-inbox?classification=banaan")
        assert resp.status_code == 422

    def test_limit_capped_at_200(self):
        db = _FakeDB()
        _get(db, "/reply-inbox?limit=5000")
        assert db.calls["limit"] == 200

    def test_ordered_newest_first(self):
        db = _FakeDB()
        _get(db, "/reply-inbox")
        assert db.calls["order"] == ("received_at", True)

    def test_lead_lookup_workspace_scoped_and_batched(self):
        db = _FakeDB(rows=[_ROW])
        _get(db, "/reply-inbox")
        assert ("workspace_id", "aerys") in db.lead_calls["eq"]
        assert ("id", ["L1"]) in db.lead_calls["in_"]


# ==============================================================================
# Webhook-vervolg: deterministische classification + crm_stage-enum
# ==============================================================================

class _WebhookChain:
    def __init__(self, sink, table, db):
        self._sink, self._table, self._db = sink, table, db
        self._op, self._payload, self._eq = None, None, {}

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
        if self._op in ("insert", "update"):
            self._sink.setdefault(self._table, []).append((self._op, self._payload))

        class _R:
            data = []
            count = 0
        r = _R()
        if self._op is None and self._table == "leads" and self._eq.get("id") == "L1":
            r.data = {"id": "L1", "email": "prospect@kliniek.nl"}
        return r


class _WebhookDB:
    def __init__(self):
        self.sink: dict = {}

    def table(self, name):
        return _WebhookChain(self.sink, name, self)


def _post_webhook(monkeypatch, event: str):
    monkeypatch.setenv("WARMR_WEBHOOK_SECRET", "testsecret")
    from fastapi.testclient import TestClient
    import api.main as m

    db = _WebhookDB()
    m.app.dependency_overrides[m.get_supabase] = lambda: db
    body = json.dumps({
        "event": event,
        "custom_fields": {"heatr_lead_id": "L1", "workspace_id": "aerys"},
        "from_email": "prospect@kliniek.nl",
    }).encode()
    sig = hmac.new(b"testsecret", body, hashlib.sha256).hexdigest()
    client = TestClient(m.app)
    resp = client.post("/webhooks/warmr", content=body,
                       headers={"X-Warmr-Signature": sig, "Content-Type": "application/json"})
    m.app.dependency_overrides.clear()
    return resp, db


def _inserted(db, table):
    return [p for op, p in db.sink.get(table, []) if op == "insert"]


def _updated(db, table):
    return [p for op, p in db.sink.get(table, []) if op == "update"]


class TestWebhookDeterministicClassification:
    def test_unsubscribed_insert_has_classification(self, monkeypatch):
        _, db = _post_webhook(monkeypatch, "unsubscribed")
        rows = _inserted(db, "reply_inbox")
        assert rows and rows[0]["classification"] == "unsubscribe_request"
        assert rows[0]["classification_summary"]

    def test_bounced_insert_classified_other(self, monkeypatch):
        _, db = _post_webhook(monkeypatch, "bounced")
        rows = _inserted(db, "reply_inbox")
        assert rows and rows[0]["classification"] == "other"

    def test_replied_insert_stays_unclassified(self, monkeypatch):
        """Echte replies classificeert de Claude-classifier — niet de webhook."""
        _, db = _post_webhook(monkeypatch, "replied")
        rows = _inserted(db, "reply_inbox")
        assert rows and rows[0]["classification"] is None


class TestWebhookCrmStageEnum:
    def test_replied_writes_gereageerd(self, monkeypatch):
        _, db = _post_webhook(monkeypatch, "replied")
        stages = [u["crm_stage"] for u in _updated(db, "leads") if "crm_stage" in u]
        assert "gereageerd" in stages
        assert "beantwoord" not in stages  # oude, in de CRM-UI onzichtbare stage

    def test_unsubscribed_writes_verloren(self, monkeypatch):
        _, db = _post_webhook(monkeypatch, "unsubscribed")
        stages = [u["crm_stage"] for u in _updated(db, "leads") if "crm_stage" in u]
        assert "verloren" in stages
        assert "afgesloten" not in stages
        # de autoritaire blokkade blijft: status=unsubscribed
        assert any(u.get("status") == "unsubscribed" for u in _updated(db, "leads"))
