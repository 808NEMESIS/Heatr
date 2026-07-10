"""
tests/test_gdpr_forget.py — forget_lead (Art. 17) na de fase 2-fix.

Valideert: per-lead-unieke placeholder (geen unique-index-collision bij de
tweede forget), eerlijke ok/errors-semantiek (geen vals succes bij een
gefaalde of 0-rijen-anonimisatie), en idempotente retry.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.gdpr_manager import _redacted_email, forget_lead


class _Query:
    def __init__(self, db: "FakeDB", table: str):
        self.db = db
        self.table_name = table
        self.op = "select"
        self.payload: dict | None = None
        self.filters: list[tuple[str, object]] = []
        self._maybe_single = False

    def select(self, *_):
        return self

    def update(self, patch):
        self.op, self.payload = "update", dict(patch)
        return self

    def delete(self):
        self.op = "delete"
        return self

    def insert(self, row):
        self.op, self.payload = "insert", dict(row)
        return self

    def eq(self, col, val):
        self.filters.append((col, val))
        return self

    def maybe_single(self):
        self._maybe_single = True
        return self

    def _match(self, row):
        return all(row.get(c) == v for c, v in self.filters)

    def execute(self):
        rows = self.db.tables.setdefault(self.table_name, [])
        res = type("Res", (), {})()
        if self.op == "insert":
            rows.append(self.payload)
            res.data = [self.payload]
            return res
        if self.op == "update":
            if self.table_name == "leads" and self.db.leads_update_raises:
                raise RuntimeError(self.db.leads_update_raises)
            matched = [r for r in rows if self._match(r)]
            # Emuleer de unique index (workspace_id, lower(email)) uit 021:
            # een update naar een email die al bij een ANDERE rij bestaat → 23505.
            new_email = (self.payload or {}).get("email")
            if self.table_name == "leads" and new_email:
                for other in rows:
                    if other not in matched and (other.get("email") or "").lower() == new_email.lower() \
                            and other.get("workspace_id") == (matched[0].get("workspace_id") if matched else None):
                        raise RuntimeError(
                            'duplicate key value violates unique constraint '
                            '"uq_heatr_leads_ws_email" (23505)'
                        )
            for r in matched:
                r.update(self.payload)
            res.data = [dict(r) for r in matched]
            return res
        if self.op == "delete":
            matched = [r for r in rows if self._match(r)]
            self.db.tables[self.table_name] = [r for r in rows if r not in matched]
            res.data = matched
            return res
        # select
        matched = [dict(r) for r in rows if self._match(r)]
        res.data = (matched[0] if matched else None) if self._maybe_single else matched
        return res


class _Storage:
    def from_(self, _bucket):
        return self

    def remove(self, _paths):
        return None


class FakeDB:
    def __init__(self):
        self.tables: dict[str, list[dict]] = {}
        self.storage = _Storage()
        self.leads_update_raises: str | None = None

    def table(self, name):
        return _Query(self, name)

    def add_lead(self, lead_id: str, email: str, ws: str = "aerys") -> dict:
        row = {"id": lead_id, "workspace_id": ws, "email": email,
               "contact_first_name": "Piet", "status": "enriched", "domain": None}
        self.tables.setdefault("leads", []).append(row)
        return row


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_placeholder_is_unique_per_lead_and_invalid_tld():
    a, b = _redacted_email("lead-a"), _redacted_email("lead-b")
    assert a != b
    assert a.endswith("@anoniem.invalid") and b.endswith("@anoniem.invalid")
    # stabiel: retry produceert dezelfde waarde (idempotent)
    assert _redacted_email("lead-a") == a


def test_two_forgets_same_workspace_no_collision():
    """DE fase 2-regressietest: met de oude gedeelde placeholder faalde de
    tweede forget op de unique index en bleef de PII staan."""
    db = FakeDB()
    l1 = db.add_lead("lead-1", "piet@praktijk.nl")
    l2 = db.add_lead("lead-2", "anna@kliniek.nl")

    r1 = _run(forget_lead("lead-1", "aerys", db))
    r2 = _run(forget_lead("lead-2", "aerys", db))

    assert r1["ok"] is True and r2["ok"] is True
    assert l1["email"] == _redacted_email("lead-1")
    assert l2["email"] == _redacted_email("lead-2")
    assert l1["status"] == "forgotten" and l2["status"] == "forgotten"
    assert l2["contact_first_name"] == "VERWIJDERD"


def test_failed_anonymize_reports_not_ok():
    """Geen vals succes: faalt de kritieke leads-update, dan ok=False."""
    db = FakeDB()
    db.add_lead("lead-1", "piet@praktijk.nl")
    db.leads_update_raises = "db down"
    result = _run(forget_lead("lead-1", "aerys", db))
    assert result["ok"] is False
    assert any("leads_anonymize" in e for e in result["errors"])
    assert result["anonymized_records"] == 0


def test_unknown_lead_reports_not_ok():
    """0 rijen gematcht (verkeerde lead/workspace) is een fout, geen succes."""
    db = FakeDB()
    result = _run(forget_lead("bestaat-niet", "aerys", db))
    assert result["ok"] is False
    assert any("0 rijen" in e for e in result["errors"])


def test_retry_is_idempotent():
    db = FakeDB()
    lead = db.add_lead("lead-1", "piet@praktijk.nl")
    r1 = _run(forget_lead("lead-1", "aerys", db))
    r2 = _run(forget_lead("lead-1", "aerys", db))  # retry
    assert r1["ok"] is True and r2["ok"] is True
    assert lead["email"] == _redacted_email("lead-1")  # zelfde waarde, geen collision


def test_gdpr_log_written_with_original_email():
    db = FakeDB()
    db.add_lead("lead-1", "piet@praktijk.nl")
    _run(forget_lead("lead-1", "aerys", db, performed_by="test"))
    log = db.tables.get("gdpr_log", [])
    assert len(log) == 1
    assert log[0]["action"] == "forget"
    assert log[0]["lead_email"] == "piet@praktijk.nl"
    assert log[0]["performed_by"] == "test"
