"""
tests/test_warmr_plaintext.py — DELIV-02: plain-text-intentie expliciet op de wire.

Audit-bevinding (2026-07-24): Heatr vertelde Warmr nergens 'plain-text', dus de
content-type was Warmrs onbekende default. push_lead zet nu een expliciete
content_type in custom_fields bij een dispatch-send (custom_body).
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from integrations.warmr_client import WarmrClient


def _make_client() -> WarmrClient:
    client = WarmrClient.__new__(WarmrClient)
    client.api_url = "http://localhost:8000/api/v1"
    client.api_key = "wrmr_test_key"
    client._sb = None
    client.workspace_id = "aerys"
    return client


class _Capture:
    def __init__(self):
        self.payload = None

    async def __call__(self, method, path, **kwargs):
        self.payload = kwargs.get("json")
        return {"id": "warmr-lead-1"}


LEAD = {"id": "l1", "email": "info@kliniek.nl", "company_name": "Kliniek",
        "contact_first_name": "Sanne", "city": "Utrecht"}


def test_push_lead_declares_plain_text_by_default():
    client = _make_client()
    cap = _Capture(); client._request = cap
    asyncio.run(client.push_lead(LEAD, campaign_id="c1", custom_subject="S",
                                 custom_body="Hoi Sanne, ..."))
    assert cap.payload["custom_fields"]["content_type"] == "text/plain"
    assert cap.payload["custom_fields"]["custom_body"] == "Hoi Sanne, ..."


def test_push_lead_no_content_type_without_custom_body():
    # zonder dispatch-body (frozen campaign-sequence-pad) geen content_type-injectie.
    client = _make_client()
    cap = _Capture(); client._request = cap
    asyncio.run(client.push_lead(LEAD, campaign_id="c1"))
    assert "content_type" not in (cap.payload.get("custom_fields") or {})
