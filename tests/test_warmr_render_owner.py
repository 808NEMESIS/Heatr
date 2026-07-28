"""tests/test_warmr_render_owner.py — Route C keten-schakel (Heatr-kant).

Bewijst dat wat Heatr aanlevert (de QA-gegate'te body) exact als custom_body naar
Warmr gepusht wordt, MÉT custom_fields.render_owner='heatr' zodat Warmr's scheduler
'm opaak verstuurt. Samen met de Warmr-side byte-voor-byte-test sluit dit de keten:
  gegate'te body → custom_body (deze test) → verzonden body (Warmr-side test).
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from integrations.warmr_client import WarmrClient


def _run(coro):
    """Privé event-loop: raakt de gedeelde pytest-asyncio-loop niet."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _client_with_capture(captured: dict) -> WarmrClient:
    client = WarmrClient.__new__(WarmrClient)      # geen live-init
    client._sb = None                              # → geen post-store update
    client.workspace_id = "aerys"

    async def _fake_request(method, path, json=None):
        captured["method"], captured["path"], captured["payload"] = method, path, json
        return {"id": "warmr-lead-1"}

    client._request = _fake_request
    return client


GATED_BODY = ("Hoi Sami,\n\nEen volledige, QA-gegate'te body met {niet|geen} en "
              "{{first_name}} die letterlijk moeten blijven.\n\nGroet,\nSami")


def test_push_lead_flags_render_owner_and_carries_body():
    captured: dict = {}
    client = _client_with_capture(captured)
    _run(client.push_lead(
        {"id": "l1", "email": "a@b.nl", "contact_first_name": "Sami"},
        campaign_id="camp-1", custom_subject="Onderwerp", custom_body=GATED_BODY,
    ))
    cf = captured["payload"]["custom_fields"]
    assert cf["custom_body"] == GATED_BODY          # byte-voor-byte doorgegeven
    assert cf["custom_subject"] == "Onderwerp"
    assert cf["render_owner"] == "heatr"            # de vlag die Warmr's C-pad activeert


def test_push_lead_without_custom_body_no_render_owner():
    # Zonder custom_body → geen render_owner (het template-pad blijft de default).
    captured: dict = {}
    client = _client_with_capture(captured)
    _run(client.push_lead({"id": "l1", "email": "a@b.nl"}, campaign_id="camp-1"))
    cf = captured["payload"].get("custom_fields", {})
    assert "render_owner" not in cf
