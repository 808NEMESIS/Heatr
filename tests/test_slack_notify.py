"""tests/test_slack_notify.py — operationele Slack-meldingen (reactie + afmelding).

Kern: fail-soft (geen URL → no-op, fouten nooit geraised) en correcte, pure
opmaak van het reactie-rapport en de afmeld-melding.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils import slack_notify as sn


LEAD = {
    "company_name": "Glow Kliniek", "contact_first_name": "Sanne", "city": "Utrecht",
    "sector": "cosmetische_behandelaars", "score": 66, "email": "info@glow.nl",
    "domain": "glow.nl", "personalized_opener": "Glow Kliniek staat op 48 reviews met een 4.8.",
}
WI = {"total_score": 34, "priority": "high"}


# ── fail-soft / config-gating ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_notify_slack_noop_without_url(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    assert sn.slack_enabled() is False
    assert await sn.notify_slack("hoi") is False


@pytest.mark.asyncio
async def test_notify_reply_noop_without_url(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    db = MagicMock()
    assert await sn.notify_reply(db, "lead-1", "aerys", {}, "replied") is False
    db.table.assert_not_called()          # geen DB-werk als Slack uit staat


@pytest.mark.asyncio
async def test_notify_slack_posts_when_configured(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/x")
    fake_resp = MagicMock(status_code=200, text="ok")
    client = AsyncMock()
    client.__aenter__.return_value.post = AsyncMock(return_value=fake_resp)
    with patch("httpx.AsyncClient", return_value=client):
        assert await sn.notify_slack("hoi", blocks=[{"type": "section"}]) is True


@pytest.mark.asyncio
async def test_notify_slack_failsoft_on_http_error(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.test/x")
    client = AsyncMock()
    client.__aenter__.return_value.post = AsyncMock(side_effect=RuntimeError("boom"))
    with patch("httpx.AsyncClient", return_value=client):
        assert await sn.notify_slack("hoi") is False        # nooit geraised


# ── reactie-rapport (pure formatter) ─────────────────────────────────────────

def test_reply_message_contains_lead_report():
    payload = {"subject": "Re: jullie site", "body_text": "Ja, interessant! Bel me maar."}
    text, blocks = sn.format_reply_message(LEAD, WI, payload, "interested", lead_id="l1")
    assert "Geïnteresseerd" in text and "Glow Kliniek" in text
    assert "Sanne" in text and "Utrecht" in text
    # website-score + reactie-snippet + opener zitten in de blocks
    flat = str(blocks)
    assert "34/100" in flat and "prio high" in flat
    assert "interessant" in flat and "48 reviews" in flat


def test_reply_message_generic_label_for_replied():
    text, _ = sn.format_reply_message(LEAD, {}, {"body_text": "hallo"}, "replied", lead_id="l1")
    assert text.startswith("💬 Reactie:")


def test_reply_message_handles_missing_wi_and_opener():
    lead = {"company_name": "X", "email": "info@x.nl"}
    text, blocks = sn.format_reply_message(lead, {}, {}, "replied")
    assert "X" in text and "—" in text                     # ontbrekende velden → em-dash


def test_reply_snippet_is_truncated():
    long = "a" * 900
    _, blocks = sn.format_reply_message(LEAD, WI, {"body_text": long}, "replied")
    assert "…" in str(blocks)


# ── afmeld-melding ───────────────────────────────────────────────────────────

def test_unsubscribe_message_format():
    text, blocks = sn.format_unsubscribe_message(LEAD, {}, lead_id="l1")
    assert text.startswith("🔕 Afmelding: Glow Kliniek")
    assert "info@glow.nl" in text and "gesuppressed" in text


def test_unsubscribe_uses_payload_email_fallback():
    text, _ = sn.format_unsubscribe_message({"company_name": "Y"}, {"from_email": "a@y.nl"})
    assert "a@y.nl" in text


# ── lead-link ────────────────────────────────────────────────────────────────

def test_lead_url_only_when_base_configured(monkeypatch):
    monkeypatch.delenv("HEATR_BASE_URL", raising=False)
    assert sn._lead_url("l1") is None
    monkeypatch.setenv("HEATR_BASE_URL", "https://app.heatr.nl/")
    assert sn._lead_url("l1") == "https://app.heatr.nl/heatr/leads/l1"
