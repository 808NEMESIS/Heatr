"""
tests/test_lead_thread.py — build_lead_thread merge-logic.

Mocks Supabase fetches voor 3 scenarios:
  1. Lead met sent + received → chronologisch interleaved
  2. Lege thread (lead nog niet gemaild)
  3. Alleen sent (geen replies binnen)
"""
from unittest.mock import MagicMock

import pytest

from utils.lead_thread import _strip_html_to_text, build_lead_thread


# ---------------------------------------------------------------------------
# HTML strip helper
# ---------------------------------------------------------------------------

def test_strip_html_to_text_handles_br_and_p():
    html = "Hi<br/>Mark<p>Hoe gaat het?</p>"
    out = _strip_html_to_text(html)
    assert "<" not in out
    assert "Hi" in out
    assert "Mark" in out
    assert "Hoe gaat het?" in out


def test_strip_html_to_text_decodes_entities():
    assert _strip_html_to_text("&amp;") == "&"
    assert _strip_html_to_text("Q&amp;A") == "Q&A"


def test_strip_html_to_text_caps_at_5000():
    long = "<p>" + ("x" * 10000) + "</p>"
    assert len(_strip_html_to_text(long)) <= 5000


def test_strip_html_to_text_handles_none_and_empty():
    assert _strip_html_to_text(None) == ""
    assert _strip_html_to_text("") == ""


# ---------------------------------------------------------------------------
# Mocking helpers — Supabase chain that returns canned data per table
# ---------------------------------------------------------------------------

def _mock_db(table_data: dict[str, list[dict]]):
    """Build a MagicMock supabase-client that returns specific rows per table.

    Args:
        table_data: mapping table-naam → list of rows that .execute() returns
    """
    db = MagicMock()
    last_table = {"name": None}

    def table_factory(name):
        last_table["name"] = name
        chain = MagicMock()
        chain.select.return_value = chain
        chain.eq.return_value = chain
        chain.in_.return_value = chain
        chain.order.return_value = chain
        result = MagicMock()
        result.data = table_data.get(name, [])
        chain.execute.return_value = result
        return chain

    db.table.side_effect = table_factory
    return db


# ---------------------------------------------------------------------------
# Scenario 1: lead met sent + received → correcte chronologische volgorde
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_thread_merges_sent_and_received_chronologically():
    sent_step_0 = {"subject": "Mail 1 onderwerp", "body": "Mail 1 body"}
    sent_step_1 = {"subject": "Re: Mail 2 onderwerp", "body": "Mail 2 follow-up"}

    db = _mock_db({
        "lead_timeline": [
            # Verstuurd Mail 1 op T=10:00
            {"id": "ts1", "event_type": "email_sent", "title": "Verstuurd via campagne",
             "body": None, "metadata": {"campaign_id": "camp-1", "step_index": 0},
             "created_at": "2026-04-21T10:00:00+00:00"},
            # Verstuurd Mail 2 op T=14:00 (3 dagen later)
            {"id": "ts2", "event_type": "email_sent", "title": "Verstuurd via campagne",
             "body": None, "metadata": {"campaign_id": "camp-1", "step_index": 1},
             "created_at": "2026-04-24T14:00:00+00:00"},
        ],
        "lead_campaign_history": [
            {"warmr_campaign_id": "camp-1", "sequence_steps": [sent_step_0, sent_step_1],
             "step_index": 1, "sent_at": "2026-04-24T14:00:00+00:00"},
        ],
        "reply_inbox": [
            # Reply tussen Mail 1 en Mail 2
            {"id": "r1", "subject": "Re: Mail 1 onderwerp", "body": "<p>Bedankt!</p>",
             "body_text": "Bedankt!", "received_at": "2026-04-22T08:30:00+00:00",
             "classification": "interested", "classification_summary": "warm response",
             "from_email": "mark@kliniek.nl"},
        ],
    })

    out = await build_lead_thread("lead-1", "aerys", db)
    assert out["counts"] == {"total": 3, "sent": 2, "received": 1}
    items = out["thread"]
    assert len(items) == 3
    # Volgorde: sent T=21, received T=22, sent T=24
    assert items[0]["direction"] == "sent"
    assert items[0]["subject"] == "Mail 1 onderwerp"
    assert items[0]["body"] == "Mail 1 body"
    assert items[1]["direction"] == "received"
    assert items[1]["subject"] == "Re: Mail 1 onderwerp"
    assert items[1]["classification"] == "interested"
    assert items[2]["direction"] == "sent"
    assert items[2]["step_index"] == 1


# ---------------------------------------------------------------------------
# Scenario 2: lege thread (lead nog niet gemaild)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_empty_thread_for_unmailed_lead():
    db = _mock_db({"lead_timeline": [], "reply_inbox": [], "lead_campaign_history": []})
    out = await build_lead_thread("lead-virgin", "aerys", db)
    assert out["counts"] == {"total": 0, "sent": 0, "received": 0}
    assert out["thread"] == []


# ---------------------------------------------------------------------------
# Scenario 3: alleen sent (geen replies)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_thread_with_only_sent():
    db = _mock_db({
        "lead_timeline": [
            {"id": "ts1", "event_type": "email_sent", "title": "Mail 1",
             "body": None, "metadata": {"campaign_id": "camp-1", "step_index": 0},
             "created_at": "2026-04-21T10:00:00+00:00"},
        ],
        "lead_campaign_history": [
            {"warmr_campaign_id": "camp-1",
             "sequence_steps": [{"subject": "Hi", "body": "Body"}],
             "step_index": 0, "sent_at": "2026-04-21T10:00:00+00:00"},
        ],
        "reply_inbox": [],
    })
    out = await build_lead_thread("lead-1", "aerys", db)
    assert out["counts"] == {"total": 1, "sent": 1, "received": 0}
    assert out["thread"][0]["direction"] == "sent"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_thread_falls_back_when_campaign_history_missing():
    """Sent event zonder matching campaign_history → gebruikt timeline title."""
    db = _mock_db({
        "lead_timeline": [
            {"id": "ts1", "event_type": "email_sent", "title": "Verstuurd: Praktijk X — observatie",
             "body": None, "metadata": {"campaign_id": "camp-orphan", "step_index": 0},
             "created_at": "2026-04-21T10:00:00+00:00"},
        ],
        "lead_campaign_history": [],  # Geen matching row
        "reply_inbox": [],
    })
    out = await build_lead_thread("lead-1", "aerys", db)
    assert out["counts"]["sent"] == 1
    assert "Verstuurd" in out["thread"][0]["subject"]


@pytest.mark.asyncio
async def test_thread_handles_only_received():
    """Reply zonder eerdere sent (raar maar mogelijk: forwarded / typo)."""
    db = _mock_db({
        "lead_timeline": [],
        "reply_inbox": [
            {"id": "r1", "subject": "Random", "body": "ok", "body_text": "ok",
             "received_at": "2026-04-22T08:30:00+00:00",
             "classification": "other", "classification_summary": None,
             "from_email": "x@y.nl"},
        ],
        "lead_campaign_history": [],
    })
    out = await build_lead_thread("lead-1", "aerys", db)
    assert out["counts"] == {"total": 1, "sent": 0, "received": 1}
    assert out["thread"][0]["direction"] == "received"


@pytest.mark.asyncio
async def test_thread_graceful_when_table_query_fails():
    """Reply_inbox table-fetch faalt → returnt nog steeds wat van timeline."""
    db = MagicMock()
    sent_chain = MagicMock()
    sent_chain.select.return_value = sent_chain
    sent_chain.eq.return_value = sent_chain
    sent_chain.in_.return_value = sent_chain
    sent_chain.order.return_value = sent_chain
    sent_result = MagicMock()
    sent_result.data = [
        {"id": "ts1", "event_type": "email_sent", "title": "Mail 1",
         "body": None, "metadata": {},
         "created_at": "2026-04-21T10:00:00+00:00"},
    ]
    sent_chain.execute.return_value = sent_result

    failing_chain = MagicMock()
    failing_chain.select.return_value = failing_chain
    failing_chain.eq.return_value = failing_chain
    failing_chain.in_.return_value = failing_chain
    failing_chain.order.return_value = failing_chain
    failing_chain.execute.side_effect = Exception("table missing")

    def table_factory(name):
        if name == "lead_timeline":
            return sent_chain
        return failing_chain

    db.table.side_effect = table_factory

    out = await build_lead_thread("lead-1", "aerys", db)
    # Thread bevat tenminste de sent event, geen crash
    assert out["counts"]["sent"] == 1
