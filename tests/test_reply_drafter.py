"""
tests/test_reply_drafter.py — reply_drafter skip-list + cache + category routing.

Mocks Anthropic + Supabase. No live API calls.
Run: pytest tests/test_reply_drafter.py -v
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from campaigns.reply_drafter import (
    _CATEGORY_GUIDANCE,
    _SKIP_CATEGORIES,
    draft_reply,
)


def _mock_db_no_cache():
    """Supabase mock waar cache-lookup leeg returnt."""
    db = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.maybe_single.return_value = chain
    miss = MagicMock()
    miss.data = None
    chain.execute.return_value = miss
    chain.insert.return_value = chain
    chain.upsert.return_value = chain
    db.table.return_value = chain
    return db


def _mock_db_with_cache(cached_response: dict):
    """Supabase mock waar cache-lookup een response retourneert."""
    import json
    db = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.maybe_single.return_value = chain
    hit = MagicMock()
    hit.data = {"response_text": json.dumps(cached_response)}
    chain.execute.return_value = hit
    chain.insert.return_value = chain
    chain.upsert.return_value = chain
    db.table.return_value = chain
    return db


def _mock_anthropic(text: str, in_tokens: int = 100, out_tokens: int = 50):
    """AsyncAnthropic mock that returns text + token usage."""
    client = MagicMock()
    response = MagicMock()
    response.content = [MagicMock(text=text)]
    response.usage = MagicMock(input_tokens=in_tokens, output_tokens=out_tokens)
    client.messages.create = AsyncMock(return_value=response)
    return client


# ---------------------------------------------------------------------------
# Skip-list
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unsubscribe_request_returns_no_draft():
    """Auto-unsubscribe replies krijgen geen draft — net zo min als auto-replies."""
    result = await draft_reply(
        reply_inbox_row={"id": "r1", "body_text": "Haal me eraf"},
        lead={"id": "l1", "company_name": "X"},
        classification={"category": "unsubscribe_request"},
        workspace_id="aerys",
        supabase_client=_mock_db_no_cache(),
        anthropic_client=_mock_anthropic(""),
    )
    assert result["draft"] is None
    assert "unsubscribe_request" in result["skip_reason"]


@pytest.mark.asyncio
async def test_auto_reply_returns_no_draft():
    result = await draft_reply(
        reply_inbox_row={"id": "r1", "body_text": "Out of office until June"},
        lead={"id": "l1", "company_name": "X"},
        classification={"category": "auto_reply"},
        workspace_id="aerys",
        supabase_client=_mock_db_no_cache(),
        anthropic_client=_mock_anthropic(""),
    )
    assert result["draft"] is None


@pytest.mark.asyncio
async def test_too_short_reply_returns_no_draft():
    result = await draft_reply(
        reply_inbox_row={"id": "r1", "body_text": "ok"},
        lead=None,
        classification={"category": "interested"},
        workspace_id="aerys",
        supabase_client=_mock_db_no_cache(),
        anthropic_client=_mock_anthropic(""),
    )
    assert result["draft"] is None
    assert "te kort" in result["skip_reason"]


# ---------------------------------------------------------------------------
# Happy path — Claude is called for drafts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_interested_category_calls_claude_and_returns_draft():
    fake_draft = (
        "Mark, dank voor de reactie. Plan via deze link een 15-min "
        "kennismaking: cal.com/sami — kies een moment dat schikt.\n\n— Sami"
    )
    client = _mock_anthropic(fake_draft, in_tokens=400, out_tokens=80)
    result = await draft_reply(
        reply_inbox_row={"id": "r1", "body_text": "Klinkt interessant, hoe verder?"},
        lead={"id": "l1", "company_name": "Praktijk Mark", "contact_first_name": "Mark"},
        classification={"category": "interested", "summary": "wil meer info"},
        workspace_id="aerys",
        supabase_client=_mock_db_no_cache(),
        anthropic_client=client,
    )
    assert result["draft"] == fake_draft
    assert result["category"] == "interested"
    assert result["cached"] is False
    assert result["cost_eur"] > 0
    # Claude was actually called
    client.messages.create.assert_called_once()


@pytest.mark.asyncio
async def test_question_category_uses_question_guidance():
    """Categorie-guidance moet daadwerkelijk in prompt landen."""
    client = _mock_anthropic("Eric, deelne van de audit duurt ongeveer 60 minuten. — Sami")
    await draft_reply(
        reply_inbox_row={"id": "r1", "body_text": "Hoelang duurt de audit?"},
        lead={"id": "l1", "company_name": "X", "contact_first_name": "Eric"},
        classification={"category": "question", "summary": "vraag over duur"},
        workspace_id="aerys",
        supabase_client=_mock_db_no_cache(),
        anthropic_client=client,
    )
    # Verify de question-guidance daadwerkelijk in user-prompt zat
    call_kwargs = client.messages.create.call_args.kwargs
    user_msg = call_kwargs["messages"][0]["content"]
    assert _CATEGORY_GUIDANCE["question"] in user_msg


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cache_hit_skips_claude_call():
    """Tweede call met dezelfde reply hits de cache, betaalt geen Claude opnieuw."""
    cached = {"draft": "Mark, dank. — Sami", "category": "interested", "skip_reason": None, "cached": False}
    client = _mock_anthropic("ZOU NIET MOETEN VERSCHIJNEN")
    result = await draft_reply(
        reply_inbox_row={"id": "r1", "body_text": "Ja graag"},
        lead={"id": "l1", "company_name": "X"},
        classification={"category": "interested", "summary": "ja"},
        workspace_id="aerys",
        supabase_client=_mock_db_with_cache(cached),
        anthropic_client=client,
    )
    assert result["draft"] == "Mark, dank. — Sami"
    assert result["cached"] is True
    assert result["cost_eur"] == 0.0
    client.messages.create.assert_not_called()


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------

def test_skip_categories_are_subset_of_known():
    expected_in_skip = {"auto_reply", "unsubscribe_request"}
    assert _SKIP_CATEGORIES == expected_in_skip


def test_all_non_skip_categories_have_guidance():
    """Elke niet-skipped category moet een tone-guidance hebben."""
    needed = {"interested", "question", "not_now", "wrong_person", "not_interested", "other"}
    assert needed.issubset(set(_CATEGORY_GUIDANCE.keys()))


# ---------------------------------------------------------------------------
# Quote-trim
# ---------------------------------------------------------------------------

def test_trim_quoted_thread_strips_op_x_schreef():
    from campaigns.reply_drafter import trim_quoted_thread
    body = (
        "Bedankt voor je bericht. Ja, plan maar in.\n\n"
        "Op vrijdag 25 april 2026 schreef Sami Jansema <sami@aerys.nl>:\n"
        "> Hoi Mark, ik kwam jullie praktijk tegen via Google en heb kort op de site gekeken — die draait al een paar jaar mee...\n"
        "> [veel meer quoted text]"
    )
    out = trim_quoted_thread(body)
    assert "Bedankt voor je bericht" in out
    assert "ik kwam jullie praktijk tegen" not in out
    assert ">" not in out


def test_trim_quoted_thread_strips_on_x_wrote():
    from campaigns.reply_drafter import trim_quoted_thread
    body = (
        "Sounds great, when can we chat about implementation details and pricing?\n\n"
        "On Mon, Apr 28, 2026 at 9:30 AM Sami <sami@aerys.nl> wrote:\n"
        "> [quoted email content here]"
    )
    out = trim_quoted_thread(body)
    assert "Sounds great" in out
    assert "quoted email content" not in out


def test_trim_quoted_thread_preserves_short_unquoted():
    from campaigns.reply_drafter import trim_quoted_thread
    body = "Yes, please."
    # Te kort om aggressief te trimmen → hou origineel
    out = trim_quoted_thread(body)
    assert out == "Yes, please."


def test_trim_quoted_thread_strips_dutch_van_header():
    from campaigns.reply_drafter import trim_quoted_thread
    body = (
        "Klinkt prima, ik heb deze week tijd op donderdag of vrijdag.\n\n"
        "Van: Sami Jansema <sami@aerys.nl>\n"
        "Verzonden: vrijdag 25 april 2026 09:30\n"
        "Aan: Mark Jansen\n"
        "Onderwerp: Chiropractie Praktijk Utrecht — een observatie\n\n"
        "[lange quote-tekst]"
    )
    out = trim_quoted_thread(body)
    assert "Klinkt prima" in out
    assert "Verzonden:" not in out


def test_trim_quoted_thread_no_quote_returns_full():
    from campaigns.reply_drafter import trim_quoted_thread
    body = "Dit is een gewone reply zonder enige quote-marker. Volledig behouden."
    assert trim_quoted_thread(body) == body


# ---------------------------------------------------------------------------
# Wachttijd-context
# ---------------------------------------------------------------------------

def test_hours_since_recent_iso_string():
    from datetime import datetime, timezone, timedelta
    from campaigns.reply_drafter import hours_since
    five_h_ago = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    h = hours_since(five_h_ago)
    assert h == 5 or h == 4  # boundary tolerance — fractional second


def test_hours_since_old_iso_string():
    from datetime import datetime, timezone, timedelta
    from campaigns.reply_drafter import hours_since
    three_days_ago = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    h = hours_since(three_days_ago)
    assert 71 <= h <= 73


def test_hours_since_none_returns_none():
    from campaigns.reply_drafter import hours_since
    assert hours_since(None) is None
    assert hours_since("") is None
    assert hours_since("not-a-date") is None


def test_timing_instruction_buckets():
    from campaigns.reply_drafter import _timing_instruction
    assert "binnen 2 uur" in _timing_instruction(1)
    assert "werkdag" in _timing_instruction(8)
    assert "acknowledgen" in _timing_instruction(48)
    assert "dagen" in _timing_instruction(96)
    assert _timing_instruction(None) == ""


# ---------------------------------------------------------------------------
# Cal.com auto-injection
# ---------------------------------------------------------------------------

def test_ensure_scheduling_link_appends_for_interested(monkeypatch):
    monkeypatch.setenv("HEATR_SCHEDULING_URL", "cal.com/sami-aerys/15min")
    # Re-import om env-var op te pikken
    import importlib, campaigns.reply_drafter
    importlib.reload(campaigns.reply_drafter)
    draft = "Mark, dank. Plan een tijd die past.\n\n— Sami"
    result, appended = campaigns.reply_drafter._ensure_scheduling_link(draft, "interested")
    assert appended is True
    assert "cal.com" in result.lower()


def test_ensure_scheduling_link_skips_when_already_present(monkeypatch):
    monkeypatch.setenv("HEATR_SCHEDULING_URL", "cal.com/sami-aerys/15min")
    import importlib, campaigns.reply_drafter
    importlib.reload(campaigns.reply_drafter)
    draft = "Mark, dank. Boek via cal.com/sami-aerys/15min.\n\n— Sami"
    result, appended = campaigns.reply_drafter._ensure_scheduling_link(draft, "interested")
    assert appended is False
    assert result == draft


def test_ensure_scheduling_link_skips_for_non_interested(monkeypatch):
    monkeypatch.setenv("HEATR_SCHEDULING_URL", "cal.com/sami-aerys/15min")
    import importlib, campaigns.reply_drafter
    importlib.reload(campaigns.reply_drafter)
    draft = "Mark, snap ik. Tot een volgende keer.\n\n— Sami"
    result, appended = campaigns.reply_drafter._ensure_scheduling_link(draft, "not_now")
    assert appended is False
    assert "cal.com" not in result.lower()


def test_ensure_scheduling_link_skips_when_env_unset(monkeypatch):
    """Geen HEATR_SCHEDULING_URL → geen auto-injection, voorkomt 404-link."""
    monkeypatch.delenv("HEATR_SCHEDULING_URL", raising=False)
    import importlib, campaigns.reply_drafter
    importlib.reload(campaigns.reply_drafter)
    draft = "Mark, dank. Hoe vinden we een moment?\n\n— Sami"
    result, appended = campaigns.reply_drafter._ensure_scheduling_link(draft, "interested")
    assert appended is False
    assert result == draft


# ---------------------------------------------------------------------------
# Integration: trim + thread + cal.com all wired into draft_reply
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_draft_reply_uses_trimmed_body_and_thread_context():
    """Integration: quote-trim + thread-context + timing must all reach Claude."""
    client = _mock_anthropic("Mark, top. Plan een tijd via cal.com/sami-aerys/15min.\n\n— Sami")
    raw_body = (
        "Klinkt goed, hoe verder?\n\n"
        "Op 25 april schreef Sami:\n"
        "> Hier is een veel langere mail die we eerder stuurden..."
    )
    await draft_reply(
        reply_inbox_row={
            "id": "r-x",
            "body_text": raw_body,
            "received_at": "2026-04-26T08:00:00+00:00",  # ~2 dagen oud
        },
        lead={"id": "l1", "company_name": "Praktijk Y", "contact_first_name": "Mark", "archetype": "lichaamswerk_pragmatisch"},
        classification={"category": "interested", "summary": "ja graag"},
        workspace_id="aerys",
        supabase_client=_mock_db_no_cache(),
        anthropic_client=client,
        original_emails=[{"subject": "Een observatie", "body": "Originele Mail 1 tekst hier"}],
    )
    # User-prompt moet gestripte body bevatten (geen "Op 25 april schreef") MAAR wel
    # de oorspronkelijk verstuurde mail als thread-context
    user_prompt = client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "Klinkt goed, hoe verder?" in user_prompt
    assert "Op 25 april schreef Sami" not in user_prompt
    assert "Originele Mail 1 tekst hier" in user_prompt
    # Archetype + timing-instructie meegegeven
    assert "lichaamswerk_pragmatisch" in user_prompt
    assert "Wachttijd-instructie" in user_prompt
