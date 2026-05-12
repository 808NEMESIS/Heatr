"""
tests/test_principles_loader.py — opener_principles.md loader + injection.

Verifies:
- Loader cacht na 1× lezen
- HEATR_PRINCIPLES_DISABLED=true returnt None
- File-truncatie op 12KB
- Reply-drafter injecteert "Reply draft principles" subset in system prompt
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from config import principles_loader


def setup_function():
    principles_loader.reset_cache()


def test_loader_returns_string_when_file_exists():
    text = principles_loader.get_principles()
    assert isinstance(text, str)
    assert len(text) > 100
    assert "evidence" in text.lower() or "rubric" in text.lower()


def test_loader_caches_after_first_call(monkeypatch):
    principles_loader.reset_cache()
    first = principles_loader.get_principles()
    # Tweede call mag GEEN disk-read meer doen
    second = principles_loader.get_principles()
    assert first == second
    # Load-flag staat aan na eerste call
    assert principles_loader._loaded is True


def test_loader_disabled_via_env(monkeypatch):
    principles_loader.reset_cache()
    monkeypatch.setenv("HEATR_PRINCIPLES_DISABLED", "true")
    text = principles_loader.get_principles()
    assert text is None


def test_loader_handles_missing_file(monkeypatch):
    principles_loader.reset_cache()
    from pathlib import Path
    fake_path = Path("/tmp/_nonexistent_heatr_principles.md")
    monkeypatch.setattr(principles_loader, "_PRINCIPLES_PATH", fake_path)
    text = principles_loader.get_principles()
    assert text is None


def test_loader_truncates_above_12kb(monkeypatch, tmp_path):
    principles_loader.reset_cache()
    big = tmp_path / "big.md"
    big.write_text("x" * 20000)
    monkeypatch.setattr(principles_loader, "_PRINCIPLES_PATH", big)
    text = principles_loader.get_principles()
    assert text is not None
    assert len(text) <= 12100  # 12000 cap + small "[truncated]" suffix


# ---------------------------------------------------------------------------
# Injection in reply_drafter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reply_drafter_injects_principles_in_system_prompt():
    """draft_reply moet evidence-rubric toevoegen aan system."""
    principles_loader.reset_cache()

    from campaigns.reply_drafter import draft_reply

    # Mock supabase (no cache) en anthropic
    db = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.maybe_single.return_value = chain
    miss = MagicMock(); miss.data = None
    chain.execute.return_value = miss
    chain.insert.return_value = chain
    chain.upsert.return_value = chain
    db.table.return_value = chain

    client = MagicMock()
    response = MagicMock()
    response.content = [MagicMock(text="Mark, prima. — Sami")]
    response.usage = MagicMock(input_tokens=100, output_tokens=20)
    client.messages.create = AsyncMock(return_value=response)

    await draft_reply(
        reply_inbox_row={"id": "r1", "body_text": "Klinkt goed, hoe verder?"},
        lead={"id": "l1", "company_name": "X", "contact_first_name": "Mark"},
        classification={"category": "interested", "summary": "ja"},
        workspace_id="aerys",
        supabase_client=db,
        anthropic_client=client,
    )

    # Inspect system prompt
    call_kwargs = client.messages.create.call_args.kwargs
    system = call_kwargs["system"][0]["text"]
    # Body bevat zowel base prompt als rubric
    assert "Sami Jansema" in system  # base
    assert "RUBRIC" in system or "rubric" in system.lower()


@pytest.mark.asyncio
async def test_reply_drafter_falls_back_when_principles_disabled(monkeypatch):
    """Als principles unavailable: legacy prompt blijft werken."""
    principles_loader.reset_cache()
    monkeypatch.setenv("HEATR_PRINCIPLES_DISABLED", "true")

    from campaigns.reply_drafter import draft_reply

    db = MagicMock()
    chain = MagicMock()
    chain.select.return_value = chain
    chain.eq.return_value = chain
    chain.maybe_single.return_value = chain
    miss = MagicMock(); miss.data = None
    chain.execute.return_value = miss
    chain.insert.return_value = chain
    chain.upsert.return_value = chain
    db.table.return_value = chain

    client = MagicMock()
    response = MagicMock()
    response.content = [MagicMock(text="ok")]
    response.usage = MagicMock(input_tokens=50, output_tokens=10)
    client.messages.create = AsyncMock(return_value=response)

    await draft_reply(
        reply_inbox_row={"id": "r1", "body_text": "Klinkt goed, hoe verder?"},
        lead={"id": "l1"},
        classification={"category": "interested"},
        workspace_id="aerys",
        supabase_client=db,
        anthropic_client=client,
    )

    system = client.messages.create.call_args.kwargs["system"][0]["text"]
    # Geen rubric want disabled
    assert "RUBRIC" not in system
    assert "Sami Jansema" in system  # base prompt nog wel
