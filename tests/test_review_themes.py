"""tests/test_review_themes.py — review-thema-extractie (value-first opener)."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from enrichment import review_themes as rt


def test_dedup_reviews():
    revs = [{"text": "top", "rating": 5}, {"text": "top", "rating": 5}, {"text": "ok", "rating": 4}]
    assert len(rt.dedup_reviews(revs)) == 2


def test_split_by_sentiment():
    revs = [{"text": "a", "rating": 5}, {"text": "b", "rating": 1}, {"text": "c", "rating": 4}]
    pos, crit = rt.split_by_sentiment(revs)
    assert [r["text"] for r in pos] == ["a", "c"]
    assert [r["text"] for r in crit] == ["b"]


def test_maps_url_encodes_query():
    url = rt.build_maps_search_url("Face Institute", "Amsterdam")
    assert url.startswith("https://www.google.com/maps/search/")
    assert "Face" in url and "Amsterdam" in url and " " not in url


def test_parse_theme_lines_strips_bullets():
    out = rt._parse_theme_lines("- hoe op je gemak je je voelt\n2. het mooie resultaat\nextra regel")
    assert out == ["hoe op je gemak je je voelt", "het mooie resultaat"]  # max 2, schoon


@pytest.mark.asyncio
async def test_mine_themes_needs_min_two_positive():
    # 1 positieve review → te weinig materiaal → []
    client = MagicMock()
    out = await rt.mine_themes_from_reviews([{"text": "top", "rating": 5}], "X", anthropic_client=client)
    assert out == []


@pytest.mark.asyncio
async def test_mine_themes_grounded_call():
    client = MagicMock()
    resp = MagicMock()
    resp.content = [MagicMock(text="hoe persoonlijk het advies is\nhet nette eindresultaat")]
    client.messages = MagicMock()
    client.messages.create = AsyncMock(return_value=resp)
    revs = [{"text": "heel persoonlijk advies gekregen", "rating": 5},
            {"text": "netjes resultaat, blij mee", "rating": 5}]
    out = await rt.mine_themes_from_reviews(revs, "Kliniek X", anthropic_client=client)
    assert out == ["hoe persoonlijk het advies is", "het nette eindresultaat"]
    # alleen positieve reviews naar het model
    prompt = client.messages.create.await_args.kwargs["messages"][0]["content"]
    assert "Kliniek X" in prompt


@pytest.mark.asyncio
async def test_mine_themes_failsoft_on_error():
    client = MagicMock()
    client.messages = MagicMock()
    client.messages.create = AsyncMock(side_effect=RuntimeError("boom"))
    revs = [{"text": "a", "rating": 5}, {"text": "b", "rating": 5}]
    assert await rt.mine_themes_from_reviews(revs, "X", anthropic_client=client) == []
