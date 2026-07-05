"""
tests/test_anthropic_retry.py — 429-retry gedrag voor Anthropic-calls.

Valideert: 429 → retry met exponential backoff, retry-after header wint,
max-retries → duidelijke raise (geen silent failure), niet-retryable
exceptions gaan direct door (fallback-logica caller blijft intact).
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import anthropic
import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.anthropic_retry import anthropic_call_with_retry


def _rate_limit_error(retry_after: str | None = None) -> anthropic.RateLimitError:
    headers = {"retry-after": retry_after} if retry_after else {}
    response = httpx.Response(
        429,
        headers=headers,
        request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"),
    )
    return anthropic.RateLimitError("rate limited", response=response, body=None)


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_429_retries_then_succeeds():
    calls = {"n": 0}
    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)

    async def make_call():
        calls["n"] += 1
        if calls["n"] <= 2:
            raise _rate_limit_error()
        return "ok"

    result = _run(anthropic_call_with_retry(
        make_call, lead_id="l1", context="test", base_delay=2.0, sleep=fake_sleep,
    ))
    assert result == "ok"
    assert calls["n"] == 3
    # Exponential backoff: 2.0 * 2^0, 2.0 * 2^1
    assert sleeps == [2.0, 4.0]


def test_retry_after_header_wins_over_backoff():
    calls = {"n": 0}
    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)

    async def make_call():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _rate_limit_error(retry_after="7")
        return "ok"

    result = _run(anthropic_call_with_retry(
        make_call, base_delay=2.0, sleep=fake_sleep,
    ))
    assert result == "ok"
    assert sleeps == [7.0]


def test_max_retries_exhausted_raises(caplog):
    async def make_call():
        raise _rate_limit_error()

    async def fake_sleep(_s):
        pass

    with pytest.raises(anthropic.RateLimitError):
        _run(anthropic_call_with_retry(
            make_call, lead_id="l9", context="batch", max_retries=2, sleep=fake_sleep,
        ))
    # Duidelijke failure-log met attributie — geen silent failure
    assert any("opgegeven" in r.message and "l9" in r.message for r in caplog.records)


def test_non_retryable_error_passes_through_immediately():
    calls = {"n": 0}

    async def make_call():
        calls["n"] += 1
        raise ValueError("event loop kapot")

    async def fake_sleep(_s):
        raise AssertionError("mag niet slapen voor niet-retryable errors")

    with pytest.raises(ValueError):
        _run(anthropic_call_with_retry(make_call, sleep=fake_sleep))
    assert calls["n"] == 1  # géén retry
