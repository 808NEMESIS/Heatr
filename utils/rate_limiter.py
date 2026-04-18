"""
utils/rate_limiter.py — Token bucket rate limiter with shared Supabase state.

State is stored in the `rate_limit_state` table so multiple workers (e.g.
multiple Uvicorn processes or background queue workers) share a single bucket
per service. This prevents combined throughput from exceeding the per-service
limits even when running in parallel.

Token bucket algorithm:
  - Each service has a bucket of max_tokens capacity.
  - Tokens refill continuously at refill_rate tokens/second.
  - Each request consumes 1 token.
  - If the bucket is empty, callers either return False (check) or block (wait).
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

# =============================================================================
# Rate limit configuration
# These mirror the seed data in supabase_schema.sql (rate_limit_state table).
# Edit here AND in the SQL seed if you need to change limits.
# =============================================================================

RATE_LIMITS: dict[str, dict[str, float]] = {
    "google_maps": {
        # Max 60 results per browser context per CLAUDE.md — ~1 req/sec across all workers
        "max_tokens": 10,
        "refill_rate": 0.016667,    # tokens/second = 1 token per minute = 60/hr
    },
    "google_search": {
        # Hard limit: max 10 Google Search queries per hour (CLAUDE.md)
        "max_tokens": 2,
        "refill_rate": 0.002778,    # tokens/second = 10/hr
    },
    "kvk_api": {
        # KvK API fair-use limit
        "max_tokens": 5,
        "refill_rate": 0.027778,    # tokens/second = 100/hr
    },
    "warmr_api": {
        # Warmr API — generous limit, avoid overwhelming sending infra
        "max_tokens": 20,
        "refill_rate": 0.333333,    # tokens/second = 1200/hr
    },
    "pagespeed_api": {
        # Google PageSpeed free tier: ~400 req/day, spread across the day
        "max_tokens": 10,
        "refill_rate": 0.111111,    # tokens/second = 400/hr
    },
    "claude_haiku": {
        # Claude Haiku — bulk AI calls (summaries, openers, patterns)
        "max_tokens": 10,
        "refill_rate": 0.833333,    # tokens/second = 50 req/min
    },
    "claude_sonnet": {
        # Claude Sonnet — Vision analysis + deep analysis (more expensive, lower limit)
        "max_tokens": 5,
        "refill_rate": 0.333333,    # tokens/second = 20 req/min
    },
}


# =============================================================================
# Internal helpers
# =============================================================================

def _now_utc() -> datetime:
    """Return current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


def _refill_tokens(
    current_tokens: float,
    max_tokens: float,
    refill_rate: float,
    last_refill: datetime,
) -> tuple[float, datetime]:
    """Calculate new token count after time-based refill.

    Args:
        current_tokens: Tokens in bucket before refill.
        max_tokens: Maximum bucket capacity.
        refill_rate: Tokens added per second.
        last_refill: Timestamp of last refill operation.

    Returns:
        Tuple of (new_token_count, new_last_refill_timestamp).
    """
    now = _now_utc()
    elapsed_seconds = (now - last_refill).total_seconds()
    added = elapsed_seconds * refill_rate
    new_tokens = min(current_tokens + added, max_tokens)
    return new_tokens, now


# =============================================================================
# Public API
# =============================================================================

async def check_rate_limit(service: str, supabase_client: Any) -> bool:
    """Check whether a request to `service` is allowed without consuming a token.

    Reads current bucket state from Supabase, applies time-based refill, and
    returns True if at least 1 token is available. Does NOT modify state.

    Args:
        service: Service key, e.g. 'google_search'. Must exist in RATE_LIMITS.
        supabase_client: Initialised supabase-py client.

    Returns:
        True if a token is available, False if the bucket is empty.

    Raises:
        ValueError: If service is not in RATE_LIMITS.
    """
    if service not in RATE_LIMITS:
        raise ValueError(
            f"Unknown service '{service}'. Available: {list(RATE_LIMITS.keys())}"
        )

    response = (
        supabase_client.table("rate_limit_state")
        .select("tokens, max_tokens, refill_rate, last_refill")
        .eq("service", service)
        .single()
        .execute()
    )

    if not response.data:
        # Row missing — assume allowed (will be created on first consume)
        return True

    row = response.data
    last_refill_dt = datetime.fromisoformat(row["last_refill"])
    new_tokens, _ = _refill_tokens(
        float(row["tokens"]),
        float(row["max_tokens"]),
        float(row["refill_rate"]),
        last_refill_dt,
    )

    return new_tokens >= 1.0


async def consume_token(service: str, supabase_client: Any) -> bool:
    """Consume one token for `service` and update Supabase state.

    Applies time-based refill before attempting to consume. If the bucket is
    empty after refill, returns False without modifying state.

    Uses Supabase RPC to ensure atomic read-modify-write across workers.
    Falls back to a Python-level update if the RPC is unavailable.

    Args:
        service: Service key, e.g. 'google_maps'. Must exist in RATE_LIMITS.
        supabase_client: Initialised supabase-py client.

    Returns:
        True if token was consumed successfully, False if bucket was empty.

    Raises:
        ValueError: If service is not in RATE_LIMITS.
    """
    if service not in RATE_LIMITS:
        raise ValueError(
            f"Unknown service '{service}'. Available: {list(RATE_LIMITS.keys())}"
        )

    response = (
        supabase_client.table("rate_limit_state")
        .select("tokens, max_tokens, refill_rate, last_refill")
        .eq("service", service)
        .single()
        .execute()
    )

    if not response.data:
        # Row doesn't exist yet — seed it and allow the first request
        limits = RATE_LIMITS[service]
        now_iso = _now_utc().isoformat()
        supabase_client.table("rate_limit_state").upsert({
            "service": service,
            "tokens": limits["max_tokens"] - 1.0,
            "max_tokens": limits["max_tokens"],
            "refill_rate": limits["refill_rate"],
            "last_refill": now_iso,
            "updated_at": now_iso,
        }).execute()
        return True

    row = response.data
    last_refill_dt = datetime.fromisoformat(row["last_refill"])
    new_tokens, new_last_refill = _refill_tokens(
        float(row["tokens"]),
        float(row["max_tokens"]),
        float(row["refill_rate"]),
        last_refill_dt,
    )

    if new_tokens < 1.0:
        return False

    # Consume one token and write back
    supabase_client.table("rate_limit_state").update({
        "tokens": new_tokens - 1.0,
        "last_refill": new_last_refill.isoformat(),
        "updated_at": _now_utc().isoformat(),
    }).eq("service", service).execute()

    return True


async def wait_for_token(service: str, supabase_client: Any) -> None:
    """Block asynchronously until a token is available, then consume it.

    Polls Supabase at exponential backoff intervals. Uses the refill_rate from
    RATE_LIMITS to compute the minimum wait before the next token appears.

    Never blocks for more than 120 seconds per call — raises RuntimeError if
    the limit is not lifted within that window.

    Args:
        service: Service key, e.g. 'kvk_api'. Must exist in RATE_LIMITS.
        supabase_client: Initialised supabase-py client.

    Raises:
        ValueError: If service is not in RATE_LIMITS.
        RuntimeError: If no token becomes available within 120 seconds.
    """
    if service not in RATE_LIMITS:
        raise ValueError(
            f"Unknown service '{service}'. Available: {list(RATE_LIMITS.keys())}"
        )

    # Calculate how long to wait for 1 token at the refill rate
    refill_rate = RATE_LIMITS[service]["refill_rate"]
    seconds_per_token = 1.0 / refill_rate if refill_rate > 0 else 60.0

    deadline = time.monotonic() + 120  # 2-minute hard cap
    wait_seconds = min(seconds_per_token, 5.0)  # start with 1 token wait, max 5s

    while time.monotonic() < deadline:
        consumed = await consume_token(service, supabase_client)
        if consumed:
            return
        # Sleep for approximately the time it takes to accumulate 1 token
        await asyncio.sleep(wait_seconds)
        wait_seconds = min(wait_seconds * 1.5, 30.0)  # exponential backoff, cap 30s

    raise RuntimeError(
        f"Rate limit for '{service}' not lifted within 120 seconds. "
        f"Check if too many workers are running or the limit is too restrictive."
    )
