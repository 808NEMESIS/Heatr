"""
utils/claude_cache.py — Claude API call caching via Supabase.

Reduces Claude costs by caching deterministic responses.
Cache key is SHA-256 of normalized prompt + model.

Cache = 7 dagen TTL default. Hit count tracked for analytics.
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Cost per 1M tokens in EUR (approximate, update if pricing changes)
COST_PER_1M_TOKENS: dict[str, float] = {
    "claude-haiku-4-5-20251001": 0.25 / 1_000_000,   # input
    "claude-sonnet-4-6":         3.00 / 1_000_000,
    "claude-opus-4-6":           15.0 / 1_000_000,
}


def _make_cache_key(prompt: str, model: str) -> str:
    """SHA-256 hash of normalized prompt + model string."""
    normalized = f"{model}::{prompt.strip()}"
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


async def cached_claude_call(
    prompt: str,
    cache_key_suffix: str = "",
    model: str = "claude-haiku-4-5-20251001",
    max_tokens: int = 100,
    system: str | None = None,
    ttl_hours: int = 168,  # 7 days
    supabase_client=None,
) -> str:
    """
    Check Supabase cache first. Return cached response on hit.
    On miss: call Claude API, store result, return.

    Args:
        prompt: The user message content.
        cache_key_suffix: Optional extra string to differentiate keys for
                          the same prompt used in different contexts.
        model: Claude model ID.
        max_tokens: Max tokens for Claude response.
        system: Optional system prompt (not cached separately — baked into key).
        ttl_hours: Cache TTL in hours.
        supabase_client: Supabase client instance.

    Returns:
        Response text string.
    """
    key_source = f"{cache_key_suffix}:{system or ''}:{prompt}"
    cache_key = _make_cache_key(key_source, model)

    # --- Cache lookup ---
    if supabase_client:
        try:
            now = datetime.now(timezone.utc).isoformat()
            hit = (
                supabase_client
                .table("claude_cache")
                .select("response, hit_count")
                .eq("cache_key", cache_key)
                .gt("expires_at", now)
                .maybe_single()
                .execute()
            )
            if hit.data:
                # Increment hit count async-fire-and-forget style
                try:
                    supabase_client.table("claude_cache").update(
                        {"hit_count": hit.data["hit_count"] + 1}
                    ).eq("cache_key", cache_key).execute()
                except Exception:
                    pass
                logger.debug("Claude cache HIT for key %s…", cache_key[:12])
                return hit.data["response"]
        except Exception as e:
            logger.debug("Claude cache lookup failed: %s", e)

    # --- API call ---
    import anthropic
    client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    messages = [{"role": "user", "content": prompt}]
    kwargs: dict = dict(model=model, max_tokens=max_tokens, messages=messages)
    if system:
        kwargs["system"] = system

    response = await client.messages.create(**kwargs)
    text = response.content[0].text if response.content else ""
    input_tokens = response.usage.input_tokens or 0
    output_tokens = response.usage.output_tokens or 0
    tokens_used = input_tokens + output_tokens

    # --- Store in cache ---
    if supabase_client and text:
        try:
            expires_at = (datetime.now(timezone.utc) + timedelta(hours=ttl_hours)).isoformat()
            supabase_client.table("claude_cache").upsert({
                "cache_key": cache_key,
                "prompt_hash": cache_key[:16],
                "model": model,
                "response": text,
                "tokens_used": tokens_used,
                "expires_at": expires_at,
                "hit_count": 0,
            }, on_conflict="cache_key").execute()
            logger.debug("Claude cache MISS stored for key %s…", cache_key[:12])
        except Exception as e:
            logger.debug("Claude cache store failed: %s", e)

    # --- Log cost ---
    if supabase_client:
        cost = tokens_used * COST_PER_1M_TOKENS.get(model, 0)
        await _log_api_cost(
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_eur=cost,
            context=cache_key_suffix[:50] if cache_key_suffix else "",
            supabase_client=supabase_client,
        )

    return text


async def invalidate_cache_for_domain(domain: str, supabase_client) -> int:
    """Remove all cached entries that reference a specific domain.
    Used when a website is re-analysed after an update."""
    try:
        # We can't efficiently reverse-lookup by domain without extra columns,
        # so we rely on TTL expiry. This is a soft invalidation.
        res = supabase_client.table("claude_cache").delete().like("prompt_hash", f"%{domain[:8]}%").execute()
        return len(res.data or [])
    except Exception:
        return 0


async def _log_api_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_eur: float,
    context: str = "",
    workspace_id: str = "aerys",
    lead_id: str | None = None,
    supabase_client=None,
) -> None:
    """Log a Claude API cost entry to heatr_api_cost_log."""
    if not supabase_client:
        return
    try:
        row: dict = {
            "workspace_id": workspace_id,
            "model": model,
            "prompt_tokens": input_tokens,
            "response_tokens": output_tokens,
            "cost_eur": round(cost_eur, 6),
            "context": context,
        }
        if lead_id:
            row["lead_id"] = lead_id
        supabase_client.table("api_cost_log").insert(row).execute()
    except Exception as e:
        logger.warning("Cost log failed (context=%s): %s", context, e)


async def log_api_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
    cost_eur: float,
    workspace_id: str,
    supabase_client,
    context: str = "",
    lead_id: str | None = None,
) -> None:
    """Public interface for cost logging from other modules."""
    await _log_api_cost(
        model=model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_eur=cost_eur,
        context=context,
        workspace_id=workspace_id,
        lead_id=lead_id,
        supabase_client=supabase_client,
    )
