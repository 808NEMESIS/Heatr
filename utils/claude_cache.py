"""
utils/claude_cache.py — Claude API call caching via Supabase.

Reduces Claude costs by caching deterministic responses.
Cache key is SHA-256 of normalized prompt + model + (context|cache_key_suffix).

Per-context TTL (sinds migration 015_claude_cache, 2026-05-11):
  - Stable classifiers (sector_checker, treatment_classifier, archetype): 30 dagen
  - Enrichment (owner_extract, personalization, contact_extract, batched): 7 dagen
  - Vision (visual_analyzer): 14 dagen
  - Replies (reply_classifier, reply_drafter, review_email_generator): GEEN cache
  - Onbekende context: 7 dagen (veilige default)

Hit count tracked for analytics. `cache_hit=True` wordt ook gelogd in api_cost_log
zodat verify-scripts cache-effectiviteit kunnen meten.
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

# Per-context TTL voor heatr_claude_cache. Aangehaakt aan caller-context-string
# (parameter `context` van cached_claude_call). Onbekende context → DEFAULT_TTL_DAYS.
CONTEXT_TTL_DAYS: dict[str, int] = {
    # Stable classifiers — sector/treatment/archetype zijn semi-deterministisch
    # en wijzigen zelden per bedrijf, dus lange TTL is veilig.
    "sector_checker":          30,
    "treatment_classifier":    30,
    "archetype_classify":      30,
    "archetype_classifier":    30,
    # Enrichment — page-content kan vaker veranderen; matige TTL.
    "owner_extract":            7,
    "owner_extractor":          7,
    "personalization":          7,
    "personalization_extractor": 7,
    "contact_extract":          7,
    "contact_extractor":        7,
    "batched_enrichment":       7,
    # Vision — screenshots zijn duur (Sonnet); cache wat langer.
    "visual_analyzer":         14,
    "vision":                  14,
}

# Contexts waar cache structureel NIET zinvol is — replies zijn per-lead-uniek
# en mailtekst-generatie wil je per send fresh.
NO_CACHE_CONTEXTS: set[str] = {
    "reply_classifier",
    "reply_drafter",
    "review_email_generator",
}

DEFAULT_TTL_DAYS = 7


def _ttl_days_for_context(context: str) -> int:
    """Return TTL in days op basis van context-string. Default 7."""
    if not context:
        return DEFAULT_TTL_DAYS
    return CONTEXT_TTL_DAYS.get(context, DEFAULT_TTL_DAYS)


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
    ttl_hours: int | None = None,
    supabase_client=None,
    context: str = "",
) -> str:
    """
    Check Supabase cache first. Return cached response on hit.
    On miss: call Claude API, store result, return.

    Args:
        prompt: The user message content.
        cache_key_suffix: Optional extra string to differentiate keys (legacy).
                          Voor nieuwe callers: gebruik `context` ipv dit.
        model: Claude model ID.
        max_tokens: Max tokens for Claude response.
        system: Optional system prompt (not cached separately — baked into key).
        ttl_hours: Override TTL in hours. None = derive from context via
                   CONTEXT_TTL_DAYS (default 7 days).
        supabase_client: Supabase client instance.
        context: Caller-naam (bv. 'sector_checker'). Bepaalt:
                 - TTL via CONTEXT_TTL_DAYS
                 - Cache-skip via NO_CACHE_CONTEXTS
                 - api_cost_log.context veld
                 Backward-compat: als context leeg is maar cache_key_suffix
                 wel ingevuld, gebruik cache_key_suffix als context-proxy.

    Returns:
        Response text string.
    """
    # Context-resolutie: nieuwe `context`-param wint, anders fallback op
    # legacy `cache_key_suffix` (12 bestaande callers gebruiken die zo).
    effective_context = context or cache_key_suffix or ""

    # Bepaal effectieve TTL (param overrides context-mapping)
    if ttl_hours is None:
        ttl_hours = _ttl_days_for_context(effective_context) * 24

    # Bypass cache volledig voor reply/draft/email contexts — niet zinvol om
    # per-lead-unieke responses te cachen.
    skip_cache = effective_context in NO_CACHE_CONTEXTS

    key_source = f"{cache_key_suffix or context}:{system or ''}:{prompt}"
    cache_key = _make_cache_key(key_source, model)

    # --- Cache lookup ---
    cache_hit_flag = False
    if supabase_client and not skip_cache:
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
            if hit and hit.data:
                # Increment hit count async-fire-and-forget style
                try:
                    supabase_client.table("claude_cache").update(
                        {"hit_count": (hit.data.get("hit_count") or 0) + 1}
                    ).eq("cache_key", cache_key).execute()
                except Exception:
                    pass
                logger.debug("Claude cache HIT for key %s…", cache_key[:12])
                # Log cache-hit naar api_cost_log met cost=0 (niets betaald)
                if supabase_client:
                    await _log_api_cost(
                        model=model,
                        input_tokens=0,
                        output_tokens=0,
                        cost_eur=0.0,
                        context=(effective_context or "")[:50],
                        cache_hit=True,
                        supabase_client=supabase_client,
                    )
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
    if supabase_client and text and not skip_cache:
        try:
            expires_at = (datetime.now(timezone.utc) + timedelta(hours=ttl_hours)).isoformat()
            supabase_client.table("claude_cache").upsert({
                "cache_key":     cache_key,
                "context":       effective_context[:100],
                "prompt_hash":   cache_key[:16],
                "model":         model,
                "response":      text,
                "input_tokens":  input_tokens,
                "output_tokens": output_tokens,
                # tokens_used is een GENERATED column in DB — niet schrijven
                "expires_at":    expires_at,
                "hit_count":     0,
            }, on_conflict="cache_key").execute()
            logger.debug("Claude cache MISS stored for key %s… (TTL %dh)", cache_key[:12], ttl_hours)
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
            context=(effective_context or "")[:50],
            cache_hit=cache_hit_flag,
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
    cache_hit: bool = False,
    supabase_client=None,
) -> None:
    """Log a Claude API cost entry to heatr_api_cost_log.

    cache_hit=True wordt gezet voor cache-hit-rows zodat verify-scripts
    en cost-audits cache-effectiviteit kunnen meten. Cache-hits hebben
    typically input/output_tokens=0 en cost_eur=0.0 (niets betaald).
    """
    if not supabase_client:
        return
    row: dict = {
        "workspace_id": workspace_id,
        "model": model,
        "prompt_tokens": input_tokens,
        "response_tokens": output_tokens,
        "cost_eur": round(cost_eur, 6),
        "context": context,
        "cache_hit": cache_hit,
    }
    if lead_id:
        row["lead_id"] = lead_id
    try:
        supabase_client.table("api_cost_log").insert(row).execute()
    except Exception as e:
        # Fallback: als cache_hit-kolom nog niet bestaat (migration 016 niet toegepast),
        # log zonder dat veld zodat de pipeline blijft werken.
        msg = str(e).lower()
        if "cache_hit" in msg and ("schema cache" in msg or "does not exist" in msg):
            row.pop("cache_hit", None)
            try:
                supabase_client.table("api_cost_log").insert(row).execute()
                logger.debug("Cost log fallback (no cache_hit column) succeeded — apply migration 016")
            except Exception as e2:
                logger.warning("Cost log fallback also failed (context=%s): %s", context, e2)
        else:
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
    cache_hit: bool = False,
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
        cache_hit=cache_hit,
        supabase_client=supabase_client,
    )
