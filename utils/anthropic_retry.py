"""
utils/anthropic_retry.py — retry-wrapper voor Anthropic-calls bij rate-limits.

Waarom: enrichment-batches (5+ concurrente Claude-calls) kunnen 429 triggeren.
Vóór deze helper werd een 429 door een bare `except Exception` opgeslokt en
viel de call terug op een sync-client die dezelfde 429 kreeg — silent failure
terwijl de per-lead cost al gemaakt was (audit 2026-07-05, backend sectie 2).

Gedrag:
  - Retryt op HTTP 429 (RateLimitError) en 503/529 (overloaded) met
    exponential backoff: base_delay * 2^attempt.
  - Respecteert de `retry-after` response-header als die aanwezig is.
  - Logt elke retry-poging met lead_id + context (attributie in logs).
  - Na max_retries: duidelijke error-log + re-raise. NOOIT silent.
  - Andere exceptions worden direct doorgelaten — bestaande fallback-logica
    in callers (bv. sync-client fallback) blijft intact voor niet-rate-limit
    fouten.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

import anthropic

logger = logging.getLogger(__name__)

# Statussen waarvoor een retry zinvol is: rate-limit + tijdelijke overload.
_RETRYABLE_STATUS_CODES = (429, 503, 529)


def _retry_after_seconds(exc: Exception) -> float | None:
    """Lees de retry-after header uit een Anthropic API-error, als aanwezig."""
    response = getattr(exc, "response", None)
    if response is None:
        return None
    raw = response.headers.get("retry-after")
    if not raw:
        return None
    try:
        return max(float(raw), 0.0)
    except ValueError:
        return None


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, anthropic.RateLimitError):
        return True
    if isinstance(exc, anthropic.APIStatusError):
        return getattr(exc, "status_code", None) in _RETRYABLE_STATUS_CODES
    return False


async def anthropic_call_with_retry(
    make_call: Callable[[], Awaitable[Any]],
    *,
    lead_id: str | None = None,
    context: str = "",
    max_retries: int = 3,
    base_delay: float = 2.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> Any:
    """Voer een async Anthropic-call uit met backoff-retry op rate-limits.

    Args:
        make_call: async callable zonder args die de daadwerkelijke
            messages.create(...) doet.
        lead_id: voor log-attributie (welke lead kostte retries).
        context: enrichment-context voor log-attributie.
        max_retries: aantal HERHALINGEN na de eerste poging (3 = max 4 calls).
        base_delay: startvertraging in seconden; verdubbelt per poging.
        sleep: injectable voor tests (default asyncio.sleep).

    Raises:
        De laatste rate-limit-error na uitputting van retries, of elke
        niet-retryable exception direct (fallback-logica caller blijft werken).
    """
    for attempt in range(max_retries + 1):
        try:
            return await make_call()
        except Exception as exc:
            if not _is_retryable(exc):
                raise
            if attempt >= max_retries:
                logger.error(
                    "anthropic_retry: opgegeven na %d pogingen (lead_id=%s context=%s): %s",
                    attempt + 1, lead_id, context, exc,
                )
                raise
            delay = _retry_after_seconds(exc)
            if delay is None:
                delay = base_delay * (2 ** attempt)
            logger.warning(
                "anthropic_retry: rate-limit (poging %d/%d, lead_id=%s context=%s) — "
                "retry over %.1fs",
                attempt + 1, max_retries + 1, lead_id, context, delay,
            )
            await sleep(delay)
