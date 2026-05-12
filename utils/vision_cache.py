"""
utils/vision_cache.py — SHA-256 content-based cache voor Claude Sonnet Vision calls.

Claude Sonnet Vision is de duurste stap in de enrichment pipeline (~€0.014/lead,
87% van totaalkost per lead). Veel websites veranderen zelden — dus dezelfde
screenshot-bytes = dezelfde vision-response. Cache daarop.

Sleutel: sha256(png_bytes). Twee pixels verschil = nieuwe key = nieuwe call.
Dat is conservatief maar veilig: we willen nooit een stale Vision-result op
een website die visueel is veranderd.

TTL: 90 dagen via periodieke cleanup. Niet afgedwongen op read — een hit op
oude cache is geldig (de website was toen goed/slecht, daar plannen we cold
outreach op).

Tabel: heatr_vision_cache (migration 006).
"""
from __future__ import annotations

import base64
import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def screenshot_hash(screenshot_b64: str) -> str:
    """SHA-256 hex digest of decoded PNG bytes."""
    if not screenshot_b64:
        return ""
    try:
        raw = base64.b64decode(screenshot_b64)
    except Exception:
        return ""
    return hashlib.sha256(raw).hexdigest()


async def get_cached_vision(
    screenshot_b64: str,
    supabase_client: Any,
) -> dict | None:
    """Return cached vision result dict, or None on miss / error.

    Also bumps `cost_saved_count` + `last_hit_at` on a hit so dashboards
    can measure cache ROI without extra plumbing.
    """
    key = screenshot_hash(screenshot_b64)
    if not key:
        return None
    try:
        res = (
            supabase_client.table("vision_cache")
            .select("screenshot_hash, result, cost_saved_count")
            .eq("screenshot_hash", key)
            .maybe_single()
            .execute()
        )
    except Exception as e:
        logger.debug("vision_cache: lookup failed: %s", e)
        return None

    if not res.data:
        return None

    cached = res.data
    # Fire-and-forget bump of cost_saved_count
    try:
        supabase_client.table("vision_cache").update({
            "cost_saved_count": (cached.get("cost_saved_count") or 0) + 1,
            "last_hit_at": datetime.now(timezone.utc).isoformat(),
        }).eq("screenshot_hash", key).execute()
    except Exception as e:
        logger.debug("vision_cache: bump failed: %s", e)

    logger.info("vision_cache: HIT key=%s saved_count=%d", key[:12], cached.get("cost_saved_count") or 0)
    return cached.get("result")


async def store_vision_result(
    screenshot_b64: str,
    domain: str,
    result: dict,
    input_tokens: int,
    output_tokens: int,
    supabase_client: Any,
) -> None:
    """Persist a fresh Vision result. Idempotent upsert on screenshot_hash.

    Silent on error — caching is a best-effort optimization.
    """
    key = screenshot_hash(screenshot_b64)
    if not key:
        return
    try:
        supabase_client.table("vision_cache").upsert({
            "screenshot_hash": key,
            "domain": domain,
            "result": result,
            "input_tokens": input_tokens or 0,
            "output_tokens": output_tokens or 0,
        }, on_conflict="screenshot_hash").execute()
    except Exception as e:
        logger.debug("vision_cache: store failed: %s", e)
