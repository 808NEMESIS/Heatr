"""
config/principles_loader.py — Lazy-cache loader voor opener_principles.md.

De rubric (399 regels) wordt 1× per worker-process van disk gelezen, daarna
in-memory gecached. Wordt geinjecteerd in Claude Haiku system prompt voor
opener_generator en reply_drafter — voor consistente evidence-based tone.

Cache invalidatie: process-restart. Voor pytest: principle_text() retourneert
direct of expliciet None geforced via env (`HEATR_PRINCIPLES_DISABLED=true`).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_PRINCIPLES_PATH = Path(__file__).parent / "opener_principles.md"
_cache: str | None = None
_loaded: bool = False


def _load_from_disk() -> str | None:
    """Read en cache. Returns None als file ontbreekt of disabled via env."""
    global _cache, _loaded
    if os.getenv("HEATR_PRINCIPLES_DISABLED", "").lower() == "true":
        _loaded = True
        return None
    try:
        text = _PRINCIPLES_PATH.read_text(encoding="utf-8")
        # Truncate to ~12KB om context-window-budget niet op te eten
        if len(text) > 12000:
            text = text[:12000] + "\n\n[truncated voor context-budget]"
        _cache = text.strip()
        _loaded = True
        logger.info("Loaded opener_principles.md (%d chars)", len(_cache))
        return _cache
    except FileNotFoundError:
        _loaded = True
        logger.warning("opener_principles.md ontbreekt op %s — drafts gebruiken legacy prompts", _PRINCIPLES_PATH)
        return None
    except Exception as e:
        _loaded = True
        logger.error("Kon opener_principles.md niet lezen: %s", e)
        return None


def get_principles() -> str | None:
    """Return de rubric-tekst (gecached) of None als niet beschikbaar."""
    if not _loaded:
        return _load_from_disk()
    return _cache


def reset_cache() -> None:
    """Forceer herlezen — voor tests of na file-update zonder restart."""
    global _cache, _loaded
    _cache = None
    _loaded = False
