"""
config/pricing.py — Heatr API pricing, single source of truth.

Prices in USD (Anthropic publieke prijzen, geverifieerd 2026-05-11), geconverteerd
naar EUR op module load time via EUR_USD_RATE. Override via env var
HEATR_EUR_USD_RATE (handig voor staging of bij Anthropic-prijs-wijzigingen).

Last updated: 2026-05-11
Anthropic prices verified: https://www.anthropic.com/pricing

Vervangt 9 gefragmenteerde prijslijsten in module-code (zie cost_audit_2026-05-11.md
Fix 2). Replaces:
  utils/claude_cache.py (Haiku €0.25/M — ~3.7× te laag)
  7 cluster-A modules (€0.74/M input + €3.68/M output — ~25% te laag bij EUR/USD=0.93)
  enrichment/batched_enrichment.py (had al cache-pricing inline)

Twee public functions:
  - get_price_eur(model, input_tokens, output_tokens, cache_read, cache_write)
      Full pricing met cache-tokens. Voor nieuwe code + claude_cache + batched_enrichment.
  - get_legacy_per_m_eur(model) → {"input_per_m_eur", "output_per_m_eur"}
      Drop-in voor de 7 cluster-A modules die nu _COST_PER_M_INPUT/_OUTPUT constants
      gebruiken. DEPRECATED — verwijderen in volgende commit na full port.
"""
from __future__ import annotations

import logging
import os
from typing import TypedDict

logger = logging.getLogger(__name__)


class ModelPricing(TypedDict):
    input_per_m_usd:        float   # Reguliere input tokens
    output_per_m_usd:       float   # Output tokens
    cache_read_per_m_usd:   float   # Anthropic prompt-cache HIT (90% off input)
    cache_write_per_m_usd:  float   # Anthropic prompt-cache MISS write (25% premium)


# === USD-prijzen, bron: Anthropic publiek (2026-05-11) ===
PRICES_USD: dict[str, ModelPricing] = {
    "claude-haiku-4-5-20251001": {
        "input_per_m_usd":       1.00,
        "output_per_m_usd":      5.00,
        "cache_read_per_m_usd":  0.10,   # 90% korting op input
        "cache_write_per_m_usd": 1.25,   # 25% premium op cache-write
    },
    "claude-sonnet-4-6": {
        "input_per_m_usd":       3.00,
        "output_per_m_usd":     15.00,
        "cache_read_per_m_usd":  0.30,
        "cache_write_per_m_usd": 3.75,
    },
    "claude-opus-4-6": {
        "input_per_m_usd":      15.00,
        "output_per_m_usd":     75.00,
        "cache_read_per_m_usd":  1.50,
        "cache_write_per_m_usd":18.75,
    },
}


# === EUR conversion ===
# Default 0.93 (Q4 2025-koers, mid-range tussen 0.85-0.95).
# Update bij audit refresh — override via HEATR_EUR_USD_RATE env var voor staging
# of bij Anthropic prijswijzigingen.
EUR_USD_RATE = float(os.getenv("HEATR_EUR_USD_RATE", "0.93"))


def get_price_eur(
    model: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_read_tokens: int = 0,
    cache_write_tokens: int = 0,
) -> float:
    """Calculate cost in EUR voor een Claude API-call.

    Args:
        model: model-string (bv. "claude-haiku-4-5-20251001")
        input_tokens: reguliere input (geen cache)
        output_tokens: output tokens
        cache_read_tokens: tokens uit Anthropic prompt-cache (90% korting)
        cache_write_tokens: tokens naar Anthropic prompt-cache geschreven (25% premium)

    Returns:
        Cost in EUR, rounded to 6 decimals.

    Raises:
        ValueError: bij onbekend model (force expliciete toevoeging aan PRICES_USD).
    """
    if model not in PRICES_USD:
        raise ValueError(
            f"Unknown model: {model!r}. Add it to PRICES_USD in config/pricing.py."
        )
    p = PRICES_USD[model]
    cost_usd = (
          (input_tokens        / 1_000_000) * p["input_per_m_usd"]
        + (output_tokens       / 1_000_000) * p["output_per_m_usd"]
        + (cache_read_tokens   / 1_000_000) * p["cache_read_per_m_usd"]
        + (cache_write_tokens  / 1_000_000) * p["cache_write_per_m_usd"]
    )
    cost_eur = round(cost_usd * EUR_USD_RATE, 6)

    # Opus is duur — log expliciet zodat onbedoelde Opus-calls opvallen.
    # Pricing.py blokkeert niet (dat is cost_guard.py's rol); alleen waarschuwen.
    if model.startswith("claude-opus"):
        logger.warning(
            "Opus call detected (model=%s, in=%d, out=%d, cache_r=%d, cache_w=%d). "
            "Cost: €%.4f. Confirm dit was bedoeld.",
            model, input_tokens, output_tokens,
            cache_read_tokens, cache_write_tokens, cost_eur,
        )

    return cost_eur


def get_legacy_per_m_eur(model: str) -> dict[str, float]:
    """[DEPRECATED — verwijder na port van alle 7 cluster-A modules naar get_price_eur()]

    Return EUR-prijzen in legacy 'X per M tokens'-format. Drop-in replacement voor
    de _COST_PER_M_INPUT / _COST_PER_M_OUTPUT constants in:
      - enrichment/{treatment_classifier, archetype_classifier, owner_extractor}.py
      - website_intelligence/sector_checker.py
      - integrations/reply_classifier.py
      - campaigns/{reply_drafter, review_email_generator}.py

    Mist cache-token-support (cluster-A doet die niet). Voor cache-aware modules
    (claude_cache.py + batched_enrichment.py): gebruik get_price_eur() direct.

    Raises:
        ValueError: bij onbekend model.
    """
    if model not in PRICES_USD:
        raise ValueError(f"Unknown model: {model!r}")
    p = PRICES_USD[model]
    return {
        "input_per_m_eur":  round(p["input_per_m_usd"]  * EUR_USD_RATE, 4),
        "output_per_m_eur": round(p["output_per_m_usd"] * EUR_USD_RATE, 4),
    }
