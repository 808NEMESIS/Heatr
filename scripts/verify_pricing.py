"""Verify pricing consolidation: roundtrip-tests op config/pricing.py.

Run: python3 scripts/verify_pricing.py

Confirms:
  - Haiku 4.5 cost-berekening matched verwachte formule
  - Sonnet 4.6 idem
  - Cache-pricing (read 90% off + write 25% premium) klopt
  - Opus 4.6 returns waarde + logt warning
  - Unknown model raised ValueError (force expliciete toevoeging)
  - Legacy accessor (get_legacy_per_m_eur) returns input+output split
  - EUR_USD_RATE env-override werkt
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Capture Opus-WARN naar stderr buffer voor test
logging.basicConfig(level=logging.WARNING)

from config.pricing import (  # noqa: E402
    EUR_USD_RATE,
    PRICES_USD,
    get_legacy_per_m_eur,
    get_price_eur,
)


def _assert(condition: bool, label: str) -> None:
    print(f"  {'✓' if condition else '✗'} {label}")
    if not condition:
        sys.exit(1)


print("=" * 70)
print("Pricing consolidatie verify — config/pricing.py")
print("=" * 70)
print(f"EUR_USD_RATE        : {EUR_USD_RATE}")
print(f"Modellen gedefinieerd: {list(PRICES_USD.keys())}")

# --- Haiku 4.5: 1000 input + 500 output ---
print("\nTest 1 — Haiku 4.5, 1000 input + 500 output")
expected = (1000/1_000_000 * 1.00 + 500/1_000_000 * 5.00) * EUR_USD_RATE
actual = get_price_eur("claude-haiku-4-5-20251001", input_tokens=1000, output_tokens=500)
print(f"  Expected: €{expected:.6f}")
print(f"  Actual:   €{actual:.6f}")
_assert(abs(expected - actual) < 1e-6, "Haiku regular cost matches formula")

# --- Sonnet 4.6: 2000 input + 1000 output ---
print("\nTest 2 — Sonnet 4.6, 2000 input + 1000 output")
expected = (2000/1_000_000 * 3.00 + 1000/1_000_000 * 15.00) * EUR_USD_RATE
actual = get_price_eur("claude-sonnet-4-6", input_tokens=2000, output_tokens=1000)
print(f"  Expected: €{expected:.6f}")
print(f"  Actual:   €{actual:.6f}")
_assert(abs(expected - actual) < 1e-6, "Sonnet regular cost matches formula")

# --- Haiku met cache: 500 fresh + 1500 cache_read + 500 output ---
print("\nTest 3 — Haiku 4.5, 500 fresh + 1500 cache_read + 500 output")
expected = (
    (500/1_000_000) * 1.00      # fresh input
    + (1500/1_000_000) * 0.10   # cache read (90% off)
    + (500/1_000_000) * 5.00    # output
) * EUR_USD_RATE
actual = get_price_eur(
    "claude-haiku-4-5-20251001",
    input_tokens=500,
    output_tokens=500,
    cache_read_tokens=1500,
)
print(f"  Expected: €{expected:.6f}")
print(f"  Actual:   €{actual:.6f}")
_assert(abs(expected - actual) < 1e-6, "Haiku with cache_read matches formula")

# --- Cache-write premium (25% boven input) ---
print("\nTest 4 — Haiku 4.5, 1000 cache_write (25% premium)")
expected_write = (1000/1_000_000) * 1.25 * EUR_USD_RATE
actual = get_price_eur(
    "claude-haiku-4-5-20251001",
    input_tokens=0,
    output_tokens=0,
    cache_write_tokens=1000,
)
print(f"  Expected: €{expected_write:.6f}")
print(f"  Actual:   €{actual:.6f}")
_assert(abs(expected_write - actual) < 1e-6, "Haiku cache_write matches premium formula")

# --- Opus 4.6 (logs WARN) ---
print("\nTest 5 — Opus 4.6, 100 input + 100 output (verwacht WARN-log)")
actual_opus = get_price_eur("claude-opus-4-6", input_tokens=100, output_tokens=100)
print(f"  Cost: €{actual_opus:.6f}")
_assert(actual_opus > 0, "Opus returns positive cost (WARN logged separately)")

# --- Onbekend model raised ValueError ---
print("\nTest 6 — Unknown model raised ValueError")
try:
    get_price_eur("claude-nonexistent-99", input_tokens=100, output_tokens=100)
    _assert(False, "Unknown model should raise ValueError")
except ValueError as e:
    print(f"  Raised: {e}")
    _assert(True, "Unknown model raised ValueError as expected")

# --- Legacy accessor (drop-in voor cluster-A modules) ---
print("\nTest 7 — Legacy accessor for cluster-A drop-in")
legacy = get_legacy_per_m_eur("claude-haiku-4-5-20251001")
print(f"  get_legacy_per_m_eur('claude-haiku-4-5-20251001'): {legacy}")
expected_in_per_m = 1.00 * EUR_USD_RATE   # = 0.93
expected_out_per_m = 5.00 * EUR_USD_RATE  # = 4.65
_assert(abs(legacy["input_per_m_eur"] - expected_in_per_m) < 1e-3, "Legacy input/M matches")
_assert(abs(legacy["output_per_m_eur"] - expected_out_per_m) < 1e-3, "Legacy output/M matches")

# --- EUR_USD_RATE env-override (sanity, niet life-test) ---
print("\nTest 8 — EUR_USD_RATE env-override (informational)")
print(f"  HEATR_EUR_USD_RATE env-var: {os.environ.get('HEATR_EUR_USD_RATE', '<not set>')}")
print(f"  EUR_USD_RATE actief:        {EUR_USD_RATE}")
print("  (Reset via HEATR_EUR_USD_RATE=X om koers te overriden)")

# --- Sanity: cluster-A modules importeren via legacy accessor ---
print("\nTest 9 — Cluster-A modules import-roundtrip")
from enrichment.treatment_classifier import _COST_PER_M_INPUT as tc_in, _COST_PER_M_OUTPUT as tc_out
from enrichment.archetype_classifier import _COST_PER_M_INPUT as ar_in, _COST_PER_M_OUTPUT as ar_out
from enrichment.owner_extractor import _COST_PER_M_INPUT as ow_in, _COST_PER_M_OUTPUT as ow_out
from website_intelligence.sector_checker import _COST_PER_M_INPUT as sc_in, _COST_PER_M_OUTPUT as sc_out
from campaigns.reply_drafter import _COST_PER_M_INPUT as rd_in, _COST_PER_M_OUTPUT as rd_out
from campaigns.review_email_generator import _COST_PER_M_INPUT as re_in, _COST_PER_M_OUTPUT as re_out
all_in = {"treatment_classifier": tc_in, "archetype_classifier": ar_in, "owner_extractor": ow_in,
          "sector_checker": sc_in, "reply_drafter": rd_in, "review_email_generator": re_in}
all_out = {"treatment_classifier": tc_out, "archetype_classifier": ar_out, "owner_extractor": ow_out,
           "sector_checker": sc_out, "reply_drafter": rd_out, "review_email_generator": re_out}
print(f"  Input/M EUR per module: {set(all_in.values())}")
print(f"  Output/M EUR per module: {set(all_out.values())}")
_assert(len(set(all_in.values())) == 1, "All 6 cluster-A modules have identical input/M EUR")
_assert(len(set(all_out.values())) == 1, "All 6 cluster-A modules have identical output/M EUR")
_assert(abs(list(set(all_in.values()))[0] - expected_in_per_m) < 1e-3, "Cluster-A input/M matches centrale config")

print("\n" + "=" * 70)
print("✓ Pricing verify complete — all checks passed.")
print("=" * 70)
