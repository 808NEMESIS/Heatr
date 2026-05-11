"""Verify claude_cache werkt: 2 keer zelfde call, tweede moet cache_hit zijn.

Run pas NÁ migration 015_claude_cache.sql in Supabase. Doet 2 echte Anthropic-calls
(€0.0001 totaal) — geen runaway-cost.

Output: per-test resultaat + DB-state-bevestiging + cache-hit-counts.
"""
import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv("/Users/nemesis/Heatr/.env")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.database import get_heatr_supabase
from utils.claude_cache import cached_claude_call


TEST_PROMPT_SECTOR = "Test verify_claude_cache: returneer letterlijk het woord OK en niets anders."
TEST_PROMPT_REPLY = "Test verify_claude_cache (reply pad): returneer letterlijk het woord OK."


async def main() -> int:
    db = get_heatr_supabase()

    # Verifieer dat tabel bestaat — anders stoppen voordat we API-calls verspillen
    try:
        check = db.table("claude_cache").select("cache_key").limit(1).execute()
        print(f"✓ heatr_claude_cache tabel bestaat ({len(check.data or [])} bestaande rows)")
    except Exception as e:
        print(f"✗ heatr_claude_cache tabel niet gevonden: {e}")
        print("  → Run eerst migration 015_claude_cache.sql in Supabase SQL editor.")
        return 1

    print()
    print("=" * 70)
    print("Test 1: sector_checker context (TTL 30 dagen) — 2 identieke calls")
    print("=" * 70)

    r1 = await cached_claude_call(
        prompt=TEST_PROMPT_SECTOR,
        model="claude-haiku-4-5-20251001",
        max_tokens=20,
        context="sector_checker",
        supabase_client=db,
    )
    print(f"r1 (verwacht MISS, fresh API call): {r1!r}")

    r2 = await cached_claude_call(
        prompt=TEST_PROMPT_SECTOR,
        model="claude-haiku-4-5-20251001",
        max_tokens=20,
        context="sector_checker",
        supabase_client=db,
    )
    print(f"r2 (verwacht HIT, uit cache):       {r2!r}")

    print()
    print("=" * 70)
    print("Test 2: reply_classifier (NO_CACHE) — 2 calls, beide fresh")
    print("=" * 70)

    r3 = await cached_claude_call(
        prompt=TEST_PROMPT_REPLY,
        model="claude-haiku-4-5-20251001",
        max_tokens=20,
        context="reply_classifier",
        supabase_client=db,
    )
    print(f"r3 (verwacht fresh — no cache):     {r3!r}")

    r4 = await cached_claude_call(
        prompt=TEST_PROMPT_REPLY,
        model="claude-haiku-4-5-20251001",
        max_tokens=20,
        context="reply_classifier",
        supabase_client=db,
    )
    print(f"r4 (verwacht fresh — no cache):     {r4!r}")

    # DB-state na de calls
    print()
    print("=" * 70)
    print("DB-state na verify")
    print("=" * 70)

    # Welke contexts staan in cache?
    cache_rows = (
        db.table("claude_cache")
        .select("cache_key, context, hit_count, expires_at, input_tokens, output_tokens")
        .order("created_at", desc=True)
        .limit(20)
        .execute()
    )
    print(f"\nclaude_cache (top 20 newest):")
    print(f"  {'context':24s} {'hit_count':>10s} {'in_tok':>7s} {'out_tok':>7s}  cache_key (first 12)")
    for row in cache_rows.data or []:
        print(f"  {(row.get('context') or '<empty>')[:23]:24s} "
              f"{row.get('hit_count') or 0:>10d} "
              f"{row.get('input_tokens') or 0:>7d} "
              f"{row.get('output_tokens') or 0:>7d}  "
              f"{(row.get('cache_key') or '')[:12]}…")

    # Recente api_cost_log entries voor onze test-contexts.
    # Probeer eerst met cache_hit-kolom (migration 016); fallback zonder.
    print(f"\napi_cost_log (recente entries voor sector_checker + reply_classifier):")
    cache_hit_col_available = True
    try:
        log_rows = (
            db.table("api_cost_log")
            .select("model, prompt_tokens, response_tokens, cost_eur, cache_hit, context, created_at")
            .in_("context", ["sector_checker", "reply_classifier"])
            .order("created_at", desc=True)
            .limit(10)
            .execute()
        )
    except Exception as e:
        if "cache_hit" in str(e).lower():
            cache_hit_col_available = False
            print("  ⚠ cache_hit-kolom niet aanwezig — apply migration 016_api_cost_log_cache_hit.sql")
            log_rows = (
                db.table("api_cost_log")
                .select("model, prompt_tokens, response_tokens, cost_eur, context, created_at")
                .in_("context", ["sector_checker", "reply_classifier"])
                .order("created_at", desc=True)
                .limit(10)
                .execute()
            )
        else:
            raise
    cols = "  {:22s} {:>7s} {:>7s} {:>10s}".format("context", "in_tok", "out_tok", "cost_eur")
    if cache_hit_col_available:
        cols += " {:>10s}".format("cache_hit")
    print(cols)
    for row in log_rows.data or []:
        line = "  {:22s} {:>7d} {:>7d} {:>10.6f}".format(
            (row.get('context') or '<empty>')[:21],
            row.get('prompt_tokens') or 0,
            row.get('response_tokens') or 0,
            float(row.get('cost_eur') or 0),
        )
        if cache_hit_col_available:
            line += " {:>10s}".format(str(row.get('cache_hit')))
        print(line)

    # Verdict
    print()
    print("=" * 70)
    print("Verdict")
    print("=" * 70)
    same_text = (r1.strip() == r2.strip())
    sector_in_cache = any((r.get("context") or "").startswith("sector_checker") for r in (cache_rows.data or []))
    reply_in_cache = any((r.get("context") or "").startswith("reply_classifier") for r in (cache_rows.data or []))

    print(f"  r1 == r2 (identieke response):                {'✓' if same_text else '✗'}")
    print(f"  sector_checker in claude_cache (cached):      {'✓' if sector_in_cache else '✗'}")
    print(f"  reply_classifier NIET in claude_cache (skip): {'✓' if not reply_in_cache else '✗'}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
