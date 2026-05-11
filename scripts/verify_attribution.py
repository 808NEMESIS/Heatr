"""Verify attribution-fix: alle 3 B-cluster functies geven lead_id door naar api_cost_log.

Run pas NA fix 1 + 2 (claude_cache tabel + api_cost_log.cache_hit kolom).

Tests:
  - B1: cached_claude_call(..., lead_id=X) → api_cost_log row met lead_id=X
  - B2: check_sector_specific(..., lead_id=X) → api_cost_log row met lead_id=X
  - B3: classify_reply(..., lead_id=X) → api_cost_log row met lead_id=X
  - Soft-warning: _log_api_cost(lead_id=None) logt WARN

Output: per cluster verdict, DB-state, cleanup van test-rows.
"""
from __future__ import annotations

import asyncio
import io
import logging
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv
load_dotenv("/Users/nemesis/Heatr/.env")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

TEST_LEAD_ID = str(uuid.uuid4())

# Capture warning-output voor soft-warning test
log_stream = io.StringIO()
log_handler = logging.StreamHandler(log_stream)
log_handler.setLevel(logging.WARNING)
logging.getLogger("utils.claude_cache").addHandler(log_handler)
logging.getLogger("utils.claude_cache").setLevel(logging.WARNING)


async def run_tests() -> int:
    import os
    import anthropic
    from config.database import get_heatr_supabase
    from utils.claude_cache import _log_api_cost, cached_claude_call
    from integrations.reply_classifier import classify_reply
    from website_intelligence.sector_checker import check_sector_specific

    db = get_heatr_supabase()
    anth_client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    print(f"Test lead_id: {TEST_LEAD_ID}\n")

    # === B1: cached_claude_call ===
    print("=" * 70)
    print("B1: cached_claude_call(..., lead_id=...)")
    print("=" * 70)
    r1 = await cached_claude_call(
        prompt="Attribution test: returneer alleen het woord OK.",
        cache_key_suffix=f"attribution_test_b1:{TEST_LEAD_ID}",
        model="claude-haiku-4-5-20251001",
        max_tokens=20,
        context="personalization",
        supabase_client=db,
        lead_id=TEST_LEAD_ID,
    )
    print(f"  Response: {r1!r}")

    # === B2: check_sector_specific ===
    print("\n" + "=" * 70)
    print("B2: check_sector_specific(..., lead_id=...)")
    print("=" * 70)
    # Geef een minimal HTML zodat de Claude-call triggert maar token-budget laag blijft
    page_html = "<html><body><h1>Attribution test clinic</h1><p>Cosmetisch test.</p></body></html>"
    try:
        await check_sector_specific(
            domain=f"attribution-test-{uuid.uuid4().hex[:6]}.example",
            page_html=page_html,
            sector_key="cosmetische_behandelaars",
            anthropic_client=anth_client,
            supabase_client=db,
            lead_id=TEST_LEAD_ID,
        )
        print("  ✓ check_sector_specific call completed")
    except Exception as e:
        print(f"  ⚠ check_sector_specific raised (ok if pricing-only fail): {e!r}")

    # === B3: classify_reply ===
    print("\n" + "=" * 70)
    print("B3: classify_reply(..., lead_id=...)")
    print("=" * 70)
    try:
        await classify_reply(
            reply_text="Bedankt voor jullie mail, klinkt interessant.",
            reply_from="test@attribution.example",
            lead_company="Attribution Test BV",
            supabase_client=db,
            anthropic_client=anth_client,
            lead_id=TEST_LEAD_ID,
        )
        print("  ✓ classify_reply call completed")
    except Exception as e:
        print(f"  ⚠ classify_reply raised: {e!r}")

    # === DB-check: alle 3 contexts moeten rij met lead_id hebben ===
    print("\n" + "=" * 70)
    print(f"DB-check: rows met lead_id={TEST_LEAD_ID}")
    print("=" * 70)
    rows = (
        db.table("api_cost_log")
        .select("context, lead_id, cost_eur, cache_hit, created_at")
        .eq("lead_id", TEST_LEAD_ID)
        .execute()
    )
    print(f"Rows gevonden: {len(rows.data or [])}")
    contexts_found: set[str] = set()
    for r in rows.data or []:
        print(f"  context={(r.get('context') or '')[:35]:35s} cost={float(r.get('cost_eur') or 0):.6f} cache_hit={r.get('cache_hit')}")
        ctx = (r.get("context") or "").split(":", 1)[0]
        contexts_found.add(ctx)

    # === Verdict per cluster ===
    print("\nVerdict per cluster:")
    b1_ok = "personalization" in contexts_found
    b2_ok = "sector_checker" in contexts_found
    b3_ok = "reply_classify" in contexts_found
    print(f"  B1 (personalization):  {'✓' if b1_ok else '✗'}")
    print(f"  B2 (sector_checker):   {'✓' if b2_ok else '✗'}")
    print(f"  B3 (reply_classify):   {'✓' if b3_ok else '✗'}")

    # === Soft-warning test ===
    print("\n" + "=" * 70)
    print("Soft-warning: _log_api_cost(lead_id=None)")
    print("=" * 70)
    log_stream.seek(0)
    log_stream.truncate(0)
    await _log_api_cost(
        model="claude-haiku-4-5-20251001",
        input_tokens=10,
        output_tokens=5,
        cost_eur=0.0,
        context="attribution_test_no_lead_id",
        cache_hit=False,
        lead_id=None,
        supabase_client=None,  # skip DB-write voor pure warning-test
    )
    warning_output = log_stream.getvalue()
    warning_ok = "without lead_id" in warning_output.lower() and "attribution-drift" in warning_output.lower()
    print(f"  Captured WARN: {warning_output.strip()[:200]!r}")
    print(f"  ✓ Soft-warning geactiveerd" if warning_ok else f"  ✗ Verwachte warning niet gezien")

    # === Cleanup test-rows ===
    print("\n" + "=" * 70)
    print(f"Cleanup test-rows (lead_id={TEST_LEAD_ID}) + claude_cache test-key")
    print("=" * 70)
    del_rows = db.table("api_cost_log").delete().eq("lead_id", TEST_LEAD_ID).execute()
    print(f"  api_cost_log deleted: {len(del_rows.data or [])} rows")
    # Cleanup eventuele cache row van b1
    del_cache = db.table("claude_cache").delete().like("context", "personalization").lt("created_at", "9999-12-31").gte("created_at", "2026-05-12").execute()
    print(f"  claude_cache test-rows deleted (best-effort): {len(del_cache.data or [])}")

    # Final verdict
    print()
    all_ok = b1_ok and b2_ok and b3_ok and warning_ok
    print("=" * 70)
    print(f"FINAL: {'✓ ALL 4 CHECKS PASS' if all_ok else '✗ SOME CHECKS FAILED'}")
    print("=" * 70)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(run_tests()))
