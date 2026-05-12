"""Verify accumulator-fix: per-lead cost-cap wordt nu daadwerkelijk gehandhaafd.

Run pas NA de accumulator-binding fix is doorgevoerd in:
  - personalization_extractor, contact_extractor, review_analyzer,
    sector_checker, reply_classifier, batched_enrichment
  - job_queue/enrichment_queue.run_enrichment_for_lead (orchestrator)

Tests (flag-check pattern — geen exceptions, want charge() raise't niet):
  1. charge() onder cap → accumulator.blocked == False
  2. charge() over cap → accumulator.blocked == True + reason gezet
  3. Echte fase-call (extract_personalization) met accumulator → spent_eur > 0
  4. Soft-warning bij accumulator=None → WARN gelogd met "accumulator" + "cap not enforced"

Output: per check verdict. Geen DB-cleanup nodig (alleen 1 api_cost_log-row
van test 3, gemarkeerd met test_lead_id).
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
TEST_WORKSPACE = "aerys"

# Capture WARNINGs voor soft-warning test
log_stream = io.StringIO()
log_handler = logging.StreamHandler(log_stream)
log_handler.setLevel(logging.WARNING)
for name in (
    "website_intelligence.personalization_extractor",
    "website_intelligence.contact_extractor",
    "website_intelligence.sector_checker",
    "enrichment.review_analyzer",
    "enrichment.batched_enrichment",
    "integrations.reply_classifier",
):
    lg = logging.getLogger(name)
    lg.addHandler(log_handler)
    lg.setLevel(logging.WARNING)


async def run_tests() -> int:
    import os
    import anthropic
    from config.database import get_heatr_supabase
    from utils.cost_guard import LeadCostAccumulator
    from website_intelligence.personalization_extractor import extract_personalization

    db = get_heatr_supabase()
    anth_client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    print(f"Test lead_id: {TEST_LEAD_ID}\n")

    # ====================================================================
    # Test 1: charge() onder cap → blocked = False
    # ====================================================================
    print("=" * 70)
    print("Test 1: charge() onder cap → blocked = False")
    print("=" * 70)
    acc1 = LeadCostAccumulator(lead_id=TEST_LEAD_ID, workspace_id=TEST_WORKSPACE)
    acc1.charge(0.001, "fake_context_1")
    acc1.charge(0.002, "fake_context_2")
    test1_ok = (not acc1.blocked) and abs(acc1.spent_eur - 0.003) < 1e-9
    print(f"  spent_eur={acc1.spent_eur:.6f} ceiling={acc1.ceiling_eur:.6f} blocked={acc1.blocked}")
    print(f"  {'✓' if test1_ok else '✗'} Test 1")

    # ====================================================================
    # Test 2: charge() over cap → blocked = True (flag-check, geen exception)
    # ====================================================================
    print("\n" + "=" * 70)
    print("Test 2: charge() over cap → blocked = True")
    print("=" * 70)
    acc2 = LeadCostAccumulator(lead_id=TEST_LEAD_ID, workspace_id=TEST_WORKSPACE)
    # Forceer cap = €0.05 (default). Charge tot we overheen gaan.
    # Eerste charge: net onder cap
    acc2.charge(0.04, "fake_step_1")
    block_before = acc2.blocked
    # Tweede charge: zet ons over de cap
    acc2.charge(0.02, "fake_step_2")
    block_after = acc2.blocked
    test2_ok = (not block_before) and block_after and acc2.blocked_reason
    print(f"  na 1e charge: spent={acc2.spent_eur - 0.02:.6f} blocked={block_before}")
    print(f"  na 2e charge: spent={acc2.spent_eur:.6f} blocked={block_after}")
    print(f"  reason={acc2.blocked_reason}")
    print(f"  {'✓' if test2_ok else '✗'} Test 2 (flag-check: geen exception, .blocked=True)")

    # ====================================================================
    # Test 3: Echte fase-call met accumulator → spent_eur > 0 + charge zichtbaar
    # ====================================================================
    print("\n" + "=" * 70)
    print("Test 3: extract_personalization(..., accumulator=acc) → spent_eur > 0")
    print("=" * 70)
    acc3 = LeadCostAccumulator(lead_id=TEST_LEAD_ID, workspace_id=TEST_WORKSPACE)
    fake_html = (
        "<html><body><h1>Verify-test kliniek</h1>"
        "<p>Cosmetische behandelingen sinds 2010 — Botox, filler, lasertherapie.</p>"
        "<p>Ervaren team, persoonlijke aanpak, GZ-arts beschikbaar.</p>"
        "<p>Boek online via onze afsprakentool of bel ons direct.</p>"
        "</body></html>"
    )
    try:
        result = await extract_personalization(
            domain=f"verify-acc-{uuid.uuid4().hex[:6]}.example",
            page_html=fake_html,
            sector="cosmetische_behandelaars",
            anthropic_client=anth_client,
            supabase_client=db,
            lead_id=TEST_LEAD_ID,
            accumulator=acc3,
        )
        spent_after = acc3.spent_eur
        # Een echte call charged minstens _ESTIMATED_COST_EUR (=0.0005)
        test3_ok = spent_after > 0 and not acc3.blocked
        print(f"  positioning={(result.get('positioning') or '')[:60]!r}")
        print(f"  spent_eur={spent_after:.6f} blocked={acc3.blocked}")
        print(f"  {'✓' if test3_ok else '✗'} Test 3 (accumulator gecharged door echte phase-call)")
    except Exception as e:
        print(f"  ✗ Test 3 raised: {e!r}")
        test3_ok = False

    # ====================================================================
    # Test 4: Soft-warning bij accumulator=None → WARN gelogd
    # ====================================================================
    print("\n" + "=" * 70)
    print("Test 4: extract_personalization(accumulator=None) → soft-warning")
    print("=" * 70)
    log_stream.seek(0)
    log_stream.truncate(0)
    try:
        await extract_personalization(
            domain=f"verify-acc-none-{uuid.uuid4().hex[:6]}.example",
            page_html=fake_html,
            sector="cosmetische_behandelaars",
            anthropic_client=anth_client,
            supabase_client=db,
            lead_id=TEST_LEAD_ID,
            accumulator=None,
        )
    except Exception as e:
        print(f"  ⚠ extract_personalization raised (niet-fataal voor warn-test): {e!r}")

    warn_text = log_stream.getvalue().lower()
    test4_ok = "accumulator" in warn_text and "cap not enforced" in warn_text
    print(f"  Captured WARN: {log_stream.getvalue().strip()[:200]!r}")
    print(f"  {'✓' if test4_ok else '✗'} Test 4 (soft-warning fired)")

    # ====================================================================
    # Cleanup test-rows (alleen test 3 schrijft naar api_cost_log)
    # ====================================================================
    print("\n" + "=" * 70)
    print(f"Cleanup test-rows (lead_id={TEST_LEAD_ID})")
    print("=" * 70)
    try:
        deleted = db.table("api_cost_log").delete().eq("lead_id", TEST_LEAD_ID).execute()
        print(f"  api_cost_log deleted: {len(deleted.data or [])} rows")
    except Exception as e:
        print(f"  cleanup failed (non-fatal): {e!r}")

    # ====================================================================
    # Final verdict
    # ====================================================================
    all_ok = test1_ok and test2_ok and test3_ok and test4_ok
    print()
    print("=" * 70)
    print(f"FINAL: {'✓ ALL 4 CHECKS PASS' if all_ok else '✗ SOME CHECKS FAILED'}")
    print(f"  Test 1 (under cap):     {'✓' if test1_ok else '✗'}")
    print(f"  Test 2 (over cap flag): {'✓' if test2_ok else '✗'}")
    print(f"  Test 3 (real charge):   {'✓' if test3_ok else '✗'}")
    print(f"  Test 4 (soft-warning):  {'✓' if test4_ok else '✗'}")
    print("=" * 70)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(run_tests()))
