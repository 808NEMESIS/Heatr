"""
tests/run_optimized_enrichment.py — Enrichment with all cost optimizations.

Uses:
  - Skip gate (no Claude for low-score leads)
  - Batched Claude call (personalization + openers in 1 API call)
  - Prompt caching (system prompt cached, 90% discount on reuse)

Measures actual cost from api_cost_log to validate savings.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
if not os.getenv("SUPABASE_URL"):
    load_dotenv("/Users/nemesis/warmr/.env")

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")

LIMIT = 100
for i, arg in enumerate(sys.argv):
    if arg == "--limit" and i + 1 < len(sys.argv):
        LIMIT = int(sys.argv[i + 1])


async def run():
    import importlib, config.database
    importlib.reload(config.database)
    from config.database import get_heatr_supabase
    import anthropic

    sb = get_heatr_supabase()
    ac = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    workspace_id = "aerys"

    # Baseline: measure cost before
    before_res = sb.table("api_cost_log").select("cost_eur").execute()
    cost_before = sum(float(r.get("cost_eur") or 0) for r in (before_res.data or []))
    calls_before = len(before_res.data or [])

    print(f"\n{'='*70}")
    print(f"OPTIMIZED ENRICHMENT — measuring cost reduction")
    print(f"{'='*70}")
    print(f"BEFORE: {calls_before} API calls, total €{cost_before:.6f}\n")

    # Load leads
    res = sb.table("leads").select(
        "id, company_name, domain, sector, city, google_maps_url, google_rating, google_review_count"
    ).eq("workspace_id", workspace_id).order("google_review_count", desc=True).limit(LIMIT).execute()
    leads = res.data or []

    stats = {
        "total": len(leads),
        "skipped_broken": 0,
        "skipped_low_score": 0,
        "claude_calls": 0,
        "full_enrichment": 0,
    }

    for i, lead in enumerate(leads, 1):
        lead_id = lead["id"]
        name = lead["company_name"]
        domain = lead.get("domain") or ""
        sector = lead.get("sector") or ""
        city = lead.get("city") or ""

        if not domain:
            continue

        print(f"[{i}/{len(leads)}] {name[:50]:<50} ", end="")

        # ── Cheap stages (no Claude, no cost) ────────────────────────
        from enrichment.website_prescreener import is_real_website
        is_real, _ = await is_real_website(domain)
        if not is_real:
            print("✗ not real website")
            continue

        import httpx
        page_html = ""
        try:
            async with httpx.AsyncClient(timeout=10, follow_redirects=True,
                                          headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"}) as client:
                r = await client.get(f"https://{domain}")
                if r.status_code == 200:
                    page_html = r.text
        except Exception:
            pass

        if not page_html:
            print("✗ could not fetch")
            continue

        from website_intelligence.technical_checker import check_technical
        from website_intelligence.conversion_checker import check_conversion
        from website_intelligence.sector_checker import check_sector_specific

        tech = await check_technical(domain)
        conv = await check_conversion(domain, page_html, sector)
        sec = await check_sector_specific(domain, page_html, sector, conv, tech)

        total = tech["technical_score"] + conv["conversion_score"] + sec["sector_score"]

        # Store WI
        try:
            from datetime import datetime, timezone
            sb.table("website_intelligence").upsert({
                "lead_id": lead_id, "workspace_id": workspace_id, "domain": domain,
                "total_score": min(total, 100),
                "technical_score": tech["technical_score"],
                "conversion_score": conv["conversion_score"],
                "sector_score": sec["sector_score"],
                "technical_details": tech, "conversion_details": conv, "sector_details": sec,
                "analyzed_at": datetime.now(timezone.utc).isoformat(),
            }, on_conflict="lead_id").execute()
            sb.table("leads").update({"website_score": min(total, 100)}).eq("id", lead_id).execute()
        except Exception:
            pass

        # ── Gate decision ────────────────────────────────────────────
        from enrichment.enrichment_gate import decide_enrichment
        decision = decide_enrichment(
            lead, tech["technical_score"], conv["conversion_score"], sec["sector_score"],
        )

        if decision.skips_claude():
            if "broken" in decision.reason:
                stats["skipped_broken"] += 1
                print(f"✗ SKIP {decision.reason} (score={total})")
            else:
                stats["skipped_low_score"] += 1
                print(f"~ SKIP {decision.reason}")
            continue

        # ── Competitor benchmark (no Claude cost) ────────────────────
        from website_intelligence.competitor_analyzer import benchmark_lead
        await benchmark_lead(
            lead_id=lead_id, domain=domain, sector=sector, city=city,
            lead_total_score=total, workspace_id=workspace_id, supabase_client=sb,
        )

        # ── Reviews (Playwright + 1 Claude call, optional) ───────────
        if decision.run_reviews:
            from enrichment.review_analyzer import enrich_lead_with_reviews
            await enrich_lead_with_reviews(lead_id, workspace_id, sb, ac)

        # ── BATCHED Claude call: personalization + openers ───────────
        if decision.run_personalization or decision.run_openers:
            from enrichment.batched_enrichment import batched_enrich
            batch_result = await batched_enrich(lead_id, workspace_id, sb, ac)
            stats["claude_calls"] += 1
            stats["full_enrichment"] += 1
            opener_count = len(batch_result.get("openers", []))
            print(f"✓ score={total} claude=1 openers={opener_count}")
        else:
            print(f"~ score={total} (partial enrichment)")

    # ── Measure actual cost after ────────────────────────────────────
    after_res = sb.table("api_cost_log").select("cost_eur, context, prompt_tokens, response_tokens").execute()
    rows_after = after_res.data or []
    cost_after = sum(float(r.get("cost_eur") or 0) for r in rows_after)
    calls_after = len(rows_after)

    new_cost = cost_after - cost_before
    new_calls = calls_after - calls_before

    # Breakdown by context (batch vs individual)
    batch_rows = [r for r in rows_after if "batched" in (r.get("context") or "")]
    batch_cost = sum(float(r.get("cost_eur") or 0) for r in batch_rows)

    print(f"\n{'='*70}")
    print("COST MEASUREMENT (actual data from api_cost_log)")
    print(f"{'='*70}")
    print(f"Leads total:              {stats['total']}")
    print(f"  Skipped (broken):       {stats['skipped_broken']}")
    print(f"  Skipped (low score):    {stats['skipped_low_score']}")
    print(f"  Full enrichment:        {stats['full_enrichment']}")
    print()
    print(f"Claude calls made:        {new_calls}")
    print(f"Total cost this run:      €{new_cost:.6f}")
    print(f"  — of which batched:     €{batch_cost:.6f} ({len(batch_rows)} calls)")
    print()
    if stats["full_enrichment"] > 0:
        per_full_lead = new_cost / stats["full_enrichment"]
        print(f"Cost per enriched lead:   €{per_full_lead:.6f}")
    per_total_lead = new_cost / max(stats["total"], 1)
    print(f"Cost per lead (all):      €{per_total_lead:.6f}")

    # Compare to previous baseline
    previous_claim = 0.0008  # What we were spending before optimization
    savings_pct = ((previous_claim - per_total_lead) / previous_claim * 100) if per_total_lead < previous_claim else 0
    print(f"\nPrevious baseline:        €0.000800/lead")
    print(f"Savings:                  {savings_pct:.0f}%")


if __name__ == "__main__":
    asyncio.run(run())
