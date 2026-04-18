"""
tests/run_full_enrichment.py — Full enrichment with all features + validation.

Runs: website analysis → competitor benchmark → review analysis →
      opener generation → data verification → validation → scoring

Usage:
    python tests/run_full_enrichment.py              # All leads
    python tests/run_full_enrichment.py --limit 3    # First 3 leads only
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("full_enrichment")

LIMIT = 100
for i, arg in enumerate(sys.argv):
    if arg == "--limit" and i + 1 < len(sys.argv):
        LIMIT = int(sys.argv[i + 1])


async def run():
    import importlib
    import config.database
    importlib.reload(config.database)
    from config.database import get_heatr_supabase
    import anthropic

    sb = get_heatr_supabase()
    ac = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    workspace_id = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")

    # Load leads
    res = sb.table("leads").select(
        "id, company_name, domain, sector, city, google_maps_url, google_rating, google_review_count"
    ).eq("workspace_id", workspace_id).order("google_review_count", desc=True).limit(LIMIT).execute()
    leads = res.data or []

    print(f"\n{'='*70}")
    print(f"FULL ENRICHMENT — {len(leads)} leads")
    print(f"{'='*70}")

    results_summary: list[dict] = []

    for i, lead in enumerate(leads, 1):
        lead_id = lead["id"]
        name = lead["company_name"]
        domain = lead.get("domain") or ""
        sector = lead.get("sector") or ""
        city = lead.get("city") or ""

        print(f"\n[{i}/{len(leads)}] {name}")
        print(f"  {domain} | {sector} | {city} | ★{lead.get('google_rating', '?')} ({lead.get('google_review_count', 0)} reviews)")
        print(f"  {'─'*60}")

        lead_result: dict = {"name": name, "domain": domain}

        # ── 1. Website pre-screen ────────────────────────────────────
        if not domain:
            print(f"  ✗ No domain — skipping")
            continue

        from enrichment.website_prescreener import is_real_website
        is_real, reason = await is_real_website(domain)
        print(f"  Pre-screen:     {'✓' if is_real else '✗ ' + reason}")
        if not is_real:
            continue

        # ── 2. Website intelligence (tech + conv + sector) ───────────
        import httpx
        page_html = ""
        try:
            async with httpx.AsyncClient(timeout=12, follow_redirects=True,
                                          headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"}) as client:
                r = await client.get(f"https://{domain}")
                if r.status_code == 200:
                    page_html = r.text
        except Exception:
            pass

        if not page_html:
            print(f"  ✗ Could not fetch website")
            continue

        from website_intelligence.technical_checker import check_technical
        from website_intelligence.conversion_checker import check_conversion
        from website_intelligence.sector_checker import check_sector_specific

        tech = await check_technical(domain)
        conv = await check_conversion(domain, page_html, sector)
        sec = await check_sector_specific(domain, page_html, sector, conv, tech)

        total_web_score = tech["technical_score"] + conv["conversion_score"] + sec["sector_score"]
        print(f"  Website score:  {total_web_score}/100 (tech:{tech['technical_score']} conv:{conv['conversion_score']} sec:{sec['sector_score']})")

        # Store website intelligence
        try:
            sb.table("website_intelligence").upsert({
                "lead_id": lead_id,
                "workspace_id": workspace_id,
                "domain": domain,
                "total_score": min(total_web_score, 100),
                "technical_score": tech["technical_score"],
                "conversion_score": conv["conversion_score"],
                "sector_score": sec["sector_score"],
                "technical_details": tech,
                "conversion_details": conv,
                "sector_details": sec,
                "analyzed_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            }, on_conflict="lead_id").execute()
            sb.table("leads").update({"website_score": min(total_web_score, 100)}).eq("id", lead_id).execute()
        except Exception as e:
            logger.debug(f"WI store: {e}")

        # ── 3. Competitor benchmarking ───────────────────────────────
        from website_intelligence.competitor_analyzer import benchmark_lead
        comp = await benchmark_lead(
            lead_id=lead_id, domain=domain, sector=sector, city=city,
            lead_total_score=total_web_score, workspace_id=workspace_id,
            supabase_client=sb,
        )
        comp_names = [c.get("name", "?") for c in comp.get("competitors", [])]
        print(f"  Competitors:    {len(comp.get('competitors', []))} found — {comp_names}")
        print(f"  vs Market:      {comp.get('score_vs_market', 0):+d} (rank {comp.get('lead_rank', '?')}/{comp.get('total_analyzed', '?')})")
        lead_result["score_vs_market"] = comp.get("score_vs_market", 0)

        # ── 4. Review analysis ───────────────────────────────────────
        from enrichment.review_analyzer import enrich_lead_with_reviews
        review = await enrich_lead_with_reviews(
            lead_id=lead_id, workspace_id=workspace_id,
            supabase_client=sb, anthropic_client=ac,
        )
        reviews_found = review.get("reviews_found", 0)
        pains = review.get("aerys_relevant_pains") or []
        quote = (review.get("best_quote") or "")[:80]
        print(f"  Reviews:        {reviews_found} found, {len(pains)} pains, quote: '{quote}'")
        lead_result["reviews"] = reviews_found

        # ── 5. Personalization extraction ────────────────────────────
        from website_intelligence.personalization_extractor import extract_personalization
        pers = await extract_personalization(domain, page_html, sector, ac, sb)
        hooks = pers.get("hooks") or []
        print(f"  Personalize:    {len(hooks)} hooks")
        try:
            sb.table("leads").update({
                "personalization_hooks": hooks,
                "personalization_observations": pers.get("observations") or [],
                "company_positioning": pers.get("positioning") or "",
            }).eq("id", lead_id).execute()
        except Exception:
            pass

        # ── 6. Opener generation ─────────────────────────────────────
        from enrichment.opener_generator import generate_openers
        openers = await generate_openers(
            lead_id=lead_id, workspace_id=workspace_id,
            supabase_client=sb, anthropic_client=ac,
        )
        for o in openers[:2]:
            print(f"  Opener #{o['rank']}:     {o['opener'][:90]}...")
        lead_result["openers"] = len(openers)

        # ── 7. Data verification ─────────────────────────────────────
        from enrichment.data_verification import verify_lead_data
        vr = await verify_lead_data(lead_id, workspace_id, sb)
        print(f"  Data quality:   {vr.data_quality_score:.2f}")

        # ── 8. Enrichment validation (cross-check claims) ────────────
        from enrichment.enrichment_validator import validate_enrichment
        validation = await validate_enrichment(lead_id, workspace_id, sb)
        v_score = validation.get("validation_score", 0)
        verified = validation.get("verified_count", 0)
        inferred = validation.get("inferred_count", 0)
        wrong = validation.get("wrong_count", 0)
        print(f"  Validation:     score={v_score:.2f} (✓{verified} ~{inferred} ✗{wrong})")
        if validation.get("warnings"):
            for w in validation["warnings"]:
                print(f"    ⚠ {w}")
        lead_result["validation_score"] = v_score
        lead_result["wrong_claims"] = wrong

        # ── 9. Scoring ───────────────────────────────────────────────
        from scoring.lead_scoring import score_lead
        score = await score_lead(lead_id, workspace_id, sb)
        print(f"  LEAD SCORE:     {score['score']}/100 (fit:{score['fit_score']} dq:{score['data_quality_score_num']:.0f} reach:{score['reachability_score']} pers:{score['personalization_potential']})")
        print(f"  Push eligible:  {'✓' if score['push_eligible'] else '✗ ' + ', '.join(score['push_block_reasons'])}")
        lead_result["score"] = score["score"]
        lead_result["push_eligible"] = score["push_eligible"]

        results_summary.append(lead_result)

    # ── Final summary ────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("ENRICHMENT COMPLETE — SUMMARY")
    print(f"{'='*70}")
    print(f"\n{'Company':<35} {'Score':>5} {'Web':>4} {'vsMkt':>6} {'Rev':>4} {'Open':>4} {'Val':>5} {'Wrong':>5}")
    print("─" * 75)
    for r in sorted(results_summary, key=lambda x: x.get("score", 0), reverse=True):
        print(
            f"{r['name'][:34]:<35} "
            f"{r.get('score', 0):>5} "
            f"{'—':>4} "
            f"{r.get('score_vs_market', 0):>+6} "
            f"{r.get('reviews', 0):>4} "
            f"{r.get('openers', 0):>4} "
            f"{r.get('validation_score', 0):>5.2f} "
            f"{r.get('wrong_claims', 0):>5}"
        )

    total_wrong = sum(r.get("wrong_claims", 0) for r in results_summary)
    total_leads = len(results_summary)
    print(f"\nTotal leads enriched: {total_leads}")
    print(f"Total wrong claims:  {total_wrong}")
    print(f"Accuracy rate:       {((total_leads * 8 - total_wrong) / max(total_leads * 8, 1)):.0%}")


if __name__ == "__main__":
    asyncio.run(run())
