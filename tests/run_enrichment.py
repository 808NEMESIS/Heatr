"""
tests/run_enrichment.py — Run enrichment pipeline for all pending leads.

Executes the enrichment steps that don't require Warmr:
  website → email_waterfall → website_intelligence → contact_discovery →
  data_verification → scoring

Skips: kvk (needs API key), inbox_selection (no push yet), company_enrichment (needs Claude)

Usage:
    python tests/run_enrichment.py
    python tests/run_enrichment.py --with-claude   # Include Claude-powered steps
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
logger = logging.getLogger("enrichment_runner")

WITH_CLAUDE = "--with-claude" in sys.argv


async def run():
    from config.database import get_heatr_supabase
    sb = get_heatr_supabase()
    workspace_id = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")

    # Load all leads
    res = sb.table("leads").select("id, company_name, domain, sector, status").eq(
        "workspace_id", workspace_id,
    ).execute()
    leads = res.data or []

    print(f"\n{'='*60}")
    print(f"ENRICHMENT PIPELINE — {len(leads)} leads")
    print(f"Claude enabled: {WITH_CLAUDE}")
    print(f"{'='*60}\n")

    for i, lead in enumerate(leads, 1):
        lead_id = lead["id"]
        name = lead["company_name"]
        domain = lead.get("domain") or ""
        sector = lead.get("sector") or ""

        print(f"\n[{i}/{len(leads)}] {name} ({domain})")
        print(f"{'─'*50}")

        # ── Step 1: Website pre-screening ────────────────────────────
        if domain:
            from enrichment.website_prescreener import is_real_website
            is_real, reason = await is_real_website(domain)
            print(f"  Pre-screen:   {'✓ real' if is_real else f'✗ {reason}'}")
            if not is_real:
                print(f"  → Skipping website analysis")
                sb.table("leads").update({"status": "prescreened_fail"}).eq("id", lead_id).execute()
                continue
        else:
            print(f"  Pre-screen:   ✗ no domain")
            continue

        # ── Step 2: Technical check ──────────────────────────────────
        from website_intelligence.technical_checker import check_technical
        tech = await check_technical(domain)
        print(f"  Technical:    {tech['technical_score']}/25 (SSL:{tech['has_ssl']} CMS:{tech['cms']} Sitemap:{tech['has_sitemap']})")

        # ── Step 3: Conversion check ─────────────────────────────────
        import httpx
        page_html = ""
        try:
            async with httpx.AsyncClient(timeout=12, follow_redirects=True,
                                          headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"}) as client:
                r = await client.get(f"https://{domain}")
                if r.status_code == 200:
                    page_html = r.text
        except Exception:
            try:
                async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                    r = await client.get(f"http://{domain}")
                    page_html = r.text
            except Exception:
                pass

        if page_html:
            from website_intelligence.conversion_checker import check_conversion
            conv = await check_conversion(domain, page_html, sector)
            print(f"  Conversion:   {conv['conversion_score']}/30 (CTA:{conv['has_cta_above_fold']} Tel:{conv['has_phone_clickable']} WA:{conv['has_whatsapp']} Book:{conv['has_online_booking']} Chat:{conv['has_chatbot']})")

            # ── Step 4: Sector-specific ──────────────────────────────
            from website_intelligence.sector_checker import check_sector_specific
            sec = await check_sector_specific(domain, page_html, sector, conv, tech)
            print(f"  Sector:       {sec['sector_score']}/15+")
            for check in sec.get("checks", []):
                if check["passed"]:
                    print(f"    ✓ {check['label']} ({check['points']} pts)")

            # ── Step 5: Store website intelligence ───────────────────
            total_score = tech["technical_score"] + conv["conversion_score"] + sec["sector_score"]
            try:
                sb.table("website_intelligence").upsert({
                    "lead_id": lead_id,
                    "workspace_id": workspace_id,
                    "domain": domain,
                    "total_score": min(total_score, 100),
                    "technical_score": tech["technical_score"],
                    "conversion_score": conv["conversion_score"],
                    "sector_score": sec["sector_score"],
                    "technical_details": tech,
                    "conversion_details": conv,
                    "sector_details": sec,
                    "opportunity_types": [],
                    "analyzed_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                }, on_conflict="lead_id").execute()
            except Exception as e:
                logger.debug(f"  WI store failed: {e}")

            # Update lead
            sb.table("leads").update({
                "website_score": min(total_score, 100),
            }).eq("id", lead_id).execute()
            print(f"  TOTAL SCORE:  {min(total_score, 100)}/100")

            # ── Step 6: Personalization extraction (Claude) ──────────
            if WITH_CLAUDE and page_html:
                try:
                    import anthropic
                    ac = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
                    from website_intelligence.personalization_extractor import extract_personalization
                    pers = await extract_personalization(domain, page_html, sector, ac, sb)
                    hooks = pers.get("hooks") or []
                    print(f"  Personalize:  {len(hooks)} hooks — {hooks[:2]}")
                    sb.table("leads").update({
                        "personalization_hooks": hooks,
                        "personalization_observations": pers.get("observations") or [],
                        "company_positioning": pers.get("positioning") or "",
                    }).eq("id", lead_id).execute()
                except Exception as e:
                    print(f"  Personalize:  FAIL — {e}")

            # ── Step 7: Contact extraction (Claude) ──────────────────
            if WITH_CLAUDE:
                try:
                    from website_intelligence.contact_extractor import extract_contacts_from_website
                    contacts = await extract_contacts_from_website(domain, sb, ac)
                    if contacts:
                        print(f"  Contacts:     {len(contacts)} found")
                        for c in contacts:
                            print(f"    → {c.get('full_name')} — {c.get('title')} (conf: {c.get('confidence')})")
                except Exception as e:
                    print(f"  Contacts:     FAIL — {e}")
        else:
            print(f"  HTML:         ✗ could not fetch")

        # ── Step 8: Data verification ────────────────────────────────
        from enrichment.data_verification import verify_lead_data
        vr = await verify_lead_data(lead_id, workspace_id, sb)
        d = vr.to_dict()
        print(f"  Verification: quality={d['data_quality_score']:.2f} company={d['confidence_scores']['company_match']:.2f} website={d['confidence_scores']['website_match']:.2f}")
        if d["inconsistency_flags"]:
            print(f"    Flags: {d['inconsistency_flags']}")

        # ── Step 9: Scoring ──────────────────────────────────────────
        from scoring.lead_scoring import score_lead
        score_result = await score_lead(lead_id, workspace_id, sb)
        print(f"  LEAD SCORE:   {score_result['score']}/100 (fit:{score_result['fit_score']} dq:{score_result['data_quality_score_num']:.0f} reach:{score_result['reachability_score']} pers:{score_result['personalization_potential']})")
        print(f"  Push eligible: {'✓' if score_result['push_eligible'] else '✗ ' + ', '.join(score_result['push_block_reasons'])}")

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("ENRICHMENT COMPLETE")
    print(f"{'='*60}")

    final = sb.table("leads").select(
        "company_name, domain, sector, score, website_score, fit_score, reachability_score, personalization_potential, data_quality_score, status"
    ).eq("workspace_id", workspace_id).order("score", desc=True).execute()

    print(f"\n{'Company':<40} {'Score':>5} {'Web':>4} {'Fit':>4} {'Reach':>5} {'Pers':>5} {'DQ':>5}")
    print("─" * 75)
    for l in (final.data or []):
        print(f"{l['company_name'][:39]:<40} {l.get('score', 0):>5} {l.get('website_score') or 0:>4} {l.get('fit_score', 0):>4} {l.get('reachability_score', 0):>5} {l.get('personalization_potential', 0):>5} {float(l.get('data_quality_score') or 0):>5.2f}")


if __name__ == "__main__":
    asyncio.run(run())
