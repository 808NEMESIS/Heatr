"""
tests/test_e2e_pipeline.py — End-to-end pipeline test with real data.

Tests the full flow: discover → qualify → enrich → analyze → score
for one sector + city combination.

Usage:
    python tests/test_e2e_pipeline.py
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
# Heatr shares Supabase credentials with Warmr
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
if not os.getenv("SUPABASE_URL"):
    load_dotenv("/Users/nemesis/warmr/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("e2e_test")


async def test_pipeline(sector: str, city: str, max_leads: int = 5):
    """
    Run a mini end-to-end pipeline test.

    Instead of running the full Google Maps scraper (which needs Playwright
    and is slow), we simulate raw company data and run the rest of the
    pipeline for real.
    """
    from config.database import get_heatr_supabase

    sb = get_heatr_supabase()
    workspace_id = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")

    print(f"\n{'='*60}")
    print(f"E2E TEST: {sector} in {city}")
    print(f"{'='*60}\n")

    # ── Step 1: Check sector config loads ────────────────────────────────
    print("Step 1 — Sector config...")
    from config.sectors import get_sector
    config = get_sector(sector)
    print(f"  Loaded: {config['name']}")
    print(f"  Queries: {config['search_queries'][:3]}")
    print(f"  SBI codes: {config['kvk_sbi_codes']}")
    print()

    # ── Step 2: Simulate raw companies (skip actual Google Maps scrape) ──
    print("Step 2 — Simulating raw company data...")
    test_companies = _get_test_companies(sector, city)

    inserted = 0
    for raw in test_companies[:max_leads]:
        raw["workspace_id"] = workspace_id
        raw["sector"] = sector
        try:
            # Check if already exists
            existing = sb.table("companies_raw").select("id").eq(
                "workspace_id", workspace_id,
            ).eq("domain", raw["domain"]).limit(1).execute()
            if existing.data:
                print(f"  Skipped (exists): {raw['company_name']}")
                inserted += 1
                continue
            sb.table("companies_raw").insert(raw).execute()
            inserted += 1
            print(f"  Inserted: {raw['company_name']} ({raw['domain']})")
        except Exception as e:
            print(f"  Failed: {raw['company_name']} — {e}")
    print(f"  Total inserted: {inserted}\n")

    # ── Step 3: Qualify raw companies into leads ─────────────────────────
    print("Step 3 — Qualifying raw companies...")
    from enrichment.lead_qualifier import qualify_and_create_lead

    qualified = 0
    disqualified = 0
    lead_ids: list[str] = []

    res = sb.table("companies_raw").select("*").eq(
        "workspace_id", workspace_id,
    ).is_("qualification_status", "null").execute()

    for raw in (res.data or []):
        lead = await qualify_and_create_lead(raw, sector, workspace_id, sb)
        if lead:
            qualified += 1
            lead_ids.append(lead["id"])
            print(f"  ✓ Qualified: {raw['company_name']} (priority={lead.get('priority', '?')})")
        else:
            disqualified += 1
            print(f"  ✗ Disqualified: {raw['company_name']}")

    print(f"  Qualified: {qualified}, Disqualified: {disqualified}\n")

    if not lead_ids:
        print("No qualified leads — stopping test.")
        return

    # ── Step 4: Website pre-screening ────────────────────────────────────
    print("Step 4 — Website pre-screening...")
    from enrichment.website_prescreener import is_real_website

    real_websites = 0
    for lid in lead_ids[:3]:  # Test first 3 only (rate limiting)
        lead_res = sb.table("leads").select("domain, company_name").eq("id", lid).maybe_single().execute()
        if not lead_res.data:
            continue
        domain = lead_res.data.get("domain", "")
        name = lead_res.data.get("company_name", "")
        is_real, reason = await is_real_website(domain)
        status = "✓ real" if is_real else f"✗ {reason}"
        print(f"  {name} ({domain}): {status}")
        if is_real:
            real_websites += 1
    print(f"  Real websites: {real_websites}/{min(len(lead_ids), 3)}\n")

    # ── Step 5: Technical check (Layer 1 of website intelligence) ────────
    print("Step 5 — Technical website check (1 lead)...")
    if lead_ids:
        from website_intelligence.technical_checker import check_technical
        lead_res = sb.table("leads").select("domain").eq("id", lead_ids[0]).maybe_single().execute()
        domain = lead_res.data.get("domain", "") if lead_res.data else ""
        if domain:
            tech = await check_technical(domain)
            print(f"  Domain: {domain}")
            print(f"  SSL: {tech['has_ssl']}")
            print(f"  CMS: {tech['cms']}")
            print(f"  Sitemap: {tech['has_sitemap']}")
            print(f"  Schema: {tech['has_schema_markup']}")
            print(f"  Technical score: {tech['technical_score']}/25")
    print()

    # ── Step 6: Conversion check (Layer 3) ───────────────────────────────
    print("Step 6 — Conversion check (1 lead)...")
    if lead_ids:
        from website_intelligence.conversion_checker import check_conversion
        import httpx
        lead_res = sb.table("leads").select("domain").eq("id", lead_ids[0]).maybe_single().execute()
        domain = lead_res.data.get("domain", "") if lead_res.data else ""
        if domain:
            try:
                async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                    r = await client.get(f"https://{domain}")
                    html = r.text
                conv = await check_conversion(domain, html, sector)
                print(f"  CTA above fold: {conv['has_cta_above_fold']}")
                print(f"  Phone clickable: {conv['has_phone_clickable']}")
                print(f"  WhatsApp: {conv['has_whatsapp']}")
                print(f"  Booking: {conv['has_online_booking']} ({conv['booking_platform']})")
                print(f"  Chatbot: {conv['has_chatbot']} ({conv['chatbot_platform']})")
                print(f"  Contact form: {conv['has_contact_form']} ({conv['form_field_count']} fields)")
                print(f"  Conversion score: {conv['conversion_score']}/30")
            except Exception as e:
                print(f"  Failed: {e}")
    print()

    # ── Step 7: Data verification ────────────────────────────────────────
    print("Step 7 — Data verification (1 lead)...")
    if lead_ids:
        from enrichment.data_verification import verify_lead_data
        result = await verify_lead_data(lead_ids[0], workspace_id, sb)
        d = result.to_dict()
        print(f"  Company match:  {d['confidence_scores']['company_match']}")
        print(f"  Website match:  {d['confidence_scores']['website_match']}")
        print(f"  Email conf:     {d['confidence_scores']['email_confidence']}")
        print(f"  Contact conf:   {d['confidence_scores']['contact_match']}")
        print(f"  Quality score:  {d['data_quality_score']}")
        print(f"  Flags:          {d['inconsistency_flags']}")
    print()

    # ── Step 8: Sector-specific checks ───────────────────────────────────
    print("Step 8 — Sector-specific checks (1 lead)...")
    if lead_ids:
        from website_intelligence.sector_checker import check_sector_specific
        lead_res = sb.table("leads").select("domain").eq("id", lead_ids[0]).maybe_single().execute()
        domain = lead_res.data.get("domain", "") if lead_res.data else ""
        if domain:
            try:
                async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                    r = await client.get(f"https://{domain}")
                    html = r.text
                sector_result = await check_sector_specific(domain, html, sector)
                print(f"  Sector score: {sector_result['sector_score']}/15+")
                for check in sector_result.get("checks", []):
                    mark = "✓" if check["passed"] else "✗"
                    print(f"    {mark} {check['label']} ({check['points']} pts)")
            except Exception as e:
                print(f"  Failed: {e}")
    print()

    # ── Summary ──────────────────────────────────────────────────────────
    print(f"{'='*60}")
    print("PIPELINE TEST SUMMARY")
    print(f"{'='*60}")
    print(f"  Sector:        {sector}")
    print(f"  City:          {city}")
    print(f"  Raw companies: {inserted}")
    print(f"  Qualified:     {qualified}")
    print(f"  Disqualified:  {disqualified}")
    print(f"  Real websites: {real_websites}/{min(len(lead_ids), 3)}")
    print(f"  Lead IDs:      {lead_ids[:3]}")
    print()


def _get_test_companies(sector: str, city: str) -> list[dict]:
    """Return realistic test company data per sector."""
    if sector == "makelaars":
        return [
            {"company_name": "Broersma Makelaardij", "domain": "broersma.nl", "city": city, "phone": "020-6237272", "google_rating": 4.8, "google_review_count": 95, "google_category": "Makelaar", "source": "test"},
            {"company_name": "Makelaardij Van der Linden", "domain": "mvdlinden.nl", "city": city, "phone": "020-4704070", "google_rating": 4.5, "google_review_count": 42, "google_category": "Makelaarskantoor", "source": "test"},
            {"company_name": "Eefje Voogd Makelaardij", "domain": "eefjevoogd.nl", "city": city, "phone": "020-3050560", "google_rating": 4.9, "google_review_count": 180, "google_category": "NVM Makelaar", "source": "test"},
            {"company_name": "Gesloten Kantoor BV", "domain": "geslotenkantoor.nl", "city": city, "phone": "", "google_rating": 2.1, "google_review_count": 3, "google_category": "Makelaar", "source": "test", "business_status": "permanently_closed"},
            {"company_name": "Sociale Woningcorporatie West", "domain": "swwest.nl", "city": city, "phone": "020-9999999", "google_rating": 3.0, "google_review_count": 10, "google_category": "Woningcorporatie", "source": "test"},
        ]
    elif sector == "behandelaren":
        return [
            {"company_name": "Praktijk Samin", "domain": "praktijksamin.nl", "city": city, "phone": "030-2100200", "google_rating": 5.0, "google_review_count": 22, "google_category": "Coach", "source": "test"},
            {"company_name": "De Lichtkracht", "domain": "delichtkracht.nl", "city": city, "phone": "030-2222222", "google_rating": 4.8, "google_review_count": 67, "google_category": "Therapeut", "source": "test"},
            {"company_name": "GGZ Instelling Centraal", "domain": "ggzcentraal.nl", "city": city, "phone": "030-3333333", "google_rating": 3.5, "google_review_count": 200, "google_category": "GGZ instelling", "source": "test"},
        ]
    elif sector == "bouwbedrijven":
        return [
            {"company_name": "Bouwbedrijf Putman", "domain": "bouwbedrijfputman.nl", "city": city, "phone": "010-4114411", "google_rating": 4.6, "google_review_count": 55, "google_category": "Aannemer", "source": "test"},
            {"company_name": "Van Wijnen", "domain": "vanwijnen.nl", "city": city, "phone": "010-5555555", "google_rating": 4.4, "google_review_count": 30, "google_category": "Bouwbedrijf", "source": "test"},
            {"company_name": "Infra Waterstaat BV", "domain": "infrawaterstaat.nl", "city": city, "phone": "010-6666666", "google_rating": 4.0, "google_review_count": 5, "google_category": "Grond- weg- en waterbouw", "source": "test"},
        ]
    return []


if __name__ == "__main__":
    sector = sys.argv[1] if len(sys.argv) > 1 else "makelaars"
    city = sys.argv[2] if len(sys.argv) > 2 else "Amsterdam"
    asyncio.run(test_pipeline(sector, city))
