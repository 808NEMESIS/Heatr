"""
tests/test_google_maps_live.py — Live Google Maps scraper test.

Scrapes a small number of real businesses and runs them through
qualification. Uses the HeatrSupabaseWrapper for prefixed tables.

Usage:
    python tests/test_google_maps_live.py [sector] [city] [max_results]
    python tests/test_google_maps_live.py makelaars Amsterdam 5
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import re
from urllib.parse import quote_plus

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
if not os.getenv("SUPABASE_URL"):
    load_dotenv("/Users/nemesis/warmr/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("maps_test")


async def test_google_maps_scrape(sector: str, city: str, max_results: int = 5):
    """
    Lightweight Google Maps scraper test.

    Instead of using the full scraper (which depends on rate_limit_state table),
    we directly use Playwright to scrape Google Maps and then feed results
    through the qualification gate.
    """
    from config.database import get_heatr_supabase
    from config.sectors import get_sector

    sb = get_heatr_supabase()
    workspace_id = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")
    sector_config = get_sector(sector)

    query = sector_config["search_queries"][0].replace("{city}", city)
    search_url = f"https://www.google.com/maps/search/{quote_plus(query)}"

    print(f"\n{'='*60}")
    print(f"GOOGLE MAPS LIVE TEST: {query}")
    print(f"URL: {search_url}")
    print(f"Max results: {max_results}")
    print(f"{'='*60}\n")

    # --- Scrape Google Maps ---
    from playwright.async_api import async_playwright

    companies: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            locale="nl-NL",
            timezone_id="Europe/Amsterdam",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await context.new_page()

        print("Navigating to Google Maps...")
        await page.goto(search_url, wait_until="domcontentloaded", timeout=20_000)
        await asyncio.sleep(3)

        # Handle consent screen
        for sel in ['button:has-text("Alles accepteren")', 'button:has-text("Accept all")',
                     'button[aria-label*="Accept"]', 'form[action*="consent"] button']:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await asyncio.sleep(2)
                    break
            except Exception:
                continue

        # Wait for results
        feed_selectors = ['div[role="feed"]', 'div[role="main"]']
        for sel in feed_selectors:
            try:
                await page.wait_for_selector(sel, timeout=10_000)
                break
            except Exception:
                continue

        await asyncio.sleep(2)

        # Extract result cards
        result_links = await page.query_selector_all('a[href*="/maps/place/"]')
        print(f"Found {len(result_links)} result links")

        for i, link in enumerate(result_links[:max_results]):
            try:
                # Click the result to open details panel
                await link.click()
                await asyncio.sleep(2)

                # Extract data from the details panel
                company: dict = {"source": "google_maps", "city": city}

                # Name
                for sel in ['h1.DUwDvf', 'h1[class*="header"]', 'div.qBF1Pd', 'h1']:
                    el = await page.query_selector(sel)
                    if el:
                        company["company_name"] = (await el.inner_text()).strip()
                        break

                if not company.get("company_name"):
                    continue

                # Rating
                for sel in ['span.ceNzKf', 'span[role="img"][aria-label*="ster"]', 'span.MW4etd']:
                    el = await page.query_selector(sel)
                    if el:
                        text = await el.get_attribute("aria-label") or await el.inner_text()
                        rating_match = re.search(r"(\d[,.]?\d?)", text.replace(",", "."))
                        if rating_match:
                            company["google_rating"] = float(rating_match.group(1))
                        break

                # Review count
                for sel in ['span.UY7F9', 'button[aria-label*="review"]', 'span[aria-label*="review"]']:
                    el = await page.query_selector(sel)
                    if el:
                        text = await el.inner_text()
                        count_match = re.search(r"(\d[\d.]*)", text.replace(".", ""))
                        if count_match:
                            company["google_review_count"] = int(count_match.group(1))
                        break

                # Category
                for sel in ['button[jsaction*="category"]', 'span.DkEaL']:
                    el = await page.query_selector(sel)
                    if el:
                        company["google_category"] = (await el.inner_text()).strip()
                        break

                # Website
                website_el = await page.query_selector('a[data-item-id="authority"]')
                if website_el:
                    href = await website_el.get_attribute("href") or ""
                    if href:
                        from urllib.parse import urlparse
                        parsed = urlparse(href)
                        company["domain"] = parsed.netloc.replace("www.", "")

                # Phone
                phone_el = await page.query_selector('button[data-item-id*="phone"] div.Io6YTe')
                if phone_el:
                    company["phone"] = (await phone_el.inner_text()).strip()

                # Address
                addr_el = await page.query_selector('button[data-item-id="address"] div.Io6YTe')
                if addr_el:
                    company["address"] = (await addr_el.inner_text()).strip()

                if company.get("company_name"):
                    companies.append(company)
                    print(f"  {i+1}. {company.get('company_name')} | {company.get('domain', 'no website')} | ★{company.get('google_rating', '?')} ({company.get('google_review_count', 0)} reviews)")

                # Go back to results list
                await page.go_back()
                await asyncio.sleep(1.5)

            except Exception as e:
                logger.debug(f"Failed to extract result {i}: {e}")
                try:
                    await page.go_back()
                    await asyncio.sleep(1)
                except Exception:
                    pass

        await browser.close()

    print(f"\n--- Scraped {len(companies)} companies ---\n")

    if not companies:
        print("No companies found — Google may have shown a CAPTCHA or different layout.")
        return

    # --- Insert into heatr_companies_raw ---
    print("Inserting into heatr_companies_raw...")
    inserted = 0
    for c in companies:
        c["workspace_id"] = workspace_id
        c["sector"] = sector
        try:
            domain = c.get("domain", "")
            if domain:
                existing = sb.table("companies_raw").select("id").eq(
                    "workspace_id", workspace_id
                ).eq("domain", domain).limit(1).execute()
                if existing.data:
                    print(f"  Skipped (exists): {c['company_name']}")
                    continue
            sb.table("companies_raw").insert(c).execute()
            inserted += 1
        except Exception as e:
            print(f"  Failed: {c.get('company_name')} — {e}")
    print(f"  Inserted: {inserted}")

    # --- Qualify into leads ---
    print("\nQualifying into leads...")
    from enrichment.lead_qualifier import qualify_and_create_lead

    qualified = 0
    disqualified = 0

    raw_res = sb.table("companies_raw").select("*").eq(
        "workspace_id", workspace_id
    ).eq("sector", sector).is_("qualification_status", "null").execute()

    for raw in (raw_res.data or []):
        lead = await qualify_and_create_lead(raw, sector, workspace_id, sb)
        if lead:
            qualified += 1
            print(f"  ✓ {raw['company_name']} ({raw.get('domain', 'no domain')})")
        else:
            disqualified += 1
            print(f"  ✗ {raw['company_name']}")

    # --- Summary ---
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"  Scraped:      {len(companies)}")
    print(f"  Inserted:     {inserted}")
    print(f"  Qualified:    {qualified}")
    print(f"  Disqualified: {disqualified}")

    # Show what's in the DB
    leads_res = sb.table("leads").select(
        "company_name, domain, city, google_rating, google_review_count, status"
    ).eq("workspace_id", workspace_id).eq("sector", sector).execute()

    if leads_res.data:
        print(f"\n  Leads in database:")
        for l in leads_res.data:
            print(f"    {l['company_name']:<35} {l.get('domain', ''):<25} ★{l.get('google_rating', '?')} ({l.get('google_review_count', 0)} reviews)")
    print()


if __name__ == "__main__":
    sector = sys.argv[1] if len(sys.argv) > 1 else "makelaars"
    city = sys.argv[2] if len(sys.argv) > 2 else "Amsterdam"
    max_results = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    asyncio.run(test_google_maps_scrape(sector, city, max_results))
