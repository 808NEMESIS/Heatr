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
                # Get the place URL from the link before clicking
                place_href = await link.get_attribute("href") or ""

                # Click the result to open details panel
                await link.click()
                await asyncio.sleep(2.5)

                # Wait for detail panel to load — the name heading appears
                try:
                    await page.wait_for_selector('h1.DUwDvf, h1.fontHeadlineLarge', timeout=5000)
                except Exception:
                    pass

                company: dict = {"source": "google_maps", "city": city}

                # Store Google Maps URL
                current_url = page.url
                if "/maps/place/" in current_url:
                    company["google_maps_url"] = current_url
                elif "/maps/place/" in place_href:
                    company["google_maps_url"] = place_href

                # --- Extract from detail panel using page.evaluate for accuracy ---
                # This runs JS in the browser to extract data from the active panel,
                # avoiding stale selectors that match the wrong element.
                detail_data = await page.evaluate("""() => {
                    const result = {};

                    // Name — the h1 in the detail panel
                    const nameEl = document.querySelector('h1.DUwDvf') || document.querySelector('h1.fontHeadlineLarge');
                    result.name = nameEl ? nameEl.innerText.trim() : '';

                    // Rating — look for the star rating text near the name
                    const ratingEl = document.querySelector('div.F7nice span[aria-hidden="true"]')
                                  || document.querySelector('span.ceNzKf');
                    if (ratingEl) {
                        const ratingText = ratingEl.innerText.replace(',', '.');
                        const match = ratingText.match(/(\\d\\.?\\d?)/);
                        result.rating = match ? parseFloat(match[1]) : null;
                    }

                    // Review count — the "(123)" text next to rating
                    const reviewEl = document.querySelector('div.F7nice span[aria-label*="review"]')
                                  || document.querySelector('span.UY7F9');
                    if (reviewEl) {
                        const text = reviewEl.innerText.replace(/[^\\d]/g, '');
                        result.reviews = text ? parseInt(text) : 0;
                    } else {
                        // Fallback: look for aria-label with review count
                        const ratingBtn = document.querySelector('button[aria-label*="review"]');
                        if (ratingBtn) {
                            const match = ratingBtn.getAttribute('aria-label').match(/(\\d[\\d,.]*)/);
                            result.reviews = match ? parseInt(match[1].replace(/[.,]/g, '')) : 0;
                        }
                    }

                    // Category
                    const catEl = document.querySelector('button[jsaction*="category"]')
                               || document.querySelector('span.DkEaL');
                    result.category = catEl ? catEl.innerText.trim() : '';

                    // Website
                    const websiteEl = document.querySelector('a[data-item-id="authority"]');
                    result.website = websiteEl ? websiteEl.getAttribute('href') : '';

                    // Phone
                    const phoneEl = document.querySelector('button[data-item-id*="phone"] div.Io6YTe')
                                 || document.querySelector('button[data-tooltip*="telefoon"] div.Io6YTe')
                                 || document.querySelector('button[aria-label*="Telefoon"] div.Io6YTe');
                    result.phone = phoneEl ? phoneEl.innerText.trim() : '';

                    // Address
                    const addrEl = document.querySelector('button[data-item-id="address"] div.Io6YTe')
                                || document.querySelector('button[aria-label*="Adres"] div.Io6YTe');
                    result.address = addrEl ? addrEl.innerText.trim() : '';

                    return result;
                }""")

                # Map JS results to company dict
                company["company_name"] = detail_data.get("name") or ""
                if not company["company_name"]:
                    continue

                if detail_data.get("rating"):
                    company["google_rating"] = detail_data["rating"]
                if detail_data.get("reviews"):
                    company["google_review_count"] = detail_data["reviews"]
                if detail_data.get("category"):
                    company["google_category"] = detail_data["category"]
                if detail_data.get("phone"):
                    company["phone"] = detail_data["phone"]
                if detail_data.get("address"):
                    company["address"] = detail_data["address"]

                # Parse domain from website URL
                website_url = detail_data.get("website") or ""
                if website_url:
                    from urllib.parse import urlparse
                    parsed = urlparse(website_url)
                    if parsed.netloc:
                        company["domain"] = parsed.netloc.replace("www.", "")

                companies.append(company)
                print(f"  {i+1}. {company.get('company_name')}")
                print(f"     Domain: {company.get('domain', '—')} | ★{company.get('google_rating', '?')} ({company.get('google_review_count', 0)} reviews)")
                print(f"     Cat: {company.get('google_category', '—')} | Tel: {company.get('phone', '—')}")
                print(f"     Adres: {company.get('address', '—')}")

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
