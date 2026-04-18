"""
scrapers/directory_scraper.py — Sector-specific directory scrapers for Heatr.

Secondary lead discovery source. Used when Google Maps doesn't yield enough
results for a city/sector combination, or to cross-reference and supplement
the Google Maps pipeline.

Implemented directories:
  - Zorgkaart Nederland  (alternatieve_zorg)
  - NatuurlijkBeter.nl   (alternatieve_zorg)
  - ClinicFinder.nl      (cosmetische_klinieken)

Each scraper uses Playwright (these sites require JS). All results upsert to
companies_raw with the appropriate source tag.

The dispatcher function run_directory_scrapers_for_sector() reads directory_urls
from config/sectors.py and routes to the correct scraper automatically —
no code changes needed when adding a new directory URL to a sector config.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlencode, urlparse, quote

from playwright.async_api import async_playwright, Page

from config.sectors import get_sector
from utils.playwright_helpers import (
    new_browser_context,
    random_delay,
    random_mouse_movement,
    extract_domain,
    extract_city_from_address,
)
from utils.rate_limiter import wait_for_token
from utils.deduplicator import is_domain_known

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared selectors / patterns
# ---------------------------------------------------------------------------

_MAX_RESULTS_PER_DIRECTORY = 100
_PAGE_TIMEOUT_MS = 15_000


# =============================================================================
# Zorgkaart Nederland
# =============================================================================

async def scrape_zorgkaart_nederland(
    specialty: str,
    city: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Scrape Zorgkaart Nederland for practitioners in a specialty + city.

    Follows pagination via "Volgende" button. Extracts practitioner name,
    practice name, address, website, phone, rating, and specialty.
    Upserts to companies_raw with source='directory_zorgkaart'.

    Args:
        specialty: Specialty search term, e.g. "fysiotherapeut".
        city: City name, e.g. "Amsterdam".
        workspace_id: Workspace slug — all writes are scoped to this.
        supabase_client: Initialised supabase-py client.

    Returns:
        Dict with keys: ``found`` (int), ``new`` (int).
    """
    summary = {"found": 0, "new": 0}

    encoded_specialty = quote(specialty)
    encoded_city = quote(city)
    start_url = (
        f"https://www.zorgkaartnederland.nl/zoeken"
        f"?q={encoded_specialty}&place={encoded_city}"
    )

    await wait_for_token("website", supabase_client)

    async with async_playwright() as playwright:
        browser, context = await new_browser_context(playwright)
        try:
            page = await context.new_page()
            await page.goto(start_url, wait_until="domcontentloaded", timeout=_PAGE_TIMEOUT_MS)
            await _handle_cookie_consent(page)

            page_num = 1
            while summary["found"] < _MAX_RESULTS_PER_DIRECTORY:
                logger.info(
                    "Zorgkaart: specialty=%s city=%s page=%d",
                    specialty, city, page_num,
                )

                # Wait for listing cards
                try:
                    await page.wait_for_selector(
                        'article, [class*="search-result"], [class*="provider"]',
                        timeout=8_000,
                    )
                except Exception:
                    logger.info("Zorgkaart: no results found for %s in %s", specialty, city)
                    break

                # Extract all listings on this page
                listings = await _extract_zorgkaart_listings(page)

                for listing in listings:
                    if summary["found"] >= _MAX_RESULTS_PER_DIRECTORY:
                        break
                    listing["workspace_id"] = workspace_id
                    listing["source"] = "directory_zorgkaart"
                    listing["sector"] = "alternatieve_zorg"
                    listing["country"] = "NL"

                    result = await _upsert_company_from_directory(
                        listing, workspace_id, supabase_client
                    )
                    summary["found"] += 1
                    if result == "new":
                        summary["new"] += 1

                    await random_delay(0.3, 0.8)

                # Check for "Volgende" (next page) button
                next_btn = await page.query_selector(
                    'a[aria-label="Volgende"], a:has-text("Volgende"), '
                    '[rel="next"], .pagination-next'
                )
                if not next_btn:
                    logger.info("Zorgkaart: no more pages after page %d", page_num)
                    break

                await random_mouse_movement(page)
                await next_btn.click()
                await page.wait_for_load_state("domcontentloaded")
                page_num += 1
                await random_delay()

        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass

    logger.info("Zorgkaart done: found=%d new=%d", summary["found"], summary["new"])
    return summary


async def _extract_zorgkaart_listings(page: Page) -> list[dict]:
    """Extract all practitioner listings from the current Zorgkaart results page.

    Args:
        page: Playwright Page with search results loaded.

    Returns:
        List of dicts with practitioner/practice data.
    """
    listings: list[dict] = []

    # Try multiple card selectors — Zorgkaart has changed layout over time
    card_selectors = [
        'article[class*="provider"]',
        'div[class*="search-result"]',
        'li[class*="result"]',
        'article',
    ]
    cards = []
    for sel in card_selectors:
        cards = await page.query_selector_all(sel)
        if cards:
            break

    for card in cards:
        try:
            data: dict = {
                "company_name": None,
                "contact_name": None,
                "address": None,
                "city": None,
                "phone": None,
                "website": None,
                "domain": None,
                "google_rating": None,
                "raw_data": {},
            }

            # Name: try heading first, then any prominent text
            for name_sel in ('h2', 'h3', '[class*="name"]', '[class*="title"]'):
                el = await card.query_selector(name_sel)
                if el:
                    text = (await el.inner_text()).strip()
                    if text:
                        data["company_name"] = text
                        break

            # Address
            for addr_sel in ('[class*="address"]', '[itemprop="address"]', 'address'):
                el = await card.query_selector(addr_sel)
                if el:
                    raw_addr = (await el.inner_text()).strip()
                    data["address"] = raw_addr
                    data["city"] = extract_city_from_address(raw_addr)
                    break

            # Phone
            tel_el = await card.query_selector('a[href^="tel:"]')
            if tel_el:
                href = await tel_el.get_attribute("href") or ""
                data["phone"] = href.replace("tel:", "").strip()

            # Website
            website_el = await card.query_selector('a[href^="http"]:not([href*="zorgkaart"])')
            if website_el:
                raw_url = await website_el.get_attribute("href") or ""
                if raw_url:
                    data["website"] = raw_url
                    data["domain"] = extract_domain(raw_url)

            # Rating
            for rating_sel in ('[class*="rating"]', '[class*="score"]', '[itemprop="ratingValue"]'):
                el = await card.query_selector(rating_sel)
                if el:
                    rating_text = (await el.inner_text()).strip()
                    rating = _parse_nl_rating(rating_text)
                    if rating is not None:
                        data["google_rating"] = rating
                    break

            data["raw_data"] = {"scraped_at": datetime.now(timezone.utc).isoformat()}

            if data["company_name"]:
                listings.append(data)

        except Exception as e:
            logger.warning("Error extracting Zorgkaart listing: %s", e)
            continue

    return listings


# =============================================================================
# NatuurlijkBeter.nl
# =============================================================================

async def scrape_natuurlijkbeter(
    therapy_type: str,
    region: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Scrape NatuurlijkBeter.nl for natural therapists by therapy type and region.

    Args:
        therapy_type: Therapy type slug, e.g. "acupunctuur" or "osteopathie".
        region: Region or city slug, e.g. "amsterdam" or "utrecht".
        workspace_id: Workspace slug.
        supabase_client: Initialised supabase-py client.

    Returns:
        Dict with keys: ``found`` (int), ``new`` (int).
    """
    summary = {"found": 0, "new": 0}

    # NatuurlijkBeter uses lowercase slugified paths
    therapy_slug = therapy_type.lower().replace(" ", "-")
    region_slug = region.lower().replace(" ", "-")
    start_url = f"https://www.natuurlijkbeter.nl/therapeuten/{therapy_slug}/{region_slug}"

    await wait_for_token("website", supabase_client)

    async with async_playwright() as playwright:
        browser, context = await new_browser_context(playwright)
        try:
            page = await context.new_page()

            try:
                await page.goto(
                    start_url, wait_until="domcontentloaded", timeout=_PAGE_TIMEOUT_MS
                )
            except Exception:
                # URL pattern may not exist for this combination — not an error
                logger.info("NatuurlijkBeter: no page for %s/%s", therapy_slug, region_slug)
                return summary

            await _handle_cookie_consent(page)

            # Check for empty results
            body_text = (await page.inner_text("body")).lower()
            if "geen resultaten" in body_text or "not found" in body_text:
                return summary

            # Extract all listings on the page (NatuurlijkBeter typically shows
            # all results on one page with lazy loading rather than pagination)
            listings = await _extract_natuurlijkbeter_listings(page)

            for listing in listings[:_MAX_RESULTS_PER_DIRECTORY]:
                listing["workspace_id"] = workspace_id
                listing["source"] = "directory_natuurlijkbeter"
                listing["sector"] = "alternatieve_zorg"
                listing["country"] = "NL"

                result = await _upsert_company_from_directory(
                    listing, workspace_id, supabase_client
                )
                summary["found"] += 1
                if result == "new":
                    summary["new"] += 1

        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass

    logger.info("NatuurlijkBeter done: found=%d new=%d", summary["found"], summary["new"])
    return summary


async def _extract_natuurlijkbeter_listings(page: Page) -> list[dict]:
    """Extract practitioner listings from NatuurlijkBeter results page.

    Args:
        page: Playwright Page with NatuurlijkBeter results loaded.

    Returns:
        List of data dicts.
    """
    listings: list[dict] = []

    card_selectors = [
        '[class*="therapeut"]',
        '[class*="practitioner"]',
        'article',
        '.item',
    ]
    cards = []
    for sel in card_selectors:
        cards = await page.query_selector_all(sel)
        if len(cards) > 0:
            break

    for card in cards:
        try:
            data: dict = {
                "company_name": None,
                "contact_name": None,
                "city": None,
                "phone": None,
                "website": None,
                "domain": None,
                "address": None,
                "raw_data": {},
            }

            # Name
            for sel in ('h2', 'h3', '[class*="naam"]', '[class*="name"]'):
                el = await card.query_selector(sel)
                if el:
                    text = (await el.inner_text()).strip()
                    if text:
                        data["company_name"] = text
                        break

            # City
            for sel in ('[class*="plaats"]', '[class*="city"]', '[class*="locatie"]'):
                el = await card.query_selector(sel)
                if el:
                    data["city"] = (await el.inner_text()).strip().title()
                    break

            # Website link
            for el in await card.query_selector_all('a[href^="http"]'):
                href = await el.get_attribute("href") or ""
                if href and "natuurlijkbeter.nl" not in href:
                    data["website"] = href
                    data["domain"] = extract_domain(href)
                    break

            # Phone
            tel_el = await card.query_selector('a[href^="tel:"]')
            if tel_el:
                href = await tel_el.get_attribute("href") or ""
                data["phone"] = href.replace("tel:", "").strip()

            data["raw_data"] = {"scraped_at": datetime.now(timezone.utc).isoformat()}

            if data["company_name"]:
                listings.append(data)

        except Exception as e:
            logger.warning("Error extracting NatuurlijkBeter listing: %s", e)
            continue

    return listings


# =============================================================================
# ClinicFinder.nl
# =============================================================================

async def scrape_clinicfinder(
    treatment: str,
    city: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Scrape ClinicFinder.nl for cosmetic clinics by treatment and city.

    Args:
        treatment: Treatment type slug, e.g. "botox" or "laser-haarverwijdering".
        city: City name, e.g. "Rotterdam".
        workspace_id: Workspace slug.
        supabase_client: Initialised supabase-py client.

    Returns:
        Dict with keys: ``found`` (int), ``new`` (int).
    """
    summary = {"found": 0, "new": 0}

    treatment_slug = treatment.lower().replace(" ", "-")
    city_slug = city.lower().replace(" ", "-")
    start_url = f"https://www.clinicfinder.nl/klinieken/{treatment_slug}/{city_slug}"

    await wait_for_token("website", supabase_client)

    async with async_playwright() as playwright:
        browser, context = await new_browser_context(playwright)
        try:
            page = await context.new_page()

            try:
                response = await page.goto(
                    start_url, wait_until="domcontentloaded", timeout=_PAGE_TIMEOUT_MS
                )
                # ClinicFinder returns 404 for unknown city/treatment combos
                if response and response.status == 404:
                    logger.info("ClinicFinder: 404 for %s/%s", treatment_slug, city_slug)
                    return summary
            except Exception:
                return summary

            await _handle_cookie_consent(page)

            body_text = (await page.inner_text("body")).lower()
            if "geen resultaten" in body_text or "geen klinieken" in body_text:
                return summary

            listings = await _extract_clinicfinder_listings(page)

            for listing in listings[:_MAX_RESULTS_PER_DIRECTORY]:
                listing["workspace_id"] = workspace_id
                listing["source"] = "directory_clinicfinder"
                listing["sector"] = "cosmetische_klinieken"
                listing["country"] = "NL"

                result = await _upsert_company_from_directory(
                    listing, workspace_id, supabase_client
                )
                summary["found"] += 1
                if result == "new":
                    summary["new"] += 1

        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass

    logger.info("ClinicFinder done: found=%d new=%d", summary["found"], summary["new"])
    return summary


async def _extract_clinicfinder_listings(page: Page) -> list[dict]:
    """Extract clinic listings from ClinicFinder results page.

    Args:
        page: Playwright Page with ClinicFinder results loaded.

    Returns:
        List of data dicts.
    """
    listings: list[dict] = []

    card_selectors = [
        '[class*="clinic"]',
        '[class*="kliniek"]',
        'article',
        '.provider-card',
        '.listing-item',
    ]
    cards = []
    for sel in card_selectors:
        cards = await page.query_selector_all(sel)
        if len(cards) > 0:
            break

    for card in cards:
        try:
            data: dict = {
                "company_name": None,
                "address": None,
                "city": None,
                "phone": None,
                "website": None,
                "domain": None,
                "treatments": [],
                "raw_data": {},
            }

            # Name
            for sel in ('h2', 'h3', '[class*="name"]', '[class*="naam"]', '[class*="title"]'):
                el = await card.query_selector(sel)
                if el:
                    text = (await el.inner_text()).strip()
                    if text:
                        data["company_name"] = text
                        break

            # Address
            for addr_sel in ('[class*="address"]', '[class*="adres"]', 'address', '[itemprop="address"]'):
                el = await card.query_selector(addr_sel)
                if el:
                    raw_addr = (await el.inner_text()).strip()
                    data["address"] = raw_addr
                    data["city"] = extract_city_from_address(raw_addr)
                    break

            # Phone
            tel_el = await card.query_selector('a[href^="tel:"]')
            if tel_el:
                href = await tel_el.get_attribute("href") or ""
                data["phone"] = href.replace("tel:", "").strip()

            # Website
            for el in await card.query_selector_all('a[href^="http"]'):
                href = await el.get_attribute("href") or ""
                if href and "clinicfinder.nl" not in href:
                    data["website"] = href
                    data["domain"] = extract_domain(href)
                    break

            # Treatments listed (store in raw_data for enrichment)
            treatment_els = await card.query_selector_all('[class*="treatment"], [class*="behandeling"]')
            treatments = []
            for el in treatment_els:
                t = (await el.inner_text()).strip()
                if t:
                    treatments.append(t)
            data["treatments"] = treatments
            data["raw_data"] = {
                "treatments": treatments,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            }

            if data["company_name"]:
                listings.append(data)

        except Exception as e:
            logger.warning("Error extracting ClinicFinder listing: %s", e)
            continue

    return listings


# =============================================================================
# Dispatcher
# =============================================================================

async def run_directory_scrapers_for_sector(
    sector_key: str,
    city: str,
    country: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Dispatch directory scrapers based on sector config directory_urls.

    Reads ``directory_urls`` from the sector config in config/sectors.py and
    routes each URL to the correct scraper function. No code changes needed
    when adding a new directory URL to a sector config.

    Args:
        sector_key: Sector key string, e.g. 'alternatieve_zorg'.
        city: City name, e.g. "Utrecht".
        country: ISO 2-letter country code.
        workspace_id: Workspace slug.
        supabase_client: Initialised supabase-py client.

    Returns:
        Combined summary dict: ``{"found": int, "new": int, "sources": list[str]}``.
    """
    combined: dict = {"found": 0, "new": 0, "sources": []}

    try:
        sector = get_sector(sector_key)
    except ValueError as e:
        logger.error("run_directory_scrapers_for_sector: %s", e)
        return combined

    directory_urls: list[str] = sector.get("directory_urls", [])

    for url_template in directory_urls:
        # Substitute {city} placeholder
        url = url_template.replace("{city}", city.lower().replace(" ", "-"))

        source_label = _classify_directory_url(url)
        logger.info("Directory scraper: source=%s city=%s", source_label, city)

        try:
            result = await _dispatch_directory_url(
                url=url,
                source_label=source_label,
                city=city,
                sector_key=sector_key,
                workspace_id=workspace_id,
                supabase_client=supabase_client,
            )
            combined["found"] += result.get("found", 0)
            combined["new"] += result.get("new", 0)
            combined["sources"].append(source_label)

        except Exception as e:
            logger.error("Directory scraper failed for %s: %s", url, e)
            continue

    return combined


async def _scrape_generic_listing_page(
    url: str,
    city: str,
    source: str,
    workspace_id: str,
    supabase_client: Any,
    name_selector: str,
    link_selector: str,
    max_results: int = 60,
) -> dict:
    """Generic Playwright-based directory listing scraper.

    Navigates to the URL, finds business names and detail links using the
    provided CSS selectors, visits each detail page to extract contact info,
    and upserts results via _upsert_company_from_directory.

    Works for any directory that renders listings as a list of named links.
    Selector strings support comma-separated fallbacks (tried in order).

    Returns:
        {"found": int, "new": int}
    """
    from utils.playwright_helpers import new_browser_context

    found = 0
    new = 0

    try:
        async with new_browser_context() as (browser, context):
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
            await _handle_cookie_consent(page)

            import asyncio
            await asyncio.sleep(2)

            # Find all listing name elements
            names_els = await page.query_selector_all(name_selector)
            link_els = await page.query_selector_all(link_selector)

            # Build list of (name, href) pairs
            listings: list[tuple[str, str]] = []
            for i, name_el in enumerate(names_els[:max_results]):
                name_text = (await name_el.inner_text()).strip()
                href = ""
                if i < len(link_els):
                    href = await link_els[i].get_attribute("href") or ""
                if name_text:
                    listings.append((name_text, href))

            logger.info(
                "Generic directory scraper [%s]: found %d listings on %s",
                source, len(listings), url,
            )

            for company_name, detail_href in listings:
                found += 1

                # Extract domain from detail link if it points to external site
                detail_data: dict[str, Any] = {
                    "company_name": company_name,
                    "city": city,
                    "source": source,
                    "directory_url": detail_href or url,
                }

                # Try visiting detail page for more data
                if detail_href and detail_href.startswith(("http", "/")):
                    full_url = detail_href if detail_href.startswith("http") else f"{url.split('/')[0]}//{url.split('/')[2]}{detail_href}"
                    try:
                        detail_page = await context.new_page()
                        await detail_page.goto(full_url, wait_until="domcontentloaded", timeout=15_000)
                        body_text = await detail_page.inner_text("body")

                        # Extract website link
                        website_links = await detail_page.query_selector_all('a[href*="http"]:not([href*="' + url.split("/")[2] + '"])')
                        for wl in website_links[:5]:
                            href_val = await wl.get_attribute("href") or ""
                            text_val = (await wl.inner_text()).strip().lower()
                            if any(kw in text_val for kw in ["website", "site", "bezoek", "www"]) or any(kw in href_val.lower() for kw in [".nl", ".be", ".com"]):
                                from urllib.parse import urlparse
                                parsed = urlparse(href_val)
                                if parsed.netloc and parsed.netloc != url.split("/")[2]:
                                    detail_data["domain"] = parsed.netloc.replace("www.", "")
                                    detail_data["website"] = href_val
                                    break

                        # Extract phone
                        import re
                        phone_links = await detail_page.query_selector_all('a[href^="tel:"]')
                        if phone_links:
                            phone_href = await phone_links[0].get_attribute("href") or ""
                            detail_data["phone"] = phone_href.replace("tel:", "").strip()
                        else:
                            phone_match = re.search(r"(?:0[1-9][0-9][\s\-]?[0-9]{6,7}|06[\s\-]?[0-9]{8}|\+31[\s\-]?[0-9\s\-]{9,11})", body_text)
                            if phone_match:
                                detail_data["phone"] = phone_match.group().strip()

                        # Extract email
                        email_links = await detail_page.query_selector_all('a[href^="mailto:"]')
                        if email_links:
                            mailto = await email_links[0].get_attribute("href") or ""
                            detail_data["email"] = mailto.replace("mailto:", "").split("?")[0].strip()

                        await detail_page.close()
                    except Exception as exc:
                        logger.debug("Generic scraper: detail page failed for %s: %s", full_url, exc)

                result = await _upsert_company_from_directory(
                    detail_data, workspace_id, supabase_client,
                )
                if result == "new":
                    new += 1

    except Exception as exc:
        logger.error("Generic directory scraper [%s] failed: %s", source, exc)

    return {"found": found, "new": new}


async def _dispatch_directory_url(
    url: str,
    source_label: str,
    city: str,
    sector_key: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Route a directory URL to the correct scraper function.

    Args:
        url: Full URL (with {city} already substituted).
        source_label: Identifier string used to pick the scraper.
        city: City name (passed to sector-specific scrapers).
        sector_key: Sector key string.
        workspace_id: Workspace slug.
        supabase_client: Supabase client.

    Returns:
        Summary dict from the invoked scraper.
    """
    # --- Makelaars directories ---
    if "funda.nl/makelaars" in url:
        return await _scrape_generic_listing_page(
            url=url, city=city, source="funda",
            workspace_id=workspace_id, supabase_client=supabase_client,
            name_selector='a.agent-name, h2.name, [data-test-id="agent-name"]',
            link_selector='a.agent-name, a[href*="/makelaars/"]',
        )

    elif "nvm.nl/makelaars" in url:
        return await _scrape_generic_listing_page(
            url=url, city=city, source="nvm",
            workspace_id=workspace_id, supabase_client=supabase_client,
            name_selector='.makelaar-name, .card-title, h3',
            link_selector='a[href*="/makelaar/"]',
        )

    elif "vbo.nl" in url:
        return await _scrape_generic_listing_page(
            url=url, city=city, source="vbo",
            workspace_id=workspace_id, supabase_client=supabase_client,
            name_selector='.makelaar-naam, h3, .card-title',
            link_selector='a[href*="/makelaar"]',
        )

    # --- Behandelaren directories ---
    elif "coachfinder.nl" in url:
        return await _scrape_generic_listing_page(
            url=url, city=city, source="coachfinder",
            workspace_id=workspace_id, supabase_client=supabase_client,
            name_selector='.coach-name, h3.name, .listing-title',
            link_selector='a[href*="/coach/"]',
        )

    elif "natuurlijkbeter.nl" in url:
        therapy_type = _extract_path_segment(url, before_slug=city.lower())
        return await scrape_natuurlijkbeter(
            therapy_type=therapy_type or "therapeut",
            region=city,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )

    elif "therapiepsycholoog.nl" in url:
        return await _scrape_generic_listing_page(
            url=url, city=city, source="therapiepsycholoog",
            workspace_id=workspace_id, supabase_client=supabase_client,
            name_selector='.therapist-name, h3, .listing-title',
            link_selector='a[href*="/psycholoog/"]',
        )

    # --- Bouwbedrijven directories ---
    elif "werkspot.nl" in url:
        return await _scrape_generic_listing_page(
            url=url, city=city, source="werkspot",
            workspace_id=workspace_id, supabase_client=supabase_client,
            name_selector='.company-name, h3.name, [data-testid="company-name"]',
            link_selector='a[href*="/profiel/"]',
        )

    elif "bouwend-nederland.nl" in url:
        return await _scrape_generic_listing_page(
            url=url, city=city, source="bouwend_nederland",
            workspace_id=workspace_id, supabase_client=supabase_client,
            name_selector='.lid-naam, h3, .member-name',
            link_selector='a[href*="/lid/"], a[href*="/leden/"]',
        )

    elif "thuisvakman.nl" in url:
        return await _scrape_generic_listing_page(
            url=url, city=city, source="thuisvakman",
            workspace_id=workspace_id, supabase_client=supabase_client,
            name_selector='.vakman-name, h3, .listing-title',
            link_selector='a[href*="/vakman/"]',
        )

    # --- Legacy directories (still functional) ---
    elif "zorgkaartnederland.nl" in url:
        specialty = _extract_query_param(url, "q") or "therapeut"
        return await scrape_zorgkaart_nederland(
            specialty=specialty,
            city=city,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )

    elif "clinicfinder.nl" in url:
        treatment = _extract_path_segment(url, before_slug=city.lower())
        return await scrape_clinicfinder(
            treatment=treatment or "cosmetisch",
            city=city,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )

    else:
        logger.warning("No scraper mapped for directory URL: %s", url)
        return {"found": 0, "new": 0}


# =============================================================================
# Shared internal helpers
# =============================================================================

async def _handle_cookie_consent(page: Page) -> None:
    """Dismiss cookie consent banners common on Dutch websites.

    Args:
        page: Playwright Page instance.
    """
    consent_selectors = [
        'button:has-text("Accepteren")',
        'button:has-text("Alles accepteren")',
        'button:has-text("Akkoord")',
        'button:has-text("Cookies accepteren")',
        'button[id*="accept"]',
        'button[class*="accept"]',
        'button[aria-label*="accept"]',
    ]
    for sel in consent_selectors:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                import asyncio
                await asyncio.sleep(0.8)
                return
        except Exception:
            continue


async def _upsert_company_from_directory(
    data: dict,
    workspace_id: str,
    supabase_client: Any,
) -> str:
    """Upsert a directory-scraped company into companies_raw.

    Skips records with no name. On domain conflict: skips (don't overwrite
    Google Maps data with potentially lower-quality directory data).

    Args:
        data: Listing data dict with at minimum a 'company_name' key.
        workspace_id: Workspace slug.
        supabase_client: Supabase client.

    Returns:
        'new', 'skipped', or 'duplicate'.
    """
    name = data.get("company_name")
    if not name:
        return "skipped"

    domain = data.get("domain") or None

    # Skip if domain already known (don't overwrite Google Maps data)
    if domain and await is_domain_known(domain, workspace_id, supabase_client):
        return "duplicate"

    record = {
        "workspace_id": workspace_id,
        "company_name": name,
        "domain": domain,
        "sector": data.get("sector", ""),
        "city": data.get("city"),
        "country": data.get("country", "NL"),
        "phone": data.get("phone"),
        "address": data.get("address"),
        "google_rating": data.get("google_rating"),
        "source": data.get("source", "directory"),
        "raw_data": data.get("raw_data", {}),
    }

    try:
        supabase_client.table("companies_raw").insert(record).execute()
        return "new"
    except Exception as e:
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            return "duplicate"
        logger.error("Failed to insert directory company %s: %s", name, e)
        return "skipped"


def _parse_nl_rating(text: str) -> float | None:
    """Parse a Dutch rating string like "8,4" or "4.2/5" to a float.

    Zorgkaart uses a 0-10 scale; we normalise to 0-10 and store as-is.
    The lead scorer handles the scale difference from Google's 1-5.

    Args:
        text: Raw rating string.

    Returns:
        Float rating or None.
    """
    if not text:
        return None
    normalised = text.replace(",", ".").strip()
    match = re.search(r"\b(\d+(?:\.\d+)?)\b", normalised)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass
    return None


def _classify_directory_url(url: str) -> str:
    """Return a short source label for a directory URL.

    Args:
        url: Full URL string.

    Returns:
        Short identifier, e.g. 'zorgkaart', 'natuurlijkbeter', 'clinicfinder'.
    """
    if "zorgkaartnederland.nl" in url:
        return "zorgkaart"
    if "natuurlijkbeter.nl" in url:
        return "natuurlijkbeter"
    if "clinicfinder.nl" in url:
        return "clinicfinder"
    return urlparse(url).netloc.replace("www.", "")


def _extract_query_param(url: str, param: str) -> str | None:
    """Extract a query parameter value from a URL string.

    Args:
        url: Full URL string.
        param: Query parameter name.

    Returns:
        Parameter value string or None.
    """
    from urllib.parse import parse_qs
    qs = parse_qs(urlparse(url).query)
    values = qs.get(param, [])
    return values[0] if values else None


def _extract_path_segment(url: str, before_slug: str) -> str | None:
    """Extract the path segment immediately before a known slug in a URL path.

    Used to extract therapy_type or treatment from directory URL templates.

    Args:
        url: Full URL string.
        before_slug: The slug that follows the segment we want.

    Returns:
        Path segment string or None.

    Example:
        _extract_path_segment(
            "https://www.natuurlijkbeter.nl/therapeuten/acupunctuur/amsterdam",
            "amsterdam"
        ) → "acupunctuur"
    """
    path = urlparse(url).path
    parts = [p for p in path.split("/") if p]
    slug_lower = before_slug.lower()
    for i, part in enumerate(parts):
        if part.lower() == slug_lower and i > 0:
            return parts[i - 1]
    return None
