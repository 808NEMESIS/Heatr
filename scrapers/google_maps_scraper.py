"""
scrapers/google_maps_scraper.py — Google Maps scraper for Heatr lead discovery.

Own Playwright implementation. No Apify, no third-party scraping APIs.

Anti-detection rules from CLAUDE.md are enforced throughout:
- new_browser_context() for every session
- random_mouse_movement() before every click
- random_delay() between results
- Context rotation every 60 results
- CAPTCHA detection → graceful stop
- Rate limiting via Supabase token bucket

Google Maps DOM note: Google changes selectors frequently. This implementation
uses multiple fallbacks per field and relies on structural patterns (role=feed,
aria-labels) rather than CSS class names where possible.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

from playwright.async_api import async_playwright, Page, BrowserContext

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
# Google Maps selectors — with fallbacks.
# Primary selectors target structural/aria attributes; fallbacks use
# common class patterns that are less stable but more explicit.
# ---------------------------------------------------------------------------

_SEL_FEED = 'div[role="feed"]'
_SEL_RESULT_ITEM = 'div[role="feed"] > div'
_SEL_RESULT_LINK = 'a[href*="maps/place"]'

# Detail panel selectors (opened after clicking a result card)
_SEL_DETAIL_NAME_PRIMARY = 'h1[class*="DUwDvf"]'
_SEL_DETAIL_NAME_FALLBACK = 'h1'
_SEL_DETAIL_ADDRESS = 'button[data-item-id="address"]'
_SEL_DETAIL_PHONE = 'button[data-item-id^="phone:tel:"]'
_SEL_DETAIL_WEBSITE = 'a[data-item-id="authority"]'
_SEL_DETAIL_RATING = '[aria-label*="sterren"], [aria-label*="stars"]'
_SEL_DETAIL_CATEGORY = 'button[jsaction*="category"]'
_SEL_DETAIL_INSTAGRAM = 'a[href*="instagram.com"]'
_SEL_DETAIL_BOOKING = (
    'a[href*="calendly"], a[href*="doctolib"], a[href*="acuity"], '
    'a[href*="planity"], a[href*="booking"], a[href*="afspraak"]'
)

# CAPTCHA detection patterns
_CAPTCHA_PATTERNS = [
    "unusual traffic",
    "ongebruikelijk verkeer",
    "recaptcha",
    "captcha",
    "ik ben geen robot",
    "i'm not a robot",
]


# =============================================================================
# Public API
# =============================================================================

async def scrape_google_maps(
    query: str,
    location: str,
    country: str,
    sector_key: str,
    workspace_id: str,
    supabase_client: Any,
    max_results: int = 60,
    job_id: str | None = None,
) -> dict:
    """Scrape Google Maps for business leads matching a query in a location.

    Entry point for all Google Maps lead discovery. Upserts results to
    ``companies_raw`` in Supabase. Respects rate limits, rotates browser
    contexts every 60 results, and handles CAPTCHAs gracefully.

    Args:
        query: Search query string, e.g. "fysiotherapeut".
        location: City or region name, e.g. "Amsterdam".
        country: ISO 2-letter country code — 'NL' or 'BE'.
        sector_key: Sector key from config/sectors.py (stored with each result).
        workspace_id: Workspace slug — all writes are scoped to this.
        supabase_client: Initialised supabase-py client.
        max_results: Hard cap on results scraped (default 60, env overrides).
        job_id: Optional scraping_jobs row ID for incremental progress updates.

    Returns:
        Dict with keys: ``found`` (int), ``new`` (int), ``updated`` (int),
        ``errors`` (list[str]).
    """
    # Respect GOOGLE_MAPS_MAX_RESULTS env var as absolute hard cap
    env_max = int(os.getenv("GOOGLE_MAPS_MAX_RESULTS", "60"))
    max_results = min(max_results, env_max)

    result_summary: dict = {"found": 0, "new": 0, "updated": 0, "errors": []}

    await wait_for_token("google_maps", supabase_client)

    async with async_playwright() as playwright:
        browser, context = await new_browser_context(playwright)
        results_in_context = 0

        try:
            search_url = await build_search_url(query, location, country)
            logger.info("Google Maps scrape: %s | url=%s", query, search_url)

            page = await context.new_page()

            # Navigate and wait for results feed
            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=20_000)
            except Exception as e:
                logger.warning("Navigation failed: %s", e)
                result_summary["errors"].append(f"navigation_failed: {e}")
                return result_summary

            # Handle Google consent screen (cookie banner for EU)
            await _handle_consent_screen(page)

            # Wait for results feed
            try:
                await page.wait_for_selector(_SEL_FEED, timeout=15_000)
            except Exception:
                # Check for CAPTCHA before giving up
                if await _is_captcha_page(page):
                    logger.warning("CAPTCHA detected at start of scrape")
                    await _update_job_status(job_id, "captcha_blocked", supabase_client)
                    return result_summary
                logger.warning("Results feed not found for query: %s %s", query, location)
                return result_summary

            # Scroll to load all results up to max_results
            total_visible = await scroll_results_panel(page, max_results)
            logger.info("Visible results after scroll: %d", total_visible)

            # Collect all result links from the feed
            result_links = await page.query_selector_all(_SEL_RESULT_LINK)
            logger.info("Result links found: %d", len(result_links))

            processed_urls: set[str] = set()

            for i, link_el in enumerate(result_links):
                if result_summary["found"] >= max_results:
                    break

                # Rotate context every 60 results
                if results_in_context >= 60:
                    logger.info("Rotating browser context after 60 results")
                    await context.close()
                    await browser.close()
                    browser, context = await new_browser_context(playwright)
                    page = await context.new_page()
                    await wait_for_token("google_maps", supabase_client)
                    # Re-navigate to the search results
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=20_000)
                    await _handle_consent_screen(page)
                    await page.wait_for_selector(_SEL_FEED, timeout=15_000)
                    await scroll_results_panel(page, max_results)
                    result_links = await page.query_selector_all(_SEL_RESULT_LINK)
                    results_in_context = 0
                    if i >= len(result_links):
                        break
                    link_el = result_links[i]

                # Get the href to detect duplicates within this run
                href = await link_el.get_attribute("href") or ""
                if href in processed_urls:
                    continue
                processed_urls.add(href)

                try:
                    # Simulate human behaviour before clicking
                    await random_mouse_movement(page)

                    # Scroll element into view, then click
                    await link_el.scroll_into_view_if_needed()
                    await asyncio.sleep(0.3)
                    await link_el.click()

                    # Wait for detail panel to load
                    await page.wait_for_selector(_SEL_DETAIL_NAME_FALLBACK, timeout=10_000)
                    await asyncio.sleep(0.5)

                    # Check for CAPTCHA after click
                    if await _is_captcha_page(page):
                        logger.warning("CAPTCHA detected after clicking result %d", i)
                        await _update_job_status(job_id, "captcha_blocked", supabase_client)
                        result_summary["errors"].append("captcha_blocked")
                        break

                    place = await extract_place_details(page)
                    place["sector"] = sector_key
                    place["country"] = country
                    place["workspace_id"] = workspace_id
                    place["scraping_job_id"] = job_id
                    place["source"] = "google_maps"

                    result_summary["found"] += 1
                    results_in_context += 1

                    # Upsert to Supabase
                    upsert_result = await _upsert_company(place, workspace_id, supabase_client)
                    if upsert_result == "new":
                        result_summary["new"] += 1
                    elif upsert_result == "updated":
                        result_summary["updated"] += 1

                    # Increment job counters incrementally
                    if job_id:
                        await _increment_job_counts(job_id, result_summary, supabase_client)

                    await random_delay()

                except Exception as exc:
                    logger.warning("Error processing result %d: %s", i, exc)
                    result_summary["errors"].append(str(exc))
                    # Continue to next result — never stop the pipeline
                    continue

        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass

    logger.info(
        "Google Maps done: found=%d new=%d updated=%d errors=%d",
        result_summary["found"],
        result_summary["new"],
        result_summary["updated"],
        len(result_summary["errors"]),
    )
    return result_summary


# =============================================================================
# URL builder
# =============================================================================

async def build_search_url(query: str, location: str, country: str) -> str:
    """Build the correct Google Maps search URL per country.

    Args:
        query: Search query string, e.g. "fysiotherapeut".
        location: City or region, e.g. "Amsterdam".
        country: ISO 2-letter country code — 'NL' uses google.nl, 'BE' uses google.be.

    Returns:
        Full Google Maps search URL with encoded query.
    """
    combined = f"{query} {location}".strip()
    encoded = quote_plus(combined)

    domain = "google.be" if country.upper() == "BE" else "google.nl"
    return f"https://www.{domain}/maps/search/{encoded}"


# =============================================================================
# Detail panel extractor
# =============================================================================

async def extract_place_details(page: Page) -> dict:
    """Extract all available fields from an open Google Maps detail panel.

    Uses multiple selector fallbacks per field. Never raises — missing fields
    are returned as None.

    Args:
        page: Playwright Page with a detail panel currently open.

    Returns:
        Dict with keys: name, address, phone, website, domain, google_rating,
        google_review_count, google_category, google_maps_url, city,
        has_instagram, has_online_booking.
    """
    data: dict = {
        "name": None,
        "address": None,
        "phone": None,
        "website": None,
        "domain": None,
        "google_rating": None,
        "google_review_count": None,
        "google_category": None,
        "google_maps_url": page.url,
        "city": None,
        "has_instagram": False,
        "has_online_booking": False,
        "raw_data": {},
    }

    # --- Name ----------------------------------------------------------------
    for sel in (_SEL_DETAIL_NAME_PRIMARY, _SEL_DETAIL_NAME_FALLBACK):
        try:
            el = await page.query_selector(sel)
            if el:
                data["name"] = (await el.inner_text()).strip()
                break
        except Exception:
            continue

    # --- Address -------------------------------------------------------------
    try:
        el = await page.query_selector(_SEL_DETAIL_ADDRESS)
        if el:
            raw_addr = (await el.inner_text()).strip()
            data["address"] = raw_addr
            data["city"] = extract_city_from_address(raw_addr)
    except Exception:
        pass

    # Address fallback: aria-label on address button
    if not data["address"]:
        try:
            el = await page.query_selector('button[aria-label*="Adres"]')
            if el:
                raw_addr = (await el.get_attribute("aria-label") or "").replace("Adres:", "").strip()
                data["address"] = raw_addr
                data["city"] = extract_city_from_address(raw_addr)
        except Exception:
            pass

    # --- Phone ---------------------------------------------------------------
    try:
        el = await page.query_selector(_SEL_DETAIL_PHONE)
        if el:
            aria = await el.get_attribute("aria-label") or ""
            # aria-label format: "Telefoonnummer: 020-1234567"
            phone_match = re.search(r"[\d\s\-\+\(\)]{7,}", aria)
            if phone_match:
                data["phone"] = phone_match.group(0).strip()
    except Exception:
        pass

    # Phone fallback: tel: href
    if not data["phone"]:
        try:
            el = await page.query_selector('a[href^="tel:"]')
            if el:
                href = await el.get_attribute("href") or ""
                data["phone"] = href.replace("tel:", "").strip()
        except Exception:
            pass

    # --- Website -------------------------------------------------------------
    try:
        el = await page.query_selector(_SEL_DETAIL_WEBSITE)
        if el:
            raw_url = await el.get_attribute("href") or ""
            if raw_url:
                data["website"] = raw_url
                data["domain"] = extract_domain(raw_url)
    except Exception:
        pass

    # Website fallback: look for any external link button with "Website" aria-label
    if not data["website"]:
        try:
            el = await page.query_selector('a[aria-label*="Website"]')
            if el:
                raw_url = await el.get_attribute("href") or ""
                if raw_url and "google." not in raw_url:
                    data["website"] = raw_url
                    data["domain"] = extract_domain(raw_url)
        except Exception:
            pass

    # --- Rating + review count -----------------------------------------------
    try:
        # Rating often in a span with aria-label "4,6 sterren"
        rating_els = await page.query_selector_all('[aria-label*="sterren"], [aria-label*="stars"]')
        for el in rating_els:
            aria = await el.get_attribute("aria-label") or ""
            rating = parse_rating(aria)
            if rating is not None:
                data["google_rating"] = rating
                break
    except Exception:
        pass

    # Review count: look for text like "(123)" near the rating
    try:
        review_els = await page.query_selector_all('[aria-label*="recensie"], [aria-label*="review"]')
        for el in review_els:
            aria = await el.get_attribute("aria-label") or ""
            count = parse_review_count(aria)
            if count is not None:
                data["google_review_count"] = count
                break
    except Exception:
        pass

    # --- Category ------------------------------------------------------------
    try:
        el = await page.query_selector(_SEL_DETAIL_CATEGORY)
        if el:
            data["google_category"] = (await el.inner_text()).strip()
    except Exception:
        pass

    # Category fallback: button with jsaction containing "category"
    if not data["google_category"]:
        try:
            # Category often appears as a plain text span near the name
            cat_el = await page.query_selector('button[jsaction*="category"]')
            if not cat_el:
                # Some layouts use a span between name and address
                cat_el = await page.query_selector('span[jstcache] + div > span')
            if cat_el:
                data["google_category"] = (await cat_el.inner_text()).strip()
        except Exception:
            pass

    # --- Instagram -----------------------------------------------------------
    try:
        ig_el = await page.query_selector(_SEL_DETAIL_INSTAGRAM)
        data["has_instagram"] = ig_el is not None
    except Exception:
        pass

    # --- Online booking signals ----------------------------------------------
    try:
        booking_el = await page.query_selector(_SEL_DETAIL_BOOKING)
        data["has_online_booking"] = booking_el is not None
    except Exception:
        pass

    # Also check page text for booking keywords
    if not data["has_online_booking"]:
        try:
            page_text = (await page.inner_text("body")).lower()
            booking_keywords = ["afspraak", "reserveer", "boek nu", "online booking"]
            if any(kw in page_text for kw in booking_keywords):
                data["has_online_booking"] = True
        except Exception:
            pass

    # Store raw snapshot for debugging
    data["raw_data"] = {
        "url": data["google_maps_url"],
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }

    return data


# =============================================================================
# Scroll helper
# =============================================================================

async def scroll_results_panel(page: Page, target_count: int) -> int:
    """Scroll the Google Maps results panel until target_count results are visible.

    Scrolls inside the ``[role="feed"]`` container, not the page body. Stops
    when target_count results are loaded, or when two consecutive scrolls yield
    no new results (end of list).

    Args:
        page: Playwright Page with the search results loaded.
        target_count: Desired number of visible results.

    Returns:
        Actual count of result links visible after scrolling.
    """
    try:
        feed = await page.query_selector(_SEL_FEED)
        if not feed:
            return 0
    except Exception:
        return 0

    prev_count = 0
    stale_scrolls = 0
    max_stale = 3  # Stop after 3 scrolls with no new results

    while True:
        current_links = await page.query_selector_all(_SEL_RESULT_LINK)
        current_count = len(current_links)

        if current_count >= target_count:
            break

        if current_count == prev_count:
            stale_scrolls += 1
            if stale_scrolls >= max_stale:
                logger.info("No new results after %d scrolls, stopping at %d", max_stale, current_count)
                break
        else:
            stale_scrolls = 0

        prev_count = current_count

        # Scroll inside the feed container
        try:
            await feed.evaluate("el => el.scrollBy(0, 800)")
        except Exception:
            # Feed reference may be stale — re-query
            try:
                await page.evaluate(
                    f'document.querySelector("{_SEL_FEED}").scrollBy(0, 800)'
                )
            except Exception:
                break

        # Wait for lazy-loaded results to appear
        await asyncio.sleep(1.5)

    final_links = await page.query_selector_all(_SEL_RESULT_LINK)
    return len(final_links)


# =============================================================================
# Parsing helpers
# =============================================================================

def parse_rating(rating_text: str) -> float | None:
    """Parse a Google Maps rating string to a float.

    Handles Dutch format "4,6 sterren" and English "4.6 stars".

    Args:
        rating_text: Raw aria-label or text string containing a rating.

    Returns:
        Float rating (e.g. 4.6) or None if not parseable.

    Examples:
        >>> parse_rating("4,6 sterren")
        4.6
        >>> parse_rating("4.6 stars")
        4.6
        >>> parse_rating("Geen beoordelingen")
        None
    """
    if not rating_text:
        return None

    # Normalise: replace comma with dot, remove thousands separators
    normalised = rating_text.replace(",", ".").strip()

    # Find a number between 1.0 and 5.0
    match = re.search(r"\b([1-5](?:\.\d)?)\b", normalised)
    if match:
        try:
            value = float(match.group(1))
            if 1.0 <= value <= 5.0:
                return value
        except ValueError:
            pass
    return None


def parse_review_count(text: str) -> int | None:
    """Parse a Google Maps review count string to an integer.

    Handles formats: "(123)", "123 recensies", "123 reviews", "1.234 recensies".

    Args:
        text: Raw string containing review count.

    Returns:
        Integer review count or None if not parseable.

    Examples:
        >>> parse_review_count("(123)")
        123
        >>> parse_review_count("1.234 recensies")
        1234
        >>> parse_review_count("Geen recensies")
        None
    """
    if not text:
        return None

    # Remove dots used as thousands separators, then find digits
    cleaned = text.replace(".", "").replace(",", "")
    match = re.search(r"\b(\d+)\b", cleaned)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return None


# =============================================================================
# Internal helpers
# =============================================================================

async def _handle_consent_screen(page: Page) -> None:
    """Accept Google's consent/cookie screen if present.

    Args:
        page: Playwright Page instance.
    """
    consent_selectors = [
        'button[aria-label="Alles accepteren"]',
        'button[aria-label="Accept all"]',
        'form[action*="consent"] button',
        '#L2AGLb',  # Google consent button ID (may change)
    ]
    for sel in consent_selectors:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                await asyncio.sleep(1.0)
                return
        except Exception:
            continue


async def _is_captcha_page(page: Page) -> bool:
    """Detect if the current page is a CAPTCHA challenge.

    Args:
        page: Playwright Page instance.

    Returns:
        True if CAPTCHA patterns found in page content or URL.
    """
    url = page.url.lower()
    if "sorry" in url or "captcha" in url:
        return True

    try:
        body_text = (await page.inner_text("body")).lower()
        return any(pattern in body_text for pattern in _CAPTCHA_PATTERNS)
    except Exception:
        return False


async def _upsert_company(
    place: dict,
    workspace_id: str,
    supabase_client: Any,
) -> str:
    """Upsert a scraped place record to companies_raw.

    ON CONFLICT(workspace_id, domain): updates rating, review_count, and
    last_scraped_at but never overwrites manually entered data.

    Args:
        place: Extracted place dict from extract_place_details().
        workspace_id: Workspace slug.
        supabase_client: Supabase client.

    Returns:
        'new' if inserted, 'updated' if existing record refreshed,
        'skipped' if no domain and no name.
    """
    name = place.get("name")
    if not name:
        return "skipped"

    domain = place.get("domain") or None
    now_iso = datetime.now(timezone.utc).isoformat()

    # Check dedup by domain (only if domain is known)
    if domain and await is_domain_known(domain, workspace_id, supabase_client):
        # Update mutable fields only
        try:
            supabase_client.table("companies_raw").update({
                "google_rating": place.get("google_rating"),
                "google_review_count": place.get("google_review_count"),
                "raw_data": place.get("raw_data", {}),
            }).eq("workspace_id", workspace_id).eq("domain", domain).execute()
        except Exception as e:
            logger.warning("Failed to update existing company %s: %s", domain, e)
        return "updated"

    # Insert new record
    record = {
        "workspace_id": workspace_id,
        "company_name": name,
        "domain": domain,
        "sector": place.get("sector", ""),
        "city": place.get("city"),
        "country": place.get("country", "NL"),
        "phone": place.get("phone"),
        "address": place.get("address"),
        "google_rating": place.get("google_rating"),
        "google_review_count": place.get("google_review_count"),
        "google_maps_url": place.get("google_maps_url"),
        "source": place.get("source", "google_maps"),
        "scraping_job_id": place.get("scraping_job_id"),
        "raw_data": place.get("raw_data", {}),
    }

    try:
        supabase_client.table("companies_raw").insert(record).execute()
        return "new"
    except Exception as e:
        # Duplicate constraint race condition (concurrent workers) — treat as updated
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            return "updated"
        logger.error("Failed to insert company %s: %s", name, e)
        return "skipped"


async def _update_job_status(
    job_id: str | None,
    status: str,
    supabase_client: Any,
) -> None:
    """Update a scraping_jobs row status.

    Args:
        job_id: UUID string of the scraping job, or None (no-op).
        status: New status string.
        supabase_client: Supabase client.
    """
    if not job_id:
        return
    try:
        supabase_client.table("scraping_jobs").update({
            "status": status,
        }).eq("id", job_id).execute()
    except Exception as e:
        logger.warning("Failed to update job %s status: %s", job_id, e)


async def _increment_job_counts(
    job_id: str,
    summary: dict,
    supabase_client: Any,
) -> None:
    """Write current found/new counts back to the scraping_jobs row.

    Args:
        job_id: UUID string of the scraping job.
        summary: Current summary dict with 'found' and 'new' keys.
        supabase_client: Supabase client.
    """
    try:
        supabase_client.table("scraping_jobs").update({
            "total_found": summary["found"],
            "total_new": summary["new"],
        }).eq("id", job_id).execute()
    except Exception as e:
        logger.warning("Failed to update job counts for %s: %s", job_id, e)
