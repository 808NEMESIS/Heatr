"""
scrapers/google_search_scraper.py — Google Search email fallback scraper.

Step 3 of the 4-step email waterfall. Best-effort only — failure must never
crash the pipeline. Returns empty list on any error.

Rate limit: max 10 Google Search queries per hour (seeded in rate_limit_state).
Delays: 5-12 seconds between queries to avoid triggering CAPTCHA.
CAPTCHA: stores a 2-hour block in system_state table, skips immediately.

Playwright is used (not httpx) because Google Search uses JS rendering and
bot detection. One browser context per call, closed after all queries finish.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus

from playwright.async_api import async_playwright

from utils.playwright_helpers import (
    new_browser_context,
    random_delay,
    random_mouse_movement,
)
from utils.rate_limiter import wait_for_token

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_GOOGLE_NL_SEARCH = "https://www.google.nl/search"
_CAPTCHA_BLOCK_KEY = "google_search_blocked_until"
_CAPTCHA_BLOCK_HOURS = 2
_RESULTS_TIMEOUT_MS = 10_000

_EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Google CAPTCHA indicators
_CAPTCHA_INDICATORS = [
    "recaptcha",
    "unusual traffic",
    "ongebruikelijk verkeer",
    "captcha",
    "sorry/index",
    "google.com/sorry",
]

# Result selectors — Google changes these; using multiple fallbacks
_RESULT_CONTAINER = "#search, #rso, [id='search']"
_SNIPPET_SELECTORS = [
    ".VwiC3b",          # Standard snippet class (changes periodically)
    "[data-sncf]",      # Structured data snippets
    ".lEBKkf",          # Alternative snippet class
    "span.aCOpRe",      # Older snippet class
    "div.kb0PBd",       # Featured snippets
    "span[jsname]",     # Generic JS-rendered spans
]
_TITLE_SELECTOR = "h3"
_URL_SELECTOR = "cite"


# =============================================================================
# Public API
# =============================================================================

async def search_for_email(
    company_name: str,
    city: str,
    domain: str,
    supabase_client: Any,
) -> list[str]:
    """Search Google for email addresses related to a company.

    Runs up to 3 targeted queries and extracts email candidates from titles,
    snippets, and visible URLs. Does not verify emails — verification happens
    in email_verifier.py.

    Args:
        company_name: Company name string, e.g. 'Fysiotherapie de Vries'.
        city: City name, e.g. 'Amsterdam'.
        domain: Clean domain string, e.g. 'devries-fysiotherapie.nl'.
        supabase_client: Supabase client (rate limits + captcha state).

    Returns:
        Deduplicated list of candidate email strings. Empty list on any error.
    """
    # Check if Google Search is currently blocked due to a recent CAPTCHA
    if await is_google_search_blocked(supabase_client):
        logger.info("Google Search blocked (captcha cooldown) — skipping step 3")
        return []

    all_emails: set[str] = set()

    queries = _build_queries(company_name, city, domain)

    async with async_playwright() as playwright:
        browser, context = await new_browser_context(playwright)
        try:
            for i, query in enumerate(queries):
                # Rate limit: max 10/hour
                try:
                    await wait_for_token("google_search", supabase_client)
                except Exception:
                    logger.warning("Google Search rate limit exhausted")
                    break

                emails = await _run_single_query(query, context, supabase_client)

                if emails is None:
                    # None = CAPTCHA detected — stop immediately
                    logger.warning("CAPTCHA during Google Search query %d", i + 1)
                    await _store_captcha_block(supabase_client)
                    break

                all_emails.update(emails)

                # Delay between queries — 5-12 seconds
                if i < len(queries) - 1:
                    await random_delay(5, 12)

        except Exception as e:
            logger.warning("google_search_scraper unexpected error: %s", e)
        finally:
            try:
                await context.close()
                await browser.close()
            except Exception:
                pass

    # Filter obviously invalid candidates
    result = [e for e in all_emails if "@" in e and "." in e.split("@")[1]]
    logger.info(
        "Google Search done: company=%s found=%d emails",
        company_name, len(result),
    )
    return result


async def is_google_search_blocked(supabase_client: Any) -> bool:
    """Check whether Google Search is currently blocked due to a recent CAPTCHA.

    Reads the ``system_state`` table for an active block record.

    Args:
        supabase_client: Supabase client.

    Returns:
        True if blocked (should skip step 3), False if clear to proceed.
    """
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        response = (
            supabase_client.table("system_state")
            .select("value, expires_at")
            .eq("key", _CAPTCHA_BLOCK_KEY)
            .gte("expires_at", now_iso)
            .limit(1)
            .execute()
        )
        return bool(response.data)
    except Exception:
        return False


# =============================================================================
# Internal helpers
# =============================================================================

def _build_queries(company_name: str, city: str, domain: str) -> list[str]:
    """Build the three targeted search queries for email discovery.

    Args:
        company_name: Company name.
        city: City name.
        domain: Clean domain.

    Returns:
        List of up to 3 Google Search query strings.
    """
    queries = [
        f'"{company_name}" "{city}" email OR "e-mail" OR "@"',
        f'"{company_name}" "@{domain}"',
        f"site:{domain} contact",
    ]
    return queries


async def _run_single_query(
    query: str,
    context,
    supabase_client: Any,
) -> list[str] | None:
    """Execute a single Google Search query and extract email candidates.

    Args:
        query: Google Search query string.
        context: Active Playwright BrowserContext.
        supabase_client: Supabase client (for CAPTCHA state if needed).

    Returns:
        List of email strings found, or None if CAPTCHA detected.
    """
    encoded = quote_plus(query)
    url = f"{_GOOGLE_NL_SEARCH}?q={encoded}&hl=nl&num=10"

    try:
        page = await context.new_page()
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=15_000)
            await _handle_consent(page)

            # Check for CAPTCHA
            if await _is_captcha(page):
                return None

            # Wait for search results
            try:
                await page.wait_for_selector(_RESULT_CONTAINER, timeout=_RESULTS_TIMEOUT_MS)
            except Exception:
                logger.debug("Google Search results container not found for: %s", query[:60])
                return []

            await asyncio.sleep(0.8)  # Brief wait for lazy-loaded snippets

            # Extract text from all result components
            all_text_parts: list[str] = []

            # Titles
            title_els = await page.query_selector_all(_TITLE_SELECTOR)
            for el in title_els:
                try:
                    all_text_parts.append(await el.inner_text())
                except Exception:
                    pass

            # Snippets — try each selector
            for sel in _SNIPPET_SELECTORS:
                snippet_els = await page.query_selector_all(sel)
                for el in snippet_els:
                    try:
                        all_text_parts.append(await el.inner_text())
                    except Exception:
                        pass

            # Displayed URLs
            url_els = await page.query_selector_all(_URL_SELECTOR)
            for el in url_els:
                try:
                    all_text_parts.append(await el.inner_text())
                except Exception:
                    pass

            # Also scan the full page text as a catch-all
            try:
                full_text = await page.inner_text("body")
                all_text_parts.append(full_text)
            except Exception:
                pass

            combined_text = " ".join(all_text_parts)
            emails = _extract_emails_from_text(combined_text)

            await random_mouse_movement(page)
            return emails

        finally:
            await page.close()

    except Exception as e:
        logger.warning("_run_single_query failed for query=%s: %s", query[:60], e)
        return []


def _extract_emails_from_text(text: str) -> list[str]:
    """Extract and deduplicate email addresses from plain text.

    Args:
        text: Raw text string (from page titles + snippets).

    Returns:
        Deduplicated list of email strings.
    """
    found: set[str] = set()
    for match in _EMAIL_REGEX.finditer(text):
        email = match.group(0).lower().strip(".,;:\"'()")
        if "@" in email and "." in email.split("@")[1]:
            found.add(email)
    return list(found)


async def _handle_consent(page) -> None:
    """Accept Google's consent/cookie screen if present."""
    for sel in [
        'button[aria-label="Alles accepteren"]',
        'button[aria-label="Accept all"]',
        '#L2AGLb',
        'form[action*="consent"] button',
    ]:
        try:
            btn = await page.query_selector(sel)
            if btn:
                await btn.click()
                await asyncio.sleep(0.8)
                return
        except Exception:
            continue


async def _is_captcha(page) -> bool:
    """Detect if the current Google page is a CAPTCHA challenge."""
    url = page.url.lower()
    if any(ind in url for ind in _CAPTCHA_INDICATORS):
        return True
    try:
        body = (await page.inner_text("body")).lower()
        return any(ind in body for ind in _CAPTCHA_INDICATORS)
    except Exception:
        return False


async def _store_captcha_block(supabase_client: Any) -> None:
    """Store a 2-hour CAPTCHA block for Google Search in system_state.

    Args:
        supabase_client: Supabase client.
    """
    expires = datetime.now(timezone.utc) + timedelta(hours=_CAPTCHA_BLOCK_HOURS)
    try:
        supabase_client.table("system_state").upsert({
            "key": _CAPTCHA_BLOCK_KEY,
            "value": "blocked",
            "expires_at": expires.isoformat(),
        }).execute()
        logger.warning(
            "Google Search CAPTCHA block stored — will resume at %s",
            expires.isoformat(),
        )
    except Exception as e:
        logger.error("Failed to store captcha block state: %s", e)
