"""
scrapers/website_scraper.py — NL-optimised website scraper for Heatr.

This is email discovery step 1 in the 4-step waterfall. It visits a company's
website, extracts emails, CMS signals, tracking tools, contact names, and
conversion elements that feed both the email waterfall and the website
intelligence pipeline.

Two-layer fetch strategy:
  Layer 1 — httpx (fast, low footprint, no JS execution)
  Layer 2 — Playwright (full browser, for JS-heavy / SPA sites)

Layer 2 is triggered when the httpx response body is empty, too short (<500
chars), or contains no readable paragraph text — which catches React/Vue SPAs
that render everything client-side.

Max 3 pages per domain (homepage + up to 2 contact subpages).
Respects GDPR_MODE=strict: only stores gdpr_safe=True emails.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from playwright.async_api import async_playwright, BrowserContext

from utils.playwright_helpers import (
    new_browser_context,
    random_delay,
    DUTCH_CONTACT_PAGE_PATTERNS,
    extract_dutch_name,
    classify_email_gdpr,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PAGE_TIMEOUT_MS = 15_000
_MIN_MEANINGFUL_HTML_LEN = 500
_MAX_PAGES_PER_DOMAIN = 3

# Email regex — broad capture, GDPR filter applied afterwards
_EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# False-positive patterns in email-like strings (image filenames, CSS, etc.)
_EMAIL_FALSE_POSITIVE_PATTERNS = [
    re.compile(r"\.(png|jpg|jpeg|gif|svg|webp|woff|ttf|css|js)@", re.IGNORECASE),
    re.compile(r"@\d+x\d+"),           # "@2x" retina image suffixes
    re.compile(r"example\.(com|nl|org)", re.IGNORECASE),
    re.compile(r"sentry\.io"),
    re.compile(r"schema\.org"),
    re.compile(r"w3\.org"),
    re.compile(r"placeholder"),
]

# CMS fingerprints: (pattern_in_html_or_header, cms_name)
_CMS_FINGERPRINTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"/wp-content/|/wp-includes/|wp-json", re.IGNORECASE), "WordPress"),
    (re.compile(r"cdn\.shopify\.com|myshopify\.com", re.IGNORECASE), "Shopify"),
    (re.compile(r"webflow\.io|data-wf-", re.IGNORECASE), "Webflow"),
    (re.compile(r"/sites/default/files/|Drupal\.settings", re.IGNORECASE), "Drupal"),
    (re.compile(r"/components/com_|Joomla!", re.IGNORECASE), "Joomla"),
    (re.compile(r"squarespace\.com|squarespace-cdn", re.IGNORECASE), "Squarespace"),
    (re.compile(r"wix\.com|wixsite\.com|wix-bolt", re.IGNORECASE), "Wix"),
    (re.compile(r"framer\.com|framer-motion", re.IGNORECASE), "Framer"),
    (re.compile(r"ghost\.org|content/ghost/", re.IGNORECASE), "Ghost"),
    (re.compile(r"typo3", re.IGNORECASE), "TYPO3"),
    (re.compile(r"sitefinity", re.IGNORECASE), "Sitefinity"),
]

# Tracking tool fingerprints: (pattern, tool_name)
_TRACKING_FINGERPRINTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"gtag\.js|analytics\.js|ga\.js|google-analytics\.com", re.IGNORECASE), "Google Analytics"),
    (re.compile(r"googletagmanager\.com/gtm\.js", re.IGNORECASE), "Google Tag Manager"),
    (re.compile(r"fbq\(|connect\.facebook\.net|facebook-jssdk", re.IGNORECASE), "Facebook Pixel"),
    (re.compile(r"hs-scripts\.com|hubspot\.com/hs-web-interactives", re.IGNORECASE), "HubSpot"),
    (re.compile(r"snap\.licdn\.com|linkedin\.com/insight", re.IGNORECASE), "LinkedIn Insight"),
    (re.compile(r"hotjar\.com|_hjSettings", re.IGNORECASE), "Hotjar"),
    (re.compile(r"clarity\.ms|microsoft\.com/clarity", re.IGNORECASE), "Microsoft Clarity"),
    (re.compile(r"intercomcdn\.com|intercom\.io", re.IGNORECASE), "Intercom"),
]

# Online booking platform fingerprints
_BOOKING_FINGERPRINTS = re.compile(
    r"calendly\.com|doctolib\.nl|acuityscheduling\.com|planity\.com"
    r"|janeapp\.com|timify\.com|appointy\.com|setmore\.com"
    r"|afspraak\.nl|online.afspraken|reserveer|booksy\.com",
    re.IGNORECASE,
)

# Cookie consent platform fingerprints
_COOKIE_FINGERPRINTS = re.compile(
    r"cookiebot|cookiebar|cookie-consent|cookie-notice|cookiehub"
    r"|usercentrics|onetrust|axeptio|klaro",
    re.IGNORECASE,
)

# Dutch name indicators on contact/team pages
_NAME_CONTEXT_PATTERNS = re.compile(
    r"(?:behandelaar|therapeut|eigenaar|oprichter|directeur|dokter|dr\.|drs\.|"
    r"dhr\.|mevr\.|mr\.|ir\.|ing\.|bsc|msc|coach|praktijkhouder)\s*[:\-]?\s*"
    r"([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\.\-\']{3,40})",
    re.IGNORECASE,
)


# =============================================================================
# Public API
# =============================================================================

async def scrape_website(
    domain: str,
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Scrape a company website to extract emails and enrichment signals.

    Entry point for email waterfall step 1. Tries httpx first, falls back to
    Playwright for JS-heavy sites. Visits homepage plus up to 2 contact
    subpages. GDPR-filters all found emails. Stores raw results in
    ``enrichment_data`` table.

    Args:
        domain: Clean domain string, e.g. 'example.nl'. No protocol prefix.
        lead_id: UUID of the lead row in Supabase (for enrichment_data FK).
        workspace_id: Workspace slug, scopes all writes.
        supabase_client: Initialised supabase-py client.

    Returns:
        Dict with keys:
          emails (list[str]), contact_name (str|None), cms (str),
          tracking_tools (list[str]), has_instagram (bool),
          has_online_booking (bool), has_whatsapp (bool),
          has_cookie_banner (bool), phone (str|None).
    """
    result: dict = {
        "emails": [],
        "contact_name": None,
        "cms": "unknown",
        "tracking_tools": [],
        "has_instagram": False,
        "has_online_booking": False,
        "has_whatsapp": False,
        "has_cookie_banner": False,
        "phone": None,
    }

    if not domain:
        return result

    homepage_url = f"https://{domain}"
    gdpr_mode = os.getenv("GDPR_MODE", "strict")
    pages_visited = 0
    all_emails: list[str] = []
    contact_page_urls: list[str] = []

    # -------------------------------------------------------------------------
    # Layer 1: httpx (fast path)
    # -------------------------------------------------------------------------
    html, headers = await fetch_page_httpx(homepage_url)
    pages_visited += 1

    use_playwright = _should_use_playwright(html)

    if not use_playwright:
        # Extract everything from httpx response
        _merge_extractions(result, html, headers, homepage_url)
        all_emails.extend(extract_emails_from_html(html))
        contact_page_urls = find_contact_page_links(html, homepage_url)

        # Fetch up to 2 contact subpages via httpx
        for contact_url in contact_page_urls[:2]:
            if pages_visited >= _MAX_PAGES_PER_DOMAIN:
                break
            try:
                sub_html, sub_headers = await fetch_page_httpx(contact_url)
                pages_visited += 1
                _merge_extractions(result, sub_html, sub_headers, contact_url)
                all_emails.extend(extract_emails_from_html(sub_html))
                await random_delay(0.5, 1.5)
            except Exception as e:
                logger.warning("httpx subpage fetch failed %s: %s", contact_url, e)

    # -------------------------------------------------------------------------
    # Layer 2: Playwright (JS-heavy / SPA fallback)
    # -------------------------------------------------------------------------
    if use_playwright:
        async with async_playwright() as playwright:
            browser, context = await new_browser_context(playwright)
            try:
                pw_html = await fetch_page_playwright(homepage_url, context)
                pages_visited += 1
                _merge_extractions(result, pw_html, {}, homepage_url)
                all_emails.extend(extract_emails_from_html(pw_html))
                contact_page_urls = find_contact_page_links(pw_html, homepage_url)

                # Visit contact subpages
                for contact_url in contact_page_urls[:2]:
                    if pages_visited >= _MAX_PAGES_PER_DOMAIN:
                        break
                    try:
                        sub_html = await fetch_page_playwright(contact_url, context)
                        pages_visited += 1
                        _merge_extractions(result, sub_html, {}, contact_url)
                        all_emails.extend(extract_emails_from_html(sub_html))
                        await random_delay(0.5, 2.0)
                    except Exception as e:
                        logger.warning("Playwright subpage failed %s: %s", contact_url, e)
            finally:
                try:
                    await context.close()
                    await browser.close()
                except Exception:
                    pass

    # -------------------------------------------------------------------------
    # GDPR filter — only keep safe emails in strict mode
    # -------------------------------------------------------------------------
    seen: set[str] = set()
    safe_emails: list[str] = []
    for email in all_emails:
        email_lower = email.lower()
        if email_lower in seen:
            continue
        seen.add(email_lower)
        email_type, gdpr_safe = classify_email_gdpr(email_lower, mode=gdpr_mode)
        if gdpr_mode == "strict" and not gdpr_safe:
            continue
        safe_emails.append(email_lower)

    result["emails"] = safe_emails

    # -------------------------------------------------------------------------
    # Store raw result in enrichment_data table
    # -------------------------------------------------------------------------
    await _store_enrichment_result(
        lead_id=lead_id,
        workspace_id=workspace_id,
        step=1,
        source="website",
        emails_found=safe_emails,
        raw_result={
            "domain": domain,
            "pages_visited": pages_visited,
            "used_playwright": use_playwright,
            "cms": result["cms"],
            "tracking_tools": result["tracking_tools"],
            "contact_name_raw": result.get("contact_name"),
        },
        succeeded=bool(safe_emails),
        supabase_client=supabase_client,
    )

    logger.info(
        "website_scraper done: domain=%s emails=%d cms=%s playwright=%s",
        domain, len(safe_emails), result["cms"], use_playwright,
    )
    return result


# =============================================================================
# Fetch helpers
# =============================================================================

async def fetch_page_httpx(url: str) -> tuple[str, dict]:
    """Fetch a page via httpx and return (html_content, response_headers).

    Uses a browser-like User-Agent and Accept-Language header to avoid
    being blocked by basic bot filters. Timeout: 15 seconds.

    Args:
        url: Full URL to fetch, including protocol.

    Returns:
        Tuple of (html_content: str, headers: dict).
        Returns ("", {}) on any error so callers can fall back to Playwright.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=15.0,
            headers=headers,
        ) as client:
            response = await client.get(url)
            return response.text, dict(response.headers)
    except Exception as e:
        logger.debug("httpx fetch failed for %s: %s", url, e)
        return "", {}


async def fetch_page_playwright(url: str, context: BrowserContext) -> str:
    """Fetch a page via Playwright and return the fully-rendered HTML.

    Waits for ``networkidle`` so JS-rendered content is visible in the DOM.
    Timeout: 15 seconds. Returns empty string on error.

    Args:
        url: Full URL to fetch.
        context: Existing Playwright BrowserContext (reused across subpages).

    Returns:
        Full HTML string of the rendered page, or "" on error/timeout.
    """
    try:
        page = await context.new_page()
        try:
            await page.goto(
                url,
                wait_until="networkidle",
                timeout=_PAGE_TIMEOUT_MS,
            )
            html = await page.content()
            return html
        except Exception as e:
            logger.debug("Playwright fetch failed for %s: %s", url, e)
            # Try to get whatever content loaded before the timeout
            try:
                return await page.content()
            except Exception:
                return ""
        finally:
            await page.close()
    except Exception as e:
        logger.debug("Playwright page open failed for %s: %s", url, e)
        return ""


# =============================================================================
# Extraction helpers
# =============================================================================

def extract_emails_from_html(html: str) -> list[str]:
    """Extract email addresses from raw HTML, filtering obvious false positives.

    Searches both mailto: href values and raw text. Deduplicates results.

    Args:
        html: Raw HTML string.

    Returns:
        Deduplicated list of candidate email strings (not yet GDPR-filtered).
    """
    if not html:
        return []

    # Extract mailto: hrefs first (highest confidence)
    mailto_emails: list[str] = []
    mailto_pattern = re.compile(r'mailto:([^\s"\'?&<>]+)', re.IGNORECASE)
    for match in mailto_pattern.finditer(html):
        candidate = match.group(1).strip().lower()
        if "@" in candidate and not _is_false_positive_email(candidate):
            mailto_emails.append(candidate)

    # Regex scan over full text (catches obfuscated addresses and plain text)
    text_emails: list[str] = []
    for match in _EMAIL_REGEX.finditer(html):
        candidate = match.group(0).lower()
        if not _is_false_positive_email(candidate):
            text_emails.append(candidate)

    # Merge: mailto results take priority, then text results
    seen: set[str] = set()
    result: list[str] = []
    for email in mailto_emails + text_emails:
        if email not in seen:
            seen.add(email)
            result.append(email)

    return result


def detect_cms(html: str, headers: dict) -> str:
    """Detect the CMS platform from HTML content and HTTP response headers.

    Checks X-Powered-By and other headers first (more reliable), then HTML.

    Args:
        html: Raw HTML string.
        headers: HTTP response headers dict (case-insensitive keys work).

    Returns:
        CMS name string (e.g. 'WordPress') or 'unknown'.
    """
    # Check headers
    for header_key in ("x-powered-by", "X-Powered-By"):
        header_val = headers.get(header_key, "")
        if "wordpress" in header_val.lower():
            return "WordPress"
        if "drupal" in header_val.lower():
            return "Drupal"

    if not html:
        return "unknown"

    for pattern, cms_name in _CMS_FINGERPRINTS:
        if pattern.search(html):
            return cms_name

    return "unknown"


def detect_tracking_tools(html: str) -> list[str]:
    """Detect analytics and tracking platforms in HTML source.

    Args:
        html: Raw HTML string.

    Returns:
        List of detected tool names (e.g. ['Google Analytics', 'HubSpot']).
    """
    if not html:
        return []
    found: list[str] = []
    for pattern, tool_name in _TRACKING_FINGERPRINTS:
        if pattern.search(html):
            found.append(tool_name)
    return found


def find_contact_page_links(html: str, base_url: str) -> list[str]:
    """Find internal contact/team page URLs using Dutch URL patterns.

    Checks all ``<a href>`` values against ``DUTCH_CONTACT_PAGE_PATTERNS``
    from playwright_helpers. Returns absolute URLs.

    Args:
        html: Raw HTML string of the homepage.
        base_url: Base URL of the site (used to resolve relative hrefs).

    Returns:
        List of absolute URLs matching Dutch contact page patterns.
        Ordered by pattern priority (contact before team, etc.).
    """
    if not html:
        return []

    # Extract all internal hrefs
    href_pattern = re.compile(r'href=["\']([^"\'#?]+)["\']', re.IGNORECASE)
    all_hrefs: list[str] = href_pattern.findall(html)

    found_urls: list[str] = []
    seen_paths: set[str] = set()

    # Check in DUTCH_CONTACT_PAGE_PATTERNS priority order
    for pattern in DUTCH_CONTACT_PAGE_PATTERNS:
        for href in all_hrefs:
            href_lower = href.lower().rstrip("/")
            if href_lower.endswith(pattern) or f"{pattern}/" in href_lower:
                # Make absolute URL
                if href.startswith("http"):
                    absolute = href
                else:
                    absolute = urljoin(base_url, href)

                path = urlparse(absolute).path.rstrip("/")
                if path not in seen_paths:
                    seen_paths.add(path)
                    found_urls.append(absolute)
                break  # One URL per pattern is enough

    return found_urls


# =============================================================================
# Internal helpers
# =============================================================================

def _should_use_playwright(html: str) -> bool:
    """Decide whether Playwright is needed based on httpx response quality.

    Triggers Playwright when:
    - HTML is empty or very short (<500 chars)
    - HTML has almost no text content (mostly scripts/markup)
    - Page contains SPA root div with no meaningful children

    Args:
        html: HTML string from httpx.

    Returns:
        True if Playwright should be used.
    """
    if not html or len(html) < _MIN_MEANINGFUL_HTML_LEN:
        return True

    # Strip tags and measure text content ratio
    text_only = re.sub(r"<[^>]+>", " ", html)
    text_only = re.sub(r"\s+", " ", text_only).strip()

    # Less than 100 chars of readable text = SPA that needs JS
    if len(text_only) < 100:
        return True

    # React/Vue/Angular SPA indicator: root div with nothing inside
    if re.search(r'<div\s+id=["\'](?:app|root|__nuxt|__next)["\'][^>]*>\s*</div>', html):
        return True

    return False


def _merge_extractions(
    result: dict,
    html: str,
    headers: dict,
    page_url: str,
) -> None:
    """Extract and merge all signals from a page into the result dict.

    Mutates ``result`` in place. Merges rather than overwrites so that
    data from multiple pages accumulates.

    Args:
        result: Current result dict (mutated in place).
        html: Raw HTML of the page.
        headers: HTTP response headers.
        page_url: URL of the page (for context).
    """
    if not html:
        return

    # CMS — update only if still unknown
    if result["cms"] == "unknown":
        result["cms"] = detect_cms(html, headers)

    # Tracking tools — merge list
    new_tools = detect_tracking_tools(html)
    for tool in new_tools:
        if tool not in result["tracking_tools"]:
            result["tracking_tools"].append(tool)

    # Instagram
    if not result["has_instagram"]:
        if re.search(r'instagram\.com', html, re.IGNORECASE):
            result["has_instagram"] = True

    # WhatsApp
    if not result["has_whatsapp"]:
        if re.search(r'wa\.me|api\.whatsapp\.com|whatsapp\.com/send', html, re.IGNORECASE):
            result["has_whatsapp"] = True

    # Online booking
    if not result["has_online_booking"]:
        if _BOOKING_FINGERPRINTS.search(html):
            result["has_online_booking"] = True

    # Cookie banner
    if not result["has_cookie_banner"]:
        if _COOKIE_FINGERPRINTS.search(html):
            result["has_cookie_banner"] = True

    # Phone number — tel: links
    if not result["phone"]:
        tel_match = re.search(r'href=["\']tel:([^"\']+)["\']', html, re.IGNORECASE)
        if tel_match:
            result["phone"] = tel_match.group(1).strip()

    # Contact name — extract from Dutch practitioner context patterns
    if not result["contact_name"]:
        for match in _NAME_CONTEXT_PATTERNS.finditer(html):
            raw_name = match.group(1).strip()
            # Filter out obviously non-name strings (too short, has digits)
            if len(raw_name) >= 5 and not re.search(r"\d", raw_name):
                parsed = extract_dutch_name(raw_name)
                if parsed.get("first_name") and parsed.get("last_name"):
                    result["contact_name"] = raw_name
                    break


def _is_false_positive_email(email: str) -> bool:
    """Check if an email-like string is a known false positive.

    Args:
        email: Candidate email string.

    Returns:
        True if this is a false positive (should be discarded).
    """
    for pattern in _EMAIL_FALSE_POSITIVE_PATTERNS:
        if pattern.search(email):
            return True
    # Filter very long locals (>64 chars is invalid per RFC)
    local = email.split("@")[0]
    if len(local) > 64:
        return True
    return False


async def _store_enrichment_result(
    lead_id: str,
    workspace_id: str,
    step: int,
    source: str,
    emails_found: list[str],
    raw_result: dict,
    succeeded: bool,
    supabase_client: Any,
) -> None:
    """Persist enrichment attempt to the enrichment_data table.

    Args:
        lead_id: Lead UUID.
        workspace_id: Workspace slug.
        step: Waterfall step number (1 for website scraper).
        source: Source label (e.g. 'website').
        emails_found: List of GDPR-safe emails found.
        raw_result: Full raw extraction data dict.
        succeeded: True if at least one email was found.
        supabase_client: Supabase client.
    """
    record = {
        "workspace_id": workspace_id,
        "lead_id": lead_id,
        "enrichment_step": step,
        "source": source,
        "email_candidate": emails_found[0] if emails_found else None,
        "email_verified": False,  # verification happens in email_verifier.py
        "email_status": None,
        "catch_all": False,
        "mx_records": [],
        "raw_result": raw_result,
        "succeeded": succeeded,
    }
    try:
        supabase_client.table("enrichment_data").insert(record).execute()
    except Exception as e:
        logger.warning("Failed to store enrichment_data for lead %s: %s", lead_id, e)
