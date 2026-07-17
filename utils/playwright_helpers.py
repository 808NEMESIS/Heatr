"""
utils/playwright_helpers.py — Shared Playwright browser utilities for all Heatr scrapers.

All anti-detection measures are applied here. Import and use these helpers
in every scraper to stay consistent with the anti-detection rules in CLAUDE.md.

Anti-detection rules applied:
- Rotate realistic Chrome user agents (Mac + Windows)
- Dutch locale (nl-NL) + Europe/Amsterdam timezone
- Accept-Language: nl-NL,nl;q=0.9,en;q=0.8
- Random mouse movements before clicks
- Random delays between SCRAPE_DELAY_MIN and SCRAPE_DELAY_MAX
- Randomised viewport sizes
- Optional proxy routing when PROXY_ENABLED=true
"""

from __future__ import annotations

import asyncio
import os
import random
import re
from typing import Optional
from urllib.parse import urlparse

from playwright.async_api import Browser, BrowserContext, Page, Playwright


# =============================================================================
# User Agents — Realistic Chrome on Mac and Windows (2024)
# =============================================================================

USER_AGENTS: list[str] = [
    # Chrome 124 — macOS Sonoma
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Chrome 123 — macOS Ventura
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_6) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.6312.122 Safari/537.36"
    ),
    # Chrome 124 — Windows 11
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Chrome 123 — Windows 10
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.6312.105 Safari/537.36"
    ),
    # Chrome 122 — Windows 11
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.6261.128 Safari/537.36"
    ),
    # Chrome 121 — macOS Monterey
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_7_4) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.6167.184 Safari/537.36"
    ),
]

# =============================================================================
# Dutch contact page URL patterns (checked in order during website scraping)
# =============================================================================

DUTCH_CONTACT_PAGE_PATTERNS: list[str] = [
    "/contact",
    "/contacteer-ons",
    "/contactpagina",
    "/contact-us",
    "/bereikbaarheid",
    "/over-ons",
    "/over-mij",
    "/team",
    "/praktijk",
    "/locatie",
    "/afspraak",
    "/afspreken",
    "/maak-een-afspraak",
    "/intake",
    "/kennismaking",
]

# =============================================================================
# Dutch tussenvoegsels (name particles) — complete list
# Used when parsing scraped contact names into structured name fields.
# =============================================================================

DUTCH_TUSSENVOEGSELS: list[str] = [
    "van den",
    "van der",
    "van de",
    "van 't",
    "in 't",
    "op 't",
    "van",
    "de",
    "den",
    "der",
    "ter",
    "ten",
    "te",
    "het",
    "'t",
]
# Sorted longest-first so the regex matches greedily (e.g. "van den" before "van")
DUTCH_TUSSENVOEGSELS_SORTED: list[str] = sorted(
    DUTCH_TUSSENVOEGSELS, key=len, reverse=True
)


# =============================================================================
# Async helpers
# =============================================================================

async def random_delay(min_s: Optional[float] = None, max_s: Optional[float] = None) -> None:
    """Sleep for a random duration between min_s and max_s seconds.

    If min_s / max_s are not provided, reads SCRAPE_DELAY_MIN and
    SCRAPE_DELAY_MAX from the environment (defaults: 2 and 6).

    Args:
        min_s: Minimum sleep duration in seconds.
        max_s: Maximum sleep duration in seconds.
    """
    if min_s is None:
        min_s = float(os.getenv("SCRAPE_DELAY_MIN", "2"))
    if max_s is None:
        max_s = float(os.getenv("SCRAPE_DELAY_MAX", "6"))
    duration = random.uniform(min_s, max_s)
    await asyncio.sleep(duration)


async def random_mouse_movement(page: Page) -> None:
    """Simulate realistic human mouse movement across the viewport.

    Moves the mouse in a series of small curved steps to avoid straight-line
    patterns that bot-detection systems flag. Called before any click.

    Args:
        page: Active Playwright Page instance.
    """
    viewport = page.viewport_size or {"width": 1366, "height": 768}
    width = viewport["width"]
    height = viewport["height"]

    # Generate 3-6 intermediate waypoints across the viewport
    steps = random.randint(3, 6)
    for _ in range(steps):
        x = random.randint(100, width - 100)
        y = random.randint(100, height - 100)
        # steps= parameter makes the movement curved (Playwright interpolates)
        await page.mouse.move(x, y, steps=random.randint(8, 20))
        # Micro-pause between movements (humans don't move at constant speed)
        await asyncio.sleep(random.uniform(0.05, 0.25))


async def new_browser_context(
    playwright: Playwright,
    proxy: Optional[dict] = None,
    capture_network: bool = False,
) -> tuple[Browser, BrowserContext]:
    """Create a new Playwright browser + context with full anti-detection settings.

    Applies Dutch locale, realistic viewport, random user agent, and optional proxy.
    Always use this factory instead of calling playwright.chromium.launch() directly.

    Args:
        playwright: Active Playwright instance (from async_playwright().__aenter__).
        proxy: Optional proxy dict, e.g. {"server": "http://user:pass@host:port"}.
               Automatically read from env if PROXY_ENABLED=true and proxy is None.

    Returns:
        Tuple of (Browser, BrowserContext). Caller is responsible for closing both.
    """
    headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() != "false"

    # Resolve proxy from environment if not explicitly passed
    if proxy is None and os.getenv("PROXY_ENABLED", "false").lower() == "true":
        proxy_url = os.getenv("PROXY_URL")
        if proxy_url:
            proxy = {"server": proxy_url}

    browser = await playwright.chromium.launch(
        headless=headless,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ],
    )

    # Randomise viewport to avoid fingerprinting
    viewport = random.choice([
        {"width": 1366, "height": 768},   # Most common laptop resolution
        {"width": 1440, "height": 900},   # MacBook 13"
    ])

    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        locale="nl-NL",
        timezone_id="Europe/Amsterdam",
        viewport=viewport,
        extra_http_headers={
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
        },
        proxy=proxy,
        java_script_enabled=True,
        ignore_https_errors=False,
    )

    # Mask navigator.webdriver property (stealth)
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        Object.defineProperty(navigator, 'languages', {get: () => ['nl-NL', 'nl', 'en']});
    """)

    # Optionele netwerk-capture: listeners op de context, log als attribuut.
    # De caller leest `context.heatr_network_log` (lijst van ruwe entries met een
    # monotone t_request/t_response; timing t.o.v. navigatiestart + fase bepaalt
    # de caller, zie website_intelligence/site_capture.py).
    if capture_network:
        import time as _time
        log: list[dict] = []
        reqmap: dict = {}

        def _on_request(req) -> None:
            entry = {
                "url": req.url,
                "method": req.method,
                "resource_type": req.resource_type,
                "t_request": _time.monotonic(),
                "status": None,
                "failed": False,
            }
            reqmap[req] = entry
            log.append(entry)

        def _on_response(resp) -> None:
            e = reqmap.get(resp.request)
            if e is not None:
                e["status"] = resp.status
                e["t_response"] = _time.monotonic()

        def _on_failed(req) -> None:
            e = reqmap.get(req)
            if e is not None:
                e["failed"] = True

        context.on("request", _on_request)
        context.on("response", _on_response)
        context.on("requestfailed", _on_failed)
        context.heatr_network_log = log  # type: ignore[attr-defined]

    return browser, context


async def mobile_context(
    browser: Browser,
    playwright: Playwright,
    device_name: str = "iPhone 13",
) -> BrowserContext:
    """Maak een MOBIELE context op een bestaande browser via een device-descriptor.

    Gebruikt de Playwright-device (juiste user agent, device_scale_factor,
    is_mobile, has_touch) — niet alleen een viewport, anders serveert de site de
    desktopvariant en meet je niets. Valt terug op een handmatige 390x844-config
    als de descriptor ontbreekt.
    """
    try:
        device = dict(playwright.devices[device_name])
        device.pop("default_browser_type", None)
    except Exception:
        device = {
            "viewport": {"width": 390, "height": 844},
            "device_scale_factor": 3,
            "is_mobile": True,
            "has_touch": True,
            "user_agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 "
                "Mobile/15E148 Safari/604.1"
            ),
        }
    return await browser.new_context(
        locale="nl-NL", timezone_id="Europe/Amsterdam", **device,
    )


_WEBP_MAX_DIM = 16383  # harde WebP-limiet; full-page mobiel (DSF 3) gaat er anders overheen


def encode_webp(image_bytes: bytes, quality: int = 80) -> bytes:
    """Converteer PNG/JPEG-bytes naar WebP — Playwright's screenshot doet geen WebP.

    Bij duizend leads scheelt WebP q80 t.o.v. PNG gigabytes in Supabase Storage.
    Een full-page screenshot kan langer zijn dan de WebP-max (16383px); dan wordt
    de afbeelding proportioneel verkleind zodat de langste zijde past.
    """
    from io import BytesIO
    from PIL import Image
    with Image.open(BytesIO(image_bytes)) as img:
        if img.mode not in ("RGB", "RGBA"):
            img = img.convert("RGB")
        w, h = img.size
        longest = max(w, h)
        if longest > _WEBP_MAX_DIM:
            scale = _WEBP_MAX_DIM / longest
            img = img.resize((max(1, int(w * scale)), max(1, int(h * scale))))
        out = BytesIO()
        img.save(out, format="WEBP", quality=quality, method=6)
        return out.getvalue()


# =============================================================================
# Synchronous helpers
# =============================================================================

def extract_domain(url: str) -> str:
    """Extract a clean domain from any URL format.

    Handles: no protocol, trailing slashes, subdomains, query strings.

    Args:
        url: Raw URL string in any format.

    Returns:
        Clean domain string (e.g. 'example.nl'), or empty string if unparseable.

    Examples:
        >>> extract_domain("https://www.example.nl/contact?foo=bar")
        'example.nl'
        >>> extract_domain("example.nl/")
        'example.nl'
        >>> extract_domain("http://sub.example.nl")
        'sub.example.nl'
    """
    if not url:
        return ""

    url = url.strip()

    # Add protocol if missing so urlparse works correctly
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    try:
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
    except Exception:
        return ""

    # Strip leading www.
    if hostname.startswith("www."):
        hostname = hostname[4:]

    return hostname.lower()


def extract_city_from_address(address: str) -> str:
    """Extract a city name from a Dutch or Belgian address string.

    Handles common Dutch address formats:
    - "Straatnaam 12, 1234 AB Amsterdam"
    - "Straatnaam 12A 1234AB Amsterdam"
    - "Postbus 100, 1234 AB Amsterdam"

    Args:
        address: Raw address string as scraped from website or KvK.

    Returns:
        City name string, or empty string if not extractable.
    """
    if not address:
        return ""

    address = address.strip()

    # Pattern: Dutch postcode (4 digits + optional space + 2 letters) followed by city
    # e.g. "1234 AB Amsterdam" or "1234AB Amsterdam"
    postcode_city_pattern = re.compile(
        r"\b\d{4}\s*[A-Z]{2}\s+([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-]*?)(?:\s*[,\n]|$)",
        re.IGNORECASE,
    )
    match = postcode_city_pattern.search(address)
    if match:
        return match.group(1).strip().title()

    # Belgian postcode pattern: 4 digits (no letter suffix)
    be_pattern = re.compile(
        r"\b\d{4}\s+([A-Za-zÀ-ÿ][A-Za-zÀ-ÿ\s\-]*?)(?:\s*[,\n]|$)",
        re.IGNORECASE,
    )
    match = be_pattern.search(address)
    if match:
        candidate = match.group(1).strip()
        # Avoid matching street names that accidentally follow a house number range
        if len(candidate) >= 3 and not candidate[0].isdigit():
            return candidate.title()

    # Last resort: take the last comma-separated segment
    parts = [p.strip() for p in address.split(",")]
    if parts:
        last = parts[-1].strip()
        # Postcode at start of last segment → city is after it
        m = re.match(r"^\d{4}\s*[A-Z]{0,2}\s+(.+)$", last, re.IGNORECASE)
        if m:
            return m.group(1).strip().title()
        # If last segment looks like a city (alphabetic, not too short)
        if len(last) >= 3 and re.match(r"^[A-Za-zÀ-ÿ\s\-]+$", last):
            return last.title()

    return ""


def classify_email_gdpr(
    email: str,
    mode: str = "strict",
) -> tuple[str, bool]:
    """Classify an email address for GDPR compliance.

    Email types:
    - 'role'     — generic business address (info@, contact@, praktijk@, etc.)
    - 'personal' — personal address (firstname.lastname@ or free email domain)
    - 'unknown'  — cannot be classified

    In strict mode: personal emails are gdpr_safe=False.
    In relaxed mode: personal emails are gdpr_safe=True but flagged.

    Args:
        email: Email address string to classify.
        mode: 'strict' (default) or 'relaxed'.

    Returns:
        Tuple of (email_type: str, gdpr_safe: bool).
    """
    if not email or "@" not in email:
        return ("unknown", False)

    email = email.lower().strip()
    local, domain = email.split("@", 1)

    # --- Personal free email domains (always GDPR-unsafe) --------------------
    personal_domains = {
        "gmail.com", "hotmail.com", "hotmail.nl", "outlook.com", "outlook.nl",
        "yahoo.com", "yahoo.nl", "icloud.com", "live.nl", "live.com",
        "ziggo.nl", "kpnmail.nl", "xs4all.nl", "planet.nl",
    }
    if domain in personal_domains:
        return ("personal", False)

    # --- Role email prefixes -------------------------------------------------
    role_prefixes = {
        "info", "contact", "hallo", "hello", "praktijk", "kliniek",
        "receptie", "administratie", "admin", "office", "mail",
        "post", "boekhouding", "planning", "afspraken", "intake",
        "behandeling", "zorg", "team", "service", "support",
        "algemeen", "secretariaat", "bureau",
    }
    if local in role_prefixes:
        return ("role", True)

    # --- firstname.lastname@ pattern detection --------------------------------
    # Matches: jan.smit@ | j.smit@ | jan_smit@ | jansmit@ (heuristic)
    personal_pattern = re.compile(
        r"^[a-z]{2,}\.[a-z]{2,}$|^[a-z]\.[a-z]{2,}$|^[a-z]{2,}_[a-z]{2,}$"
    )
    if personal_pattern.match(local):
        if mode == "strict":
            return ("personal", False)
        else:
            return ("personal", True)

    # --- Cannot determine —treat as unknown ----------------------------------
    return ("unknown", True)


def extract_dutch_name(text: str) -> dict[str, str]:
    """Extract first_name, tussenvoegsel, and last_name from a Dutch name string.

    Handles tussenvoegsels (van, de, van den, etc.) correctly by matching
    longest-first.

    Args:
        text: Raw name string, e.g. "Jan van den Berg" or "Marieke de Vries".

    Returns:
        Dict with keys: 'first_name', 'tussenvoegsel', 'last_name'.
        Any unresolvable part returns an empty string.

    Examples:
        >>> extract_dutch_name("Jan van den Berg")
        {'first_name': 'Jan', 'tussenvoegsel': 'van den', 'last_name': 'Berg'}
        >>> extract_dutch_name("Marieke de Vries")
        {'first_name': 'Marieke', 'tussenvoegsel': 'de', 'last_name': 'Vries'}
        >>> extract_dutch_name("Pieter Jansen")
        {'first_name': 'Pieter', 'tussenvoegsel': '', 'last_name': 'Jansen'}
    """
    result: dict[str, str] = {"first_name": "", "tussenvoegsel": "", "last_name": ""}

    if not text:
        return result

    text = text.strip()

    # Build a regex that matches any tussenvoegsel (longest first)
    tv_pattern = "|".join(
        re.escape(tv) for tv in DUTCH_TUSSENVOEGSELS_SORTED
    )
    full_pattern = re.compile(
        rf"^([A-Za-zÀ-ÿ\-]+)\s+({tv_pattern})\s+(.+)$",
        re.IGNORECASE,
    )

    match = full_pattern.match(text)
    if match:
        result["first_name"] = match.group(1).strip().capitalize()
        result["tussenvoegsel"] = match.group(2).strip().lower()
        result["last_name"] = match.group(3).strip().title()
        return result

    # No tussenvoegsel found — split into first + last
    parts = text.split(maxsplit=1)
    if len(parts) == 2:
        result["first_name"] = parts[0].strip().capitalize()
        result["last_name"] = parts[1].strip().title()
    elif len(parts) == 1:
        result["first_name"] = parts[0].strip().capitalize()

    return result
