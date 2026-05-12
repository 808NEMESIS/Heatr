"""
enrichment/website_crawler_v2.py — Playwright-based contact harvester.

Vervangt de raw-HTML regex-only scraper door een echte browser-rendering
aanpak. Pakt:
  - Emails uit mailto: links (ook op JS-gerenderde pagina's)
  - Emails uit body tekst (regex over volledig gerenderde DOM)
  - Sociale media (Instagram, Facebook, LinkedIn, YouTube, TikTok)
  - Telefoon uit tel: links (als backup op Google Maps phone)
  - Schema.org structured data (JSON-LD) — levert vaak contactEmail, owner
  - Openingstijden (als aanwezig)

Nooit raiset. Returns dict met alles wat gevonden is, lege lijsten anders.

Cost: €0 (alleen Playwright tijd, ~5-10s per lead).
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Bezoekstrategie: max 3 paths. Homepage levert vaak al genoeg data, /contact vult email,
# /over-ons of /team levert page_text voor owner-extractor. Early-exit op alle drie zodra
# we voldoende hebben — gemiddeld 1-2 paths per lead, niet 9.
_PRIMARY_PATH = ""
_CONTACT_FALLBACKS = ["/contact", "/contact-us"]
_TEAM_FALLBACKS = ["/over-ons", "/over", "/team", "/about"]
_TEAM_PATH_KEYS = {"/over-ons", "/over", "/team", "/about", "/kennismaken"}

_EMAIL_REGEX = re.compile(
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
)
_PHONE_REGEX = re.compile(
    r"(?:\+31[\s\-]?|0)[\d\s\-]{9,13}",
)

_SOCIAL_PATTERNS = {
    "instagram": re.compile(r"(?:instagram\.com|instagr\.am)/([A-Za-z0-9_\.]+)", re.I),
    "facebook":  re.compile(r"facebook\.com/([A-Za-z0-9_\.\-]+)", re.I),
    "linkedin":  re.compile(r"linkedin\.com/(?:company|in)/([A-Za-z0-9_\-\.%]+)", re.I),
    "youtube":   re.compile(r"youtube\.com/(?:@|channel/|user/)([A-Za-z0-9_\-]+)", re.I),
    "tiktok":    re.compile(r"tiktok\.com/@([A-Za-z0-9_\.\-]+)", re.I),
}

# Emails die we overslaan — spam/noreply/sentry/etc.
_EMAIL_BLOCKLIST = {
    "example.com", "example.org", "domain.com", "email.com",
    "sentry.io", "wixpress.com", "wordpress.com", "google.com",
    "facebook.com", "twitter.com",
}


async def crawl_website_contact(
    domain: str,
    timeout_total_s: int = 30,
) -> dict[str, Any]:
    """Return {emails, phones, socials, schema_org, opening_hours, pages_visited}.

    Never raises. On any failure returns partial result.
    """
    out: dict[str, Any] = {
        "emails": [],
        "phones": [],
        "socials": {},
        "schema_org": {},
        "opening_hours": None,
        "pages_visited": [],
        "page_text": "",
        "error": None,
    }
    if not domain:
        out["error"] = "no_domain"
        return out

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        out["error"] = "playwright_not_installed"
        return out

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                locale="nl-NL",
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            )
            page = await context.new_page()

            deadline = asyncio.get_event_loop().time() + timeout_total_s
            emails_found: set[str] = set()
            phones_found: set[str] = set()
            socials_found: dict[str, str] = {}
            schema_data: dict[str, Any] = {}
            page_text_parts: list[str] = []

            # Build path queue lazily — only fetch fallbacks if primary didn't yield
            paths_to_try: list[str] = [_PRIMARY_PATH]
            visited_count = 0
            MAX_PATHS = 3
            team_text_collected = False

            for path in paths_to_try:
                if visited_count >= MAX_PATHS:
                    break
                if asyncio.get_event_loop().time() > deadline:
                    break
                url = f"https://{domain}{path}"
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=8000)
                    await asyncio.sleep(1.0)  # let JS-rendering settle (was 1.5s)
                    out["pages_visited"].append(path or "/")
                    visited_count += 1
                except Exception as e:
                    logger.debug("crawler: goto %s failed: %s", url, e)
                    continue

                # ── Harvest mailto + tel from anchor hrefs ──
                try:
                    mailtos = await page.eval_on_selector_all(
                        'a[href^="mailto:"]',
                        "els => els.map(e => e.href.replace(/^mailto:/, '').split('?')[0])",
                    )
                    for m in mailtos or []:
                        if m and _valid_email(m):
                            emails_found.add(m.lower())
                except Exception:
                    pass

                try:
                    tels = await page.eval_on_selector_all(
                        'a[href^="tel:"]',
                        "els => els.map(e => e.href.replace(/^tel:/, ''))",
                    )
                    for t in tels or []:
                        cleaned = re.sub(r"[^\d+]", "", t)
                        if len(cleaned) >= 9:
                            phones_found.add(cleaned)
                except Exception:
                    pass

                # ── Full rendered DOM text → regex for emails + phones ──
                try:
                    body_text = await page.inner_text("body")
                except Exception:
                    body_text = ""

                if body_text:
                    # Store per-page text for downstream owner-extraction
                    # Prefer /contact, /over-ons, /team content (heuristic)
                    if path in _TEAM_PATH_KEYS or path == "/contact":
                        page_text_parts.append(f"=== {path or '/'} ===\n{body_text[:4000]}")
                        if path in _TEAM_PATH_KEYS:
                            team_text_collected = True
                    elif not page_text_parts:
                        page_text_parts.append(f"=== {path or '/'} ===\n{body_text[:4000]}")
                    for m in _EMAIL_REGEX.findall(body_text):
                        if _valid_email(m):
                            emails_found.add(m.lower())
                    for m in _PHONE_REGEX.findall(body_text):
                        cleaned = re.sub(r"[^\d+]", "", m)
                        if len(cleaned) >= 9 and not cleaned.startswith("00000"):
                            phones_found.add(cleaned)

                # ── Harvest socials from all <a href> ──
                try:
                    hrefs = await page.eval_on_selector_all(
                        "a[href]",
                        "els => els.map(e => e.href)",
                    )
                    for href in hrefs or []:
                        for platform, pat in _SOCIAL_PATTERNS.items():
                            m = pat.search(href)
                            if m and platform not in socials_found:
                                socials_found[platform] = m.group(0)
                except Exception:
                    pass

                # ── Schema.org JSON-LD ──
                try:
                    scripts = await page.eval_on_selector_all(
                        'script[type="application/ld+json"]',
                        "els => els.map(e => e.textContent)",
                    )
                    for raw in scripts or []:
                        if not raw:
                            continue
                        try:
                            parsed = json.loads(raw)
                        except Exception:
                            continue
                        _extract_schema(parsed, schema_data, emails_found, phones_found)
                except Exception:
                    pass

                # Hard early-exit: enough data, owner-relevant text, no need for fallbacks
                has_email = bool(emails_found) or bool(schema_data.get("email"))
                has_phone = bool(phones_found)
                if has_email and has_phone and team_text_collected:
                    break
                if len(emails_found) >= 3 and len(phones_found) >= 1:
                    break

                # Decide whether to queue fallback paths (lazy — only when primary missed)
                queued = set(paths_to_try)
                # Need email/phone? Try /contact-style fallback once.
                if (not has_email or not has_phone):
                    for p in _CONTACT_FALLBACKS:
                        if p not in queued and visited_count + (len(paths_to_try) - paths_to_try.index(path) - 1) < MAX_PATHS:
                            paths_to_try.append(p)
                            queued.add(p)
                            break
                # Need team text for owner extractor? Try /over-ons or /team once.
                if not team_text_collected:
                    for p in _TEAM_FALLBACKS:
                        if p not in queued and visited_count + (len(paths_to_try) - paths_to_try.index(path) - 1) < MAX_PATHS:
                            paths_to_try.append(p)
                            queued.add(p)
                            break

            out["emails"] = sorted(emails_found)
            out["phones"] = sorted(phones_found)
            out["socials"] = socials_found
            out["schema_org"] = schema_data
            out["page_text"] = "\n\n".join(page_text_parts)[:12000]

        finally:
            await browser.close()

    logger.info(
        "crawler_v2: %s → emails=%d phones=%d socials=%d schema=%s pages=%s",
        domain, len(out["emails"]), len(out["phones"]),
        len(out["socials"]), bool(out["schema_org"]), out["pages_visited"],
    )
    return out


def _valid_email(email: str) -> bool:
    """Filter out junk / blocklist / tracking emails."""
    email = email.strip().lower()
    if not email or "@" not in email:
        return False
    if len(email) > 100:
        return False
    domain = email.rsplit("@", 1)[-1]
    if domain in _EMAIL_BLOCKLIST:
        return False
    # Reject tracking-like patterns
    if any(bad in email for bad in ["@sentry", "@wixpress", "@wordpress", "noreply@", "no-reply@", "donotreply@"]):
        return False
    # Reject obvious file-extensions that regex can catch
    if email.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".pdf")):
        return False
    return True


def _extract_schema(obj: Any, schema: dict, emails: set, phones: set) -> None:
    """Recursively scan a JSON-LD blob for useful contact fields."""
    if isinstance(obj, list):
        for item in obj:
            _extract_schema(item, schema, emails, phones)
        return
    if not isinstance(obj, dict):
        return

    # Common Schema.org keys
    for key in ("email", "telephone", "faxNumber", "name", "address", "openingHours", "openingHoursSpecification", "founder", "employee", "sameAs"):
        v = obj.get(key)
        if v and key not in schema:
            schema[key] = v

    # Harvest contact fields
    if email := obj.get("email"):
        if isinstance(email, str) and _valid_email(email):
            emails.add(email.lower())
    if phone := obj.get("telephone"):
        if isinstance(phone, str):
            cleaned = re.sub(r"[^\d+]", "", phone)
            if len(cleaned) >= 9:
                phones.add(cleaned)

    # Recurse into nested structures
    for v in obj.values():
        if isinstance(v, (dict, list)):
            _extract_schema(v, schema, emails, phones)
