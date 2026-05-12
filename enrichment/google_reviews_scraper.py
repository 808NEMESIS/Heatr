"""
enrichment/google_reviews_scraper.py — Latest-review-date harvester.

Visits a lead's google_maps_url via Playwright, opens the "Recensies" tab,
parses the first (= most recent) review's relative timestamp, converts to
an absolute date.

Output written to leads.latest_review_date (timestamptz). Caller derives
days_since_last_review op leestijd via lead.latest_review_date − now.

Cost: €0 (Playwright tijd, ~5-7s per lead). Geen Claude. Geen Google API key.

Used by config/sequence_templates.pick_observation_block to enable BLOK B
(reviews-cadans observation): rating 4.0-4.4 + days_since_last_review > 28.

Failures (geen URL / Playwright error / DOM gewijzigd) → return None, never raise.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Dutch + English relative date phrases zoals Google Maps ze rendert.
# Format: pattern → callable(value: int) → timedelta
_RELATIVE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(\d+)\s*(?:dag|day|days|dagen)\s*geleden", re.I), "days"),
    (re.compile(r"(\d+)\s*(?:week|weken|weeks)\s*geleden", re.I), "weeks"),
    (re.compile(r"(\d+)\s*(?:maand|maanden|month|months)\s*geleden", re.I), "months"),
    (re.compile(r"(\d+)\s*(?:jaar|year|years)\s*geleden", re.I), "years"),
    (re.compile(r"(?:een|1|a)\s*(?:dag|day)\s*geleden", re.I), "1day"),
    (re.compile(r"(?:een|1|a)\s*(?:week)\s*geleden", re.I), "1week"),
    (re.compile(r"(?:een|1|a)\s*(?:maand|month)\s*geleden", re.I), "1month"),
    (re.compile(r"(?:een|1|a)\s*(?:jaar|year)\s*geleden", re.I), "1year"),
    (re.compile(r"gisteren|yesterday", re.I), "1day"),
    (re.compile(r"vandaag|today", re.I), "0day"),
]


def _parse_relative(text: str, now: datetime) -> datetime | None:
    """Convert "3 weken geleden" → absolute datetime (UTC)."""
    text = text.strip()
    for pattern, unit in _RELATIVE_PATTERNS:
        m = pattern.search(text)
        if not m:
            continue
        try:
            value = int(m.group(1)) if m.groups() else 1
        except (ValueError, IndexError):
            value = 1

        if unit in ("days", "1day"):
            delta = timedelta(days=value if unit == "days" else 1)
        elif unit in ("weeks", "1week"):
            delta = timedelta(weeks=value if unit == "weeks" else 1)
        elif unit in ("months", "1month"):
            delta = timedelta(days=(value if unit == "months" else 1) * 30)
        elif unit in ("years", "1year"):
            delta = timedelta(days=(value if unit == "years" else 1) * 365)
        elif unit == "0day":
            delta = timedelta(0)
        else:
            return None
        return now - delta
    return None


async def fetch_latest_review_date(
    google_maps_url: str,
    timeout_s: int = 25,
) -> datetime | None:
    """Open Google Maps reviews tab, parse first review's relative date.

    Returns absolute UTC datetime of most recent review, or None on any failure.
    """
    if not google_maps_url:
        return None

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.warning("playwright not installed — review-date scrape unavailable")
        return None

    now = datetime.now(timezone.utc)
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                locale="nl-NL",
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            )
            page = await context.new_page()
            try:
                await page.goto(google_maps_url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
                await asyncio.sleep(2)  # let detail panel hydrate
            except Exception as e:
                logger.debug("reviews_scraper: goto failed: %s", e)
                return None

            # Click "Recensies" / "Reviews" tab
            for sel in (
                'button[aria-label*="Recensies"]',
                'button[aria-label*="Reviews"]',
                'button[jsaction*="reviewlist"]',
            ):
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click(timeout=3000)
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            # Sort by "Newest" if available — Google sometimes defaults to "most relevant"
            try:
                sort_btn = await page.query_selector('button[aria-label*="Sorteren"]')
                if sort_btn:
                    await sort_btn.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    # Click "Nieuwste eerst" / "Newest"
                    for sel in ('text=Nieuwste eerst', 'text=Newest', 'text=Most recent'):
                        try:
                            opt = await page.query_selector(sel)
                            if opt:
                                await opt.click(timeout=2000)
                                await asyncio.sleep(1)
                                break
                        except Exception:
                            continue
            except Exception:
                pass

            # Find first review's relative-date text. Selector strategy:
            # Google Maps uses span with class containing "rsqaWe" (often) for the
            # date. We try multiple selectors then regex-scan for date phrases.
            candidates: list[str] = []
            for sel in (
                'div[data-review-id] span:has-text("geleden")',
                'div[jslog*="review"] span:has-text("geleden")',
                'span.rsqaWe',
                '.DU9Pgb span',
                'div[role="article"] span',
            ):
                try:
                    els = await page.query_selector_all(sel)
                    for el in els[:5]:
                        txt = (await el.inner_text()).strip()
                        if txt:
                            candidates.append(txt)
                except Exception:
                    continue

            # Fallback: grab the first 3000 chars of body and regex over the whole thing
            if not candidates:
                try:
                    body_text = await page.inner_text("body")
                    candidates.append(body_text[:3000])
                except Exception:
                    pass

            for txt in candidates:
                parsed = _parse_relative(txt, now)
                if parsed is not None:
                    logger.info("reviews_scraper: %s → latest=%s (from %r)", google_maps_url[:80], parsed.date(), txt[:60])
                    return parsed

            logger.info("reviews_scraper: no relative-date phrase found on %s", google_maps_url[:80])
            return None
        finally:
            await browser.close()


def days_since(latest_review_date: datetime | str | None) -> int | None:
    """Helper: int days between latest_review_date and now (UTC). None if missing."""
    if not latest_review_date:
        return None
    if isinstance(latest_review_date, str):
        try:
            latest_review_date = datetime.fromisoformat(latest_review_date.replace("Z", "+00:00"))
        except ValueError:
            return None
    if latest_review_date.tzinfo is None:
        latest_review_date = latest_review_date.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - latest_review_date
    return max(int(delta.total_seconds() // 86400), 0)
