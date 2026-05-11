"""
enrichment/review_analyzer.py — Google Review scraping + analysis.

Extracts recent Google reviews from a business's Maps page and uses
Claude Haiku to identify pain points relevant to Aerys' services.

Two extraction modes:
  - Quick: grab 2-3 review snippets visible on the detail panel (free, during initial scrape)
  - Deep: click into reviews tab, sort by newest, extract 5-10 (extra page load)

Cost: 1 Claude Haiku call per lead (~€0.00025)
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


# Dutch + English relative date parsing for Google Reviews "X weken geleden" strings.
# Maps unit → approx days.
_REL_UNIT_DAYS: dict[str, int] = {
    "dag": 1, "day": 1, "days": 1, "dagen": 1,
    "week": 7, "weken": 7, "weeks": 7,
    "maand": 30, "maanden": 30, "month": 30, "months": 30,
    "jaar": 365, "year": 365, "years": 365, "jaren": 365,
}


def _parse_relative_date(s: str, now: datetime | None = None) -> datetime | None:
    """Parse a Google Reviews relative date string into absolute UTC datetime.

    Accepts Dutch ("2 weken geleden", "een maand geleden") and English
    ("2 weeks ago", "a month ago"). Returns None on parse failure.
    """
    if not s:
        return None
    now = now or datetime.now(timezone.utc)
    s_lower = s.strip().lower()

    # Normalise "een"/"a" → 1
    s_lower = re.sub(r"\b(een|a|an)\b", "1", s_lower)

    m = re.search(r"(\d+)\s*([a-zà-ÿ]+)", s_lower)
    if not m:
        return None
    try:
        n = int(m.group(1))
    except ValueError:
        return None

    unit_raw = m.group(2)
    # Match to unit (tolerate plural + punctuation)
    days = None
    for unit_key, day_value in _REL_UNIT_DAYS.items():
        if unit_raw.startswith(unit_key):
            days = day_value
            break
    if days is None:
        return None

    return now - timedelta(days=n * days)


def latest_review_date_from_reviews(reviews: list[dict]) -> datetime | None:
    """Return the most recent review date as UTC datetime, or None."""
    parsed: list[datetime] = []
    for r in reviews or []:
        dt = _parse_relative_date(r.get("date") or "")
        if dt is not None:
            parsed.append(dt)
    return max(parsed) if parsed else None


async def scrape_google_reviews(
    google_maps_url: str,
    max_reviews: int = 8,
) -> list[dict]:
    """
    Scrape recent reviews from a Google Maps business page.

    Opens the Maps URL, clicks into the reviews tab, and extracts
    review text, author, rating, and approximate date.

    Returns:
        List of {"author": str, "rating": int, "text": str, "date": str}
    """
    if not google_maps_url:
        return []

    reviews: list[dict] = []

    try:
        from playwright.async_api import async_playwright

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                locale="nl-NL",
                timezone_id="Europe/Amsterdam",
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            )
            page = await context.new_page()
            await page.goto(google_maps_url, wait_until="domcontentloaded", timeout=20_000)
            await asyncio.sleep(3)

            # Handle consent
            for sel in ['button:has-text("Alles accepteren")', 'button:has-text("Accept all")']:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue

            # Click on the reviews tab/button
            review_clicked = False
            for sel in [
                'button[jsaction*="reviewChart"]',
                'button[aria-label*="recensie"]',
                'button[aria-label*="review"]',
                'span.UY7F9',  # review count text — clickable
                'div.F7nice',  # rating area — clickable
            ]:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(2)
                        review_clicked = True
                        break
                except Exception:
                    continue

            if not review_clicked:
                # Fallback: extract whatever review snippets are visible on the detail panel
                reviews = await _extract_visible_reviews(page)
                await browser.close()
                return reviews[:max_reviews]

            # Sort by newest (optional — not all panels have sort)
            try:
                sort_btn = await page.query_selector('button[aria-label*="Sorteer"], button[data-value="sort"]')
                if sort_btn:
                    await sort_btn.click()
                    await asyncio.sleep(1)
                    newest = await page.query_selector('li[data-index="1"], div[data-index="1"]')
                    if newest:
                        await newest.click()
                        await asyncio.sleep(2)
            except Exception:
                pass

            # Scroll to load more reviews
            try:
                review_panel = await page.query_selector('div.m6QErb.DxyBCb, div[role="main"]')
                if review_panel:
                    for _ in range(3):
                        await review_panel.evaluate("el => el.scrollTop = el.scrollHeight")
                        await asyncio.sleep(1)
            except Exception:
                pass

            # Extract reviews via page.evaluate
            reviews = await page.evaluate("""(maxReviews) => {
                const results = [];
                // Multiple selector strategies for review containers
                const containers = document.querySelectorAll(
                    'div[data-review-id], div.jftiEf, div.GHT2ce'
                );

                for (const container of containers) {
                    if (results.length >= maxReviews) break;

                    // Author
                    const authorEl = container.querySelector('div.d4r55, button.al6Kze, span.OSrXXb');
                    const author = authorEl ? authorEl.innerText.trim() : '';

                    // Rating (stars)
                    const starsEl = container.querySelector('span[aria-label*="ster"], span[role="img"]');
                    let rating = 0;
                    if (starsEl) {
                        const label = starsEl.getAttribute('aria-label') || '';
                        const match = label.match(/(\\d)/);
                        rating = match ? parseInt(match[1]) : 0;
                    }

                    // Text
                    const textEl = container.querySelector('span.wiI7pd, div.MyEned span');
                    const text = textEl ? textEl.innerText.trim() : '';

                    // Date
                    const dateEl = container.querySelector('span.rsqaWe, span.xRkPPb');
                    const date = dateEl ? dateEl.innerText.trim() : '';

                    if (text && text.length > 10) {
                        results.push({ author, rating, text, date });
                    }
                }
                return results;
            }""", max_reviews)

            await browser.close()

    except Exception as e:
        logger.warning("review_analyzer: scraping failed for %s: %s", google_maps_url, e)

    logger.info("review_analyzer: extracted %d reviews from %s", len(reviews), google_maps_url[:50])
    return reviews[:max_reviews]


async def _extract_visible_reviews(page: Any) -> list[dict]:
    """Fallback: extract 2-3 review snippets visible on the detail panel."""
    try:
        return await page.evaluate("""() => {
            const results = [];
            const snippets = document.querySelectorAll('span.wiI7pd');
            for (const el of snippets) {
                const text = el.innerText.trim();
                if (text && text.length > 20) {
                    results.push({ author: '', rating: 0, text: text, date: '' });
                }
                if (results.length >= 3) break;
            }
            return results;
        }""")
    except Exception:
        return []


async def analyze_reviews(
    reviews: list[dict],
    sector: str,
    company_name: str,
    supabase_client: Any,
    anthropic_client: Any,
    *,
    lead_id: str,
) -> dict[str, Any]:
    """
    Analyze reviews with Claude Haiku to find outreach-relevant pain points.

    One Claude call per lead, all reviews batched together.

    Returns:
        {
            "complaints": [...],
            "compliments": [...],
            "patterns": [...],
            "aerys_relevant_pains": ["no_online_booking", ...],
            "best_quote": "quote text — reviewer name",
            "summary": "one sentence",
        }
    """
    result: dict[str, Any] = {
        "complaints": [],
        "compliments": [],
        "patterns": [],
        "aerys_relevant_pains": [],
        "best_quote": "",
        "summary": "",
    }

    if not reviews:
        return result

    # Format reviews for Claude — truncate long reviews to prevent JSON parse errors
    review_text = "\n\n".join(
        f"★{'★' * (r.get('rating', 0) - 1)}{'☆' * (5 - r.get('rating', 5))} "
        f"({r.get('date', '?')})\n{(r.get('text', '') or '')[:300]}"
        for r in reviews
    )

    from utils.claude_cache import cached_claude_call

    prompt = (
        f"Analyseer deze Google reviews van '{company_name}' (sector: {sector}).\n\n"
        f"{review_text}\n\n"
        "Geef je antwoord als JSON:\n"
        "- complaints: array van klachten of negatieve punten\n"
        "- compliments: array van positieve punten\n"
        "- patterns: array van terugkerende thema's (meerdere reviewers noemen hetzelfde)\n"
        "- aerys_relevant_pains: array van pijnpunten die relevant zijn voor een webbureau:\n"
        "  mogelijke waarden: no_online_booking, slow_website, bad_mobile, outdated_design,\n"
        "  hard_to_find_info, no_contact_form, poor_communication, no_whatsapp\n"
        "- best_quote: de meest impactvolle quote voor gebruik in een outreach email (max 1 zin)\n"
        "- summary: samenvatting in 1 zin\n\n"
        "Return ALLEEN valid JSON."
    )

    try:
        response = await cached_claude_call(
            prompt=prompt,
            cache_key_suffix=f"review_analysis:{company_name}",
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system="Je analyseert Google reviews en zoekt pijnpunten relevant voor een webbureau. Antwoord alleen in valid JSON.",
            supabase_client=supabase_client,
            lead_id=lead_id,
        )

        import json
        text = response.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)

        parsed = json.loads(text)
        result["complaints"] = parsed.get("complaints") or []
        result["compliments"] = parsed.get("compliments") or []
        result["patterns"] = parsed.get("patterns") or []
        result["aerys_relevant_pains"] = parsed.get("aerys_relevant_pains") or []
        result["best_quote"] = parsed.get("best_quote") or ""
        result["summary"] = parsed.get("summary") or ""

    except Exception as e:
        logger.warning("review_analyzer: Claude analysis failed for %s: %s", company_name, e)

    return result


async def enrich_lead_with_reviews(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
) -> dict:
    """
    Full review enrichment for a single lead.

    Scrapes reviews, analyzes with Claude, stores results.
    """
    # Load lead
    lead_res = supabase_client.table("leads").select(
        "company_name, google_maps_url, domain, sector",
    ).eq("id", lead_id).maybe_single().execute()

    if not lead_res.data:
        return {}

    lead = lead_res.data
    maps_url = lead.get("google_maps_url") or ""
    company_name = lead.get("company_name") or ""
    sector = lead.get("sector") or ""

    if not maps_url:
        # Try to construct from domain
        if lead.get("domain"):
            maps_url = f"https://www.google.com/maps/search/{company_name}"
        else:
            return {}

    # Scrape reviews
    reviews = await scrape_google_reviews(maps_url, max_reviews=8)

    if not reviews:
        logger.info("review_analyzer: no reviews found for %s", company_name)
        return {"reviews_found": 0}

    # Analyze
    analysis = await analyze_reviews(reviews, sector, company_name, supabase_client, anthropic_client, lead_id=lead_id)

    # Derive latest review date (Warmr Sequence v1.0 datapoint)
    latest_dt = latest_review_date_from_reviews(reviews)

    # Store on lead
    try:
        update_payload: dict = {
            "review_analysis": analysis,
            "review_best_quote": analysis.get("best_quote") or "",
            "review_pain_points": analysis.get("aerys_relevant_pains") or [],
        }
        if latest_dt is not None:
            update_payload["latest_review_date"] = latest_dt.isoformat()
        supabase_client.table("leads").update(update_payload).eq("id", lead_id).execute()
    except Exception as e:
        logger.debug("review_analyzer: failed to store results: %s", e)

    logger.info(
        "review_analyzer: %s — %d reviews, %d complaints, %d aerys-relevant pains, quote='%s'",
        company_name, len(reviews), len(analysis.get("complaints", [])),
        len(analysis.get("aerys_relevant_pains", [])),
        (analysis.get("best_quote") or "")[:50],
    )

    return {"reviews_found": len(reviews), **analysis}
