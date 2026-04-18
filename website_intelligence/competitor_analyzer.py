"""
website_intelligence/competitor_analyzer.py — Local competitor benchmarking.

Finds 2-3 competitors in the same city + sector via Google Maps,
runs lightweight website analysis (technical + conversion only, no Claude),
and computes a score_vs_market delta.

Results are cached per city+sector for 7 days — if lead A and lead B
are both "makelaars in Amsterdam", they share the same competitor data.

Cost: 0 Claude calls. Only HTTP requests for competitor website HTML.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote_plus, urlparse

import httpx

logger = logging.getLogger(__name__)

CACHE_TTL_DAYS = 7
MAX_COMPETITORS = 3


async def benchmark_lead(
    lead_id: str,
    domain: str,
    sector: str,
    city: str,
    lead_total_score: int,
    workspace_id: str,
    supabase_client: Any,
) -> dict[str, Any]:
    """
    Find local competitors and compare website quality.

    Uses cached results if available (same sector+city within 7 days).

    Returns:
        {
            "competitors": [{"name": ..., "domain": ..., "score": ...}, ...],
            "market_avg_score": int,
            "score_vs_market": int,  # negative = below average
            "lead_rank": int,        # 1 = best in market
            "total_analyzed": int,
        }
    """
    result: dict[str, Any] = {
        "competitors": [],
        "market_avg_score": 0,
        "score_vs_market": 0,
        "lead_rank": 1,
        "total_analyzed": 0,
    }

    if not city or not sector:
        return result

    # Check cache first
    cached = await _get_cached_competitors(sector, city, workspace_id, supabase_client)
    if cached:
        logger.info("competitor_analyzer: cache hit for %s/%s", sector, city)
        competitors = cached.get("competitors") or []
    else:
        # Scrape competitors from Google Maps
        competitors = await _find_competitors(sector, city, domain)
        if not competitors:
            return result

        # Analyze each competitor's website (lightweight — no Claude)
        for comp in competitors:
            comp_domain = comp.get("domain")
            if not comp_domain:
                continue
            comp_scores = await _analyze_competitor_website(comp_domain)
            comp.update(comp_scores)

        # Cache results
        await _cache_competitors(sector, city, workspace_id, competitors, supabase_client)

    # Filter out competitors without scores
    scored_comps = [c for c in competitors if c.get("total_score") is not None]
    if not scored_comps:
        return result

    # Calculate market average (competitors only, excluding our lead)
    comp_scores = [c["total_score"] for c in scored_comps]
    market_avg = round(sum(comp_scores) / len(comp_scores))

    # Calculate rank (1 = best)
    all_scores = comp_scores + [lead_total_score]
    all_scores.sort(reverse=True)
    lead_rank = all_scores.index(lead_total_score) + 1

    result["competitors"] = scored_comps[:MAX_COMPETITORS]
    result["market_avg_score"] = market_avg
    result["score_vs_market"] = lead_total_score - market_avg
    result["lead_rank"] = lead_rank
    result["total_analyzed"] = len(scored_comps) + 1  # competitors + lead

    # Store on website_intelligence
    try:
        supabase_client.table("website_intelligence").update({
            "competitor_data": result,
            "score_vs_market": result["score_vs_market"],
        }).eq("lead_id", lead_id).execute()
    except Exception as e:
        logger.debug("competitor_analyzer: failed to store results: %s", e)

    logger.info(
        "competitor_analyzer: %s in %s — lead=%d avg=%d delta=%+d rank=%d/%d",
        sector, city, lead_total_score, market_avg,
        result["score_vs_market"], lead_rank, result["total_analyzed"],
    )

    return result


async def _find_competitors(
    sector: str,
    city: str,
    exclude_domain: str,
) -> list[dict]:
    """Search Google Maps for competitors in the same sector + city."""
    from config.sectors import get_sector

    try:
        config = get_sector(sector)
        query = config["search_queries"][0].replace("{city}", city)
    except (ValueError, IndexError):
        query = f"{sector} {city}"

    search_url = f"https://www.google.com/maps/search/{quote_plus(query)}"
    competitors: list[dict] = []

    try:
        from playwright.async_api import async_playwright
        import asyncio

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                locale="nl-NL",
                timezone_id="Europe/Amsterdam",
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            )
            page = await context.new_page()
            await page.goto(search_url, wait_until="domcontentloaded", timeout=20_000)
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

            # Extract competitor names + websites from results list
            # Use page.evaluate to get data from all visible results at once
            comp_data = await page.evaluate("""() => {
                const results = [];
                const links = document.querySelectorAll('a[href*="/maps/place/"]');
                const seen = new Set();

                for (const link of links) {
                    const name = link.getAttribute('aria-label') || '';
                    if (!name || seen.has(name)) continue;
                    seen.add(name);
                    results.push({
                        name: name,
                        maps_url: link.href || '',
                    });
                    if (results.length >= 8) break;
                }
                return results;
            }""")

            # For each competitor, click to get website
            exclude_domain_clean = exclude_domain.lower().replace("www.", "")
            links = await page.query_selector_all('a[href*="/maps/place/"]')

            for i, link in enumerate(links[:6]):
                if len(competitors) >= MAX_COMPETITORS:
                    break

                try:
                    await link.click()
                    await asyncio.sleep(2)

                    data = await page.evaluate("""() => {
                        const nameEl = document.querySelector('h1.DUwDvf') || document.querySelector('h1.fontHeadlineLarge');
                        const websiteEl = document.querySelector('a[data-item-id="authority"]');
                        return {
                            name: nameEl ? nameEl.innerText.trim() : '',
                            website: websiteEl ? websiteEl.getAttribute('href') : '',
                        };
                    }""")

                    comp_name = data.get("name") or ""
                    website = data.get("website") or ""

                    if website:
                        parsed = urlparse(website)
                        comp_domain = parsed.netloc.replace("www.", "").lower()

                        # Skip if it's the same company
                        if comp_domain == exclude_domain_clean:
                            await page.go_back()
                            await asyncio.sleep(1)
                            continue

                        competitors.append({
                            "name": comp_name,
                            "domain": comp_domain,
                        })

                    await page.go_back()
                    await asyncio.sleep(1)
                except Exception:
                    try:
                        await page.go_back()
                        await asyncio.sleep(1)
                    except Exception:
                        pass

            await browser.close()

    except Exception as e:
        logger.warning("competitor_analyzer: Google Maps search failed: %s", e)

    logger.info("competitor_analyzer: found %d competitors for %s in %s", len(competitors), sector, city)
    return competitors


async def _analyze_competitor_website(domain: str) -> dict:
    """Lightweight website analysis — technical + conversion only, no Claude."""
    from website_intelligence.technical_checker import check_technical
    from website_intelligence.conversion_checker import check_conversion

    tech_score = 0
    conv_score = 0

    try:
        tech = await check_technical(domain)
        tech_score = tech.get("technical_score") or 0
    except Exception:
        pass

    try:
        async with httpx.AsyncClient(
            timeout=10, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"},
        ) as client:
            r = await client.get(f"https://{domain}")
            if r.status_code == 200:
                conv = await check_conversion(domain, r.text, "")
                conv_score = conv.get("conversion_score") or 0
    except Exception:
        pass

    total = tech_score + conv_score
    return {
        "technical_score": tech_score,
        "conversion_score": conv_score,
        "total_score": total,
    }


async def _get_cached_competitors(
    sector: str, city: str, workspace_id: str, supabase_client: Any,
) -> dict | None:
    """Check if we have recent competitor data for this sector+city."""
    try:
        now = datetime.now(timezone.utc).isoformat()
        res = supabase_client.table("competitor_cache").select("*").eq(
            "workspace_id", workspace_id,
        ).eq("sector", sector).eq("city", city.lower()).gte(
            "expires_at", now,
        ).maybe_single().execute()
        return res.data
    except Exception:
        return None


async def _cache_competitors(
    sector: str, city: str, workspace_id: str,
    competitors: list[dict], supabase_client: Any,
) -> None:
    """Cache competitor data for this sector+city."""
    expires = (datetime.now(timezone.utc) + timedelta(days=CACHE_TTL_DAYS)).isoformat()
    try:
        supabase_client.table("competitor_cache").upsert({
            "workspace_id": workspace_id,
            "sector": sector,
            "city": city.lower(),
            "competitors": competitors,
            "market_avg_score": round(
                sum(c.get("total_score") or 0 for c in competitors) / max(len(competitors), 1)
            ),
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": expires,
        }, on_conflict="workspace_id,sector,city").execute()
    except Exception as e:
        logger.debug("competitor_analyzer: cache write failed: %s", e)
