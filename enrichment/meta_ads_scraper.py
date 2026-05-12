"""
enrichment/meta_ads_scraper.py — Draait de kliniek actief Meta/Facebook ads?

Datapunten voor Warmr Sequence v1.0:
  - meta_ads_active (bool)   — Mail 1 BLOK C trigger
  - ad_focus (string)         — gebruikt als [ad_focus] in mail body

Twee paden:
  1. Graph API — vereist META_AD_LIBRARY_TOKEN env. Stabiel, wit onder Meta ToS.
     Endpoint: https://graph.facebook.com/v18.0/ads_archive
  2. Playwright fallback — fetch publieke Ad Library search page, parse DOM.
     Werkt zonder auth. Broos (Meta wijzigt layout) maar functioneel.

Cache: heatr_meta_ads_cache (migration 006). TTL 7 dagen — ad-campagnes
draaien typisch 2-8 weken.

Never raises. Bij fail: returnt {"meta_ads_active": None, "ad_focus": None}
zodat de lead door kan.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from utils.rate_limiter import wait_for_token

logger = logging.getLogger(__name__)

_CACHE_TTL_DAYS = 7
_GRAPH_TOKEN_ENV = "META_AD_LIBRARY_TOKEN"
_GRAPH_URL = "https://graph.facebook.com/v18.0/ads_archive"
_PUBLIC_AD_LIB_URL = "https://www.facebook.com/ads/library/?active_status=active&ad_type=all&country=NL&q={q}"

# Behandeling-keywords die we als ad_focus herkennen in ad copy
_AD_FOCUS_KEYWORDS = [
    "botox", "fillers", "laserontharing", "laser ontharing", "huidverjonging",
    "microblading", "permanente make-up", "permanente makeup", "pmu",
    "acne", "pigment", "lashlift", "wimperextensions", "morpheus8",
    "hifu", "microneedling", "profhilo", "mesotherapie", "peeling",
    "hydrafacial", "cryolipolyse", "coolsculpting",
]


def _cache_key(company_name: str, domain: str) -> str:
    base = f"{(company_name or '').strip().lower()}::{(domain or '').strip().lower()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


async def _check_cache(
    key: str, supabase_client: Any,
) -> dict | None:
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=_CACHE_TTL_DAYS)).isoformat()
        res = (
            supabase_client.table("meta_ads_cache")
            .select("meta_ads_active, ad_focus, checked_at")
            .eq("cache_key", key)
            .gte("checked_at", cutoff)
            .maybe_single()
            .execute()
        )
        if res.data:
            return {
                "meta_ads_active": res.data.get("meta_ads_active"),
                "ad_focus": res.data.get("ad_focus"),
                "from_cache": True,
            }
    except Exception as e:
        logger.debug("meta_ads_scraper: cache lookup failed: %s", e)
    return None


async def _store_cache(
    key: str,
    company_name: str,
    domain: str,
    meta_ads_active: bool | None,
    ad_focus: str | None,
    raw_snippet: dict | None,
    supabase_client: Any,
) -> None:
    try:
        supabase_client.table("meta_ads_cache").upsert({
            "cache_key": key,
            "company_name": company_name,
            "domain": domain,
            "meta_ads_active": meta_ads_active,
            "ad_focus": ad_focus,
            "raw_snippet": raw_snippet or {},
        }, on_conflict="cache_key").execute()
    except Exception as e:
        logger.debug("meta_ads_scraper: cache store failed: %s", e)


def _extract_ad_focus(text: str) -> str | None:
    """Find first behandeling-keyword in ad copy. Returns capitalized label or None."""
    if not text:
        return None
    tl = text.lower()
    for kw in _AD_FOCUS_KEYWORDS:
        if kw in tl:
            # Return titled version for natural insertion in NL mail body
            return kw.title().replace("Make-Up", "make-up").replace("Pmu", "PMU")
    return None


async def _fetch_via_graph_api(
    company_name: str, token: str,
) -> dict | None:
    params = {
        "access_token": token,
        "ad_active_status": "ACTIVE",
        "ad_reached_countries": "['NL']",
        "search_terms": company_name,
        "limit": 10,
        "fields": "ad_creative_bodies,ad_creative_link_captions,page_name",
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(_GRAPH_URL, params=params)
        if r.status_code >= 400:
            logger.debug("meta_ads graph api %d: %s", r.status_code, r.text[:200])
            return None
        body = r.json()
    except Exception as e:
        logger.warning("meta_ads_scraper: graph api failed: %s", e)
        return None

    ads = body.get("data") or []
    if not ads:
        return {"meta_ads_active": False, "ad_focus": None, "raw_snippet": {"source": "graph", "count": 0}}

    # Aggregate ad-copy text
    text_blob = " ".join(
        (" ".join(a.get("ad_creative_bodies") or [])) + " " +
        (" ".join(a.get("ad_creative_link_captions") or []))
        for a in ads
    )
    focus = _extract_ad_focus(text_blob)
    return {
        "meta_ads_active": True,
        "ad_focus": focus,
        "raw_snippet": {"source": "graph", "count": len(ads), "focus_hit": bool(focus)},
    }


async def _fetch_via_playwright(company_name: str) -> dict | None:
    """Fallback: scrape public Ad Library page via Playwright.

    Broos — Meta kan selectors wijzigen. Returnt None bij DOM-mismatch.
    """
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        logger.warning("meta_ads_scraper: playwright not available: %s", e)
        return None

    url = _PUBLIC_AD_LIB_URL.format(q=httpx.URL("").copy_with(params={"q": company_name}).query.decode())
    # Simpler: use httpx URL quoting
    from urllib.parse import quote
    url = _PUBLIC_AD_LIB_URL.format(q=quote(company_name))

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            try:
                context = await browser.new_context(locale="nl-NL")
                page = await context.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
                import asyncio
                await asyncio.sleep(3)

                # Handle Meta cookie banner
                for sel in ['button[data-cookiebanner="accept_button"]', 'button:has-text("Alles toestaan")']:
                    try:
                        btn = await page.query_selector(sel)
                        if btn:
                            await btn.click()
                            await asyncio.sleep(1)
                            break
                    except Exception:
                        continue

                # The number of results is rendered as e.g. "~12 resultaten"
                # The ads themselves carry data-ad-id or similar.
                body_text = (await page.content()).lower()

                # Heuristic 1: explicit "0 results" → inactive
                if re.search(r"~?\s*0\s*resultaat", body_text) or "geen resultaten" in body_text:
                    return {"meta_ads_active": False, "ad_focus": None, "raw_snippet": {"source": "playwright", "reason": "zero_results"}}

                # Heuristic 2: any visible ad card → active. Meta uses divs with aria-labels
                # like "Inzicht in advertentie" or visible "Actief" badges.
                active_markers = 0
                for marker in ["actief", "uitgevoerd", "inzicht in advertentie"]:
                    active_markers += body_text.count(marker)

                if active_markers >= 2:
                    focus = _extract_ad_focus(body_text)
                    return {
                        "meta_ads_active": True,
                        "ad_focus": focus,
                        "raw_snippet": {"source": "playwright", "markers": active_markers},
                    }

                return {"meta_ads_active": False, "ad_focus": None, "raw_snippet": {"source": "playwright", "markers": active_markers}}

            finally:
                await browser.close()
    except Exception as e:
        logger.warning("meta_ads_scraper: playwright crash: %s", e)
        return None


async def check_meta_ads(
    company_name: str,
    domain: str,
    supabase_client: Any,
) -> dict[str, Any]:
    """Return {meta_ads_active: bool|None, ad_focus: str|None, from_cache: bool}.

    Never raises. Strategy:
      1. Cache (7 days)
      2. Graph API if token in env
      3. Playwright fallback
      4. Give up — return None-values
    """
    out: dict[str, Any] = {
        "meta_ads_active": None,
        "ad_focus": None,
        "from_cache": False,
        "source": None,
    }

    if not company_name:
        return out

    key = _cache_key(company_name, domain)

    cached = await _check_cache(key, supabase_client)
    if cached is not None:
        out.update(cached)
        out["source"] = "cache"
        return out

    # Rate-limit guard before external call
    try:
        await wait_for_token("meta_ads_playwright", supabase_client)
    except Exception as e:
        logger.warning("meta_ads_scraper: rate-limit wait failed: %s", e)
        return out

    token = os.environ.get(_GRAPH_TOKEN_ENV)
    result: dict | None = None
    if token:
        result = await _fetch_via_graph_api(company_name, token)
        if result is not None:
            out["source"] = "graph_api"

    if result is None:
        result = await _fetch_via_playwright(company_name)
        if result is not None:
            out["source"] = "playwright"

    if result is None:
        # Total miss — cache a "not checked" marker so we don't retry every run
        await _store_cache(
            key, company_name, domain,
            meta_ads_active=None, ad_focus=None,
            raw_snippet={"source": "failed", "ts": datetime.now(timezone.utc).isoformat()},
            supabase_client=supabase_client,
        )
        return out

    out["meta_ads_active"] = result.get("meta_ads_active")
    out["ad_focus"] = result.get("ad_focus")

    await _store_cache(
        key, company_name, domain,
        meta_ads_active=out["meta_ads_active"],
        ad_focus=out["ad_focus"],
        raw_snippet=result.get("raw_snippet"),
        supabase_client=supabase_client,
    )

    logger.info(
        "meta_ads_scraper: %s → active=%s focus=%s source=%s",
        company_name, out["meta_ads_active"], out["ad_focus"], out["source"],
    )
    return out
