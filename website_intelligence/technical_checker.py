"""
website_intelligence/technical_checker.py — Layer 1: Technical website analysis.

Checks SSL, PageSpeed, CMS detection, server location, sitemap, schema markup.
Max 25 points per CLAUDE.md spec.
"""
from __future__ import annotations

import logging
import os
from typing import Any
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

PAGESPEED_API_KEY = os.getenv("PAGESPEED_API_KEY", "")


async def check_technical(domain: str, supabase_client: Any = None) -> dict[str, Any]:
    """
    Run all Layer 1 technical checks for a domain.

    Returns dict with:
        has_ssl, mobile_friendly, pagespeed_mobile, pagespeed_desktop,
        cms, server_country, has_sitemap, has_schema_markup,
        technical_score (0-25), details (list of check results)
    """
    result: dict[str, Any] = {
        "has_ssl": False,
        "mobile_friendly": False,
        "pagespeed_mobile": 0,
        "pagespeed_desktop": 0,
        "cms": None,
        "server_country": None,
        "has_sitemap": False,
        "has_schema_markup": False,
        "technical_score": 0,
        "details": [],
    }

    base_url = f"https://{domain}"
    score = 0

    async with httpx.AsyncClient(
        timeout=15.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"},
    ) as client:
        # --- SSL check (3 pts) ---
        try:
            r = await client.get(base_url)
            result["has_ssl"] = True
            score += 3
            result["details"].append({"check": "ssl", "passed": True})
            page_html = r.text
        except Exception:
            try:
                r = await client.get(f"http://{domain}")
                page_html = r.text
                result["details"].append({"check": "ssl", "passed": False, "note": "HTTP only"})
            except Exception:
                result["details"].append({"check": "ssl", "passed": False, "note": "Unreachable"})
                page_html = ""

        # --- CMS detection (4 pts) ---
        if page_html:
            cms = _detect_cms(page_html)
            result["cms"] = cms
            if cms:
                score += 4
                result["details"].append({"check": "cms", "passed": True, "value": cms})
            else:
                result["details"].append({"check": "cms", "passed": False})

        # --- Schema markup (3 pts) ---
        if page_html and ('application/ld+json' in page_html or 'itemtype=' in page_html):
            result["has_schema_markup"] = True
            score += 3
            result["details"].append({"check": "schema_markup", "passed": True})
        else:
            result["details"].append({"check": "schema_markup", "passed": False})

        # --- Sitemap (1 pt) ---
        try:
            r = await client.head(f"{base_url}/sitemap.xml")
            if r.status_code == 200:
                result["has_sitemap"] = True
                score += 1
                result["details"].append({"check": "sitemap", "passed": True})
            else:
                result["details"].append({"check": "sitemap", "passed": False})
        except Exception:
            result["details"].append({"check": "sitemap", "passed": False})

        # --- Server location (2 pts) ---
        try:
            r = await client.get(f"http://ip-api.com/json/{domain}?fields=countryCode", timeout=5.0)
            if r.status_code == 200:
                country = r.json().get("countryCode", "")
                result["server_country"] = country
                if country in ("NL", "BE", "DE", "LU"):
                    score += 2
                    result["details"].append({"check": "server_location", "passed": True, "value": country})
                else:
                    result["details"].append({"check": "server_location", "passed": False, "value": country})
        except Exception:
            result["details"].append({"check": "server_location", "passed": False})

    # --- PageSpeed (mobile 5 pts, desktop 3 pts) ---
    if PAGESPEED_API_KEY:
        for strategy, max_pts in [("mobile", 5), ("desktop", 3)]:
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    r = await client.get(
                        "https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
                        params={"url": f"https://{domain}", "key": PAGESPEED_API_KEY, "strategy": strategy},
                    )
                    if r.status_code == 200:
                        data = r.json()
                        perf = data.get("lighthouseResult", {}).get("categories", {}).get("performance", {})
                        perf_score = int((perf.get("score") or 0) * 100)
                        result[f"pagespeed_{strategy}"] = perf_score

                        threshold = 50 if strategy == "mobile" else 70
                        if perf_score >= threshold:
                            score += max_pts
                            result["details"].append({"check": f"pagespeed_{strategy}", "passed": True, "value": perf_score})
                        else:
                            result["details"].append({"check": f"pagespeed_{strategy}", "passed": False, "value": perf_score})

                        # Mobile friendly from Lighthouse
                        if strategy == "mobile" and perf_score >= 40:
                            result["mobile_friendly"] = True
                            score += 4  # mobile friendly = 4 pts
            except Exception as e:
                logger.debug("PageSpeed %s failed for %s: %s", strategy, domain, e)
    else:
        result["details"].append({"check": "pagespeed", "passed": False, "note": "PAGESPEED_API_KEY not set"})

    result["technical_score"] = min(score, 25)
    return result


# ---------------------------------------------------------------------------
# CMS fingerprints (reused from website_scraper patterns)
# ---------------------------------------------------------------------------

_CMS_FINGERPRINTS = {
    "WordPress": ["wp-content/", "wp-includes/", "wp-json"],
    "Shopify": ["cdn.shopify.com", "myshopify.com"],
    "Webflow": ["webflow.com", "wf-layout"],
    "Wix": ["wixsite.com", "wix.com", "X-Wix-"],
    "Squarespace": ["squarespace.com", "sqsp.net"],
    "Drupal": ["drupal.js", "drupal.css", 'name="Generator" content="Drupal'],
    "Joomla": ["joomla", "/media/system/js/"],
    "PrestaShop": ["prestashop", "/modules/ps_"],
    "Magento": ["mage/", "Magento_"],
    "HubSpot": ["hubspot.com", "hs-scripts.com"],
    "Ghost": ["ghost.org", "ghost-"],
}


def _detect_cms(html: str) -> str | None:
    """Detect CMS from HTML source. Returns name or None."""
    html_lower = html.lower()
    for cms, patterns in _CMS_FINGERPRINTS.items():
        if any(p.lower() in html_lower for p in patterns):
            return cms
    return None
