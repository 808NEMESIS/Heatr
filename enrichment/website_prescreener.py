"""
enrichment/website_prescreener.py — Quick website validation before expensive analysis.

Prevents wasting Claude credits on parked domains, redirect-only sites,
"coming soon" pages, and aggregator/directory pages.

Called before website_intelligence step in the enrichment pipeline.
Single httpx request, no Playwright, no Claude.
"""
from __future__ import annotations

import logging
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Title patterns that indicate a non-real website
_JUNK_TITLE_PATTERNS = [
    r"domain\s*(is\s*)?(parked|for\s*sale|expired|available)",
    r"coming\s*soon",
    r"under\s*construction",
    r"binnenkort\s*beschikbaar",
    r"deze\s*pagina\s*is\s*niet\s*beschikbaar",
    r"site\s*niet\s*gevonden",
    r"pagina\s*niet\s*gevonden",
    r"default\s*web\s*site\s*page",
    r"apache.*default",
    r"nginx.*welcome",
    r"hosting.*provider",
    r"plesk",
    r"cpanel",
    r"directadmin",
]

# Known redirect-only domains (social/aggregator)
_REDIRECT_DOMAINS = {
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "youtube.com", "linktr.ee", "linktree.com",
}


async def is_real_website(domain: str) -> tuple[bool, str]:
    """
    Quick check: is this domain a real business website worth analyzing?

    Checks:
      1. Domain resolves and returns 200
      2. HTML has >2000 chars of content
      3. Has a meaningful <title> (not "parked", "coming soon", etc.)
      4. Has at least 1 navigation link
      5. Doesn't redirect to a social platform

    Returns:
        (is_real: bool, reason: str)
    """
    try:
        async with httpx.AsyncClient(
            timeout=10.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"},
        ) as client:
            r = await client.get(f"https://{domain}")

            # Check for redirects to social platforms
            final_url = str(r.url).lower()
            for rd in _REDIRECT_DOMAINS:
                if rd in final_url and domain.lower() not in final_url:
                    return False, f"redirects_to_{rd}"

            html = r.text

            # Content length check
            if len(html) < 2000:
                return False, "too_short"

            html_lower = html.lower()

            # Title check
            title_match = re.search(r"<title[^>]*>(.*?)</title>", html_lower, re.DOTALL)
            if title_match:
                title = title_match.group(1).strip()
                for pattern in _JUNK_TITLE_PATTERNS:
                    if re.search(pattern, title, re.IGNORECASE):
                        return False, f"junk_title"
            else:
                # No title at all — suspicious
                pass

            # Navigation check — at least 3 internal links
            internal_links = re.findall(
                r'<a[^>]+href=["\'](?:/|https?://' + re.escape(domain) + r')[^"\']*["\']',
                html_lower,
            )
            if len(internal_links) < 3:
                # Could be a single-page placeholder
                # Check for other content signals
                has_forms = "<form" in html_lower
                has_images = "<img" in html_lower
                word_count = len(re.findall(r"\w{3,}", re.sub(r"<[^>]+>", " ", html_lower)))

                if not has_forms and not has_images and word_count < 100:
                    return False, "thin_content"

            return True, "ok"

    except httpx.TimeoutException:
        return False, "timeout"
    except Exception as e:
        # Try HTTP fallback
        try:
            async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
                r = await client.get(f"http://{domain}")
                if len(r.text) >= 2000:
                    return True, "http_only"
                return False, "too_short_http"
        except Exception:
            return False, f"unreachable"
