"""website_intelligence/rendered_fetch.py — Playwright-fallback voor conversion-metingen.

httpx ziet twee klassen leads niet: botmuren (403/JS-challenge met body) en
JS-gerenderde SPA's (200 met een lege shell — Dr. Penny: 610KB, nul signalen).
Deze module rendert zo'n pagina in een echte headless browser mét de bestaande
anti-detectie (utils/playwright_helpers.new_browser_context: NL-locale, random UA,
webdriver-mask) en geeft (status, gerenderde DOM-HTML) terug, zodat dezelfde
check_conversion/usable_measurement-logica erop kan draaien als op httpx-HTML.

Gebruik als async context manager (één browser per batch, pagina's serieel per
fetch — renders zijn zwaar; de aanroeper regelt concurrency/semafoor):

    async with RenderedFetcher() as rf:
        status, html = await rf.fetch("https://voorbeeld.nl")

Fail-closed: elke fout → (None, "") — de aanroeper behandelt dat als onmeetbaar,
nooit als meting.
"""
from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)

_NAV_TIMEOUT_MS = 25_000
_SETTLE_SECONDS = 2.5          # JS-widgets (boekknoppen!) laden vaak nét na 'load'


class RenderedFetcher:
    """Eén browser voor de hele batch; per fetch een verse page (goedkoop)."""

    def __init__(self, settle_seconds: float = _SETTLE_SECONDS):
        self._settle = settle_seconds
        self._pw = self._browser = self._context = None

    async def __aenter__(self):
        from playwright.async_api import async_playwright
        from utils.playwright_helpers import new_browser_context
        self._pw = await async_playwright().__aenter__()
        self._browser, self._context = await new_browser_context(self._pw)
        return self

    async def __aexit__(self, *exc):
        for closer in (self._context, self._browser):
            try:
                await closer.close()
            except Exception:
                pass
        try:
            await self._pw.__aexit__(*exc)
        except Exception:
            pass

    async def fetch(self, url: str) -> tuple[int | None, str]:
        """Render één URL. Returned (http_status, gerenderde DOM-HTML) of (None, '')
        bij elke fout — fail-closed, aanroeper telt dat als onmeetbaar."""
        if not url.startswith("http"):
            url = f"https://{url}"
        page = None
        try:
            page = await self._context.new_page()
            resp = await page.goto(url, wait_until="load", timeout=_NAV_TIMEOUT_MS)
            await asyncio.sleep(self._settle)              # JS-nalevering (widgets)
            html = await page.content()
            status = resp.status if resp else None
            return status, html or ""
        except Exception as e:
            logger.info("rendered_fetch: %s faalde (%s)", url, str(e)[:80])
            return None, ""
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
