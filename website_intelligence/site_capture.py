"""
website_intelligence/site_capture.py — het enige capture-pad van de crawl.

Draait ONGATED in analyze_website (los van enable_vision). Legt per scan vast wat
de audit-scorer nodig heeft en de oude crawl niet opsloeg:
  - screenshots desktop (1440x900) + mobiel (iPhone 13, device-emulatie), beide
    full-page, als WebP q80;
  - het pre/post-consent netwerkgedrag op een VERSE context zonder interactie
    (geen klik/scroll/cookie-accept); elke request krijgt een fase pre_freeze of
    post_freeze rond het vries-moment (networkidle of load+3s). Post-load trackers
    worden bewust NIET afgekapt — die zijn juist het signaal.
  - content-hashes (DOM-tekst + screenshots) voor rescan-skip.

Never-raise: een capture-fout mag de scan/score nooit breken. De caller
(analyze_website) roept dit fail-soft aan.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import os
import time
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_DESKTOP_VIEWPORT = {"width": 1440, "height": 900}
_NAV_TIMEOUT_MS = 20_000
_FREEZE_SETTLE_MS = 3_000  # networkidle binnen 3s, anders effectief load+3s
_WEBP_QUALITY = 80


def _host(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def _is_third_party(url: str, site_host: str) -> bool:
    """True als de request-host niet het site-domein of een subdomein ervan is."""
    h = _host(url)
    if not h:
        return False
    h = h[4:] if h.startswith("www.") else h
    s = site_host[4:] if site_host.startswith("www.") else site_host
    if not s:
        return False
    return not (h == s or h.endswith("." + s))


def _build_network(raw: list[dict], nav_start: float, freeze_ts: float, site_host: str) -> tuple[list[dict], dict]:
    """Zet de ruwe listener-entries om naar persisteerbare requests + summary.

    Fase = pre_freeze als de request vóór het vries-moment vuurde, anders
    post_freeze. Alles blijft behouden (niet afkappen).
    """
    reqs: list[dict] = []
    for e in raw:
        t_req = e.get("t_request", nav_start)
        reqs.append({
            "url": e.get("url"),
            "method": e.get("method"),
            "resource_type": e.get("resource_type"),
            "status": e.get("status"),
            "failed": bool(e.get("failed")),
            "timing_ms": round((t_req - nav_start) * 1000),
            "is_third_party": _is_third_party(e.get("url", ""), site_host),
            "phase": "pre_freeze" if t_req <= freeze_ts else "post_freeze",
        })
    tp = [r for r in reqs if r["is_third_party"]]
    summary = {
        "total": len(reqs),
        "third_party": len(tp),
        "pre_freeze": sum(1 for r in reqs if r["phase"] == "pre_freeze"),
        "post_freeze": sum(1 for r in reqs if r["phase"] == "post_freeze"),
        # De kern-metriek voor de pre-consent tracking-check:
        "third_party_pre_freeze": sum(1 for r in tp if r["phase"] == "pre_freeze"),
        "third_party_post_freeze": sum(1 for r in tp if r["phase"] == "post_freeze"),
    }
    return reqs, summary


def _upload_webp(supabase_client: Any, object_key: str, data: bytes) -> str | None:
    """Upload WebP-bytes naar Storage en geef de publieke URL terug (of None)."""
    bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "screenshots")
    key = f"captures/{object_key}"
    try:
        supabase_client.storage.from_(bucket).upload(
            path=key, file=data,
            file_options={"content-type": "image/webp", "upsert": "true"},
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("site_capture: upload faalde (%s): %s", key, e)
        return None
    base = os.getenv("SUPABASE_URL", "").rstrip("/")
    return f"{base}/storage/v1/object/public/{bucket}/{key}"


async def _goto_with_fallback(page: Any, domain: str) -> None:
    """Navigeer naar https, val terug op http. Geen interactie."""
    try:
        await page.goto(f"https://{domain}", wait_until="load", timeout=_NAV_TIMEOUT_MS)
        return
    except Exception as e:
        logger.debug("site_capture: https-goto faalde voor %s: %s", domain, e)
    await page.goto(f"http://{domain}", wait_until="load", timeout=_NAV_TIMEOUT_MS)


async def _screenshot_webp(page: Any) -> bytes:
    """Full-page PNG via Playwright -> WebP q80 via Pillow (Playwright doet geen WebP)."""
    from utils.playwright_helpers import encode_webp
    png = await page.screenshot(full_page=True, type="png")
    return encode_webp(png, _WEBP_QUALITY)


async def capture_site(
    domain: str,
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Capture screenshots + netwerkgedrag + hashes voor één domein. Never-raise.

    Returns een dict met (o.a.): network, network_summary, dom_text_hash,
    screenshot_desktop_url/hash, screenshot_mobile_url/hash, timings, en
    _desktop_webp_bytes/_mobile_webp_bytes (voor meting). 'error' bij falen.
    """
    from playwright.async_api import async_playwright
    from utils.playwright_helpers import new_browser_context, mobile_context

    screenshots_enabled = os.getenv("SCREENSHOT_ENABLED", "true").lower() == "true"
    site_host = (domain or "").lower()
    result: dict[str, Any] = {
        "domain": domain, "network": [], "network_summary": {},
        "dom_text_hash": None,
        "screenshot_desktop_url": None, "screenshot_mobile_url": None,
        "screenshot_desktop_hash": None, "screenshot_mobile_hash": None,
        "timings": {}, "error": None,
    }
    t0 = time.monotonic()
    browser = None
    try:
        async with async_playwright() as pw:
            browser, ctx = await new_browser_context(pw, capture_network=True)
            net_log = getattr(ctx, "heatr_network_log", [])
            page = await ctx.new_page()
            await page.set_viewport_size(_DESKTOP_VIEWPORT)

            nav_start = time.monotonic()
            await _goto_with_fallback(page, domain)
            # Vries-moment: networkidle binnen 3s, anders load+3s.
            try:
                await page.wait_for_load_state("networkidle", timeout=_FREEZE_SETTLE_MS)
            except Exception:
                pass
            freeze_ts = time.monotonic()
            result["timings"]["nav_ms"] = round((freeze_ts - nav_start) * 1000)

            # DOM-tekst-hash (stabieler dan screenshot-hash voor rescan-skip).
            try:
                dom_text = await page.inner_text("body")
                result["dom_text_hash"] = hashlib.sha256(dom_text.encode("utf-8", "ignore")).hexdigest()
            except Exception as e:
                logger.debug("site_capture: dom-hash faalde %s: %s", domain, e)

            if screenshots_enabled:
                ts = time.monotonic()
                try:
                    webp = await _screenshot_webp(page)
                    result["screenshot_desktop_hash"] = hashlib.sha256(webp).hexdigest()
                    result["screenshot_desktop_url"] = _upload_webp(supabase_client, f"{domain}/desktop.webp", webp)
                    result["_desktop_webp_bytes"] = len(webp)
                    # In-memory (niet gepersisteerd) — Vision (Laag 2) hergebruikt
                    # deze bytes in dezelfde run i.p.v. zelf te capturen.
                    result["screenshot_desktop_b64"] = base64.b64encode(webp).decode("ascii")
                except Exception as e:
                    logger.warning("site_capture: desktop-screenshot faalde %s: %s", domain, e)
                result["timings"]["desktop_shot_ms"] = round((time.monotonic() - ts) * 1000)

                ts = time.monotonic()
                mctx = None
                try:
                    mctx = await mobile_context(browser, pw)
                    mpage = await mctx.new_page()
                    await _goto_with_fallback(mpage, domain)
                    mwebp = await _screenshot_webp(mpage)
                    result["screenshot_mobile_hash"] = hashlib.sha256(mwebp).hexdigest()
                    result["screenshot_mobile_url"] = _upload_webp(supabase_client, f"{domain}/mobile.webp", mwebp)
                    result["_mobile_webp_bytes"] = len(mwebp)
                    result["screenshot_mobile_b64"] = base64.b64encode(mwebp).decode("ascii")
                except Exception as e:
                    logger.warning("site_capture: mobiel-screenshot faalde %s: %s", domain, e)
                finally:
                    if mctx is not None:
                        try:
                            await mctx.close()
                        except Exception:
                            pass
                result["timings"]["mobile_shot_ms"] = round((time.monotonic() - ts) * 1000)

            # Netwerk-log bevriezen NA de captures: post-load/-scroll requests
            # blijven behouden en krijgen fase post_freeze.
            result["network"], result["network_summary"] = _build_network(
                list(net_log), nav_start, freeze_ts, site_host,
            )
            try:
                await ctx.close()
            except Exception:
                pass
    except Exception as e:  # noqa: BLE001 - never-raise
        logger.error("site_capture: onverwacht gefaald voor %s: %s", domain, e)
        result["error"] = str(e)[:200]
    finally:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
    result["timings"]["total_ms"] = round((time.monotonic() - t0) * 1000)
    return result
