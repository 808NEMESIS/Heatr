"""scripts/integration_health.py — A1: dagelijkse gezondheid van alle externe integraties.

Aanleiding (audit-programma 2026-08): Bouncer-tegoed-op en het uitgefaseerde KvK-v1-
endpoint waren toevalstreffers — externe contracten rotten stil. Dit script probet elke
integratie read-only en maakt "stil kapot" binnen een dag zichtbaar.

Statussen: OK · DEGRADED (dienst leeft, onze toegang/tegoed niet) · DOWN (onbereikbaar).
Exit-code: 0 = alles OK · 1 = ergens DEGRADED · 2 = ergens DOWN.

Gebruik:
    python3 scripts/integration_health.py                # volledige run (incl. browser-smoke)
    python3 scripts/integration_health.py --skip-maps    # zonder Playwright (sneller, voor cron)
Cron-voorstel (dagelijks 07:45):
    45 7 * * * cd /Users/nemesis/Heatr && /usr/bin/python3 scripts/integration_health.py --skip-maps >> logs/integration-health.log 2>&1
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))
import httpx

OK, DEGRADED, DOWN = "OK", "DEGRADED", "DOWN"


async def _get(url: str, headers: dict | None = None, timeout: float = 15.0, params: dict | None = None):
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as c:
        return await c.get(url, headers=headers or {}, params=params or {})


async def check_bouncer() -> tuple[str, str]:
    key = os.getenv("BOUNCER_API_KEY", "").strip()
    if not key:
        return DEGRADED, "BOUNCER_API_KEY leeg"
    try:
        r = await _get("https://api.usebouncer.com/v1.1/credits", headers={"x-api-key": key})
    except Exception as e:
        return DOWN, f"onbereikbaar: {type(e).__name__}"
    if r.status_code != 200:
        return DOWN, f"HTTP {r.status_code}"
    credits = (r.json() or {}).get("credits", 0)
    if not credits:
        return DEGRADED, "tegoed = 0 — verificatie ligt stil (opwaarderen op usebouncer.com)"
    return OK, f"tegoed: {credits}"


async def check_kvk() -> tuple[str, str]:
    key = os.getenv("KVK_API_KEY", "").strip()
    if not key:
        return DEGRADED, "KVK_API_KEY leeg"
    try:
        r = await _get("https://api.kvk.nl/api/v2/zoeken", headers={"apikey": key}, params={"naam": "test"})
    except Exception as e:
        return DOWN, f"onbereikbaar: {type(e).__name__}"
    if r.status_code in (401, 403):
        return DEGRADED, f"HTTP {r.status_code} — Zoeken-abonnement niet actief voor deze key (developers.kvk.nl)"
    if r.status_code != 200:
        return DOWN, f"HTTP {r.status_code}"
    return OK, "zoeken-API bereikbaar + key geldig"


async def check_pagespeed() -> tuple[str, str]:
    key = os.getenv("PAGESPEED_API_KEY", "").strip()
    if not key:
        return DEGRADED, "PAGESPEED_API_KEY leeg"
    try:
        r = await _get("https://www.googleapis.com/pagespeedonline/v5/runPagespeed",
                       params={"url": "https://example.com", "key": key, "category": "performance"},
                       timeout=60.0)
    except Exception as e:
        return DOWN, f"onbereikbaar: {type(e).__name__}"
    if r.status_code == 200:
        return OK, "key geldig, analyse draait"
    if r.status_code in (400, 403, 429):
        return DEGRADED, f"HTTP {r.status_code} — key/quota-probleem"
    return DOWN, f"HTTP {r.status_code}"


async def check_anthropic() -> tuple[str, str]:
    key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not key:
        return DOWN, "ANTHROPIC_API_KEY leeg — enrichment ligt stil"
    try:
        r = await _get("https://api.anthropic.com/v1/models",
                       headers={"x-api-key": key, "anthropic-version": "2023-06-01"})
    except Exception as e:
        return DOWN, f"onbereikbaar: {type(e).__name__}"
    return (OK, "auth geldig") if r.status_code == 200 else (DEGRADED, f"HTTP {r.status_code}")


def check_supabase() -> tuple[str, str]:
    try:
        from config.database import get_heatr_supabase
        db = get_heatr_supabase()
        db.table("leads").select("id").limit(1).execute()
    except Exception as e:
        return DOWN, f"REST faalt: {str(e)[:80]}"
    try:
        bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "screenshots")
        db.storage.from_(bucket).list(path="", options={"limit": 1})  # type: ignore[attr-defined]
        return OK, "REST + storage bereikbaar"
    except Exception as e:
        return DEGRADED, f"REST ok, storage faalt: {str(e)[:60]}"


async def check_warmr() -> tuple[str, str]:
    try:
        from integrations.warmr_client import WarmrClient
        inboxes = await WarmrClient().get_ready_inboxes()
    except Exception as e:
        return DOWN, f"API onbereikbaar: {str(e)[:80]}"
    if not inboxes:
        return DEGRADED, "bereikbaar maar 0 ready-inboxes"
    cap = sum(i.get("daily_campaign_target") or 0 for i in inboxes)
    return OK, f"{len(inboxes)} ready-inboxes, capaciteit {cap}/dag"


async def check_unsubscribe_host() -> tuple[str, str]:
    url = "https://www.aeryssolution.nl"
    try:
        r = await _get(url)
    except Exception as e:
        return DOWN, f"afmeld-host onbereikbaar: {type(e).__name__}"
    return (OK, f"HTTP {r.status_code}") if r.status_code < 500 else (DOWN, f"HTTP {r.status_code}")


async def check_local(url: str, name: str) -> tuple[str, str]:
    try:
        r = await _get(url, timeout=5.0)
    except Exception:
        return DOWN, f"{name} niet bereikbaar op {url}"
    return OK, f"HTTP {r.status_code}"


async def check_maps_browser() -> tuple[str, str]:
    """Smoke: browser start + Maps laadt. Selector-rot wordt door queue-health gedekt."""
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            b = await p.chromium.launch(headless=True)
            pg = await b.new_page()
            await pg.goto("https://www.google.com/maps", timeout=30000, wait_until="domcontentloaded")
            title = await pg.title()
            await b.close()
        return (OK, "chromium + maps laadt") if "maps" in title.lower() or "google" in title.lower() \
            else (DEGRADED, f"onverwachte titel: {title[:40]}")
    except Exception as e:
        return DOWN, f"browser-smoke faalt: {str(e)[:80]}"


async def main(skip_maps: bool) -> int:
    checks: list[tuple[str, tuple[str, str]]] = []
    checks.append(("bouncer", await check_bouncer()))
    checks.append(("kvk", await check_kvk()))
    checks.append(("pagespeed", await check_pagespeed()))
    checks.append(("anthropic", await check_anthropic()))
    checks.append(("supabase", check_supabase()))
    checks.append(("warmr_api", await check_warmr()))
    checks.append(("afmeld_host", await check_unsubscribe_host()))
    checks.append(("heatr_api", await check_local("http://localhost:8001/healthz", "Heatr-API")))
    checks.append(("n8n", await check_local("http://localhost:5678", "n8n")))
    if not skip_maps:
        checks.append(("maps_browser", await check_maps_browser()))

    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    worst = 0
    print(f"INTEGRATIE-GEZONDHEID — {stamp}")
    for name, (status, detail) in checks:
        icon = {"OK": "✅", "DEGRADED": "🟠", "DOWN": "🔴"}[status]
        print(f"  {icon} {name:<14} {status:<9} {detail}")
        worst = max(worst, {"OK": 0, "DEGRADED": 1, "DOWN": 2}[status])
    print(f"eind-verdict: {['ALLES OK', 'DEGRADED — actie nodig', 'DOWN — blokkerend'][worst]}")
    return worst


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-maps", action="store_true", help="sla de Playwright-smoke over (sneller, cron)")
    args = ap.parse_args()
    raise SystemExit(asyncio.run(main(args.skip_maps)))
