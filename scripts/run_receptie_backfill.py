"""
scripts/run_receptie_backfill.py — backfill de receptie-haakje-ladder over de
bestaande leads (Fase 2d).

Draait hook_detector.detect_receptie (2-run, fail-closed) per lead met een domein
en persist de uitkomst naar heatr_website_intelligence (migratie 041). Dry-run
detecteert + rapporteert zonder te schrijven; --apply schrijft.

Vereist dat migratie 041 gedraaid is voor --apply (anders fail-soft False per lead,
detectie draait wel door). READ-ONLY zonder --apply. Geen mail — puur detectie/DB.

  python3 scripts/run_receptie_backfill.py            # dry-run, 500 leads
  python3 scripts/run_receptie_backfill.py --apply --limit 100
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

WORKSPACE = "aerys"
CONCURRENCY = 5


async def _one(sem, sb, pw, browser, lead, apply):
    from website_intelligence.hook_detector import detect_receptie
    from website_intelligence.hook_writer import persist_receptie
    async with sem:
        domain = (lead.get("domain") or "").strip()
        if not domain:
            return {"lead": lead.get("id"), "hook_code": None, "skip": "geen_domein"}
        try:
            result = await detect_receptie(domain, pw, browser=browser)
        except Exception as e:
            return {"lead": lead.get("id"), "hook_code": None, "error": str(e)[:80]}
        persisted = None
        if apply:
            persisted = persist_receptie(sb, lead.get("id"), lead.get("workspace_id") or WORKSPACE, result)
        print(f"  {domain[:40]:40} hook={result.get('hook_code') or '-':4} "
              f"ladder={result.get('hook_ladder')} 2e={result.get('second_hook') or '-'} "
              f"{'WROTE' if persisted else ('FAIL-WRITE' if apply else 'dry')}", flush=True)
        return {"lead": lead.get("id"), "domain": domain, "hook_code": result.get("hook_code"),
                "second_hook": result.get("second_hook"), "persisted": persisted}


async def main(apply: bool = False, limit: int = 500) -> int:
    from config.database import get_heatr_supabase
    from utils.playwright_helpers import new_browser_context
    from playwright.async_api import async_playwright

    sb = get_heatr_supabase()
    rows = (sb.table("leads").select("id, domain, workspace_id, status")
            .eq("workspace_id", WORKSPACE)
            .not_.is_("domain", "null")
            .limit(limit).execute().data or [])
    rows = [r for r in rows if (r.get("domain") or "").strip()]
    mode = "APPLY (schrijft naar WI, vereist migratie 041)" if apply else "DRY-RUN (schrijft niets)"
    print(f"Receptie-backfill over {len(rows)} leads — {mode}. GEEN mail.\n")

    async with async_playwright() as pw:
        browser, _ = await new_browser_context(pw)
        try:
            sem = asyncio.Semaphore(CONCURRENCY)
            res = await asyncio.gather(*[_one(sem, sb, pw, browser, r, apply) for r in rows])
        finally:
            await browser.close()

    hc = Counter(r.get("hook_code") or "GEEN" for r in res)
    mailbaar = sum(1 for r in res if r.get("hook_code"))
    wrote = sum(1 for r in res if r.get("persisted"))
    print(f"\nKlaar: {len(res)} leads · mailbaar {mailbaar} · verdeling {dict(hc)}")
    if apply:
        print(f"Geschreven naar WI: {wrote}/{mailbaar} "
              f"({'migratie 041 lijkt te ontbreken' if wrote == 0 and mailbaar else 'ok'})")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="schrijf naar heatr_website_intelligence")
    ap.add_argument("--limit", type=int, default=500)
    args = ap.parse_args()
    raise SystemExit(asyncio.run(main(apply=args.apply, limit=args.limit)))
