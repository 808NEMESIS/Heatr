"""scripts/measure_name_recovery.py — recovery-meting voornaam-enrichment (Fase 1).

Draait de nieuwe owner_name_resolver op no-name cosmetische leads en meet hoeveel een
BETROUWBARE eigenaarsvoornaam krijgen, per bron:
  - company_name  (Source 1, gratis: persoonsnaam in de bedrijfsnaam)
  - over_ons_role (Source 2: over-ons/team renderen + owner_extractor + rol-hiërarchie)
  - (linkedin = Source 3: data niet beschikbaar, altijd 0)

Rapporteert per bron + de reden voor onbepaald (geen over-ons-pagina / wel pagina maar
geen rolsignaal / meerdere gelijke kandidaten). Schrijft NIETS terug (puur meten) en een
JSON-detailbestand voor de blind-verificatie. Geen mail.

  python3 scripts/measure_name_recovery.py --limit 40 --out <pad.json>
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv  # noqa: E402
load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WORKSPACE = "aerys"
CONC = 4
_OVER = re.compile(r"(over-ons|over-mij|over_ons|/team|ons-team|wie-zijn|medewerkers|specialisten|artsen|behandelaars|/contact)", re.I)


async def _render(page, url, dismiss):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await dismiss(page)
        try:
            await page.wait_for_load_state("networkidle", timeout=4000)
        except Exception:
            pass
        return (await page.inner_text("body"))[:6000]
    except Exception:
        return ""


async def _one(sem, pw, br, sb, client, lead):
    from utils.playwright_helpers import mobile_context
    from website_intelligence.hook_detector import _dismiss_cookie_banner
    from enrichment.owner_extractor import extract_team_from_page_text
    from enrichment.owner_name_resolver import first_name_from_company, resolve_owner_first_name
    async with sem:
        dom = (lead.get("domain") or "").strip()
        company = lead.get("company_name") or dom
        # Source 1 kan zonder render (gratis) — check meteen.
        s1 = first_name_from_company(company)

        text, over_found = "", False
        if dom:
            ctx = await mobile_context(br, pw); page = await ctx.new_page()
            try:
                for sch in ("https://", "http://"):
                    t = await _render(page, sch + dom, _dismiss_cookie_banner)
                    if t:
                        text = t; break
                try:
                    links = await page.eval_on_selector_all("a", "els=>els.map(a=>a.href).filter(h=>h)")
                    over = next((h for h in links if _OVER.search(h) and dom.split(".")[0] in h), None)
                    if over:
                        over_found = True
                        t2 = await _render(page, over, _dismiss_cookie_banner)
                        if t2:
                            text += " " + t2
                except Exception:
                    pass
            finally:
                await ctx.close()

        team = []
        if len(text) >= 150:
            try:
                team = await extract_team_from_page_text(company, lead.get("sector") or "", text[:8000],
                                                         WORKSPACE, sb, client, lead_id=str(lead.get("id"))) or []
            except Exception:
                team = []

        fn, source, detail = resolve_owner_first_name(lead, team=team)
        # onbepaald-reden verfijnen met render-context
        if source == "none":
            if not s1 and len(text) < 150:
                detail = "geen_site_tekst"
            elif not s1 and not over_found:
                detail = "geen_over_ons_pagina"
            elif not s1 and not team:
                detail = "over_ons_maar_geen_namen"
        print(f"  {dom[:32]:32} -> {(fn or '(onbepaald)'):16} [{source}] {detail}", flush=True)
        return {"id": str(lead.get("id")), "domain": dom, "company": company,
                "first_name": fn, "source": source, "detail": detail,
                "text_len": len(text), "team_names": [t.get("first_name") for t in team]}


async def main(limit: int, out: str | None) -> int:
    from config.database import get_heatr_supabase
    from utils.playwright_helpers import new_browser_context
    from utils.lead_naming import safe_first_name
    from playwright.async_api import async_playwright
    import anthropic

    sb = get_heatr_supabase()
    rows = (sb.table("leads").select("id,domain,company_name,sector,contact_first_name,email")
            .eq("workspace_id", WORKSPACE).eq("sector", "cosmetische_behandelaars")
            .not_.is_("domain", "null").limit(limit * 4).execute().data) or []
    noname = [r for r in rows if r.get("domain") and not safe_first_name(r)][:limit]
    print(f"Recovery-meting over {len(noname)} no-name cosmetische leads. Geen mail, niets geschreven.\n")

    client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    async with async_playwright() as pw:
        br, _ = await new_browser_context(pw)
        try:
            sem = asyncio.Semaphore(CONC)
            res = await asyncio.gather(*[_one(sem, pw, br, sb, client, l) for l in noname])
        finally:
            await br.close()

    by_source = Counter(r["source"] for r in res)
    found = [r for r in res if r["first_name"]]
    onbepaald_reasons = Counter(r["detail"] for r in res if r["source"] == "none")
    n = len(res)
    print(f"\n{'='*60}\nRECOVERY over {n} leads:")
    print(f"  company_name  : {by_source.get('company_name',0)}")
    print(f"  over_ons_role : {by_source.get('over_ons_role',0)}")
    print(f"  linkedin      : 0 (data niet beschikbaar)")
    print(f"  TOTAAL gevonden: {len(found)}/{n} ({100*len(found)//max(1,n)}%)")
    print(f"\nONBEPAALD ({by_source.get('none',0)}) — redenen:")
    for reason, c in onbepaald_reasons.most_common():
        print(f"  {c:2}  {reason}")
    if out:
        Path(out).write_text(json.dumps(res, ensure_ascii=False, indent=1))
        print(f"\ndetails -> {out} (voor blind-verificatie)")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    raise SystemExit(asyncio.run(main(a.limit, a.out)))
