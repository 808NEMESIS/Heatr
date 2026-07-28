"""
scripts/recover_owner_names.py — voornaam-recovery aan de bron (backlog-blocker).

Voor leads zonder bruikbare voornaam: render de over-ons/team-pagina (Playwright,
zoals productie), draai de bestaande owner_extractor (Haiku, rol-signaal), en
schrijf ALLEEN een geldige eigenaars-voornaam terug (safe_first_name-gate → nooit
junk/domein/initiaal). Verbetert de bron i.p.v. het e-mail-local-part te pakken.

Gemeten recovery op een cosmetic-sample: ~32% (rendered) vs ~12% (statisch) — de
rest heeft geen vindbare eigenaarsnaam op de site (booking-microsite / alleen
behandelingen) → blijft "Hallo," (permanent, geen bug).

  python3 scripts/recover_owner_names.py                 # dry-run, 200 leads
  python3 scripts/recover_owner_names.py --apply --limit 400 --sector cosmetische_behandelaars
"""
from __future__ import annotations
import argparse, asyncio, os, re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv; load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

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


async def _one(sem, pw, br, sb, client, lead, apply):
    from utils.playwright_helpers import mobile_context
    from website_intelligence.hook_detector import _dismiss_cookie_banner
    from enrichment.owner_extractor import extract_team_from_page_text
    from utils.lead_naming import safe_first_name
    async with sem:
        dom = (lead.get("domain") or "").strip()
        company = lead.get("company_name") or dom
        if not dom:
            return {"id": lead.get("id"), "rec": None}
        ctx = await mobile_context(br, pw); page = await ctx.new_page(); text = ""
        try:
            for sch in ("https://", "http://"):
                t = await _render(page, sch + dom, _dismiss_cookie_banner)
                if t:
                    text = t; break
            try:
                links = await page.eval_on_selector_all("a", "els=>els.map(a=>a.href).filter(h=>h)")
                over = next((h for h in links if _OVER.search(h) and dom.split(".")[0] in h), None)
                if over:
                    t2 = await _render(page, over, _dismiss_cookie_banner)
                    if t2:
                        text += " " + t2
            except Exception:
                pass
        finally:
            await ctx.close()
        if len(text) < 150:
            return {"id": lead.get("id"), "dom": dom, "rec": None}
        try:
            team = await extract_team_from_page_text(company, lead.get("sector") or "", text[:8000],
                                                     WORKSPACE, sb, client, lead_id=str(lead.get("id")))
        except Exception:
            return {"id": lead.get("id"), "dom": dom, "rec": None}
        # Bron-prioriteit-resolver (Source 1 bedrijfsnaam + Source 2 over-ons/rol-
        # hiërarchie); nooit domein/e-mail-local-part/initiaal. Zelfde logica als de
        # recovery-meting (measure_name_recovery.py).
        from enrichment.owner_name_resolver import resolve_owner_first_name
        valid, source, detail = resolve_owner_first_name(lead, team=team)
        wrote = False
        if valid and apply:
            try:
                sb.table("leads").update({
                    "contact_first_name": valid,
                    "contact_source": f"owner_resolver:{source}",
                    "contact_why_chosen": f"Eigenaarsvoornaam via {source} ({detail}) — bron-prioriteit recovery",
                }).eq("id", lead.get("id")).execute()
                wrote = True
            except Exception:
                pass
        print(f"  {dom[:34]:34} -> {valid or '(geen)':16} [{source}] {'WROTE' if wrote else ('dry' if valid else '')}", flush=True)
        return {"id": lead.get("id"), "dom": dom, "rec": valid or None, "source": source, "wrote": wrote}


async def main(apply=False, limit=200, sector=None):
    from config.database import get_heatr_supabase
    from utils.playwright_helpers import new_browser_context
    from utils.lead_naming import safe_first_name
    from playwright.async_api import async_playwright
    import anthropic
    sb = get_heatr_supabase()
    q = sb.table("leads").select("id,domain,company_name,sector,contact_first_name,email").eq("workspace_id", WORKSPACE).not_.is_("domain", "null")
    if sector:
        q = q.eq("sector", sector)
    rows = (q.limit(limit * 3).execute().data) or []
    # alleen no-name leads
    noname = [r for r in rows if r.get("domain") and not safe_first_name(r)][:limit]
    print(f"Voornaam-recovery over {len(noname)} no-name leads — {'APPLY' if apply else 'DRY-RUN'}. Geen mail.\n")
    client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    async with async_playwright() as pw:
        br, _ = await new_browser_context(pw)
        try:
            sem = asyncio.Semaphore(CONC)
            res = await asyncio.gather(*[_one(sem, pw, br, sb, client, l, apply) for l in noname])
        finally:
            await br.close()
    rec = [r for r in res if r.get("rec")]
    wrote = sum(1 for r in res if r.get("wrote"))
    print(f"\nKlaar: {len(res)} leads · recovered {len(rec)} ({100*len(rec)//max(1,len(res))}%)"
          + (f" · geschreven {wrote}" if apply else " (dry-run, niets geschreven)"))
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--sector", default=None)
    a = ap.parse_args()
    raise SystemExit(asyncio.run(main(apply=a.apply, limit=a.limit, sector=a.sector)))
