#!/usr/bin/env python3
"""scripts/reverify_conversion_checks.py — pre-send waarheidscheck van conversion_checks.

De vers-gate (is_friction_fresh) checkt of een meting RECENT is, niet of hij nog KLOPT.
Een site die zes weken geleden geen boekknop had en er vorige week een kreeg, is "vers"
maar onwaar. Deze runner haalt de homepage opnieuw op (httpx, zoals de analyzer),
draait check_conversion en vergelijkt de verse `has_online_booking` met de opgeslagen —
zodat een achterhaalde frictieclaim NOOIT de deur uit gaat.

DRY-RUN default (READ-ONLY): rapporteert alleen de delta. Met --apply schrijft 'ie de
verse conversion_details + een `checked_at`-stempel terug naar heatr_website_intelligence
(gated write — draai alleen na expliciete go). Ophalen is ~0.4s/lead, dus 25 = ~10s:
draai 'm de ochtend van de verzending.

Gebruik: python3 scripts/reverify_conversion_checks.py [--apply] [--limit N] [--sector S]
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import httpx

from config.database import get_heatr_supabase
from campaigns.frictie_copywriter import select_leak
from utils.legal_form import receptie_avg_safe
from utils.lead_naming import clean_company_name, display_first_name
from website_intelligence.conversion_checker import check_conversion

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
_UA = {"User-Agent": "Mozilla/5.0 (compatible; AerysBot/1.0; +https://aeryssolution.nl)"}


def _fetch_all(db, table, cols):
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).eq("workspace_id", WS).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def _launchable(l, sector_filter):
    if sector_filter and l.get("sector") != sector_filter:
        return False
    return (l.get("email_status") == "valid" and (l.get("score") or 0) >= 55
            and (l.get("icp_match") or 0) >= 0.50 and (l.get("sector") or "") in ACTIVE
            and not l.get("pushed_to_warmr_at") and not (l.get("contact_attempt_count") or 0))


async def _get(dom: str) -> str:
    url = dom if dom.startswith("http") else f"https://{dom}"
    for u in (url, url.replace("https://", "http://")):
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers=_UA) as c:
                r = await c.get(u)
                if r.text:
                    return r.text
        except Exception:
            continue
    return ""


async def _double_check(sem, l):
    """Twee onafhankelijke fetches + check_conversion. Returned de status:
    'confirmed_no_booking' (beide geen-boeking), 'now_booking' (beide boeking) of
    'uncertain' (fetches oneens of een fout) — plus de verse conversion_details."""
    dom = (l.get("domain") or "").strip()
    async with sem:
        html1 = await _get(dom)
        html2 = await _get(dom)
    if not html1 or not html2:
        return l, "uncertain", None
    f1 = await check_conversion(dom, html1, l["sector"])
    f2 = await check_conversion(dom, html2, l["sector"])
    b1 = f1.get("has_online_booking") is False
    b2 = f2.get("has_online_booking") is False
    if b1 and b2:
        return l, "confirmed_no_booking", f1
    if (not b1) and (not b2):
        return l, "now_booking", f1
    return l, "uncertain", None


async def run(apply: bool, limit: int, sector_filter: str | None):
    db = get_heatr_supabase()
    leads = {l["id"]: l for l in _fetch_all(db, "leads",
             "id,company_name,domain,sector,email_status,score,icp_match,pushed_to_warmr_at,"
             "contact_attempt_count,kvk_legal_form,email_discovery_source,contact_first_name,"
             "contact_why_chosen,email")}
    wi = {w["lead_id"]: (w.get("conversion_details") or {})
          for w in _fetch_all(db, "website_intelligence", "lead_id,conversion_details")}

    # Doel = Frame-A-cohort: launchbaar + voornaam + schone naam + AVG + opgeslagen geen-boeking.
    targets = []
    for lid, l in leads.items():
        if not _launchable(l, sector_filter):
            continue
        if not display_first_name(l, fallback=""):
            continue
        n, nr = clean_company_name(l.get("company_name"))
        if not n or nr:
            continue
        ok, _r = receptie_avg_safe(l)
        if not ok:
            continue
        if select_leak(wi.get(lid, {})) is None:
            continue
        targets.append(l)
    targets = targets[:limit] if limit else targets
    print(f"Herverificatie van {len(targets)} Frame-A-kandidaten "
          f"({'APPLY — schrijft naar prod' if apply else 'DRY-RUN — read-only'}).\n")

    sem = asyncio.Semaphore(8)
    results = await asyncio.gather(*[_double_check(sem, l) for l in targets])
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()

    confirmed, now_booking, uncertain, applied = 0, 0, 0, 0
    for l, status, fresh in results:
        lid = l["id"]
        if status == "confirmed_no_booking":
            confirmed += 1
            update = {**fresh, "checked_at": now_iso}
        elif status == "now_booking":
            now_booking += 1
            print(f"  NU-BOEKING   {l['company_name'][:40]}  (frictieclaim vervallen → Frame C)")
            update = {**fresh, "checked_at": now_iso}
        else:  # uncertain: twee fetches oneens of fout → geen claim
            uncertain += 1
            print(f"  ONZEKER      {l['company_name'][:40]}  (fetches oneens/fout → Frame C)")
            update = None
        if apply:
            if update is None:
                # markeer onzeker zonder de opgeslagen meting te overschrijven
                cur = dict(wi.get(lid, {})); cur["reverify_uncertain"] = True; cur["checked_at"] = now_iso
                update = cur
            else:
                update = {**update, "reverify_uncertain": False}
            db.table("website_intelligence").update({"conversion_details": update}) \
              .eq("lead_id", lid).eq("workspace_id", WS).execute()
            applied += 1

    frame_a = confirmed
    print(f"\nSAMENVATTING — Frame-A-waardig (2x geen-boeking): {frame_a} · "
          f"nu wél boeking: {now_booking} · onzeker: {uncertain}")
    print(f"  → cohort = de {frame_a} bevestigde; {now_booking + uncertain} vallen terug op Frame C / eruit.")
    if apply:
        print(f"  → {applied} rijen bijgewerkt (conversion_details + checked_at + reverify_uncertain).")
    else:
        print("  → DRY-RUN: niets geschreven. Draai met --apply (na go) om de verse data te persisteren.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="schrijf verse conversion_details terug (gated)")
    ap.add_argument("--limit", type=int, default=0, help="max aantal leads")
    ap.add_argument("--sector", type=str, default="alternatieve_geneeskunde", help="sector-filter ('' = alle)")
    a = ap.parse_args()
    asyncio.run(run(a.apply, a.limit, a.sector or None))


if __name__ == "__main__":
    main()
