#!/usr/bin/env python3
"""scripts/refresh_pool_measurements.py — pool-brede verversing van site-metingen.

Grondwaarheid-audit 2026-08-24: de opgeslagen conversion_details lopen een
detector-generatie achter (8/19 booking-mismatches in de steekproef, allemaal
stored=False terwijl er wél boeking is). Deze runner ververst de héle
kwaliteitspool via de canonieke meetlaag (httpx→Playwright-fallback, detector v2)
en schrijft het provenance-contract mee. Meeliftend (brug 3): vergelijkt de
opgeslagen bedrijfsnaam met de site-<title> en rapporteert mismatches — flag
voor review, GEEN auto-fix (titels zijn rommelig; auto-fixen is hoe de
artefacten ooit ontstonden).

NB: dit is DB-hygiëne met één meting per lead. De claim-gate vóór verzending
(dubbele fetch + checked_at=vandaag, reverify_conversion_checks) blijft de
strengere laatste stap. DRY-RUN default; --apply schrijft.

  python3 scripts/refresh_pool_measurements.py [--apply]
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv  # noqa: E402
load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
_STOP = {"praktijk", "kliniek", "clinic", "voor", "the", "van", "den", "der", "het"}


def fa(db, t, c):
    o, off = [], 0
    while True:
        d = db.table(t).select(c).eq("workspace_id", WS).range(off, off + 999).execute().data or []
        o += d
        if len(d) < 1000:
            return o
        off += 1000


def name_matches_title(name: str, title: str) -> bool | None:
    """Strak criterium (les uit de audit: 'Nova verbetering' gleed door op één token):
    ≥60% van de betekenisvolle naam-tokens moet in de titel staan. None = geen titel."""
    if not title:
        return None
    toks = [t for t in re.findall(r"\w{3,}", name.lower()) if t not in _STOP]
    if not toks:
        return None
    hit = sum(1 for t in toks if t in title.lower())
    return hit / len(toks) >= 0.6


async def main(apply: bool) -> None:
    from config.database import get_heatr_supabase
    from website_intelligence.measurement import measure_conversion
    from website_intelligence.rendered_fetch import RenderedFetcher
    from utils.legal_form import receptie_avg_safe
    from utils.lead_naming import clean_company_name, display_first_name
    from utils.lead_selection import selection_exclusion

    db = get_heatr_supabase()
    leads = fa(db, "leads", "id,company_name,domain,email,sector,city,contact_first_name,"
               "contact_why_chosen,email_status,score,icp_match,pushed_to_warmr_at,"
               "contact_attempt_count,kvk_legal_form,email_discovery_source")
    wi = {w["lead_id"]: (w.get("conversion_details") or {})
          for w in fa(db, "website_intelligence", "lead_id,conversion_details")}

    pool = []
    for l in leads:
        if (l.get("sector") or "") not in ACTIVE or l.get("email_status") != "valid":
            continue
        if (l.get("score") or 0) < 55 or (l.get("icp_match") or 0) < 0.50:
            continue
        if l.get("pushed_to_warmr_at") or (l.get("contact_attempt_count") or 0):
            continue
        if not display_first_name(l, fallback=""):
            continue
        nm, nr = clean_company_name(l.get("company_name"))
        if not nm or nr:
            continue
        ok, _ = receptie_avg_safe(l)
        if not ok or selection_exclusion(l):
            continue
        if (l.get("domain") or "").strip():
            pool.append(l)
    print(f"kwaliteitspool met domein: {len(pool)} "
          f"({'APPLY' if apply else 'DRY-RUN'})\n")

    sem = asyncio.Semaphore(4)
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    changed = {"booking_F→T": 0, "booking_T→F": 0, "whatsapp": 0, "phone": 0}
    unusable, name_flags, applied = [], [], 0

    async with RenderedFetcher() as rf:
        async def one(l):
            async with sem:
                return l, await measure_conversion(l["domain"], l["sector"], renderer=rf)
        results = await asyncio.gather(*[one(l) for l in pool])

    for l, m in results:
        cd_old = wi.get(l["id"], {})
        nm, _ = clean_company_name(l.get("company_name"))
        if not m["usable"]:
            unusable.append((l, m["provenance"]["reason"]))
            if apply:
                upd = {**cd_old, "reverify_uncertain": True,
                       "reverify_probe": m["provenance"]}
                db.table("website_intelligence").update({"conversion_details": upd}) \
                  .eq("lead_id", l["id"]).eq("workspace_id", WS).execute()
            continue
        fresh = m["result"]
        ob_old, ob_new = cd_old.get("has_online_booking"), fresh.get("has_online_booking")
        if ob_old is False and ob_new is True:
            changed["booking_F→T"] += 1
        if ob_old is True and ob_new is False:
            changed["booking_T→F"] += 1
        if bool(cd_old.get("has_whatsapp")) != bool(fresh.get("has_whatsapp")):
            changed["whatsapp"] += 1
        if bool(cd_old.get("has_phone_clickable")) != bool(fresh.get("has_phone_clickable")):
            changed["phone"] += 1
        tm = name_matches_title(nm, m.get("title") or "")
        if tm is False:
            name_flags.append((nm, m.get("title")))
        if apply:
            upd = {**fresh, "checked_at": now_iso, "reverify_uncertain": False,
                   "reverify_probe": m["provenance"]}
            db.table("website_intelligence").update({"conversion_details": upd}) \
              .eq("lead_id", l["id"]).eq("workspace_id", WS).execute()
            applied += 1

    print(f"metingen: bruikbaar {len(results) - len(unusable)} · onmeetbaar {len(unusable)}")
    print(f"veranderingen t.o.v. stored: {changed}")
    print(f"\nBEDRIJFSNAAM-FLAGS ({len(name_flags)}) — review, geen auto-fix:")
    for nm, title in name_flags:
        print(f"  {nm[:36]:<38} vs title {str(title)[:60]!r}")
    if unusable:
        print(f"\nonmeetbaar (reverify_uncertain=True):")
        for l, reason in unusable[:12]:
            print(f"  {l['company_name'][:36]:<38} ({reason})")
    if apply:
        print(f"\n→ {applied} rijen ververst (detector v2 + provenance) · "
              f"{len(unusable)} op onzeker gezet.")
    Path("logs").mkdir(exist_ok=True)
    Path("logs/pool_refresh_last.json").write_text(json.dumps(
        [{"name": l.get("company_name"), "usable": m["usable"],
          "title": m.get("title"), "prov": m["provenance"],
          "booking": (m["result"] or {}).get("has_online_booking")}
         for l, m in results], ensure_ascii=False, indent=1))
    print("audit → logs/pool_refresh_last.json")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    asyncio.run(main(a.apply))
