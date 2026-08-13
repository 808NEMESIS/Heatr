#!/usr/bin/env python3
"""scripts/preview_sendpath.py — render mail 1 via het ECHTE send-pad (read-only).

Gebruikt render_faseA_marker → _render_receptie_marker → _try_frictie_mail, dus álle
gates tellen mee: reverify_uncertain (revert), de checked_at-vers-gate, AVG, suppressie.
Toont per lead of het Frame A / Frame C / GEBLOKKEERD wordt en waarom. Verstuurt niets;
kill-switch onaangeroerd.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

# compliance-tokens zoals in prod (render-only)
os.environ["RECEPTIE_PRIVACY_NOTICE"] = ("Je ontvangt deze mail omdat je praktijk openbaar "
                                         "vindbaar is; zie aeryssolution.nl/privacy")
os.environ["RECEPTIE_UNSUBSCRIBE_VIA_WARMR"] = "true"

from config.database import get_heatr_supabase
from campaigns.frictie_copywriter import niche_for_sector, select_leak
from campaigns.sequence_engine import render_faseA_marker
from utils.legal_form import receptie_avg_safe
from utils.lead_naming import clean_company_name, display_first_name

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
MARK = {"faseA_brug": "receptie", "faseA_step": 0, "delay_days": 0}


def _fetch_all(db, table, cols):
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).eq("workspace_id", WS).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def _launchable(l):
    return (l.get("email_status") == "valid" and (l.get("score") or 0) >= 55
            and (l.get("icp_match") or 0) >= 0.50 and (l.get("sector") or "") in ACTIVE
            and not l.get("pushed_to_warmr_at") and not (l.get("contact_attempt_count") or 0))


async def main():
    db = get_heatr_supabase()
    leads = _fetch_all(db, "leads",
                       "id,company_name,domain,email,sector,city,contact_first_name,archetype,"
                       "contact_why_chosen,email_status,score,icp_match,pushed_to_warmr_at,"
                       "contact_attempt_count,kvk_legal_form,email_discovery_source")
    wi = {w["lead_id"]: (w.get("conversion_details") or {})
          for w in _fetch_all(db, "website_intelligence", "lead_id,conversion_details")}

    picks = []
    for l in leads:
        if not _launchable(l) or not display_first_name(l, fallback=""):
            continue
        n, nr = clean_company_name(l.get("company_name"))
        if not n or nr:
            continue
        ok, _r = receptie_avg_safe(l)
        if not ok:
            continue
        if (wi.get(l["id"], {})).get("has_online_booking") is not False:
            continue
        picks.append(l)
    picks.sort(key=lambda l: 0 if niche_for_sector(l["sector"]) == "alt" else 1)

    cats = {"Frame A": [], "Frame C": [], "GEBLOKKEERD": [], "anders": []}
    samples = {}
    for l in picks:
        out = await render_faseA_marker(MARK, l, db, WS, seed=l["id"])
        body = out.get("body") or ""
        if not out.get("sendable"):
            cat = "GEBLOKKEERD" if out.get("block_reason") == "frictie_not_reverified_today" else "anders"
        elif "Wie wil boeken, moet bellen." in body:
            cat = "Frame A"
        else:
            cat = "Frame C"
        cats[cat].append((l, out))
        samples.setdefault(cat, (l, out))

    print(f"Send-pad over {len(picks)} leads:")
    for cat in ("Frame A", "Frame C", "GEBLOKKEERD", "anders"):
        ls = cats[cat]
        if ls:
            niches = {}
            for l, _ in ls:
                niches[niche_for_sector(l["sector"])] = niches.get(niche_for_sector(l["sector"]), 0) + 1
            print(f"  {cat:<12} {len(ls):>2}  ({niches})")

    # blokkade-redenen + een paar namen
    print("\n  GEBLOKKEERD (checked_at niet vandaag → geen claim de deur uit):")
    print("    ", [l.get("company_name")[:26] for l, _ in cats["GEBLOKKEERD"]][:12], "...")
    print("  Frame C — o.a. de teruggedraaide-onzeker leads:")
    print("    ", [l.get("company_name")[:26] for l, _ in cats["Frame C"]][:12])

    # toon: Hijama (was valse Frame A, nu?) + één Frame A
    def _show(title, l, out):
        print("\n" + "=" * 74 + f"\n{title}: {l.get('company_name')} — sendable={out.get('sendable')} "
              f"block={out.get('block_reason')}\n" + "-" * 74)
        print(out.get("body") or "(leeg)")

    hij = next((t for t in (cats["Frame A"] + cats["Frame C"] + cats["GEBLOKKEERD"])
                if "Hijama" in t[0].get("company_name", "")), None)
    if hij:
        _show("HIJAMA (was valse Frame A)", *hij)
    if cats["Frame A"]:
        _show("VOORBEELD FRAME A", *cats["Frame A"][0])


if __name__ == "__main__":
    asyncio.run(main())
