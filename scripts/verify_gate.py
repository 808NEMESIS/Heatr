#!/usr/bin/env python3
"""scripts/verify_gate.py — verificatie van de geharde gate (READ-ONLY, geen write).

A) Drie leads live door de nieuwe gate: Hijama (403) → uncertain, Dr. Penny
   (200/JS/nul signalen) → uncertain, een lead met echte content → confirmed_no_booking.
B) De volledige selectie opnieuw over de 48: gate-validiteit (rijkheid) + send-pad
   (select_leak + checked_at). Vergelijk met de 29/19 uit stap 1.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from config.database import get_heatr_supabase
from campaigns.frictie_copywriter import friction_reverified_today, select_leak
from scripts.reverify_conversion_checks import (
    _get, _launchable, _richness, classify, usable_measurement,
)
from utils.legal_form import receptie_avg_safe
from utils.lead_naming import clean_company_name, display_first_name

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}


def _fetch_all(db, table, cols):
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).eq("workspace_id", WS).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


async def _gate_verdict(l):
    dom = (l.get("domain") or "").strip()
    s1, t1 = await _get(dom)
    s2, t2 = await _get(dom)
    f1 = await usable_measurement(dom, s1, t1, l["sector"])
    f2 = await usable_measurement(dom, s2, t2, l["sector"])
    verdict, _res, reason = classify(f1, f2)
    return verdict, reason, s1, len(t1 or ""), f1[2]


async def main():
    db = get_heatr_supabase()
    leads = _fetch_all(db, "leads",
                       "id,company_name,domain,sector,email_status,score,icp_match,pushed_to_warmr_at,"
                       "contact_attempt_count,kvk_legal_form,email_discovery_source,contact_first_name,contact_why_chosen")
    wi = {w["lead_id"]: (w.get("conversion_details") or {})
          for w in _fetch_all(db, "website_intelligence", "lead_id,conversion_details")}
    by_name = {l["company_name"]: l for l in leads}

    def _find(sub):
        for name, l in by_name.items():
            if sub.lower() in name.lower():
                return l
        return None

    print("=== A · drie leads live door de nieuwe gate ===")
    for sub, verwacht in (("Hijama & Cupping", "uncertain"),
                          ("Dr. Penny", "uncertain"),
                          ("Osteopathie Anna", "confirmed_no_booking")):
        l = _find(sub)
        if not l:
            print(f"  {sub}: niet gevonden"); continue
        verdict, reason, st, size, rich = await _gate_verdict(l)
        ok = "OK" if verdict == verwacht else "AFWIJKING"
        print(f"  {l['company_name'][:34]:<34} status={st} size={size} rijkheid={rich} "
              f"→ {verdict} ({reason}) [verwacht {verwacht}: {ok}]")

    # B · selectie opnieuw over de 48 (read-only, op stored data na de revert)
    picks = []
    for l in leads:
        if not _launchable(l, None) or not display_first_name(l, fallback=""):
            continue
        n, nr = clean_company_name(l.get("company_name"))
        if not n or nr:
            continue
        ok, _r = receptie_avg_safe(l)
        if not ok:
            continue
        cd = wi.get(l["id"], {})
        # zelfde poolvoorwaarde als stap 1: opgeslagen geen-boeking (vóór de gate)
        if cd.get("has_online_booking") is not False:
            continue
        picks.append((l, cd))

    gate_A = gate_C = send_A = send_C = 0
    for l, cd in picks:
        valid = _richness(cd) >= 1
        # gate-validiteit: geldige meting + geen-boeking + niet onzeker
        if valid and not cd.get("reverify_uncertain"):
            gate_A += 1
        else:
            gate_C += 1
        # send-pad: select_leak (respecteert reverify_uncertain) + checked_at=vandaag
        if select_leak(cd) is not None and friction_reverified_today(cd):
            send_A += 1
        else:
            send_C += 1

    print(f"\n=== B · selectie opnieuw over de 48 (pool: opgeslagen geen-boeking = {len(picks)}) ===")
    print(f"  gate-validiteit (rijkheid≥1 + niet-onzeker):  Frame A {gate_A} · Frame C {gate_C}")
    print(f"  send-pad (select_leak + checked_at=vandaag):   Frame A {send_A} · Frame C {send_C}")


if __name__ == "__main__":
    asyncio.run(main())
