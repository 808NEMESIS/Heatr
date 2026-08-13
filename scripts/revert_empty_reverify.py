#!/usr/bin/env python3
"""scripts/revert_empty_reverify.py — draai onjuiste 'bevestigd' terug naar onzeker.

De oude gate schreef leads met een all-lege (geblokkeerde) fetch weg als
reverify_uncertain=False (bevestigd geen-boeking). Die staan er nu in als betrouwbaar
en elke volgende run vertrouwt ze. Dit corrigeert exact die rijen: checked_at gezet +
reverify_uncertain=False + stored content-rijkheid 0 → reverify_uncertain=True.

Overschrijft de inhoudelijke checks NIET; zet alleen de vlag om + een noot. DRY-RUN
default; --apply is een gated prod-write. Toont lijst vooraf en (bij apply) achteraf.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from config.database import get_heatr_supabase
from scripts.reverify_conversion_checks import _richness

WS = "aerys"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    db = get_heatr_supabase()

    rows = []
    off = 0
    while True:
        d = (db.table("website_intelligence").select("lead_id,conversion_details")
             .eq("workspace_id", WS).range(off, off + 999).execute().data) or []
        rows += d
        if len(d) < 1000:
            break
        off += 1000

    # doel: checked_at gezet + reverify_uncertain==False + stored-rijkheid 0
    targets = []
    for r in rows:
        cd = r.get("conversion_details") or {}
        if cd.get("checked_at") and cd.get("reverify_uncertain") is False and _richness(cd) == 0:
            targets.append((r["lead_id"], cd))

    names = {}
    if targets:
        ids = [t[0] for t in targets]
        for l in (db.table("leads").select("id,company_name").eq("workspace_id", WS)
                  .in_("id", ids).execute().data or []):
            names[l["id"]] = l["company_name"]

    print(f"=== VOORAF — {len(targets)} rij(en) met bevestigd-op-lege-fetch ===")
    for lid, cd in targets:
        print(f"  {names.get(lid, lid)[:40]:<40} reverify_uncertain={cd.get('reverify_uncertain')} "
              f"rijkheid={_richness(cd)} has_online_booking={cd.get('has_online_booking')}")

    if not a.apply:
        print("\nDRY-RUN — niets geschreven. Draai met --apply om terug te draaien.")
        return

    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    for lid, cd in targets:
        upd = {**cd, "reverify_uncertain": True,
               "reverify_reverted": {"at": now_iso, "reason": "stored_richness_0_blocked_fetch"}}
        db.table("website_intelligence").update({"conversion_details": upd}) \
          .eq("lead_id", lid).eq("workspace_id", WS).execute()

    # achteraf-verificatie
    print(f"\n=== ACHTERAF — {len(targets)} rij(en) bijgewerkt ===")
    for lid, _ in targets:
        w = (db.table("website_intelligence").select("conversion_details")
             .eq("lead_id", lid).eq("workspace_id", WS).execute().data)
        cd = (w[0]["conversion_details"] if w else {}) or {}
        print(f"  {names.get(lid, lid)[:40]:<40} reverify_uncertain={cd.get('reverify_uncertain')}")


if __name__ == "__main__":
    main()
