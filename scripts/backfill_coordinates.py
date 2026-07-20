"""
scripts/backfill_coordinates.py — vul leads.lat/lng uit google_maps_url (spec 4).

Nul API: parseert het !3d<lat>!4d<lng>-place-paar uit de al-opgeslagen
google_maps_url. Resume-veilig (slaat leads met lat al gezet over, tenzij --all).
Raakt website_intelligence NIET → veilig naast de lopende website-backfill.

    python3 scripts/backfill_coordinates.py            # resume
    python3 scripts/backfill_coordinates.py --all      # herbereken alle
    python3 scripts/backfill_coordinates.py --dry-run  # niets schrijven
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from config.database import get_heatr_supabase
from utils.geo import extract_place_coords

WORKSPACE = "aerys"


def main() -> int:
    args = sys.argv[1:]
    do_all = "--all" in args
    dry = "--dry-run" in args
    sb = get_heatr_supabase()

    rows = (sb.table("leads").select("id, domain, google_maps_url, lat")
            .eq("workspace_id", WORKSPACE).not_.is_("google_maps_url", "null")
            .execute()).data or []
    todo = [r for r in rows if do_all or r.get("lat") is None]
    print(f"{len(rows)} leads met maps_url; {len(todo)} te verwerken "
          f"({'--all' if do_all else 'resume'}{', DRY-RUN' if dry else ''}).")

    ok = miss = 0
    for r in todo:
        coords = extract_place_coords(r.get("google_maps_url"))
        if not coords:
            miss += 1
            continue
        lat, lng = coords
        if not dry:
            sb.table("leads").update({"lat": lat, "lng": lng}).eq("id", r["id"]).execute()
        ok += 1
    print(f"Klaar: {ok} coördinaten gezet, {miss} zonder parsebaar place-paar.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
