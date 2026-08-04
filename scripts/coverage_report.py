"""scripts/coverage_report.py — dekkingsmeter per sector: hoe volledig is de niche-voorraad?

Meet per doelstad (config/cities.NL_TOP_CITIES) de voorraad + welke discipline-keywords
al gescrapet zijn (keyword-level, matcht de sweep-generator). Toont de gaten. READ-ONLY.

    python3 scripts/coverage_report.py                                   # cosmetisch
    python3 scripts/coverage_report.py --sector alternatieve_geneeskunde
    python3 scripts/coverage_report.py --sector alternatieve_geneeskunde --all-cities
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WORKSPACE = "aerys"


def _fetch_all(db, table: str, cols: str) -> list[dict]:
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def _sendable(l: dict) -> bool:
    es, vm = l.get("email_status"), l.get("email_verification_method")
    if not l.get("email"):
        return False
    if es == "valid":
        return True
    return es in ("risky", "catchall_risky") and vm in ("smtp", "bouncer_api")


def main(sector: str, all_cities: bool) -> int:
    from config.database import get_heatr_supabase
    from config.cities import NL_TOP_CITIES
    from config.sectors import get_sector, get_subcategory_keywords

    db = get_heatr_supabase()
    all_kw = {kw.strip().lower()
              for sub in get_sector(sector)["subcategories"]
              for kw in get_subcategory_keywords(sector, sub)}
    n_kw = len(all_kw)

    leads = [l for l in _fetch_all(
        db, "leads", "city, sector, email, email_status, email_verification_method, score, icp_match, status, workspace_id")
        if l.get("workspace_id") == WORKSPACE and l.get("sector") == sector and l.get("status") != "archived"]
    jobs = [j for j in _fetch_all(db, "scraping_jobs", "search_query, city, status, workspace_id")
            if j.get("workspace_id") == WORKSPACE and j.get("status") == "completed"]

    scraped_kw: dict[str, set[str]] = {}    # stad → set gescrapete keywords (van deze sector)
    for j in jobs:
        q = (j.get("search_query") or "").strip().lower()
        if q in all_kw:
            scraped_kw.setdefault(j.get("city") or "?", set()).add(q)

    by_city: dict[str, list[dict]] = {}
    for l in leads:
        by_city.setdefault(l.get("city") or "?", []).append(l)

    cities = list(NL_TOP_CITIES) + (sorted(set(by_city) - set(NL_TOP_CITIES)) if all_cities else [])
    print(f"DEKKINGSMETER — {sector} · {n_kw} discipline-keywords · {len(NL_TOP_CITIES)} doelsteden")
    print(f"{'stad':<20}{'leads':>6}{'sendable':>9}{'launchbaar':>11}{'keywords gescrapet':>20}")
    print("-" * 66)
    tot_l = tot_s = 0
    for city in cities:
        ls = by_city.get(city, [])
        snd = sum(1 for l in ls if _sendable(l))
        lnch = sum(1 for l in ls if _sendable(l) and (l.get("score") or 0) >= 55 and (l.get("icp_match") or 0) >= 0.50)
        kw_done = len(scraped_kw.get(city, set()))
        mark = "  ← GAT" if not ls and not kw_done else ""
        print(f"{city:<20}{len(ls):>6}{snd:>9}{lnch:>11}{kw_done:>13}/{n_kw}{mark}")
        tot_l += len(ls); tot_s += snd
    print("-" * 66)
    open_combos = sum(n_kw - len(scraped_kw.get(c, set())) for c in NL_TOP_CITIES)
    empty = [c for c in NL_TOP_CITIES if not by_city.get(c) and not scraped_kw.get(c)]
    print(f"totaal: {tot_l} leads · {tot_s} sendable")
    print(f"open (keyword × doelstad)-combo's: {open_combos}  →  sweep-surface")
    print(f"doelsteden zonder enige data: {len(empty)} → {', '.join(empty[:10])}{'…' if len(empty) > 10 else ''}")
    return 0


if __name__ == "__main__":
    from config.sectors import ACTIVE_SECTORS
    ap = argparse.ArgumentParser()
    ap.add_argument("--sector", choices=ACTIVE_SECTORS, default="cosmetische_behandelaars")
    ap.add_argument("--all-cities", action="store_true")
    args = ap.parse_args()
    raise SystemExit(main(args.sector, args.all_cities))
