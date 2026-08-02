"""scripts/coverage_report.py — dekkingsmeter: hoe volledig is de niche-voorraad? (stroom 1)

Meet per doelstad (config/cities.NL_TOP_CITIES) de cosmetische voorraad: leads,
sendable, en welke (subcategorie × stad)-combinaties al gescrapet zijn. Toont de
gaten — dat stuurt de sweep-generator. READ-ONLY.

Gebruik:
    python3 scripts/coverage_report.py                 # overzicht doelsteden
    python3 scripts/coverage_report.py --all-cities    # ook niet-doelsteden in de data
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WORKSPACE = "aerys"
SECTOR = "cosmetische_behandelaars"


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


def _kw_to_subcat() -> dict[str, str]:
    """Map elk lead_keyword → subcategorie-key (om scrape-jobs te herleiden)."""
    from config.sectors import get_sector, get_subcategory_keywords
    m: dict[str, str] = {}
    for sub in get_sector(SECTOR)["subcategories"]:
        for kw in get_subcategory_keywords(SECTOR, sub):
            m[kw.lower()] = sub
    return m


def main(all_cities: bool) -> int:
    from config.database import get_heatr_supabase
    from config.cities import NL_TOP_CITIES
    from config.sectors import get_sector

    db = get_heatr_supabase()
    subcats = list(get_sector(SECTOR)["subcategories"].keys())
    kwmap = _kw_to_subcat()

    leads = [l for l in _fetch_all(
        db, "leads",
        "city, sector, email, email_status, email_verification_method, score, icp_match, workspace_id")
        if l.get("workspace_id") == WORKSPACE and l.get("sector") == SECTOR]
    jobs = [j for j in _fetch_all(
        db, "scraping_jobs", "search_query, city, status, workspace_id")
        if j.get("workspace_id") == WORKSPACE and j.get("status") == "completed"]

    # (stad → set van gescrapete subcategorieën); query kan "keyword stad" zijn → strip stad
    scraped: dict[str, set[str]] = {}
    for j in jobs:
        q = (j.get("search_query") or "").lower()
        city = j.get("city") or "?"
        sub = kwmap.get(q) or kwmap.get(q.replace(city.lower(), "").strip())
        if sub:
            scraped.setdefault(city, set()).add(sub)

    by_city: dict[str, list[dict]] = {}
    for l in leads:
        by_city.setdefault(l.get("city") or "?", []).append(l)

    cities = list(NL_TOP_CITIES)
    if all_cities:
        cities += sorted(set(by_city) - set(NL_TOP_CITIES))

    print(f"DEKKINGSMETER — {SECTOR} · {len(subcats)} subcategorieën · {len(NL_TOP_CITIES)} doelsteden")
    print(f"{'stad':<22}{'leads':>6}{'sendable':>9}{'launchbaar':>11}{'subcats gescrapet':>19}")
    print("-" * 67)
    tot_l = tot_s = covered = 0
    for city in cities:
        ls = by_city.get(city, [])
        snd = sum(1 for l in ls if _sendable(l))
        lnch = sum(1 for l in ls if _sendable(l) and (l.get("score") or 0) >= 55 and (l.get("icp_match") or 0) >= 0.50)
        subs_done = len(scraped.get(city, set()))
        mark = "" if ls or subs_done else "  ← GAT"
        print(f"{city:<22}{len(ls):>6}{snd:>9}{lnch:>11}{subs_done:>10}/{len(subcats)}{mark}")
        tot_l += len(ls); tot_s += snd
        if subs_done == len(subcats):
            covered += 1
    print("-" * 67)
    empty = [c for c in NL_TOP_CITIES if not by_city.get(c) and not scraped.get(c)]
    full_gap_combos = sum(len(subcats) - len(scraped.get(c, set())) for c in NL_TOP_CITIES)
    print(f"totaal: {tot_l} leads · {tot_s} sendable · {covered}/{len(NL_TOP_CITIES)} doelsteden volledig geveegd")
    print(f"open (subcat × stad)-combinaties in doelsteden: {full_gap_combos}")
    print(f"doelsteden zonder enige data: {len(empty)} → {', '.join(empty[:10])}{'…' if len(empty) > 10 else ''}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--all-cities", action="store_true")
    args = ap.parse_args()
    raise SystemExit(main(args.all_cities))
