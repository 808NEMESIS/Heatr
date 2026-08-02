"""scripts/sweep_generator.py — de niche-sweep: (subcategorie × stad)-scrape-jobs, gedoseerd (stroom 1).

Vult de gaten die scripts/coverage_report.py toont. Een schedule-run scrapet alleen
keywords[0]; voor échte dekking enqueue't deze generator jobs per (subcategorie × stad)
rechtstreeks in de bestaande scraping-queue — de daemon-workers doen de rest
(scrape → promotie → enrichment → scoring), en de €1/dag-enrichment-cap gardeert de kosten.

Standen:
    python3 scripts/sweep_generator.py                          # DRY-RUN: volgende jobs + raming
    python3 scripts/sweep_generator.py --apply --max-jobs 2     # enqueue de eerste 2 open combo's
    python3 scripts/sweep_generator.py --apply --max-jobs 2 --cities Breda --subcats injectables_anti_aging,laser_huidverjonging

Prioriteit: config/cities-volgorde × COSMETISCH_SUBCAT_PRIORITY (medisch eerst).
Al-gescrapete combo's worden overgeslagen. Advies-dosering: 2-4 jobs/dag — een verse
stad levert ~15-30 nieuwe leads/job en de enrichment-cap pauzeert vanzelf bij €1/dag.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WORKSPACE = "aerys"
SECTOR = "cosmetische_behandelaars"
EST_LEADS_PER_JOB = (15, 30)          # verse stad, na dedup/kwalificatie
EST_COST_PER_LEAD_EUR = 0.05          # all-in (Claude + Bouncer + evt. KvK)


def _fetch_all(db, table: str, cols: str) -> list[dict]:
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def open_combos(db, cities: list[str], subcats: list[str]) -> list[tuple[str, str, str]]:
    """(stad, subcat, query) voor elke nog niet gescrapete combinatie, in prioriteitsvolgorde."""
    from config.sectors import get_subcategory_keywords, get_sector

    kwmap: dict[str, str] = {}
    for sub in get_sector(SECTOR)["subcategories"]:
        for kw in get_subcategory_keywords(SECTOR, sub):
            kwmap[kw.lower()] = sub

    jobs = [j for j in _fetch_all(db, "scraping_jobs", "search_query, city, status, workspace_id")
            if j.get("workspace_id") == WORKSPACE and j.get("status") in ("completed", "pending", "running")]
    scraped: dict[str, set[str]] = {}
    for j in jobs:
        q = (j.get("search_query") or "").lower()
        city = j.get("city") or "?"
        sub = kwmap.get(q) or kwmap.get(q.replace(city.lower(), "").strip())
        if sub:
            scraped.setdefault(city, set()).add(sub)

    todo: list[tuple[str, str, str]] = []
    for city in cities:                       # stad-prioriteit buitenste lus:
        for sub in subcats:                   # eerst gaten dichten per stad, medisch eerst
            if sub in scraped.get(city, set()):
                continue
            query = get_subcategory_keywords(SECTOR, sub)[0]
            todo.append((city, sub, query))
    return todo


async def run(apply: bool, max_jobs: int, cities: list[str], subcats: list[str]) -> int:
    from config.database import get_heatr_supabase

    db = get_heatr_supabase()
    todo = open_combos(db, cities, subcats)
    lo = len(todo) * EST_LEADS_PER_JOB[0] * EST_COST_PER_LEAD_EUR
    hi = len(todo) * EST_LEADS_PER_JOB[1] * EST_COST_PER_LEAD_EUR
    print(f"open combo's binnen selectie: {len(todo)}  ·  geschatte opbrengst "
          f"{len(todo) * EST_LEADS_PER_JOB[0]}-{len(todo) * EST_LEADS_PER_JOB[1]} leads  ·  "
          f"raming €{lo:.0f}-€{hi:.0f} (gegate door €1/dag-cap)")

    batch = todo[:max_jobs]
    if not apply:
        print(f"\nDRY-RUN — eerstvolgende {len(batch)} job(s) bij --apply:")
        for city, sub, q in batch:
            print(f"  {city:<20} {sub:<32} query='{q}'")
        print("\nAdvies-dosering: --max-jobs 2-4 per dag; de enrichment-cap pauzeert vanzelf.")
        return 0

    from job_queue.scraping_queue import create_scraping_job
    created = 0
    for city, sub, q in batch:
        job_id = await create_scraping_job(
            job_type="google_maps", sector_key=SECTOR, query=q, location=city,
            country="NL", workspace_id=WORKSPACE, supabase_client=db,
        )
        print(f"  ENQUEUED {city} · {sub} · '{q}' → job {str(job_id)[:8]}")
        created += bool(job_id)
    print(f"\n{created} job(s) in de queue — de scraping-worker pakt ze automatisch op.")
    return 0


if __name__ == "__main__":
    from config.cities import NL_TOP_CITIES, COSMETISCH_SUBCAT_PRIORITY

    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-jobs", type=int, default=4)
    ap.add_argument("--cities", default=None, help="komma-gescheiden; default config/cities.NL_TOP_CITIES")
    ap.add_argument("--subcats", default=None, help="komma-gescheiden; default medisch-eerst-prioriteit")
    args = ap.parse_args()
    cities = [c.strip() for c in args.cities.split(",")] if args.cities else NL_TOP_CITIES
    subcats = [s.strip() for s in args.subcats.split(",")] if args.subcats else COSMETISCH_SUBCAT_PRIORITY
    raise SystemExit(asyncio.run(run(args.apply, args.max_jobs, cities, subcats)))
