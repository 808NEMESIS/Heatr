"""scripts/sweep_generator.py — niche-sweep: (stad × discipline-keyword)-scrape-jobs, gedoseerd.

Vult de gaten die scripts/coverage_report.py toont, per sector. KEYWORD-LEVEL: elke
discipline (acupunctuur, homeopaat, orthomoleculair, reiki, mesologie, …) krijgt een
eigen Google-Maps-query — een schedule-run scrapet maar keywords[0], dus zonder deze
generator worden de meeste disciplines nooit los gezocht. Enqueue't direct in de queue;
de daemon-workers doen de rest (scrape → promotie → enrichment → scoring); de €1/dag-
enrichment-cap gardeert de kosten.

    python3 scripts/sweep_generator.py --sector alternatieve_geneeskunde            # DRY-RUN
    python3 scripts/sweep_generator.py --sector alternatieve_geneeskunde --apply --max-jobs 3
    python3 scripts/sweep_generator.py --sector cosmetische_behandelaars --apply --max-jobs 3
    (extra: --cities Breda,Tilburg  --keyword-filter homeopaat)

Prioriteit: stad-volgorde (config/cities.NL_TOP_CITIES) buitenste lus → binnen een stad
eerst de gaten dichten, subcats in prioriteit (cosmetisch: medisch-eerst) dan config-orde.
Al-gescrapete (stad × keyword)-combo's worden overgeslagen. Advies: 2-4 jobs/dag.
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
EST_LEADS_PER_JOB = (10, 25)
EST_COST_PER_LEAD_EUR = 0.05


def _fetch_all(db, table: str, cols: str) -> list[dict]:
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def _ordered_keywords(sector: str) -> list[tuple[str, str]]:
    """(subcat, keyword) in prioriteitsvolgorde: per-sector subcat-prio (indien gezet)
    dan config-orde; binnen subcat de keyword-lijstvolgorde."""
    from config.sectors import get_sector, get_subcategory_keywords
    from config.cities import SUBCAT_PRIORITY

    subs = list(get_sector(sector)["subcategories"].keys())
    prio = SUBCAT_PRIORITY.get(sector)
    if prio:
        subs = [s for s in prio if s in subs] + [s for s in subs if s not in prio]
    out: list[tuple[str, str]] = []
    for sub in subs:
        for kw in get_subcategory_keywords(sector, sub):
            out.append((sub, kw))
    return out


def open_combos(db, sector: str, cities: list[str], kw_filter: str | None) -> list[tuple[str, str, str]]:
    """(stad, subcat, keyword) voor elke nog niet gescrapete (stad × keyword)-combi."""
    jobs = [j for j in _fetch_all(db, "scraping_jobs", "search_query, city, status, workspace_id")
            if j.get("workspace_id") == WORKSPACE and j.get("status") in ("completed", "pending", "running")]
    scraped = {((j.get("city") or "").strip().lower(), (j.get("search_query") or "").strip().lower())
               for j in jobs}
    kws = _ordered_keywords(sector)
    if kw_filter:
        kws = [(s, k) for (s, k) in kws if kw_filter.lower() in k.lower()]
    todo: list[tuple[str, str, str]] = []
    for city in cities:                      # stad buitenste lus → dicht een stad af
        for sub, kw in kws:
            if (city.strip().lower(), kw.strip().lower()) not in scraped:
                todo.append((city, sub, kw))
    return todo


def _spread_by_city(todo: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    """Herordent een city-outer todo naar round-robin OVER de steden: ronde 0 =
    de eerste openstaande niche van elke stad (steden in prioriteit = grootste
    eerst), ronde 1 = de tweede, enz. Zo raakt een batch van N N verschillende
    steden i.p.v. één stad te verdiepen — evenredige geografische dekking."""
    from collections import OrderedDict
    by_city: "OrderedDict[str, list]" = OrderedDict()
    for item in todo:                         # todo is al stad-prioriteit-geordend
        by_city.setdefault(item[0], []).append(item)
    out: list[tuple[str, str, str]] = []
    i = 0
    while True:
        added = False
        for items in by_city.values():
            if i < len(items):
                out.append(items[i]); added = True
        if not added:
            return out
        i += 1


async def run(sector: str, apply: bool, max_jobs: int, cities: list[str], kw_filter: str | None,
              spread: bool = False) -> int:
    from config.database import get_heatr_supabase

    db = get_heatr_supabase()
    todo = open_combos(db, sector, cities, kw_filter)
    if spread:
        todo = _spread_by_city(todo)          # round-robin over steden → evenredig
    lo = len(todo) * EST_LEADS_PER_JOB[0] * EST_COST_PER_LEAD_EUR
    hi = len(todo) * EST_LEADS_PER_JOB[1] * EST_COST_PER_LEAD_EUR
    print(f"sector={sector} · open (stad × keyword)-combo's: {len(todo)} · "
          f"raming {len(todo) * EST_LEADS_PER_JOB[0]}-{len(todo) * EST_LEADS_PER_JOB[1]} leads "
          f"(~€{lo:.0f}-€{hi:.0f}, gegate door €1/dag-cap)")

    batch = todo[:max_jobs]
    if not apply:
        print(f"\nDRY-RUN — eerstvolgende {len(batch)} job(s) bij --apply:")
        for city, sub, kw in batch:
            print(f"  {city:<18} {sub:<26} query='{kw}'")
        print("\nAdvies-dosering: --max-jobs 2-4 per dag; de enrichment-cap pauzeert vanzelf.")
        return 0

    from job_queue.scraping_queue import create_scraping_job
    created = 0
    for city, sub, kw in batch:
        job_id = await create_scraping_job(
            job_type="google_maps", sector_key=sector, query=kw, location=city,
            country="NL", workspace_id=WORKSPACE, supabase_client=db,
        )
        print(f"  ENQUEUED {city} · {sub} · '{kw}' → job {str(job_id)[:8]}")
        created += bool(job_id)
    print(f"\n{created} job(s) in de queue — de scraping-worker pakt ze automatisch op.")
    return 0


if __name__ == "__main__":
    from config.cities import NL_TOP_CITIES
    from config.sectors import ACTIVE_SECTORS

    ap = argparse.ArgumentParser()
    ap.add_argument("--sector", choices=ACTIVE_SECTORS, default="cosmetische_behandelaars")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-jobs", type=int, default=4)
    ap.add_argument("--cities", default=None, help="komma-gescheiden; default config/cities.NL_TOP_CITIES")
    ap.add_argument("--keyword-filter", default=None, help="alleen keywords die deze term bevatten")
    ap.add_argument("--spread", action="store_true",
                    help="round-robin over steden (evenredige geografische spreiding) i.p.v. stad-voor-stad")
    args = ap.parse_args()
    cities = [c.strip() for c in args.cities.split(",")] if args.cities else NL_TOP_CITIES
    raise SystemExit(asyncio.run(
        run(args.sector, args.apply, args.max_jobs, cities, args.keyword_filter, spread=args.spread)))
