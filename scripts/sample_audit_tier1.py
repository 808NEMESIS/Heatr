"""
scripts/sample_audit_tier1.py — READ-ONLY Tier 1-steekproef.

Draait de audit op een steekproef leads en toont per lead: genormaliseerde score,
noemer, welke checks NOT_MEASURABLE waren, en de categorie-verdeling. Schrijft
NIETS (geen audit_reports). Bedoeld om vóór de volle run te zien of de noemer
groot genoeg is — veel not_measurable = kleine noemer = score wordt ruis.

    python3 scripts/sample_audit_tier1.py --n 15
    python3 scripts/sample_audit_tier1.py --n 15 --sector chiropractoren
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")
logging.disable(logging.INFO)


async def _run(n: int, sector: str | None) -> int:
    from config.database import get_heatr_supabase
    from audit.scorer import score_lead
    import config.audit_weights as W
    sb = get_heatr_supabase()

    q = (sb.table("leads").select("*").eq("workspace_id", WORKSPACE)
         .not_.is_("domain", "null").order("score", desc=True))
    if sector:
        q = q.eq("sector", sector)
    leads = [l for l in (q.limit(n * 2).execute().data or []) if (l.get("domain") or "").strip()][:n]
    print(f"{len(leads)} leads in de steekproef.\n")

    nm_counter: Counter = Counter()
    denoms = []
    for lead in leads:
        sec = lead.get("sector") or ""
        max_possible = W.total_max(sec)
        r = await score_lead(lead, sb, tier=1)
        denoms.append(r["score_denominator"])
        nm_counter.update(r["not_measurable"])
        print(f"{lead['domain']:28} score={r['score_normalized']:>3} "
              f"noemer={r['score_denominator']:>3}/{max_possible} "
              f"({len(r['not_measurable'])} not_measurable) cap={r['score_capped_by'] or '-'}")
        if r["not_measurable"]:
            print(f"    not_measurable: {', '.join(r['not_measurable'])}")

    if denoms:
        denoms.sort()
        print(f"\n-- noemer-verdeling: min {denoms[0]} / mediaan {denoms[len(denoms)//2]} / max {denoms[-1]} --")
        print("-- vaakst not_measurable (structureel?) --")
        for cid, c in nm_counter.most_common(12):
            print(f"    {cid:28} {c}/{len(leads)}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Read-only Tier 1-steekproef met not_measurable-detail.")
    ap.add_argument("--n", type=int, default=15)
    ap.add_argument("--sector", default=None)
    args = ap.parse_args()
    return asyncio.run(_run(args.n, args.sector))


if __name__ == "__main__":
    raise SystemExit(main())
