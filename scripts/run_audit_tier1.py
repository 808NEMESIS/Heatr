"""
scripts/run_audit_tier1.py — Tier 1 audit over de bestaande database.

Tier 1 = alleen gratis checks (geen Places, geen nieuwe Vision — visual_score ligt
er al). Leest grotendeels bestaande data + één lichte homepage-fetch. Vult de
benchmark-dataset (heatr_audit_reports).

    python3 scripts/run_audit_tier1.py --dry-run      # tel alleen
    python3 scripts/run_audit_tier1.py --limit 20     # steekproef
    python3 scripts/run_audit_tier1.py                # alle (resume: skip al-geaudite)
    python3 scripts/run_audit_tier1.py --all          # ook opnieuw (nieuwe version)

Resume: leads met een bestaand tier-1 audit_report worden overgeslagen tenzij
--all. Append-only -> --all maakt een nieuwe version, overschrijft niets.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")
logging.disable(logging.INFO)


async def _run(limit: int | None, dry_run: bool, process_all: bool) -> int:
    from config.database import get_heatr_supabase
    from audit.scorer import score_lead, persist_audit_report
    sb = get_heatr_supabase()

    leads = [l for l in ((sb.table("leads").select("*")
              .eq("workspace_id", WORKSPACE).not_.is_("domain", "null")
              .order("score", desc=True).execute()).data or [])
             if (l.get("domain") or "").strip()]

    done: set = set()
    if not process_all:
        try:
            dres = (sb.table("audit_reports").select("lead_id")
                    .eq("workspace_id", WORKSPACE).eq("tier", 1).execute()).data or []
            done = {r["lead_id"] for r in dres if r.get("lead_id")}
        except Exception as e:
            print(f"  (resume-check overgeslagen — audit_reports nog niet beschikbaar? {str(e)[:60]})")
    todo = [l for l in leads if l["id"] not in done]
    if limit:
        todo = todo[:limit]

    print(f"{len(leads)} leads met domein; {len(done)} al geaudit; {len(todo)} te doen.")
    if dry_run:
        return 0

    ok = failed = capped = empty = 0
    t0 = time.monotonic()
    for i, lead in enumerate(todo, 1):
        t = time.monotonic()
        try:
            report = await score_lead(lead, sb, tier=1)
            await persist_audit_report(report, sb)
            ok += 1
            if report["score_capped_by"]:
                capped += 1
            if report["is_empty_site"]:
                empty += 1
            print(f"  [{i}/{len(todo)}] {lead['domain']:28} score={report['score_normalized']:>3} "
                  f"noemer={report['score_denominator']:>3} nm={len(report['not_measurable']):>2} "
                  f"cap={report['score_capped_by'] or '-'} ({time.monotonic()-t:.1f}s)")
        except Exception as e:
            failed += 1
            print(f"  [{i}/{len(todo)}] {lead.get('domain'):28} FOUT: {str(e)[:60]}")

    dt = time.monotonic() - t0
    print(f"\nKlaar: {ok} ok ({capped} met knock-out, {empty} lege sites), {failed} gefaald "
          f"in {dt/60:.1f} min ({dt/max(1,ok):.1f}s/lead).")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Tier 1 audit over de database.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--all", action="store_true", help="ook al-geaudite leads (nieuwe version)")
    args = ap.parse_args()
    return asyncio.run(_run(args.limit, args.dry_run, args.all))


if __name__ == "__main__":
    raise SystemExit(main())
