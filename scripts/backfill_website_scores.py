"""
scripts/backfill_website_scores.py — herbereken alle website-scores mét Vision.

Draait analyze_website (volledige rescan: capture + Vision + genormaliseerde score)
over alle leads met een domein. Nodig na het aanzetten van Vision + normalisatie:
bestaande website_intelligence-rijen zijn op de oude 70-schaal berekend zonder
Vision.

    python3 scripts/backfill_website_scores.py --dry-run        # tel alleen
    python3 scripts/backfill_website_scores.py --limit 10       # kleine batch
    python3 scripts/backfill_website_scores.py                  # alles (sequentieel)

NIET zomaar draaien — pas na akkoord op de dry-run-cijfers
(scripts/dryrun_score_normalization.py). Schrijft WI-rijen + leads.website_score.

Kosten (gemeten): ~EUR 0,008 Vision/lead -> ~EUR 6,65 voor 862 leads (geen
cache-hits). Doorlooptijd sequentieel: ~45-60s/lead (capture + Vision + PageSpeed
+ Haiku-lagen). content_hash-skip: de vision_cache slaat Vision over bij identieke
screenshot-bytes; dat helpt vooral bij HERHAALDE backfills, niet de eerste.
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


def _anthropic():
    import anthropic
    return anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


async def _run(limit: int | None, dry_run: bool, process_all: bool) -> int:
    from config.database import get_heatr_supabase
    sb = get_heatr_supabase()

    all_leads = [
        l for l in ((sb.table("leads").select("id, domain, sector")
                     .eq("workspace_id", WORKSPACE).not_.is_("domain", "null")
                     .order("score", desc=True).execute()).data or [])
        if (l.get("domain") or "").strip()
    ]

    # Resume-veiligheid: sla leads over die al een score_denominator hebben (=
    # reeds met de nieuwe logica herberekend). Een run van ~13u is zo hervatbaar
    # en her-draaibaar. --all forceert opnieuw verwerken.
    done: set = set()
    if not process_all:
        dres = (sb.table("website_intelligence").select("lead_id")
                .eq("workspace_id", WORKSPACE)
                .not_.is_("score_denominator", "null").execute()).data or []
        done = {r["lead_id"] for r in dres if r.get("lead_id")}
    leads = [l for l in all_leads if l["id"] not in done]
    if limit:
        leads = leads[:limit]

    print(f"{len(all_leads)} leads met domein; {len(done)} al gedaan; "
          f"{len(leads)} te verwerken{f' (limit {limit})' if limit else ''}.")
    if dry_run:
        print("[dry-run] niets herberekend.")
        return 0

    from website_intelligence.analyzer import analyze_website
    client = _anthropic()

    ok = failed = 0
    t_start = time.monotonic()
    for i, lead in enumerate(leads, 1):
        lead_id, domain, sector = lead["id"], lead["domain"], lead.get("sector") or ""
        t = time.monotonic()
        try:
            res = await asyncio.wait_for(
                analyze_website(
                    lead_id=lead_id, domain=domain, sector=sector,
                    workspace_id=WORKSPACE, supabase_client=sb, anthropic_client=client,
                ),
                timeout=int(os.getenv("WEBSITE_ANALYSIS_TIMEOUT", "180")),
            )
            ok += 1
            score = (res or {}).get("total_score")
            vi = (res or {}).get("visual_included")
            print(f"  [{i}/{len(leads)}] {domain:28} score={score} visual={vi} ({time.monotonic()-t:.0f}s)")
        except Exception as e:
            failed += 1
            print(f"  [{i}/{len(leads)}] {domain:28} FOUT: {str(e)[:70]}")

    dt = time.monotonic() - t_start
    print(f"\nKlaar: {ok} ok, {failed} gefaald in {dt/60:.1f} min "
          f"({dt/max(1,ok):.0f}s/lead gemiddeld).")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Herbereken website-scores mét Vision + normalisatie.")
    ap.add_argument("--dry-run", action="store_true", help="tel alleen, herbereken niet")
    ap.add_argument("--limit", type=int, default=None, help="max aantal leads (test)")
    ap.add_argument("--all", action="store_true", help="ook al-verwerkte leads opnieuw (negeer resume)")
    args = ap.parse_args()
    return asyncio.run(_run(args.limit, args.dry_run, args.all))


if __name__ == "__main__":
    raise SystemExit(main())
