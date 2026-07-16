"""
scripts/run_retarget_cron.py — verstuur alle due check-up retargets.

Vindt de gesprekken met een geplande retarget die nu due is (via de data-laag,
direct op Supabase) en POST't elk naar de service-only retarget-endpoint met
X-API-Key. De endpoint doet de echte beslissing (variant, QA-gate, kill-switch,
poging-telling); dit script is enkel de planner.

    python3 scripts/run_retarget_cron.py --dry-run   # toon wat er zou gaan
    python3 scripts/run_retarget_cron.py             # verstuur due retargets

Draai onder cron/n8n (analoog aan de reply-classifier). Twee sloten blijven van
kracht: CHECKUP_REPORT_ENABLED + ENABLE_PROSPECT_SENDS -> zonder die weigert de
endpoint met 403 en verstuurt dit script niets.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import httpx

WORKSPACE = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")


async def _due_calls() -> list[dict]:
    from config.database import get_heatr_supabase
    from calls.call_records import list_due_retargets
    sb = get_heatr_supabase()
    return await list_due_retargets(sb, WORKSPACE, limit=100)


def main() -> int:
    ap = argparse.ArgumentParser(description="Verstuur due check-up retargets.")
    ap.add_argument("--dry-run", action="store_true", help="toon due retargets, verstuur niet")
    ap.add_argument("--api-url", default=os.getenv("HEATR_API_URL", "http://127.0.0.1:8001"))
    args = ap.parse_args()

    api_key = os.getenv("HEATR_API_KEY")
    if not api_key:
        print("FOUT: HEATR_API_KEY ontbreekt in .env", file=sys.stderr)
        return 2

    due = asyncio.run(_due_calls())
    if not due:
        print("Geen due retargets.")
        return 0
    print(f"{len(due)} due retarget(s).")

    base = args.api_url.rstrip("/")
    sent = failed = 0
    for c in due:
        cid = c["id"]
        try:
            resp = httpx.post(
                f"{base}/calls/{cid}/retarget",
                headers={"X-API-Key": api_key},
                json={"dry_run": args.dry_run},
                timeout=90.0,
            )
            body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
        except httpx.HTTPError as e:
            print(f"  {cid}: FOUT kon API niet bereiken: {e}", file=sys.stderr)
            failed += 1
            continue
        if resp.status_code >= 400:
            print(f"  {cid}: geweigerd ({resp.status_code}) {body}")
            failed += 1
        else:
            sent += 1
            print(f"  {cid}: OK {body}")

    print(f"Klaar: {sent} ok, {failed} gefaald/geweigerd.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
