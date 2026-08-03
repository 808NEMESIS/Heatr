"""scripts/rediscover_emails.py — e-mail-waterval opnieuw draaien voor not_found-leads.

Reparatie voor de Bouncer-402-regressie (2026-08-02): toen het tegoed op was gooide
de waterval GEVONDEN adressen weg en schreef 'not_found' (43/43 op de Breda-sweep,
terwijl de sites gewoon info@ tonen). Na de fix (_INFRA_STATUSES + circuit-breaker)
bewaart de waterval zo'n adres als not_checked — fail-closed (niet sendable) maar
herstelbaar: zodra het Bouncer-tegoed is opgewaardeerd flipt reverify_email_full.py
ze naar valid/invalid.

Gebruik:
    python3 scripts/rediscover_emails.py --city Breda            # dry-run: toont doelleads
    python3 scripts/rediscover_emails.py --city Breda --apply    # waterval opnieuw + persist
    python3 scripts/rediscover_emails.py --apply                 # alle not_found zonder e-mail (workspace-breed)

Doelselectie: workspace aerys · email IS NULL · email_status='not_found' (+ optioneel stad).
Hergebruikt run_waterfall_for_lead → identiek schrijfpad als de enrichment-queue.
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


def select_targets(db, city: str | None, since: str | None = None) -> list[dict]:
    q = (db.table("leads")
         .select("id, company_name, domain, city, email, email_status")
         .eq("workspace_id", WORKSPACE)
         .eq("email_status", "not_found")
         .is_("email", "null"))
    if city:
        q = q.eq("city", city)
    if since:  # alleen leads aangemaakt sinds deze datum (402-slachtoffer-scope)
        q = q.gte("created_at", since)
    return q.execute().data or []


async def run(city: str | None, apply: bool, limit: int, since: str | None = None) -> int:
    from config.database import get_heatr_supabase
    from enrichment.email_waterfall import run_waterfall_for_lead

    db = get_heatr_supabase()
    targets = select_targets(db, city, since)[:limit]
    print(f"doelleads (not_found, geen e-mail{f', {city}' if city else ''}): {len(targets)}")

    if not apply:
        for l in targets[:15]:
            print(f"  {l['id'][:8]}  {(l.get('company_name') or '?')[:40]:<42} {l.get('domain') or '—'}")
        if len(targets) > 15:
            print(f"  … +{len(targets) - 15} meer")
        print("\nDRY-RUN — draai met --apply om de waterval opnieuw te laten lopen.")
        return 0

    stats: dict[str, int] = {}
    for i, l in enumerate(targets, 1):
        try:
            r = await run_waterfall_for_lead(l["id"], db)
            st = r.get("email_status") or "?"
            stats[st] = stats.get(st, 0) + 1
            mark = "✓" if r.get("email") else "✗"
            print(f"  [{i}/{len(targets)}] {mark} {(l.get('company_name') or '?')[:36]:<38} → {r.get('email') or '—'} [{st}]")
        except Exception as e:  # per-lead vangen
            stats["fout"] = stats.get("fout", 0) + 1
            print(f"  [{i}/{len(targets)}] FOUT {l['id'][:8]}: {str(e)[:80]}")
        await asyncio.sleep(0.3)

    print(f"\nresultaat: {stats}")
    if stats.get("not_checked"):
        print("not_checked = adres bewaard maar onge-verifieerd (Bouncer-tegoed op) → na opwaarderen: "
              "python3 scripts/reverify_email_full.py")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default=None)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=200)
    ap.add_argument("--since", default=None, help="alleen leads aangemaakt sinds YYYY-MM-DD")
    args = ap.parse_args()
    raise SystemExit(asyncio.run(run(args.city, args.apply, args.limit, args.since)))
