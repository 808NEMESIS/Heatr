"""
scripts/pipeline_health.py — READ-ONLY operationeel dashboard in de terminal.

Combineert de ops-health (queue/worker/stall — utils.pipeline_ops) met de
conversie-funnel (utils.pipeline_metrics). Eén blik op "draait de machine, en
waar vallen leads af?". Schrijft niets. Cron-baar; exit-code 2 bij een stall
(zodat een wrapper/alert erop kan reageren).

Gebruik:
    python3 scripts/pipeline_health.py
    python3 scripts/pipeline_health.py --days 7
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = "aerys"
_ICON = {"healthy": "🟢", "degraded": "🟡", "stalled": "🔴"}


async def main(days: int = 1) -> int:
    import logging
    logging.disable(logging.INFO)
    from config.database import get_heatr_supabase
    from utils.pipeline_ops import ops_health
    from utils.pipeline_metrics import collect_pipeline_health

    sb = get_heatr_supabase()
    health = await ops_health(WORKSPACE, sb)
    st = health["status"]

    print(f"{_ICON.get(st, '⚪')}  PIJPLIJN-STATUS: {st.upper()}   ({health['checked_at'][:19]}Z)\n")

    if health["alerts"]:
        print("── ALERTS ──")
        for a in health["alerts"]:
            mark = "🔴" if a["severity"] == "critical" else "🟡"
            print(f"  {mark} {a['message']}")
        print()

    snap = health["snapshot"]
    e, s = snap["enrichment"], snap["scraping"]
    print("── QUEUES ──")
    print(f"  enrichment : pending={e['pending']}  running={e['running']}  vastgelopen={e['stuck_running']}  "
          f"laatste-completion={str(e['last_completed_at'])[:19]}")
    print(f"  scraping   : pending={s['pending']}  running={s['running']}  vastgelopen={s['stuck_running']}")
    print(f"\n── STADIA (nu) ──")
    print(f"  discovered={snap['stages']['discovered']}  enriched={snap['stages']['enriched']}  "
          f"launchbaar(valid+gate)={snap['launchable']}")

    # Conversie-funnel over de periode
    try:
        conv = await collect_pipeline_health(WORKSPACE, sb, days=days)
        f = conv["funnel"]
        print(f"\n── CONVERSIE-FUNNEL (laatste {days}d) ──")
        for k in ("raw_companies", "qualified", "email_found", "email_verified",
                  "contact_found", "scored_above_gate", "pushed_to_warmr"):
            print(f"  {k:18s}: {f.get(k, 0)}")
        print(f"  overall conversie : {conv.get('overall_conversion_pct', 0)}%  "
              f"| kosten: €{conv.get('costs', {}).get('total_claude_eur', 0)}")
    except Exception as ex:
        print(f"\n(conversie-funnel niet beschikbaar: {str(ex)[:80]})")

    # Migratie-drift-check (schuld-fix 2026-07-18): 033 bleek maandenlang niet
    # toegepast terwijl iedereen "gedraaid" aannam — fail-soft writes verborgen
    # dat. Elke health-run checkt nu de migratiestaat mee (subprocess, zodat een
    # check-fout de health-run zelf nooit breekt).
    try:
        import subprocess
        vm = subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "verify_migrations.py"), "--check"],
            capture_output=True, text=True, timeout=120,
        )
        print("── MIGRATIES ──")
        print("  " + (vm.stdout.strip().replace("\n", "\n  ") or "(geen output)"))
        if vm.returncode != 0:
            print("  🔴 migratie-drift gedetecteerd — draai de ontbrekende migratie(s) in de Supabase SQL-editor")
    except Exception as ex:
        print(f"(migratie-check niet beschikbaar: {str(ex)[:80]})")

    print()
    return 2 if st == "stalled" else 0


if __name__ == "__main__":
    _days = 1
    if "--days" in sys.argv:
        _days = int(sys.argv[sys.argv.index("--days") + 1])
    raise SystemExit(asyncio.run(main(days=_days)))
