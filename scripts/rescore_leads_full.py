"""
scripts/rescore_leads_full.py — herbereken icp_match + score na de scoring-fix.

De oude icp_matcher normaliseerde niet en telde onhaalbare componenten (KvK-SBI)
mee → icp_match capte op ~0.45, score op ~51, terwijl de gate 0.6/65 eist → 0
launchbare leads. Na de fix (scoring/icp_matcher.py compute_icp_match) moeten de
bestaande leads herberekend worden.

Twee modi (patroon van reverify_email_full.py):
  - dry-run (default): berekent oud vs nieuw IN-MEMORY, schrijft NIETS. Print de
    verdeling per metriek + per sector + een drempel-grid (hoeveel leads
    passeren bij welke drempels, gecombineerd met email_status='valid').
  - --apply: roept het productiepad `score_lead` per lead aan (schrijft
    icp_match + score + dimensies + scored_at). Idempotent, workspace-safe,
    fail-soft per lead. Verstuurt GEEN mail; puur DB, geen API-kosten.

Gebruik:
    python3 scripts/rescore_leads_full.py            # dry-run
    python3 scripts/rescore_leads_full.py --apply     # schrijf
    python3 scripts/rescore_leads_full.py --limit 100
"""
from __future__ import annotations

import asyncio
import os
import statistics as st
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = "aerys"

# Drempel-kandidaten voor het grid (score, icp). Eerste = huidige (onmogelijke).
_THRESHOLD_GRID = [(65, 0.6), (60, 0.6), (60, 0.55), (55, 0.5), (55, 0.45), (50, 0.45), (45, 0.4)]


def _pct(vals: list[float]) -> str:
    vals = [float(v) for v in vals if v is not None]
    if not vals:
        return "geen data"
    vs = sorted(vals)

    def p(q):
        return vs[min(int(len(vs) * q), len(vs) - 1)]
    return (f"n={len(vs)} min={min(vs):.2f} p25={p(.25):.2f} p50={st.median(vs):.2f} "
            f"p75={p(.75):.2f} p90={p(.90):.2f} max={max(vs):.2f}")


async def main(apply: bool = False, limit: int = 5000) -> int:
    from config.database import get_heatr_supabase
    from config.sectors import get_sector
    from scoring.icp_matcher import compute_icp_match
    from scoring.lead_scoring import compute_lead_score, score_lead

    sb = get_heatr_supabase()
    rows = (sb.table("leads").select("*")
            .eq("workspace_id", WORKSPACE)
            .limit(limit).execute().data or [])
    mode = "APPLY (schrijft)" if apply else "DRY-RUN (schrijft niets)"
    print(f"Rescore van {len(rows)} leads — {mode}. GEEN mail, puur DB.\n")

    old_icp, new_icp, old_score, new_score = [], [], [], []
    by_sector_new_icp: dict[str, list] = defaultdict(list)
    sector_counts = Counter()
    errors = 0

    for i, lead in enumerate(rows, 1):
        sector = lead.get("sector") or ""
        sector_counts[sector] += 1
        try:
            sector_config = get_sector(sector)
            nicp = compute_icp_match(lead, sector_config)["icp_match"]
        except ValueError:
            nicp = 0.0  # sector niet (meer) in SECTORS → uit-ICP, zoals match_icp
        nscore = compute_lead_score(lead, nicp)["score"]

        old_icp.append(lead.get("icp_match"))
        new_icp.append(nicp)
        old_score.append(lead.get("score"))
        new_score.append(nscore)
        by_sector_new_icp[sector].append(nicp)
        # bewaar het nieuwe getal op de dict voor het drempel-grid
        lead["_new_icp"], lead["_new_score"] = nicp, nscore

        if apply:
            try:
                await score_lead(lead["id"], WORKSPACE, sb)
            except Exception as e:
                errors += 1
                print(f"  ⚠️  score_lead faalde voor {lead.get('id')}: {str(e)[:80]}")
            if i % 50 == 0 or i == len(rows):
                print(f"  {i}/{len(rows)} herscoord…")

    print("── VERDELING: icp_match ──")
    print(f"  OUD : {_pct(old_icp)}")
    print(f"  NIEUW: {_pct(new_icp)}")
    print("\n── VERDELING: score ──")
    print(f"  OUD : {_pct(old_score)}")
    print(f"  NIEUW: {_pct(new_score)}")

    print("\n── NIEUWE icp_match per sector ──")
    for sec, vals in sorted(by_sector_new_icp.items(), key=lambda kv: -len(kv[1])):
        print(f"  {sec or '(leeg)':28s} ({len(vals):3d}): {_pct(vals)}")

    print("\n── DREMPEL-GRID (met NIEUWE waarden) ──")
    print("  drempel            | passeren totaal | passeren & email=valid")
    for ms, mi in _THRESHOLD_GRID:
        total = sum(1 for l in rows if l["_new_score"] >= ms and l["_new_icp"] >= mi)
        valid = sum(1 for l in rows if l["_new_score"] >= ms and l["_new_icp"] >= mi
                    and l.get("email_status") == "valid")
        marker = "  ← huidig (onmogelijk)" if (ms, mi) == (65, 0.6) else ""
        print(f"  score>={ms:2d} & icp>={mi:.2f}  | {total:5d}           | {valid:5d}{marker}")

    if apply:
        print(f"\n✅ APPLY klaar. Fouten: {errors}/{len(rows)}.")
    else:
        print("\n(DRY-RUN — er is niets geschreven. Draai met --apply na akkoord op de drempels.)")
    return 0


if __name__ == "__main__":
    _apply = "--apply" in sys.argv
    _limit = 5000
    if "--limit" in sys.argv:
        _limit = int(sys.argv[sys.argv.index("--limit") + 1])
    raise SystemExit(asyncio.run(main(apply=_apply, limit=_limit)))
