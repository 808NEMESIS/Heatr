"""
scripts/dryrun_score_normalization.py — impact van de score-normalisatie.

READ-ONLY. Schrijft niets. Berekent over de bestaande website_intelligence-rijen
wat total_score zou worden mét normalisatie maar ZONDER Vision (die data is er
niet voor oude rijen -> noemer 70). Toont de verschuiving en de drempel-kruisingen.

    python3 scripts/dryrun_score_normalization.py

Belangrijk: dit is de normalisatie-impact op zichzelf. De backfill voegt Vision
toe (noemer 95) -> de uiteindelijke scores liggen doorgaans nog hoger. En:
website_score stuurt de opportunity-classificatie (drempels 30/40/50 + opp=50),
NIET de Warmr-send-gate (dat is leads.score, drempel 55). De 65-kruising staat er
puur informatief bij.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")
_ELIGIBLE_EMAIL = {"valid", "risky", "catch_all", "catchall_risky"}
_BUCKETS = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 101)]


def _normalize(t, c, s, v) -> int:
    """Genormaliseerde score over de behaalde noemer (zoals analyzer.py)."""
    achieved = (t or 0) + (c or 0) + (s or 0)
    denom = 70
    if v is not None:
        achieved += v
        denom += 25
    return round(achieved / denom * 100) if denom else 0


def _bucket(score) -> str:
    for lo, hi in _BUCKETS:
        if lo <= score < hi:
            return f"{lo}-{hi if hi <= 100 else 100}"
    return "?"


def main() -> int:
    from config.database import get_heatr_supabase
    sb = get_heatr_supabase()

    rows = (sb.table("website_intelligence")
            .select("lead_id, total_score, technical_score, conversion_score, sector_score, visual_score")
            .eq("workspace_id", WORKSPACE).execute()).data or []
    rows = [r for r in rows if r.get("lead_id")]
    if not rows:
        print("Geen website_intelligence-rijen.")
        return 0

    # email_status per lead (batch)
    lead_ids = sorted({r["lead_id"] for r in rows})
    email_by_lead: dict = {}
    for i in range(0, len(lead_ids), 200):
        chunk = lead_ids[i:i + 200]
        lres = (sb.table("leads").select("id, email_status")
                .eq("workspace_id", WORKSPACE).in_("id", chunk).execute()).data or []
        for l in lres:
            email_by_lead[l["id"]] = l.get("email_status")

    old_hist: dict = {}
    new_hist: dict = {}
    deltas = []
    thresholds = [30, 40, 50, 65]
    cross_up = {t: [] for t in thresholds}
    cross_down = {t: [] for t in thresholds}

    for r in rows:
        old = r.get("total_score")
        if old is None:
            continue
        new = _normalize(r.get("technical_score"), r.get("conversion_score"),
                         r.get("sector_score"), r.get("visual_score"))
        old_hist[_bucket(old)] = old_hist.get(_bucket(old), 0) + 1
        new_hist[_bucket(new)] = new_hist.get(_bucket(new), 0) + 1
        deltas.append(new - old)
        for t in thresholds:
            if old < t <= new:
                cross_up[t].append(r["lead_id"])
            elif new < t <= old:
                cross_down[t].append(r["lead_id"])

    n = len(deltas)
    print(f"\n=== Score-normalisatie dry-run (workspace {WORKSPACE}) ===")
    print(f"website_intelligence-rijen met score: {n}")
    print(f"gemiddelde delta (nieuw - oud): +{sum(deltas)/n:.1f} punten  (min {min(deltas)}, max {max(deltas)})")

    print("\n-- Verdeling OUD -> NIEUW (bucket: aantal) --")
    for lo, hi in _BUCKETS:
        b = f"{lo}-{hi if hi <= 100 else 100}"
        print(f"  {b:>7} : {old_hist.get(b,0):>4}  ->  {new_hist.get(b,0):>4}")

    def _elig(ids):
        return sum(1 for lid in ids if email_by_lead.get(lid) in _ELIGIBLE_EMAIL)

    print("\n-- Drempel-kruisingen (website_score stuurt opportunity/priority) --")
    for t in thresholds:
        up, down = cross_up[t], cross_down[t]
        tag = ""
        if t == 50:
            tag = "  <- MIN_WEBSITE_SCORE_FOR_OPPORTUNITY / priority high"
        elif t == 40:
            tag = "  <- website_rebuild-grens"
        elif t == 30:
            tag = "  <- priority urgent-grens"
        elif t == 65:
            tag = "  <- INFORMATIEF: dit is de lead-score-Warmr-drempel, NIET website_score"
        print(f"  {t}: +{len(up)} omhoog (waarvan {_elig(up)} met sendable e-mail) / "
              f"-{len(down)} omlaag (waarvan {_elig(down)} sendable){tag}")

    print("\nLET OP: normalisatie-only (geen Vision). De backfill voegt Vision toe")
    print("(noemer 95) -> scores stijgen doorgaans verder. Herbereken pas na akkoord.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
