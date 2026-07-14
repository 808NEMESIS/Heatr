"""
scripts/batch_readiness_report.py — READ-ONLY funnel-rapport: hoeveel leads zijn
écht launch-klaar, en waarop lopen de rest vast?

Draait `utils.launch_readiness.assess_launch_readiness` (exact dezelfde gate als
de echte launch) over alle workspace-leads. Schrijft NIETS, verstuurt GEEN mail.
Doel: "493 valid e-mails" vertalen naar "N daadwerkelijk verzendbaar", en de
top-blokkades kwantificeren (bv. score/icp-drempel, completeness).

Gebruik:
    python3 scripts/batch_readiness_report.py
    python3 scripts/batch_readiness_report.py --valid-only   # alleen email_status=valid
"""
from __future__ import annotations

import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = "aerys"


def _blocker_key(detail: str) -> str:
    """Normaliseer een blocker-detail naar een groepeerbare sleutel (strip
    lead-specifieke waarden zodat we kunnen tellen)."""
    d = detail.lower()
    if "score=" in d or "icp_match" in d:
        return "score/icp onder drempel"
    if "mist verplichte velden" in d:
        return "mist verplichte velden (completeness)"
    if "niet sendable" in d:
        return "email niet sendable"
    if "cooldown" in d or "snooze" in d:
        return "cooldown/snooze actief"
    if "gdpr" in d or "status" in d:
        return "compliance (gdpr/status)"
    return detail[:60]


def main(valid_only: bool = False) -> int:
    from config.database import get_heatr_supabase
    from utils.launch_readiness import assess_launch_readiness

    sb = get_heatr_supabase()
    q = (sb.table("leads").select("*")
         .eq("workspace_id", WORKSPACE)
         .not_.is_("email", "null"))
    if valid_only:
        q = q.eq("email_status", "valid")
    leads = q.limit(5000).execute().data or []

    scope = "email_status=valid" if valid_only else "alle leads met e-mail"
    print(f"Readiness-rapport — {scope}, n={len(leads)} (READ-ONLY, geen mail)\n")

    verdicts = Counter()
    blockers = Counter()
    reviews = Counter()
    ready_valid = 0
    for lead in leads:
        r = assess_launch_readiness(lead)
        verdicts[r["verdict"]] += 1
        for b in r["blockers"]:
            blockers[_blocker_key(b)] += 1
        for rv in r["reviews"]:
            reviews[_blocker_key(rv)] += 1
        if r["verdict"] == "ready" and (lead.get("email_status") == "valid"):
            ready_valid += 1

    print("── VERDICT ──")
    for k in ("ready", "needs_review", "blocked"):
        print(f"  {k:13s}: {verdicts.get(k, 0)}")
    print(f"\n  → 'ready' MÉT valid e-mail: {ready_valid}")

    print("\n── TOP BLOKKADES (block-severity, verhindert launch) ──")
    for k, v in blockers.most_common(10):
        print(f"  {v:4d} × {k}")

    print("\n── TOP REVIEW-FLAGS (needs_review, operator beslist) ──")
    for k, v in reviews.most_common(10):
        print(f"  {v:4d} × {k}")

    print("\n(READ-ONLY — er is niets geschreven en geen mail verzonden.)")
    return 0


if __name__ == "__main__":
    _valid_only = "--valid-only" in sys.argv
    raise SystemExit(main(valid_only=_valid_only))
