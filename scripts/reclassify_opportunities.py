"""
scripts/reclassify_opportunities.py — herbereken opportunity_types + priority op
alle opgeslagen leads (herijking + sector-poort, 2026-07-21).

Geen re-crawl: reconstrueert de classify_opportunities-inputs uit de opgeslagen
website_intelligence (total_score, conversion_details, visual) + leads.sector, en
schrijft de nieuwe opportunity_types/priority/reasons. Past in één keer toe:
  - de percentiel-drempels (config/scoring_thresholds.py), en
  - de sector-poort (allowed_offers: alt-zorg/chiro nooit automatisering).

NB: de secundaire visual<4-rebuild-trigger wordt overgeslagen (WI bewaart de
0-25 visual_score, niet de 0-10 overall die die check verwacht) — de total_score-
banden vangen die rebuild-gevallen alsnog. Dry-run: --dry-run.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from collections import Counter
from config.database import get_heatr_supabase
from website_intelligence.opportunity_classifier import classify_opportunities

WORKSPACE = "aerys"


def main() -> int:
    dry = "--dry-run" in sys.argv
    sb = get_heatr_supabase()

    sectors = {l["id"]: l.get("sector")
               for l in (sb.table("leads").select("id, sector").eq("workspace_id", WORKSPACE).execute()).data or []}
    wis = (sb.table("website_intelligence")
           .select("lead_id, total_score, conversion_score, conversion_details, visual_score")
           .eq("workspace_id", WORKSPACE).not_.is_("total_score", "null").execute()).data or []
    print(f"{len(wis)} WI-rijen te herclassificeren{' (DRY-RUN)' if dry else ''}.")

    prio_before = Counter(); prio_after = Counter(); auto_dropped = 0
    for w in wis:
        conv = w.get("conversion_details") or {}
        conv.setdefault("conversion_score", w.get("conversion_score") or 0)
        sector = sectors.get(w["lead_id"])
        opp = classify_opportunities(
            total_score=w.get("total_score") or 0,
            technical_result={}, conversion_result=conv, sector_result={},
            visual_score=None, sector=sector,
        )
        prio_after[opp["priority"]] += 1
        # tel hoeveel leads een automatiserings-offer verliezen door de sector-poort
        if sector in ("alternatieve_geneeskunde", "chiropractoren") and \
           not any(t in ("chatbot", "ai_audit") for t in opp["opportunity_types"]):
            auto_dropped += 1
        if not dry:
            sb.table("website_intelligence").update({
                "opportunity_types": opp["opportunity_types"],
                "priority": opp["priority"],
                "opportunity_reasons": opp["reasons"],
            }).eq("lead_id", w["lead_id"]).eq("workspace_id", WORKSPACE).execute()

    print(f"Klaar. Nieuwe priority-verdeling: {dict(prio_after)}")
    print(f"Alt-zorg/chiro zonder automatiserings-offer (sector-poort): {auto_dropped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
