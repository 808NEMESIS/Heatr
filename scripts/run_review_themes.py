"""scripts/run_review_themes.py — vul review-thema's per lead (voor value-first mail-1).

Scrapet echte Google-reviews → mine't 2 gegronde lof-thema's → slaat ze op in
website_intelligence.personalization.review_themes (jsonb, geen DDL). Gedoseerd,
want scrapen is traag (~30s/lead) + Claude-kosten. READ-ONLY tenzij --apply.

    python3 scripts/run_review_themes.py                 # dry-run, launchbaar zonder thema's
    python3 scripts/run_review_themes.py --limit 5 --apply
    python3 scripts/run_review_themes.py --sample info@faceinstitute.nl --apply
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(str(Path(__file__).resolve().parents[1] / ".env"))

WORKSPACE = "aerys"
_CALL_DELAY_S = float(os.getenv("REVIEW_THEME_DELAY_S", "2.0"))


def _fetch_all(db, table, cols):
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).eq("workspace_id", WORKSPACE).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


async def run(apply: bool, limit: int, sample: str | None) -> int:
    from config.database import get_heatr_supabase
    from enrichment.review_themes import extract_review_themes

    db = get_heatr_supabase()
    leads = _fetch_all(db, "leads",
                       "id, company_name, city, email, email_status, score, icp_match, sector, status")
    wis = {w["lead_id"]: w for w in _fetch_all(db, "website_intelligence", "lead_id, personalization")}

    ACTIVE = {"cosmetische_behandelaars", "chiropractoren", "alternatieve_geneeskunde"}

    def _eligible(l):
        if sample:
            return l.get("email") == sample
        if l.get("email_status") != "valid" or (l.get("score") or 0) < 55 or (l.get("icp_match") or 0) < 0.50:
            return False
        if (l.get("sector") or "") not in ACTIVE or (l.get("status") or "") == "archived":
            return False
        pers = (wis.get(l["id"], {}) or {}).get("personalization") or {}
        return not (pers.get("review_themes"))                    # nog geen thema's

    todo = [l for l in leads if _eligible(l)]
    if limit:
        todo = todo[:limit]
    mode = "APPLY (schrijft)" if apply else "DRY-RUN (schrijft niets)"
    print(f"Leads zonder review-thema's: {len(todo)} — {mode}. GEEN mail verzonden.\n")

    done = wrote = 0
    for i, l in enumerate(todo, 1):
        naam = l.get("company_name") or ""
        res = await extract_review_themes(naam, l.get("city"))
        themes = res.get("themes") or []
        print(f"  {i}/{len(todo)} {naam[:32]:<33} themes={themes} "
              f"(pos={res.get('positive_count')} crit={res.get('critical_count')})")
        done += 1
        if apply and len(themes) >= 2:
            wi = wis.get(l["id"], {}) or {}
            pers = dict(wi.get("personalization") or {})
            pers["review_themes"] = themes
            try:
                db.table("website_intelligence").update({"personalization": pers}) \
                    .eq("lead_id", l["id"]).eq("workspace_id", WORKSPACE).execute()
                wrote += 1
            except Exception as e:
                print(f"     ⚠ opslaan faalde: {str(e)[:80]}")
        await asyncio.sleep(_CALL_DELAY_S)

    print(f"\nverwerkt: {done} · thema's weggeschreven: {wrote}"
          + ("" if apply else "  (dry-run — niets geschreven)"))
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sample", default=None, help="alleen dit e-mailadres verwerken")
    args = ap.parse_args()
    raise SystemExit(asyncio.run(run(args.apply, args.limit, args.sample)))
