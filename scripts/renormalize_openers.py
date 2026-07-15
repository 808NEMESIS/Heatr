"""
scripts/renormalize_openers.py — herstel stale personalized_opener/company_summary.

Generatie normaliseert sinds de P1-fix (company_enrichment) correct, maar leads
van vóór die wiring dragen nog ruwe LLM-scaffolding (# Openingszin, **Beste X,**)
die de eerdere backfill miste → onverzendbaar. Deze runner draait dezelfde
normalizer (utils.text_normalizer.normalize_generated_text) over bestaande velden
en persisteert alleen de schone, gevalideerde versie. Idempotent, geen mail.

    python3 scripts/renormalize_openers.py            # dry-run
    python3 scripts/renormalize_openers.py --apply
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = "aerys"


def main(apply: bool = False, limit: int = 5000) -> int:
    import logging
    logging.disable(logging.INFO)
    from config.database import get_heatr_supabase
    from utils.text_normalizer import normalize_generated_text

    sb = get_heatr_supabase()
    rows = (sb.table("leads")
            .select("id, personalized_opener, company_summary")
            .eq("workspace_id", WORKSPACE)
            .not_.is_("personalized_opener", "null")
            .limit(limit).execute().data or [])

    mode = "APPLY (schrijft)" if apply else "DRY-RUN (schrijft niets)"
    print(f"Renormaliseer {len(rows)} leads met opener — {mode}. GEEN mail.\n")

    changed = rejected = unchanged = 0
    shown = 0
    for r in rows:
        raw = r.get("personalized_opener")
        clean, ok, reason = normalize_generated_text(raw, max_sentences=3)
        if not ok:
            rejected += 1
            continue
        if clean == (raw or "").strip():
            unchanged += 1
            continue
        changed += 1
        if shown < 6:
            print(f"  VOOR : {raw[:75]!r}")
            print(f"  NA   : {clean[:75]!r}\n")
            shown += 1
        if apply:
            patch = {"personalized_opener": clean}
            # summary meepakken als die ook vuil is
            sc, sok, _ = normalize_generated_text(r.get("company_summary"), max_sentences=3)
            if sok and sc and sc != (r.get("company_summary") or "").strip():
                patch["company_summary"] = sc
            try:
                sb.table("leads").update(patch).eq("id", r["id"]).eq("workspace_id", WORKSPACE).execute()
            except Exception as e:
                print(f"  ⚠️ update faalde voor {r['id']}: {str(e)[:70]}")

    print(f"── UITKOMST ── opgeschoond: {changed} | al schoon: {unchanged} | afgekeurd(leeg/refusal): {rejected}")
    if not apply:
        print("\n(DRY-RUN — niets geschreven. Draai met --apply.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(apply="--apply" in sys.argv))
