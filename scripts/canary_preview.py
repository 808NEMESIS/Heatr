"""
scripts/canary_preview.py — READ-ONLY preview van de eerste canary-set.

Selecteert de sterkste launchbare leads (valid email + boven de gate + schone
opener), draait ze door de ECHTE launch-gate (utils.launch_readiness) en toont
wat er zou uitgaan — GEEN verzending, GEEN schrijfacties.

    python3 scripts/canary_preview.py            # top 20, terminal
    python3 scripts/canary_preview.py --n 30 --json /pad/out.json
"""
from __future__ import annotations

import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = "aerys"
_DIRTY = re.compile(r"(^#|^\*|^_|\bopeningszin\b|voorstel\s*:|^beste\b|^hallo\b|\*\*|^\"|^hier is)", re.I)
_ACTIVE_ICP = {"cosmetische_behandelaars", "chiropractoren", "alternatieve_geneeskunde"}


def main(n: int = 20, json_out: str | None = None) -> int:
    import logging
    logging.disable(logging.INFO)
    from collections import Counter
    from config.database import get_heatr_supabase
    from utils.launch_readiness import assess_launch_readiness
    from utils.legal_form import receptie_avg_safe

    sb = get_heatr_supabase()
    rows = (sb.table("leads").select("*")
            .eq("workspace_id", WORKSPACE)
            .eq("email_status", "valid")
            .gte("score", 55).gte("icp_match", 0.50)
            .not_.is_("personalized_opener", "null")
            .limit(2000).execute().data or [])

    cand = []
    for r in rows:
        op = (r.get("personalized_opener") or "").strip()
        if not op or _DIRTY.search(op):
            continue
        if (r.get("sector") or "") not in _ACTIVE_ICP:
            continue
        # niet eerder benaderd
        if r.get("pushed_to_warmr_at") or (r.get("contact_attempt_count") or 0) > 0:
            continue
        verdict = assess_launch_readiness(r)
        avg_ok, avg_reason = receptie_avg_safe(r)
        cand.append({
            "company": r.get("company_name"), "city": r.get("city"),
            "sector": r.get("sector"), "contact": r.get("contact_first_name"),
            "email": r.get("email"), "score": r.get("score"),
            "icp": round(r.get("icp_match") or 0, 2),
            "verdict": verdict["verdict"], "reviews": verdict.get("reviews", []),
            "avg_ok": avg_ok, "avg_grond": avg_reason,
            "opener": op,
        })

    # sterkste eerst: ready vóór needs_review, dan score desc
    order = {"ready": 0, "needs_review": 1, "blocked": 2}
    cand.sort(key=lambda c: (order.get(c["verdict"], 3), -(c["score"] or 0)))
    top = cand[:n]

    ready = sum(1 for c in cand if c["verdict"] == "ready")
    print(f"Canary-kandidaten (valid + gate + schone opener + in-ICP + niet benaderd): {len(cand)}")
    print(f"  waarvan 'ready' (volledige gate): {ready} | 'needs_review': {sum(1 for c in cand if c['verdict']=='needs_review')}\n")

    # AVG-grond-verdeling (art. 11.7): waarom mag deze lead koud gemaild worden?
    avg_pass = sum(1 for c in cand if c["avg_ok"])
    gronden = Counter(c["avg_grond"] for c in cand)
    print(f"AVG-grond (art. 11.7): {avg_pass}/{len(cand)} met geldige grond")
    for grond, cnt in gronden.most_common():
        mark = "✓" if grond in ("rechtspersoon", "gepubliceerd_zakelijk_adres_art_11_7_lid_3") else "✗"
        print(f"  {mark} {grond}: {cnt}")
    print()

    print(f"── TOP {len(top)} PREVIEW (geen verzending) ──\n")
    for i, c in enumerate(top, 1):
        first = c["contact"] or "—"
        avg = "AVG:" + ("rechtspersoon" if c["avg_grond"] == "rechtspersoon"
                         else "site-adres" if c["avg_ok"] else f"GEBLOKT/{c['avg_grond']}")
        print(f"{i:2d}. {c['company']} · {c['city']} · {c['sector'][:14]} · score {c['score']}/icp {c['icp']} · [{c['verdict']}] · {avg}")
        print(f"    {first} <{c['email']}>")
        print(f"    opener: {c['opener'][:200]}")
        print()

    if json_out:
        with open(json_out, "w") as f:
            json.dump({"total_candidates": len(cand), "ready": ready, "top": top}, f, ensure_ascii=False, indent=2)
        print(f"(JSON → {json_out})")
    return 0


if __name__ == "__main__":
    _n = 20
    if "--n" in sys.argv:
        _n = int(sys.argv[sys.argv.index("--n") + 1])
    _json = None
    if "--json" in sys.argv:
        _json = sys.argv[sys.argv.index("--json") + 1]
    raise SystemExit(main(n=_n, json_out=_json))
