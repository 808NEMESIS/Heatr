"""scripts/run_vision_refresh.py — verse Claude-Sonnet-Vision + observaties (gedoseerd).

De 910 visual_scores zijn een oude batch (~april) en de verbeterpunten-TEKST is nooit
opgeslagen. Deze runner draait Vision opnieuw op een gekozen set: verse screenshot
(capture_site) → analyze_visual → schrijft visual_score + visual_observations
({improvements, strengths, overall, at}) + de verse screenshot-urls. Daarna kan de
pitch een CONCRETE design-observatie noemen i.p.v. alleen een percentiel.

Hergebruikt exact het productie-pad (capture_site + analyze_visual). Vision wordt
geforceerd (technical_score=0) zodat de refresh niet door de skip-drempel valt.

Kosten: ~€0,02/lead (1 Sonnet-Vision-call) + Playwright-capture. DRY-RUN default.
    python3 scripts/run_vision_refresh.py --scope cohort              # targets + raming, 0 kosten
    python3 scripts/run_vision_refresh.py --scope cohort --sample 2   # 2 echte calls, PRINT, schrijf indien kolom bestaat
    python3 scripts/run_vision_refresh.py --scope cohort --apply      # hele cohort
Scopes: cohort (receptie_hook_code) | stale (visual_score >90d/None, launchbaar) | sector.
Vereist migratie 046 (visual_observations-kolom) voor de observatie-write; zonder de
kolom schrijft 'ie alleen visual_score en waarschuwt luid.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WORKSPACE = "aerys"
COST_PER_LEAD_EUR = 0.02


def _fetch_all(db, table: str, cols: str) -> list[dict]:
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def select_targets(db, scope: str) -> list[dict]:
    leads = {l["id"]: l for l in _fetch_all(
        db, "leads", "id, company_name, domain, sector, city, score, icp_match, workspace_id")
        if l.get("workspace_id") == WORKSPACE and l.get("domain")}
    wi = {w["lead_id"]: w for w in _fetch_all(
        db, "website_intelligence", "lead_id, receptie_hook_code, visual_score, workspace_id")
        if w.get("workspace_id") == WORKSPACE}

    if scope == "cohort":
        ids = [lid for lid, w in wi.items() if w.get("receptie_hook_code")]
    elif scope == "sector":
        ids = [lid for lid, l in leads.items() if l.get("sector") == "cosmetische_behandelaars"]
    else:  # stale: launchbaar + geen/oude visual_score
        ids = [lid for lid, l in leads.items()
               if (l.get("score") or 0) >= 55 and (l.get("icp_match") or 0) >= 0.50
               and (wi.get(lid, {}).get("visual_score") is None)]
    return [leads[i] for i in ids if i in leads]


async def refresh_one(db, anthropic_client, lead: dict, write: bool, obs_col_ok: list[bool]) -> dict:
    from website_intelligence.site_capture import capture_site
    from website_intelligence.visual_analyzer import analyze_visual

    domain, lead_id = lead["domain"], lead["id"]
    cap = await capture_site(domain, lead_id, WORKSPACE, db)
    if cap.get("error") or not cap.get("screenshot_desktop_b64"):
        return {"ok": False, "reason": cap.get("error") or "geen screenshot"}
    vis = await analyze_visual(
        domain, WORKSPACE, db, anthropic_client, sector=lead.get("sector") or "",
        technical_score=0,  # forceer Vision (anders skip-drempel)
        screenshot_desktop_b64=cap.get("screenshot_desktop_b64"),
        screenshot_mobile_b64=cap.get("screenshot_mobile_b64"),
    )
    if vis.get("visual_score") is None:
        return {"ok": False, "reason": vis.get("skipped_reason") or "geen score"}

    observations = {
        "improvements": vis.get("top_improvements") or [],
        "strengths": vis.get("top_strengths") or [],
        "overall": vis.get("overall_score"),
        "at": datetime.now(timezone.utc).isoformat(),
    }
    if write:
        patch = {
            "visual_score": vis["visual_score"],
            "screenshot_desktop_url": cap.get("screenshot_desktop_url"),
            "screenshot_mobile_url": cap.get("screenshot_mobile_url"),
            "screenshot_desktop_hash": cap.get("screenshot_desktop_hash"),
            "screenshot_mobile_hash": cap.get("screenshot_mobile_hash"),
        }
        if obs_col_ok[0]:
            patch["visual_observations"] = observations
        try:
            db.table("website_intelligence").update(patch).eq("lead_id", lead_id).eq("workspace_id", WORKSPACE).execute()
        except Exception as e:
            if "visual_observations" in str(e) and obs_col_ok[0]:
                obs_col_ok[0] = False  # kolom bestaat niet → val terug op score-only
                patch.pop("visual_observations", None)
                db.table("website_intelligence").update(patch).eq("lead_id", lead_id).eq("workspace_id", WORKSPACE).execute()
                print("  ⚠ visual_observations-kolom ontbreekt (migratie 046 nog niet gedraaid) → alleen visual_score geschreven")
            else:
                return {"ok": False, "reason": f"write: {str(e)[:60]}"}
    return {"ok": True, "visual_score": vis["visual_score"], "observations": observations}


async def run(scope: str, apply: bool, sample: int) -> int:
    from config.database import get_heatr_supabase

    db = get_heatr_supabase()
    targets = select_targets(db, scope)
    print(f"scope={scope} → {len(targets)} targets · raming ~€{len(targets) * COST_PER_LEAD_EUR:.2f} "
          f"({COST_PER_LEAD_EUR:.02f}/lead Vision + capture)")

    if not apply and not sample:
        for l in targets[:15]:
            print(f"  {l['id'][:8]}  {(l.get('company_name') or '?')[:40]:<42} {l.get('domain')}")
        if len(targets) > 15:
            print(f"  … +{len(targets) - 15} meer")
        print("\nDRY-RUN — geen calls. Volgende: --sample 2 (echte Vision, print) → --apply.")
        return 0

    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        print("ANTHROPIC_API_KEY leeg — kan Vision niet draaien.")
        return 2
    import anthropic
    ac = anthropic.AsyncAnthropic(api_key=key)

    todo = targets[:sample] if sample else targets
    print(f"\n{'SAMPLE' if sample else 'APPLY'} {len(todo)} — verse Vision …\n")
    ok = fail = 0
    obs_col_ok = [True]  # mutable vlag: gedeeld over de run zodra we merken dat de kolom mist
    for i, l in enumerate(todo, 1):
        try:
            r = await refresh_one(db, ac, l, write=apply, obs_col_ok=obs_col_ok)
        except Exception as e:
            r = {"ok": False, "reason": str(e)[:80]}
        if r["ok"]:
            ok += 1
            imp = (r["observations"]["improvements"] or ["—"])[0]
            print(f"  [{i}/{len(todo)}] ✓ {(l.get('company_name') or '?')[:32]:<34} score={r['visual_score']} · '{str(imp)[:70]}'")
        else:
            fail += 1
            print(f"  [{i}/{len(todo)}] ✗ {(l.get('company_name') or '?')[:32]:<34} {r['reason']}")
        await asyncio.sleep(0.5)

    print(f"\nresultaat: ok={ok} fout={fail}" + ("  (SAMPLE — geen writes, puur bewijs)" if sample and not apply else ""))
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--scope", choices=["cohort", "stale", "sector"], default="cohort")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--sample", type=int, default=0, metavar="N")
    args = ap.parse_args()
    raise SystemExit(asyncio.run(run(args.scope, args.apply, args.sample)))
