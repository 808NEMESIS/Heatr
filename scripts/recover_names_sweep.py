#!/usr/bin/env python3
"""scripts/recover_names_sweep.py — voornaam-terugwinning over naamloze launchbare leads.

De funnel-meting (2026-08-19) toonde: van 351 launchbare leads verliezen we er 201
op 'geen bruikbare voornaam' — de grootste enkele verliespost ná e-mail. Deze sweep
draait de bestaande, bewust-conservatieve owner_name_resolver (bronnen: persoonsnaam
in bedrijfsnaam → over-ons/team-pagina + rolsignaal; nooit e-mail-local-part/domein;
bij twijfel onbepaald) over die groep en schrijft ALLEEN betrouwbare vondsten terug.

DRY-RUN default (niets geschreven). --apply schrijft contact_first_name +
contact_why_chosen mét sweep-marker en bron-attributie — elke write is daardoor
terugvindbaar en omkeerbaar. Verstuurt niets; kill-switch onaangeroerd.

  python3 scripts/recover_names_sweep.py [--apply] [--limit N]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv  # noqa: E402
load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
MARKER = "name-sweep-2026-08"
# Bron → confidence in contact_why_chosen (parser: extract_contact_confidence).
# company_name = eponiem (praktijk heet naar de eigenaar) — vrijwel zeker de juiste
# persoon; over_ons_role = rolsignaal op de eigen site, blind-check bewaakte precisie.
_CONFIDENCE = {"company_name": 90, "over_ons_role": 80}


# Sweep-geheugen (2026-08-31, stopregel-fix): eerder geadjudiceerde extracties
# mogen nooit terugkomen. (a) leads die de naam-audit expliciet leegmaakte
# ([name-audit-marker]) — bv. 'Wever'; (b) selectie-uitgesloten leads (Ben Cao
# e.d.); (c) bekende slechte extractie-doelen (achternaam-/merk-klasse).
_SWEEP_SUPPRESS = ("nova aesthetics", "chiropractor amsterdam", "mijn roos",
                   "doctors karar", "ben cao")
# Rollen die géén eigenaar/behandelaar zijn → naam weigeren (skins/'Amal'-les).
_BAD_ROLE = ("customer experience", "recept", "assistent", "marketing",
             "stagiair", "administrat", "front desk", "office")


def _launchable_nameless(l, safe_first_name):
    from utils.lead_selection import selection_exclusion
    if "[name-audit" in (l.get("contact_why_chosen") or ""):
        return False                                   # audit-cleared: nooit heropvoeren
    if selection_exclusion(l):
        return False
    if any(x in (l.get("company_name") or "").lower() for x in _SWEEP_SUPPRESS):
        return False
    return (l.get("email_status") == "valid" and (l.get("score") or 0) >= 55
            and (l.get("icp_match") or 0) >= 0.50 and (l.get("sector") or "") in ACTIVE
            and not l.get("pushed_to_warmr_at") and not (l.get("contact_attempt_count") or 0)
            and (l.get("domain") or "").strip() and not safe_first_name(l))


async def main(apply: bool, limit: int) -> int:
    from config.database import get_heatr_supabase
    from utils.lead_naming import safe_first_name
    from utils.playwright_helpers import new_browser_context
    from scripts.measure_name_recovery import _one
    from playwright.async_api import async_playwright
    import anthropic

    sb = get_heatr_supabase()
    rows, off = [], 0
    while True:
        d = (sb.table("leads").select(
             "id,domain,company_name,sector,contact_first_name,contact_why_chosen,email,"
             "email_status,score,icp_match,pushed_to_warmr_at,contact_attempt_count")
             .eq("workspace_id", WS).range(off, off + 999).execute().data) or []
        rows += d
        if len(d) < 1000:
            break
        off += 1000
    targets = [l for l in rows if _launchable_nameless(l, safe_first_name)]
    if limit:
        targets = targets[:limit]
    print(f"Voornaam-sweep over {len(targets)} naamloze launchbare leads "
          f"({'APPLY — schrijft namen mét marker' if apply else 'DRY-RUN'}).\n")

    client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    async with async_playwright() as pw:
        br, _ctx = await new_browser_context(pw)
        try:
            sem = asyncio.Semaphore(6)
            res = await asyncio.gather(*[_one(sem, pw, br, sb, client, l) for l in targets])
        finally:
            await br.close()

    found = [r for r in res if r["first_name"]
             and not any(b in (r.get("detail") or "").lower() for b in _BAD_ROLE)]
    by_source = Counter(r["source"] for r in found)
    onbep = Counter(r["detail"] for r in res if not r["first_name"])
    print(f"\n{'=' * 62}")
    print(f"GEVONDEN: {len(found)}/{len(res)} ({100 * len(found) // max(1, len(res))}%) — "
          f"per bron: {dict(by_source)}")
    print("ONBEPAALD — redenen:", dict(onbep.most_common(6)))

    applied = 0
    if apply:
        for r in found:
            conf = _CONFIDENCE.get(r["source"], 70)
            why = (f"Eigenaar via {r['source']} ({r['detail'] or 'match'}) "
                   f"[{MARKER}] (confidence: {conf}%)")
            sb.table("leads").update({"contact_first_name": r["first_name"],
                                      "contact_why_chosen": why}) \
              .eq("id", r["id"]).eq("workspace_id", WS).execute()
            applied += 1
        print(f"\n→ {applied} namen geschreven (marker {MARKER}; omkeerbaar via de marker).")
    else:
        print("\n→ DRY-RUN: niets geschreven. Voorbeeld-vondsten:")
        for r in found[:12]:
            print(f"    {(r['company'] or '')[:34]:<36} → {r['first_name']:<14} [{r['source']}]")
    out = Path(__file__).resolve().parent.parent / "logs" / "name_sweep_last.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(res, ensure_ascii=False, indent=1))
    print(f"details → {out}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    raise SystemExit(asyncio.run(main(a.apply, a.limit)))
