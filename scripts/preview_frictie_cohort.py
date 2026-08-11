#!/usr/bin/env python3
"""scripts/preview_frictie_cohort.py — Fase 1 preview: render de Frame A/C-cohort.

READ-ONLY. Verstuurt NIETS (kill-switch onaangeroerd). Selecteert launchbare,
AVG-veilige leads MET een geverifieerde voornaam (harde filter — een koude mail
zonder naam is een verspilde lead), en met een VERSE geen-boeking-frictie. Rendert
mail-1 via de frictie-engine en draait de geautomatiseerde 10-punts zelfcontrole.
Print elke mail + een geaggregeerd rapport (frames, fails per categorie, stale).

Cohort 1 = alternatieve zorg (Sami/Hormozi 2026-08-11): bij cosmetisch is telefonisch
boeken vaak beleid, dan botst de frictieobservatie met hun ontwerp. Gebruik:
  python3 scripts/preview_frictie_cohort.py [n_alt=25] [n_cosm=0] [max_age_days=45]
"""
from __future__ import annotations

import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from config.database import get_heatr_supabase
from campaigns.frictie_copywriter import render_frictie_mail, select_leak, niche_for_sector
from utils.legal_form import receptie_avg_safe
from utils.lead_naming import clean_company_name, display_first_name

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
PRIVACY = (os.getenv("RECEPTIE_PRIVACY_NOTICE")
           or "Je ontvangt deze mail omdat je praktijk openbaar vindbaar is; "
              "hoe wij met gegevens omgaan lees je op aeryssolution.nl/privacy")


def _fetch_all(db, table, cols):
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).eq("workspace_id", WS).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def _launchable(l):
    return (l.get("email_status") == "valid" and (l.get("score") or 0) >= 55
            and (l.get("icp_match") or 0) >= 0.50 and (l.get("sector") or "") in ACTIVE
            and not l.get("pushed_to_warmr_at") and not (l.get("contact_attempt_count") or 0))


def main():
    n_alt = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    n_cosm = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    max_age = int(sys.argv[3]) if len(sys.argv) > 3 else 30
    today = dt.date.today()
    db = get_heatr_supabase()
    leads = _fetch_all(db, "leads",
                       "id,company_name,domain,email,sector,city,contact_first_name,archetype,"
                       "contact_why_chosen,email_status,score,icp_match,pushed_to_warmr_at,"
                       "contact_attempt_count,kvk_legal_form,email_discovery_source")
    wi = {w["lead_id"]: w for w in _fetch_all(db, "website_intelligence",
                                              "lead_id,conversion_details,analyzed_at")}

    # Kandidaten: launchbaar + AVG-veilig + geverifieerde voornaam + verse geen-boeking-frictie.
    cands = {"cosmetisch": [], "alt": []}
    for l in leads:
        if not _launchable(l):
            continue
        if not display_first_name(l, fallback=""):
            continue                                   # harde voornaam-filter
        _naam, needs_review = clean_company_name(l.get("company_name"))
        if not _naam or needs_review:
            continue                                   # geen title-tag-naam de deur uit
        ok, _r = receptie_avg_safe(l)
        if not ok:
            continue
        w = wi.get(l["id"], {})
        cd = w.get("conversion_details") or {}
        if select_leak(cd) is None:                    # alleen Frame-A-kandidaten (geen-boeking)
            continue
        cands[niche_for_sector(l["sector"])].append((l, cd, w.get("analyzed_at")))

    picks = cands["alt"][:n_alt] + cands["cosmetisch"][:n_cosm]
    print(f"Frame-A-kandidaten (voornaam + verse geen-boeking): alt {len(cands['alt'])}, "
          f"cosmetisch {len(cands['cosmetisch'])}.")
    print(f"Preview: {n_alt} alt + {n_cosm} cosmetisch = {len(picks)} (vers-gate {max_age}d).\n")

    fail_tally, frame_tally, arch_tally = {}, {}, {}
    n_pass, n_stale, words = 0, 0, []
    for i, (l, cd, analyzed_at) in enumerate(picks, 1):
        arch = l.get("archetype") or "—"
        arch_tally[arch] = arch_tally.get(arch, 0) + 1
        mail = render_frictie_mail(l, sector=l["sector"], conversion_details=cd,
                                   privacy_notice=PRIVACY, unsubscribe="",
                                   warmr_owns_unsubscribe=True,
                                   analyzed_at=analyzed_at, max_age_days=max_age, now=today)
        if mail is None:
            print(f"[{i}] {l.get('company_name')!r} — GEEN mail (naam onbruikbaar)\n")
            continue
        sc = mail["selfcheck"]
        frame_tally[mail["frame"]] = frame_tally.get(mail["frame"], 0) + 1
        n_pass += 1 if sc["passed"] else 0
        n_stale += 1 if mail["stale_friction"] else 0
        words.append(sc["words"])
        for c in sc["fails"]:
            fail_tally[c] = fail_tally.get(c, 0) + 1
        status = "PASS" if sc["passed"] else f"FAIL {sc['detail']}"
        print("=" * 78)
        print(f"[{i}] {l.get('company_name')}  ·  {l.get('sector')}  ·  {l.get('city') or '?'}  ·  {arch}")
        print(f"    frame {mail['frame']} · {sc['words']}w · {sc['iks']}x ik · {status}")
        print(f"    Onderwerp: {mail['subject']}")
        print("-" * 78)
        print(mail["body"])
        print()

    print("=" * 78)
    print(f"SAMENVATTING — {n_pass}/{len(picks)} passeren de zelfcontrole.")
    print(f"  frames: {frame_tally} · stale (→ Frame C): {n_stale}")
    if words:
        print(f"  woorden pitch: min {min(words)}, max {max(words)}, gem {sum(words)//len(words)} (limiet 160)")
    print(f"  fails per categorie: {fail_tally or 'geen'}")
    # Archetype-attributie (Sami 2026-08-11): zodat je in de replies de osteopaten-rand
    # (lichaamswerk_pragmatisch, mildste twijfelaar-fit) kunt terugzien.
    print(f"  archetype-verdeling: {dict(sorted(arch_tally.items(), key=lambda x: -x[1]))}")


if __name__ == "__main__":
    main()
