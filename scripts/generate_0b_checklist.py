#!/usr/bin/env python3
"""scripts/generate_0b_checklist.py — de 0b-handcheck als uitdraai van het systeem.

Skill-regel 0b: vóór élke verzending minimaal vijf leads handmatig op mobiel
controleren op de exácte inhoud van hun mail; 1 fout → lead eruit, 2+ → cohort
dicht. Dit script maakt die check een uitdraai in plaats van ad-hoc werk: het
selecteert het C-canary-cohort (Frame C = claimloze kale ask; leads mét bevestigde
boeking, zodat de schaarse Frame-A-pool onaangeroerd blijft), rendert per lead de
échte mail en genereert een afvinklijst (naam+bron, bedrijfsnaam, entiteit, URL).

READ-ONLY; schrijft alleen een lokaal checklist-bestand (logs/, met PII — nooit
committen). Verstuurt niets; kill-switch onaangeroerd.

  python3 scripts/generate_0b_checklist.py [n=25] [uit.md]
"""
from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv  # noqa: E402
load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
PRIVACY = ("Je ontvangt deze mail omdat je praktijk openbaar vindbaar is; "
           "hoe wij met gegevens omgaan lees je op aeryssolution.nl/privacy")


def _fetch_all(db, table, cols):
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).eq("workspace_id", WS).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def main():
    from config.database import get_heatr_supabase
    from campaigns.frictie_copywriter import build_kale_ask_mail1, niche_for_sector
    from utils.legal_form import receptie_avg_safe
    from utils.lead_naming import clean_company_name, display_first_name
    from utils.lead_selection import selection_exclusion

    n = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    out_path = sys.argv[2] if len(sys.argv) > 2 else "logs/0b_checklist_canary.md"

    db = get_heatr_supabase()
    leads = _fetch_all(db, "leads",
                       "id,company_name,domain,email,sector,city,contact_first_name,"
                       "contact_why_chosen,email_status,score,icp_match,pushed_to_warmr_at,"
                       "contact_attempt_count,kvk_legal_form,email_discovery_source")
    wi = {w["lead_id"]: (w.get("conversion_details") or {})
          for w in _fetch_all(db, "website_intelligence", "lead_id,conversion_details")}

    cohort = []
    for l in leads:
        if (l.get("sector") or "") not in ACTIVE or l.get("email_status") != "valid":
            continue
        if (l.get("score") or 0) < 55 or (l.get("icp_match") or 0) < 0.50:
            continue
        if l.get("pushed_to_warmr_at") or (l.get("contact_attempt_count") or 0):
            continue
        if not display_first_name(l, fallback=""):
            continue
        nm, nr = clean_company_name(l.get("company_name"))
        if not nm or nr:
            continue
        ok, _r = receptie_avg_safe(l)
        if not ok or selection_exclusion(l):
            continue
        # C-cohort = bevestigde boeking (True): claimloze mail, en de schaarse
        # geen-boeking-leads (Frame-A-pool) blijven gereserveerd.
        if wi.get(l["id"], {}).get("has_online_booking") is not True:
            continue
        cohort.append(l)
        if len(cohort) >= n:
            break

    lines = [f"# 0b-handcheck — C-canary ({len(cohort)} leads) · {dt.date.today().isoformat()}",
             "",
             "Frame C bevat GEEN claims over de site; de check is dus: klopt de naam,",
             "klopt de bedrijfsnaam, is dit een behandelpraktijk, leest de mail goed.",
             "Regel: 1 fout → die lead eruit. 2+ fout → cohort dicht, oorzaak eerst.",
             ""]
    for i, l in enumerate(cohort, 1):
        first = display_first_name(l, fallback="")
        nm, _ = clean_company_name(l.get("company_name"))
        mail = build_kale_ask_mail1(l, niche=niche_for_sector(l["sector"]),
                                    privacy_notice=PRIVACY, unsubscribe="",
                                    warmr_owns_unsubscribe=True)
        why = (l.get("contact_why_chosen") or "—")[:90]
        lines += [f"## {i}. {nm}  ·  https://{l.get('domain')}",
                  f"- [ ] begroeting klopt: **Hoi {first},**  (bron: {why})",
                  f"- [ ] bedrijfsnaam klopt: **{nm}**",
                  f"- [ ] dit is een behandelpraktijk (geen vereniging/webshop/opleider)",
                  f"- [ ] mail leest goed op mobiel",
                  "", "```",
                  f"Onderwerp: {mail['subject'] if mail else '(RENDER FAALDE)'}", "",
                  (mail["body"] if mail else "(geen mail — naam/gate)"), "```", ""]

    p = Path(out_path)
    p.parent.mkdir(exist_ok=True)
    p.write_text("\n".join(lines))
    print(f"cohort: {len(cohort)} leads → checklist: {p}")


if __name__ == "__main__":
    main()
