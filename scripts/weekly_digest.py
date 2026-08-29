#!/usr/bin/env python3
"""scripts/weekly_digest.py — wekelijks voorraad-overzicht (maandag 08:00 via launchd).

Beantwoordt de "hoeveel nu?"-vragen permanent: lijstgroei, analyse-dekking,
kwaliteitspool, verzendbaar-potentieel, Bouncer-wachtrij, benchmark-dichtheid en
canary-status — naar logs/weekly_digest.md (en stdout). READ-ONLY.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv  # noqa: E402
load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}


def fa(db, t, c):
    o, off = [], 0
    while True:
        d = db.table(t).select(c).eq("workspace_id", WS).range(off, off + 999).execute().data or []
        o += d
        if len(d) < 1000:
            return o
        off += 1000


def main():
    from config.database import get_heatr_supabase
    from website_intelligence.measurement import richness
    from utils.legal_form import receptie_avg_safe
    from utils.lead_naming import clean_company_name, display_first_name
    from utils.lead_selection import selection_exclusion
    import collections

    db = get_heatr_supabase()
    today = dt.date.today()
    week_ago = (today - dt.timedelta(days=7)).isoformat()
    leads = fa(db, "leads", "id,company_name,domain,sector,city,status,email_status,score,"
               "icp_match,pushed_to_warmr_at,contact_attempt_count,kvk_legal_form,"
               "email_discovery_source,contact_first_name,contact_why_chosen,created_at")
    wi = {w["lead_id"]: (w.get("conversion_details") or {})
          for w in fa(db, "website_intelligence", "lead_id,conversion_details")}

    tot = len(leads)
    nieuw7 = sum(1 for l in leads if (l.get("created_at") or "") >= week_ago)
    es = collections.Counter(l.get("email_status") or "leeg" for l in leads
                             if (l.get("sector") or "") in ACTIVE)
    have_wi = sum(1 for l in leads if l["id"] in wi)
    valid_wi = sum(1 for l in leads if l["id"] in wi and richness(wi[l["id"]]) >= 1
                   and not wi[l["id"]].get("reverify_uncertain"))
    prov = sum(1 for l in leads if l["id"] in wi and wi[l["id"]].get("reverify_probe"))

    pool = c_ready = a_res = 0
    for l in leads:
        if (l.get("sector") or "") not in ACTIVE or l.get("email_status") != "valid":
            continue
        if (l.get("score") or 0) < 55 or (l.get("icp_match") or 0) < 0.50:
            continue
        if l.get("pushed_to_warmr_at") or (l.get("contact_attempt_count") or 0):
            continue
        if not display_first_name(l, fallback=""):
            continue
        n, nr = clean_company_name(l.get("company_name"))
        if not n or nr:
            continue
        ok, _ = receptie_avg_safe(l)
        if not ok or selection_exclusion(l):
            continue
        pool += 1
        cd = wi.get(l["id"], {})
        if cd.get("has_online_booking") is False and not cd.get("reverify_uncertain"):
            a_res += 1
        else:
            c_ready += 1

    # benchmark-dichtheid (cellen N>=10, geldige metingen)
    bycell = collections.Counter()
    lm = {l["id"]: l for l in leads}
    for lid, cd in wi.items():
        l = lm.get(lid)
        if not l or (l.get("sector") or "") not in ACTIVE:
            continue
        if richness(cd) >= 1 and not cd.get("reverify_uncertain"):
            bycell[(l.get("city") or "?", l["sector"])] += 1
    cells = sum(1 for _k, n in bycell.items() if n >= 10)

    lines = [f"# Heatr weekly digest · {today.isoformat()}", "",
             f"| | |", f"|--|--|",
             f"| totale lijst | **{tot}** (+{nieuw7} in 7 dagen) |",
             f"| analyse-dekking | {have_wi} ({100*have_wi//max(1,tot)}%) · geldig {valid_wi} · met provenance {prov} |",
             f"| kwaliteitspool | **{pool}** |",
             f"| → C-ready (claimloos frame) | {c_ready} |",
             f"| → Frame-A-reserve (geen boeking) | {a_res} |",
             f"| Bouncer-wachtrij (not_checked) | **{es.get('not_checked', 0)}** — wacht op tegoed |",
             f"| e-mail valid / catchall / not_found | {es.get('valid',0)} / {es.get('catchall_risky',0)} / {es.get('not_found',0)} |",
             f"| benchmark-cellen (stad×sector N≥10) | {cells} |",
             "",
             "Canary: 14 leads klaar (logs/0b_checklist_canary.md) · faseplan bevroren tot arming.",
             "Hendels bij Sami: Bouncer-tegoed · canary-arming · inbox-capaciteit."]
    out = Path("logs/weekly_digest.md")
    out.parent.mkdir(exist_ok=True)
    out.write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
