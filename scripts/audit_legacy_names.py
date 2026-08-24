#!/usr/bin/env python3
"""scripts/audit_legacy_names.py — audit van legacy-voornamen (niet-sweep) in de pool.

Grondwaarheid-audit 2026-08-24: álle voornaam-fouten in de steekproef waren
legacy-namen (Wever=achternaam, Dilan/Annelies=onvindbaar); sweep-namen met
provenance-marker scoorden schoon. Deze audit haalt de legacy-voorraad door
dezelfde molen: verse fetch van homepage + over/team-pagina's (het meetgat van de
eerste audit), naam-met-citaat, en achternaam-contextsignalen (Dr./Drs. ervoor,
kapitaalwoord ervoor/erna) zodat de reviewer 'Hoi Wever'-gevallen ziet.

DRY-RUN default: print per naam het bewijs voor handmatige review. --apply-clears
maakt de OPGEGEVEN lead-ids leeg (contact_first_name=None + audit-notitie in
why_chosen) — expliciete lijst, geen automatische beslissing: een naam leegmaken
is een oordeel, geen regex.

  python3 scripts/audit_legacy_names.py                       # dry-run, bewijs
  python3 scripts/audit_legacy_names.py --apply-clears id1,id2,...
"""
from __future__ import annotations

import argparse
import asyncio
import html as _h
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv  # noqa: E402
load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
_SUB = re.compile(r'href=["\']([^"\']*(?:over|team|about|wie|specialist|behandelaar)[^"\']*)["\']', re.I)
AUDIT_MARKER = "name-audit-2026-08"


def fa(db, t, c):
    o, off = [], 0
    while True:
        d = db.table(t).select(c).eq("workspace_id", WS).range(off, off + 999).execute().data or []
        o += d
        if len(d) < 1000:
            return o
        off += 1000


def strip(t):
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", t, flags=re.I | re.S)
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", _h.unescape(t)).strip()


def contexts(text, name, maxn=3):
    """Alle voorkomens mét context + achternaam-signalen voor de reviewer."""
    out = []
    for m in list(re.finditer(rf"\b{re.escape(name)}\b", text, re.I))[:maxn]:
        i, j = m.start(), m.end()
        before = text[max(0, i - 40):i]
        after = text[j:j + 40]
        flags = []
        if re.search(r"(dr|drs|dokter|prof)\.?\s*$", before, re.I):
            flags.append("TITEL-ERVOOR")           # 'Dr. X' → X is vaak achternaam
        if re.search(r"[A-ZÀ-Ž][a-zà-ž]+\s$", before):
            flags.append("KAPITAAL-ERVOOR")        # 'Voornaam X' → X in achternaam-positie
        if re.search(r"^\s[A-ZÀ-Ž][a-zà-ž]+", after):
            flags.append("kapitaal-erna")          # 'X Achternaam' → X in voornaam-positie
        out.append({"ctx": re.sub(r"\s+", " ", before + name + after).strip(), "flags": flags})
    return out


async def gather_evidence():
    from config.database import get_heatr_supabase
    from website_intelligence.measurement import fetch_httpx, usable_measurement
    from website_intelligence.rendered_fetch import RenderedFetcher
    from utils.legal_form import receptie_avg_safe
    from utils.lead_naming import clean_company_name, display_first_name
    from utils.lead_selection import selection_exclusion

    db = get_heatr_supabase()
    leads = fa(db, "leads", "id,company_name,domain,email,sector,city,contact_first_name,"
               "contact_why_chosen,email_status,score,icp_match,pushed_to_warmr_at,"
               "contact_attempt_count,kvk_legal_form,email_discovery_source")
    targets = []
    for l in leads:
        if (l.get("sector") or "") not in ACTIVE or l.get("email_status") != "valid":
            continue
        if (l.get("score") or 0) < 55 or (l.get("icp_match") or 0) < 0.50:
            continue
        if l.get("pushed_to_warmr_at") or (l.get("contact_attempt_count") or 0):
            continue
        if not display_first_name(l, fallback=""):
            continue
        if "[name-sweep" in (l.get("contact_why_chosen") or ""):
            continue                                  # sweep-namen: al provenance-gedragen
        nm, nr = clean_company_name(l.get("company_name"))
        if not nm or nr:
            continue
        ok, _ = receptie_avg_safe(l)
        if not ok or selection_exclusion(l):
            continue
        targets.append(l)
    print(f"legacy-namen in de kwaliteitspool: {len(targets)}\n")

    sem = asyncio.Semaphore(3)
    rows = []
    async with RenderedFetcher() as rf:
        async def one(l):
            dom = (l.get("domain") or "").strip()
            first = display_first_name(l, fallback="")
            async with sem:
                st, htm = await fetch_httpx(dom)
                usable, _r, _n, reason = await usable_measurement(dom, st, htm, l["sector"])
                if not usable:
                    st, htm = await rf.fetch(dom)
                    usable, _r, _n, reason = await usable_measurement(dom, st, htm, l["sector"])
                text = strip(htm) if usable else ""
                # over/team-subpagina's meenemen (meetgat eerste audit)
                if usable:
                    links = []
                    for href in _SUB.findall(htm)[:6]:
                        if href.startswith("/"):
                            href = f"https://{dom.rstrip('/')}" + href
                        if href.startswith("http") and dom.replace("www.", "").split("/")[0] in href:
                            links.append(href)
                    for href in list(dict.fromkeys(links))[:2]:
                        s2, h2 = await fetch_httpx(href)
                        if s2 and 200 <= s2 <= 299 and h2:
                            text += " " + strip(h2)
            occ = contexts(text, first) if text else []
            return {"id": l["id"], "company": l.get("company_name"), "domain": dom,
                    "first": first, "why": (l.get("contact_why_chosen") or "")[:70],
                    "usable": usable, "textlen": len(text), "occ": occ}
        rows = await asyncio.gather(*[one(l) for l in targets])

    n_found = n_miss = n_unus = 0
    for r in sorted(rows, key=lambda r: (bool(r["occ"]), r["company"] or "")):
        if not r["usable"]:
            n_unus += 1
            print(f"?? ONLEESBAAR   {r['company'][:36]:<38} {r['first']!r}")
            continue
        if not r["occ"]:
            n_miss += 1
            print(f"✗  NIET GEVONDEN {r['company'][:36]:<38} {r['first']!r} ({r['textlen']}ch; bron: {r['why'][:40]!r})")
            print(f"      id={r['id']}")
        else:
            n_found += 1
            flags = sorted({f for o in r["occ"] for f in o["flags"]})
            warn = "  ⚠" + ",".join(flags) if any(f in ("TITEL-ERVOOR", "KAPITAAL-ERVOOR") for f in flags) else ""
            print(f"✓  {r['company'][:36]:<38} {r['first']!r}{warn}")
            for o in r["occ"][:2]:
                print(f"      «{o['ctx'][:96]}» {o['flags']}")
            if warn:
                print(f"      id={r['id']}")
    print(f"\ngevonden {n_found} · niet gevonden {n_miss} · onleesbaar {n_unus} (van {len(rows)})")
    Path("logs").mkdir(exist_ok=True)
    Path("logs/legacy_name_audit.json").write_text(json.dumps(rows, ensure_ascii=False, indent=1))
    print("bewijs → logs/legacy_name_audit.json")


def apply_clears(ids):
    from config.database import get_heatr_supabase
    db = get_heatr_supabase()
    for lid in ids:
        cur = db.table("leads").select("contact_first_name,contact_why_chosen") \
                .eq("id", lid).eq("workspace_id", WS).execute().data
        if not cur:
            print(f"  ?? {lid}: niet gevonden"); continue
        old = cur[0].get("contact_first_name")
        note = (f"[{AUDIT_MARKER}] voornaam {old!r} leeggemaakt na audit "
                f"(niet/verkeerd op eigen site); was: {(cur[0].get('contact_why_chosen') or '')[:80]}")
        db.table("leads").update({"contact_first_name": None, "contact_why_chosen": note}) \
          .eq("id", lid).eq("workspace_id", WS).execute()
        print(f"  leeggemaakt: {lid} (was {old!r})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply-clears", type=str, default="",
                    help="komma-gescheiden lead-ids om leeg te maken (na review)")
    a = ap.parse_args()
    if a.apply_clears:
        apply_clears([x.strip() for x in a.apply_clears.split(",") if x.strip()])
    else:
        asyncio.run(gather_evidence())
