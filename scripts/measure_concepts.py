#!/usr/bin/env python3
"""scripts/measure_concepts.py — MEET-ronde over de 48 concept-leads (geen fixes).

READ-ONLY. Verandert niets aan engine/copy/selectie. Reproduceert dezelfde 48
eligible Frame-A-leads, haalt elke homepage op en verzamelt bewijs voor vier assen:
entiteitstype, voornaamgeldigheid, naamdubbelingen, onderwerpregelhygiëne. Plus:
tweede niet-generieke vondst voor mail 2. Print een compacte tabel + schrijft de
homepage-bewijsblokken (title/meta/koppen/tekstsample) naar een bestand om op te
oordelen. Gokt niets — lege/onleesbare inhoud → onbepaald.
"""
from __future__ import annotations

import asyncio
import html as _html
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import httpx

from config.database import get_heatr_supabase
from campaigns.frictie_copywriter import (
    _domain, _subject, niche_for_sector, select_leak, select_second_finding,
)
from utils.legal_form import receptie_avg_safe
from utils.lead_naming import clean_company_name, display_first_name

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
_UA = {"User-Agent": "Mozilla/5.0 (compatible; AerysBot/1.0; +https://aeryssolution.nl)"}
SCRATCH = "/private/tmp/claude-501/-Users-nemesis-Heatr/2bb63301-a1e9-411c-a043-938e885ebdc3/scratchpad"

# Signaalwoorden per entiteitstype (indicatief — oordeel volgt uit title/meta/koppen).
_KW = {
    "webshop": ["winkelwagen", "winkelmand", "afrekenen", "bestellen", "gratis verzending",
                "op voorraad", "webshop", "voeg toe", "shop", "assortiment"],
    "vereniging_koepel": ["beroepsvereniging", "vereniging", "leden", "aangesloten", "word lid",
                          "lidmaatschap", "koepel", "belangenbehartiging", "onze leden", "register"],
    "opleider": ["opleiding", "cursus", "cursussen", "inschrijven", "lesrooster", "studenten",
                 "academie", "opleidingen", "workshop", "docent"],
    "praktijk": ["afspraak", "behandeling", "consult", "spreekuur", "onze praktijk", "patiënt",
                 "intake", "klachten", "eerste afspraak", "behandelingen", "therapeut"],
}


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


def _strip(t):
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", t, flags=re.I | re.S)
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", _html.unescape(t)).strip()


async def _get(dom):
    url = dom if dom.startswith("http") else f"https://{dom}"
    for u in (url, url.replace("https://", "http://")):
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers=_UA) as c:
                r = await c.get(u)
                if r.text:
                    return r.text
        except Exception:
            continue
    return ""


async def _probe(sem, lead):
    async with sem:
        html = await _get((lead.get("domain") or "").strip())
    if not html:
        return {"ok": False}
    title = (re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S) or [None, ""])[1]
    meta = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']', html, re.I)
    ogd = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']', html, re.I)
    heads = [_strip(h) for h in re.findall(r"<h[1-3][^>]*>(.*?)</h[1-3]>", html, re.I | re.S)][:8]
    text = _strip(html)
    low = text.lower()
    hits = {k: [w for w in ws if w in low] for k, ws in _KW.items()}
    price = len(re.findall(r"€\s?\d", html))
    return {"ok": True, "title": _strip(title), "meta": _strip((meta or ogd or [None, ""])[1] if (meta or ogd) else ""),
            "heads": heads, "text": text[:900], "hits": hits, "price": price}


async def main():
    out_path = os.path.join(SCRATCH, "meting_48.txt")
    db = get_heatr_supabase()
    leads = {l["id"]: l for l in _fetch_all(db, "leads",
             "id,company_name,domain,email,sector,city,contact_first_name,archetype,"
             "contact_why_chosen,email_status,score,icp_match,pushed_to_warmr_at,"
             "contact_attempt_count,kvk_legal_form,email_discovery_source")}
    wi = {w["lead_id"]: (w.get("conversion_details") or {})
          for w in _fetch_all(db, "website_intelligence", "lead_id,conversion_details")}

    picks = []
    for l in leads.values():
        if not _launchable(l) or not display_first_name(l, fallback=""):
            continue
        n, nr = clean_company_name(l.get("company_name"))
        if not n or nr:
            continue
        ok, _r = receptie_avg_safe(l)
        if not ok:
            continue
        if select_leak(wi.get(l["id"], {})) is None:
            continue
        picks.append(l)
    # alt eerst, dan cosmetisch (zelfde volgorde-idee als generate_concepts)
    picks.sort(key=lambda l: 0 if niche_for_sector(l["sector"]) == "alt" else 1)

    sem = asyncio.Semaphore(8)
    probes = await asyncio.gather(*[_probe(sem, l) for l in picks])

    lines = []
    for i, (l, p) in enumerate(zip(picks, probes), 1):
        naam, _ = clean_company_name(l.get("company_name"))
        first = display_first_name(l, fallback="")
        subj = _subject("{naam} op mobiel", naam)
        # subject-hygiëne
        pollution = [m for m in (":", ",", "|", "B.V.", "D.O.", " BV", " NV", "•") if m in subj]
        # voornaam-signalen
        in_company = bool(first) and first.lower() in (l.get("company_name") or "").lower()
        on_page = bool(first) and p.get("ok") and re.search(rf"\b{re.escape(first)}\b", p["text"], re.I) is not None
        # tweede vondst mail 2
        sf = select_second_finding(wi.get(l["id"], {}))
        sf_lbl = sf[0] if sf else "GEEN"
        hits = p.get("hits", {})
        hit_str = " ".join(f"{k[:4]}={len(v)}" for k, v in hits.items()) if p.get("ok") else "FETCH-FOUT"
        lines.append(
            f"[{i:02d}] {l.get('company_name')[:44]!r}\n"
            f"     niche={niche_for_sector(l['sector'])} arch={l.get('archetype')} "
            f"voornaam={first!r} in_bedrijfsnaam={in_company} op_pagina={on_page}\n"
            f"     why_chosen={(l.get('contact_why_chosen') or '')[:80]!r}\n"
            f"     onderwerp={subj!r} vervuild={pollution or 'schoon'}\n"
            f"     mail2_2e_vondst={sf_lbl} | prijs€={p.get('price',0)} | {hit_str}\n"
            f"     title={p.get('title','')[:110]!r}\n"
            f"     meta={p.get('meta','')[:160]!r}\n"
            f"     koppen={p.get('heads',[])[:5]}\n"
            f"     tekst={p.get('text','')[:350]!r}\n"
        )
    with open(out_path, "w") as f:
        f.write("\n".join(lines))

    # compacte console-tabel
    print(f"{len(picks)} leads gemeten. Volledig bewijs → {out_path}\n")
    # naamdubbelingen
    from collections import Counter, defaultdict
    byname = defaultdict(list)
    for l in picks:
        byname[display_first_name(l, fallback="")].append(l)
    dups = {k: v for k, v in byname.items() if len(v) > 1}
    print("=== NAAMDUBBELINGEN ===")
    for name, ls in sorted(dups.items(), key=lambda x: -len(x[1])):
        print(f"  {name!r} ×{len(ls)}:")
        for l in ls:
            print(f"      - {l.get('company_name')[:40]!r}  bron={(l.get('contact_why_chosen') or '')[:60]!r}")
    # subject-vervuiling
    print("\n=== ONDERWERP-VERVUILING ===")
    nvuil = 0
    for l in picks:
        naam, _ = clean_company_name(l.get("company_name"))
        subj = _subject("{naam} op mobiel", naam)
        poll = [m for m in (":", ",", "|", "B.V.", "D.O.", " BV", " NV", "•") if m in subj]
        if poll:
            nvuil += 1
            print(f"  {poll} · {subj!r}")
    print(f"  → vervuild: {nvuil}/{len(picks)}")
    # mail 2 tweede vondst
    print("\n=== MAIL 2 — TWEEDE NIET-GENERIEKE VONDST ===")
    c = Counter()
    geen = []
    for l in picks:
        sf = select_second_finding(wi.get(l["id"], {}))
        c[sf[0] if sf else "GEEN"] += 1
        if not sf:
            geen.append(l.get("company_name"))
    print(f"  verdeling: {dict(c)}")
    print(f"  GEEN tweede vondst ({len(geen)}): {[g[:34] for g in geen]}")
    # fetch-fouten
    err = [l.get("company_name") for l, p in zip(picks, probes) if not p.get("ok")]
    print(f"\n=== FETCH-FOUTEN ({len(err)}) → entiteitstype onbepaald ===\n  {[e[:34] for e in err]}")


if __name__ == "__main__":
    asyncio.run(main())
