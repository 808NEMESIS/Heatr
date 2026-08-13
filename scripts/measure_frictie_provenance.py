#!/usr/bin/env python3
"""scripts/measure_frictie_provenance.py — STAP 1 (alleen meten, geen fixes).

Bepaalt per lead of de opgeslagen has_online_booking uit BRUIKBARE HTML komt of uit
een geblokkeerde/lege fetch. conversion_details bewaart GEEN statuscode/grootte, dus
de enige stored-herkomstindicator is content-rijkheid (positieve signalen). Voor de
all-lege leads wordt een VERSE probe gedaan om de subklasse te bepalen (blokkeert de
site httpx, of is de herkomst niet vast te stellen). Verandert niets aan engine/gate/
selectie; verstuurt niets.

Uitkomst per lead: frictie_gemeten / frictie_uit_geblokkeerde_fetch / onbepaald.
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import httpx

from config.database import get_heatr_supabase
from campaigns.frictie_copywriter import niche_for_sector, select_leak
from utils.legal_form import receptie_avg_safe
from utils.lead_naming import clean_company_name, display_first_name
from website_intelligence.conversion_checker import check_conversion

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
_UA = {"User-Agent": "Mozilla/5.0 (compatible; AerysBot/1.0; +https://aeryssolution.nl)"}
_WALL = ("checking your browser", "just a moment", "enable javascript", "attention required")


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


def _richness(cd: dict) -> int:
    """Aantal POSITIEVE content-signalen in conversion_details. 0 = de fetch leverde
    geen herkenbare pagina-inhoud op (handtekening van geblokkeerde/lege respons)."""
    if not cd:
        return 0
    sig = [
        bool(cd.get("cta_texts")),
        cd.get("has_phone_clickable") is True,
        cd.get("has_whatsapp") is True,
        cd.get("has_contact_form") is True,
        (cd.get("form_field_count") or 0) > 0,
        cd.get("has_chatbot") is True,
        bool(cd.get("booking_platform")),
        (cd.get("conversion_score") or 0) > 0,
        cd.get("has_cta_above_fold") is True,
        any(d.get("passed") is True for d in (cd.get("details") or [])),
    ]
    return sum(1 for s in sig if s)


async def _probe(sem, l):
    """Verse probe alleen voor de all-lege leads: status + grootte + muur-detectie +
    verse content-rijkheid (via check_conversion op de verse HTML)."""
    dom = (l.get("domain") or "").strip()
    url = dom if dom.startswith("http") else f"https://{dom}"
    async with sem:
        for u in (url, url.replace("https://", "http://")):
            try:
                async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers=_UA) as c:
                    r = await c.get(u)
                    text = r.text or ""
                    wall = any(w in text.lower()[:3000] for w in _WALL)
                    blocked = (r.status_code in (401, 403, 429, 503) or wall or len(text.strip()) < 400)
                    fresh_rich = 0 if blocked or not text else _richness(await check_conversion(dom, text, l["sector"]))
                    return {"status": r.status_code, "size": len(text), "wall": wall,
                            "blocked": blocked, "fresh_rich": fresh_rich}
            except Exception:
                continue
    return {"status": None, "size": 0, "wall": False, "blocked": True, "fresh_rich": 0, "err": True}


async def main():
    db = get_heatr_supabase()
    leads = {l["id"]: l for l in _fetch_all(db, "leads",
             "id,company_name,domain,sector,email_status,score,icp_match,pushed_to_warmr_at,"
             "contact_attempt_count,kvk_legal_form,email_discovery_source,contact_first_name,contact_why_chosen")}
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
        cd = wi.get(l["id"], {})
        if select_leak(cd) is None:
            continue
        picks.append((l, cd))
    picks.sort(key=lambda t: 0 if niche_for_sector(t[0]["sector"]) == "alt" else 1)

    # richness uit STORED data (primair signaal)
    rows = [(l, cd, _richness(cd)) for l, cd in picks]
    empties = [(l, cd) for l, cd, r in rows if r == 0]

    # verse probe alleen voor de all-lege
    sem = asyncio.Semaphore(8)
    probes = dict(zip((l["id"] for l, _ in empties),
                      await asyncio.gather(*[_probe(sem, l) for l, _ in empties])))

    gemeten, geblokkeerd, onbepaald = [], [], []
    for l, cd, r in rows:
        rev = "rev" if cd.get("checked_at") else "orig"     # alt=gereverifieerd, cosm=origineel
        if r >= 1:
            gemeten.append((l, r, rev, None))
        else:
            p = probes[l["id"]]
            tag = f"{rev} status={p.get('status')} size={p.get('size')} wall={p.get('wall')} versrijk={p.get('fresh_rich')}"
            if p["blocked"]:
                geblokkeerd.append((l, r, rev, tag))
            else:
                onbepaald.append((l, r, rev, tag))       # site nu bereikbaar, stored leeg → herkomst onbepaald

    def _line(t):
        l, r, rev, tag = t
        return f"    - {l.get('company_name')[:40]:<40} [{niche_for_sector(l['sector'])[:4]}] stored_rijkheid={r} {tag or rev}"

    print(f"TOTAAL {len(rows)} leads · stored-rijkheid=0 (all-leeg): {len(empties)}\n")
    print(f"=== frictie_gemeten ({len(gemeten)}) — stored bewijst bruikbare HTML ===")
    alt_g = sum(1 for l, *_ in gemeten if niche_for_sector(l['sector']) == 'alt')
    print(f"    (alt {alt_g} / cosm {len(gemeten)-alt_g}) — niet uitgeprint\n")
    print(f"=== frictie_uit_geblokkeerde_fetch ({len(geblokkeerd)}) — leeg + site blokkeert httpx ===")
    for t in geblokkeerd:
        print(_line(t))
    print(f"\n=== onbepaald ({len(onbepaald)}) — stored leeg maar herkomst niet vast te stellen ===")
    for t in onbepaald:
        print(_line(t))

    # kruispunt: alt-leads die de reverify --apply als 'bevestigd' schreef maar all-leeg zijn
    alt_rev_empty = [l for l, cd, r in rows if r == 0 and cd.get("checked_at") and not cd.get("reverify_uncertain")]
    print(f"\n=== KRUISPUNT: reverify --apply schreef 'bevestigd geen-boeking' op all-lege data ===")
    print(f"    {len(alt_rev_empty)} alt-lead(s): {[l.get('company_name')[:34] for l in alt_rev_empty]}")

    print(f"\nSAMENVATTING: gemeten {len(gemeten)} · geblokkeerd {len(geblokkeerd)} · onbepaald {len(onbepaald)}")


if __name__ == "__main__":
    asyncio.run(main())
