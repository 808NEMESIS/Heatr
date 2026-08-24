#!/usr/bin/env python3
"""scripts/audit_enrichment_ground_truth.py — enrichment-grondwaarheid-audit (READ-ONLY).

Vast instrument (brug 4, 2026-08-24): draai na elke sweep/apply als regressiemeting.
Gerepareerd t.o.v. de eerste versie: (a) naam-check scant ook over/team-subpagina's,
(b) bedrijfsnaam-criterium met woordgrenzen (substring-match liet 'Nova verbetering'
vs 'Huidverbetering' door). Gebruik: python3 scripts/audit_enrichment_ground_truth.py [n] [seed]

Aselecte steekproef (seed=42) uit de kwaliteitspool. Per lead: verse fetch
(httpx→Playwright-fallback) van homepage + contactpagina, en veld-voor-veld
vergelijking van de opgeslagen enrichment met wat er op de site staat. Per veld:
correct / fout / onbepaald + bewijsregel. Geen writes, niets verstuurd.
"""
import asyncio, html as _h, random, re, sys
sys.path.insert(0, "/Users/nemesis/Heatr")
from dotenv import load_dotenv; load_dotenv("/Users/nemesis/Heatr/.env")

from config.database import get_heatr_supabase
from website_intelligence.conversion_checker import detect_booking, _WHATSAPP_LINK_RE
from website_intelligence.measurement import fetch_httpx, usable_measurement
from website_intelligence.rendered_fetch import RenderedFetcher
from utils.legal_form import receptie_avg_safe
from utils.lead_naming import clean_company_name, display_first_name
from utils.lead_selection import selection_exclusion

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
N = int(sys.argv[1]) if len(sys.argv) > 1 else 20
SEED = int(sys.argv[2]) if len(sys.argv) > 2 else 42
COSM_KW = ("botox", "filler", "laser", "huidtherap", "huidverbeter", "injectable",
           "cosmetisch", "peeling", "ontharen", "aesthetic", "esthetisch", "implanto")
ALT_KW = ("acupunctuur", "osteopat", "osteopath", "homeopat", "natuurgenees", "hijama",
          "cupping", "shiatsu", "tcm", "kruiden", "haptonom", "reflex")


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


async def fetch_usable(rf, dom, sector):
    st, htm = await fetch_httpx(dom)
    usable, _r, _n, reason = await usable_measurement(dom, st, htm, sector)
    method = "httpx"
    if not usable:
        st, htm = await rf.fetch(dom)
        usable, _r, _n, reason = await usable_measurement(dom, st, htm, sector)
        method = "playwright"
    return usable, htm, method, reason


SUB_RE = re.compile(r'href=["\']([^"\']*(?:over|team|about|wie)[^"\']*)["\']', re.I)


async def subpage_html(dom, base_html, pattern):
    m = pattern.search(base_html)
    if not m:
        return ""
    href = m.group(1)
    if href.startswith("/"):
        href = f"https://{dom.rstrip('/')}" + href
    if not href.startswith("http") or dom.split("//")[-1].split("/")[0].replace("www.", "") not in href:
        return ""
    _st, htm = await fetch_httpx(href)
    return htm or ""


async def contact_html(dom, base_html):
    m = re.search(r'href=["\']([^"\']*contact[^"\']*)["\']', base_html, re.I)
    if not m:
        return ""
    href = m.group(1)
    if href.startswith("/"):
        href = f"https://{dom.rstrip('/')}" + href
    if not href.startswith("http") or dom.split("//")[-1].split("/")[0].replace("www.", "") not in href:
        return ""
    _st, htm = await fetch_httpx(href)
    return htm or ""


async def main():
    db = get_heatr_supabase()
    leads = fa(db, "leads", "id,company_name,domain,email,sector,city,contact_first_name,"
               "contact_why_chosen,email_status,score,icp_match,pushed_to_warmr_at,"
               "contact_attempt_count,kvk_legal_form,email_discovery_source")
    wi = {w["lead_id"]: (w.get("conversion_details") or {})
          for w in fa(db, "website_intelligence", "lead_id,conversion_details")}

    pool = []
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
        ok, _ = receptie_avg_safe(l)
        if not ok or selection_exclusion(l):
            continue
        pool.append(l)
    rng = random.Random(SEED)
    sample = rng.sample(pool, min(N, len(pool)))
    print(f"kwaliteitspool {len(pool)} → steekproef {len(sample)} (seed 42)\n")

    sem = asyncio.Semaphore(3)
    tallies = {}

    def mark(field, verdict):
        tallies.setdefault(field, {"correct": 0, "fout": 0, "onbepaald": 0})
        tallies[field][verdict] += 1

    async with RenderedFetcher() as rf:
        async def one(l):
            async with sem:
                usable, htm, method, reason = await fetch_usable(rf, l["domain"], l["sector"])
                c_html = await contact_html(l["domain"], htm) if usable else ""
                o_html = await subpage_html(l["domain"], htm, SUB_RE) if usable else ""
            return l, usable, htm, c_html + " " + o_html, method, reason
        results = await asyncio.gather(*[one(l) for l in sample])

    for l, usable, htm, c_html, method, reason in results:
        nm, _ = clean_company_name(l.get("company_name"))
        first = display_first_name(l, fallback="")
        cd = wi.get(l["id"], {})
        print("=" * 88)
        print(f"### {nm}  · {l['domain']}  · via {method}" + ("" if usable else f"  ONLEESBAAR ({reason})"))
        if not usable:
            for f in ("voornaam", "email", "bedrijfsnaam", "sector", "stad", "entiteit",
                      "booking", "telefoon", "whatsapp"):
                mark(f, "onbepaald")
            continue
        text = strip(htm); textc = strip(c_html); alltext = text + " " + textc
        low = alltext.lower()
        # voornaam
        m = re.search(rf"\b{re.escape(first)}\b", alltext, re.I)
        src = "sweep" if "[name-sweep" in (l.get("contact_why_chosen") or "") else "legacy"
        if m:
            i = alltext.lower().find(first.lower())
            ev = re.sub(r"\s+", " ", alltext[max(0, i - 50):i + len(first) + 50])
            mark("voornaam", "correct"); print(f"  voornaam[{src}] ✓ {first!r}: …{ev[:88]}…")
        else:
            mark("voornaam", "fout" if len(alltext) > 2000 else "onbepaald")
            print(f"  voornaam[{src}] ✗ {first!r} NIET op home+contact ({len(alltext)}ch)")
        # email
        em = (l.get("email") or "").lower()
        dom_match = em.split("@")[-1].replace("www.", "") in (l["domain"] or "").lower().replace("www.", "")
        on_site = em in low
        v = "correct" if (on_site or dom_match) else "fout"
        mark("email", v); print(f"  email {'✓' if v=='correct' else '✗'} {em} (op site: {on_site}, domein-match: {dom_match})")
        # bedrijfsnaam
        title = strip((re.search(r"<title[^>]*>(.*?)</title>", htm, re.I | re.S) or [None, ""])[1])
        toks = [t for t in re.findall(r"\w{4,}", nm.lower()) if t not in ("praktijk", "kliniek", "clinic", "voor")]
        hay=(title + " " + text[:1500]).lower()
        okn = (sum(1 for t in toks if re.search(rf"\b{re.escape(t)}\b", hay)) >= max(1, int(0.6 * len(toks)))) if toks else None
        v = "correct" if okn else ("onbepaald" if okn is None else "fout")
        mark("bedrijfsnaam", v); print(f"  bedrijfsnaam {'✓' if v=='correct' else '✗'} {nm!r} vs title {title[:55]!r}")
        # sector
        kws = COSM_KW if l["sector"] == "cosmetische_behandelaars" else ALT_KW
        hits = [k for k in kws if k in low]
        other = [k for k in (ALT_KW if kws is COSM_KW else COSM_KW) if k in low]
        v = "correct" if hits else ("fout" if other else "onbepaald")
        mark("sector", v); print(f"  sector {'✓' if v=='correct' else '✗'} {l['sector'][:12]} (eigen: {hits[:3]}, ander: {other[:3]})")
        # stad
        city = (l.get("city") or "").lower()
        v = "correct" if city and city in low else ("onbepaald" if not city else "fout")
        mark("stad", v); print(f"  stad {'✓' if v=='correct' else '✗'} {l.get('city')!r}")
        # entiteit (praktijk?)
        bad = [k for k in ("word lid", "winkelwagen", "lesrooster", "cursusaanbod", "onze leden") if k in low]
        v = "fout" if bad else "correct"
        mark("entiteit", v); print(f"  entiteit {'✓' if v=='correct' else '✗ verdacht: '+str(bad)}")
        # conversie-booleans: stored vs vers
        okb, _lbl, _ev = detect_booking(htm)
        for field, stored, fresh in (
                ("booking", cd.get("has_online_booking"), okb),
                ("telefoon", cd.get("has_phone_clickable"), 'href="tel:' in htm.lower() or "href='tel:" in htm.lower()),
                ("whatsapp", cd.get("has_whatsapp"), bool(_WHATSAPP_LINK_RE.search(htm)) or "whatsapp" in low)):
            if stored is None:
                mark(field, "onbepaald"); print(f"  {field} ? stored=None")
            else:
                v = "correct" if bool(stored) == bool(fresh) else "fout"
                mark(field, v); print(f"  {field} {'✓' if v=='correct' else '✗'} stored={stored} vers={fresh}")

    print("\n" + "=" * 88)
    print(f"{'veld':<14}{'correct':>8}{'fout':>6}{'onbep':>7}   score (correct/beoordeeld)")
    for f, t in tallies.items():
        beoordeeld = t["correct"] + t["fout"]
        pct = f"{100 * t['correct'] // beoordeeld}%" if beoordeeld else "—"
        print(f"{f:<14}{t['correct']:>8}{t['fout']:>6}{t['onbepaald']:>7}   {pct}")

asyncio.run(main())
