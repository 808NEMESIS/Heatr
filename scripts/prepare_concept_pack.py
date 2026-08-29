#!/usr/bin/env python3
"""scripts/prepare_concept_pack.py — werkmap voor het Loom-concept na een "ja".

De mail belooft: 'Antwoord met "ja" en hij staat er binnen twee werkdagen.' Deze
generator maakt die belofte waar te maken: bij een ja-reply verzamelt hij per lead
alles wat Sami nodig heeft om direct te kunnen ontwerpen — in plaats van eerst een
uur te sprokkelen. Output: concepts/<slug>/ met brief.md, text.md en images/.

Verzamelt (via de canonieke meetlaag, httpx→Playwright):
  - brief.md   — praktijk/eigenaar(+bron), stad, sector, rating/reviews, why-you-
                 citaat, diensten-koppen, kleuren (uit CSS), tel/adres-snippets,
                 de gedane mailbelofte + foto-toestemmingsstatus, benchmark-context.
  - text.md    — gestripte tekst van home + over/behandel-subpagina's (bronmateriaal
                 voor de concept-teksten).
  - images/    — foto's van de site (>15KB, geen icons/svg), max 20. LET OP: gebruik
                 pas in het concept ná de foto-toestemming uit de ja-reply.

READ-ONLY t.o.v. prod. concepts/ is gitignored (prospect-materiaal).

  python3 scripts/prepare_concept_pack.py "Osteopathie MCN"
"""
from __future__ import annotations

import asyncio
import html as _h
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv  # noqa: E402
load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

import httpx

WS = "aerys"
_SUB = re.compile(r'href=["\']([^"\']*(?:over|team|about|behandel|dienst|tarie|specialis)[^"\']*)["\']', re.I)
_IMG = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.I)
_HEX = re.compile(r"#([0-9a-fA-F]{6})\b")


def strip(t):
    t = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", t, flags=re.I | re.S)
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", _h.unescape(t)).strip()


def _abs(url, dom):
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return f"https://{dom.rstrip('/')}" + url
    if not url.startswith("http"):
        return f"https://{dom.rstrip('/')}/" + url
    return url


async def main(query: str):
    from config.database import get_heatr_supabase
    from website_intelligence.measurement import fetch_httpx, usable_measurement
    from website_intelligence.rendered_fetch import RenderedFetcher
    from utils.lead_naming import clean_company_name, display_first_name
    from campaigns.frictie_copywriter import display_company_name

    db = get_heatr_supabase()
    r = (db.table("leads").select("*").eq("workspace_id", WS).eq("company_name", query).limit(1).execute().data
         or db.table("leads").select("*").eq("workspace_id", WS).ilike("company_name", f"%{query}%").limit(1).execute().data)
    if not r:
        print(f"lead niet gevonden: {query}"); return 1
    l = r[0]
    dom = (l.get("domain") or "").strip()
    wi_rows = db.table("website_intelligence").select("personalization,conversion_details") \
                .eq("lead_id", l["id"]).eq("workspace_id", WS).execute().data
    pers = (wi_rows[0].get("personalization") if wi_rows else {}) or {}
    cd = (wi_rows[0].get("conversion_details") if wi_rows else {}) or {}

    nm = display_company_name(clean_company_name(l.get("company_name"))[0])
    slug = re.sub(r"[^a-z0-9]+", "-", nm.lower()).strip("-")
    out = Path("concepts") / slug
    (out / "images").mkdir(parents=True, exist_ok=True)

    # ── site ophalen (meetlaag + render-fallback) ────────────────────────────
    st, htm = await fetch_httpx(dom)
    usable, _res, _n, reason = await usable_measurement(dom, st, htm, l["sector"])
    if not usable:
        async with RenderedFetcher() as rf:
            st, htm = await rf.fetch(dom)
    pages = {"home": htm}
    for href in list(dict.fromkeys(_SUB.findall(htm)))[:4]:
        url = _abs(href, dom)
        if dom.replace("www.", "").split("/")[0] not in url:
            continue
        s2, h2 = await fetch_httpx(url)
        if s2 and 200 <= s2 <= 299 and h2:
            pages[href[:40]] = h2

    all_html = " ".join(pages.values())
    all_text = " ".join(strip(h) for h in pages.values())

    # ── kleuren (meest gebruikte hexen uit inline css) ───────────────────────
    colors = [f"#{c.lower()}" for c, _n in Counter(m.lower() for m in _HEX.findall(all_html)).most_common(8)]
    # ── koppen (h1/h2) als usp/diensten-materiaal ────────────────────────────
    heads = [strip(x) for x in re.findall(r"<h[12][^>]*>(.*?)</h[12]>", all_html, re.I | re.S)]
    heads = [h for h in dict.fromkeys(heads) if 3 < len(h) < 90][:12]
    # ── afbeeldingen downloaden ──────────────────────────────────────────────
    imgs = list(dict.fromkeys(_IMG.findall(all_html)))
    saved = 0
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0"}) as c:
        for src in imgs:
            if saved >= 20:
                break
            if any(x in src.lower() for x in (".svg", "icon", "logo-klein", "favicon", "sprite")):
                continue
            try:
                resp = await c.get(_abs(src, dom))
                if resp.status_code == 200 and len(resp.content) > 15000:
                    ext = (re.search(r"\.(jpe?g|png|webp|avif)", src.lower()) or [None, "jpg"])[1]
                    (out / "images" / f"img{saved:02d}.{ext}").write_bytes(resp.content)
                    saved += 1
            except Exception:
                continue

    # ── brief.md ─────────────────────────────────────────────────────────────
    first = display_first_name(l, fallback="?")
    brief = [f"# Concept-brief — {nm}", "",
             f"- **Site:** https://{dom}",
             f"- **Contact:** {first} ({(l.get('contact_why_chosen') or 'bron onbekend')[:90]})",
             f"- **Stad/sector:** {l.get('city')} · {l.get('sector')} · {l.get('archetype') or '—'}",
             f"- **Reviews:** {l.get('google_rating')} ★ ({l.get('google_review_count')} reviews)",
             f"- **Why-you (uit de mail):** {pers.get('why_you') or 'rating/stad-fallback'}",
             f"- **Site-citaat (audit):** «{(pers.get('why_you_quote') or '—')[:140]}»",
             f"- **Online boeking nu:** {cd.get('has_online_booking')} ({cd.get('booking_platform') or '—'}) · tel-klikbaar: {cd.get('has_phone_clickable')} · wa: {cd.get('has_whatsapp_link')}",
             "",
             "## De gedane belofte (uit mail 1)",
             "- Loom-concept van de nieuwe homepage **binnen 2 werkdagen na de ja**",
             "- Concept = de richting; echte site pas na akkoord (3 weken tot live)",
             "- **Foto-toestemming**: gevraagd in de mail — check de ja-reply vóór je hun beeld gebruikt",
             "",
             "## Kleuren (meest gebruikt op de huidige site)",
             "  " + " · ".join(colors or ["(geen hex gevonden)"]),
             "",
             "## Koppen/USP-materiaal van de site"] + [f"- {h}" for h in heads] + [
             "",
             f"## Assets", f"- {saved} foto's in images/ · volledige sitetekst in text.md"]
    (out / "brief.md").write_text("\n".join(brief))
    (out / "text.md").write_text(f"# Sitetekst {nm}\n\n" + all_text[:20000])
    print(f"✓ concept-pack: {out}/  (brief.md · text.md · {saved} foto's · {len(pages)} pagina's · kleuren {len(colors)})")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("gebruik: prepare_concept_pack.py <lead-naam-of-deel>"); raise SystemExit(1)
    raise SystemExit(asyncio.run(main(sys.argv[1])))
