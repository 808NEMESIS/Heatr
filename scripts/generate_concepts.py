#!/usr/bin/env python3
"""scripts/generate_concepts.py — render de concept-mails (mail 1) voor review.

READ-ONLY t.o.v. de DB (verstuurt niets, schrijft niets naar prod; kill-switch
onaangeroerd). Rendert de eligible Frame-A-cohort (voornaam + schone naam + AVG +
geen-boeking) over beide niches via de frictie-engine, met de geautomatiseerde
zelfcontrole. Cosmetisch wordt read-only dubbel-gereverifieerd (waarheidscheck)
zodat de frictieclaim klopt; alt is al gereverifieerd (checked_at). Output = een
zelfstandig lokaal HTML-reviewbestand + samenvatting. Geen externe upload (AVG).

Gebruik: python3 scripts/generate_concepts.py [max=50] [out.html]
"""
from __future__ import annotations

import asyncio
import datetime as dt
import html
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import httpx

from config.database import get_heatr_supabase
from campaigns.frictie_copywriter import niche_for_sector, render_frictie_mail, select_leak
from utils.legal_form import receptie_avg_safe
from utils.lead_naming import clean_company_name, display_first_name
from website_intelligence.conversion_checker import check_conversion

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
PRIVACY = ("Je ontvangt deze mail omdat je praktijk openbaar vindbaar is; "
           "hoe wij met gegevens omgaan lees je op aeryssolution.nl/privacy")
_UA = {"User-Agent": "Mozilla/5.0 (compatible; AerysBot/1.0; +https://aeryssolution.nl)"}
SCRATCH = "/private/tmp/claude-501/-Users-nemesis-Heatr/2bb63301-a1e9-411c-a043-938e885ebdc3/scratchpad"


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


async def _get(dom: str) -> str:
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


async def _confirm_no_booking(sem, l):
    """Dubbele fetch + overeenstemming (read-only). True = 2x geen-boeking."""
    dom = (l.get("domain") or "").strip()
    async with sem:
        h1, h2 = await _get(dom), await _get(dom)
    if not h1 or not h2:
        return l, "uncertain"
    f1 = await check_conversion(dom, h1, l["sector"])
    f2 = await check_conversion(dom, h2, l["sector"])
    b1 = f1.get("has_online_booking") is False
    b2 = f2.get("has_online_booking") is False
    if b1 and b2:
        return l, "confirmed"
    if (not b1) and (not b2):
        return l, "now_booking"
    return l, "uncertain"


def _esc(s):
    return html.escape(str(s or ""))


def _card(i, l, mail):
    sc = mail["selfcheck"]
    badge = "PASS" if sc["passed"] else "FAIL"
    cls = "pass" if sc["passed"] else "fail"
    body = _esc(mail["body"]).replace("\n", "<br>")
    detail = "" if sc["passed"] else f'<div class="detail">{_esc(sc["detail"])}</div>'
    return f"""<article class="card">
  <header><span class="n">{i}</span> <b>{_esc(l.get('company_name'))}</b>
    <span class="meta">{_esc(l.get('sector'))} · {_esc(l.get('city') or '?')} · {_esc(l.get('archetype') or '—')}</span></header>
  <div class="tags">frame {mail['frame']} · {sc['words']} woorden · {sc['iks']}× ik ·
    <span class="badge {cls}">{badge}</span></div>{detail}
  <div class="subj">Onderwerp: {_esc(mail['subject'])}</div>
  <div class="body">{body}</div>
</article>"""


async def main():
    maxn = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    out_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(SCRATCH, "concepten_50.html")
    today = dt.date.today()
    db = get_heatr_supabase()
    leads = {l["id"]: l for l in _fetch_all(db, "leads",
             "id,company_name,domain,email,sector,city,contact_first_name,archetype,"
             "contact_why_chosen,email_status,score,icp_match,pushed_to_warmr_at,"
             "contact_attempt_count,kvk_legal_form,email_discovery_source")}
    wi = {w["lead_id"]: w for w in _fetch_all(db, "website_intelligence",
                                              "lead_id,conversion_details,analyzed_at")}

    alt, cosm = [], []
    for l in leads.values():
        if not _launchable(l) or not display_first_name(l, fallback=""):
            continue
        n, nr = clean_company_name(l.get("company_name"))
        if not n or nr:
            continue
        ok, _r = receptie_avg_safe(l)
        if not ok:
            continue
        w = wi.get(l["id"], {})
        cd = w.get("conversion_details") or {}
        if select_leak(cd) is None:
            continue
        (alt if niche_for_sector(l["sector"]) == "alt" else cosm).append((l, cd, w.get("analyzed_at")))

    # Cosmetisch read-only reverifiëren (waarheidscheck) — alt is al gereverifieerd.
    sem = asyncio.Semaphore(8)
    checks = await asyncio.gather(*[_confirm_no_booking(sem, l) for l, _cd, _a in cosm])
    status = {l["id"]: s for l, s in checks}
    cosm_keep, cosm_drop = [], []
    for l, cd, a in cosm:
        if status.get(l["id"]) == "confirmed":
            cd2 = {**cd, "checked_at": today.isoformat()}       # in-memory: verse claim
            cosm_keep.append((l, cd2, a))
        else:
            cosm_drop.append((l, status.get(l["id"])))

    picks = (alt + cosm_keep)[:maxn]
    cards, frame_t, niche_t, arch_t, words, npass = [], {}, {}, {}, [], 0
    for i, (l, cd, a) in enumerate(picks, 1):
        mail = render_frictie_mail(l, sector=l["sector"], conversion_details=cd,
                                   privacy_notice=PRIVACY, unsubscribe="",
                                   warmr_owns_unsubscribe=True, analyzed_at=a, now=today)
        if mail is None:
            continue
        cards.append(_card(i, l, mail))
        sc = mail["selfcheck"]
        frame_t[mail["frame"]] = frame_t.get(mail["frame"], 0) + 1
        niche_t[mail["niche"]] = niche_t.get(mail["niche"], 0) + 1
        arch_t[l.get("archetype") or "—"] = arch_t.get(l.get("archetype") or "—", 0) + 1
        words.append(sc["words"])
        npass += 1 if sc["passed"] else 0

    summary = (f"{len(cards)} concepten · {npass}/{len(cards)} door zelfcontrole · "
               f"frames {frame_t} · niches {niche_t} · "
               f"woorden {min(words)}–{max(words)} (limiet 160)")
    page = f"""<!doctype html><html lang=nl><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Concepten — eerste {len(cards)} (nieuwe methode)</title>
<style>
:root{{--bg:#faf9f7;--fg:#1c1a17;--mut:#6b6660;--line:#e6e2db;--acc:#6d5bd0;--ok:#177245;--bad:#b0241a}}
*{{box-sizing:border-box}} body{{margin:0;font:15px/1.55 -apple-system,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--fg)}}
.wrap{{max-width:820px;margin:0 auto;padding:32px 20px 80px}}
h1{{font-size:22px;margin:0 0 4px}} .sub{{color:var(--mut);margin:0 0 24px}}
.card{{background:#fff;border:1px solid var(--line);border-radius:12px;padding:18px 20px;margin:16px 0;box-shadow:0 1px 2px rgba(0,0,0,.03)}}
.card header{{display:flex;gap:8px;align-items:baseline;flex-wrap:wrap}}
.n{{color:var(--acc);font-weight:700}} .meta{{color:var(--mut);font-size:13px}}
.tags{{font-size:13px;color:var(--mut);margin:6px 0}}
.badge{{padding:1px 7px;border-radius:6px;font-weight:700;font-size:12px}}
.badge.pass{{background:#e7f3ec;color:var(--ok)}} .badge.fail{{background:#fbe9e7;color:var(--bad)}}
.detail{{color:var(--bad);font-size:12px;margin:4px 0}}
.subj{{font-weight:600;margin:10px 0 8px}}
.body{{background:#faf9f7;border:1px solid var(--line);border-radius:8px;padding:14px 16px;font-size:14px}}
.note{{background:#fff6e5;border:1px solid #f0dfae;border-radius:8px;padding:10px 14px;font-size:13px;color:#7a5a12;margin:10px 0}}
</style></head><body><div class=wrap>
<h1>Concepten — eerste {len(cards)} (nieuwe methode)</h1>
<p class=sub>{_esc(summary)}<br>Archetype: {_esc(arch_t)}</p>
<div class=note>Read-only render · niets verstuurd · kill-switch dicht. Cosmetisch is voor dit
overzicht read-only dubbel-gereverifieerd; {len(cosm_drop)} cosmetische lead(s) uitgesloten
(boeking/onzeker): {_esc([f"{l.get('company_name')} ({s})" for l, s in cosm_drop]) if cosm_drop else 'geen'}.</div>
{''.join(cards)}
</div></body></html>"""

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        f.write(page)
    print(f"alt {len(alt)} + cosmetisch bevestigd {len(cosm_keep)} (uitgesloten {len(cosm_drop)}) "
          f"→ {len(cards)} concepten gerenderd.")
    print(summary)
    print(f"archetype: {arch_t}")
    if cosm_drop:
        print(f"cosmetisch uitgesloten: {[(l.get('company_name'), s) for l, s in cosm_drop]}")
    print(f"\nHTML: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())
