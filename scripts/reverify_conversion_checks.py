#!/usr/bin/env python3
"""scripts/reverify_conversion_checks.py — pre-send waarheidscheck van conversion_checks.

De frictieclaim mag niet naar buiten als hij niet op BRUIKBARE HTML steunt. Een fetch
telt alleen als meting wanneer de respons (a) een bruikbare statuscode heeft
(200-299 — httpx volgt redirects, dus de eindstatus is de pagina; 4xx/5xx is een
fout-/challenge-pagina) én (b) herkenbare pagina-inhoud oplevert (check_conversion
vond ≥1 positief conversie-element — bewijs dat er echte HTML geparsed is). Voldoet
een respons daar niet aan, dan volgt `reverify_uncertain=True`, GEEN verdict.

Voorheen accepteerde `_get` een 403 zolang `if r.text:` waar was; een gestylede
403-body telde als geslaagd, en twee identieke 403's waren het "eens" → de gate
bevestigde een blokkade als meting (Sami 2026-08-11, meetrapport stap 1). Dat is nu
gedicht: de statuscode wordt gecontroleerd en all-lege resultaten falen.

Herkomst wordt meegeschreven (`conversion_details.reverify_probe`: status, body_size,
content_seen) zodat een volgende herkomstvraag geen verse probe nodig heeft.

DRY-RUN default (READ-ONLY). --apply schrijft naar heatr_website_intelligence (gated).
Gebruik: python3 scripts/reverify_conversion_checks.py [--apply] [--limit N] [--sector S]
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
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
from utils.lead_selection import selection_exclusion
from website_intelligence.conversion_checker import check_conversion

WS = "aerys"
ACTIVE = {"cosmetische_behandelaars", "alternatieve_geneeskunde", "chiropractoren"}
_UA = {"User-Agent": "Mozilla/5.0 (compatible; AerysBot/1.0; +https://aeryssolution.nl)"}
# Documenteerbare challenge-/wall-interstitials (Cloudflare e.d.): een pagina die dit
# als hoofdinhoud draagt is niet de site.
_WALL = ("just a moment", "checking your browser", "enable javascript",
         "attention required", "cf-browser-verification", "cf_chl_opt")


def _richness(cd: dict) -> int:
    """Aantal POSITIEVE content-signalen in een check_conversion-resultaat. 0 = geen
    herkenbare pagina-inhoud (geblokkeerd / JS-gerenderd / echt kaal)."""
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


def _fetch_all(db, table, cols):
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).eq("workspace_id", WS).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def _launchable(l, sector_filter):
    if sector_filter and l.get("sector") != sector_filter:
        return False
    return (l.get("email_status") == "valid" and (l.get("score") or 0) >= 55
            and (l.get("icp_match") or 0) >= 0.50 and (l.get("sector") or "") in ACTIVE
            and not l.get("pushed_to_warmr_at") and not (l.get("contact_attempt_count") or 0))


async def _get(dom: str):
    """Return (status, text). Geeft óók een 4xx/5xx terug (met body), zodat de gate de
    statuscode kan zien i.p.v. 'm als geslaagd te tellen. (None, '') bij netwerkfout."""
    url = dom if dom.startswith("http") else f"https://{dom}"
    for u in (url, url.replace("https://", "http://")):
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers=_UA) as c:
                r = await c.get(u)
                return r.status_code, (r.text or "")
        except Exception:
            continue
    return None, ""


async def usable_measurement(dom: str, status, text: str, sector: str):
    """Bepaal of een respons een bruikbare meting is. Returned (usable, result, richness,
    reden). result = check_conversion-output (of None bij status/leeg/wall)."""
    if status is None:
        return False, None, 0, "fetch_error"
    if not (200 <= status <= 299):
        return False, None, 0, f"http_{status}"
    if not text or not text.strip():
        return False, None, 0, "empty_body"
    if any(w in text.lower()[:4000] for w in _WALL):
        return False, None, 0, "challenge_wall"
    result = await check_conversion(dom, text, sector)
    rich = _richness(result)
    if rich == 0:
        return False, result, 0, "no_content_signals"   # JS-gerenderd/kaal → geen geldige meting
    return True, result, rich, "ok"


def classify(f1, f2):
    """(verdict, result, reden) uit twee usable_measurement-tuples. Onbruikbare fetch →
    uncertain; twee bruikbare die het oneens zijn → uncertain."""
    if not f1[0] or not f2[0]:
        return "uncertain", None, (f1 if not f1[0] else f2)[3]
    b1 = f1[1].get("has_online_booking") is False
    b2 = f2[1].get("has_online_booking") is False
    if b1 and b2:
        return "confirmed_no_booking", f1[1], "ok"
    if (not b1) and (not b2):
        return "now_booking", f1[1], "ok"
    return "uncertain", None, "fetch_disagreement"


async def _double_check(sem, l):
    dom = (l.get("domain") or "").strip()
    async with sem:
        s1, t1 = await _get(dom)
        s2, t2 = await _get(dom)
    f1 = await usable_measurement(dom, s1, t1, l["sector"])
    f2 = await usable_measurement(dom, s2, t2, l["sector"])
    verdict, result, reason = classify(f1, f2)
    probe = {"status": s1, "body_size": len(t1 or ""), "content_seen": bool(f1[0]), "reason": reason}
    return l, verdict, result, probe


async def run(apply: bool, limit: int, sector_filter, min_richness: int = 0):
    db = get_heatr_supabase()
    leads = {l["id"]: l for l in _fetch_all(db, "leads",
             "id,company_name,domain,sector,email_status,score,icp_match,pushed_to_warmr_at,"
             "contact_attempt_count,kvk_legal_form,email_discovery_source,contact_first_name,"
             "contact_why_chosen,email")}
    wi = {w["lead_id"]: (w.get("conversion_details") or {})
          for w in _fetch_all(db, "website_intelligence", "lead_id,conversion_details")}

    targets = []
    for lid, l in leads.items():
        if not _launchable(l, sector_filter) or not display_first_name(l, fallback=""):
            continue
        n, nr = clean_company_name(l.get("company_name"))
        if not n or nr:
            continue
        ok, _r = receptie_avg_safe(l)
        if not ok:
            continue
        cd = wi.get(lid, {})
        # geen frictie-kandidaat, selectie-uitgesloten, of onder de rijkheidsdrempel
        # (blinde/geblokkeerde leads met stored-rijkheid 0 wachten op de Playwright-
        # fallback — die reverifieert httpx niet nog eens).
        if select_leak(cd) is None or selection_exclusion(l) or _richness(cd) < min_richness:
            continue
        targets.append(l)
    targets = targets[:limit] if limit else targets
    print(f"Herverificatie van {len(targets)} kandidaten "
          f"({'APPLY — prod-write' if apply else 'DRY-RUN — read-only'}).\n")

    sem = asyncio.Semaphore(8)
    results = await asyncio.gather(*[_double_check(sem, l) for l in targets])
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()

    confirmed_leads, now_b, uncertain_leads, changes, applied = [], [], [], [], 0
    for l, verdict, result, probe in results:
        lid = l["id"]; stored = wi.get(lid, {})
        was_conf = (stored.get("reverify_uncertain") is False and stored.get("checked_at")
                    and stored.get("has_online_booking") is False)
        if verdict == "confirmed_no_booking":
            confirmed_leads.append(l)
            if stored.get("reverify_uncertain"):
                changes.append((l, "was onzeker → bevestigd"))
        elif verdict == "now_booking":
            now_b.append(l)
            changes.append((l, "NU boeking (frictieclaim vervallen)"))
            print(f"  NU-BOEKING   {l['company_name'][:40]}")
        else:
            uncertain_leads.append((l, probe["reason"]))
            print(f"  ONZEKER      {l['company_name'][:40]:<40} ({probe['reason']})")
            if was_conf:
                changes.append((l, f"was bevestigd → onzeker ({probe['reason']})"))
        if apply:
            if verdict in ("confirmed_no_booking", "now_booking") and result is not None:
                update = {**result, "checked_at": now_iso, "reverify_uncertain": False,
                          "reverify_probe": {**probe, "checked_at": now_iso}}
            else:
                update = {**dict(stored), "checked_at": now_iso,
                          "reverify_uncertain": True, "reverify_probe": {**probe, "checked_at": now_iso}}
            db.table("website_intelligence").update({"conversion_details": update}) \
              .eq("lead_id", lid).eq("workspace_id", WS).execute()
            applied += 1

    print(f"\n=== VERANDERD sinds de vorige meting ({len(changes)}) ===")
    for l, why in changes:
        print(f"  {l['company_name'][:38]:<38} {why}")
    print(f"\n=== GEPROJECTEERDE COHORT (bevestigd = Frame A na --apply): {len(confirmed_leads)} ===")
    for l in confirmed_leads:
        cd = wi.get(l["id"], {})
        print(f"  {l['company_name'][:36]:<36} [{niche_for_sector(l['sector'])[:4]}] "
              f"stored_rijkheid={_richness(cd)}")

    print(f"\nSAMENVATTING — bevestigd: {len(confirmed_leads)} · nu boeking: {len(now_b)} · "
          f"onzeker: {len(uncertain_leads)}")
    if apply:
        print(f"  → {applied} rijen bijgewerkt.")
    else:
        print("  → DRY-RUN: niets geschreven; checked_at wordt pas op --apply gezet.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--sector", type=str, default="alternatieve_geneeskunde", help="'' = alle")
    ap.add_argument("--min-richness", type=int, default=0,
                    help="skip leads met stored-rijkheid < N (blinde leads → Playwright)")
    a = ap.parse_args()
    asyncio.run(run(a.apply, a.limit, a.sector or None, a.min_richness))


if __name__ == "__main__":
    main()
