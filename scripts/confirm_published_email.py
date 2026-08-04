"""scripts/confirm_published_email.py — bevestig dat een e-mailadres écht op de
eigen site staat, zodat de art. 11.7 lid 3-grond GEVERIFIEERD is (niet geïnfereerd).

Achtergrond: veel launchbare leads zijn AVG-geblokkeerd omdat email_discovery_source
niet als 'website' is vastgelegd (adres ooit door de patroon-generator gegokt, of
verrijkt vóór de herkomst-kolom bestond). Terwijl het adres (info@eigen-domein)
vrijwel zeker gewoon op hun site staat. Dit script haalt de site op, checkt of het
adres er LETTERLIJK op staat, en legt bij bevestiging email_discovery_source='website'
vast — dan is de mail-grond verdedigbaar met bewijs (welke URL, welk adres).

Fail-closed: alleen een LETTERLIJKE match op de eigen site zet de vlag. Geen match,
fetch-fout of adres op een ander domein → niets geschreven. Workspace-beperkt (aerys),
idempotent. Schrijft een bewijsbestand (lead_id → url → email) voor Spoor J.

    python3 scripts/confirm_published_email.py                 # DRY-RUN, alle kandidaten
    python3 scripts/confirm_published_email.py --limit 15      # kleine steekproef
    python3 scripts/confirm_published_email.py --apply         # schrijf bevestigde herkomst
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
load_dotenv(str(Path(__file__).resolve().parents[1] / ".env"))

WORKSPACE = "aerys"
_CALL_DELAY_S = float(os.getenv("CONFIRM_PUB_DELAY_S", "1.0"))
# Homepage eerst; dan een paar veelvoorkomende contactpagina's. Klein gehouden
# (max ~4 fetches/lead) — een role-adres staat meestal sitebreed in de footer.
_CONTACT_PATHS = ("", "contact", "contact-opnemen", "over-ons")


def _norm_domain(s: str | None) -> str:
    s = (s or "").strip().lower()
    if not s:
        return ""
    if "://" not in s:
        s = "http://" + s
    host = urlparse(s).netloc or ""
    return host[4:] if host.startswith("www.") else host


def _email_domain(email: str | None) -> str:
    e = (email or "").strip().lower()
    return e.split("@", 1)[1] if "@" in e else ""


def _fetch_all(db, table: str, cols: str) -> list[dict]:
    out, off = [], 0
    while True:
        d = (db.table(table).select(cols).eq("workspace_id", WORKSPACE)
             .range(off, off + 999).execute().data or [])
        out += d
        if len(d) < 1000:
            return out
        off += 1000


async def _confirm_one(lead: dict) -> dict:
    """Fetch de eigen site en check of het adres er letterlijk op staat.
    Returns {status: confirmed|not_found|fetch_error, url, email}."""
    from scrapers.website_scraper import fetch_page_httpx, extract_emails_from_html

    email = (lead.get("email") or "").strip().lower()
    dom = _norm_domain(lead.get("domain"))
    if not email or not dom:
        return {"status": "skip", "url": None, "email": email}

    any_fetched = False
    for path in _CONTACT_PATHS:
        url = f"https://{dom}/{path}".rstrip("/")
        html, _ = await fetch_page_httpx(url)
        if not html:
            continue
        any_fetched = True
        found = {e.strip().lower() for e in extract_emails_from_html(html)}
        if email in found:
            return {"status": "confirmed", "url": url, "email": email}
    return {"status": ("not_found" if any_fetched else "fetch_error"),
            "url": None, "email": email}


async def run(apply: bool, limit: int) -> int:
    from config.database import get_heatr_supabase
    from utils.legal_form import receptie_avg_safe

    db = get_heatr_supabase()
    rows = _fetch_all(
        db, "leads",
        "id, company_name, city, email, domain, email_discovery_source, kvk_legal_form, "
        "email_status, score, icp_match, sector, personalized_opener, "
        "pushed_to_warmr_at, contact_attempt_count")

    ACTIVE = {"cosmetische_behandelaars", "chiropractoren", "alternatieve_geneeskunde"}
    # Kandidaten: launchbaar-profiel, AVG nu geblokkeerd, e-mail op het eigen domein
    # (alleen dan is 'op de eigen site' überhaupt mogelijk). Niet al benaderd.
    cand = []
    for r in rows:
        if r.get("email_status") != "valid":
            continue
        if (r.get("score") or 0) < 55 or (r.get("icp_match") or 0) < 0.50:
            continue
        if (r.get("sector") or "") not in ACTIVE or not r.get("personalized_opener"):
            continue
        if r.get("pushed_to_warmr_at") or (r.get("contact_attempt_count") or 0):
            continue
        if receptie_avg_safe(r)[0]:
            continue                                   # al een grond → niet nodig
        if _email_domain(r.get("email")) != _norm_domain(r.get("domain")):
            continue                                   # adres op ander domein → nooit 'eigen site'
        cand.append(r)

    if limit:
        cand = cand[:limit]

    mode = "APPLY (schrijft herkomst)" if apply else "DRY-RUN (schrijft niets)"
    print(f"Kandidaten (AVG-geblokkeerd, role@eigen-domein, launchbaar): {len(cand)} — {mode}\n")

    st = Counter()
    evidence = []
    for i, r in enumerate(cand, 1):
        res = await _confirm_one(r)
        st[res["status"]] += 1
        if res["status"] == "confirmed":
            evidence.append({"lead_id": r["id"], "company": r.get("company_name"),
                             "city": r.get("city"), "email": res["email"], "url": res["url"]})
            if apply:
                try:
                    db.table("leads").update({"email_discovery_source": "website"}).eq(
                        "id", r["id"]).eq("workspace_id", WORKSPACE).execute()
                except Exception as e:
                    print(f"  ⚠ schrijven faalde voor {r.get('company_name')}: {str(e)[:80]}")
        if i % 20 == 0 or i == len(cand):
            print(f"  {i}/{len(cand)} verwerkt… {dict(st)}")
        await asyncio.sleep(_CALL_DELAY_S)

    print(f"\n── UITKOMST (n={len(cand)}) ──")
    for k in ("confirmed", "not_found", "fetch_error", "skip"):
        if st.get(k):
            print(f"  {k}: {st[k]}")
    print(f"→ {st.get('confirmed', 0)} leads met GEVERIFIEERDE publicatie-grond"
          + (" (herkomst weggeschreven)" if apply else " (dry-run — nog niet geschreven)"))

    if evidence:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = Path(__file__).resolve().parents[1] / f"docs/avg_publication_evidence_{stamp}.json"
        out.write_text(json.dumps({"generated_at": stamp, "count": len(evidence),
                                   "evidence": evidence}, ensure_ascii=False, indent=2))
        print(f"\nBewijsbestand (voor Spoor J): {out}")
    if not apply:
        print("\n(DRY-RUN — draai met --apply om de bevestigde herkomst te persisteren.)")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    raise SystemExit(asyncio.run(run(apply=args.apply, limit=args.limit)))
