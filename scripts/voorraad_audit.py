"""scripts/voorraad_audit.py — A2: de leads-voorraad als dataset auditen. READ-ONLY.

Zes secties (audit-programma 2026-08): velden-dekking · score/icp-sanity · versheid ·
duplicaat-rest · sector-consistentie · cohort-vergrootglas. Her-runbaar na elke
sweep-tranche; output is het rapport.

    python3 scripts/voorraad_audit.py            # volledige audit
    python3 scripts/voorraad_audit.py --no-live  # zonder live site-checks op het cohort
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WORKSPACE = "aerys"


def _fetch_all(db, table: str, cols: str) -> list[dict]:
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def _sendable(l: dict) -> bool:
    es, vm = l.get("email_status"), l.get("email_verification_method")
    if not l.get("email"):
        return False
    if es == "valid":
        return True
    return es in ("risky", "catchall_risky") and vm in ("smtp", "bouncer_api")


def _norm(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _pct(n: int, d: int) -> str:
    return f"{round(n / d * 100)}%" if d else "—"


async def main(live_checks: bool) -> int:
    from config.database import get_heatr_supabase
    from config.sectors import ACTIVE_SECTORS

    db = get_heatr_supabase()
    leads = [l for l in _fetch_all(
        db, "leads",
        "id, company_name, domain, city, sector, status, score, icp_match, email, email_status, "
        "email_verification_method, contact_first_name, phone, google_rating, google_review_count, "
        "gdpr_safe, kvk_legal_form, created_at, updated_at, workspace_id")
        if l.get("workspace_id") == WORKSPACE]
    wi = _fetch_all(db, "website_intelligence", "lead_id, receptie_hook_code, total_score, workspace_id")
    hooks = {w["lead_id"]: w.get("receptie_hook_code") for w in wi if w.get("workspace_id") == WORKSPACE}
    wi_score = {w["lead_id"]: w.get("total_score") for w in wi if w.get("workspace_id") == WORKSPACE}
    n = len(leads)
    now = datetime.now(timezone.utc)
    active = set(ACTIVE_SECTORS)

    print(f"VOORRAAD-AUDIT — {now:%Y-%m-%d %H:%M} · {n} leads (workspace {WORKSPACE})")

    # ── 1. Velden-dekking per sector ────────────────────────────────────────
    print("\n[1] VELDEN-DEKKING (per sector: naam · telefoon · rating · domain · site-score · sendable)")
    for sector, group in sorted(({s: [l for l in leads if l.get("sector") == s]
                                  for s in {x.get("sector") for x in leads}}).items(),
                                key=lambda kv: str(kv[0])):
        if not group:
            continue
        g = len(group)
        print(f"  {str(sector):<28} n={g:<5} naam {_pct(sum(1 for l in group if l.get('contact_first_name')), g):>4}"
              f" · tel {_pct(sum(1 for l in group if l.get('phone')), g):>4}"
              f" · rating {_pct(sum(1 for l in group if l.get('google_rating')), g):>4}"
              f" · domain {_pct(sum(1 for l in group if l.get('domain')), g):>4}"
              f" · site-score {_pct(sum(1 for l in group if wi_score.get(l['id'])), g):>4}"
              f" · sendable {_pct(sum(1 for l in group if _sendable(l)), g):>4}")

    # ── 2. Score/icp-sanity ─────────────────────────────────────────────────
    print("\n[2] SCORE/ICP-SANITY")
    buckets = Counter(("0" if not (l.get("score") or 0) else
                       "1-29" if l["score"] < 30 else "30-54" if l["score"] < 55 else
                       "55-69" if l["score"] < 70 else "70+") for l in leads)
    print(f"  score-verdeling: {dict(sorted(buckets.items()))}")
    gate = [l for l in leads if (l.get("score") or 0) >= 55 and (l.get("icp_match") or 0) >= 0.50]
    print(f"  door gate 55/0.50: {len(gate)} · waarvan sendable {sum(1 for l in gate if _sendable(l))}"
          f" · waarvan gdpr_safe+sendable {sum(1 for l in gate if _sendable(l) and l.get('gdpr_safe'))}")
    weird = [l for l in leads if (l.get("icp_match") or 0) >= 0.9 and (l.get("score") or 0) < 40]
    print(f"  outlier icp≥0.9 maar score<40 (reachability sleept 'm omlaag): {len(weird)}")
    no_email_gate = sum(1 for l in gate if not l.get("email"))
    print(f"  door gate maar GEEN e-mail: {no_email_gate}")

    # ── 3. Versheid ─────────────────────────────────────────────────────────
    print("\n[3] VERSHEID (updated_at)")
    def age_bucket(l):
        try:
            d = (now - datetime.fromisoformat(str(l.get("updated_at")).replace("Z", "+00:00"))).days
        except Exception:
            return "onbekend"
        return "<30d" if d < 30 else "30-90d" if d <= 90 else ">90d STALE"
    print(f"  {dict(Counter(age_bucket(l) for l in leads))}")

    # ── 4. Duplicaat-rest (fuzzy — de unique-index vangt alleen exacte e-mail/domein) ──
    print("\n[4] DUPLICAAT-REST (fuzzy)")
    by_dom = defaultdict(list)
    for l in leads:
        # Domeinen NIET fuzzy-normaliseren (alleen lowercase + www-strip): koppeltekens
        # zijn betekenisvol — homeopathie-amsterdam.nl ≠ homeopathieamsterdam.nl
        # (vals alarm gevonden 2026-08-03: drie verschillende praktijken).
        d = (l.get("domain") or "").lower().removeprefix("www.")
        if d:
            by_dom[d].append(l)
    dom_dups = {k: v for k, v in by_dom.items() if len(v) > 1}
    by_name = defaultdict(list)
    for l in leads:
        key = (_norm(l.get("company_name")), (l.get("city") or "").lower())
        if key[0]:
            by_name[key].append(l)
    name_dups = {k: v for k, v in by_name.items() if len(v) > 1}
    print(f"  zelfde domein, meerdere leads: {len(dom_dups)} groepen")
    for k, v in list(sorted(dom_dups.items(), key=lambda x: -len(x[1])))[:5]:
        print(f"    {k}: {len(v)}× — {[x.get('city') for x in v]}")
    print(f"  zelfde naam+stad: {len(name_dups)} groepen")

    # ── 5. Sector-consistentie ──────────────────────────────────────────────
    print("\n[5] SECTOR-CONSISTENTIE")
    per_sector = Counter(l.get("sector") for l in leads)
    print(f"  actieve sectoren (config): {sorted(active)}")
    for s, c in per_sector.most_common():
        mark = "" if s in active else "  ← NIET ACTIEF (wordt niet benaderd, telt wel mee in dashboards)"
        print(f"    {str(s):<30} {c}{mark}")

    # ── 6. Cohort-vergrootglas (receptie-haak-leads) ────────────────────────
    print("\n[6] COHORT-VERGROOTGLAS (leads met receptie-haak)")
    cohort = [l for l in leads if hooks.get(l["id"])]
    print(f"  cohort-omvang: {len(cohort)}")
    issues = 0
    for l in cohort:
        probs = []
        if not l.get("contact_first_name"):
            probs.append("geen naam")
        if not _sendable(l):
            probs.append(f"niet sendable ({l.get('email_status')})")
        if not l.get("kvk_legal_form"):
            probs.append("geen rechtsvorm (AVG-02)")
        if probs:
            issues += 1
            print(f"    {l['id'][:8]} {(l.get('company_name') or '?')[:30]:<32} [{hooks[l['id']]}] → {', '.join(probs)}")
    print(f"  cohort-leads met ≥1 issue: {issues}/{len(cohort)}")

    if live_checks and cohort:
        print("\n[6b] SITE-LIVE-STEEKPROEF (cohort, HEAD-request)")
        import httpx
        dead = 0
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as c:
            for l in cohort:
                if not l.get("domain"):
                    continue
                try:
                    r = await c.head(f"https://{l['domain']}")
                    if r.status_code >= 400:
                        r = await c.get(f"https://{l['domain']}")
                    if r.status_code >= 400:
                        dead += 1
                        print(f"    🔴 {l['domain']} → HTTP {r.status_code}")
                except Exception as e:
                    dead += 1
                    print(f"    🔴 {l['domain']} → {type(e).__name__}")
        print(f"  dode/onbereikbare cohort-sites: {dead}/{len(cohort)}")

    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-live", action="store_true")
    args = ap.parse_args()
    raise SystemExit(asyncio.run(main(not args.no_live)))
