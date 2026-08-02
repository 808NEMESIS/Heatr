"""scripts/backfill_legal_form_kvk.py — kvk_legal_form vullen via de KvK-API (AVG-02, F1-route-B).

De rechtsvorm-gate (utils/legal_form) blokkeert alles zolang kvk_legal_form leeg is
(0/986 bij de gebruiksklaar-audit 2026-08-02). Dit script vult de kolom via de
betaalde KvK-API (±€0,02/call; 2 calls per lead: zoeken + basisprofiel).

Hergebruikt de bestaande bouwstenen: search_kvk → match_best_kvk_result →
get_kvk_detail (scrapers/kvk_scraper) en classify_legal_form (utils/legal_form)
voor het gate-verdict in de rapportage.

Standen (oplopend in kosten/impact):
    python3 scripts/backfill_legal_form_kvk.py                      # DRY-RUN: kandidaten + kostenraming, GEEN API-calls
    python3 scripts/backfill_legal_form_kvk.py --sample 3           # 3 echte lookups, GEEN writes (match-kwaliteit checken)
    python3 scripts/backfill_legal_form_kvk.py --apply              # lookups + writes
    python3 scripts/backfill_legal_form_kvk.py --scope cohort ...   # alleen de receptie-cohort-leads

Scopes: cohort (receptie_hook_code gezet) | launchable (gate 55/0.50 + sendable + gdpr, default) | all.
Workspace-safe (aerys), per-lead fouten gevangen, rate-limited (0,5s tussen leads).
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WORKSPACE = "aerys"
CALLS_PER_LEAD = 2          # zoeken + basisprofiel
COST_PER_CALL_EUR = 0.02


def _sendable(l: dict) -> bool:
    es, vm = l.get("email_status"), l.get("email_verification_method")
    if not l.get("email"):
        return False
    if es == "valid":
        return True
    return es in ("risky", "catchall_risky") and vm in ("smtp", "bouncer_api")


def _fetch_all(db, table: str, cols: str) -> list[dict]:
    out, off = [], 0
    while True:
        d = db.table(table).select(cols).range(off, off + 999).execute().data or []
        out += d
        if len(d) < 1000:
            return out
        off += 1000


def select_candidates(db, scope: str, sector: str | None = None) -> list[dict]:
    """Leads zonder kvk_legal_form binnen de gekozen scope (optioneel per sector)."""
    leads = _fetch_all(
        db, "leads",
        "id, company_name, city, score, icp_match, email, email_status, "
        "email_verification_method, gdpr_safe, kvk_legal_form, kvk_number, sector, workspace_id",
    )
    leads = [l for l in leads if l.get("workspace_id") == WORKSPACE and not (l.get("kvk_legal_form") or "").strip()]
    if sector:
        leads = [l for l in leads if l.get("sector") == sector]

    if scope == "all":
        return leads
    if scope == "cohort":
        wi = _fetch_all(db, "website_intelligence", "lead_id, receptie_hook_code, workspace_id")
        hook_ids = {w["lead_id"] for w in wi
                    if w.get("workspace_id") == WORKSPACE and w.get("receptie_hook_code")}
        return [l for l in leads if l["id"] in hook_ids]
    # launchable (default): zelfde funnel als de audit — gate + sendable + gdpr
    return [l for l in leads
            if (l.get("score") or 0) >= 55 and (l.get("icp_match") or 0) >= 0.50
            and _sendable(l) and l.get("gdpr_safe")]


async def lookup_legal_form(company_name: str, city: str) -> tuple[str | None, str | None]:
    """(kvk_number, rechtsvorm-omschrijving) of (None, None) bij geen betrouwbare match."""
    from scrapers.kvk_scraper import search_kvk, get_kvk_detail, match_best_kvk_result

    results = await search_kvk(company_name, city)
    if not results:
        return None, None
    best = match_best_kvk_result(results, company_name, city)
    if not best:
        return None, None
    kvk_number = str(best.get("kvkNummer", "")).zfill(8)
    profile = await get_kvk_detail(kvk_number)
    if not profile:
        return kvk_number, None
    rechtsvorm = profile.get("rechtsvorm", {})
    if isinstance(rechtsvorm, dict):
        rechtsvorm = rechtsvorm.get("omschrijving", "")
    return kvk_number, (rechtsvorm or "").strip() or None


async def run(scope: str, apply: bool, sample: int, sector: str | None = None) -> int:
    from config.database import get_heatr_supabase
    from utils.legal_form import classify_legal_form

    db = get_heatr_supabase()
    candidates = select_candidates(db, scope, sector)
    est = len(candidates) * CALLS_PER_LEAD * COST_PER_CALL_EUR
    print(f"scope={scope}{f' sector={sector}' if sector else ''} → {len(candidates)} kandidaten zonder kvk_legal_form")
    print(f"kostenraming bij volledige run: ~€{est:.2f} ({CALLS_PER_LEAD} calls/lead à €{COST_PER_CALL_EUR:.02f})")

    if not apply and not sample:
        print("\nDRY-RUN — geen API-calls gedaan. Eerste 15 kandidaten:")
        for l in candidates[:15]:
            print(f"  {l['id'][:8]}  '{(l.get('company_name') or '?')[:44]}'  {l.get('city') or '?'}  [{l.get('sector')}]")
        if len(candidates) > 15:
            print(f"  … +{len(candidates) - 15} meer")
        print("\nVolgende stap: --sample 3 (match-kwaliteit, geen writes) → --apply")
        return 0

    todo = candidates[:sample] if sample else candidates
    mode = f"SAMPLE {len(todo)} (geen writes)" if sample else f"APPLY {len(todo)}"
    print(f"\n{mode} — rate-limit 0,5s/lead …\n")

    filled = no_match = no_form = errors = 0
    verdicts: dict[str, int] = {}
    for i, l in enumerate(todo, 1):
        name, city = l.get("company_name") or "", l.get("city") or ""
        try:
            kvk_number, form = await lookup_legal_form(name, city)
            if form:
                verdict = classify_legal_form({**l, "kvk_legal_form": form})
                verdicts[verdict] = verdicts.get(verdict, 0) + 1
                print(f"  [{i}/{len(todo)}] {l['id'][:8]} '{name[:36]}' → {form}  ({verdict})")
                if apply:
                    patch: dict = {"kvk_legal_form": form}
                    if kvk_number and not l.get("kvk_number"):
                        patch["kvk_number"] = kvk_number
                    db.table("leads").update(patch).eq("id", l["id"]).eq("workspace_id", WORKSPACE).execute()
                filled += 1
            elif kvk_number:
                print(f"  [{i}/{len(todo)}] {l['id'][:8]} '{name[:36]}' → kvk {kvk_number} maar géén rechtsvorm — overgeslagen")
                no_form += 1
            else:
                print(f"  [{i}/{len(todo)}] {l['id'][:8]} '{name[:36]}' → geen betrouwbare KvK-match — overgeslagen")
                no_match += 1
        except Exception as e:  # per-lead vangen — nooit de run stoppen
            print(f"  [{i}/{len(todo)}] {l['id'][:8]} FOUT: {str(e)[:90]}")
            errors += 1
        await asyncio.sleep(0.5)

    print(f"\nresultaat: gevuld={filled}  geen-match={no_match}  geen-rechtsvorm={no_form}  fouten={errors}")
    if verdicts:
        print(f"gate-verdicts van de gevulde: {verdicts}")
        print("(natuurlijk_persoon = terecht geblokkeerd door de gate — dat is het punt van AVG-02)")
    if sample and not apply:
        print("\nSample klaar — niets geschreven. Bevalt de match-kwaliteit? Draai met --apply.")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--scope", choices=["cohort", "launchable", "all"], default="launchable")
    ap.add_argument("--apply", action="store_true", help="lookups + writes (kost geld)")
    ap.add_argument("--sample", type=int, default=0, metavar="N",
                    help="N echte lookups zonder writes (match-kwaliteit checken)")
    ap.add_argument("--sector", default=None,
                    help="alleen deze sector (bv. cosmetische_behandelaars) — scheelt kosten zolang alt-geneeskunde onbeslist is")
    args = ap.parse_args()
    raise SystemExit(asyncio.run(run(args.scope, args.apply, args.sample, args.sector)))
