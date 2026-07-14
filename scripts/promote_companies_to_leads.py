"""
scripts/promote_companies_to_leads.py — de ontbrekende brug companies_raw → leads.

De scraper vult companies_raw, en enrichment/lead_qualifier.qualify_and_create_lead
kan een raw-bedrijf kwalificeren + tot lead promoveren + voor enrichment queuen —
maar die functie had NUL callers (losgeraakt bij de scraping-runner-refactor,
recovery patch 3). Gevolg: gescrapete bedrijven bleven wezen in companies_raw.

Deze runner roept de BESTAANDE promoter aan over nog-niet-verwerkte
companies_raw-rijen (qualification_status IS NULL). Geen herbouw, puur wiren.

Modi (patroon reverify_email_full.py):
  - dry-run (default): draait alleen qualify_raw_company (geen writes) en telt
    hoeveel zouden kwalificeren / afvallen + reden.
  - --apply: qualify_and_create_lead → maakt leads (status='discovered') aan én
    queue't ze voor enrichment. Verstuurt GEEN mail. Idempotent: dedup op domein
    in de leads-tabel zit in de qualifier zelf.

Gebruik:
    python3 scripts/promote_companies_to_leads.py --city Amsterdam
    python3 scripts/promote_companies_to_leads.py --city Amsterdam --apply
    python3 scripts/promote_companies_to_leads.py --apply           # alle steden
"""
from __future__ import annotations

import asyncio
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = "aerys"


async def main(apply: bool = False, city: str | None = None, limit: int = 2000) -> int:
    from config.database import get_heatr_supabase
    from enrichment.lead_qualifier import qualify_raw_company, qualify_and_create_lead

    sb = get_heatr_supabase()
    q = (sb.table("companies_raw").select("*")
         .eq("workspace_id", WORKSPACE)
         .is_("qualification_status", "null"))   # nog niet verwerkt
    if city:
        q = q.eq("city", city)
    rows = q.limit(limit).execute().data or []

    scope = f"city={city}" if city else "alle steden"
    mode = "APPLY (maakt leads + queue't enrichment)" if apply else "DRY-RUN (schrijft niets)"
    print(f"Promotie companies_raw → leads — {scope}, {len(rows)} onverwerkte rijen — {mode}. GEEN mail.\n")

    created = 0
    disq = Counter()
    for i, raw in enumerate(rows, 1):
        sector_key = raw.get("sector") or ""
        if apply:
            lead = await qualify_and_create_lead(raw, sector_key, WORKSPACE, sb)
            if lead:
                created += 1
                if created <= 15:
                    print(f"  ✓ lead: {raw.get('company_name')} ({raw.get('domain')})")
            else:
                # qualify_and_create_lead heeft de reden al weggeschreven; herbereken voor de telling
                ok, reason, _ = await qualify_raw_company(raw, sector_key, WORKSPACE, sb)
                disq[reason] += 1
        else:
            ok, reason, prio = await qualify_raw_company(raw, sector_key, WORKSPACE, sb)
            if ok:
                created += 1
                if created <= 15:
                    print(f"  → zou promoveren: {raw.get('company_name')} ({raw.get('domain')}) prio={prio}")
            else:
                disq[reason] += 1
        if i % 50 == 0:
            print(f"  … {i}/{len(rows)} verwerkt")

    verb = "gepromoveerd naar leads" if apply else "zou promoveren"
    print(f"\n── UITKOMST ──")
    print(f"  {created} {verb}")
    print(f"  {sum(disq.values())} afgevallen: {dict(disq.most_common())}")
    if apply:
        print("\n✅ Leads aangemaakt op status 'discovered' + in de enrichment-queue gezet.")
    else:
        print("\n(DRY-RUN — niets geschreven. Draai met --apply om te promoveren.)")
    return 0


if __name__ == "__main__":
    _apply = "--apply" in sys.argv
    _city = None
    if "--city" in sys.argv:
        _city = sys.argv[sys.argv.index("--city") + 1]
    _limit = 2000
    if "--limit" in sys.argv:
        _limit = int(sys.argv[sys.argv.index("--limit") + 1])
    raise SystemExit(asyncio.run(main(apply=_apply, city=_city, limit=_limit)))
