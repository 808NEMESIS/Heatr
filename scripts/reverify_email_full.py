"""
scripts/reverify_email_full.py — volledige her-verificatie van de risky-bucket
via de externe API (remediation C1-vervolg).

Verstuurt NIETS. Idempotent, workspace-beperkt tot aerys. Twee modi:
  - dry-run (default): telt + rapporteert verdeling, schrijft niet.
  - --apply: schrijft coarse email_status + email_verification_method +
    email_verified_at naar leads (fail-soft op de 030-kolommen als die
    ontbreken) + append-only audit-log.

Stopconditie: bij >50% API-fout (not_checked/error) stopt de run — dan is
er iets mis met de API/tegoed en heeft doorgaan geen zin.

Gebruik:
    python3 scripts/reverify_email_full.py            # dry-run, alle risky
    python3 scripts/reverify_email_full.py --apply     # schrijf resultaten
    python3 scripts/reverify_email_full.py --limit 50  # beperk aantal
"""
from __future__ import annotations

import asyncio
import os
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = "aerys"
_ABORT_ERROR_FRACTION = 0.5


async def main(apply: bool = False, limit: int = 2000) -> int:
    from config.database import get_heatr_supabase
    from enrichment.verify_api import verify_via_api, is_enabled
    from enrichment.email_verifier import coarse_email_status

    if not is_enabled():
        print("⛔ Externe verify-API niet geconfigureerd (BOUNCER_API_KEY/EMAIL_VERIFY_PROVIDER)."); return 1

    sb = get_heatr_supabase()
    rows = (sb.table("leads")
            .select("id, email, email_status, sector")
            .eq("workspace_id", WORKSPACE)
            .in_("email_status", ["risky", "not_checked"])
            .not_.is_("email", "null")
            .limit(limit).execute().data or [])
    mode = "APPLY (schrijft)" if apply else "DRY-RUN (schrijft niets)"
    print(f"Her-verificatie van {len(rows)} risky/not_checked leads — {mode}. GEEN mail verzonden.\n")

    st = Counter(); errors = 0; run_label = "reverify_full_" + datetime.now(timezone.utc).strftime("%Y%m%d")
    for i, r in enumerate(rows, 1):
        res = await verify_via_api(r["email"])
        status = res["status"]; method = res.get("method", "bouncer_api")
        coarse = coarse_email_status(status)
        st[coarse] += 1
        if method in ("error", "disabled") or status == "not_checked":
            errors += 1

        if apply:
            patch = {"email_status": coarse}
            # 030-kolommen fail-soft: probeer mét, val terug op alleen status
            try:
                sb.table("leads").update({
                    **patch,
                    "email_verification_method": method,
                    "email_verified_at": datetime.now(timezone.utc).isoformat(),
                }).eq("id", r["id"]).eq("workspace_id", WORKSPACE).execute()
            except Exception:
                try:
                    sb.table("leads").update(patch).eq("id", r["id"]).eq("workspace_id", WORKSPACE).execute()
                except Exception as e:
                    print(f"  ⚠️  update faalde voor {r['email']}: {e}")
            try:
                sb.table("email_verifications").insert({
                    "workspace_id": WORKSPACE, "lead_id": r["id"], "email": r["email"],
                    "verification_status": status if status in (
                        "valid","invalid","catchall_risky","risky","timeout",
                        "connection_error","temporary_failure","not_checked","not_found") else "not_checked",
                    "verification_method": method, "run_label": run_label,
                }).execute()
            except Exception:
                pass

        if i % 25 == 0 or i == len(rows):
            print(f"  {i}/{len(rows)} verwerkt… {dict(st)}")
        if i >= 20 and errors / i >= _ABORT_ERROR_FRACTION:
            print(f"\n⛔ STOP: {errors}/{i} API-fouten (>{int(_ABORT_ERROR_FRACTION*100)}%). Check tegoed/API."); break

    n = sum(st.values())
    print(f"\n── UITKOMST (n={n}) ──")
    print("coarse email_status :", dict(st))
    print(f"→ verzendbaar (valid): {st.get('valid',0)}/{n}  ({100*st.get('valid',0)//max(n,1)}%)")
    if not apply:
        print("\n(DRY-RUN — er is niets geschreven. Draai met --apply om de statussen te persisteren.)")
    return 0


if __name__ == "__main__":
    _apply = "--apply" in sys.argv
    _limit = 2000
    if "--limit" in sys.argv:
        _limit = int(sys.argv[sys.argv.index("--limit") + 1])
    raise SystemExit(asyncio.run(main(apply=_apply, limit=_limit)))
