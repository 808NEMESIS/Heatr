"""
scripts/reverify_email_sample.py — diagnostische her-verificatie (remediation 2.5).

Beantwoordt de C1-kernvraag: is de massale 'risky' een KAPOTTE verifier/omgeving
(poort 25 dicht → timeout/connection_error) of ECHTE doelgroep-realiteit
(greylist/4xx → temporary_failure/risky met method='smtp')?

Veilig by design:
  - VERSTUURT NIETS;
  - OVERSCHRIJFT geen lead-data (alleen lezen + optioneel audit-log);
  - workspace-beperkt tot aerys;
  - kleine sample eerst; stopt bij een abnormaal foutpercentage;
  - idempotent (leest steeds vers, schrijft alleen append-only log).

Gebruik: python3 scripts/reverify_email_sample.py [sample_size]
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
ICP = ["alternatieve_geneeskunde", "cosmetische_behandelaars"]
_ABORT_INFRA_FRACTION = 0.9  # stop als >90% infra-fout → verifier is dood, verder testen zinloos


async def main(sample_size: int = 10) -> int:
    from config.database import get_heatr_supabase
    from enrichment.email_verifier import verify_email, coarse_email_status

    sb = get_heatr_supabase()
    rows = (sb.table("leads")
            .select("id, email, email_status")
            .eq("workspace_id", WORKSPACE)
            .in_("sector", ICP)
            .eq("email_status", "risky")
            .not_.is_("email", "null")
            .limit(sample_size).execute().data or [])
    if not rows:
        print("Geen risky in-ICP leads met e-mail gevonden."); return 0

    print(f"Her-verificatie van {len(rows)} risky in-ICP leads (workspace={WORKSPACE}) — GEEN verzending, GEEN overschrijving.\n")
    by_status: Counter = Counter()
    by_method: Counter = Counter()
    infra = 0
    for i, r in enumerate(rows, 1):
        email = r["email"]
        try:
            status, method = await verify_email(email, sb)
        except Exception as e:
            status, method = "not_checked", f"error:{type(e).__name__}"
        by_status[status] += 1
        by_method[method] += 1
        if status in ("timeout", "connection_error"):
            infra += 1
        print(f"  {i:2d}. {email:40s} → status={status:18s} method={method:16s} coarse={coarse_email_status(status)}")
        # append-only audit-log (fail-soft als migratie 030 ontbreekt)
        try:
            sb.table("email_verifications").insert({
                "workspace_id": WORKSPACE, "lead_id": r["id"], "email": email,
                "verification_status": status if status in (
                    "valid","invalid","catchall_risky","risky","timeout",
                    "connection_error","temporary_failure","not_checked","not_found") else "not_checked",
                "verification_method": method, "run_label": "reverify_sample",
            }).execute()
        except Exception:
            pass
        # stopconditie
        if i >= 5 and infra / i >= _ABORT_INFRA_FRACTION:
            print(f"\n⛔ STOP: {infra}/{i} infra-fouten (>{int(_ABORT_INFRA_FRACTION*100)}%) — de verifier/omgeving is kapot (poort 25?).")
            print("   Volledige 729-run heeft geen zin tot de SMTP-route werkt.")
            break

    n = sum(by_status.values())
    print(f"\n── UITKOMST (n={n}) ─────────────────────────────")
    print("status :", dict(by_status))
    print("method :", dict(by_method))
    infra_frac = infra / n if n else 0
    smtp_answered = sum(v for k, v in by_method.items() if k == "smtp")
    print(f"\nDIAGNOSE:")
    if infra_frac >= 0.5:
        print(f"  → INFRA KAPOT: {infra}/{n} ({infra_frac:.0%}) timeout/connection. De 'risky' is")
        print(f"    grotendeels een verifier/omgevings-probleem (poort 25 dicht), NIET de doelgroep.")
        print(f"    Fix = verifier op een host/route met open poort 25 (of externe verify-API).")
    elif smtp_answered >= n * 0.5:
        print(f"  → DOELGROEP-REALITEIT: {smtp_answered}/{n} kregen een echt SMTP-antwoord.")
        print(f"    De verifier werkt; 'risky' weerspiegelt greylist/catch-all. Fix = greylist-retry + accepteer bewust.")
    else:
        print(f"  → GEMENGD/ONDUIDELIJK — vergroot de sample en herhaal.")
    return 0


if __name__ == "__main__":
    size = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    raise SystemExit(asyncio.run(main(size)))
