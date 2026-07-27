"""scripts/verify_unsubscribe_compliance.py — post-send-sweep op de Warmr-afmeldlink.

Sami 2026-07-27: Warmr's afmeld-footer is best-effort (campaign_scheduler try/except),
dus af en toe gaat een mail ZONDER afmeldlink de deur uit. Deze sweep draait NÁ de
send (tussen drip-batches, via cron): voor elke lead die Warmr verstuurd heeft
(email_events.event_type='sent') controleert 'ie of er een unsubscribe_tokens-rij is.
Ontbreekt die → een HARDE, zichtbare compliance-vlag (heatr_compliance_flags, 044),
die de drip-pre-gate (sequence_engine) laat blokkeren tot afgehandeld.

Zichtbaarheid boven stilte: het aantal sent-leads wordt altijd gerapporteerd, zodat
"0 gevonden" opvalt i.p.v. stil als "alles ok" door te gaan.

  python3 scripts/verify_unsubscribe_compliance.py --campaign-id <uuid>            # dry-run
  python3 scripts/verify_unsubscribe_compliance.py --campaign-id <uuid> --apply    # zet vlaggen
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))


def _sent_lead_ids(raw_sb, campaign_id: str) -> list[str]:
    """Distinct lead_ids die Warmr voor deze campagne verstuurd heeft."""
    rows = (raw_sb.table("email_events")
            .select("lead_id")
            .eq("campaign_id", campaign_id)
            .eq("event_type", "sent")
            .execute().data or [])
    seen, out = set(), []
    for r in rows:
        lid = r.get("lead_id")
        if lid and lid not in seen:
            seen.add(lid); out.append(lid)
    return out


def main(campaign_id: str, workspace: str, apply: bool) -> int:
    from supabase import create_client
    from config.database import get_heatr_supabase
    from utils.warmr_unsubscribe import unsubscribe_token_present, verify_and_flag

    raw_sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
    heatr_sb = get_heatr_supabase()

    lead_ids = _sent_lead_ids(raw_sb, campaign_id)
    mode = "APPLY (zet vlaggen)" if apply else "DRY-RUN (rapporteert, geen vlaggen)"
    print(f"Unsubscribe-compliance-sweep · campagne {campaign_id} · {mode}")
    print(f"Verstuurde (sent) leads gevonden: {len(lead_ids)}"
          + ("  ⚠ 0 — controleer campaign_id/event_type" if not lead_ids else "") + "\n")

    missing, ok = [], 0
    for lid in lead_ids:
        if apply:
            present, detail = verify_and_flag(
                raw_sb, heatr_sb, workspace_id=workspace, lead_id=lid, campaign_id=campaign_id)
        else:
            present, detail = unsubscribe_token_present(raw_sb, lid, campaign_id)
        if present:
            ok += 1
        else:
            missing.append((lid, detail))
            print(f"  ✗ MISSEND lead={lid} — {detail}{'  → VLAG GEZET' if apply else ''}")

    print(f"\nKlaar: {len(lead_ids)} sent · {ok} met afmeldlink · {len(missing)} MISSEND"
          + (f" ({len(missing)} vlag(gen) gezet — drip geblokkeerd tot afgehandeld)" if apply and missing else ""))
    if missing and not apply:
        print("Draai met --apply om de compliance-vlaggen te zetten (blokkeert de drip).")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--campaign-id", required=True)
    ap.add_argument("--workspace", default="aerys")
    ap.add_argument("--apply", action="store_true", help="zet compliance-vlaggen bij missende afmeldlink")
    args = ap.parse_args()
    raise SystemExit(main(args.campaign_id, args.workspace, args.apply))
