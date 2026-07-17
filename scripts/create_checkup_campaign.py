"""
scripts/create_checkup_campaign.py — maak de Warmr cover-campagne voor check-up.

Warmr kan geen bijlage; de rapport-link zit in de cover-mail-body die Heatr per
lead meestuurt als custom_subject/custom_body. Warmr rendert die als
{{custom_subject}} / {{custom_body}}. Dit script maakt een campagne met precies
EEN stap die niets anders doet dan die twee tokens renderen, en print de
campaign_id die je in .env zet als CHECKUP_WARMR_CAMPAIGN_ID.

    python3 scripts/create_checkup_campaign.py            # maakt de campagne aan
    python3 scripts/create_checkup_campaign.py --dry-run  # toont payload, maakt niets

Dit verstuurt niets — het definieert alleen de campagne. De echte send loopt via
scripts/send_checkup_report.py, achter de kill-switches.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

STEP = {
    "subject": "{{custom_subject}}",
    "body": "{{custom_body}}",
    "delay_days": 0,
}


async def _run(dry: bool) -> int:
    from integrations.warmr_client import WarmrClient
    client = WarmrClient()
    inboxes = await client.get_ready_inboxes()
    if not inboxes:
        print("FOUT: geen ready Warmr-inbox. Zonder inbox kan de campagne niet sturen.", file=sys.stderr)
        return 1
    inbox_ids = [i["id"] for i in inboxes]
    settings = {"inbox_ids": inbox_ids, "from_name": os.getenv("CHECKUP_FROM_NAME", "Sami")}

    print(f"Ready inboxes: {len(inbox_ids)} -> {inbox_ids}")
    print(f"Step: subject={STEP['subject']!r} body={STEP['body']!r} delay_days=0")
    if dry:
        print("\n[dry-run] campagne NIET aangemaakt.")
        return 0

    cid = await client.create_campaign(
        name="Check-up follow-up (cover)",
        sequence_steps=[STEP],
        settings=settings,
    )
    print(f"\nCampagne aangemaakt. Zet in .env:\n\n  CHECKUP_WARMR_CAMPAIGN_ID={cid}\n")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Maak de Warmr cover-campagne voor check-up.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    return asyncio.run(_run(args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
