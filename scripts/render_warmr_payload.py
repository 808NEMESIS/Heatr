"""
scripts/render_warmr_payload.py — Pre-send inspectie van Warmr-payload + Mail 1.

Rendert voor één lead exact wat Heatr naar Warmr zou pushen — zonder iets te
versturen. Gebruikt de echte WarmrClient._build_lead_payload + sequence-engine
zodat je vóór de eerste live send kan inspecteren of:
  - {{first_name}} en {{opener}} correct gesubstitueerd worden
  - alle custom_fields gevuld zijn met zinvolle data
  - Mail 1 + 2 + 3 lezen als bedoeld
  - geen unresolved placeholders ({{...}}) meer in de output zitten

Run:
    python3 scripts/render_warmr_payload.py <lead_id>
    python3 scripts/render_warmr_payload.py --first   # eerste enriched lead

Output: JSON + rendered mails op stdout. Geen API-calls naar Warmr.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv("/Users/nemesis/Heatr/.env")
load_dotenv("/Users/nemesis/warmr/.env", override=False)


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("lead_id", nargs="?", help="UUID van lead om te renderen")
    parser.add_argument("--first", action="store_true",
                        help="Pak eerste enriched lead met email")
    parser.add_argument("--template", default=None,
                        help="Forceer template_id (default: auto op sector)")
    args = parser.parse_args()

    from config.database import get_heatr_supabase
    from config.sequence_templates import (
        SEQUENCE_TEMPLATES,
        get_observation_text,
        get_v1_sequence,
        pick_observation_block,
        template_for_sector,
    )
    from campaigns.sequence_engine import render_step, is_quality_opener
    from integrations.warmr_client import WarmrClient

    db = get_heatr_supabase()

    # 1. Pak lead
    if args.first and not args.lead_id:
        res = (
            db.table("leads")
            .select("id")
            .neq("email", None)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if not res.data:
            print("Geen lead met email gevonden.")
            return 1
        lead_id = res.data[0]["id"]
    elif args.lead_id:
        lead_id = args.lead_id
    else:
        print("Geef een lead_id of --first")
        return 1

    res = db.table("leads").select("*").eq("id", lead_id).maybe_single().execute()
    if not res or not res.data:
        print(f"Lead {lead_id} niet gevonden.")
        return 1
    lead = res.data

    print(f"\n{'='*80}")
    print(f"LEAD: {lead.get('company_name')} — {lead.get('domain')}")
    print(f"Email: {lead.get('email')} (status: {lead.get('email_status')})")
    print(f"Sector: {lead.get('sector')} · Archetype: {lead.get('archetype') or '(niet gezet)'}")
    print(f"Score: {lead.get('score')} · Pers: {lead.get('personalization_potential')}")
    print(f"{'='*80}\n")

    # 2. Pick template (auto of forced)
    # DEPRECATED: dit script gebruikt nog v1.0 + observation-block flow voor pre-send
    # inspectie van bestaande v1.0-leads. Voor v3.1 launch-flow zie /campaigns/preview
    # in api/main.py. Kept until 2026-06-06 — daarna porteren naar pick_brug + render_step.
    template_id = args.template or template_for_sector(lead.get("sector")) or "v1_cosmetisch_audit"
    template = SEQUENCE_TEMPLATES.get(template_id)
    if not template:
        print(f"Template '{template_id}' onbekend.")
        return 1

    print(f"TEMPLATE: {template_id}")
    print(f"  cadence: {template['cadence_days']}, min_pers: {template.get('min_personalization_score')}\n")

    # 3. Pick observation block + render opener
    block = pick_observation_block(lead) if template.get("sector") else None
    print(f"OBSERVATION BLOCK: {block}")
    print(f"  reason: archetype={lead.get('archetype')}, "
          f"website_age={lead.get('website_age_years')}, "
          f"has_booking={lead.get('has_online_booking')}, "
          f"meta_ads={lead.get('meta_ads_active')}\n")

    # 4. Bepaal opener: Claude wint als kwalitatief, anders static
    static_text = get_observation_text(block, lead) if block else ""
    claude_opener = lead.get("personalized_opener") or ""
    ok, reason = is_quality_opener(claude_opener)
    if ok:
        opener_source = "claude_personalized"
        opener_text = claude_opener
    else:
        opener_source = f"static_block:{block}"
        opener_text = static_text

    print(f"OPENER SOURCE: {opener_source}")
    if not ok and claude_opener:
        print(f"  (Claude opener afgewezen: {reason})")
    print(f"OPENER TEXT:\n{opener_text}\n")

    # 5. Render alle 3 mails
    sequence_steps = template["default_steps"]
    lead_for_render = {**lead, "personalized_opener": opener_text}
    print(f"{'='*80}")
    print("RENDERED MAILS:")
    print(f"{'='*80}\n")
    for i, step in enumerate(sequence_steps, 1):
        rendered = render_step(step, lead_for_render)
        print(f"--- MAIL {i} (dag +{rendered['delay_days']}) ---")
        print(f"Subject: {rendered['subject']}")
        print(f"Body:\n{rendered['body']}\n")
        # Sanity check: geen unresolved placeholders
        if "{{" in rendered["body"] or "{{" in rendered["subject"]:
            print(f"  ⚠ UNRESOLVED PLACEHOLDER in mail {i}!\n")

    # 6. Build Warmr-payload (zonder API-call!)
    print(f"{'='*80}")
    print("WARMR PAYLOAD (zou gepushed worden):")
    print(f"{'='*80}\n")
    # Inject _observation_text zoals launch endpoint dat doet
    lead_for_warmr = {
        **lead,
        "_observation_text": opener_text,
        "_observation_block": block,
    }
    try:
        # WarmrClient kan crashen bij init (geen WARMR_API_URL); we hebben
        # alleen _build_lead_payload nodig — pak die direct.
        client = WarmrClient.__new__(WarmrClient)  # bypass __init__
        client._sb = None
        client.workspace_id = "aerys"
        payload = client._build_lead_payload(
            lead=lead_for_warmr,
            campaign_id="<TEST_CAMPAIGN_ID>",
            preferred_inbox_id=None,
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"Payload-build faalde: {e}")

    # 7. Sanity check op cruciale custom_fields
    print(f"\n{'='*80}")
    print("SANITY CHECKS:")
    print(f"{'='*80}")
    cf = payload.get("custom_fields", {}) if "payload" in dir() else {}
    checks = [
        ("opener gevuld", bool(cf.get("opener")) and len(cf.get("opener", "")) > 30),
        ("first_name gevuld", bool(payload.get("first_name"))),
        ("email gevuld", bool(payload.get("email"))),
        ("heatr_lead_id correct", cf.get("heatr_lead_id") == lead_id),
        ("workspace_id gevuld", bool(cf.get("workspace_id"))),
        ("campaign_id meegegeven", bool(payload.get("campaign_id"))),
    ]
    for name, ok in checks:
        print(f"  {'✓' if ok else '✗'} {name}")

    # Per-template body sanity
    body_has_opener_placeholder = any(
        "{{opener}}" in (s.get("body") or "")
        for s in template["default_steps"]
    )
    print(f"  {'✓' if body_has_opener_placeholder else '✗'} Mail-1 frame heeft {{{{opener}}}} placeholder voor Warmr-substitutie")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
