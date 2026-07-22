"""
scripts/regenerate_openers_single.py — openers herschrijven naar ÉÉN warme zin.

De oude openers zijn 2-3 zinnen die soms op een eigen zijstapje eindigen vlak
vóór de site-brug ({{site_observatie}}). Deze run genereert per lead één warme
observatiezin (nieuwe prompt in generate_personalized_opener), toetst via de
EXACTE productie-QA-keten en schrijft pas weg met --apply.

    python3 scripts/regenerate_openers_single.py --limit 6          # sample, DRY
    python3 scripts/regenerate_openers_single.py --limit 300 --apply

QA-keten = normalize_generated_text → strip_unverified_claims → validate_opener_sendable
(zoals enrichment/company_enrichment.py). Afgekeurd → veld ongemoeid.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from anthropic import AsyncAnthropic
from config.database import get_heatr_supabase
from config.sequence_templates import faseA_brug_for, pick_brug
from campaigns.sequence_engine import render_faseA_marker
from enrichment.company_enrichment import generate_personalized_opener
from utils.text_normalizer import (
    normalize_generated_text, strip_unverified_claims, validate_opener_sendable,
)

WORKSPACE = "aerys"


def _qa(raw: str) -> tuple[str, bool, str]:
    """Exacte productie-QA-keten voor een opener."""
    clean, ok, reason = normalize_generated_text(raw, max_sentences=3)
    if not ok:
        return clean, False, reason
    clean = strip_unverified_claims(clean)
    ok, qa_reason = validate_opener_sendable(clean)
    return clean, ok, ("ok" if ok else f"qa:{qa_reason}")


async def main(apply: bool, limit: int) -> int:
    sb = get_heatr_supabase()
    client = AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    leads = (sb.table("leads").select("*")
             .eq("workspace_id", WORKSPACE).eq("sector", "cosmetische_behandelaars")
             .neq("is_test_lead", True).not_.is_("personalized_opener", "null")
             .gte("score", 55).limit(limit).execute()).data or []

    print(f"{len(leads)} kandidaat-leads\n" + "=" * 72)
    written = skipped = 0
    for lead in leads:
        raw = await generate_personalized_opener(
            company_name=lead.get("company_name") or "",
            city=lead.get("city") or "",
            industry=lead.get("industry") or "",
            contact_name=lead.get("contact_first_name"),
            summary=lead.get("company_summary") or "",
            has_instagram=bool(lead.get("has_instagram")),
            google_rating=lead.get("google_rating"),
            google_review_count=lead.get("google_review_count"),
            sector_key=lead.get("sector") or "",
            language="nl",
            anthropic_client=client,
        )
        clean, ok, reason = _qa(raw)
        print(f"\n### {lead.get('company_name')} ({lead.get('city')})")
        print(f"  OUD:   {(lead.get('personalized_opener') or '').strip()!r}")
        print(f"  NIEUW: {clean!r}   [{reason}]")
        if not ok:
            skipped += 1
            continue
        if apply:
            sb.table("leads").update({"personalized_opener": clean}) \
              .eq("id", lead["id"]).eq("workspace_id", WORKSPACE).execute()
            written += 1
        # toon de volledige mail 1 met nieuwe opener + site-brug (dry: met in-memory opener)
        preview_lead = {**lead, "personalized_opener": clean}
        brug = faseA_brug_for(pick_brug(preview_lead))
        out = await render_faseA_marker({"faseA_brug": brug, "faseA_step": 0}, preview_lead, sb, WORKSPACE)
        intro = "\n".join(out["body"].split("\n\n")[:4])
        print(f"  ── mail 1 (intro):\n     " + intro.replace("\n", "\n     "))

    print("\n" + "=" * 72)
    print(f"klaar. {'GESCHREVEN' if apply else 'DRY-RUN'} | weggeschreven: {written} | QA-afgekeurd: {skipped}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=6)
    args = ap.parse_args()
    raise SystemExit(asyncio.get_event_loop().run_until_complete(main(args.apply, args.limit)))
