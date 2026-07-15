"""
scripts/regenerate_openers.py — hergenereer stale, afgekapte personalized_opener.

Root cause (2026-07-15): 88% van de openers is mid-zin/woord afgekapt — gemaakt
toen generate_personalized_opener nog een lage max_tokens had. De huidige code
(max_tokens=1200) levert complete openers (vandaag verrijkt: 0% afgekapt). Deze
runner roept de HUIDIGE generator opnieuw aan voor afgekapte leads, normaliseert
(text_normalizer) en persisteert alleen de schone versie.

Twee modi:
  - dry-run (default): SAMPLE van --limit leads → genereert + toont VOOR/NA,
    schrijft niets. (NB: genereren kost Claude-tokens, ook in dry-run.)
  - --apply: alle afgekapte leads hergenereren + persisteren.

Verstuurt GEEN mail. ~Haiku €0,001/lead.

    python3 scripts/regenerate_openers.py --limit 10       # sample
    python3 scripts/regenerate_openers.py --apply
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

WORKSPACE = "aerys"
_TERMINAL = '.!?"”’)'
# Alleen actieve ICP-sectoren regenereren — makelaars/bouwbedrijven zijn
# uit-ICP (worden niet benaderd), dus hun opener fixen is verspilde Claude-calls.
_ACTIVE_ICP = {"cosmetische_behandelaars", "chiropractoren", "alternatieve_geneeskunde"}


def _truncated(op: str | None) -> bool:
    op = (op or "").strip()
    return bool(op) and op[-1] not in _TERMINAL


async def main(apply: bool = False, limit: int = 10) -> int:
    import logging
    logging.disable(logging.INFO)
    import anthropic
    from config.database import get_heatr_supabase
    from enrichment.company_enrichment import generate_personalized_opener
    from utils.text_normalizer import normalize_generated_text

    sb = get_heatr_supabase()
    anth = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    rows = (sb.table("leads")
            .select("id, company_name, city, industry, contact_first_name, company_summary, "
                    "has_instagram, google_rating, google_review_count, sector, personalized_opener")
            .eq("workspace_id", WORKSPACE)
            .not_.is_("personalized_opener", "null")
            .limit(5000).execute().data or [])
    stale = [r for r in rows if _truncated(r.get("personalized_opener"))
             and (r.get("sector") or "") in _ACTIVE_ICP]

    todo = stale if apply else stale[:limit]
    mode = "APPLY (schrijft)" if apply else f"DRY-RUN sample {len(todo)} (schrijft niets)"
    print(f"Afgekapte openers: {len(stale)} — verwerk {len(todo)} — {mode}. GEEN mail.\n")

    ok = fail = still_bad = 0
    for i, r in enumerate(todo, 1):
        try:
            raw = await generate_personalized_opener(
                company_name=r.get("company_name") or "",
                city=r.get("city") or "",
                industry=r.get("industry") or "",
                contact_name=r.get("contact_first_name"),
                summary=r.get("company_summary") or "",
                has_instagram=bool(r.get("has_instagram")),
                google_rating=r.get("google_rating"),
                google_review_count=r.get("google_review_count"),
                sector_key=r.get("sector") or "",
                language="nl",
                anthropic_client=anth,
            )
        except Exception as e:
            fail += 1
            print(f"  ⚠️ generatie faalde voor {r.get('company_name')}: {str(e)[:60]}")
            continue

        clean, good, reason = normalize_generated_text(raw, max_sentences=3)
        if not good or _truncated(clean):
            still_bad += 1
            continue
        ok += 1
        if not apply and i <= 12:
            print(f"── {r.get('company_name')} ──")
            print(f"  OUD: {(r.get('personalized_opener') or '').strip()[-70:]!r}")
            print(f"  NEW: {clean!r}\n")
        if apply:
            try:
                sb.table("leads").update({"personalized_opener": clean}).eq("id", r["id"]).eq("workspace_id", WORKSPACE).execute()
            except Exception as e:
                print(f"  ⚠️ update faalde voor {r['id']}: {str(e)[:60]}")
            if i % 50 == 0:
                print(f"  {i}/{len(todo)} hergenereerd…")

    print(f"\n── UITKOMST ── goed: {ok} | nog afgekapt na regen (overslaan): {still_bad} | generatie-fout: {fail}")
    if not apply:
        print("\n(DRY-RUN sample — niets geschreven. Draai met --apply voor alle afgekapte.)")
    return 0


if __name__ == "__main__":
    _apply = "--apply" in sys.argv
    _limit = 10
    if "--limit" in sys.argv:
        _limit = int(sys.argv[sys.argv.index("--limit") + 1])
    raise SystemExit(asyncio.run(main(apply=_apply, limit=_limit)))
