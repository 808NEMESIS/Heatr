"""
audit/benchmark.py — stad+niche-benchmark voor de audit (Tier 2).

BEVROREN op moment van generatie: de teruggegeven dict wordt in het report
opgeslagen (heatr_audit_reports.benchmark). Een verstuurd rapport verandert dus
NOOIT omdat de dataset eromheen groeide. Geen live query bij het tonen.

Minimum n=10 vergelijkbare praktijken voordat een percentiel getoond wordt;
daaronder percentile=null (geen slag in de lucht) + een reden.

PROVISIONAL (cohort-beslissing 2026-07): de data-gaten zijn cohort-gebonden —
oude leads worden op minder checks gemeten dan nieuwe (has_cookie_banner,
response_headers, schema_org). De per-lead-score is prima, maar de benchmark is
VOORLOPIG tot een re-enrichment-sweep het cohort op gelijke meetbaarheid brengt.
Daarom `provisional: True` tot dan (zie project-audit-scorer geheugen).
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

MIN_N = 10  # minimaal aantal vergelijkbare praktijken (incl. de lead zelf)

_NICHE_LABEL = {
    "cosmetische_behandelaars": "cosmetische klinieken",
    "chiropractoren": "chiropractoren",
}


def _benchmark_from_scores(my_score: int, cohort_scores: list[int], *,
                           city: str, sector: str) -> dict:
    """Pure aggregatie. cohort_scores = de ANDERE praktijken (zonder de lead zelf)."""
    label = _NICHE_LABEL.get(sector, "praktijken")
    n = len(cohort_scores) + 1  # inclusief de lead zelf
    base = {"city": city, "sector": sector, "n": n, "niche_label": label,
            "provisional": True}
    if n < MIN_N:
        return {**base, "percentile": None, "rank_sentence": None,
                "note": f"minder dan {MIN_N} vergelijkbare {label} in {city} — geen betrouwbaar percentiel"}
    above = sum(1 for s in cohort_scores if s > my_score)     # praktijken boven je
    below = sum(1 for s in cohort_scores if s < my_score)     # praktijken onder je
    percentile = round((below + 1) / n * 100)                 # jouw positie incl. jezelf
    return {**base, "percentile": percentile,
            "rank_sentence": f"van de {n} {label} in {city} staan er {above} boven u",
            "above": above, "below": below}


async def compute_benchmark(lead: dict, my_score: int, sb: Any) -> dict:
    """Laad de cohort (zelfde stad+niche) uit audit_reports en bevries de benchmark.

    Never-raise: bij een fout een minimale dict met percentile=None.
    """
    ws = lead["workspace_id"]
    city = lead.get("city") or ""
    sector = lead.get("sector") or ""
    try:
        if not city or not sector:
            return {"city": city, "sector": sector, "n": 1, "percentile": None,
                    "rank_sentence": None, "provisional": True, "note": "stad/sector onbekend"}
        # lead_ids in dezelfde stad+niche
        peers = (sb.table("leads").select("id")
                 .eq("workspace_id", ws).eq("city", city).eq("sector", sector).execute()).data or []
        peer_ids = [p["id"] for p in peers if p.get("id") and p["id"] != lead["id"]]
        cohort_scores: list[int] = []
        if peer_ids:
            # laatste audit-score per peer
            rep = (sb.table("audit_reports").select("lead_id, version, score_normalized")
                   .eq("workspace_id", ws).in_("lead_id", peer_ids)
                   .order("version", desc=True).execute()).data or []
            seen: set = set()
            for r in rep:
                lid = r.get("lead_id")
                if lid in seen or r.get("score_normalized") is None:
                    continue
                seen.add(lid)
                cohort_scores.append(r["score_normalized"])
        return _benchmark_from_scores(my_score, cohort_scores, city=city, sector=sector)
    except Exception as e:  # noqa: BLE001
        logger.error("compute_benchmark faalde (lead=%s): %s", lead.get("id"), e)
        return {"city": city, "sector": sector, "n": 1, "percentile": None,
                "rank_sentence": None, "provisional": True, "note": f"fout: {str(e)[:60]}"}
