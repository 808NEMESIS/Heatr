"""scoring/market_benchmark.py — score_vs_market uit EIGEN geanalyseerde voorraad.

Geen Places-/concurrent-API nodig: we hebben zelf ~940 geanalyseerde sites. Twee
losse assen, want ze pitchen verschillende diensten:
  • 'website'      = total_score      → Aerys website-rebuild
  • 'automations'  = conversion_score → conversie-optimalisatie + Curio AI-audit
    (chatbot / online booking / WhatsApp / klikbare CTA — de automatiseringsgraad)

Peers = zelfde sector × stad (min. 3), anders sector-breed (gelabeld). De lead zelf
telt niet mee. Alleen zinvol negatief verschil levert een pitch-zin op (fail-soft:
None waar er geen benchmark of geen achterstand is). Deterministisch — geschikt als
input voor copy zónder Claude (respecteert de QA-gate: website-claims nooit via Claude).
"""
from __future__ import annotations

from typing import Any

# as-key → (WI-kolom, max-schaal, mensleesbaar label)
AXES: dict[str, tuple[str, int, str]] = {
    "website": ("total_score", 100, "website"),
    "automations": ("conversion_score", 30, "online afspraken, chat en WhatsApp"),
}
_MIN_PEERS = 3


def _peer_scores(db, workspace_id: str, sector: str, city: str | None,
                 lead_id: str, score_col: str) -> list[float]:
    """Scores (>0) van peer-leads in dezelfde sector (+ optioneel stad), lead zelf eruit."""
    q = db.table("leads").select("id").eq("workspace_id", workspace_id).eq("sector", sector)
    if city:
        q = q.eq("city", city)
    ids = [l["id"] for l in (q.execute().data or []) if l.get("id") != lead_id]
    scores: list[float] = []
    for i in range(0, len(ids), 200):  # .in_() met te veel UUIDs → URL te lang (les 2026-08)
        rows = (db.table("website_intelligence").select(f"lead_id, {score_col}")
                .eq("workspace_id", workspace_id).in_("lead_id", ids[i:i + 200])
                .gt(score_col, 0).execute().data or [])
        scores += [r[score_col] for r in rows if r.get(score_col) is not None]
    return scores


def _one_axis(db, workspace_id: str, lead_id: str, sector: str, city: str | None,
              own_score: float, score_col: str) -> dict | None:
    scope, scores = "stad", (_peer_scores(db, workspace_id, sector, city, lead_id, score_col) if city else [])
    if len(scores) < _MIN_PEERS:
        scope, scores = "sector", _peer_scores(db, workspace_id, sector, None, lead_id, score_col)
    if len(scores) < _MIN_PEERS:
        return None
    avg = sum(scores) / len(scores)
    return {
        "scope": scope, "city": city, "sector": sector,
        "peer_count": len(scores), "market_avg": round(avg, 1),
        "score_vs_market": round(own_score - avg, 1),
    }


def compute_benchmarks(db, workspace_id: str, lead_id: str, wi: dict) -> dict[str, dict | None]:
    """Return {'website': {...}|None, 'automations': {...}|None} voor deze lead.

    Leest sector/stad van de lead en benchmarkt elke as waarvoor de lead een score >0 heeft.
    """
    lead = (db.table("leads").select("sector, city")
            .eq("id", lead_id).eq("workspace_id", workspace_id).limit(1).execute().data or [None])[0]
    out: dict[str, dict | None] = {ax: None for ax in AXES}
    if not lead or not lead.get("sector"):
        return out
    sector, city = lead["sector"], lead.get("city")
    for ax, (col, _max, _label) in AXES.items():
        own = wi.get(col)
        if own and own > 0:
            out[ax] = _one_axis(db, workspace_id, lead_id, sector, city, own, col)
    return out


def benchmark_sentence(axis: str, bench: dict | None) -> str | None:
    """Deterministische NL-pitch-zin voor één as — alleen bij een echte achterstand.

    Geen Claude, geen aanname: puur de gemeten getallen. None als er geen zinvolle
    (negatieve) vergelijking is, zodat copy 'm gewoon kan weglaten.
    """
    if not bench:
        return None
    diff = bench.get("score_vs_market")
    if diff is None or diff >= 0:
        return None  # gelijk of beter dan de markt → geen pitch
    n = abs(round(diff))
    peers = bench["peer_count"]
    waar = f"in {bench['city']}" if bench["scope"] == "stad" and bench.get("city") else "in de sector"
    label = AXES.get(axis, (None, None, axis))[2]
    if axis == "website":
        return (f"je website scoort {n} punten onder het gemiddelde van "
                f"{peers} vergelijkbare praktijken {waar}")
    if axis == "automations":
        return (f"op {label} loop je {n} punten achter op het gemiddelde van "
                f"{peers} vergelijkbare praktijken {waar}")
    return f"{n} punten onder het gemiddelde van {peers} praktijken {waar}"


def enrich_wi_with_benchmarks(db, workspace_id: str, lead_id: str, wi: dict) -> dict:
    """In-place: voeg market_benchmark (website), automations_benchmark en de twee
    deterministische zinnen toe aan een WI-dict. Backward-compat: score_vs_market blijft
    de website-as. Fail-soft — bij een fout blijft wi ongewijzigd."""
    try:
        b = compute_benchmarks(db, workspace_id, lead_id, wi)
    except Exception:
        return wi
    if b.get("website"):
        wi["market_benchmark"] = b["website"]
        wi["score_vs_market"] = b["website"]["score_vs_market"]
        wi["market_sentence"] = benchmark_sentence("website", b["website"])
    if b.get("automations"):
        wi["automations_benchmark"] = b["automations"]
        wi["automations_sentence"] = benchmark_sentence("automations", b["automations"])
    return wi
