"""scoring/market_benchmark.py — marktvergelijking uit EIGEN geanalyseerde voorraad.

Geen Places-/concurrent-API nodig: we hebben zelf ~940 geanalyseerde sites. Twee
losse assen, want ze pitchen verschillende diensten:
  • 'website'      → Aerys website-rebuild        (score: total_score; criteria: technical_details)
  • 'automations'  → conversie-opt + Curio-audit  (score: conversion_score; criteria: conversion_details)

BELANGRIJK (feedback 2026-08-04): een prospect weet niet wat "6 punten onder het
gemiddelde" betekent — dat is een intern getal. De PITCH grondt daarom op concrete,
controleerbare features: "we vergeleken jullie site met {N} praktijken in {stad} op
{criteria}; {X}% daarvan {doet Y} — bij jullie zagen we dat (nog) niet." De ruwe
score/score_vs_market blijft bestaan voor INTERN gebruik (dashboard/scoring), niet
voor de prospect. Peers = sector × stad (min. 3, anders sector-breed). Deterministisch
(nooit via Claude → QA-gate-veilig).
"""
from __future__ import annotations

from typing import Any

# as → (score-kolom, details-kolom, mensleesbaar sector-onafhankelijk label)
AXES: dict[str, tuple[str, str]] = {
    "website": ("total_score", "technical_details"),
    "automations": ("conversion_score", "conversion_details"),
}
_MIN_PEERS = 3
_MIN_PREVALENCE = 0.5   # feature telt pas als 'differentiator' als een meerderheid 'm heeft

# Per as: welke checks zijn prospect-relevant + hoe benoemen we wat de peer WEL heeft.
# (SEO-technische checks als schema_markup/sitemap/cms laten we bewust weg — betekenisloos
#  voor een kliniekeigenaar; ze tellen wel mee in de interne score.)
_CHECK_PHRASING: dict[str, dict[str, str]] = {
    "automations": {
        "online_booking": "laat bezoekers direct online een afspraak inplannen",
        "chatbot": "heeft een chatvenster voor directe vragen",
        "whatsapp": "biedt een WhatsApp-knop",
        "phone_clickable": "heeft een telefoonnummer dat op mobiel direct aantikbaar is",
        "cta_above_fold": "zet een duidelijke actieknop meteen bovenaan de pagina",
    },
    "website": {
        # https/schema/sitemap bewust WEG: hygiëne, geen verkoopargument. Alleen
        # laadsnelheid opent echt een gesprek ("traag op mobiel → klanten haken af").
        "pagespeed_mobile": "laadt op een telefoon binnen enkele seconden",
        "pagespeed_desktop": "laadt op desktop vlot",
    },
}
# "obv XYZ" — waar we op vergeleken (per as).
_CRITERIA_LABEL: dict[str, str] = {
    "website": "hoe snel de site laadt op mobiel en desktop",
    "automations": "online kunnen boeken, een chatoptie en een WhatsApp-knop",
}
_SECTOR_LABEL: dict[str, str] = {
    "cosmetische_behandelaars": "cosmetische praktijken",
    "alternatieve_geneeskunde": "praktijken",
    "chiropractoren": "chiropractie-praktijken",
}


def _checks(details: Any) -> dict[str, bool]:
    """Trek {check: passed} uit een *_details-veld (vorm: {'details': [{check,passed}...]})."""
    out: dict[str, bool] = {}
    if isinstance(details, dict):
        for c in details.get("details") or []:
            if isinstance(c, dict) and c.get("check"):
                out[c["check"]] = bool(c.get("passed"))
    return out


def _peer_rows(db, workspace_id: str, sector: str, city: str | None, lead_id: str,
               score_col: str, details_col: str) -> list[dict]:
    q = db.table("leads").select("id").eq("workspace_id", workspace_id).eq("sector", sector)
    if city:
        q = q.eq("city", city)
    ids = [l["id"] for l in (q.execute().data or []) if l.get("id") != lead_id]
    rows: list[dict] = []
    for i in range(0, len(ids), 200):  # .in_() met te veel UUIDs → URL te lang (les 2026-08)
        rows += (db.table("website_intelligence").select(f"lead_id, {score_col}, {details_col}")
                 .eq("workspace_id", workspace_id).in_("lead_id", ids[i:i + 200])
                 .gt(score_col, 0).execute().data or [])
    return rows


def _top_gap(axis: str, lead_details: Any, peers: list[dict], details_col: str) -> dict | None:
    """De feature die de meeste peers WEL hebben en de lead NIET — de 'wat opviel'."""
    lead_checks = _checks(lead_details)
    phrasing = _CHECK_PHRASING.get(axis, {})
    best: dict | None = None
    for check, phrase in phrasing.items():
        if lead_checks.get(check):          # lead heeft 'm al → geen gat
            continue
        have = [p for p in peers if check in _checks(p.get(details_col))]
        if len(have) < _MIN_PEERS:
            continue
        prev = sum(1 for p in have if _checks(p.get(details_col))[check]) / len(have)
        if prev >= _MIN_PREVALENCE and (best is None or prev > best["peer_pct"]):
            best = {"check": check, "peer_pct": round(prev, 2), "phrasing": phrase}
    return best


def _one_axis(db, workspace_id: str, lead_id: str, sector: str, city: str | None,
              own_score: float, wi: dict, score_col: str, details_col: str, axis: str) -> dict | None:
    scope, rows = "stad", (_peer_rows(db, workspace_id, sector, city, lead_id, score_col, details_col) if city else [])
    if len(rows) < _MIN_PEERS:
        scope, rows = "sector", _peer_rows(db, workspace_id, sector, None, lead_id, score_col, details_col)
    if len(rows) < _MIN_PEERS:
        return None
    scores = [r[score_col] for r in rows if r.get(score_col) is not None]
    avg = sum(scores) / len(scores) if scores else 0.0
    return {
        "scope": scope, "city": city, "sector": sector,
        "peer_count": len(rows), "market_avg": round(avg, 1),      # intern
        "score_vs_market": round(own_score - avg, 1),               # intern
        "top_gap": _top_gap(axis, wi.get(details_col), rows, details_col),  # prospect-facing
    }


def compute_benchmarks(db, workspace_id: str, lead_id: str, wi: dict) -> dict[str, dict | None]:
    """{'website': {...}|None, 'automations': {...}|None} — numeriek (intern) + top_gap (pitch)."""
    lead = (db.table("leads").select("sector, city")
            .eq("id", lead_id).eq("workspace_id", workspace_id).limit(1).execute().data or [None])[0]
    out: dict[str, dict | None] = {ax: None for ax in AXES}
    if not lead or not lead.get("sector"):
        return out
    sector, city = lead["sector"], lead.get("city")
    for ax, (score_col, details_col) in AXES.items():
        own = wi.get(score_col)
        if own and own > 0:
            out[ax] = _one_axis(db, workspace_id, lead_id, sector, city, own, wi, score_col, details_col, ax)
    return out


def benchmark_pitch(axis: str, bench: dict | None) -> str | None:
    """Deterministische, criteria-gegronde pitch-zin — nooit een kaal getal.

    Vorm: 'We legden jullie site naast {N} {sector} in {stad} en keken naar {criteria}.
    {X}% daarvan {doet Y} — bij jullie zagen we dat (nog) niet.' None als er geen
    concrete differentiator is (lead doet alles goed, of te weinig peers)."""
    if not bench or not bench.get("top_gap"):
        return None
    gap = bench["top_gap"]
    n = bench["peer_count"]
    sector = _SECTOR_LABEL.get(bench.get("sector"), "vergelijkbare praktijken")
    waar = f"in {bench['city']}" if bench["scope"] == "stad" and bench.get("city") else "in de regio"
    criteria = _CRITERIA_LABEL.get(axis, "een aantal vaste punten")
    pct = round(gap["peer_pct"] * 100)
    return (f"We legden jullie site naast {n} {sector} {waar} en keken naar {criteria}. "
            f"{pct}% daarvan {gap['phrasing']} — bij jullie zagen we dat (nog) niet.")


def enrich_wi_with_benchmarks(db, workspace_id: str, lead_id: str, wi: dict) -> dict:
    """In-place: market_benchmark (website) + automations_benchmark + de 2 pitch-zinnen.
    Backward-compat: score_vs_market blijft de website-as. Fail-soft."""
    try:
        b = compute_benchmarks(db, workspace_id, lead_id, wi)
    except Exception:
        return wi
    if b.get("website"):
        wi["market_benchmark"] = b["website"]
        wi["score_vs_market"] = b["website"]["score_vs_market"]
        wi["market_pitch"] = benchmark_pitch("website", b["website"])
    if b.get("automations"):
        wi["automations_benchmark"] = b["automations"]
        wi["automations_pitch"] = benchmark_pitch("automations", b["automations"])
    return wi
