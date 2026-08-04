"""tests/test_market_benchmark.py — marktvergelijking: 2 assen + criteria-gegronde pitch.

Pint scoring.market_benchmark: numerieke as (intern) + top_gap op peer-feature-prevalentie
(prospect-facing), en de deterministische criteria-zin (nooit een kaal getal).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scoring.market_benchmark import benchmark_pitch, compute_benchmarks


class _Chain:
    def __init__(self, table, store):
        self.table_name, self.store, self.filters = table, store, {}

    def select(self, *a, **k): return self
    def limit(self, *a): return self
    def gt(self, col, val): self.filters["gt_" + col] = val; return self
    def eq(self, col, val): self.filters[col] = val; return self
    def in_(self, col, vals): self.filters["in_" + col] = list(vals); return self

    def execute(self):
        out = []
        for r in self.store.get(self.table_name, []):
            ok = True
            for k, v in self.filters.items():
                if k.startswith("in_"):
                    ok &= r.get(k[3:]) in v
                elif k.startswith("gt_"):
                    ok &= (r.get(k[3:]) or 0) > v
                else:
                    ok &= r.get(k) == v
            if ok:
                out.append(r)
        return type("R", (), {"data": out})()


class _FakeDB:
    def __init__(self, store): self.store = store
    def table(self, name): return _Chain(name, self.store)


def _conv(booking, chatbot):
    return {"details": [{"check": "online_booking", "passed": booking},
                        {"check": "chatbot", "passed": chatbot}]}


def _store(peers_with_booking=8, peers_total=10):
    """peers_total cosmetische peers in Breda; 'peers_with_booking' hebben online_booking."""
    leads = [{"id": "L0", "workspace_id": "aerys", "sector": "cosmetische_behandelaars", "city": "Breda"}]
    wi = []
    for i in range(peers_total):
        leads.append({"id": f"C{i}", "workspace_id": "aerys", "sector": "cosmetische_behandelaars", "city": "Breda"})
        wi.append({"lead_id": f"C{i}", "workspace_id": "aerys",
                   "total_score": 55, "conversion_score": 12,
                   "conversion_details": _conv(i < peers_with_booking, False),
                   "technical_details": {"details": []}})
    return {"leads": leads, "website_intelligence": wi}


def test_top_gap_finds_majority_feature_lead_lacks():
    db = _FakeDB(_store(peers_with_booking=8, peers_total=10))   # 80% heeft booking
    # lead heeft GEEN booking:
    wi = {"total_score": 30, "conversion_score": 4, "conversion_details": _conv(False, False),
          "technical_details": {"details": []}}
    b = compute_benchmarks(db, "aerys", "L0", wi)
    gap = b["automations"]["top_gap"]
    assert gap and gap["check"] == "online_booking" and gap["peer_pct"] == 0.8


def test_pitch_is_criteria_grounded_not_a_number():
    b = {"scope": "stad", "city": "Breda", "sector": "cosmetische_behandelaars",
         "peer_count": 10, "market_avg": 12, "score_vs_market": -8,
         "top_gap": {"kind": "feature", "check": "online_booking", "peer_pct": 0.8,
                     "phrasing": "laat bezoekers direct online een afspraak inplannen"}}
    s = benchmark_pitch("automations", b)
    assert s and "10 cosmetische praktijken in Breda" in s
    assert "80% daarvan laat bezoekers direct online een afspraak inplannen" in s
    assert "punten" not in s          # géén kaal getal richting de prospect


def test_no_gap_when_lead_has_the_feature():
    db = _FakeDB(_store(peers_with_booking=8, peers_total=10))
    wi = {"total_score": 30, "conversion_score": 10, "conversion_details": _conv(True, False),
          "technical_details": {"details": []}}
    b = compute_benchmarks(db, "aerys", "L0", wi)
    assert b["automations"]["top_gap"] is None          # lead heeft booking al
    assert benchmark_pitch("automations", b["automations"]) is None


def test_website_visual_percentile_pitch():
    # 10 peers met hoge visual_score; lead laag → 'oogt verouderder dan X%'
    leads = [{"id": "L0", "workspace_id": "aerys", "sector": "cosmetische_behandelaars", "city": "Breda"}]
    wi = []
    for i in range(10):
        leads.append({"id": f"C{i}", "workspace_id": "aerys", "sector": "cosmetische_behandelaars", "city": "Breda"})
        wi.append({"lead_id": f"C{i}", "workspace_id": "aerys", "total_score": 55,
                   "visual_score": 18, "technical_details": {"details": []}})
    db = _FakeDB({"leads": leads, "website_intelligence": wi})
    b = compute_benchmarks(db, "aerys", "L0", {"total_score": 30, "visual_score": 6, "technical_details": {"details": []}})
    gap = b["website"]["top_gap"]
    assert gap and gap["kind"] == "visual" and gap["peer_pct"] == 1.0   # alle 10 moderner
    s = benchmark_pitch("website", b["website"])
    assert s and "uitstraling" in s and "verouderder dan 100%" in s and "punten" not in s


def test_no_pitch_when_feature_is_minority():
    db = _FakeDB(_store(peers_with_booking=3, peers_total=10))   # maar 30% heeft booking
    wi = {"total_score": 30, "conversion_score": 4, "conversion_details": _conv(False, False),
          "technical_details": {"details": []}}
    b = compute_benchmarks(db, "aerys", "L0", wi)
    assert b["automations"]["top_gap"] is None          # geen meerderheid → geen differentiator
