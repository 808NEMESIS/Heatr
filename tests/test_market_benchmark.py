"""tests/test_market_benchmark.py — score_vs_market uit eigen data, 2 assen + zinnen.

Pint scoring.market_benchmark: website (total_score) + automations (conversion_score),
sector×stad met sector-fallback <3 peers, lead zelf uitgesloten, en de deterministische
NL-zinnen (alleen bij een echte achterstand).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scoring.market_benchmark import benchmark_sentence, compute_benchmarks


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


def _store(city_peers=4, total=50, conv=5):
    leads = [{"id": "L0", "workspace_id": "aerys", "sector": "cosmetische_behandelaars", "city": "Breda"}]
    wi = []
    for i in range(city_peers):
        leads.append({"id": f"C{i}", "workspace_id": "aerys", "sector": "cosmetische_behandelaars", "city": "Breda"})
        wi.append({"lead_id": f"C{i}", "workspace_id": "aerys", "total_score": total + i, "conversion_score": conv + i})
    return {"leads": leads, "website_intelligence": wi}


def test_both_axes_city_scope():
    db = _FakeDB(_store(city_peers=4, total=50, conv=10))     # website avg 51.5 / auto avg 11.5
    b = compute_benchmarks(db, "aerys", "L0", {"total_score": 30, "conversion_score": 4})
    assert b["website"]["scope"] == "stad" and b["website"]["score_vs_market"] == -21.5
    assert b["automations"]["scope"] == "stad" and b["automations"]["score_vs_market"] == -7.5
    assert b["website"]["peer_count"] == 4


def test_axis_skipped_when_own_score_zero():
    db = _FakeDB(_store())
    b = compute_benchmarks(db, "aerys", "L0", {"total_score": 30, "conversion_score": 0})
    assert b["website"] is not None
    assert b["automations"] is None            # geen eigen conversion_score → geen as


def test_sentences_only_on_deficit():
    # website onder de markt → zin; automations boven de markt → geen zin
    wb = {"scope": "stad", "city": "Breda", "sector": "x", "peer_count": 12, "market_avg": 60, "score_vs_market": -25}
    ab = {"scope": "sector", "city": None, "sector": "x", "peer_count": 40, "market_avg": 8, "score_vs_market": 3}
    s = benchmark_sentence("website", wb)
    assert s and "25 punten onder" in s and "12 vergelijkbare praktijken in Breda" in s
    assert benchmark_sentence("automations", ab) is None      # gelijk/beter → geen pitch
    a2 = benchmark_sentence("automations", {**ab, "score_vs_market": -6})
    assert a2 and "online afspraken, chat en WhatsApp" in a2 and "in de sector" in a2


def test_none_when_too_few_peers():
    db = _FakeDB(_store(city_peers=2))          # <3 stad én <3 sector
    b = compute_benchmarks(db, "aerys", "L0", {"total_score": 30, "conversion_score": 4})
    assert b["website"] is None and b["automations"] is None
