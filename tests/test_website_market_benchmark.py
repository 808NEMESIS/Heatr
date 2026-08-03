"""tests/test_website_market_benchmark.py — score_vs_market v1 uit eigen data.

Pint de benchmark-logica van GET /leads/{id}/website (api.main._website_market_benchmark):
zelfde sector × stad, min. 3 peers, anders sector-breed (gelabeld), lead zelf uitgesloten,
score_vs_market = total_score − marktgemiddelde.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class _Chain:
    def __init__(self, table, store):
        self.table_name, self.store = table, store
        self.filters: dict = {}

    def select(self, *a, **k): return self
    def limit(self, *a): return self
    def gt(self, col, val): self.filters["gt_" + col] = val; return self

    def eq(self, col, val):
        self.filters[col] = val
        return self

    def in_(self, col, vals):
        self.filters["in_" + col] = list(vals)
        return self

    def execute(self):
        rows = self.store.get(self.table_name, [])
        out = []
        for r in rows:
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


def _store(city_peers: int, sector_extra: int = 0):
    leads = [{"id": "L0", "workspace_id": "aerys", "sector": "cosmetische_behandelaars", "city": "Breda"}]
    wi = []
    for i in range(city_peers):
        leads.append({"id": f"C{i}", "workspace_id": "aerys", "sector": "cosmetische_behandelaars", "city": "Breda"})
        wi.append({"lead_id": f"C{i}", "workspace_id": "aerys", "total_score": 50 + i})
    for i in range(sector_extra):
        leads.append({"id": f"S{i}", "workspace_id": "aerys", "sector": "cosmetische_behandelaars", "city": "Elders"})
        wi.append({"lead_id": f"S{i}", "workspace_id": "aerys", "total_score": 60})
    return {"leads": leads, "website_intelligence": wi}


def test_city_benchmark_with_enough_peers():
    from api.main import _website_market_benchmark
    db = _FakeDB(_store(city_peers=4))                     # scores 50,51,52,53 → avg 51.5
    b = _website_market_benchmark(db, "aerys", "L0", 30)
    assert b and b["scope"] == "stad" and b["peer_count"] == 4
    assert b["market_avg"] == 51.5 and b["score_vs_market"] == -21.5


def test_sector_fallback_below_three_city_peers():
    from api.main import _website_market_benchmark
    db = _FakeDB(_store(city_peers=2, sector_extra=3))     # stad n=2 → sector-breed n=5
    b = _website_market_benchmark(db, "aerys", "L0", 70)
    assert b and b["scope"] == "sector" and b["peer_count"] == 5
    assert b["score_vs_market"] > 0                        # 70 boven het gemiddelde


def test_none_when_no_peers():
    from api.main import _website_market_benchmark
    db = _FakeDB(_store(city_peers=0))
    assert _website_market_benchmark(db, "aerys", "L0", 40) is None
