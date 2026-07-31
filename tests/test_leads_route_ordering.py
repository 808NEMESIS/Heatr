"""tests/test_leads_route_ordering.py — literale /leads/<naam>-routes vóór /leads/{lead_id}.

Regressie-vanger. FastAPI matcht routes in registratie-volgorde. Stond de parametrische
`/leads/{lead_id}` vóór een literale route als `/leads/recontact-ready-signals`, dan ving
get_lead die af met lead_id="recontact-ready-signals" → .maybe_single() op 0 rijen → 500.
Deze test pint dat elke literale enkelsegment /leads/<naam>-GET-route eerder geregistreerd
staat dan de parametrische route.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Literale routes die anders door /leads/{lead_id} worden afgevangen.
_LITERALS = ["/leads/recontact-ready", "/leads/recontact-ready-signals"]


def _get_index(routes, path: str) -> int:
    for i, r in enumerate(routes):
        if getattr(r, "path", None) == path and "GET" in getattr(r, "methods", set()):
            return i
    raise AssertionError(f"GET-route niet gevonden: {path}")


def test_literal_leads_routes_precede_parametric():
    import api.main as m

    routes = m.app.router.routes
    param_idx = _get_index(routes, "/leads/{lead_id}")
    for literal in _LITERALS:
        assert _get_index(routes, literal) < param_idx, (
            f"{literal} staat NA /leads/{{lead_id}} → wordt afgevangen (500). "
            "Verplaats de route omhoog."
        )
