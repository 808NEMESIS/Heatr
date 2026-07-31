"""tests/test_sequences_templates_endpoint.py — GET /sequences/templates mag niet 500'en.

Regressie-vanger (UI-audit): de handler las `t["segment"]` hard, maar de v3_1-brug-
templates missen die key → KeyError → HTTP 500 → CampagneLaunch template-picker leeg.
Deze test pint dat het endpoint 200 geeft met álle templates (incl. v3_1) en dat een
ontbrekende optionele key geen crash meer geeft.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_templates_endpoint_ok():
    from fastapi.testclient import TestClient
    import api.main as m

    resp = TestClient(m.app).get("/sequences/templates")
    assert resp.status_code == 200, resp.text
    templates = resp.json()["templates"]
    ids = {t["id"] for t in templates}
    # de v3_1-brug-templates (die 'segment' missen) moeten meekomen zonder 500
    assert {"v3_1_website", "v3_1_workflow", "v3_1_ai_audit"} <= ids
    # 'segment' mag None zijn, maar de key hoort er te zijn voor elke template
    assert all("segment" in t for t in templates)
