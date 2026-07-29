"""tests/test_lead_qualifier_disqualifiers.py — pre-enrichment disqualifier-gate.

Drift-audit 2026-07-28: qualify_raw_company flatte ALLE subcategory-disqualifiers
samen met de sector-globale en rejecteerde ze via substring-match vóór lead-creatie.
Voor cosmetische_behandelaars rejecteerde dat z'n EIGEN ICP ('arts' ⊂ 'cosmetisch
arts', 'huidtherapeut'/'schoonheidssalon' = eigen subcats). Fix: alleen sector-globale
disqualifiers vuren (spiegel van scoring/icp_matcher.compute_icp_match).

Deze suite pint beide kanten: kern-ICP mag NIET meer gerejecteerd worden, en de echte
sector-brede non-ICP (ziekenhuis/apotheek/…) blijft WEL geblokkeerd.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from enrichment.lead_qualifier import qualify_raw_company

SECTOR = "cosmetische_behandelaars"
WORKSPACE = "aerys"


class _FakeQuery:
    def select(self, *a, **k):
        return self

    def eq(self, *a, **k):
        return self

    def limit(self, *a, **k):
        return self

    def execute(self):
        return type("R", (), {"data": []})()  # geen duplicaat


class _FakeSupabase:
    def table(self, *a, **k):
        return _FakeQuery()


def _qualify(raw: dict) -> tuple[bool, str, int]:
    # Zelfde patroon als de rest van de suite: hergebruik de gedeelde event-loop
    # (asyncio.run() zou 'm sluiten → "Event loop is closed" in latere async-tests).
    return asyncio.get_event_loop().run_until_complete(
        qualify_raw_company(raw, SECTOR, WORKSPACE, _FakeSupabase())
    )


def _lead(name: str, category: str = "", domain: str = "voorbeeldkliniek.nl") -> dict:
    return {"company_name": name, "domain": domain, "google_category": category}


# ── Kern-ICP mag NIET gerejecteerd worden (dit was de bug) ───────────────────
def test_cosmetisch_arts_not_rejected():
    ok, reason, _ = _qualify(_lead("Cosmetisch Arts Kliniek Utrecht", "cosmetisch arts"))
    assert ok is True, reason              # 'arts' was subcat-disq → rejecteerde eigen ICP


def test_huidtherapeut_not_rejected():
    ok, reason, _ = _qualify(_lead("Huidtherapie Praktijk Den Bosch", "huidtherapeut"))
    assert ok is True, reason              # 'huidtherapeut' = schoonheidssalons-subcat-disq


def test_schoonheidssalon_not_rejected():
    ok, reason, _ = _qualify(_lead("Schoonheidssalon Belle", "schoonheidssalon"))
    assert ok is True, reason              # 'schoonheidssalon' = medische_huidtherapie-subcat-disq


def test_medisch_esthetische_kliniek_not_rejected():
    ok, reason, _ = _qualify(_lead("Medisch Esthetische Kliniek Amsterdam", "medisch centrum"))
    assert ok is True, reason              # 'medisch' = subcat-disq, maar dat IS de ICP


# ── Echte sector-brede non-ICP blijft WÉL geblokkeerd (gate niet uitgehold) ──
# Na de merge (drift-audit 2026-07-29) is er één sector-globale lijst; apotheek/
# huisartsenpraktijk (voorheen in de dode duplicaat-key) doen nu ook echt mee.
def test_ziekenhuis_still_blocked():
    ok, reason, _ = _qualify(_lead("Ziekenhuis Sint Anna", "ziekenhuis"))
    assert ok is False and reason == "disqualifier:ziekenhuis"


def test_oncologie_still_blocked_via_category():
    ok, reason, _ = _qualify(_lead("Behandelcentrum Noord", "oncologie"))
    assert ok is False and reason == "disqualifier:oncologie"


def test_spoedeisende_hulp_still_blocked():
    ok, reason, _ = _qualify(_lead("Spoedeisende Hulp Post West"))
    assert ok is False and reason == "disqualifier:spoedeisende hulp"


def test_apotheek_still_blocked():
    # stond in de dode duplicaat-lijst; na de merge doet 'ie echt mee.
    ok, reason, _ = _qualify(_lead("Kruidvat", "apotheek"))
    assert ok is False and reason == "disqualifier:apotheek"


def test_huisartsenpraktijk_still_blocked():
    ok, reason, _ = _qualify(_lead("Huisartsenpraktijk De Brug", "huisartsenpraktijk"))
    assert ok is False and reason == "disqualifier:huisartsenpraktijk"


# ── Structurele guards tegen terugkeer van de twee gevonden bugs ─────────────
def test_no_disqualifier_is_substring_of_icp_term():
    # De ①-val: 'arts' ⊂ 'cosmetisch arts' → eigen ICP rejecten. Deze guard vangt
    # elke toekomstige disqualifier die een ICP-keyword/-signal binnendringt.
    from config.sectors import get_sector
    sc = get_sector(SECTOR)
    icp = list(sc.get("lead_keywords") or [])
    for sub in (sc.get("subcategories") or {}).values():
        icp += list(sub.get("lead_keywords") or []) + list(sub.get("icp_signals") or [])
    icp_low = [t.lower() for t in icp]
    for d in (sc.get("disqualifiers") or []):
        botsing = [t for t in icp_low if d.lower() in t]
        assert not botsing, f"disqualifier {d!r} is substring van ICP-term(en): {botsing}"


def test_sectors_py_has_no_duplicate_dict_keys():
    # De duplicate-'disqualifiers'-key-bug (292 dood → 502 live). Deze guard faalt
    # als ooit weer een dict-literal in sectors.py een sleutel dubbel definieert.
    import ast
    src = (Path(__file__).resolve().parent.parent / "config" / "sectors.py").read_text()
    dupes = []
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Dict):
            seen = set()
            for k in node.keys:
                if isinstance(k, ast.Constant) and isinstance(k.value, str):
                    if k.value in seen:
                        dupes.append((k.value, k.lineno))
                    seen.add(k.value)
    assert not dupes, f"duplicate dict-keys in sectors.py: {dupes}"
