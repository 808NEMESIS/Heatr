"""
tests/test_receptie_templates.py — receptie-haakje-copy Q4/Q7/Q2/P1 (Fase 3a).

Bewaakt de F4-eis (Sami 2026-07-24): de mail-1-observatie mag NOOIT een eerste-
persoons-tijdsclaim of geënsceneerde context bevatten (geen 'ik keek gisteravond
op mijn telefoon') — één verifieerbaar-valse claim haalt de geloofwaardige-
aandacht-basis onderuit. Plus de bestaande QA-regels: geen em-/en-dash, geen
onopgeloste tokens, klinieknaam precies één keer, geen vraag in het haakje.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.hook_templates import HOOK_VARIANTS, SIGNAL_NAMES, build_haakje

RECEPTIE_CODES = ("Q4", "Q7", "Q2", "P1")

# F4: geen geclaimde persoonlijke actie / tijdstip.
_F4_TIME_CLAIM = re.compile(
    r"gisteravond|vanavond|vanochtend|vannacht|\bik keek\b|\bik zat\b|\bik wilde\b|"
    r"\bik opende\b|\bik probeerde\b|op mijn telefoon|'s avonds op de bank",
    re.IGNORECASE,
)


def test_all_receptie_codes_present():
    for code in RECEPTIE_CODES:
        assert code in HOOK_VARIANTS and len(HOOK_VARIANTS[code]) == 2
        assert code in SIGNAL_NAMES


def test_receptie_variants_are_f4_proof_and_qa_clean():
    for code in RECEPTIE_CODES:
        for variant in ("A", "B"):
            t = build_haakje(code, kliniek="Kliniek Vrijdag", variant=variant)
            assert t, f"{code}{variant} rendert leeg"
            assert "—" not in t and "–" not in t, f"{code}{variant} bevat em/en-dash"
            assert "{" not in t and "}" not in t, f"{code}{variant} onopgeloste token"
            assert not _F4_TIME_CLAIM.search(t), f"{code}{variant} F4-tijdsclaim: {t!r}"
            assert t.count("Kliniek Vrijdag") == 1, f"{code}{variant} klinieknaam niet 1x"
            assert "?" not in t, f"{code}{variant} vraag in het haakje"


def test_receptie_variants_differ_A_vs_B():
    for code in RECEPTIE_CODES:
        a = build_haakje(code, kliniek="X Clinic", variant="A")
        b = build_haakje(code, kliniek="X Clinic", variant="B")
        assert a != b, f"{code} A en B zijn identiek"


def test_receptie_haakje_no_reviews_reference():
    # reviews horen in de zonde-brug, niet in het haakje (spreidingsregel).
    for code in RECEPTIE_CODES:
        for variant in ("A", "B"):
            t = build_haakje(code, kliniek="X", variant=variant).lower()
            assert "review" not in t and "sterren" not in t


def test_receptie_falls_back_without_kliniek():
    # zonder klinieknaam blijft de zin kloppen (geen kale token).
    t = build_haakje("Q4", kliniek=None, variant="A")
    assert t and "{" not in t
