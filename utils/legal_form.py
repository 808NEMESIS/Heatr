"""
utils/legal_form.py — rechtsvorm-risico voor koude B2B-mail (AVG-02).

Audit-bevinding (2026-07-24): voor natuurlijke personen (zzp/eenmanszaak, groot
deel van de ICP: 1-15 medewerkers, eigenaar=beslisser) is koude e-mail zonder
toestemming opt-in-plichtig (Telecommunicatiewet art. 11.7 lid 1). De published-
address-uitzondering is een rechtsPERSOON-concept en dekt eenmanszaak/zzp niet.
De bestaande gdpr_safe-heuristiek kent de rechtsvorm niet en zet role-/onbekende
adressen default op veilig — precies het gat.

Deze module maakt de rechtsvorm EXPLICIET i.p.v. default-veilig. Aparte gate voor
de receptie-campagne; raakt de globale gdpr_safe NIET (bestaande 493-flow blijft).

POLICY-VOORBEHOUD: dat 'onbepaald' blokkeert is Sami's uitgangspunt ("blokkeer
tot de rechtsvorm bekend is") en staat onder juridisch advies. KvK is opt-in UIT
(CLAUDE.md), dus kvk_legal_form is doorgaans leeg → vrijwel alles is 'onbepaald'
→ de receptie-send blijft dicht tot KvK-data bestaat óf het advies dit versoepelt.
"""
from __future__ import annotations

import re

# Eenmanszaak/zzp/VOF/maatschap = natuurlijk persoon (geen aparte rechtspersoon).
_NATUURLIJK_RE = re.compile(
    r"eenmanszaak|zzp|zelfstandige|vennootschap onder firma|\bvof\b|maatschap|"
    r"commanditaire vennootschap|\bcv\b|natuurlijk persoon", re.IGNORECASE)
# Rechtspersonen: BV/NV/stichting/coöperatie/vereniging.
_RECHTSPERSOON_RE = re.compile(
    r"besloten vennootschap|\bb\.?v\.?\b|naamloze vennootschap|\bn\.?v\.?\b|"
    r"stichting|co[öo]peratie|vereniging|holding|rechtspersoon", re.IGNORECASE)


def classify_legal_form(lead: dict) -> str:
    """'rechtspersoon' | 'natuurlijk_persoon' | 'onbepaald' uit kvk_legal_form.

    Leeg/onbekend → 'onbepaald' (KvK opt-in uit → rechtsvorm niet vastgesteld).
    Fail-closed: bij twijfel 'onbepaald', nooit optimistisch 'rechtspersoon'."""
    raw = (lead.get("kvk_legal_form") or "").strip()
    if not raw:
        return "onbepaald"
    if _NATUURLIJK_RE.search(raw):
        return "natuurlijk_persoon"
    if _RECHTSPERSOON_RE.search(raw):
        return "rechtspersoon"
    return "onbepaald"


def receptie_avg_safe(lead: dict) -> tuple[bool, str]:
    """Cold-mail-gate voor de receptie-campagne (AVG-02). Alleen een bevestigde
    rechtspersoon is veilig; natuurlijk persoon vereist opt-in; onbepaald wordt
    (conservatief, policy onder voorbehoud) geblokkeerd tot de rechtsvorm bekend is.

    Returns (ok, reason). Bewust GEEN raise: de send-gate leest de reason."""
    lf = classify_legal_form(lead)
    if lf == "rechtspersoon":
        return True, "rechtspersoon"
    if lf == "natuurlijk_persoon":
        return False, "natuurlijk_persoon_opt_in_vereist"
    return False, "rechtsvorm_onbepaald"
