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

# Gratis, zekere rechtspersoon-afleiding uit bedrijfsnaam/footer (Sami 2026-07-25,
# zonder betaalde KvK-API). POSITIEF-ONLY: "B.V."/"N.V."/besloten vennootschap =
# rechtspersoon; afwezigheid zegt NIETS (een BV zet 'BV' niet altijd in de handels-
# naam) → dan blijft het onbepaald, nooit 'natuurlijk_persoon'. Streng: standalone
# BV/B.V.-token, niet een substring.
_RECHTSPERSOON_SIGNAL_RE = re.compile(
    r"\bB\.?V\.?\b|besloten vennootschap|\bN\.?V\.?\b|naamloze vennootschap|"
    r"\bco[öo]peratie\b|\bcoöperatief\b|\bB\.?V\.?\s*i\.?o\.?\b")


def derive_rechtspersoon(text: str | None) -> str | None:
    """Return 'rechtspersoon' als de tekst (bedrijfsnaam of footer) een B.V./N.V.-
    signaal draagt, anders None (= niet af te leiden, NIET natuurlijk persoon).
    Hoofdlettergevoelig voor de kale 'BV'/'NV'-token om ruis te beperken."""
    if not text:
        return None
    return "rechtspersoon" if _RECHTSPERSOON_SIGNAL_RE.search(text) else None


def classify_legal_form(lead: dict, page_text: str | None = None) -> str:
    """'rechtspersoon' | 'natuurlijk_persoon' | 'onbepaald'.

    Bronnen, in volgorde: (1) kvk_legal_form (handmatig ingevoerd of ooit via API),
    (2) gratis B.V.-afleiding uit de bedrijfsnaam, (3) idem uit de meegegeven footer/
    page_text. Leeg/onbekend → 'onbepaald'. Fail-closed: nooit optimistisch."""
    raw = (lead.get("kvk_legal_form") or "").strip()
    if raw:
        if _NATUURLIJK_RE.search(raw):
            return "natuurlijk_persoon"
        if _RECHTSPERSOON_RE.search(raw):
            return "rechtspersoon"
        return "onbepaald"
    # Geen expliciete rechtsvorm → probeer de gratis, zekere B.V.-afleiding.
    if derive_rechtspersoon(lead.get("company_name")) or derive_rechtspersoon(page_text):
        return "rechtspersoon"
    return "onbepaald"


def receptie_avg_safe(lead: dict, page_text: str | None = None) -> tuple[bool, str]:
    """Cold-mail-gate voor de receptie-campagne (AVG-02). Alleen een bevestigde
    rechtspersoon is veilig; natuurlijk persoon vereist opt-in; onbepaald wordt
    (conservatief, policy onder voorbehoud) geblokkeerd tot de rechtsvorm bekend is.

    `page_text` (optioneel) laat de gratis B.V.-afleiding ook de footer meenemen.
    Returns (ok, reason). Bewust GEEN raise: de send-gate leest de reason."""
    lf = classify_legal_form(lead, page_text=page_text)
    if lf == "rechtspersoon":
        return True, "rechtspersoon"
    if lf == "natuurlijk_persoon":
        return False, "natuurlijk_persoon_opt_in_vereist"
    return False, "rechtsvorm_onbepaald"
