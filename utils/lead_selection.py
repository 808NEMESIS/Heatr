"""utils/lead_selection.py — lead-selectie-uitsluitingen ONAFHANKELIJK van fetchkwaliteit.

Deze regels horen in de selectie, niet in de fetch-/vers-gate: een beroepsvereniging
blijft een beroepsvereniging ook als de conversion-meting slaagt, en een uit de
bedrijfsnaam afgeleide voornaam blijft afgeleid. Zonder deze laag komen ze terug zodra
hun meting wél lukt (Sami 2026-08-12, na het meetrapport dat KVHN/De AVIG en de
afgeleide namen Ping/Hero/Jan blootlegde).

BEKEND HIAAT (bewust, geen stille naad): de generieke naamregel vangt 'X Vereniging' /
'Koepel Y', maar NIET een acroniem-naam als 'De AVIG' — die staat daarom expliciet in
`_EXPLICIT`. Een volgende acroniem-vereniging of nieuwe afgeleide naam die de regel niet
vangt, moet handmatig worden toegevoegd; automatische detectie daarvan is onbetrouwbaar
(zou eponieme praktijken als 'Jeannette Hessels' meepakken). Bij twijfel: niet in de pool.
"""
from __future__ import annotations

import re

# Generieke associatie-/koepelsignalen in de bedrijfsnaam.
_ASSOC_NAME = ("verenig", "koepel", "brancheorganisat", "branchevereniging",
               "federatie", "genootschap", "belangenvereniging", "beroepsorganisat")
# Rechtsvorm-signalen (alleen bruikbaar als KvK bekend is; meestal None).
_ASSOC_LEGAL = ("vereniging", "coöperat", "cooperat")
# Entiteiten die de generieke regel NIET vangt (acroniem/merk) — expliciet uitgesloten.
# Sleutel = genormaliseerde bedrijfsnaam.
_EXPLICIT = {
    "de avig": "beroepsvereniging",                 # 'Artsen Vereniging Integrale Geneeskunde' (acroniem)
    "ping shu yuan": "voornaam_uit_bedrijfsnaam",    # 'Ping' uit klinieknaam, geen persoon
    "cosmetic heroes": "voornaam_uit_bedrijfsnaam",  # 'Hero' uit 'Heroes'
    "dokter jan": "voornaam_uit_bedrijfsnaam",       # 'Jan' = merk, site zegt 'team van specialisten'
    # entiteit-onbepaald (meetrapport): niet vastgesteld dat een individuele patiënt
    # er een afspraak maakt, terwijl het frame daar volledig op leunt (Sami 2026-08-12).
    "voedingbewegingpsyche": "entiteit_onbepaald",   # coaching/groepsprogramma-merk
    "praktijk chanti ohm": "entiteit_onbepaald",     # energiewerk/events
}


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def selection_exclusion(lead: dict) -> str | None:
    """Return een uitsluitingsreden als deze lead NIET in de pool hoort (beroepsvereniging
    / afgeleide voornaam), anders None. Onafhankelijk van fetch/meting."""
    nm = lead.get("company_name") or ""
    if _norm(nm) in _EXPLICIT:
        return _EXPLICIT[_norm(nm)]
    low = nm.lower()
    if any(k in low for k in _ASSOC_NAME):
        return "beroepsvereniging"
    lf = (lead.get("kvk_legal_form") or "").lower()
    if any(k in lf for k in _ASSOC_LEGAL):
        return "beroepsvereniging"
    return None
