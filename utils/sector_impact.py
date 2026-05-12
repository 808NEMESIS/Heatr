"""
utils/sector_impact.py — Resolver voor v3.2 token {{sector_impact_frame}}.

Levert een sector-passende impact-frase voor v3.2 Mail 1's. De frase wordt
ingebed in: "het viel me op dat [bedrijfsnaam] [sector_impact_frame]" — moet
dus grammaticaal werken na een bedrijfsnaam-onderwerp.

Mapping accepteert zowel generieke keys (zoals user-spec opgaf) als de feitelijke
heatr_leads.sector-waardes (cosmetische_behandelaars / alternatieve_geneeskunde),
zodat de mapping triggert op echte DB-data zonder extra alias-resolver.
"""
from __future__ import annotations


_DEFAULT = "waarde levert aan jullie klanten"

_MAPPING: dict[str, str] = {
    # Generieke sector-keys (zoals user-spec)
    "cosmetisch":                "patiënten goed verder helpt",
    "zorg_welzijn":              "patiënten goed verder helpt",
    "alternatieve_zorg":         "mensen ondersteunt in hun herstel",
    "lichaamswerk_pragmatisch":  "mensen ondersteunt in hun herstel",
    "techniek_ambacht":          "vakwerk levert waar mensen op rekenen",
    "zakelijke_dienstverlening": "ondernemers verder helpt met vraagstukken die ertoe doen",
    # DB-sector-aliases — zelfde output, zorgt dat live leads matchen
    "cosmetische_behandelaars":  "patiënten goed verder helpt",
    "alternatieve_geneeskunde":  "mensen ondersteunt in hun herstel",
}


def pick_sector_impact_frame(sector: str | None) -> str:
    """Returns een sector-passende impact-frase voor v3.2 Mail 1.

    Format: "[bedrijfsnaam] {return_value}" → werkt grammaticaal in zin
    "het viel me op dat [bedrijfsnaam] [return_value]".

    Onbekende sectoren of None → fallback. Geen crash op rare input.
    """
    if not sector:
        return _DEFAULT
    return _MAPPING.get(sector.strip(), _DEFAULT)
