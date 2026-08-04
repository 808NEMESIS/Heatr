"""config/cities.py — doelsteden voor de discovery-sweep (stroom 1, niche-dekking).

Data-config, geen logica: de sweep-generator en dekkingsmeter lezen deze lijst;
overal overschrijfbaar via --cities. Volgorde = prioriteit (inwonertal ≈ marktomvang).
"""

NL_TOP_CITIES: list[str] = [
    "Amsterdam", "Rotterdam", "Den Haag", "Utrecht", "Eindhoven",
    "Groningen", "Tilburg", "Almere", "Breda", "Nijmegen",
    "Apeldoorn", "Arnhem", "Haarlem", "Enschede", "Amersfoort",
    "Zaanstad", "Den Bosch", "Zwolle", "Leiden", "Maastricht",
    "Dordrecht", "Ede", "Alphen aan den Rijn", "Leeuwarden", "Alkmaar",
    "Emmen", "Delft", "Venlo", "Deventer", "Hilversum",
]

# Subcategorie-prioriteit per sector (medisch/erkend eerst = beste ICP-fit). Sectoren
# zonder entry sweept de generator in config-volgorde. Cosmetisch: volume-beauty laatst.
COSMETISCH_SUBCAT_PRIORITY: list[str] = [
    "injectables_anti_aging",
    "laser_huidverjonging",
    "plastisch_chirurgen_esthetisch",
    "medische_huidtherapie",
    "haartransplantatie",
    "bodycontouring",
    "ontharing",
    "cosmetische_tandheelkunde",
    "permanente_cosmetiek",
    "schoonheidssalons",
    "nagel_wimper",
]

# Alt-zorg: RBCZ/vereniging-erkende, vergoedbare disciplines eerst (grootste kans op een
# echte praktijk met website); holistisch coaching (niet-erkend) laatst.
ALT_SUBCAT_PRIORITY: list[str] = [
    "manueel_fysiek",          # osteopathie/craniosacraal — vaak vergoed
    "natuurgeneeskunde",       # homeopathie/orthomoleculair/mesologie — NVKH/MBOG
    "lichaam_energetisch",     # acupunctuur — NVA/ZHONG
    "lichaamsgerichte_therapie",  # haptotherapie
    "vaktherapie",             # VIT-vaktherapeuten
    "reflexzone",
    "integratieve_geest_lichaam",
    "coaching_holistisch",     # niet-erkend → laatst
]

SUBCAT_PRIORITY: dict[str, list[str]] = {
    "cosmetische_behandelaars": COSMETISCH_SUBCAT_PRIORITY,
    "alternatieve_geneeskunde": ALT_SUBCAT_PRIORITY,
}
