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

# Subcategorie-prioriteit voor cosmetische_behandelaars: medisch/esthetisch eerst
# (beste ICP-fit + laagste kosten per lead volgens cost-attribution), volume-beauty laatst.
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
