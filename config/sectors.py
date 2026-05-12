"""
Sector configuraties voor Heatr.

Elke sector definieert:
- sbi_codes: SBI 2025 codes (5-cijferig, herzien sep 2025) voor KvK filtering
- subcategories: subcategorieen binnen de sector voor gerichte scraping + ICP matching
- lead_keywords: Google Maps zoektermen (globaal, aanvullend op subcategorie keywords)
- disqualifiers: keywords die een lead uitsluiten (bv. reguliere zorg, ziekenhuizen)
- website_signals: sector-specifieke signalen voor laag 4 van website intelligence (15 pnt)

SBI codes zijn sinds 5-8 sep 2025 5-cijferig. Oude 4-cijferige codes (86.90, 86.21, 96.02)
zijn niet meer geldig in het Handelsregister.

LET OP: dit bestand bevat GEEN budget-, tier- of omzet aannames. Die komen pas wanneer
we feedback data hebben uit de feedback_processor (gewonnen vs verloren leads, conversie
per subcategorie, gemiddelde dealwaarde). Tot die tijd: geen aannames over welke
subcategorie meer budget heeft.
"""
from __future__ import annotations

from typing import TypedDict


class SubcategoryConfig(TypedDict):
    label: str
    lead_keywords: list[str]
    disqualifiers: list[str]
    # Signalen die dit een ICP match maken (niet als scoring, maar als classificatie hint)
    icp_signals: list[str]


class SectorConfig(TypedDict):
    label: str
    sbi_codes: list[str]
    sbi_codes_notes: dict[str, str]
    subcategories: dict[str, SubcategoryConfig]
    lead_keywords: list[str]  # globale keywords, aanvullend op subcategorieen
    disqualifiers: list[str]  # globale disqualifiers
    website_signals: dict[str, list[str]]  # signalen voor laag 4 scoring
    notes: str


SECTORS: dict[str, SectorConfig] = {
    # =========================================================================
    # ALTERNATIEVE GENEESKUNDE
    # =========================================================================
    "alternatieve_geneeskunde": {
        "label": "Alternatieve Geneeskunde",
        "sbi_codes": [
            "86919",  # Overige paramedische praktijken en alternatieve genezers (hoofdcode)
            "86911",  # Praktijken van fysiotherapeuten
            "86912",  # Praktijken van oefentherapeuten Cesar/Mensendieck (en soms huidtherapeuten)
            "86913",  # Praktijken van psychotherapeuten, psychologen en pedagogen
            "86211",  # Huisartsen (voor arts-homeopaten, arts-acupuncturisten)
            "96099",  # Overige persoonlijke dienstverlening n.e.g. (coaches, holistisch)
        ],
        "sbi_codes_notes": {
            "86919": "Breedste code: homeopaten, natuurgeneeskundigen, niet-arts acupuncturisten, "
                     "chiropractors, reflexologen, dietisten, huidtherapeuten, massagesalons (vaak)",
            "86911": "Fysiotherapeuten - grensgebied. Meeste zijn BIG geregistreerd, regulier. "
                     "Alleen relevant als ze aanvullend alternatief werk doen (manueel, dry needling).",
            "86912": "Oefentherapeuten Cesar/Mensendieck. Soms ook huidtherapeuten hier.",
            "86913": "Psychotherapeuten, psychologen, pedagogen, vaktherapeuten (nieuw sinds herziening).",
            "86211": "Huisartsen - alleen relevant als ze alternatieve geneeswijzen aanbieden.",
            "96099": "Catch-all voor coaches/holistisch zonder medische pretentie.",
        },
        "subcategories": {
            "lichaam_energetisch": {
                "label": "Energetisch en lichaamsenergie",
                "lead_keywords": [
                    "acupunctuur praktijk",
                    "shiatsu therapie",
                    "energetisch therapeut",
                    "reiki master praktijk",
                    "bowen therapie",
                    "dorn therapie",
                    "polariteit therapie",
                ],
                "disqualifiers": ["ziekenhuis", "arts acupunctuur"],
                "icp_signals": [
                    "acupunctuur", "shiatsu", "reiki", "energetisch", "bowen",
                    "dorn", "polariteit", "meridiaan", "chakra", "aura",
                ],
            },
            "manueel_fysiek": {
                "label": "Manueel en fysiek",
                "lead_keywords": [
                    "osteopathie praktijk",
                    "chiropractor",
                    "craniosacraal therapie",
                    "manueel therapeut",
                    "rolfing",
                    "feldenkrais",
                ],
                "disqualifiers": ["ziekenhuis", "revalidatiecentrum"],
                "icp_signals": [
                    "osteopathie", "chiropractie", "craniosacraal", "cranio sacraal",
                    "rolfing", "feldenkrais", "structurele integratie", "manueel",
                ],
            },
            "natuurgeneeskunde": {
                "label": "Natuurgeneeskunde en homeopathie",
                "lead_keywords": [
                    "natuurgeneeskunde praktijk",
                    "homeopaat",
                    "orthomoleculair therapeut",
                    "fytotherapie",
                    "darmtherapeut",
                    "ayurveda praktijk",
                    "TCM therapie",
                ],
                "disqualifiers": ["apotheek", "drogist"],
                "icp_signals": [
                    "natuurgeneeskunde", "homeopathie", "orthomoleculair", "fytotherapie",
                    "darmtherapie", "ayurveda", "TCM", "traditionele chinese geneeskunde",
                    "kruidenrecepten", "kruiden",
                ],
            },
            "lichaamsgerichte_therapie": {
                "label": "Lichaamsgerichte therapie",
                "lead_keywords": [
                    "haptotherapeut",
                    "lichaamsgerichte psychotherapie",
                    "bioenergetica therapie",
                    "somatic experiencing",
                    "TRE practitioner",
                ],
                "disqualifiers": ["psychiater", "GGZ"],
                "icp_signals": [
                    "haptotherapie", "haptonomie", "lichaamsgericht", "bioenergetica",
                    "somatic experiencing", "TRE", "trauma release",
                ],
            },
            "reflexzone": {
                "label": "Reflexzone en reflexologie",
                "lead_keywords": [
                    "reflexoloog",
                    "voetreflex praktijk",
                    "reflexzone therapie",
                    "ooracupunctuur",
                ],
                "disqualifiers": [],
                "icp_signals": [
                    "reflexologie", "reflexzone", "voetreflex", "handreflex",
                    "ooracupunctuur", "zoneterapie",
                ],
            },
            "integratieve_geest_lichaam": {
                "label": "Integratieve geest-lichaam therapie",
                "lead_keywords": [
                    "mindfulness trainer",
                    "hypnotherapeut",
                    "NLP therapeut",
                    "ademtherapie",
                    "familieopstellingen",
                    "systemisch werker",
                ],
                "disqualifiers": ["psychiater", "GGZ instelling"],
                "icp_signals": [
                    "mindfulness", "hypnotherapie", "NLP", "ademtherapie", "pranayama",
                    "wim hof", "familieopstellingen", "systemisch werk",
                ],
            },
            "vaktherapie": {
                "label": "Vaktherapie",
                "lead_keywords": [
                    "kunstzinnige therapie",
                    "muziektherapie",
                    "danstherapie",
                    "psychomotore therapie",
                    "dramatherapie",
                    "speltherapie",
                ],
                "disqualifiers": [],
                "icp_signals": [
                    "vaktherapie", "kunstzinnige therapie", "muziektherapie",
                    "danstherapie", "psychomotore", "PMT", "dramatherapie", "speltherapie",
                ],
            },
            "coaching_holistisch": {
                "label": "Holistische coaching",
                "lead_keywords": [
                    "holistisch coach",
                    "spiritueel coach",
                    "energetisch coach",
                    "life coach holistisch",
                ],
                "disqualifiers": [
                    "business coach", "executive coach", "sales coach",
                    "zakelijk coach", "team coach",
                ],
                "icp_signals": [
                    "holistisch", "spiritueel", "energetisch", "zielsmissie",
                    "levenspad", "innerlijk kind", "hoogsensitief",
                ],
            },
        },
        "lead_keywords": [
            # Globale zoektermen die over alle subcategorieen heen werken
            "alternatieve geneeskunde",
            "natuurgeneeskunde",
            "complementaire zorg",
            "holistische praktijk",
        ],
        "disqualifiers": [
            "ziekenhuis", "UMC", "academisch medisch centrum",
            "GGZ instelling", "revalidatiecentrum",
            "apotheek", "drogisterij", "drogist",
            "verpleeghuis", "zorginstelling",
        ],
        "website_signals": {
            # Signalen die een website als hoogwaardige alternatieve praktijk classificeren
            "positive": [
                "RBCZ", "SRBAG", "NVA", "NVKH", "NWP",  # beroepsverenigingen
                "klachtenregeling", "AVG", "privacyverklaring",
                "vergoeding zorgverzekeraar", "aanvullende verzekering",
                "online afspraak maken", "online booking",
                "Wkkgz", "geschillencommissie",
            ],
            "negative": [
                # Signalen die twijfelachtig zijn (let op: niet per definitie slecht)
                "genezing gegarandeerd", "wondermiddel",
                "vervangt medische behandeling",
            ],
        },
        "notes": (
            "86919 is de breedste code en vangt het grootste deel. "
            "Let op dat deze code OOK dietisten, ergotherapeuten, logopedisten en "
            "huidtherapeuten bevat - filter op activiteitenomschrijving en subcategorie "
            "keywords. Gebruik disqualifiers agressief om reguliere zorg eruit te filteren."
        ),
    },

    # =========================================================================
    # COSMETISCHE BEHANDELAARS
    # =========================================================================
    "cosmetische_behandelaars": {
        "label": "Cosmetische Behandelaars",
        "sbi_codes": [
            "96022",  # Schoonheidsverzorging, pedicures, manicures, visagie, image consulting
            "86221",  # Medisch specialisten incl. plastische chirurgie (hoofdcode voor klinieken)
            "86919",  # Overige paramedische praktijken (voor huidtherapeuten)
            "86912",  # Oefentherapeuten (soms huidtherapeuten ook hier)
            "96040",  # Lichamelijk welzijn (sauna, solaria, wellness)
            "96099",  # Overige persoonlijke dienstverlening (PMU, wimpers vaak hier)
            "86230",  # Tandartsen (voor cosmetische tandheelkunde)
        ],
        "sbi_codes_notes": {
            "96022": "Hoofdcode schoonheidsverzorging. Exclusief plastische chirurgie en wellness.",
            "86221": "Medisch specialisten incl. plastisch chirurgen, cosmetisch artsen. "
                     "Borstvergroting, liposuctie, ooglidcorrectie vallen hier onder.",
            "86919": "Huidtherapeuten (paramedisch, Wet BIG) staan vaak hier. "
                     "Let op overlap met alternatieve geneeskunde - disambigueren via keywords.",
            "86912": "Soms ook huidtherapeuten (inconsistent gecodeerd bij KvK).",
            "96040": "Sauna, solaria, wellness. Niet-medische beauty.",
            "96099": "Catch-all. PMU, tattoo, permanente wimpers, nagelstudios vaak hier.",
            "86230": "Tandartsen. Alleen relevant bij expliciete cosmetische tandheelkunde focus.",
        },
        "subcategories": {
            "injectables_anti_aging": {
                "label": "Injectables en anti-aging",
                "lead_keywords": [
                    "botox kliniek",
                    "fillers behandeling",
                    "cosmetische arts",
                    "anti aging kliniek",
                    "skinbooster",
                    "mesotherapie",
                    "PRP behandeling",
                ],
                "disqualifiers": ["ziekenhuis", "spoedeisende hulp"],
                "icp_signals": [
                    "botox", "botuline", "filler", "fillers", "hyaluronzuur",
                    "skinbooster", "biostimulator", "radiesse", "sculptra",
                    "profhilo", "mesotherapie", "PRP", "vampire facial",
                    "BIG geregistreerd", "cosmetisch arts",
                ],
            },
            "laser_huidverjonging": {
                "label": "Laser en huidverjonging",
                "lead_keywords": [
                    "laserkliniek huidverjonging",
                    "IPL behandeling",
                    "fractional laser",
                    "morpheus8 behandeling",
                    "HIFU ultherapy",
                    "microneedling praktijk",
                ],
                "disqualifiers": ["laserontharing only"],
                "icp_signals": [
                    "fractional laser", "CO2 laser", "erbium laser", "IPL",
                    "morpheus8", "HIFU", "ultherapy", "radiofrequentie", "RF microneedling",
                    "microneedling", "fotorejuvenation", "huidverjonging",
                ],
            },
            "ontharing": {
                "label": "Ontharing",
                "lead_keywords": [
                    "laserontharing kliniek",
                    "definitieve ontharing",
                    "IPL ontharing",
                    "waxing salon",
                    "sugaring",
                ],
                "disqualifiers": [],
                "icp_signals": [
                    "laserontharing", "definitieve ontharing", "IPL ontharing",
                    "elektrische ontharing", "elektrolyse", "waxing", "harsen",
                    "sugaring", "threading",
                ],
            },
            "medische_huidtherapie": {
                "label": "Medische huidtherapie",
                "lead_keywords": [
                    "huidtherapeut praktijk",
                    "huidtherapie kliniek",
                    "acne behandeling huidtherapeut",
                    "littekenbehandeling",
                    "oedeemtherapie",
                ],
                "disqualifiers": ["schoonheidssalon", "wellness"],
                "icp_signals": [
                    "huidtherapeut", "huidtherapie", "NVH", "Nederlandse Vereniging Huidtherapeuten",
                    "Wet BIG huidtherapie", "paramedisch", "oedeemtherapie",
                    "acnebehandeling medisch", "littekentherapie", "couperose",
                    "pigmentstoornissen", "vergoeding huidtherapie",
                ],
            },
            "schoonheidssalons": {
                "label": "Schoonheidssalons",
                "lead_keywords": [
                    "schoonheidssalon",
                    "gezichtsbehandeling",
                    "huidverzorging salon",
                    "hydrafacial",
                    "peeling salon",
                ],
                "disqualifiers": ["huidtherapeut", "medisch", "arts"],
                "icp_signals": [
                    "schoonheidsspecialist", "ANBOS", "gezichtsbehandeling",
                    "hydrafacial", "microdermabrasie", "peeling", "collageen masker",
                    "huidanalyse", "dagcreme", "nachtcreme",
                ],
            },
            "permanente_cosmetiek": {
                "label": "Permanente make-up",
                "lead_keywords": [
                    "permanente make-up",
                    "microblading salon",
                    "PMU wenkbrauwen",
                    "lip blush",
                    "powder brows",
                    "scalp micropigmentation",
                ],
                "disqualifiers": ["tattoo shop algemeen"],
                "icp_signals": [
                    "PMU", "permanente make up", "microblading", "powder brows",
                    "lip blush", "eyeliner PMU", "scalp micropigmentation",
                    "areola tattoo", "paramedische tattoo",
                ],
            },
            "bodycontouring": {
                "label": "Body contouring",
                "lead_keywords": [
                    "cryolipolyse",
                    "coolsculpting",
                    "EMsculpt",
                    "cavitation behandeling",
                    "endermologie",
                ],
                "disqualifiers": [],
                "icp_signals": [
                    "cryolipolyse", "coolsculpting", "EMsculpt", "cavitation",
                    "endermologie", "LPG", "lymfedrainage cosmetisch",
                    "vetbevriezing", "vetreductie",
                ],
            },
            "nagel_wimper": {
                "label": "Nagel en wimper",
                "lead_keywords": [
                    "nagelstudio",
                    "wimperextensions salon",
                    "lashlift",
                    "browlift",
                    "henna brows",
                ],
                "disqualifiers": [],
                "icp_signals": [
                    "nagelstyliste", "gelnagels", "acrylnagels", "wimperextensions",
                    "lash extensions", "lashlift", "browlift", "henna brows",
                    "wenkbrauw styling",
                ],
            },
            "cosmetische_tandheelkunde": {
                "label": "Cosmetische tandheelkunde",
                "lead_keywords": [
                    "cosmetische tandarts",
                    "facings behandeling",
                    "tanden bleken praktijk",
                    "invisalign provider",
                    "composiet bonding",
                ],
                "disqualifiers": ["algemeen tandarts", "kindertandarts"],
                "icp_signals": [
                    "facings", "veneers", "tanden bleken", "composiet bonding",
                    "invisalign", "esthetische tandheelkunde", "cosmetische tandheelkunde",
                    "smile design",
                ],
            },
            "plastisch_chirurgen_esthetisch": {
                "label": "Plastische chirurgie esthetisch",
                "lead_keywords": [
                    "plastisch chirurg kliniek",
                    "ooglidcorrectie",
                    "neuscorrectie",
                    "borstvergroting",
                    "liposuctie kliniek",
                    "buikwandcorrectie",
                ],
                "disqualifiers": ["reconstructieve chirurgie only", "handchirurgie"],
                "icp_signals": [
                    "plastisch chirurg", "cosmetisch chirurg", "NVPC",
                    "ooglidcorrectie", "blepharoplastiek", "neuscorrectie", "rhinoplastiek",
                    "borstvergroting", "borstverkleining", "mammaplastiek", "liposuctie",
                    "liposculptuur", "buikwandcorrectie", "facelift",
                    "ZKN keurmerk", "ZBC", "zelfstandig behandelcentrum",
                ],
            },
            "haartransplantatie": {
                "label": "Haartransplantatie",
                "lead_keywords": [
                    "haartransplantatie kliniek",
                    "FUE haartransplantatie",
                    "PRP haarbehandeling",
                    "baardimplantaat",
                ],
                "disqualifiers": [],
                "icp_signals": [
                    "haartransplantatie", "FUE", "DHI", "PRP haar",
                    "baardtransplantatie", "wenkbrauwtransplantatie",
                    "haarverlies behandeling",
                ],
            },
        },
        "lead_keywords": [
            "cosmetische kliniek",
            "beauty kliniek",
            "huidkliniek",
            "esthetische kliniek",
        ],
        "disqualifiers": [
            "ziekenhuis", "UMC", "academisch medisch centrum",
            "spoedeisende hulp", "SEH",
            "dermatologie vergoed",  # puur medische dermatologie zonder cosmetische focus
            "oncologie", "kinderkliniek",
        ],
        "website_signals": {
            "positive": [
                "ZKN keurmerk", "NVCG", "NVPC", "NVH",  # beroepsverenigingen
                "BIG nummer", "BIG geregistreerd", "cosmetisch arts",
                "voor en na foto", "behandelresultaten", "consultatie gratis",
                "online afspraak", "whatsapp contact",
                "CE gecertificeerd", "FDA approved",
                "garantie", "nazorg",
                "financiering", "iDEAL", "klarna",
            ],
            "negative": [
                "prijzen op aanvraag only",  # geen transparantie
                "geen BIG",
            ],
        },
        "notes": (
            "Grote spread in deze sector: van solo schoonheidssalon (96022) tot plastische "
            "chirurgie kliniek (86221). Subcategorieen zijn belangrijker dan SBI codes voor "
            "ICP matching. Huidtherapeuten zitten in 86919/86912 en overlappen met "
            "alternatieve_geneeskunde - classificeer op basis van website content, niet SBI. "
            "GEEN aannames over budget of dealwaarde per subcategorie tot feedback data "
            "dit valideert."
        ),
    },
}


def get_sector(sector_key: str) -> SectorConfig:
    """Haal een sector config op."""
    if sector_key not in SECTORS:
        raise ValueError(f"Onbekende sector: {sector_key}. Beschikbaar: {list(SECTORS.keys())}")
    return SECTORS[sector_key]


def get_all_sbi_codes(sector_key: str) -> list[str]:
    """Alle SBI codes voor een sector, voor KvK filtering."""
    return get_sector(sector_key)["sbi_codes"]


def get_subcategory_keywords(sector_key: str, subcategory_key: str) -> list[str]:
    """Lead keywords voor een specifieke subcategorie, voor Google Maps scraping."""
    sector = get_sector(sector_key)
    if subcategory_key not in sector["subcategories"]:
        raise ValueError(
            f"Onbekende subcategorie {subcategory_key} voor {sector_key}. "
            f"Beschikbaar: {list(sector['subcategories'].keys())}"
        )
    return sector["subcategories"][subcategory_key]["lead_keywords"]


def classify_subcategory(sector_key: str, website_text: str) -> str | None:
    """
    Classificeer een lead in een subcategorie op basis van website tekst.
    Eenvoudige keyword matching - telt icp_signals hits per subcategorie.
    Returns subcategorie key of None als geen duidelijke match.

    Let op: dit is een simpele implementatie. Voor productie overweeg Claude Haiku
    voor betere classificatie (zie classify_subcategory_with_claude in
    website_intelligence/opportunity_classifier.py).
    """
    sector = get_sector(sector_key)
    text_lower = website_text.lower()

    scores: dict[str, int] = {}
    for subcat_key, subcat in sector["subcategories"].items():
        score = 0
        # Positieve signalen
        for signal in subcat["icp_signals"]:
            if signal.lower() in text_lower:
                score += 1
        # Disqualifiers aftrekken
        for disq in subcat["disqualifiers"]:
            if disq.lower() in text_lower:
                score -= 2
        scores[subcat_key] = score

    # Alleen teruggeven bij duidelijke winnaar (min 2 hits, en minimaal 2 hoger dan #2)
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    if not sorted_scores or sorted_scores[0][1] < 2:
        return None
    if len(sorted_scores) >= 2 and sorted_scores[0][1] - sorted_scores[1][1] < 2:
        return None
    return sorted_scores[0][0]


def list_sectors() -> list[str]:
    """Return all active sector keys."""
    return list(SECTORS.keys())
