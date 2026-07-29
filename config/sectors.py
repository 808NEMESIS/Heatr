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
    allowed_offers: list[str]  # welke Aerys-diensten de classifier mag pitchen
    notes: str


SECTORS: dict[str, SectorConfig] = {
    # =========================================================================
    # ALTERNATIEVE GENEESKUNDE
    # =========================================================================
    "alternatieve_geneeskunde": {
        "label": "Alternatieve Geneeskunde",
        # Volume-website-spel: ALLEEN website. Nooit automatisering/AI-receptie —
        # deze praktijken werken met pijnklachten + medische info (compliance).
        "allowed_offers": ["website_rebuild", "conversie_optimalisatie"],
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
                    "craniosacraal therapie",
                    "manueel therapeut",
                    "rolfing",
                    "feldenkrais",
                ],
                "disqualifiers": ["ziekenhuis", "revalidatiecentrum"],
                "icp_signals": [
                    "osteopathie", "craniosacraal", "cranio sacraal",
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
            # Globale zoektermen die over alle subcategorieen heen werken.
            # Verbreed 2026-07-20 naar de volledige alternatieve-geneeswijze-markt.
            # NB: "chiropractor" bewust NIET hier — dat blijft de aparte sector
            # 'chiropractoren' (voorkomt dubbele match).
            "alternatieve geneeskunde",
            "natuurgeneeskunde",
            "complementaire zorg",
            "holistische praktijk",
            "acupuncturist", "acupunctuur",
            "homeopaat", "homeopathie",
            "natuurgeneeskundige",
            "energetisch therapeut", "reiki",
            "hypnotherapeut", "hypnotherapie",
            "kinesioloog", "kinesiologie",
            "orthomoleculair therapeut", "orthomoleculaire geneeskunde",
            "iriscopie", "iriscopist",
            "osteopaat", "osteopathie",
            "haptonoom", "haptotherapie",
            "shiatsu", "ayurveda",
            "Chinese geneeskunde", "TCM", "traditionele Chinese geneeskunde",
            "voedingsdeskundige", "voedingscoach",
            "mindfulness coach", "meditatiecoach",
        ],
        "disqualifiers": [
            "ziekenhuis", "UMC", "academisch medisch centrum",
            "GGZ instelling", "revalidatiecentrum",
            "apotheek", "drogisterij", "drogist",
            "verpleeghuis", "zorginstelling",
        ],
        "website_signals": {
            # Generiek voor de bréde alternatieve-geneeswijze-markt (2026-07-20):
            # discipline-neutrale trust-/conversie-signalen i.p.v. fysio-specifiek.
            "positive": [
                # beroepsverenigingen/keurmerken over meerdere disciplines
                "RBCZ", "SRBAG", "NVA", "NVKH", "NWP", "SNRO", "CAT", "VBAG", "NVRT",
                # generieke vertrouwens- + compliance-signalen
                "klachtenregeling", "AVG", "privacyverklaring",
                "vergoeding zorgverzekeraar", "aanvullende verzekering",
                "Wkkgz", "geschillencommissie", "kwalificaties", "diploma", "certificaat",
                # generieke conversie-/praktijk-signalen
                "online afspraak maken", "online booking", "gratis kennismaking",
                "behandelingen", "ervaringen", "reviews",
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
        "label": "Cosmetische Klinieken",
        # Volledige funnel: website is de instap, automatisering (AI-receptie) de
        # premium upsell.
        "allowed_offers": ["website_rebuild", "conversie_optimalisatie", "automatisering"],
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
                    "tatoeage verwijdering kliniek",
                    "laser tattoo removal",
                ],
                "disqualifiers": ["laserontharing only"],
                "icp_signals": [
                    "fractional laser", "CO2 laser", "erbium laser", "IPL",
                    "morpheus8", "HIFU", "ultherapy", "radiofrequentie", "RF microneedling",
                    "microneedling", "fotorejuvenation", "huidverjonging",
                    "tatoeage verwijdering", "laser tattoo removal", "PicoSure",
                    "Q-switched laser", "tatoeage laseren",
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
                    "EMS afslank kliniek",
                    "cavitation behandeling",
                    "endermologie",
                    "body contouring kliniek",
                ],
                "disqualifiers": [],
                "icp_signals": [
                    "cryolipolyse", "coolsculpting", "EMsculpt", "EMS", "spierstimulatie afslank",
                    "cavitation", "endermologie", "LPG", "lymfedrainage cosmetisch",
                    "vetbevriezing", "vetreductie", "body contouring", "cellulitis behandeling",
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
            "beautykliniek",
            "huidkliniek",
            "esthetische kliniek",
            "medical spa",
            "medi-spa",
            "medspa",
            "huidinstituut",
            "esthetisch centrum",
            "cosmetisch centrum",
        ],
        # Sector-BREDE uitsluitingen — de ENIGE "disqualifiers"-sleutel. Drift-audit
        # 2026-07-29: dit was een DUPLICAAT-key (een dode lijst boven de subcategories
        # + deze); samengevoegd tot één, dode key opgeheven. ECHTE non-ICP: reguliere/
        # acute/kliniek-zorg, apotheek/drogist, verpleeg-/zorginstelling, pure wellness.
        # BEWUST NIET "medisch"/"arts"/"huidtherapeut"/"schoonheidssalon"/"wellness":
        # dat IS de ICP of een ICP-subcategorie — die horen in subcategory-disqualifiers
        # als DISAMBIGUATIE, niet sector-globaal (zie icp_matcher + lead_qualifier).
        # Geen enkele term is substring van een ICP-keyword (getoetst in tests). 'SEH'
        # bewust weggelaten (⊂ namen als '...seh...'; 'spoedeisende hulp' dekt het al).
        "disqualifiers": [
            "ziekenhuis", "UMC", "academisch medisch centrum", "spoedeisende hulp",
            "huisartsenpraktijk", "huisartsenpost", "apotheek", "drogisterij",
            "verpleeghuis", "zorginstelling",
            "dermatologie vergoed",  # puur medische dermatologie zonder cosmetische focus
            "oncologie", "kinderkliniek",
            # Pure wellness zonder medische component (subcats als 'schoonheidssalons'
            # hebben hun eigen ICP-logica):
            "alleen massage", "wellness resort", "sauna only", "zonnebank",
            "alleen pedicure", "alleen kapper",
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

    # =========================================================================
    # CHIROPRACTOREN
    # =========================================================================
    "chiropractoren": {
        "label": "Chiropractoren",
        # Website-only, net als alt-zorg: chiro werkt met pijnklachten → geen
        # automatisering/AI-receptie (momenteel geen automations-target).
        "allowed_offers": ["website_rebuild", "conversie_optimalisatie"],
        "sbi_codes": [],  # KvK-flow is opt-in (kost geld) — Maps-scraping gebruikt geen SBI.
        "sbi_codes_notes": {},
        "subcategories": {
            "chiropractie_algemeen": {
                "label": "Chiropractie algemeen",
                "lead_keywords": [
                    "chiropractor",
                    "chiropractie",
                    "chiropractie praktijk",
                    "chiropractisch centrum",
                    "rugklachten chiropractor",
                    "wervelkolom behandeling",
                ],
                "disqualifiers": [
                    "alleen fysiotherapie",
                    "alleen massage",
                    # 'osteopaat' alleen disqualifier als de praktijk GEEN chiro doet.
                    # Pure osteopathie zit in alternatieve_geneeskunde.manueel_fysiek.
                    "alleen osteopaat",
                ],
                "icp_signals": [
                    "chiropractor", "chiropractie", "DC", "Doctor of Chiropractic",
                    "SCN", "SCN-geregistreerd", "wervelkolom", "manuele aanpassing",
                    "spinale manipulatie", "subluxatie",
                ],
            },
        },
        "lead_keywords": [
            "chiropractor",
            "chiropractie",
            "chiropractie praktijk",
        ],
        "disqualifiers": [
            "ziekenhuis", "revalidatiecentrum",
            "alleen fysiotherapie", "alleen massage",
        ],
        "website_signals": {
            "positive": [
                "chiropractor", "chiropractie",
                "SCN", "SCN-geregistreerd", "Stichting Chiropractie Nederland",
                "DC", "Doctor of Chiropractic",
                "wervelkolom", "manuele aanpassing", "spinale manipulatie",
                "klachtenregeling", "AVG", "privacyverklaring",
                "online afspraak", "online booking",
                "Wkkgz", "geschillencommissie",
            ],
            "negative": [
                "alleen fysio", "alleen massage",
                "genezing gegarandeerd", "wondermiddel",
            ],
        },
        "notes": (
            "Chiropractoren als eigen ICP-sector — voorheen ondergebracht in "
            "alternatieve_geneeskunde.manueel_fysiek (samen met osteopathie, "
            "craniosacraal, rolfing, feldenkrais). Per 2026-05-20 zijn chiropractor- "
            "en chiropractie-keywords daar verwijderd om dubbele classificatie te "
            "voorkomen. Lead-bron is Google Maps query-scraping (zoekquery-termen "
            "hierboven). Geen KvK/SBI-filtering."
        ),
    },
}


# Sectoren die op dit moment actief worden gescraped + getarget.
# alternatieve_geneeskunde blijft in SECTORS dict voor backwards-compat met
# bestaande leads (sector='alternatieve_geneeskunde' in DB), maar valt uit
# list_sectors() zodat frontend-dropdown + scrape-flows alleen actieve ICP's
# tonen.
ACTIVE_SECTORS: list[str] = [
    "cosmetische_behandelaars",
    "chiropractoren",
]


def get_sector(sector_key: str) -> SectorConfig:
    """Haal een sector config op."""
    if sector_key not in SECTORS:
        raise ValueError(f"Onbekende sector: {sector_key}. Beschikbaar: {list(SECTORS.keys())}")
    return SECTORS[sector_key]


# Website-only = veilige default: een onbekende/niet-geconfigureerde sector krijgt
# NOOIT per ongeluk een automatiserings-pitch.
_DEFAULT_ALLOWED_OFFERS: list[str] = ["website_rebuild", "conversie_optimalisatie"]


def get_allowed_offers(sector_key: str | None) -> list[str]:
    """Toegestane pitch-diensten per sector (businessmodel: cosmetiek = volledige
    funnel incl. automatisering; alt-zorg/chiro = alleen website). Onbekend/leeg
    → website-only."""
    if not sector_key or sector_key not in SECTORS:
        return list(_DEFAULT_ALLOWED_OFFERS)
    return list(SECTORS[sector_key].get("allowed_offers") or _DEFAULT_ALLOWED_OFFERS)


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
    """Return only currently active sector keys (per ACTIVE_SECTORS).

    alternatieve_geneeskunde is per 2026-05-20 inactief — blijft in SECTORS
    dict (backwards-compat met bestaande leads) maar valt hier uit zodat
    nieuwe scrape-flows + frontend-dropdowns alleen de actieve ICP's tonen.
    Voor admin-zicht op alle sectoren: gebruik api/main.py /sectors/full
    dat direct over SECTORS.items() itereert.
    """
    return [s for s in ACTIVE_SECTORS if s in SECTORS]
