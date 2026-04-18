"""
config/sectors.py — Sector configurations for Heatr.

Adding a new sector requires only a new entry in SECTORS dict — no code changes elsewhere.
All scrapers, enrichers, scorers, and opportunity classifiers read from this file.

Active sectors (2026-04):
  - makelaars         — Real estate agents / brokers (NL + BE)
  - behandelaren      — Practitioners, coaches, therapists — NOT medical specialists
  - bouwbedrijven     — Construction companies, contractors, renovation firms
"""
from __future__ import annotations

SECTORS: dict[str, dict] = {

    # =========================================================================
    # Sector: Makelaars
    # Real estate agents / brokers. Typically 2-15 person offices.
    # Decision-maker: office owner or vestigingsmanager.
    # Strong digital presence expected — Funda listing, social media, virtual tours.
    # =========================================================================
    "makelaars": {
        "name": "Makelaars",
        "description": (
            "Makelaars, makelaardijen, vastgoedkantoren, taxateurs. "
            "NVM-, VBO- en VastgoedPRO-leden. Aankoop- en verkoopbegeleiding."
        ),
        "countries": ["NL", "BE"],

        "search_queries": [
            "makelaar {city}",
            "makelaardij {city}",
            "vastgoed makelaar {city}",
            "huis verkopen makelaar {city}",
            "aankoopmakelaar {city}",
            "verkoopmakelaar {city}",
            "NVM makelaar {city}",
            "taxateur {city}",
            "vastgoedkantoor {city}",
        ],

        "directory_urls": [
            "https://www.funda.nl/makelaars/{city}/",
            "https://www.nvm.nl/makelaars/?zoekterm={city}",
            "https://www.vbo.nl/makelaar-zoeken?plaats={city}",
        ],

        "kvk_sbi_codes": ["68.31", "68.32", "68.20"],

        "icp_keywords": [
            "makelaar", "makelaardij", "vastgoed", "taxatie", "NVM", "VBO",
            "woning", "verkoop", "aankoop", "hypotheek", "bezichtiging",
            "woningaanbod", "huis kopen", "huis verkopen", "VastgoedPRO",
            "courtage", "waardebepaling", "verkoopstrategie", "funda",
        ],

        "exclude_keywords": [
            "woningcorporatie", "sociale huurwoningen", "projectontwikkeling",
            "beleggingsfonds", "vastgoedbelegger", "hypotheekadviseur",
            "notaris", "vastgoedmanagement",
        ],

        "scoring_boosts": {
            "has_funda_listing": 4,
            "has_nvm_certification": 5,
            "has_virtual_tour": 3,
            "has_online_booking": 3,
            "google_rating_above_4_5": 3,
            "kvk_sbi_match": 5,
            "has_instagram": 3,
        },

        "sector_website_expectations": {
            "must_have": [
                {"key": "has_property_listings", "points": 5,
                 "label": "Woningaanbod / portfolio zichtbaar op site"},
                {"key": "has_team_page", "points": 5,
                 "label": "Teampagina met makelaars + foto's"},
                {"key": "has_nvm_vbo_certification", "points": 5,
                 "label": "NVM / VBO / VastgoedPRO certificering zichtbaar"},
            ],
            "should_have": [
                {"key": "has_virtual_tour", "bonus_points": 3,
                 "label": "Virtuele rondleiding of video van woningen"},
                {"key": "has_client_reviews", "bonus_points": 3,
                 "label": "Klantbeoordelingen / testimonials"},
            ],
            "nice_to_have": [
                {"key": "has_market_reports", "label": "Marktrapportages / woningmarktcijfers"},
                {"key": "has_blog_or_news", "label": "Blog of nieuwssectie"},
                {"key": "has_waardebepaling_cta", "label": "Gratis waardebepaling CTA"},
            ],
        },

        # Contact discovery: who to look for
        "decision_maker_titles": [
            "eigenaar", "oprichter", "vestigingsmanager", "directeur",
            "managing partner", "kantoormanager", "office manager",
        ],
        "typical_company_size": "2-15",
    },

    # =========================================================================
    # Sector: Alternatieve Geneeskunde
    # Alternative medicine practitioners in private practice.
    # Owner = practitioner = decision-maker. Usually 1-3 person practices.
    # NOT medical specialists, hospitals, GGZ, or regular physiotherapy chains.
    # =========================================================================
    "alternatieve_geneeskunde": {
        "name": "Alternatieve Geneeskunde",
        "description": (
            "Acupuncturisten, osteopaten, homeopaten, chiropractoren, "
            "natuurgeneeskundigen, haptotherapeuten, reflexologen, "
            "energetisch therapeuten, manueel therapeuten, "
            "integratief therapeuten. Privépraktijken, geen instellingen."
        ),
        "countries": ["NL", "BE"],

        "search_queries": [
            # Per micro-niche zoeken voor gerichte resultaten
            "acupuncturist {city}",
            "acupunctuur praktijk {city}",
            "osteopaat {city}",
            "osteopathie praktijk {city}",
            "homeopaat {city}",
            "homeopathie praktijk {city}",
            "chiropractor {city}",
            "chiropractie {city}",
            "natuurgeneeskundige {city}",
            "haptotherapeut {city}",
            "reflexoloog {city}",
            "voetreflexologie {city}",
            "energetisch therapeut {city}",
            "reiki therapeut {city}",
            "manueel therapeut {city}",
            "integratief therapeut {city}",
            "holistische therapeut {city}",
            "kruidengeneeskunde {city}",
            "ayurvedisch therapeut {city}",
        ],

        "directory_urls": [
            "https://www.natuurlijkbeter.nl/therapeuten/{city}",
            "https://www.zorgkaart.nl/therapeut/{city}",
            "https://www.holistische-therapeuten.nl/{city}",
        ],

        "kvk_sbi_codes": ["86.90", "86.21", "86.22", "86.23"],

        "icp_keywords": [
            "acupunctuur", "acupuncturist", "osteopathie", "osteopaat",
            "homeopathie", "homeopaat", "chiropractie", "chiropractor",
            "natuurgeneeskunde", "haptonomie", "haptotherapie",
            "reflexologie", "voetreflex", "reiki", "energetisch",
            "manuele therapie", "manueel therapeut", "healing",
            "integratieve geneeskunde", "holistisch", "kruidengeneeskunde",
            "ayurveda", "chinese geneeskunde", "TCM", "praktijk",
            "behandeling", "sessie", "vergoeding", "zorgverzekering",
            "natuurgeneeskundig", "complementair",
        ],

        "exclude_keywords": [
            "ziekenhuis", "ggz instelling", "ggz", "medisch specialist",
            "psychiater", "klinisch psycholoog", "revalidatiecentrum",
            "verpleeghuis", "thuiszorg", "apotheek", "huisartsenpraktijk",
            "keten", "franchise", "fysiotherapie keten",
            "sportschool", "fitness",
        ],

        "scoring_boosts": {
            "has_gratis_kennismaking_cta": 4,
            "has_personal_photo": 3,
            "email_starts_with_name": 2,
            "kvk_sbi_match": 5,
            "google_rating_above_4_5": 3,
            "has_online_booking": 4,
            "has_insurance_info": 3,
        },

        "sector_website_expectations": {
            "must_have": [
                {"key": "has_qualifications_visible", "points": 5,
                 "label": "Kwalificaties / opleiding / registratie zichtbaar (RBCZ, NVAO, etc.)"},
                {"key": "has_services_explained", "points": 5,
                 "label": "Behandelaanbod duidelijk beschreven met werkwijze"},
                {"key": "has_intake_or_booking", "points": 5,
                 "label": "Intakeformulier of online afspraak mogelijkheid"},
            ],
            "should_have": [
                {"key": "has_gratis_kennismaking_cta", "bonus_points": 3,
                 "label": "Gratis kennismakingsgesprek CTA"},
                {"key": "has_personal_photo", "bonus_points": 3,
                 "label": "Persoonlijke foto van behandelaar"},
            ],
            "nice_to_have": [
                {"key": "has_insurance_info", "label": "Vergoedingsinformatie zorgverzekering"},
                {"key": "has_client_reviews", "label": "Klantreviews / ervaringsverhalen"},
                {"key": "has_blog_or_articles", "label": "Kennisartikelen of blog"},
                {"key": "has_video", "label": "Introductievideo behandelaar"},
                {"key": "has_klachtenregeling", "label": "Klachtenregeling / beroepsvereniging"},
            ],
        },

        "decision_maker_titles": [
            "eigenaar", "oprichter", "praktijkhouder", "behandelaar",
            "therapeut", "acupuncturist", "osteopaat", "homeopaat",
            "chiropractor", "natuurgeneeskundige",
        ],
        "typical_company_size": "1-3",
    },

    # =========================================================================
    # Sector: Cosmetische Behandelaars
    # Aesthetic treatment providers — from premium clinics to solo practitioners.
    # Instagram presence is a strong qualifying signal.
    # Decision-maker: owner or clinic manager. Usually 1-10 persons.
    # =========================================================================
    "cosmetische_behandelaars": {
        "name": "Cosmetische Behandelaars",
        "description": (
            "Botox/filler klinieken, laserklinieken, huidtherapeuten, "
            "permanente make-up studios, premium schoonheidssalons, "
            "microblading specialisten, ontharing/waxing studios, "
            "gezichtsbehandeling specialisten."
        ),
        "countries": ["NL", "BE"],

        "search_queries": [
            "botox kliniek {city}",
            "filler kliniek {city}",
            "laserkliniek {city}",
            "huidtherapeut {city}",
            "huidtherapie praktijk {city}",
            "permanente make-up {city}",
            "microblading {city}",
            "schoonheidssalon {city}",
            "schoonheidsspecialiste {city}",
            "gezichtsbehandeling {city}",
            "cosmetische kliniek {city}",
            "anti-aging behandeling {city}",
            "ontharing laser {city}",
            "waxing salon {city}",
            "huidkliniek {city}",
            "medisch esthetiek {city}",
            "microneedling {city}",
        ],

        "directory_urls": [
            "https://www.beautynetwerk.nl/klinieken/{city}",
            "https://www.treatwell.nl/{city}/",
            "https://www.beautysalon.nl/zoeken/{city}",
        ],

        "kvk_sbi_codes": ["86.21", "96.02", "96.01", "96.09"],

        "icp_keywords": [
            "botox", "filler", "hyaluronzuur", "laser", "huidtherapie",
            "huidtherapeut", "schoonheidsbehandeling", "schoonheidssalon",
            "anti-aging", "cosmetisch", "esthetiek", "kliniek",
            "permanente make-up", "microblading", "ontharing",
            "microneedling", "peeling", "gezichtsbehandeling",
            "huidverjonging", "rejuvenation", "waxing", "epilatie",
            "dermapen", "BB glow", "lash lift", "wimperextensions",
            "medisch verantwoord", "behandelplan",
        ],

        "exclude_keywords": [
            "ziekenhuis", "plastisch chirurg groot", "keten landelijk",
            "drogisterij", "supermarkt", "apotheek keten",
            "kapper", "barbershop", "nagelgroothandel",
        ],

        "scoring_boosts": {
            "has_instagram": 5,
            "has_before_after_gallery": 4,
            "has_instagram_feed_embed": 3,
            "has_online_booking": 4,
            "kvk_sbi_match": 5,
            "google_rating_above_4_5": 3,
            "has_treatwell_profile": 3,
        },

        "sector_website_expectations": {
            "must_have": [
                {"key": "has_treatment_menu", "points": 5,
                 "label": "Behandelmenu met prijzen of prijsindicatie"},
                {"key": "has_certifications_visible", "points": 5,
                 "label": "Certificaten / BIG-registratie / medische kwalificaties"},
                {"key": "has_social_proof", "points": 5,
                 "label": "Reviews, testimonials of voor/na foto's"},
            ],
            "should_have": [
                {"key": "has_before_after_gallery", "bonus_points": 3,
                 "label": "Voor/na galerij met resultaten"},
                {"key": "has_instagram_feed_embed", "bonus_points": 3,
                 "label": "Geëmbedde Instagram feed of link"},
            ],
            "nice_to_have": [
                {"key": "has_team_page", "label": "Teamspagina met behandelaars"},
                {"key": "has_video", "label": "Behandelingsvideo of clinic tour"},
                {"key": "has_faq", "label": "FAQ over behandelingen"},
                {"key": "has_online_booking", "label": "Online booking systeem"},
            ],
        },

        "decision_maker_titles": [
            "eigenaar", "oprichter", "kliniekhoudster", "kliniekhouder",
            "directeur", "salon eigenaar", "schoonheidsspecialiste",
            "huidtherapeut", "behandelaar",
        ],
        "typical_company_size": "1-10",
    },

    # =========================================================================
    # Sector: Bouwbedrijven
    # Construction companies, contractors, renovation firms.
    # Range from 1-person ZZP-ers to 50+ person firms.
    # Decision-maker varies: ZZP = owner, larger = directeur / hoofd commercie.
    # =========================================================================
    "bouwbedrijven": {
        "name": "Bouwbedrijven",
        "description": (
            "Aannemers, bouwbedrijven, renovatiebedrijven, dakdekkers, "
            "timmerbedrijven, klusbedrijven, installatietechniek, "
            "schildersbedrijven, verbouwingsspecialisten."
        ),
        "countries": ["NL", "BE"],

        "search_queries": [
            "aannemer {city}",
            "bouwbedrijf {city}",
            "verbouwing {city}",
            "renovatie aannemer {city}",
            "dakdekker {city}",
            "klusbedrijf {city}",
            "timmerman {city}",
            "schildersbedrijf {city}",
            "installatiebedrijf {city}",
            "badkamer verbouwen {city}",
            "keuken verbouwen {city}",
            "aanbouw {city}",
        ],

        "directory_urls": [
            "https://www.werkspot.nl/vakmannen/{city}",
            "https://www.bouwend-nederland.nl/leden?plaats={city}",
            "https://www.thuisvakman.nl/{city}",
        ],

        "kvk_sbi_codes": [
            "41.20",  # Algemene burgerlijke en utiliteitsbouw
            "43.11",  # Slopen van bouwwerken
            "43.12",  # Grondverzet
            "43.21",  # Elektrotechnische bouwinstallatie
            "43.22",  # Loodgieters- en fitterswerk
            "43.29",  # Overige bouwinstallatie
            "43.31",  # Stukadoorswerk
            "43.32",  # Schrijnwerk
            "43.34",  # Schilderen en glaszetten
            "43.91",  # Dakdekken en dakbewerking
            "43.99",  # Overige gespecialiseerde bouw
        ],

        "icp_keywords": [
            "aannemer", "bouwbedrijf", "verbouwing", "renovatie", "nieuwbouw",
            "dakdekker", "timmerman", "schilder", "installateur", "loodgieter",
            "klusjesman", "aanbouw", "opbouw", "badkamer", "keuken",
            "kozijnen", "isolatie", "fundering", "metselwerk", "stucwerk",
            "Bouwend Nederland", "Techniek Nederland", "vakmanschap",
        ],

        "exclude_keywords": [
            "woningcorporatie", "infra", "waterstaat",
            "grond- weg- en waterbouw", "baggerbedrijf", "offshore",
            "industriebouw groot", "projectontwikkelaar",
        ],

        "scoring_boosts": {
            "has_project_portfolio": 4,
            "has_werkspot_profile": 3,
            "has_bouwend_nl_lid": 5,
            "has_client_reviews": 3,
            "kvk_sbi_match": 5,
            "google_rating_above_4_5": 3,
            "has_before_after_gallery": 3,
        },

        "sector_website_expectations": {
            "must_have": [
                {"key": "has_project_portfolio", "points": 5,
                 "label": "Projecten / portfolio met foto's"},
                {"key": "has_services_listed", "points": 5,
                 "label": "Diensten / specialisaties duidelijk vermeld"},
                {"key": "has_contact_info_visible", "points": 5,
                 "label": "Contactgegevens duidelijk zichtbaar (telefoon, email)"},
            ],
            "should_have": [
                {"key": "has_client_reviews", "bonus_points": 3,
                 "label": "Klantbeoordelingen / referenties"},
                {"key": "has_before_after_gallery", "bonus_points": 3,
                 "label": "Voor/na foto's van projecten"},
            ],
            "nice_to_have": [
                {"key": "has_certifications", "label": "Certificeringen (Bouwgarant, VCA, Keurmerk)"},
                {"key": "has_team_page", "label": "Teampagina met vaklui"},
                {"key": "has_quote_form", "label": "Offerte aanvraagformulier"},
                {"key": "has_blog_or_news", "label": "Blog of projectupdates"},
            ],
        },

        "decision_maker_titles": [
            "eigenaar", "directeur", "oprichter", "bedrijfsleider",
            "hoofd commercie", "projectleider", "uitvoerder",
            "managing director", "DGA",
        ],
        "typical_company_size": "1-50",
    },
}


def get_sector(sector_key: str) -> dict:
    """Return the full config dict for a sector key.

    Args:
        sector_key: Sector identifier string, e.g. 'makelaars'.

    Returns:
        Sector configuration dict.

    Raises:
        ValueError: If sector_key is not found in SECTORS.
    """
    if sector_key not in SECTORS:
        available = ", ".join(SECTORS.keys())
        raise ValueError(
            f"Unknown sector '{sector_key}'. Available sectors: {available}"
        )
    return SECTORS[sector_key]


def list_sectors() -> list[str]:
    """Return all active sector keys.

    Returns:
        List of sector key strings (e.g. ['makelaars', 'behandelaren', 'bouwbedrijven']).
    """
    return list(SECTORS.keys())
