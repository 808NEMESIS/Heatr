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
    # Sector: Behandelaren / Coaches
    # Practitioners, coaches, therapists in private practice.
    # EXPLICITLY NOT medical specialists — no GGZ institutions, hospitals, psychiatrists.
    # Decision-maker: the owner = the practitioner. Usually 1-5 person practice.
    # =========================================================================
    "behandelaren": {
        "name": "Behandelaren & Coaches",
        "description": (
            "Coaches (loopbaan, burnout, life, executive), therapeuten (relatie, "
            "mindfulness, gestalt), fysiotherapeuten, osteopaten, diëtisten, "
            "personal trainers, yogadocenten. Privépraktijken, geen instellingen."
        ),
        "countries": ["NL", "BE"],

        "search_queries": [
            "coach {city}",
            "loopbaancoach {city}",
            "burnout coach {city}",
            "personal coach {city}",
            "relatietherapeut {city}",
            "fysiotherapeut {city}",
            "osteopaat {city}",
            "diëtist {city}",
            "mindfulness trainer {city}",
            "executive coach {city}",
            "life coach {city}",
            "yoga studio {city}",
            "personal trainer {city}",
        ],

        "directory_urls": [
            "https://www.coachfinder.nl/coaches/{city}",
            "https://www.natuurlijkbeter.nl/therapeuten/{city}",
            "https://www.therapiepsycholoog.nl/zoeken/psycholoog/{city}",
        ],

        "kvk_sbi_codes": ["85.59", "86.90", "86.21", "86.22", "86.23", "93.13"],

        "icp_keywords": [
            "coaching", "coach", "therapie", "therapeut", "praktijk",
            "sessie", "traject", "intake", "persoonlijke ontwikkeling",
            "mindfulness", "burnout", "loopbaan", "relatie", "gestalt",
            "ACT", "EMDR", "fysiotherapie", "osteopathie", "diëtist",
            "personal training", "yoga", "pilates", "hollistisch",
            "gratis kennismaking", "kennismakingsgesprek",
        ],

        "exclude_keywords": [
            "ziekenhuis", "ggz instelling", "ggz", "medisch specialist",
            "psychiater", "klinisch psycholoog", "revalidatiecentrum",
            "verpleeghuis", "thuiszorg", "apotheek", "huisartsenpraktijk",
            "keten", "franchise landelijk",
        ],

        "scoring_boosts": {
            "has_gratis_kennismaking_cta": 4,
            "has_personal_photo": 3,
            "email_starts_with_name": 2,
            "kvk_sbi_match": 5,
            "google_rating_above_4_5": 3,
            "has_online_booking": 4,
        },

        "sector_website_expectations": {
            "must_have": [
                {"key": "has_qualifications_visible", "points": 5,
                 "label": "Kwalificaties / opleiding / certificering zichtbaar"},
                {"key": "has_services_explained", "points": 5,
                 "label": "Diensten / behandelaanbod duidelijk beschreven"},
                {"key": "has_intake_or_booking", "points": 5,
                 "label": "Intakeformulier of boekingsmogelijkheid"},
            ],
            "should_have": [
                {"key": "has_gratis_kennismaking_cta", "bonus_points": 3,
                 "label": "Gratis kennismakingsgesprek CTA"},
                {"key": "has_personal_photo", "bonus_points": 3,
                 "label": "Persoonlijke foto van behandelaar/coach"},
            ],
            "nice_to_have": [
                {"key": "has_insurance_info", "label": "Vergoedingsinformatie / tarieven"},
                {"key": "has_client_reviews", "label": "Klantreviews / testimonials"},
                {"key": "has_blog_or_articles", "label": "Kennisartikelen of blog"},
                {"key": "has_video", "label": "Introductievideo"},
            ],
        },

        "decision_maker_titles": [
            "eigenaar", "oprichter", "praktijkhouder", "behandelaar",
            "coach", "therapeut", "trainer",
        ],
        "typical_company_size": "1-5",
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
