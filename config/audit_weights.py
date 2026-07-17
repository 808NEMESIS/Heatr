"""
config/audit_weights.py — gewichten + labels van de prospect-facing audit-scorer.

EIGEN NAMESPACE, los van scoring_weights.py (de interne classifier). Deze module
raakt website_intelligence-scoring niet aan.

Bron van waarheid = de CHECKS-lijst. Categorie-maxima worden UIT de checks
berekend (per sector), niet uit een los header-getal — zo kan een spec-header
en de som van z'n items niet stil uiteenlopen.

SPEC-DISCREPANTIE (gemeld, niet stil "gefixt"): de bijlage-tabel zegt Lead
Conversion = 34, maar de items sommen tot 32 (cosmetiek) / 31 (chiro). De
overige zeven categorieen kloppen wel met hun header. Omdat we over de behaalde
noemer normaliseren (achieved/max * 100), gebruiken we de itemsom als max; het
2-punts-gat verandert niets aan de normalisatie, maar het moet expliciet zijn.
Totaal-max daardoor 108 (cosmetiek) i.p.v. 110.
"""
from __future__ import annotations

# Interne categorie -> respectvol rapportlabel (vraag vanuit de bezoeker).
CATEGORY_LABELS: dict[str, str] = {
    "lead_conversion":   "Komen bezoekers tot actie",
    "social_proof":      "Waarom zouden ze u kiezen",
    "local_trust":       "Bent u vindbaar",
    "professional_trust": "Bent u geloofwaardig",
    "privacy":           "Voldoet u aan de regels",
    "seo_visibility":    "Wordt u gevonden",
    "technical":         "Werkt uw site",
    "visual":            "Hoe komt u over",
}

# Sector-sleutels (config/sectors.py). "all" = beide.
_COS = "cosmetische_behandelaars"
_CHI = "chiropractoren"

# Elke check: id, categorie, punten, sectors (welke sectoren 'm meetellen).
# De detectie zit in audit/checks.py; hier staan alleen punten + toepasbaarheid.
CHECKS: list[dict] = [
    # --- Lead Conversion (items: 32 cosmetiek / 31 chiro) ---
    {"check_id": "online_afspraak_widget", "category": "lead_conversion", "points": 10, "sectors": "all"},
    {"check_id": "whatsapp_klikbaar",      "category": "lead_conversion", "points": 5,  "sectors": "all"},
    {"check_id": "tel_above_fold",         "category": "lead_conversion", "points": 3,  "sectors": "all"},
    {"check_id": "contactform_max5",       "category": "lead_conversion", "points": 4,  "sectors": "all"},
    {"check_id": "cta_above_fold",         "category": "lead_conversion", "points": 3,  "sectors": "all"},
    {"check_id": "consult_intake_aanbod",  "category": "lead_conversion", "points": 3,  "sectors": [_COS]},
    {"check_id": "vergoeding_aanvullend",  "category": "lead_conversion", "points": 2,  "sectors": [_CHI]},
    {"check_id": "responstijd_belofte",    "category": "lead_conversion", "points": 2,  "sectors": "all"},
    {"check_id": "live_chat",              "category": "lead_conversion", "points": 2,  "sectors": "all"},

    # --- Social Proof (18) ---
    {"check_id": "reviews_zichtbaar",         "category": "social_proof", "points": 5, "sectors": "all"},
    {"check_id": "google_rating_min",         "category": "social_proof", "points": 4, "sectors": "all"},
    {"check_id": "voor_na_galerij",           "category": "social_proof", "points": 4, "sectors": [_COS]},
    {"check_id": "patientverhalen",           "category": "social_proof", "points": 4, "sectors": [_CHI]},
    {"check_id": "behandelaars_naam_foto_kwal", "category": "social_proof", "points": 3, "sectors": "all"},
    {"check_id": "echte_praktijkfotos",       "category": "social_proof", "points": 2, "sectors": "all"},

    # --- Local Trust (13) ---
    {"check_id": "maps_embed_contact",      "category": "local_trust", "points": 4, "sectors": "all"},
    {"check_id": "adres_footer_volledig",   "category": "local_trust", "points": 4, "sectors": "all"},
    {"check_id": "gbp_link",                "category": "local_trust", "points": 2, "sectors": "all"},
    {"check_id": "openingstijden_structured", "category": "local_trust", "points": 3, "sectors": "all"},

    # --- Beroeps- & Medisch Trust (9) — per sector andere checks, zelfde max ---
    {"check_id": "big_nummer",              "category": "professional_trust", "points": 4, "sectors": [_COS]},
    {"check_id": "keurmerk_cosmetiek",      "category": "professional_trust", "points": 3, "sectors": [_COS]},
    {"check_id": "wkkgz_klachten",          "category": "professional_trust", "points": 1, "sectors": "all"},
    {"check_id": "voor_na_toestemming",     "category": "professional_trust", "points": 1, "sectors": [_COS]},
    {"check_id": "scn_nca_registratie",     "category": "professional_trust", "points": 4, "sectors": [_CHI]},
    {"check_id": "erkende_opleiding_chiro", "category": "professional_trust", "points": 3, "sectors": [_CHI]},
    {"check_id": "geschilleninstantie",     "category": "professional_trust", "points": 1, "sectors": [_CHI]},

    # --- AVG (10) ---
    {"check_id": "privacyverklaring_bereikbaar", "category": "privacy", "points": 3, "sectors": "all"},
    {"check_id": "cookiebanner_weigeren",        "category": "privacy", "points": 3, "sectors": "all"},
    {"check_id": "geen_tracking_pre_consent",    "category": "privacy", "points": 3, "sectors": "all"},
    {"check_id": "verwerkersregister_dpo",       "category": "privacy", "points": 1, "sectors": "all"},

    # --- SEO (10) ---
    {"check_id": "unieke_title_meta",   "category": "seo_visibility", "points": 2, "sectors": "all"},
    {"check_id": "een_h1_per_pagina",   "category": "seo_visibility", "points": 1, "sectors": "all"},
    {"check_id": "schema_medical_valide", "category": "seo_visibility", "points": 2, "sectors": "all"},
    {"check_id": "behandelpaginas_3plus", "category": "seo_visibility", "points": 3, "sectors": [_COS]},
    {"check_id": "klachtenpaginas_3plus", "category": "seo_visibility", "points": 3, "sectors": [_CHI]},
    {"check_id": "interne_links_key",   "category": "seo_visibility", "points": 1, "sectors": "all"},
    {"check_id": "sitemap_robots",      "category": "seo_visibility", "points": 1, "sectors": "all"},

    # --- Technical (8) ---
    {"check_id": "psi_mobile_50",       "category": "technical", "points": 3, "sectors": "all"},
    {"check_id": "lcp_onder_2_5",       "category": "technical", "points": 2, "sectors": "all"},
    {"check_id": "https_geldig_cert",   "category": "technical", "points": 1, "sectors": "all"},
    {"check_id": "geen_mixed_content",  "category": "technical", "points": 1, "sectors": "all"},
    {"check_id": "security_headers",    "category": "technical", "points": 1, "sectors": "all"},

    # --- Visual (8) — visual_score (0-25) genormaliseerd naar 0-8. Eén getal. ---
    {"check_id": "visuele_indruk",      "category": "visual", "points": 8, "sectors": "all"},
]

# Knock-outs — cappen de EINDscore ongeacht de rest. Reden -> score_capped_by.
KNOCKOUTS = {
    "no_appointment": {"cap": 70, "reason": "geen enkele afspraakmogelijkheid"},
    "no_phone":       {"cap": 80, "reason": "geen telefonisch contact"},
}

# Lege-site-detectie: DOM-tekstlengte onder deze drempel = verlaten placeholder
# (avichi.nl had 25 tekens body-tekst). Aparte finding, geen lage score.
EMPTY_SITE_DOM_CHARS = 150


def checks_for_sector(sector: str) -> list[dict]:
    """De checks die voor deze sector meetellen."""
    return [c for c in CHECKS if c["sectors"] == "all" or sector in c["sectors"]]


def category_max(sector: str) -> dict[str, int]:
    """Max-punten per categorie voor deze sector (som van de meetellende checks)."""
    out: dict[str, int] = {}
    for c in checks_for_sector(sector):
        out[c["category"]] = out.get(c["category"], 0) + c["points"]
    return out


def total_max(sector: str) -> int:
    return sum(category_max(sector).values())
