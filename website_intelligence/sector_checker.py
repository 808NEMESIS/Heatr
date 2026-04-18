"""
website_intelligence/sector_checker.py — Layer 4: Sector-specific website scoring.

Reads sector_website_expectations from config/sectors.py and checks each criterion.
Max 15 points (must_have) + bonus points (should_have).
"""
from __future__ import annotations

import logging
import re
from typing import Any

from config.sectors import get_sector

logger = logging.getLogger(__name__)


async def check_sector_specific(
    domain: str,
    page_html: str,
    sector_key: str,
    conversion_result: dict | None = None,
    technical_result: dict | None = None,
) -> dict[str, Any]:
    """
    Run sector-specific checks against the website.

    Uses heuristic keyword matching on page HTML + results from other layers.

    Returns dict with:
        sector_score (0-15+bonus), checks (list of pass/fail per criterion)
    """
    result: dict[str, Any] = {
        "sector_score": 0,
        "checks": [],
    }

    try:
        sector = get_sector(sector_key)
    except ValueError:
        return result

    expectations = sector.get("sector_website_expectations", {})
    html_lower = (page_html or "").lower()
    score = 0

    # --- Must-have checks (5 pts each, max 15) ---
    for item in expectations.get("must_have", []):
        key = item["key"]
        points = item.get("points", 5)
        passed = _check_criterion(key, html_lower, conversion_result, technical_result)
        if passed:
            score += points
        result["checks"].append({"key": key, "label": item["label"], "passed": passed, "points": points if passed else 0})

    # --- Should-have checks (bonus points) ---
    for item in expectations.get("should_have", []):
        key = item["key"]
        bonus = item.get("bonus_points", 3)
        passed = _check_criterion(key, html_lower, conversion_result, technical_result)
        if passed:
            score += bonus
        result["checks"].append({"key": key, "label": item["label"], "passed": passed, "points": bonus if passed else 0})

    # --- Nice-to-have (no points, just tracking) ---
    for item in expectations.get("nice_to_have", []):
        key = item["key"]
        passed = _check_criterion(key, html_lower, conversion_result, technical_result)
        result["checks"].append({"key": key, "label": item["label"], "passed": passed, "points": 0})

    result["sector_score"] = score
    return result


def _check_criterion(
    key: str,
    html_lower: str,
    conv: dict | None,
    tech: dict | None,
) -> bool:
    """Check a single sector criterion using keyword heuristics and layer results."""
    conv = conv or {}
    tech = tech or {}

    # --- Makelaars ---
    if key == "has_property_listings":
        return any(w in html_lower for w in ["woningaanbod", "te koop", "aanbod", "woning", "funda", "object"])
    if key == "has_nvm_vbo_certification":
        return any(w in html_lower for w in ["nvm", "vbo", "vastgoedpro", "register makelaar"])

    # --- Behandelaren ---
    if key == "has_qualifications_visible":
        return any(w in html_lower for w in ["geregistreerd", "certificering", "opleiding", "diploma", "big-register", "nobco", "lvsc"])
    if key == "has_services_explained":
        return any(w in html_lower for w in ["behandelingen", "diensten", "aanbod", "werkwijze", "sessie", "coaching traject"])
    if key == "has_intake_or_booking":
        return conv.get("has_online_booking", False) or any(w in html_lower for w in ["intake", "aanmelden", "inschrijven", "boek"])
    if key == "has_insurance_info":
        return any(w in html_lower for w in ["vergoeding", "zorgverzekering", "vergoed", "tarief", "prijs"])

    # --- Bouwbedrijven ---
    if key == "has_project_portfolio":
        return any(w in html_lower for w in ["projecten", "portfolio", "referenties", "gerealiseerd", "realisaties"])
    if key == "has_services_listed":
        return any(w in html_lower for w in ["diensten", "specialisaties", "werkzaamheden", "wat wij doen", "onze diensten"])
    if key == "has_contact_info_visible":
        return 'href="tel:' in html_lower or any(w in html_lower for w in ["bel ons", "telefoon", "neem contact"])
    if key == "has_certifications":
        return any(w in html_lower for w in ["bouwgarant", "vca", "keurmerk", "gecertificeerd", "iso"])
    if key == "has_quote_form":
        return any(w in html_lower for w in ["offerte", "vrijblijvend", "aanvra"])

    # --- Shared ---
    if key == "has_team_page":
        return any(w in html_lower for w in ["team", "medewerkers", "over ons", "wie zijn wij", "ons team"])
    if key == "has_client_reviews":
        return any(w in html_lower for w in ["review", "beoordeling", "ervaring", "referentie", "testimonial", "klanten vertellen"])
    if key == "has_before_after_gallery":
        return any(w in html_lower for w in ["voor en na", "before after", "voor/na", "resultaten"])
    if key == "has_virtual_tour":
        return any(w in html_lower for w in ["360", "virtuele rondleiding", "virtual tour", "rondleiding"])
    if key == "has_blog_or_news" or key == "has_blog_or_articles":
        return any(w in html_lower for w in ["/blog", "/nieuws", "/articles", "/news", "kennisbank", "artikelen"])
    if key == "has_video":
        return any(w in html_lower for w in ["youtube.com/embed", "vimeo.com", "video", "wistia"])
    if key == "has_faq":
        return any(w in html_lower for w in ["faq", "veelgestelde vragen", "veel gestelde"])
    if key == "has_market_reports":
        return any(w in html_lower for w in ["woningmarkt", "marktrapport", "cijfers", "kwartaal"])
    if key == "has_waardebepaling_cta":
        return any(w in html_lower for w in ["waardebepaling", "gratis taxatie", "wat is mijn huis waard"])
    if key == "has_gratis_kennismaking_cta":
        return any(w in html_lower for w in ["gratis kennismak", "kennismakingsgesprek", "gratis intake", "vrijblijvend gesprek"])
    if key == "has_personal_photo":
        return any(w in html_lower for w in ["over mij", "wie ben ik", "mijn verhaal", "mijn praktijk"])
    if key == "has_social_proof":
        return any(w in html_lower for w in ["review", "testimonial", "ervaring", "★", "sterren", "google review"])
    if key == "has_instagram_feed_embed":
        return "instagram.com" in html_lower

    logger.debug("Unknown sector check key: %s", key)
    return False
