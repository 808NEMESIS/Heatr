"""
scrapers/kvk_scraper.py — KvK Handelsregister API client.

Official KvK Open API — no scraping. NL-only. Used as:
  1. Email discovery step 4 (correspondence address sometimes has email)
  2. Company data enrichment source (founding year, employees, SBI code)

KvK API base URL: https://api.kvk.nl/api/v1/
Auth: apikey header with KVK_API_KEY env var.
Rate limit: max 10 requests per minute (seeded in rate_limit_state).
"""

from __future__ import annotations

import logging
import os
import re
from difflib import SequenceMatcher
from typing import Any

import httpx

from utils.rate_limiter import wait_for_token

logger = logging.getLogger(__name__)

_KVK_BASE_URL = "https://api.kvk.nl/api/v1"
_KVK_SEARCH_PATH = "/zoeken"
_KVK_PROFILE_PATH = "/basisprofielen"


# =============================================================================
# SBI code → industry name mapping
# Covers all target sector codes + 30 most common NL business SBI codes.
# =============================================================================

SBI_TO_INDUSTRY: dict[str, str] = {
    # --- Alternatieve Zorg sector codes ---
    "86.90": "Alternatieve gezondheidszorg",
    "86.21": "Huisartsen en specialisten",
    "86.22": "Medische specialisten (polikliniek)",
    "86.23": "Tandartsen en mondhygiënisten",
    "85.59": "Overige opleidingen en cursussen",
    # --- Cosmetische Klinieken sector codes ---
    "96.02": "Kappers en schoonheidssalons",
    "96.01": "Wasserijen en chemisch reinigingsbedrijven",
    # --- Top 30 common Dutch business SBI codes ---
    "47.11": "Supermarkten en levensmiddelendetailhandel",
    "47.71": "Kledingdetailhandel",
    "47.91": "Detailhandel via internet",
    "55.10": "Hotels en pensions",
    "56.10": "Restaurants en eetcafés",
    "56.30": "Cafés en bars",
    "62.01": "Ontwikkelen en produceren van software",
    "62.02": "IT-consultancy en -beheer",
    "63.11": "Verwerken en hosting van data",
    "68.20": "Verhuur van onroerend goed",
    "69.10": "Juridische dienstverlening",
    "69.20": "Accountancy en belastingadvies",
    "70.22": "Organisatieadvies",
    "71.12": "Ingenieurs en technisch ontwerp",
    "73.11": "Reclamebureaus",
    "73.20": "Markt- en opinieonderzoek",
    "74.10": "Industrieel en grafisch ontwerp",
    "74.90": "Overige gespecialiseerde zakelijke diensten",
    "77.11": "Autoverhuur",
    "78.10": "Uitzendbureaus",
    "80.10": "Particuliere beveiliging",
    "81.21": "Gebouwreiniging",
    "82.11": "Algemene administratie",
    "85.20": "Basisonderwijs",
    "85.31": "Voortgezet onderwijs",
    "85.42": "Hoger beroepsonderwijs",
    "86.10": "Ziekenhuizen",
    "87.10": "Verpleeghuizen",
    "88.91": "Kinderopvang en peuterspeelzalen",
    "90.01": "Podiumkunsten",
    "93.11": "Sportaccommodaties",
    "96.09": "Overige persoonlijke dienstverlening",
    # --- Extended health & wellness ---
    "86.90.1": "Fysiotherapie",
    "86.90.2": "Oefentherapie",
    "86.90.3": "Logopedist",
    "86.90.4": "Diëtist",
    "86.90.9": "Overige paramedische beroepen",
    "86.21.0": "Huisartsenpraktijk",
    "96.02.1": "Kapper",
    "96.02.2": "Schoonheidssalon",
    "96.02.3": "Beautysalon",
}


def sbi_to_industry(sbi_code: str) -> str:
    """Map a KvK SBI code to a human-readable industry string.

    Tries exact match first, then truncated 5-char, then 5-char, then 4-char prefix.

    Args:
        sbi_code: SBI code string, e.g. '86.90' or '86.90.1'.

    Returns:
        Industry name string, or 'Onbekende sector' if not mapped.
    """
    if not sbi_code:
        return "Onbekende sector"
    sbi_code = sbi_code.strip()
    # Try exact match
    if sbi_code in SBI_TO_INDUSTRY:
        return SBI_TO_INDUSTRY[sbi_code]
    # Try 5-char prefix (e.g. "86.90" from "86.90.1")
    prefix5 = sbi_code[:5]
    if prefix5 in SBI_TO_INDUSTRY:
        return SBI_TO_INDUSTRY[prefix5]
    # Try 4-char prefix (e.g. "86.9")
    prefix4 = sbi_code[:4]
    for key, val in SBI_TO_INDUSTRY.items():
        if key.startswith(prefix4):
            return val
    return "Onbekende sector"


# =============================================================================
# Google Maps category → industry mapping
# 40+ common NL categories
# =============================================================================

GOOGLE_CATEGORY_TO_INDUSTRY: dict[str, str] = {
    # --- Alternatieve Zorg ---
    "fysiotherapeut": "Fysiotherapie",
    "fysiotherapie": "Fysiotherapie",
    "osteopaat": "Osteopathie",
    "acupuncturist": "Acupunctuur",
    "homeopaat": "Homeopathie",
    "psycholoog": "Psychologie praktijk",
    "coach": "Coaching",
    "life coach": "Coaching",
    "diëtist": "Diëtistenpraktijk",
    "diëtiste": "Diëtistenpraktijk",
    "energetisch therapeut": "Energetische therapie",
    "manueel therapeut": "Manuele therapie",
    "chiropractor": "Chiropractie",
    "therapeut": "Alternatieve therapie",
    "holistische praktijk": "Holistische gezondheidszorg",
    "massagetherapeut": "Massagetherapie",
    "sporttherapeut": "Sporttherapie",
    "rebalancing": "Alternatieve therapie",
    # --- Cosmetische klinieken ---
    "schoonheidssalon": "Schoonheidssalon",
    "schoonheidsinstituut": "Schoonheidsinstituut",
    "cosmetische kliniek": "Cosmetische kliniek",
    "cosmetisch arts": "Medisch esthetiek",
    "laserbehandeling": "Laserkliniek",
    "huidkliniek": "Huidkliniek",
    "huidtherapeut": "Huidtherapie",
    "botox": "Cosmetische injectiekliniek",
    "filler kliniek": "Cosmetische injectiekliniek",
    "anti-aging kliniek": "Anti-aging kliniek",
    "permanente make-up": "Permanente make-up studio",
    "nail studio": "Nagelstudio",
    "nagelstudio": "Nagelstudio",
    # --- General business ---
    "restaurant": "Horeca",
    "café": "Horeca",
    "hotel": "Horeca",
    "supermarkt": "Retail",
    "kledingwinkel": "Modewinkel",
    "bouwbedrijf": "Bouw",
    "advocaat": "Juridische dienstverlening",
    "accountant": "Accountancy",
    "notaris": "Notariaat",
    "architect": "Architectuur",
    "verzekeringsagent": "Verzekeringen",
    "makelaar": "Vastgoed",
    "kinderopvang": "Kinderopvang",
    "tandarts": "Tandartspraktijk",
    "huisarts": "Huisartsenpraktijk",
    "apotheek": "Apotheek",
}


# =============================================================================
# Public API
# =============================================================================

async def search_kvk(
    company_name: str,
    city: str,
) -> list[dict]:
    """Search the KvK Handelsregister for companies matching name + city.

    Args:
        company_name: Company name to search for.
        city: City to filter results (used in fuzzy match, not API parameter).

    Returns:
        List of raw KvK search result dicts. Empty list on API error.
    """
    api_key = os.getenv("KVK_API_KEY", "")
    if not api_key:
        logger.warning("KVK_API_KEY not set — skipping KvK search")
        return []

    params = {
        "naam": company_name,
        "pagina": 1,
        "resultatenPerPagina": 10,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{_KVK_BASE_URL}{_KVK_SEARCH_PATH}",
                params=params,
                headers={"apikey": api_key},
            )
            if response.status_code == 200:
                data = response.json()
                return data.get("resultaten", [])
            elif response.status_code == 401:
                logger.error("KvK API: invalid API key")
                return []
            elif response.status_code == 429:
                logger.warning("KvK API: rate limit exceeded")
                return []
            else:
                logger.warning("KvK API search returned %d", response.status_code)
                return []
    except Exception as e:
        logger.warning("KvK search failed: %s", e)
        return []


async def get_kvk_detail(kvk_number: str) -> dict | None:
    """Fetch full company profile from KvK basisprofiel API.

    Args:
        kvk_number: 8-digit KvK number string.

    Returns:
        Full KvK basisprofiel dict, or None on error/not-found.
    """
    api_key = os.getenv("KVK_API_KEY", "")
    if not api_key:
        return None

    # Validate format: exactly 8 digits
    if not re.match(r"^\d{8}$", kvk_number.strip()):
        logger.warning("Invalid KvK number format: %s", kvk_number)
        return None

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{_KVK_BASE_URL}{_KVK_PROFILE_PATH}/{kvk_number}",
                headers={"apikey": api_key},
            )
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.debug("KvK number not found: %s", kvk_number)
                return None
            else:
                logger.warning("KvK profile API returned %d for %s", response.status_code, kvk_number)
                return None
    except Exception as e:
        logger.warning("KvK detail fetch failed for %s: %s", kvk_number, e)
        return None


async def find_email_in_kvk(kvk_number: str) -> str | None:
    """Check KvK profile correspondence address for an email address.

    The KvK basisprofiel sometimes includes email in the correspondence
    address or other contact fields, though this is uncommon.

    Args:
        kvk_number: 8-digit KvK number string.

    Returns:
        Email string if found, None otherwise.
    """
    profile = await get_kvk_detail(kvk_number)
    if not profile:
        return None

    # Search recursively through the profile dict for email patterns
    profile_text = str(profile)
    email_pattern = re.compile(
        r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
        re.IGNORECASE,
    )
    matches = email_pattern.findall(profile_text)

    # Filter out KvK system emails and noise
    ignore_domains = {"kvk.nl", "example.com", "example.nl"}
    for match in matches:
        domain = match.split("@")[1].lower()
        if domain not in ignore_domains:
            return match.lower()

    return None


async def enrich_company_kvk(
    domain: str,
    company_name: str,
    city: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Search KvK, match best result, fetch details, and update companies_raw.

    Args:
        domain: Company domain string, e.g. 'example.nl'.
        company_name: Company name for fuzzy matching.
        city: City for fuzzy matching.
        workspace_id: Workspace slug.
        supabase_client: Supabase client.

    Returns:
        Dict with extracted KvK fields:
        kvk_number, kvk_sbi_code, kvk_sbi_description, kvk_founding_year,
        kvk_employee_count_range, kvk_legal_form, industry.
        Empty dict if KvK not available or no match found.
    """
    await wait_for_token("kvk_api", supabase_client)

    result: dict = {}

    search_results = await search_kvk(company_name, city)
    if not search_results:
        return result

    best = match_best_kvk_result(search_results, company_name, city)
    if not best:
        return result

    kvk_number = str(best.get("kvkNummer", "")).zfill(8)
    if not kvk_number or kvk_number == "00000000":
        return result

    profile = await get_kvk_detail(kvk_number)
    if not profile:
        return result

    # Extract fields from profile
    result["kvk_number"] = kvk_number

    # SBI code — from hoofdactiviteit
    sbi_code = ""
    sbi_description = ""
    activiteiten = profile.get("activiteiten", [])
    if activiteiten:
        hoofdactiviteit = next(
            (a for a in activiteiten if a.get("indicatieHoofdactiviteit")),
            activiteiten[0] if activiteiten else None,
        )
        if hoofdactiviteit:
            sbi_code = str(hoofdactiviteit.get("sbiCode", ""))
            sbi_description = hoofdactiviteit.get("sbiOmschrijving", "")

    result["kvk_sbi_code"] = sbi_code
    result["kvk_sbi_description"] = sbi_description
    result["industry"] = sbi_to_industry(sbi_code) if sbi_code else ""

    # Founding year
    founding_date = profile.get("datumOprichting", "")
    if founding_date and len(founding_date) >= 4:
        try:
            result["kvk_founding_year"] = int(founding_date[:4])
        except ValueError:
            pass

    # Employee count range
    employees_raw = profile.get("totaalWerkzamePersonen", {})
    if isinstance(employees_raw, dict):
        count = employees_raw.get("totaal")
        if count is not None:
            result["kvk_employee_count_range"] = _count_to_range(int(count))
    elif isinstance(employees_raw, int):
        result["kvk_employee_count_range"] = _count_to_range(employees_raw)

    # Legal form
    rechtsvorm = profile.get("rechtsvorm", {})
    if isinstance(rechtsvorm, dict):
        result["kvk_legal_form"] = rechtsvorm.get("omschrijving", "")
    elif isinstance(rechtsvorm, str):
        result["kvk_legal_form"] = rechtsvorm

    # Update companies_raw with KvK data
    if domain:
        try:
            supabase_client.table("companies_raw").update({
                "raw_data": {
                    "kvk_number": result.get("kvk_number"),
                    "kvk_sbi_code": result.get("kvk_sbi_code"),
                    "kvk_founding_year": result.get("kvk_founding_year"),
                    "kvk_employee_count_range": result.get("kvk_employee_count_range"),
                    "kvk_legal_form": result.get("kvk_legal_form"),
                },
            }).eq("workspace_id", workspace_id).eq("domain", domain).execute()
        except Exception as e:
            logger.warning("Failed to update companies_raw with KvK data: %s", e)

    logger.info(
        "KvK enrichment done: company=%s kvk=%s sbi=%s",
        company_name, kvk_number, sbi_code,
    )
    return result


def match_best_kvk_result(
    results: list[dict],
    company_name: str,
    city: str,
) -> dict | None:
    """Fuzzy-match KvK search results to find the most likely correct company.

    Scoring:
    - Name similarity via SequenceMatcher (0–1, weight 0.7)
    - City match in address (boolean, weight 0.3)

    Args:
        results: List of KvK search result dicts.
        company_name: Target company name for matching.
        city: Target city for matching.

    Returns:
        Best-matching result dict, or None if list is empty or no result
        scores above the minimum threshold (0.4).
    """
    if not results:
        return None

    company_name_lower = company_name.lower().strip()
    city_lower = city.lower().strip()
    min_threshold = 0.4

    best_result: dict | None = None
    best_score: float = 0.0

    for result in results:
        # Name similarity
        kvk_name = result.get("naam", "").lower().strip()
        name_similarity = SequenceMatcher(
            None, company_name_lower, kvk_name
        ).ratio()

        # City match — check in address string
        address_str = str(result.get("adres", "")).lower()
        city_match = 1.0 if city_lower in address_str else 0.0

        score = name_similarity * 0.7 + city_match * 0.3

        if score > best_score:
            best_score = score
            best_result = result

    if best_score >= min_threshold:
        return best_result

    return None


# =============================================================================
# Internal helpers
# =============================================================================

def _count_to_range(count: int) -> str:
    """Convert a raw employee count to a human-readable range bucket.

    Args:
        count: Integer employee count.

    Returns:
        Range string, e.g. '1-5', '6-10', '11-50', '51-250', '250+'.
    """
    if count <= 5:
        return "1-5"
    elif count <= 10:
        return "6-10"
    elif count <= 50:
        return "11-50"
    elif count <= 250:
        return "51-250"
    else:
        return "250+"
