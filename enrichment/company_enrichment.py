"""
enrichment/company_enrichment.py — Industry inference and Claude Haiku enrichment.

Runs after the email waterfall completes. Generates:
  - Industry label (SBI → Google category → Claude fallback)
  - Company summary (80-token Dutch business description)
  - Personalised opener (60-token Dutch outreach intro)
  - Company size estimate (from KvK + signals)

Claude is used only as a last resort for industry inference, and always for
summary + opener generation. If Claude fails: log, return partial result,
never block the pipeline.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sector-specific industry lists — Claude must return only these values
# ---------------------------------------------------------------------------

_INDUSTRY_LIST_MAKELAARS = [
    "Makelaarskantoor", "Makelaardij", "Vastgoedkantoor",
    "Aankoopmakelaar", "Verkoopmakelaar", "Taxatiekantoor",
    "Hypotheekadvies + makelaardij", "Bedrijfsmakelaar",
    "Verhuurmakelaar", "Vastgoedadvies",
]

_INDUSTRY_LIST_BEHANDELAREN = [
    "Coaching praktijk", "Loopbaancoach", "Executive coach",
    "Burnout coach", "Life coach", "Relatietherapeut",
    "Fysiotherapie", "Osteopathie", "Diëtistenpraktijk",
    "Personal training", "Yoga studio", "Mindfulness trainer",
    "Gestalttherapie", "ACT therapeut", "Massagetherapie",
    "Pilates studio", "Holistische praktijk",
    "Overige coaching / therapie",
]

_INDUSTRY_LIST_BOUWBEDRIJVEN = [
    "Aannemersbedrijf", "Bouwbedrijf", "Renovatiebedrijf",
    "Dakdekkersbedrijf", "Timmerbedrijf", "Schildersbedrijf",
    "Installatiebedrijf", "Loodgietersbedrijf", "Stukadoorsbedrijf",
    "Klusbedrijf", "Verbouwingsspecialist", "Metselwerk",
    "Kozijnen & gevelbekleding", "Isolatiebedrijf",
    "Badkamer- & keukenspecialist", "Elektrotechnisch bedrijf",
    "Overige bouw",
]

_INDUSTRY_LISTS: dict[str, list[str]] = {
    "makelaars": _INDUSTRY_LIST_MAKELAARS,
    "behandelaren": _INDUSTRY_LIST_BEHANDELAREN,
    "bouwbedrijven": _INDUSTRY_LIST_BOUWBEDRIJVEN,
}

_OPENER_LANGUAGE_NL = "nl"


# =============================================================================
# Public API
# =============================================================================

async def enrich_company(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
) -> dict:
    """Run full company enrichment for a lead: industry, summary, opener, size.

    Loads the lead row, infers industry, generates Claude content, updates
    the leads table. All Claude failures are caught and logged — never raises.

    Args:
        lead_id: UUID of the lead row.
        workspace_id: Workspace slug.
        supabase_client: Supabase client.
        anthropic_client: Initialised Anthropic client.

    Returns:
        Dict with keys: industry, company_summary, personalized_opener,
        company_size_estimate. Missing fields are empty strings/None.
    """
    result: dict = {
        "industry": "",
        "company_summary": "",
        "personalized_opener": "",
        "company_size_estimate": "",
    }

    # Load lead
    try:
        response = (
            supabase_client.table("leads")
            .select(
                "id, company_name, domain, city, sector, "
                "contact_name, contact_first_name, "
                "google_rating, google_review_count, "
                "has_instagram, cms_detected, "
                "kvk_number, kvk_sbi_code, kvk_employee_count_range, "
                "google_category, website_score, enrichment_version"
            )
            .eq("id", lead_id)
            .single()
            .execute()
        )
    except Exception as e:
        logger.error("enrich_company: failed to load lead %s: %s", lead_id, e)
        return result

    if not response.data:
        return result

    lead = response.data
    company_name = lead.get("company_name", "")
    city = lead.get("city", "")
    sector_key = lead.get("sector", "")
    kvk_sbi = lead.get("kvk_sbi_code", "")
    google_category = lead.get("google_category") or ""
    contact_name = lead.get("contact_name") or lead.get("contact_first_name")
    google_rating = lead.get("google_rating")
    review_count = lead.get("google_review_count")
    has_instagram = lead.get("has_instagram", False)
    kvk_employee_range = lead.get("kvk_employee_count_range")
    domain = lead.get("domain", "")

    # --- Industry inference --------------------------------------------------
    industry = _infer_industry_local(kvk_sbi, google_category, sector_key)

    if not industry:
        # Claude fallback — only if local inference fails
        website_text = await _fetch_website_text_from_enrichment(lead_id, supabase_client)
        if website_text or google_category:
            industry = await infer_industry_claude(
                website_text=website_text,
                google_category=google_category,
                sector_key=sector_key,
                anthropic_client=anthropic_client,
            )

    result["industry"] = industry or ""

    # --- Company size estimate -----------------------------------------------
    website_enrichment = await _fetch_website_enrichment_data(lead_id, supabase_client)
    has_careers = website_enrichment.get("has_careers_page", False)
    team_count = website_enrichment.get("team_page_count")

    result["company_size_estimate"] = estimate_company_size(
        kvk_employee_range=kvk_employee_range,
        review_count=review_count,
        has_careers_page=has_careers,
        team_page_count=team_count,
    )

    # Fetch website text for summary generation
    website_text = await _fetch_website_text_from_enrichment(lead_id, supabase_client)

    # --- Claude summary + opener (best effort) -------------------------------
    if company_name:
        result["company_summary"] = await generate_company_summary(
            company_name=company_name,
            industry=result["industry"],
            city=city,
            website_text=website_text,
            anthropic_client=anthropic_client,
        )

        result["personalized_opener"] = await generate_personalized_opener(
            company_name=company_name,
            city=city,
            industry=result["industry"],
            contact_name=contact_name,
            summary=result["company_summary"],
            has_instagram=has_instagram,
            google_rating=google_rating,
            google_review_count=review_count,
            sector_key=sector_key,
            language=_OPENER_LANGUAGE_NL,
            anthropic_client=anthropic_client,
        )

    # --- Persist to leads table ---------------------------------------------
    try:
        version = (lead.get("enrichment_version") or 0) + 1
        supabase_client.table("leads").update({
            "industry": result["industry"],
            "company_summary": result["company_summary"],
            "personalized_opener": result["personalized_opener"],
            "company_size_estimate": result["company_size_estimate"],
            "enrichment_version": version,
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.error("Failed to persist enrichment for lead %s: %s", lead_id, e)

    # --- Store raw Claude output in enrichment_data -------------------------
    try:
        supabase_client.table("enrichment_data").insert({
            "workspace_id": workspace_id,
            "lead_id": lead_id,
            "enrichment_step": 0,
            "source": "claude_enrichment",
            "succeeded": bool(result["company_summary"] or result["industry"]),
            "raw_result": {
                "industry": result["industry"],
                "summary": result["company_summary"],
                "opener": result["personalized_opener"],
                "size": result["company_size_estimate"],
            },
        }).execute()
    except Exception as e:
        logger.warning("Failed to store enrichment_data for %s: %s", lead_id, e)

    logger.info("Company enrichment done for lead %s: industry=%s", lead_id, result["industry"])
    return result


async def infer_industry_claude(
    website_text: str,
    google_category: str,
    sector_key: str,
    anthropic_client: Any,
) -> str:
    """Use Claude Haiku to infer the industry from website text + category.

    Returns only values from the fixed industry list for the sector, preventing
    hallucinated industry names entering the database.

    Args:
        website_text: Plain text extracted from the company website.
        google_category: Google Maps category string.
        sector_key: Sector key used to select the allowed industry list.
        anthropic_client: Initialised Anthropic client.

    Returns:
        Industry name string from the fixed list, or "" on failure.
    """
    industry_list = _INDUSTRY_LISTS.get(sector_key, [])
    if not industry_list:
        return ""

    options = "\n".join(f"- {item}" for item in industry_list)
    context_parts = []
    if google_category:
        context_parts.append(f"Google categorie: {google_category}")
    if website_text:
        context_parts.append(f"Website tekst (excerpt): {website_text[:500]}")
    context = "\n".join(context_parts)

    prompt = (
        f"Kies de meest passende industrie uit deze lijst voor dit bedrijf.\n\n"
        f"Beschikbare industrieën:\n{options}\n\n"
        f"Bedrijfsinformatie:\n{context}\n\n"
        f"Antwoord met ALLEEN de exacte naam van de industrie uit de lijst. "
        f"Geen uitleg, geen punctuatie."
    )

    try:
        response = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=30,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        # Validate against the allowed list (case-insensitive match)
        for item in industry_list:
            if raw.lower() == item.lower():
                return item
        # Partial match fallback
        for item in industry_list:
            if item.lower() in raw.lower():
                return item
    except Exception as e:
        logger.warning("Claude industry inference failed: %s", e)

    return ""


async def generate_company_summary(
    company_name: str,
    industry: str,
    city: str,
    website_text: str,
    anthropic_client: Any,
) -> str:
    """Generate a short Dutch company summary using Claude Haiku.

    Max 80 tokens. Zakelijke beschrijving for sales context — no marketing
    language, no superlatives.

    Args:
        company_name: Company name.
        industry: Inferred industry label.
        city: City where the company operates.
        website_text: Plain text from company website (may be empty).
        anthropic_client: Anthropic client.

    Returns:
        Dutch summary string, or "" on failure.
    """
    context = f"{company_name}, {industry}, {city}."
    if website_text:
        context += f" Website: {website_text[:400]}"

    prompt = (
        f"Schrijf een zakelijke beschrijving in het Nederlands van maximaal 2 zinnen "
        f"over dit bedrijf voor gebruik in een sales-context.\n\n"
        f"Bedrijf: {context}\n\n"
        f"Regels:\n"
        f"- Geen marketingtaal of superlatieven\n"
        f"- Beschrijf wat het bedrijf doet en voor wie\n"
        f"- Maximaal 80 woorden"
    )

    try:
        response = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.warning("Claude summary generation failed: %s", e)
        return ""


async def generate_personalized_opener(
    company_name: str,
    city: str,
    industry: str,
    contact_name: str | None,
    summary: str,
    has_instagram: bool,
    google_rating: float | None,
    google_review_count: int | None,
    sector_key: str,
    language: str,
    anthropic_client: Any,
) -> str:
    """Generate a personalised email opener using Claude Haiku.

    Max 60 tokens. Dutch. References one real signal about the company.
    Does not start with 'Ik'. Sector-aware tone.

    Args:
        company_name: Company name.
        city: City.
        industry: Industry label.
        contact_name: Contact name for personalisation (or None).
        summary: Company summary from generate_company_summary().
        has_instagram: Whether the company has Instagram.
        google_rating: Google rating float or None.
        google_review_count: Number of Google reviews or None.
        sector_key: Sector key for tone guidance.
        language: Language code (currently always 'nl').
        anthropic_client: Anthropic client.

    Returns:
        Opener string, or "" on failure.
    """
    # Build real signals list
    signals: list[str] = []
    if google_rating and google_rating >= 4.0:
        signals.append(f"{google_review_count or '?'} Google reviews met een {google_rating} beoordeling")
    if has_instagram:
        signals.append("actieve Instagram-aanwezigheid")
    if not signals:
        signals.append(f"praktijk in {city}")

    signal_text = signals[0]  # Use only one signal

    tone_guidance = {
        "makelaars": "zakelijk, lokaal betrokken, persoonlijk — geen harde verkoop",
        "behandelaren": "warm, persoonlijk, professioneel — geen harde verkoop",
        "bouwbedrijven": "direct, vakkundig, no-nonsense — geen harde verkoop",
    }.get(sector_key, "professioneel en persoonlijk")

    salutation = f"aan {contact_name}" if contact_name else f"over {company_name}"
    prompt = (
        f"Schrijf een openingszin voor een zakelijke email {salutation}.\n\n"
        f"Bedrijf: {company_name}, {industry}, {city}\n"
        f"Signaal om te noemen: {signal_text}\n"
        f"Toon: {tone_guidance}\n\n"
        f"Regels:\n"
        f"- Begin NIET met 'Ik'\n"
        f"- Maximaal 60 woorden\n"
        f"- Verwijs naar het signaal op een natuurlijke manier\n"
        f"- Geen verkooppraatje\n"
        f"- Eindig niet met een vraag"
    )

    try:
        response = anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=60,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.warning("Claude opener generation failed: %s", e)
        return ""


def estimate_company_size(
    kvk_employee_range: str | None,
    review_count: int | None,
    has_careers_page: bool,
    team_page_count: int | None,
) -> str:
    """Estimate company size from available signals.

    Priority: KvK employee count (exact) → proxy signals (heuristic).

    Args:
        kvk_employee_range: Range string from KvK (e.g. '1-5', '6-10').
        review_count: Number of Google reviews.
        has_careers_page: Whether the website has a careers/vacatures page.
        team_page_count: Number of team members visible on the website.

    Returns:
        Size bucket string: '1-5' | '1-10' | '10-50' | '50-250' | '250+'.
    """
    # KvK is ground truth
    if kvk_employee_range:
        return kvk_employee_range

    # Proxy: team page count
    if team_page_count is not None:
        if team_page_count <= 5:
            return "1-5"
        elif team_page_count <= 10:
            return "1-10"
        elif team_page_count <= 50:
            return "10-50"
        else:
            return "50-250"

    # Proxy: careers page = likely >= 10 employees
    if has_careers_page:
        return "10-50"

    # Proxy: review count (higher review count → more established / larger)
    if review_count is not None:
        if review_count < 10:
            return "1-5"
        elif review_count < 50:
            return "1-10"
        elif review_count < 200:
            return "10-50"
        else:
            return "50-250"

    return "1-10"  # Default for BENELUX SMB in target sectors


# =============================================================================
# Internal helpers
# =============================================================================

def _infer_industry_local(
    kvk_sbi: str,
    google_category: str,
    sector_key: str,
) -> str:
    """Infer industry from local data without calling Claude.

    Tries SBI code first (authoritative), then Google Maps category mapping.

    Args:
        kvk_sbi: SBI code string from KvK.
        google_category: Google Maps category string.
        sector_key: Sector key for context.

    Returns:
        Industry string or "" if not determinable locally.
    """
    from scrapers.kvk_scraper import sbi_to_industry, GOOGLE_CATEGORY_TO_INDUSTRY

    # SBI code is most authoritative
    if kvk_sbi:
        industry = sbi_to_industry(kvk_sbi)
        if industry and industry != "Onbekende sector":
            return industry

    # Google Maps category lookup
    if google_category:
        category_lower = google_category.lower().strip()
        # Exact match
        if category_lower in GOOGLE_CATEGORY_TO_INDUSTRY:
            return GOOGLE_CATEGORY_TO_INDUSTRY[category_lower]
        # Partial match
        for key, val in GOOGLE_CATEGORY_TO_INDUSTRY.items():
            if key in category_lower or category_lower in key:
                return val

    return ""


async def _fetch_website_text_from_enrichment(
    lead_id: str,
    supabase_client: Any,
) -> str:
    """Load website text stored during website scraping from enrichment_data.

    Args:
        lead_id: Lead UUID.
        supabase_client: Supabase client.

    Returns:
        Website text string or "".
    """
    try:
        response = (
            supabase_client.table("enrichment_data")
            .select("raw_result")
            .eq("lead_id", lead_id)
            .eq("source", "website")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if response.data:
            raw = response.data[0].get("raw_result", {})
            return raw.get("website_text", "") or ""
    except Exception:
        pass
    return ""


async def _fetch_website_enrichment_data(
    lead_id: str,
    supabase_client: Any,
) -> dict:
    """Load website enrichment signals (careers page, team count) from DB.

    Args:
        lead_id: Lead UUID.
        supabase_client: Supabase client.

    Returns:
        Dict with website enrichment signals or empty dict.
    """
    try:
        response = (
            supabase_client.table("enrichment_data")
            .select("raw_result")
            .eq("lead_id", lead_id)
            .eq("source", "website")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0].get("raw_result", {}) or {}
    except Exception:
        pass
    return {}
