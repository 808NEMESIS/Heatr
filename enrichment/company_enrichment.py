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

_INDUSTRY_LIST_ALTERNATIEVE_GENEESKUNDE = [
    "Acupunctuurpraktijk", "Osteopathiepraktijk", "Homeopathiepraktijk",
    "Chiropractiepraktijk", "Natuurgeneeskunde", "Haptotherapie",
    "Reflexologie", "Reiki / energetische therapie",
    "Manuele therapie", "Integratieve therapie",
    "Kruidengeneeskunde", "Ayurveda", "Chinese geneeskunde",
    "Holistische therapie", "Overige alternatieve geneeskunde",
]

_INDUSTRY_LIST_COSMETISCHE_BEHANDELAARS = [
    "Cosmetische kliniek", "Botox/filler kliniek", "Laserkliniek",
    "Huidtherapiepraktijk", "Permanente make-up studio",
    "Microblading studio", "Schoonheidssalon premium",
    "Gezichtsbehandeling specialist", "Ontharing/waxing salon",
    "Anti-aging kliniek", "Medisch esthetiek",
    "Wimperextensions studio", "Overige cosmetische behandelingen",
]

_INDUSTRY_LISTS: dict[str, list[str]] = {
    "alternatieve_geneeskunde": _INDUSTRY_LIST_ALTERNATIEVE_GENEESKUNDE,
    "cosmetische_behandelaars": _INDUSTRY_LIST_COSMETISCHE_BEHANDELAARS,
}

_OPENER_LANGUAGE_NL = "nl"
_HAIKU_MODEL = "claude-haiku-4-5-20251001"


async def _account_claude(response: Any, context: str, cost_ctx: dict | None) -> None:
    """RECOVERY-FIX (Patch 5b): registreer de kosten van één Claude-call.

    company_enrichment's 3 Haiku-calls (industry/summary/opener) omzeilden vóór
    deze fix ALLE kostentracking (geen guarded_call, geen accumulator, geen
    log_api_cost) → onzichtbaar voor de per-lead-cap, de dag/maand-guard én
    /analytics/enrichment-cost. Deze helper doet de post-call boekhouding
    identiek aan de andere modules. Geen cost_ctx → no-op (bv. losse aanroep).
    """
    if not cost_ctx:
        return
    try:
        from config.pricing import get_price_eur
        from utils.claude_cache import log_api_cost

        usage = getattr(response, "usage", None)
        in_tok = int(getattr(usage, "input_tokens", 0) or 0)
        out_tok = int(getattr(usage, "output_tokens", 0) or 0)
        cost_eur = get_price_eur(_HAIKU_MODEL, in_tok, out_tok)
        await log_api_cost(
            model=_HAIKU_MODEL, input_tokens=in_tok, output_tokens=out_tok,
            cost_eur=cost_eur, workspace_id=cost_ctx.get("workspace_id"),
            supabase_client=cost_ctx.get("supabase_client"), context=context,
            lead_id=cost_ctx.get("lead_id"),
        )
        acc = cost_ctx.get("accumulator")
        if acc is not None:
            acc.charge(cost_eur, context)
    except Exception as e:
        logger.debug("company_enrichment cost-accounting faalde (%s): %s", context, e)


# =============================================================================
# Public API
# =============================================================================

async def enrich_company(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
    accumulator: Any = None,
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
                "kvk_number, kvk_sbi_code, "
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
    kvk_employee_range = lead.get("kvk_employee_count_range")  # safe: always None if column missing
    domain = lead.get("domain", "")

    # --- Cost-gate + accounting-context (recovery Patch 5b) ------------------
    # Alle Claude-calls hieronder lopen nu door de kostenbewaking: guarded_call
    # als budget-poort, en _account_claude logt/charged elke call. Wordt de
    # budget-poort gesloten (dag/maand-cap of per-lead-ceiling), dan slaan we de
    # Claude-delen over en vallen we terug op lokale industry-inferentie.
    cost_ctx = {
        "workspace_id": workspace_id,
        "lead_id": lead_id,
        "supabase_client": supabase_client,
        "accumulator": accumulator,
    }
    from utils.cost_guard import guarded_call
    claude_allowed, gate_reason = await guarded_call(
        workspace_id=workspace_id,
        lead_id=lead_id,
        context="company_enrichment",
        estimated_cost_eur=0.001,   # ~3 Haiku-calls (industry+summary+opener)
        supabase_client=supabase_client,
        accumulator=accumulator,
    )
    if not claude_allowed:
        logger.info("company_enrichment: Claude gated voor %s — %s", lead_id, gate_reason)

    # --- Industry inference --------------------------------------------------
    industry = _infer_industry_local(kvk_sbi, google_category, sector_key)

    if not industry and claude_allowed:
        # Claude fallback — only if local inference fails
        website_text = await _fetch_website_text_from_enrichment(lead_id, supabase_client)
        if website_text or google_category:
            industry = await infer_industry_claude(
                website_text=website_text,
                google_category=google_category,
                sector_key=sector_key,
                anthropic_client=anthropic_client,
                cost_ctx=cost_ctx,
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
    if company_name and claude_allowed:
        result["company_summary"] = await generate_company_summary(
            company_name=company_name,
            industry=result["industry"],
            city=city,
            website_text=website_text,
            anthropic_client=anthropic_client,
            cost_ctx=cost_ctx,
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
            cost_ctx=cost_ctx,
            anthropic_client=anthropic_client,
        )

    # --- Normaliseer vrije Claude-tekst vóór opslag (P1/C2) -----------------
    # Ruwe output werd ongeschoond opgeslagen → 89% markdown/meta-vervuild.
    # Corrupte output (refusal/leeg) wordt NIET als productieveld opgeslagen.
    from utils.text_normalizer import normalize_generated_text
    opener_clean, opener_ok, opener_reason = normalize_generated_text(
        result.get("personalized_opener"), max_sentences=3)
    summary_clean, summary_ok, _ = normalize_generated_text(
        result.get("company_summary"), max_sentences=3)
    if not opener_ok:
        logger.warning("company_enrichment: opener afgekeurd (%s) voor lead %s — niet opgeslagen",
                       opener_reason, lead_id)

    # --- Persist to leads table ---------------------------------------------
    try:
        version = (lead.get("enrichment_version") or 0) + 1
        patch = {
            "industry": result["industry"],
            "company_size_estimate": result["company_size_estimate"],
            "enrichment_version": version,
        }
        # alleen schone, gevalideerde tekst opslaan; afgekeurd → veld ongemoeid
        if opener_ok:
            patch["personalized_opener"] = opener_clean
        if summary_ok:
            patch["company_summary"] = summary_clean
        supabase_client.table("leads").update(patch).eq("id", lead_id).execute()
    except Exception as e:
        logger.error("Failed to persist enrichment for lead %s: %s", lead_id, e)

    # --- Store raw Claude output in enrichment_data -------------------------
    try:
        supabase_client.table("enrichment_data").insert({
            "workspace_id": workspace_id,
            "lead_id": lead_id,
            "step": "claude_enrichment",   # NOT NULL — recovery-fix: ontbrak → 23502
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
    cost_ctx: dict | None = None,
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
        response = await anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=30,
            messages=[{"role": "user", "content": prompt}],
        )
        await _account_claude(response, "company_enrichment:industry", cost_ctx)
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
    cost_ctx: dict | None = None,
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
        response = await anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        await _account_claude(response, "company_enrichment:summary", cost_ctx)
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
    cost_ctx: dict | None = None,
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

    # Alleen actieve + backwards-compat sectoren. makelaars/bouwbedrijven
    # verwijderd per ICP-versmalling (c153454); chiropractoren toegevoegd.
    tone_guidance = {
        "alternatieve_geneeskunde": "warm, persoonlijk, respectvol — geen harde verkoop",
        "cosmetische_behandelaars": "stijlvol, resultaatgericht, professioneel — geen harde verkoop",
        "chiropractoren": "vakkundig, patiëntgericht, nuchter — geen harde verkoop",
    }.get(sector_key, "professioneel en persoonlijk")

    # System-prompt vervangen 2026-05-07 (v3.1 templates-sessie):
    # rustig zelfvertrouwen + concrete observatie ipv vleierij/sales-taal.
    # Behoud max_tokens=1200 + temperature=0.7 zoals afgesproken — niet aanraken.
    system_prompt = (
        "Je schrijft de openingszin(nen) van een cold outreach mail vanuit Sami "
        "Jansema van Aerys Solution naar een Nederlandse SMB-ondernemer.\n\n"
        "VEREISTEN:\n"
        "- Direct beginnen met de openingszin, geen markdown-header, geen preamble\n"
        "- 25-50 woorden, eindigen met punt of vraagteken\n"
        "- Concrete observatie over DIT bedrijf — gebruik specifieke datapunten uit "
        "lead-data (cijfers, treatment-namen, plaatsnaam, opvallende details)\n"
        "- Toon: rustig zelfvertrouwen, energiek, niet sales-y, niet vleierig\n"
        "- Geen aanhef (\"Hoi X,\") — alleen de body-opener\n\n"
        "VERBODEN:\n"
        "- Em-dashes (—) — gebruik komma of punt\n"
        "- \"Uw\" en \"u\" tenzij specifiek beslisser-titel pattern (Drs., Prof., "
        "advocaat-context)\n"
        "- \"Sparren\", \"vrijblijvend\", \"kennismaken\", \"synergie\", "
        "\"in gesprek gaan\"\n"
        "- \"Indrukwekkend\", \"uitstekend\", \"perfect\", \"geweldig\"\n"
        "- \"Spreekt voor zich\", \"getuigt van\", \"reflecteert duidelijk\"\n"
        "- \"Wij zijn de beste\", \"marktleider\", \"toonaangevend\"\n\n"
        "GEWENSTE STIJL:\n"
        "- Concreet en specifiek over dit bedrijf\n"
        "- Spontaan en menselijk, alsof Sami even zijn nek uitsteekt\n"
        "- Korte zinnen mogen, fragmentzinnen ook (\"Sterk gebouwd.\" als losse zin)\n"
        "- Eerlijke observatie boven slimme framing\n\n"
        "VOORBEELDEN GOED:\n\n"
        "\"Plastische Chirurgie Groningen heeft 49 reviews met een 5.0 staan. Dat soort "
        "positie bouw je niet op met een gemiddelde patiëntervaring.\"\n\n"
        "\"Annebeth Kroeskop draait al jaren een sterke praktijk in Amsterdam, blijkt "
        "uit de 50 reviews met 4.9 sterren. Maar de website? Die loopt achter.\"\n\n"
        "\"Aerys Solution zet zich op Instagram neer met een herkenbare lijn. Niet veel "
        "SMB-bureaus krijgen dat voor elkaar.\"\n\n"
        "VOORBEELDEN NIET GOED:\n\n"
        "\"De uitstekende Google-beoordelingen van [bedrijf] — 49 recensies met een "
        "perfecte 5.0 score — getuigen van de kwaliteit en patiënttevredenheid die uw "
        "praktijk kenmerkt.\" (Reden: vleierij, em-dash, \"uw\", \"getuigen van\", "
        "generieke woorden)\n\n"
        "\"[bedrijf] is een toonaangevende speler in [sector] en jullie reputatie "
        "spreekt voor zich.\" (Reden: holle claim, \"spreekt voor zich\", marketing-taal)\n\n"
        "Antwoord nu met alleen de openingszin(nen) voor de meegegeven lead."
    )

    # User-prompt = compacte lead-context. Tone-guidance zit nu in system-prompt;
    # hier alleen de feiten over deze specifieke lead.
    salutation_hint = f"contact: {contact_name}" if contact_name else f"contact: onbekend"
    user_prompt = (
        f"Bedrijfsnaam: {company_name}\n"
        f"Stad: {city}\n"
        f"Branche: {industry or '(onbekend)'}\n"
        f"Sector-tone: {tone_guidance}\n"
        f"Belangrijkste signaal: {signal_text}\n"
        f"{salutation_hint}\n"
        f"Bedrijfsbeschrijving: {summary or '(geen)'}"
    )

    try:
        response = await anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1200,
            temperature=0.7,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        await _account_claude(response, "company_enrichment:opener", cost_ctx)
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
        # H1-fix: de crawler slaat op onder source='contact_crawl_v2' met key
        # 'page_text' — NIET source='website'/'website_text' (die bestaan
        # nergens). De oude query retourneerde daardoor structureel "" →
        # industry/summary/size uitgehongerd.
        response = (
            supabase_client.table("enrichment_data")
            .select("raw_result")
            .eq("lead_id", lead_id)
            .eq("source", "contact_crawl_v2")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if response.data:
            raw = response.data[0].get("raw_result", {}) or {}
            return raw.get("page_text", "") or ""
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
        # H1-fix: zie _fetch_website_text_from_enrichment — bron is
        # 'contact_crawl_v2', niet 'website'.
        response = (
            supabase_client.table("enrichment_data")
            .select("raw_result")
            .eq("lead_id", lead_id)
            .eq("source", "contact_crawl_v2")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if response.data:
            return response.data[0].get("raw_result", {}) or {}
    except Exception:
        pass
    return {}
