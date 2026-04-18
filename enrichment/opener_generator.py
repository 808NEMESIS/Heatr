"""
enrichment/opener_generator.py — Convert detected problems into outreach openers.

Combines 3 data sources into ranked, personalized email openers:
  1. Website gaps (from conversion_checker, technical_checker, sector_checker)
  2. Competitor benchmarking (from competitor_analyzer)
  3. Google review pain points (from review_analyzer)

Rule-based mapping first (no Claude), then 1 Claude Haiku call to turn
the top 3 combinations into natural Dutch opener sentences.

Cost: 1 Claude Haiku call per lead (~€0.00020)
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# =============================================================================
# Gap → Pain Point mapping (rule-based, 0 Claude cost)
# =============================================================================

GAP_TO_PAIN: dict[str, dict] = {
    "no_online_booking": {
        "observation": "Geen online boekingssysteem gevonden",
        "template": "Ik zag dat klanten bij {company} nog niet online een afspraak kunnen maken",
        "priority": 1,
        "sectors": ["alternatieve_geneeskunde", "cosmetische_behandelaars", "makelaars"],
    },
    "no_whatsapp": {
        "observation": "Geen WhatsApp integratie",
        "template": "Veel {sector_label} in {city} bieden hun klanten WhatsApp aan voor snelle vragen — bij {company} zag ik dit nog niet",
        "priority": 2,
        "sectors": ["alternatieve_geneeskunde", "cosmetische_behandelaars", "bouwbedrijven"],
    },
    "no_chatbot": {
        "observation": "Geen live chat of chatbot",
        "template": "Op {domain} miste ik een chatfunctie voor bezoekers die snel een vraag willen stellen",
        "priority": 3,
        "sectors": None,  # All sectors
    },
    "no_cta_above_fold": {
        "observation": "Geen duidelijke call-to-action boven de vouw",
        "template": "Het eerste wat bezoekers zien op {domain} is geen actieknop — dat kost waarschijnlijk klanten",
        "priority": 4,
        "sectors": None,
    },
    "no_ssl": {
        "observation": "Website heeft geen SSL-certificaat",
        "template": "Browsers tonen een 'niet veilig' melding op {domain} — dat schrikt bezoekers af",
        "priority": 1,
        "sectors": None,
    },
    "low_pagespeed": {
        "observation": "Website laadt traag op mobiel",
        "template": "Op mobiel laadt {domain} traag — meer dan de helft van jullie bezoekers zit op een telefoon",
        "priority": 3,
        "sectors": None,
    },
    "too_many_form_fields": {
        "observation": "Contactformulier heeft te veel velden",
        "template": "Het contactformulier op {domain} heeft veel velden — elk extra veld kost gemiddeld 10% conversie",
        "priority": 5,
        "sectors": None,
    },
    "no_team_page": {
        "observation": "Geen zichtbare teampagina",
        "template": "Op {domain} miste ik een teampagina — bezoekers willen weten wie er achter {company} zit",
        "priority": 6,
        "sectors": ["makelaars", "alternatieve_geneeskunde"],
    },
    "no_reviews_visible": {
        "observation": "Geen klantreviews op de website",
        "template": "Jullie hebben mooie reviews op Google, maar op {domain} zijn die niet zichtbaar voor bezoekers",
        "priority": 4,
        "sectors": None,
    },
    "outdated_copyright": {
        "observation": "Copyright jaar is verouderd",
        "template": "De copyright op {domain} staat nog op een ouder jaar — dat kan de indruk wekken dat de site niet bijgehouden wordt",
        "priority": 7,
        "sectors": None,
    },
}

SECTOR_LABELS = {
    "makelaars": "makelaars",
    "alternatieve_geneeskunde": "therapeuten",
    "cosmetische_behandelaars": "klinieken",
    "bouwbedrijven": "aannemers",
}


def map_gaps_to_pain_points(
    conversion_result: dict,
    technical_result: dict,
    sector_result: dict,
    sector: str,
) -> list[dict]:
    """
    Map detected website gaps to ranked pain points.
    Pure rule-based — no Claude call.

    Returns list of pain point dicts sorted by priority (1 = strongest).
    """
    pains: list[dict] = []

    # Conversion gaps
    if not conversion_result.get("has_online_booking"):
        pains.append(_make_pain("no_online_booking", sector))
    if not conversion_result.get("has_whatsapp"):
        pains.append(_make_pain("no_whatsapp", sector))
    if not conversion_result.get("has_chatbot"):
        pains.append(_make_pain("no_chatbot", sector))
    if not conversion_result.get("has_cta_above_fold"):
        pains.append(_make_pain("no_cta_above_fold", sector))
    if conversion_result.get("form_field_count", 0) > 5:
        pains.append(_make_pain("too_many_form_fields", sector))

    # Technical gaps
    if not technical_result.get("has_ssl"):
        pains.append(_make_pain("no_ssl", sector))
    pagespeed = technical_result.get("pagespeed_mobile") or 0
    if 0 < pagespeed < 50:
        pains.append(_make_pain("low_pagespeed", sector))

    # Sector-specific gaps
    sector_checks = sector_result.get("checks") or []
    team_check = next((c for c in sector_checks if c.get("key") == "has_team_page"), None)
    if team_check and not team_check.get("passed"):
        pains.append(_make_pain("no_team_page", sector))
    review_check = next((c for c in sector_checks if c.get("key") == "has_client_reviews"), None)
    if review_check and not review_check.get("passed"):
        pains.append(_make_pain("no_reviews_visible", sector))

    # Filter to sector-relevant pains and sort by priority
    relevant = [p for p in pains if p is not None]
    relevant.sort(key=lambda p: p.get("priority", 99))

    return relevant


def enrich_with_competitor_context(
    pain_points: list[dict],
    competitor_data: dict,
    sector: str,
    city: str,
) -> list[dict]:
    """Add competitor comparison to pain points where applicable."""
    if not competitor_data or not competitor_data.get("competitors"):
        return pain_points

    comps = competitor_data["competitors"]
    score_vs_market = competitor_data.get("score_vs_market", 0)
    market_avg = competitor_data.get("market_avg_score", 0)
    comp_names = [c.get("name") for c in comps[:2] if c.get("name")]

    for pain in pain_points:
        if pain.get("pain_id") in ("no_online_booking", "no_whatsapp", "no_chatbot"):
            # Add specific competitor mention
            if comp_names:
                pain["competitor_context"] = (
                    f"{comp_names[0]} in {city} heeft dit wel"
                )
        if score_vs_market < -5:
            pain.setdefault("competitor_context", "")
            if not pain["competitor_context"]:
                pain["competitor_context"] = (
                    f"Jullie website scoort {abs(score_vs_market)} punten onder het gemiddelde in {city}"
                )

    return pain_points


def enrich_with_review_context(
    pain_points: list[dict],
    review_analysis: dict,
) -> list[dict]:
    """Add review quotes to matching pain points."""
    if not review_analysis:
        return pain_points

    best_quote = review_analysis.get("best_quote") or ""
    review_pains = set(review_analysis.get("aerys_relevant_pains") or [])

    for pain in pain_points:
        pain_id = pain.get("pain_id", "")
        if pain_id in review_pains:
            # This pain point is confirmed by customer reviews
            pain["review_confirmed"] = True
            pain["priority"] = max(1, pain.get("priority", 5) - 2)  # Boost priority

    # Add best quote to the highest-priority review-confirmed pain
    if best_quote:
        confirmed = [p for p in pain_points if p.get("review_confirmed")]
        if confirmed:
            confirmed[0]["review_quote"] = best_quote
        elif pain_points:
            pain_points[0]["review_quote"] = best_quote

    # Re-sort after priority adjustments
    pain_points.sort(key=lambda p: p.get("priority", 99))
    return pain_points


async def generate_openers(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
) -> list[dict]:
    """
    Generate 3 ranked outreach openers combining all data sources.

    Loads lead data + website intelligence + reviews + competitors,
    runs the mapping pipeline, then one Claude Haiku call to
    write natural openers.

    Returns:
        List of 3 opener dicts: [{rank, hook_type, opener, pain_point, data_sources}, ...]
    """
    # Load lead
    lead_res = supabase_client.table("leads").select("*").eq("id", lead_id).maybe_single().execute()
    if not lead_res.data:
        return []
    lead = lead_res.data
    company = lead.get("company_name") or ""
    domain = lead.get("domain") or ""
    city = lead.get("city") or ""
    sector = lead.get("sector") or ""

    # Load website intelligence
    wi_res = supabase_client.table("website_intelligence").select("*").eq(
        "lead_id", lead_id,
    ).maybe_single().execute()
    wi = wi_res.data if wi_res.data else {}

    conv = wi.get("conversion_details") or {}
    tech = wi.get("technical_details") or {}
    sec = wi.get("sector_details") or {}
    comp = wi.get("competitor_data") or {}
    review = lead.get("review_analysis") or {}

    # Step 1: Map gaps to pain points (rule-based)
    pains = map_gaps_to_pain_points(conv, tech, sec, sector)

    # Step 2: Enrich with competitor context
    pains = enrich_with_competitor_context(pains, comp, sector, city)

    # Step 3: Enrich with review context
    pains = enrich_with_review_context(pains, review)

    if not pains:
        logger.info("opener_generator: no pain points found for %s", company)
        return []

    # Step 4: Generate natural openers via Claude (top 3 pains)
    top_pains = pains[:3]
    sector_label = SECTOR_LABELS.get(sector, sector)

    pain_descriptions = []
    for p in top_pains:
        desc = p.get("observation", "")
        if p.get("competitor_context"):
            desc += f" — {p['competitor_context']}"
        if p.get("review_quote"):
            desc += f" — Een klant schreef: \"{p['review_quote']}\""
        pain_descriptions.append(desc)

    from utils.claude_cache import cached_claude_call

    prompt = (
        f"Schrijf 3 openingszinnen voor een zakelijke outreach email.\n\n"
        f"Bedrijf: {company}\n"
        f"Website: {domain}\n"
        f"Stad: {city}\n"
        f"Sector: {sector_label}\n"
        f"Contactpersoon: {lead.get('contact_first_name') or 'de eigenaar'}\n\n"
        f"Gedetecteerde problemen (gebruik deze als basis):\n"
    )
    for i, desc in enumerate(pain_descriptions, 1):
        prompt += f"{i}. {desc}\n"

    prompt += (
        "\nRegels:\n"
        "- Begin NIET met 'Ik'\n"
        "- Max 2 zinnen per opener\n"
        "- Verwijs naar een specifiek probleem op HUN website\n"
        "- Geen verkooppraatje, alleen observatie + vraag\n"
        "- Wees concreet en eerlijk\n"
        "- Nederlands\n\n"
        "Return JSON array met 3 objecten: [{\"opener\": \"...\", \"pain\": \"...\"}]\n"
        "Sorteer op sterkste eerst."
    )

    openers: list[dict] = []
    try:
        response = await cached_claude_call(
            prompt=prompt,
            cache_key_suffix=f"opener:{lead_id}",
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system="Je schrijft outreach openers voor een webbureau. Kort, concreet, gebaseerd op echte website-observaties. Alleen valid JSON.",
            supabase_client=supabase_client,
        )

        import json
        import re
        text = response.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)

        parsed = json.loads(text)
        if isinstance(parsed, list):
            for rank, item in enumerate(parsed[:3], 1):
                sources = ["website_analysis"]
                pain_id = top_pains[rank - 1]["pain_id"] if rank <= len(top_pains) else ""
                if top_pains[rank - 1].get("competitor_context") if rank <= len(top_pains) else False:
                    sources.append("competitor_benchmark")
                if top_pains[rank - 1].get("review_quote") if rank <= len(top_pains) else False:
                    sources.append("google_reviews")

                openers.append({
                    "rank": rank,
                    "opener": item.get("opener") or "",
                    "pain_point": item.get("pain") or pain_id,
                    "hook_type": "review_pain" if "google_reviews" in sources else "competitor_pain" if "competitor_benchmark" in sources else "website_gap",
                    "data_sources": sources,
                })
    except Exception as e:
        logger.warning("opener_generator: Claude call failed for %s: %s", company, e)

    # Store openers on lead
    if openers:
        try:
            supabase_client.table("leads").update({
                "outreach_hooks": openers,
                "personalized_opener": openers[0]["opener"],
            }).eq("id", lead_id).execute()
        except Exception as e:
            logger.debug("opener_generator: failed to store: %s", e)

    logger.info("opener_generator: %s — %d openers generated", company, len(openers))
    return openers


def _make_pain(pain_id: str, sector: str) -> dict | None:
    """Create a pain point dict from the mapping, checking sector relevance."""
    gap = GAP_TO_PAIN.get(pain_id)
    if not gap:
        return None
    allowed_sectors = gap.get("sectors")
    if allowed_sectors and sector not in allowed_sectors:
        return None
    return {
        "pain_id": pain_id,
        "observation": gap["observation"],
        "template": gap["template"],
        "priority": gap["priority"],
    }
