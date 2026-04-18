"""
enrichment/contact_discovery.py — Find the right decision-maker for a lead.

Uses multiple sources (website team pages, Google Search, LinkedIn snippets)
to find contact persons, ranks them by seniority for the sector, and selects
a primary contact.

Called as step 6 in the enrichment pipeline, after website_intelligence.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from config.sectors import get_sector

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Seniority scoring — higher = more senior / more decision power
# ---------------------------------------------------------------------------
_SENIORITY_MAP: dict[str, int] = {
    # Dutch titles
    "eigenaar": 10, "oprichter": 10, "mede-eigenaar": 10,
    "directeur": 9, "directeur-eigenaar": 10, "dga": 10,
    "managing director": 9, "general manager": 9,
    "partner": 8, "managing partner": 9, "vennoot": 8,
    "vestigingsmanager": 7, "kantoormanager": 7,
    "bedrijfsleider": 7, "praktijkhouder": 9,
    "hoofd marketing": 6, "marketing manager": 6,
    "hoofd commercie": 6, "sales manager": 6,
    "office manager": 5, "projectleider": 5,
    "uitvoerder": 4, "werkvoorbereider": 4,
    "adviseur": 4, "specialist": 3,
    "medewerker": 2, "assistent": 1,
    # English titles
    "founder": 10, "co-founder": 10, "owner": 10,
    "ceo": 9, "coo": 8, "cmo": 7, "cto": 6,
    "head of marketing": 7, "head of growth": 7,
    "head of sales": 7, "vp marketing": 7,
    "director": 8, "manager": 5,
    # Sector-specific
    "makelaar": 6, "register makelaar": 7,
    "coach": 6, "therapeut": 6, "behandelaar": 6,
    "aannemer": 7, "hoofdaannemer": 8,
}


async def discover_contacts(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
) -> list[dict]:
    """
    Find and rank contact persons for a lead.

    Sources (in order):
      1. Website team/about pages (via website_intelligence.team_contacts)
      2. Google Search for LinkedIn profiles
      3. Email-based name inference

    After discovery, selects primary contact and writes all contacts
    to lead_contacts table + updates lead with primary contact info.

    Returns:
        List of discovered contact dicts.
    """
    # Load lead
    lead_res = supabase_client.table("leads").select("*").eq(
        "id", lead_id,
    ).eq("workspace_id", workspace_id).maybe_single().execute()

    if not lead_res.data:
        logger.warning("discover_contacts: lead %s not found", lead_id)
        return []

    lead = lead_res.data
    company_name = lead.get("company_name") or ""
    domain = lead.get("domain") or ""
    city = lead.get("city") or ""
    sector = lead.get("sector") or ""

    all_contacts: list[dict] = []

    # --- Source 1: Website team contacts (from website_intelligence) ---
    try:
        wi_res = supabase_client.table("website_intelligence").select(
            "team_contacts",
        ).eq("lead_id", lead_id).maybe_single().execute()

        if wi_res.data and wi_res.data.get("team_contacts"):
            for contact in wi_res.data["team_contacts"]:
                contact["seniority_score"] = _get_seniority(contact.get("title", ""), sector)
                all_contacts.append(contact)
            logger.info("discover_contacts: %d contacts from website for %s", len(wi_res.data["team_contacts"]), domain)
    except Exception as e:
        logger.debug("discover_contacts: website_intelligence lookup failed: %s", e)

    # --- Source 2: Google Search for LinkedIn profiles ---
    if len(all_contacts) < 2 and company_name:
        try:
            linkedin_contacts = await _search_linkedin(company_name, city, domain)
            for contact in linkedin_contacts:
                # Avoid duplicates
                if not any(_names_match(contact, c) for c in all_contacts):
                    contact["seniority_score"] = _get_seniority(contact.get("title", ""), sector)
                    all_contacts.append(contact)
        except Exception as e:
            logger.debug("discover_contacts: LinkedIn search failed for %s: %s", company_name, e)

    # --- Source 3: Infer from email pattern ---
    email = lead.get("email") or ""
    if email and "@" in email and not any(c.get("source") == "email_inference" for c in all_contacts):
        inferred = _infer_contact_from_email(email, domain)
        if inferred and not any(_names_match(inferred, c) for c in all_contacts):
            all_contacts.append(inferred)

    # --- Verify roles using multi-source signals ---
    for contact in all_contacts:
        contact["confidence"] = verify_contact_role(
            contact=contact,
            lead=lead,
            all_contacts=all_contacts,
        )

    # --- Rank by (confidence * seniority) and select primary ---
    all_contacts.sort(
        key=lambda c: (c.get("confidence", 0) * c.get("seniority_score", 0)),
        reverse=True,
    )

    # Mark primary
    for i, contact in enumerate(all_contacts):
        contact["is_primary"] = (i == 0)

    if all_contacts:
        primary = all_contacts[0]
        primary["why_chosen"] = _generate_why_chosen(primary, company_name, sector)

    # --- Store contacts in lead_contacts table ---
    for contact in all_contacts:
        try:
            supabase_client.table("lead_contacts").insert({
                "workspace_id": workspace_id,
                "lead_id": lead_id,
                "full_name": contact.get("full_name") or "",
                "first_name": contact.get("first_name") or "",
                "tussenvoegsel": contact.get("tussenvoegsel") or "",
                "last_name": contact.get("last_name") or "",
                "title": contact.get("title") or "",
                "seniority_score": contact.get("seniority_score", 0),
                "linkedin_url": contact.get("linkedin_url") or "",
                "email_pattern": contact.get("email_pattern") or "",
                "source": contact.get("source") or "",
                "confidence": contact.get("confidence", 0.3),
                "is_primary": contact.get("is_primary", False),
                "why_chosen": contact.get("why_chosen") or "",
            }).execute()
        except Exception as e:
            logger.debug("Failed to insert lead_contact: %s", e)

    # --- Update lead with primary contact data ---
    if all_contacts:
        primary = all_contacts[0]
        try:
            supabase_client.table("leads").update({
                "contact_first_name": primary.get("first_name") or primary.get("full_name", "").split()[0],
                "contact_last_name": primary.get("last_name") or "",
                "contact_title": primary.get("title") or "",
                "contact_linkedin_url": primary.get("linkedin_url") or "",
                "contact_source": primary.get("source") or "",
                "contact_why_chosen": primary.get("why_chosen") or "",
            }).eq("id", lead_id).execute()
        except Exception as e:
            logger.error("Failed to update lead with primary contact: %s", e)

    logger.info(
        "discover_contacts: lead=%s found=%d primary=%s",
        lead_id, len(all_contacts),
        all_contacts[0].get("full_name") if all_contacts else "none",
    )

    return all_contacts


# ---------------------------------------------------------------------------
# LinkedIn search via Google
# ---------------------------------------------------------------------------

async def _search_linkedin(
    company_name: str,
    city: str,
    domain: str,
) -> list[dict]:
    """Search Google for LinkedIn profiles matching the company."""
    import httpx

    queries = [
        f'site:linkedin.com/in "{company_name}" "{city}"',
        f'site:linkedin.com/in "{domain}" eigenaar OR oprichter OR founder OR directeur',
    ]

    contacts: list[dict] = []

    async with httpx.AsyncClient(
        timeout=10.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    ) as client:
        for query in queries[:1]:  # Only first query to stay under rate limits
            try:
                r = await client.get(
                    "https://www.google.com/search",
                    params={"q": query, "num": 5},
                )
                if r.status_code != 200:
                    continue

                # Extract LinkedIn profile URLs and snippets
                linkedin_pattern = r'https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-]+'
                urls = re.findall(linkedin_pattern, r.text)

                for url in list(dict.fromkeys(urls))[:3]:  # Dedupe, max 3
                    # Try to extract name from URL slug
                    slug = url.rstrip("/").split("/")[-1]
                    name_parts = slug.replace("-", " ").title().split()

                    # Filter out numeric-heavy slugs (not real names)
                    if any(part.isdigit() for part in name_parts[-1:]):
                        name_parts = name_parts[:-1]  # Remove trailing ID

                    if len(name_parts) >= 2:
                        full_name = " ".join(name_parts[:3])
                        contacts.append({
                            "full_name": full_name,
                            "first_name": name_parts[0],
                            "tussenvoegsel": "",
                            "last_name": name_parts[-1] if len(name_parts) > 1 else "",
                            "title": "",
                            "linkedin_url": url,
                            "source": "linkedin_google_search",
                            "confidence": 0.5,
                        })
            except Exception as e:
                logger.debug("LinkedIn Google search failed: %s", e)

    return contacts


# ---------------------------------------------------------------------------
# Email inference
# ---------------------------------------------------------------------------

def _infer_contact_from_email(email: str, domain: str) -> Optional[dict]:
    """Try to infer a contact name from the email local part."""
    local = email.split("@")[0].lower()

    # Skip role emails
    role_prefixes = {"info", "contact", "hallo", "receptie", "administratie",
                     "boekhouding", "support", "sales", "team", "praktijk"}
    if local in role_prefixes:
        return None

    # Try name-like patterns: jan.devries, j.devries, jan
    parts = re.split(r"[._\-]", local)
    if not parts:
        return None

    first_name = parts[0].capitalize()
    last_name = parts[-1].capitalize() if len(parts) > 1 else ""

    if len(first_name) < 2:
        return None

    return {
        "full_name": f"{first_name} {last_name}".strip(),
        "first_name": first_name,
        "tussenvoegsel": "",
        "last_name": last_name,
        "title": "",
        "source": "email_inference",
        "confidence": 0.3,
        "email_pattern": email,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def verify_contact_role(
    contact: dict,
    lead: dict,
    all_contacts: list[dict],
) -> float:
    """
    Verify whether a contact is truly the decision-maker using multiple signals.

    Stacks confidence from independent sources rather than trusting any single one.

    Signal hierarchy:
      KvK "bestuurder"/"eigenaar"                   → +0.35 (official register)
      Only person on team page (1-person business)   → +0.30
      Website "Over mij" page (singular)             → +0.25
      Last name matches domain name                  → +0.20
      LinkedIn title = Owner/Founder + company match → +0.20
      Title contains owner-equivalent keyword        → +0.15
      Company size 1-5 + any senior title            → +0.10
      Email local part = first name                  → +0.05
      Multiple contacts found → lower per-person confidence  → -0.10

    Returns:
        Confidence score 0.0–1.0. Never exceeds 0.95 (absolute certainty
        is not achievable without direct confirmation).
    """
    confidence = 0.0
    title = (contact.get("title") or "").lower()
    source = contact.get("source") or ""
    full_name = (contact.get("full_name") or "").lower()
    last_name = (contact.get("last_name") or "").lower()
    domain = (lead.get("domain") or "").lower().replace("www.", "")

    # --- KvK bestuurder (official register — strongest signal) ---
    kvk_role = (lead.get("kvk_role") or "").lower()
    if kvk_role and any(kw in kvk_role for kw in ("bestuurder", "eigenaar", "vennoot")):
        # Check if this contact's name matches the KvK bestuurder
        kvk_name = (lead.get("kvk_bestuurder_name") or "").lower()
        if kvk_name and (kvk_name in full_name or full_name in kvk_name):
            confidence += 0.35

    # --- Only person on team page (1-person business) ---
    if source == "website_team_page" and len(all_contacts) == 1:
        confidence += 0.30

    # --- Website "Over mij" (singular = sole practitioner) ---
    if source == "website_team_page":
        bio = (contact.get("bio_snippet") or "").lower()
        source_url = (contact.get("source_url") or "").lower()
        if any(kw in source_url for kw in ("/over-mij", "/about-me", "/wie-ben-ik")):
            confidence += 0.25
        elif "mijn praktijk" in bio or "ik ben" in bio or "mijn bedrijf" in bio:
            confidence += 0.20

    # --- Last name in domain name ---
    if last_name and len(last_name) >= 3 and last_name in domain:
        confidence += 0.20

    # --- LinkedIn Owner/Founder with company match ---
    if source == "linkedin_google_search":
        if any(kw in title for kw in ("founder", "owner", "eigenaar", "oprichter")):
            confidence += 0.20
        elif any(kw in title for kw in ("directeur", "ceo", "managing director", "dga")):
            confidence += 0.15

    # --- Title contains owner-equivalent keyword ---
    owner_keywords = {
        "eigenaar", "oprichter", "founder", "owner", "dga",
        "directeur-eigenaar", "praktijkhouder", "mede-eigenaar",
    }
    if any(kw in title for kw in owner_keywords):
        confidence += 0.15

    # --- Small company + senior title ---
    typical_size = (lead.get("company_size_estimate") or "").lower()
    employee_count = lead.get("employee_count") or 0
    is_small = (
        employee_count <= 5
        or "1-5" in typical_size
        or "zzp" in typical_size
        or "eenmanszaak" in (lead.get("company_summary") or "").lower()
    )
    if is_small and _get_seniority(title, lead.get("sector", "")) >= 7:
        confidence += 0.10

    # --- Email pattern matches first name ---
    email = (lead.get("email") or "").lower()
    first_name = (contact.get("first_name") or "").lower()
    if email and first_name and len(first_name) >= 2:
        local = email.split("@")[0]
        if local.startswith(first_name):
            confidence += 0.05

    # --- Multiple contacts found → dilute confidence ---
    if len(all_contacts) > 3:
        confidence -= 0.10
    elif len(all_contacts) > 1:
        confidence -= 0.05

    # --- Low-confidence source penalty ---
    if source == "email_inference":
        confidence = min(confidence, 0.30)  # Cap: email inference alone is weak

    # Never exceed 0.95 (can't be 100% sure without direct confirmation)
    return round(max(0.05, min(confidence, 0.95)), 3)


def _get_seniority(title: str, sector: str) -> int:
    """Look up seniority score for a title. Falls back to keyword matching."""
    if not title:
        return 0

    title_lower = title.lower().strip()

    # Exact match
    if title_lower in _SENIORITY_MAP:
        return _SENIORITY_MAP[title_lower]

    # Partial match — find best matching key
    best_score = 0
    for key, score in _SENIORITY_MAP.items():
        if key in title_lower:
            best_score = max(best_score, score)

    # Sector-specific boost for owner-equivalent titles
    if sector and best_score == 0:
        try:
            sector_config = get_sector(sector)
            dm_titles = sector_config.get("decision_maker_titles", [])
            for dm in dm_titles:
                if dm.lower() in title_lower:
                    best_score = max(best_score, 6)
                    break
        except ValueError:
            pass

    return best_score


def _names_match(a: dict, b: dict) -> bool:
    """Check if two contact dicts refer to the same person."""
    # LinkedIn URL match
    if a.get("linkedin_url") and a.get("linkedin_url") == b.get("linkedin_url"):
        return True

    # Full name match (case-insensitive)
    name_a = (a.get("full_name") or "").lower().strip()
    name_b = (b.get("full_name") or "").lower().strip()
    if name_a and name_b and name_a == name_b:
        return True

    # First + last name match
    if (a.get("first_name") or "").lower() == (b.get("first_name") or "").lower() \
       and (a.get("last_name") or "").lower() == (b.get("last_name") or "").lower() \
       and a.get("first_name"):
        return True

    return False


def _generate_why_chosen(contact: dict, company_name: str, sector: str) -> str:
    """Generate human-readable explanation for why this contact was chosen."""
    title = contact.get("title") or "onbekende functie"
    source = contact.get("source") or "onbekende bron"
    confidence = contact.get("confidence", 0)

    source_labels = {
        "website_team_page": "gevonden op teampagina",
        "linkedin_google_search": "gevonden via LinkedIn",
        "email_inference": "afgeleid uit emailadres",
    }
    source_label = source_labels.get(source, source)

    size_hint = ""
    try:
        cfg = get_sector(sector)
        size_hint = f" ({cfg.get('typical_company_size', '')} medewerkers)"
    except ValueError:
        pass

    return (
        f"{title.title()} bij {company_name}{size_hint}, "
        f"{source_label} (confidence: {confidence:.0%})"
    )
