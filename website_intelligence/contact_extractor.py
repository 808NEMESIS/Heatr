"""
website_intelligence/contact_extractor.py — Extract team members from website pages.

Crawls /team, /over-ons, /about, /medewerkers pages and uses Claude Haiku
to extract structured contact person data.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Common team/about page paths to check
_TEAM_PATHS = [
    "/team", "/ons-team", "/over-ons", "/about", "/about-us",
    "/medewerkers", "/wie-zijn-wij", "/het-team", "/over",
    "/mensen", "/adviseurs", "/makelaars", "/behandelaars",
    "/coaches", "/specialisten", "/vaklui",
]


async def extract_contacts_from_website(
    domain: str,
    supabase_client: Any,
    anthropic_client: Any,
) -> list[dict]:
    """
    Extract team members / contact persons from website team/about pages.

    Tries multiple common team page URLs. When a page with names is found,
    uses Claude Haiku to extract structured person data.

    Returns:
        List of dicts with: full_name, first_name, tussenvoegsel, last_name,
        title, bio_snippet, source_url, confidence
    """
    import httpx

    contacts: list[dict] = []
    team_page_text: Optional[str] = None
    source_url: str = ""

    async with httpx.AsyncClient(
        timeout=12.0,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"},
    ) as client:
        for path in _TEAM_PATHS:
            url = f"https://{domain}{path}"
            try:
                r = await client.get(url)
                if r.status_code == 200 and len(r.text) > 500:
                    # Check if page actually contains person-like content
                    text = _strip_html(r.text)
                    if _looks_like_team_page(text):
                        team_page_text = text[:4000]  # Cap for Claude token budget
                        source_url = url
                        break
            except Exception:
                continue

    if not team_page_text:
        logger.debug("No team page found for %s", domain)
        return contacts

    # Use Claude Haiku to extract structured person data
    try:
        contacts = await _extract_with_claude(
            team_page_text, source_url, domain, anthropic_client, supabase_client,
        )
    except Exception as e:
        logger.error("Claude contact extraction failed for %s: %s", domain, e)

    return contacts


async def _extract_with_claude(
    page_text: str,
    source_url: str,
    domain: str,
    anthropic_client: Any,
    supabase_client: Any,
) -> list[dict]:
    """Use Claude Haiku to extract person data from team page text."""
    from utils.claude_cache import cached_claude_call

    prompt = (
        "Extract all team members / staff from this webpage text.\n"
        "For each person, extract:\n"
        "- full_name (string)\n"
        "- title (function/role, string)\n"
        "- bio_snippet (1-sentence summary of their bio if available, string)\n\n"
        "Return ONLY a JSON array. If no persons found, return [].\n"
        "Example: [{\"full_name\": \"Jan de Vries\", \"title\": \"Eigenaar\", \"bio_snippet\": \"15 jaar ervaring als makelaar.\"}]\n\n"
        f"Webpage text:\n{page_text}"
    )

    response_text = await cached_claude_call(
        prompt=prompt,
        cache_key_suffix=f"contact_extract:{domain}",
        model="claude-haiku-4-5-20251001",
        max_tokens=500,
        system="You extract person data from webpage text. Return only valid JSON.",
        supabase_client=supabase_client,
    )

    # Parse JSON response
    import json
    try:
        # Strip markdown code block if present
        text = response_text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)

        raw_contacts = json.loads(text)
        if not isinstance(raw_contacts, list):
            return []
    except (json.JSONDecodeError, ValueError):
        logger.warning("Failed to parse Claude contact extraction for %s", domain)
        return []

    # Structure the output
    contacts: list[dict] = []
    for person in raw_contacts:
        name = person.get("full_name") or ""
        if not name or len(name) < 3:
            continue

        parts = _parse_dutch_name(name)
        contacts.append({
            "full_name": name,
            "first_name": parts["first_name"],
            "tussenvoegsel": parts["tussenvoegsel"],
            "last_name": parts["last_name"],
            "title": person.get("title") or "",
            "bio_snippet": person.get("bio_snippet") or "",
            "source": "website_team_page",
            "source_url": source_url,
            "confidence": 0.9,
        })

    logger.info("Extracted %d contacts from %s", len(contacts), source_url)
    return contacts


def _looks_like_team_page(text: str) -> bool:
    """Heuristic: does this text look like it contains team member info?"""
    text_lower = text.lower()
    # Must have at least some name-like patterns or role keywords
    team_signals = [
        "eigenaar", "oprichter", "directeur", "manager", "partner",
        "makelaar", "coach", "therapeut", "behandelaar", "aannemer",
        "founder", "owner", "ceo", "team", "medewerker",
        "adviseur", "specialist", "vakman",
    ]
    signal_count = sum(1 for s in team_signals if s in text_lower)
    # At least 2 signals and page has reasonable length
    return signal_count >= 2 and len(text) > 200


def _strip_html(html: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_dutch_name(full_name: str) -> dict:
    """Parse a Dutch full name into first, tussenvoegsel, last."""
    parts = full_name.strip().split()
    if len(parts) == 1:
        return {"first_name": parts[0], "tussenvoegsel": "", "last_name": ""}

    tussenvoegsel_words = {"van", "de", "den", "der", "het", "ter", "ten", "te", "in"}
    first_name = parts[0]
    remaining = parts[1:]

    # Find tussenvoegsel
    tv_parts: list[str] = []
    last_parts: list[str] = []
    found_last = False

    for i, word in enumerate(remaining):
        if not found_last and word.lower() in tussenvoegsel_words:
            tv_parts.append(word)
        else:
            found_last = True
            last_parts.append(word)

    # If everything after first name is tussenvoegsel, last word is the last name
    if tv_parts and not last_parts:
        last_parts = [tv_parts.pop()]

    return {
        "first_name": first_name,
        "tussenvoegsel": " ".join(tv_parts),
        "last_name": " ".join(last_parts),
    }
