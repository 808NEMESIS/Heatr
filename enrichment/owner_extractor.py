"""
enrichment/owner_extractor.py — Claude Haiku pakt eigenaar/team uit contact+team pagina tekst.

Input: page_text (gerenderd door website_crawler_v2.py) + company_name + sector.
Output: lijst van {name, role, is_owner, email_hint} met confidence.

Waarom Claude ipv regex:
  - Regex vangt alleen 'namen' als we ze al kennen — circular
  - Claude herkent "Eigenaar: Sami" / "Onze oprichter is Mieke Kuipers" / "Dr. Jansen
    is al 12 jaar onze hoofdbehandelaar" betrouwbaar
  - Sector-context helpt — in cosmetische kliniek is 'kliniekhouder' / 'arts-director'
    de beslisser, in alternatieve praktijk is het 'praktijkhouder' / 'therapeut'

Cost: 1 Haiku call per lead (~€0.0003). Gecached via claude_cache op hash van page_text.
Guarded via utils.cost_guard.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from config.pricing import get_legacy_per_m_eur
from utils.claude_cache import log_api_cost
from utils.cost_guard import LeadCostAccumulator, guarded_call

logger = logging.getLogger(__name__)

_HAIKU_MODEL = "claude-haiku-4-5-20251001"
# Pricing centraal in config/pricing.py.
_PRICING = get_legacy_per_m_eur(_HAIKU_MODEL)
_COST_PER_M_INPUT = _PRICING["input_per_m_eur"]
_COST_PER_M_OUTPUT = _PRICING["output_per_m_eur"]
_ESTIMATED_COST_EUR = 0.0005

# H3-fix (remediation): een geëxtraheerde naam mag alleen als contact_name
# gebruikt worden als hij aantoonbaar in de brontekst voorkomt (geen
# hallucinatie) én de confidence de floor haalt.
OWNER_NAME_CONFIDENCE_FLOOR = 0.5


def _normalize_name(s: str) -> str:
    """Whitespace + case-normalisatie voor naam-matching."""
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def name_in_source(name: str | None, page_text: str | None) -> bool:
    """True als de (voor)naam aantoonbaar in de brontekst voorkomt.

    Matcht op de volledige naam OF op de achternaam (laatste token) — Claude
    geeft soms 'Dr. J. Jansen' terwijl de site 'Jansen' toont. Een losse
    voornaam van <3 tekens telt niet (te veel false matches). Leeg → False.
    """
    n = _normalize_name(name or "")
    text = _normalize_name(page_text or "")
    if not n or not text:
        return False

    def _word_in(needle: str) -> bool:
        # woordgrens-match: 'an' matcht NIET binnen 'jansen'
        return re.search(r"(?<![a-z0-9])" + re.escape(needle) + r"(?![a-z0-9])", text) is not None

    # volledige naam (≥3 tekens) als woord(groep)
    if len(n) >= 3 and _word_in(n):
        return True
    # anders: achternaam (laatste zinvolle token ≥3 tekens) als woord
    tokens = [t for t in n.split(" ") if len(t) >= 3]
    return bool(tokens) and _word_in(tokens[-1])

_ROLES_OWNER = {
    # Dutch
    "eigenaar", "oprichter", "mede-eigenaar", "dga", "directeur-eigenaar",
    "praktijkhouder", "praktijkeigenaar", "kliniekhouder", "kliniekhoudster",
    "founder", "co-founder", "owner",
}


async def extract_team_from_page_text(
    company_name: str,
    sector: str,
    page_text: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
    lead_id: str | None = None,
    accumulator: LeadCostAccumulator | None = None,
) -> list[dict]:
    """Return list of {name, first_name, last_name, role, is_owner, email_hint, confidence}.

    Never raises.
    """
    if not page_text or len(page_text) < 100:
        return []
    if not company_name:
        return []

    # Cost gate
    allowed, reason = await guarded_call(
        workspace_id=workspace_id, lead_id=lead_id,
        context="owner_extract", estimated_cost_eur=_ESTIMATED_COST_EUR,
        supabase_client=supabase_client, accumulator=accumulator,
    )
    if not allowed:
        logger.info("owner_extract gated for %s — %s", company_name, reason)
        return []

    # Cache-key (domain/company + hash of first 2000 chars text)
    cache_key = hashlib.sha256(
        f"owner_extract::{company_name}::{page_text[:2000]}".encode("utf-8")
    ).hexdigest()
    try:
        cached = (
            supabase_client.table("claude_cache")
            .select("response_text")
            .eq("cache_key", cache_key)
            .maybe_single()
            .execute()
        )
        if cached.data and cached.data.get("response_text"):
            try:
                parsed = json.loads(cached.data["response_text"])
                if isinstance(parsed, list):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
    except Exception as e:
        logger.debug("owner_extract: cache lookup failed: %s", e)

    text_capped = page_text[:8000]
    sector_hint = {
        "cosmetische_behandelaars": "cosmetische kliniek (botox/filler/laser/huidtherapie)",
        "alternatieve_geneeskunde": "alternatieve zorg praktijk (acupunctuur/osteopathie/natuurgeneeskunde/etc.)",
    }.get(sector, "Nederlands MKB-bedrijf")

    prompt = (
        f"Je krijgt de gerenderde tekst van de contact-/team-/over-ons-pagina's van "
        f"'{company_name}' — een {sector_hint}.\n\n"
        f"Identificeer de mensen (teamleden, behandelaars, eigenaren, oprichters) die "
        f"op deze pagina's genoemd worden. Voor elke persoon:\n"
        f"- `name`: volledige naam (bv. 'Dr. Sami Jansen')\n"
        f"- `first_name`: voornaam\n"
        f"- `last_name`: achternaam (optioneel, null als onbekend)\n"
        f"- `role`: functie/rol zoals vermeld (bv. 'praktijkhouder', 'osteopaat', 'eigenaar')\n"
        f"- `is_owner`: true als expliciet genoemd als eigenaar/oprichter/DGA/praktijkhouder/kliniekhouder\n"
        f"- `confidence`: 0-1 hoe zeker je bent (1 = naam + rol expliciet vermeld)\n\n"
        f"Regels:\n"
        f"- Verzin GEEN namen die er niet staan.\n"
        f"- Als geen personen genoemd worden: return lege lijst.\n"
        f"- Haal alleen echte personen eruit, geen fictieve namen of testimonials.\n"
        f"- Als er maar 1 persoon is en het bedrijf draagt zijn/haar naam: is_owner=true.\n\n"
        f'Website tekst:\n"""\n{text_capped}\n"""\n\n'
        f'Return ALLEEN JSON array: [{{"name": "...", "first_name": "...", "last_name": "...", "role": "...", "is_owner": true, "confidence": 0.9}}]'
    )

    try:
        response = await anthropic_client.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=800,
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        logger.warning("owner_extract Claude call failed for %s: %s", company_name, e)
        return []

    raw = response.content[0].text.strip() if response.content else ""
    # Strip markdown fence
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
    # Claude soms prefixt met korte uitleg — pak alleen van eerste '[' tot laatste ']'
    first_bracket = raw.find("[")
    last_bracket = raw.rfind("]")
    if first_bracket >= 0 and last_bracket > first_bracket:
        raw = raw[first_bracket:last_bracket + 1]

    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("owner_extract: JSON parse failed for %s: %s (raw=%r)", company_name, e, raw[:200])
        return []
    if not isinstance(parsed, list):
        return []

    # Normalize + sanity-filter
    cleaned: list[dict] = []
    for item in parsed[:10]:
        if not isinstance(item, dict):
            continue
        name = (item.get("name") or "").strip()
        if not name or len(name) < 3:
            continue
        first_name = (item.get("first_name") or "").strip()
        role = (item.get("role") or "").strip().lower()
        is_owner_flag = bool(item.get("is_owner"))
        # Auto-promote to owner if role matches
        if not is_owner_flag and role in _ROLES_OWNER:
            is_owner_flag = True
        cleaned.append({
            "name": name,
            "first_name": first_name or name.split()[0] if name else "",
            "last_name": item.get("last_name"),
            "role": item.get("role"),
            "is_owner": is_owner_flag,
            "confidence": max(0.0, min(1.0, float(item.get("confidence") or 0.5))),
        })

    # Cost accounting
    input_tokens = getattr(response.usage, "input_tokens", 0) or 0
    output_tokens = getattr(response.usage, "output_tokens", 0) or 0
    cost_eur = round(
        (input_tokens * _COST_PER_M_INPUT + output_tokens * _COST_PER_M_OUTPUT) / 1_000_000, 6,
    )
    try:
        await log_api_cost(
            model=_HAIKU_MODEL, input_tokens=input_tokens, output_tokens=output_tokens,
            cost_eur=cost_eur, workspace_id=workspace_id, supabase_client=supabase_client,
            context="owner_extract", lead_id=lead_id,
        )
    except Exception as e:
        logger.debug("owner_extract: cost log failed: %s", e)
    if accumulator is not None:
        accumulator.charge(cost_eur, "owner_extract")

    # Cache raw response
    try:
        supabase_client.table("claude_cache").upsert({
            "cache_key": cache_key,
            "model": _HAIKU_MODEL,
            "prompt_hash": cache_key,
            "response_text": json.dumps(cleaned),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }, on_conflict="cache_key").execute()
    except Exception as e:
        logger.debug("owner_extract: cache store failed: %s", e)

    logger.info(
        "owner_extract: %s → %d people (owners=%d) cost=EUR %.6f",
        company_name, len(cleaned), sum(1 for p in cleaned if p["is_owner"]), cost_eur,
    )
    return cleaned
