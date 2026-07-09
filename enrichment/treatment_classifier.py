"""
enrichment/treatment_classifier.py — Welke cosmetische behandelingen biedt een site aan?

Datapunt voor Warmr Sequence v1.0 Mail 1 BLOK A. Mail-tekst:
    "... prijsindicatie ontbreekt op [treatment_focus[0]] ..."

Claude Haiku leest de websitetekst en geeft een vrije-tekst lijst van
behandelingen terug. Sanity-filter met allowlist uit
`config/sectors.py::cosmetische_behandelaars.subcategories` + flat ICP-signals
— minimaal één vrije-tekst entry moet matchen met een bekende term, anders
output = []. Dit voorkomt Claude-hallucinatie ("laser" tegen een zorgsite die
helemaal niets met laser doet).

Output blijft vrije-tekst strings zodat Mail 1 direct leesbaar is
("Botox en fillers" in plaats van "injectables_anti_aging").

Cost: 1 Haiku call per lead (~€0.0002). Gecached via utils.claude_cache
op `domain + page_html[:2000]` hash (TTL 30 dagen).

Cost-guard geïntegreerd: check_allowed() vóór de call + charge() erna.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from config.pricing import get_legacy_per_m_eur
from config.sectors import SECTORS
from utils.claude_cache import log_api_cost
from utils.cost_guard import LeadCostAccumulator, guarded_call
from utils.rate_limiter import wait_for_token

logger = logging.getLogger(__name__)

_HAIKU_MODEL = "claude-haiku-4-5-20251001"
# Pricing centraal in config/pricing.py (single source of truth). Per-module
# resolutie at module load tijd; bij Anthropic-prijs-update alleen pricing.py wijzigen.
_PRICING = get_legacy_per_m_eur(_HAIKU_MODEL)
_COST_PER_M_INPUT = _PRICING["input_per_m_eur"]
_COST_PER_M_OUTPUT = _PRICING["output_per_m_eur"]

# Lead-level cost estimate voor cost_guard pre-check. Echte cost is meestal
# lager (korte input + kleine output), maar we zijn conservatief.
_ESTIMATED_COST_EUR = 0.0005

_MAX_RETURNED = 5


def _allowlist_terms() -> set[str]:
    """Flatten subcategory labels + icp_signals uit cosmetische_behandelaars."""
    out: set[str] = set()
    sec = SECTORS.get("cosmetische_behandelaars") or {}
    for sub in (sec.get("subcategories") or {}).values():
        out.add((sub.get("label") or "").lower())
        for s in sub.get("icp_signals") or []:
            out.add(s.lower())
    # Add global lead_keywords too
    for kw in sec.get("lead_keywords") or []:
        out.add(kw.lower())
    out.discard("")
    return out


_ALLOWLIST = _allowlist_terms()


def _sanity_check(items: list[str]) -> list[str]:
    """Filter Claude output: remove items die geen enkele match hebben met allowlist.

    Een item matcht als een allowlist-term een substring is van het item
    of vice versa (case-insensitive). Dit is opzettelijk soepel — Claude
    mag 'Botox behandelingen' zeggen zolang 'botox' bekend is.

    Als minder dan 1 item overblijft: return []. Dat triggert in de caller
    een `null`-waarde i.p.v. vuilnis in de DB.
    """
    kept: list[str] = []
    for raw in items:
        if not isinstance(raw, str):
            continue
        s = raw.strip()
        if not s:
            continue
        s_low = s.lower()
        matched = any(term in s_low or s_low in term for term in _ALLOWLIST)
        if matched:
            kept.append(s)
    return kept[:_MAX_RETURNED]


async def classify_treatment_focus(
    domain: str,
    page_text: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
    lead_id: str | None = None,
    accumulator: LeadCostAccumulator | None = None,
) -> list[str]:
    """Return tot 5 behandelingen die deze site primair aanbiedt, of [].

    Never raises — logt + returnt [] bij elke fout.
    """
    if not page_text or len(page_text) < 200:
        return []

    # Cost gate (daily budget + per-lead ceiling)
    allowed, reason = await guarded_call(
        workspace_id=workspace_id,
        lead_id=lead_id,
        context="treatment_classifier",
        estimated_cost_eur=_ESTIMATED_COST_EUR,
        supabase_client=supabase_client,
        accumulator=accumulator,
    )
    if not allowed:
        logger.info("treatment_classifier: gated for %s — %s", domain, reason)
        return []

    # Rate-limiter (protects Anthropic + Supabase bursts)
    try:
        await wait_for_token("treatment_classifier", supabase_client)
    except Exception as e:
        logger.warning("treatment_classifier: rate-limit wait failed: %s", e)
        return []

    # Dedup-cache key: domain + hash(first 2k chars)
    body_for_hash = page_text[:2000]
    cache_key = hashlib.sha256(
        f"treatment_focus::{domain}::{body_for_hash}".encode("utf-8")
    ).hexdigest()

    try:
        cached_res = (
            supabase_client.table("claude_cache")
            .select("response")   # RECOVERY-FIX: kolom heet 'response', niet
            .eq("cache_key", cache_key)   # 'response_text' → 400 → cache dood
            .maybe_single()
            .execute()
        )
        if cached_res.data and cached_res.data.get("response"):
            try:
                parsed = json.loads(cached_res.data["response"])
                items = parsed.get("treatments") if isinstance(parsed, dict) else None
                if isinstance(items, list):
                    return _sanity_check([str(x) for x in items])
            except (json.JSONDecodeError, ValueError):
                pass
    except Exception as e:
        logger.debug("treatment_classifier: cache lookup failed: %s", e)

    text_capped = page_text[:5000]
    prompt = (
        "Je krijgt de ruwe tekst van een Nederlandse cosmetische kliniek-website. "
        "Welke behandelingen biedt deze kliniek PRIMAIR aan?\n\n"
        "Regels:\n"
        "- Geef max 5 behandelingen, gesorteerd op zichtbaarheid/nadruk op de site.\n"
        "- Gebruik korte Nederlandstalige termen (bijv. 'Botox', 'Fillers', "
        "'Laserontharing', 'Huidverjonging', 'Microblading', 'Permanente make-up').\n"
        "- Als er GEEN duidelijke cosmetische behandelingen staan: return lege lijst.\n"
        "- Verzin geen behandelingen die niet in de tekst staan.\n\n"
        f'Website tekst:\n"""\n{text_capped}\n"""\n\n'
        "Return ALLEEN JSON: {\"treatments\": [\"...\", \"...\"]}"
    )

    try:
        response = await anthropic_client.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=200,
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        logger.warning("treatment_classifier: Claude call failed for %s: %s", domain, e)
        return []

    raw = response.content[0].text.strip() if response.content else ""
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)

    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("treatment_classifier: JSON parse failed for %s: %s", domain, e)
        return []

    items = parsed.get("treatments") if isinstance(parsed, dict) else None
    if not isinstance(items, list):
        return []

    filtered = _sanity_check([str(x) for x in items])

    # Cost accounting
    input_tokens = getattr(response.usage, "input_tokens", 0) or 0
    output_tokens = getattr(response.usage, "output_tokens", 0) or 0
    cost_eur = round(
        (input_tokens * _COST_PER_M_INPUT + output_tokens * _COST_PER_M_OUTPUT) / 1_000_000,
        6,
    )
    try:
        await log_api_cost(
            model=_HAIKU_MODEL,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_eur=cost_eur,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
            context="treatment_classifier",
            lead_id=lead_id,
        )
    except Exception as e:
        logger.debug("treatment_classifier: cost log failed: %s", e)
    if accumulator is not None:
        accumulator.charge(cost_eur, "treatment_classifier")

    # Cache raw response for re-runs. RECOVERY-FIX: kolom is 'response' (niet
    # 'response_text') én 'expires_at' is NOT NULL zonder default → de oude
    # upsert faalde altijd (400/NOT NULL) → cache dood → 100% Claude-kosten.
    try:
        from datetime import datetime as _dt, timedelta as _td, timezone as _tz
        supabase_client.table("claude_cache").upsert({
            "cache_key": cache_key,
            "context": "treatment_classifier",
            "model": _HAIKU_MODEL,
            "prompt_hash": cache_key[:16],
            "response": raw,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "expires_at": (_dt.now(_tz.utc) + _td(days=30)).isoformat(),
        }, on_conflict="cache_key").execute()
    except Exception as e:
        logger.debug("treatment_classifier: cache store failed: %s", e)

    logger.info(
        "treatment_classifier: %s → %s (cost=EUR %.6f)",
        domain, filtered, cost_eur,
    )
    return filtered
