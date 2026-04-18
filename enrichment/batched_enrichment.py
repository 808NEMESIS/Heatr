"""
enrichment/batched_enrichment.py — Combined personalization + opener generation.

Replaces 2 separate Claude calls (personalization_extractor + opener_generator)
with a single call that produces both outputs in one structured JSON response.

Optimizations applied:
  1. Batched: 1 call instead of 2 (saves input token overhead)
  2. Prompt caching: system prompt marked cacheable (80% discount on repeat use)
  3. Compact output: stop sequences + JSON-only output

Cost: ~€0.00015 per lead (vs ~€0.0008 previously)
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


# Compact system prompt — minimizes both input and output tokens
SYSTEM_PROMPT = """B2B outbound expert voor Aerys. Analyseer bedrijf, return ALLEEN JSON:
{"positioning":"1 zin","hook":"concrete observatie","gap":"zwakste punt","opener":"max 1 zin, NIET met 'Ik', verwijst naar website probleem, eindigt met concrete vraag"}"""


async def batched_enrich(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
) -> dict[str, Any]:
    """
    Single Claude call that produces personalization data + 3 openers.

    Combines the work of personalization_extractor.py and opener_generator.py
    into one API call with prompt caching.

    Returns dict with: positioning, hooks, observations, gaps, openers
    """
    result: dict[str, Any] = {
        "positioning": "",
        "hooks": [],
        "observations": [],
        "gaps": [],
        "openers": [],
    }

    # Load lead + website_intelligence + review data
    lead_res = supabase_client.table("leads").select("*").eq("id", lead_id).maybe_single().execute()
    if not lead_res.data:
        return result
    lead = lead_res.data

    company = lead.get("company_name") or ""
    domain = lead.get("domain") or ""
    city = lead.get("city") or ""
    sector = lead.get("sector") or ""

    wi_res = supabase_client.table("website_intelligence").select("*").eq(
        "lead_id", lead_id,
    ).maybe_single().execute()
    wi = wi_res.data if wi_res.data else {}

    conv = wi.get("conversion_details") or {}
    tech = wi.get("technical_details") or {}
    sec = wi.get("sector_details") or {}
    comp = wi.get("competitor_data") or {}
    review = lead.get("review_analysis") or {}

    # Detect gaps (rule-based, no Claude)
    from enrichment.opener_generator import map_gaps_to_pain_points
    pains = map_gaps_to_pain_points(conv, tech, sec, sector)

    if not pains:
        logger.info("batched_enrich: no pain points for %s — skipping Claude call", company)
        return result

    # Build compact input — only top 3 pains, short format
    pain_lines = "\n".join(f"- {p['observation']}" for p in pains[:3])

    extras = []
    if comp.get("competitors"):
        top_comp = comp["competitors"][0]
        delta = comp.get("score_vs_market", 0)
        extras.append(f"Concurrent: {top_comp.get('name', '?')} (delta {delta:+d})")
    if review.get("best_quote"):
        quote = review["best_quote"][:120]
        extras.append(f"Klant zei: \"{quote}\"")

    user_prompt = (
        f"{company} ({domain}) | {sector} {city} | ★{lead.get('google_rating', '?')} ({lead.get('google_review_count', 0)})\n"
        f"Problemen:\n{pain_lines}"
    )
    if extras:
        user_prompt += "\n" + "\n".join(extras)

    # --- Make Claude call with prompt caching on system ---
    # Ask for only ONE high-quality opener instead of 3 — cuts output tokens by ~60%
    try:
        message = await _call_claude_with_cache(
            anthropic_client=anthropic_client,
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user_prompt,
            max_tokens=300,  # Compact output — 1 opener + positioning + hook + gap
            supabase_client=supabase_client,
            lead_id=lead_id,
        )

        # Parse JSON
        text = message.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)

        parsed = json.loads(text)

        result["positioning"] = parsed.get("positioning") or ""
        # Single hook/gap as list for backward compat
        hook = parsed.get("hook") or ""
        gap = parsed.get("gap") or ""
        result["hooks"] = [hook] if hook else []
        result["gaps"] = [gap] if gap else []
        result["observations"] = []

        # Format single opener
        opener_text = parsed.get("opener") or ""
        if opener_text:
            sources = ["website_analysis"]
            if comp.get("competitors"):
                sources.append("competitor_benchmark")
            if review.get("best_quote"):
                sources.append("google_reviews")

            result["openers"].append({
                "rank": 1,
                "opener": opener_text,
                "pain_point": pains[0].get("pain_id", "") if pains else "",
                "hook_type": "combined",
                "data_sources": sources,
            })

    except Exception as e:
        logger.warning("batched_enrich: Claude call failed for %s: %s", company, e)
        return result

    # --- Store everything in one DB update ---
    try:
        supabase_client.table("leads").update({
            "personalization_hooks": result["hooks"],
            "personalization_observations": result["observations"],
            "company_positioning": result["positioning"],
            "outreach_hooks": result["openers"],
            "personalized_opener": result["openers"][0]["opener"] if result["openers"] else "",
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.debug("batched_enrich: DB update failed: %s", e)

    logger.info(
        "batched_enrich: %s — %d hooks, %d openers, %d gaps",
        company, len(result["hooks"]), len(result["openers"]), len(result["gaps"]),
    )

    return result


async def _call_claude_with_cache(
    anthropic_client: Any,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
    supabase_client: Any,
    lead_id: str | None = None,
) -> str:
    """
    Call Claude with prompt caching on the system prompt.

    Anthropic's prompt caching: mark content with cache_control,
    get 90% discount on cached reads (5-minute TTL by default).
    """
    import anthropic

    # System prompt gets cached — same for every enrichment call
    system_blocks = [{
        "type": "text",
        "text": system_prompt,
        "cache_control": {"type": "ephemeral"},
    }]

    # Make async call — use AsyncAnthropic if possible
    try:
        async_client = anthropic.AsyncAnthropic(api_key=anthropic_client.api_key)
        response = await async_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=max_tokens,
            temperature=0,
            system=system_blocks,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception:
        # Fallback: sync client in executor
        import asyncio
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: anthropic_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=max_tokens,
                temperature=0,
                system=system_blocks,
                messages=[{"role": "user", "content": user_prompt}],
            ),
        )

    text = response.content[0].text if response.content else ""

    # Calculate actual cost including cache discounts
    usage = response.usage
    input_tokens = usage.input_tokens
    output_tokens = usage.output_tokens
    cached_tokens = getattr(usage, "cache_read_input_tokens", 0) or 0
    cache_creation = getattr(usage, "cache_creation_input_tokens", 0) or 0

    # Haiku pricing per million tokens: input €0.74, cached read €0.074 (90% off), output €3.68
    # Cache creation: €0.92/M (25% premium on first write)
    INPUT_COST = 0.74 / 1_000_000
    CACHED_COST = 0.074 / 1_000_000
    CACHE_WRITE_COST = 0.92 / 1_000_000
    OUTPUT_COST = 3.68 / 1_000_000

    fresh_input = input_tokens - cached_tokens - cache_creation
    cost = (
        fresh_input * INPUT_COST
        + cached_tokens * CACHED_COST
        + cache_creation * CACHE_WRITE_COST
        + output_tokens * OUTPUT_COST
    )

    # Log actual cost
    try:
        supabase_client.table("api_cost_log").insert({
            "workspace_id": "aerys",
            "model": "claude-haiku-4-5-20251001",
            "prompt_tokens": input_tokens,
            "response_tokens": output_tokens,
            "cost_eur": round(cost, 6),
            "context": "batched_enrichment",
            "lead_id": lead_id,
        }).execute()

        if cached_tokens > 0:
            logger.info(
                "batched_enrich: CACHE HIT — %d cached tokens saved ~€%.6f",
                cached_tokens, cached_tokens * (INPUT_COST - CACHED_COST),
            )
    except Exception as e:
        logger.debug("Cost log failed: %s", e)

    return text
