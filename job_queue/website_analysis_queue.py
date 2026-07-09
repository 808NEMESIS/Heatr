"""
job_queue/website_analysis_queue.py — Aparte queue voor website intelligence analyses.

Waarom een eigen queue?
  Website analysis is de duurste enrichment step (Claude Sonnet Vision +
  PageSpeed API + Playwright). CLAUDE.md regel 115 noemt dit expliciet als
  aparte queue zodat het een eigen rate-limit + retry gedrag kan hebben,
  los van de bulk enrichment pipeline.

Dit bestand heeft geen eigen queue-tabel. Eligibility voor analyse wordt
afgeleid uit `heatr_leads` en `heatr_website_intelligence`:

  Een lead is eligible als:
    - workspace_id matcht
    - domain != NULL
    - email_status IN ('valid','risky','catch_all')  (gate: geen Claude credits voor onbereikbare leads)
    - status NIET IN ('disqualified','unsubscribed','forgotten')
    - geen recente row in heatr_website_intelligence (dedup)

Workers roepen `process_next_website_analysis()` aan. Die functie:
  1. Selecteert één eligible lead
  2. Draait `is_real_website()` pre-screen (cheap HTTP check)
  3. Draait `analyze_website()` uit website_intelligence.analyzer
  4. Retourneert result dict of None als queue leeg is
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


# Re-analyse window — skip leads waar WI minder dan N dagen oud is
_REANALYSIS_DAYS = int(os.getenv("WEBSITE_REANALYSIS_DAYS", "30"))

# Welke email_status waardes gelden als "worth analyzing"
_ELIGIBLE_EMAIL_STATUSES = ("valid", "risky", "catch_all", "catchall_risky")

# Welke lead statuses zijn niet-eligible (terminal)
_TERMINAL_LEAD_STATUSES = ("disqualified", "unsubscribed", "forgotten", "bounced")


async def process_next_website_analysis(
    workspace_id: str,
    supabase_client: Any,
) -> dict | None:
    """Claim en analyseer één eligible lead.

    Called by n8n workflow 04-website-analysis-worker every minute.

    Args:
        workspace_id: Workspace slug — strict filter, nooit cross-workspace.
        supabase_client: Supabase client (heatr_ prefix wrapper).

    Returns:
        Result dict {processed, lead_id, domain, total_score, duration_seconds}
        of None als er geen eligible lead is (queue_empty).
        Bij error: {processed: False, error: "..."} — raised nooit.
    """
    try:
        lead = await _find_next_eligible_lead(workspace_id, supabase_client)
    except Exception as e:
        logger.error(
            "process_next_website_analysis: lead selection failed: %s", e,
        )
        return {"processed": False, "error": f"selection_failed: {str(e)[:100]}"}

    if not lead:
        return None

    lead_id = lead["id"]
    domain = lead.get("domain") or ""
    sector = lead.get("sector") or ""
    company_name = lead.get("company_name") or "?"

    start_ts = time.monotonic()

    # Pre-screen: is dit een echte website?
    try:
        from enrichment.website_prescreener import is_real_website
        is_real, reason = await is_real_website(domain)
    except Exception as e:
        logger.warning(
            "website_analysis_queue: prescreen failed for %s: %s",
            domain, e,
        )
        is_real, reason = False, f"prescreen_error: {str(e)[:60]}"

    if not is_real:
        logger.info(
            "website_analysis_queue: skipping %s — not a real website (%s)",
            domain, reason,
        )
        _mark_failure(
            supabase_client, lead_id, workspace_id,
            reason=f"prescreen_fail: {reason}",
        )
        return {
            "processed": False,
            "lead_id": lead_id,
            "domain": domain,
            "reason": f"prescreen_fail: {reason}",
        }

    # Run website analysis — catch per-lead errors, nooit pipeline stoppen
    try:
        anthropic_client = _get_anthropic_client()
        from website_intelligence.analyzer import analyze_website

        result = await analyze_website(
            lead_id=lead_id,
            domain=domain,
            sector=sector,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
            anthropic_client=anthropic_client,
        )

        duration = round(time.monotonic() - start_ts, 2)
        total_score = (result or {}).get("total_score") or 0

        logger.info(
            "website_analysis_queue: analyzed %s (%s) — score=%d duration=%ss",
            company_name, domain, total_score, duration,
        )

        return {
            "processed": True,
            "lead_id": lead_id,
            "domain": domain,
            "total_score": total_score,
            "duration_seconds": duration,
        }

    except Exception as e:
        duration = round(time.monotonic() - start_ts, 2)
        logger.error(
            "website_analysis_queue: analyze_website failed for %s: %s",
            domain, e,
        )
        _mark_failure(
            supabase_client, lead_id, workspace_id,
            reason=f"analyze_error: {str(e)[:120]}",
        )
        return {
            "processed": False,
            "lead_id": lead_id,
            "domain": domain,
            "error": f"analyze_failed: {str(e)[:100]}",
            "duration_seconds": duration,
        }


# =============================================================================
# Helpers
# =============================================================================

async def _find_next_eligible_lead(
    workspace_id: str,
    supabase_client: Any,
) -> dict | None:
    """Select one lead eligible for website analysis.

    Priority: highest score eerst (leads die al enriched zijn en
    dicht bij push-ready staan krijgen voorrang op website analyse).

    Implementation detail: we doen geen SQL-side anti-join tegen
    website_intelligence — Supabase PostgREST ondersteunt dat niet zonder
    RPC. In plaats daarvan fetchen we top-N eligible leads, filteren
    Python-side op 'geen recente WI row', pakken de eerste.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_REANALYSIS_DAYS)).isoformat()

    # Step 1: fetch top candidate leads for this workspace
    leads_res = (
        supabase_client.table("leads")
        .select(
            "id, company_name, domain, sector, email_status, status, score"
        )
        .eq("workspace_id", workspace_id)
        .not_.is_("domain", "null")
        .in_("email_status", list(_ELIGIBLE_EMAIL_STATUSES))
        .order("score", desc=True)
        .limit(20)
        .execute()
    )
    candidates = leads_res.data or []

    if not candidates:
        return None

    # Filter terminal statuses (cannot express NOT IN cleanly in PostgREST)
    candidates = [
        lead for lead in candidates
        if (lead.get("status") or "") not in _TERMINAL_LEAD_STATUSES
    ]

    if not candidates:
        return None

    # Step 2: fetch existing WI rows for these candidates (single query)
    candidate_ids = [c["id"] for c in candidates]
    wi_res = (
        supabase_client.table("website_intelligence")
        .select("lead_id, analyzed_at")
        .eq("workspace_id", workspace_id)
        .in_("lead_id", candidate_ids)
        .execute()
    )
    wi_rows = wi_res.data or []

    # Map lead_id → most recent analyzed_at
    recent_wi: dict[str, str] = {}
    for row in wi_rows:
        lid = row.get("lead_id")
        if not lid:
            continue
        analyzed_at = row.get("analyzed_at") or ""
        # Keep only if within reanalysis window
        if analyzed_at and analyzed_at > cutoff:
            prev = recent_wi.get(lid, "")
            if analyzed_at > prev:
                recent_wi[lid] = analyzed_at

    # Pick first candidate that has no recent WI
    for lead in candidates:
        if lead["id"] not in recent_wi:
            return lead

    return None


def _mark_failure(
    supabase_client: Any,
    lead_id: str,
    workspace_id: str,
    reason: str,
) -> None:
    """Note failure reason on the lead. Silent on DB errors.

    Requires columns website_analysis_failed_reason + website_analysis_failed_at
    on heatr_leads. If those don't exist the update just no-ops.
    """
    try:
        supabase_client.table("leads").update({
            "website_analysis_failed_reason": reason[:300],
            "website_analysis_failed_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", lead_id).eq("workspace_id", workspace_id).execute()
    except Exception as e:
        logger.debug("_mark_failure: couldn't persist reason (%s): %s", reason, e)


def _get_anthropic_client() -> Any:
    """Instantiate Anthropic client from env. Never cached — workers are short-lived.

    RECOVERY-FIX: moet AsyncAnthropic zijn. De consumers (sector_checker,
    visual_analyzer) doen `await client.messages.create(...)`; een sync
    anthropic.Anthropic gaf een TypeError → gevangen → sector_score=0 én een
    geblokkeerde event-loop. De enrichment-queue gebruikte al bewust
    AsyncAnthropic; deze dedicated-queue-factory was niet meegefixt.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    import anthropic
    return anthropic.AsyncAnthropic(api_key=api_key)
