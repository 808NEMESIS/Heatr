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

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


# Re-analyse window — skip leads waar WI minder dan N dagen oud is
_REANALYSIS_DAYS = int(os.getenv("WEBSITE_REANALYSIS_DAYS", "30"))
# Harde timeout op één analyse (Playwright + Vision + meerdere Claude-calls).
# Vangt async hangs af zodat de dedicated worker doorgaat naar de volgende lead.
_ANALYSIS_TIMEOUT_SECONDS = int(os.getenv("WEBSITE_ANALYSIS_TIMEOUT", "180"))

# Welke email_status waardes gelden als "worth analyzing"
_ELIGIBLE_EMAIL_STATUSES = ("valid", "risky", "catch_all", "catchall_risky")

# Welke lead statuses zijn niet-eligible (terminal)
_TERMINAL_LEAD_STATUSES = ("disqualified", "unsubscribed", "forgotten", "bounced")

# Paginering van de eligible-selectie. We scannen de leads op score aflopend en
# stoppen bij de eerste zónder verse WI-rij. Pagegrootte ≤ 200 houdt de
# WI-dedup-query (.in_) onder de PostgREST-URL-limiet; _MAX_SCAN is een
# veiligheidsplafond tegen een oneindige scan.
_SELECT_PAGE = int(os.getenv("WEBSITE_ANALYSIS_SELECT_PAGE", "200"))
_MAX_SCAN = int(os.getenv("WEBSITE_ANALYSIS_MAX_SCAN", "5000"))


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

        result = await asyncio.wait_for(
            analyze_website(
                lead_id=lead_id,
                domain=domain,
                sector=sector,
                workspace_id=workspace_id,
                supabase_client=supabase_client,
                anthropic_client=anthropic_client,
            ),
            timeout=_ANALYSIS_TIMEOUT_SECONDS,
        )

        duration = round(time.monotonic() - start_ts, 2)
        total_score = (result or {}).get("total_score") or 0

        logger.info(
            "website_analysis_queue: analyzed %s (%s) — score=%d duration=%ss",
            company_name, domain, total_score, duration,
        )

        # Receptie-haakje-ladder (Q4/Q7/Q2/P1) detecteren + persisten. Fail-soft
        # en getimeboxt: mag de analyse-uitkomst nooit beïnvloeden. Dit is de
        # productie-flow-aanroep die de audit miste (er schreef niets hook_*).
        try:
            from website_intelligence.hook_writer import run_receptie_for_lead

            rec = await asyncio.wait_for(
                run_receptie_for_lead(
                    supabase_client,
                    {"id": lead_id, "workspace_id": workspace_id, "domain": domain},
                ),
                timeout=_ANALYSIS_TIMEOUT_SECONDS,
            )
            if rec:
                logger.info(
                    "website_analysis_queue: receptie-haak=%s voor %s (ladder=%s, 2e=%s)",
                    rec.get("hook_code"), domain, rec.get("hook_ladder"), rec.get("second_hook"),
                )
        except Exception as e:
            logger.warning(
                "website_analysis_queue: receptie-detectie faalde voor %s: %s", domain, e,
            )

        return {
            "processed": True,
            "lead_id": lead_id,
            "domain": domain,
            "total_score": total_score,
            "duration_seconds": duration,
        }

    except asyncio.TimeoutError:
        duration = round(time.monotonic() - start_ts, 2)
        logger.warning(
            "website_analysis_queue: analyze_website TIMEOUT (>%ds) voor %s — overgeslagen",
            _ANALYSIS_TIMEOUT_SECONDS, domain,
        )
        _mark_failure(
            supabase_client, lead_id, workspace_id,
            reason=f"analyze_timeout>{_ANALYSIS_TIMEOUT_SECONDS}s",
        )
        return {
            "processed": False, "lead_id": lead_id, "domain": domain,
            "error": f"timeout>{_ANALYSIS_TIMEOUT_SECONDS}s", "duration_seconds": duration,
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

    Priority: hoogste score eerst — maar over de HELE eligible-populatie, niet
    over een top-N-venster.

    Waarom paginerend: PostgREST kan geen SQL-side anti-join tegen
    website_intelligence. De oude implementatie haalde top-20-op-score op en
    filterde dán de al-geanalyseerde eruit — maar juist de hoogst scorende leads
    zijn doorgaans het eerst verrijkt en dus al geanalyseerd, waardoor het venster
    vol al-geanalyseerde leads zat en 'queue leeg' teruggaf terwijl verse, lager
    scorende leads (bv. een nieuwe sweep) nooit bereikt werden — die verhongerden.
    We pagineren nu door de score-gesorteerde eligible leads en stoppen bij de
    eerste zónder verse WI-rij. Secundaire sortering op id maakt de paginering
    deterministisch bij gelijke scores.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_REANALYSIS_DAYS)).isoformat()

    offset = 0
    while offset < _MAX_SCAN:
        leads_res = (
            supabase_client.table("leads")
            .select("id, company_name, domain, sector, email_status, status, score")
            .eq("workspace_id", workspace_id)
            .not_.is_("domain", "null")
            .in_("email_status", list(_ELIGIBLE_EMAIL_STATUSES))
            .order("score", desc=True)
            .order("id", desc=False)
            .range(offset, offset + _SELECT_PAGE - 1)
            .execute()
        )
        page = leads_res.data or []
        if not page:
            return None                     # geen eligible leads meer → queue leeg

        # Terminal statuses eruit (PostgREST kan NOT IN niet netjes uitdrukken)
        candidates = [
            lead for lead in page
            if (lead.get("status") or "") not in _TERMINAL_LEAD_STATUSES
        ]

        if candidates:
            # WI-rijen voor déze pagina (≤ _SELECT_PAGE ids → .in_ blijft binnen
            # de URL-limiet). Een lead met een WI-rij binnen het reanalyse-venster
            # is 'vers geanalyseerd' en wordt overgeslagen.
            candidate_ids = [c["id"] for c in candidates]
            wi_res = (
                supabase_client.table("website_intelligence")
                .select("lead_id, analyzed_at")
                .eq("workspace_id", workspace_id)
                .in_("lead_id", candidate_ids)
                .execute()
            )
            recent_wi: set[str] = set()
            for row in (wi_res.data or []):
                lid = row.get("lead_id")
                analyzed_at = row.get("analyzed_at") or ""
                if lid and analyzed_at and analyzed_at > cutoff:
                    recent_wi.add(lid)

            for lead in candidates:         # al op (score desc, id) gesorteerd
                if lead["id"] not in recent_wi:
                    return lead

        offset += _SELECT_PAGE

    logger.warning(
        "website_analysis_queue: _MAX_SCAN (%d) bereikt zonder eligible lead — "
        "verhoog WEBSITE_ANALYSIS_MAX_SCAN als de voorraad groter is", _MAX_SCAN,
    )
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
