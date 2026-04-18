"""
queue/enrichment_queue.py — Async enrichment pipeline manager.

Processes leads through the full enrichment sequence after scraping completes.
The final step (inbox_selection) contacts Warmr so each lead knows which
sending inbox will be used before it reaches the Warmr push gate.

Enrichment sequence per lead:
  1. website            — website_scraper (CMS, emails, signals)
  2. email_waterfall    — 4-step email discovery + verification
  3. kvk               — KvK data enrichment (NL only)
  4. company_enrichment — industry, summary, opener via Claude Haiku
  5. scoring            — stub (always 0 until session 4)
  6. inbox_selection    — select best Warmr inbox + set leads.preferred_inbox_id

Warmr inbox list is cached in system_state for 15 minutes to avoid flooding
the Warmr API during bulk enrichment runs.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from integrations.warmr_client import WarmrClient, WarmrAPIError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_RETRIES = 3
_WORKER_SLEEP_SECONDS = 10
_INBOX_CACHE_KEY = "warmr_inboxes_cache"
_INBOX_CACHE_MINUTES = 15

# Recipient email → provider bucket mapping
_MICROSOFT_DOMAINS = {
    "outlook.com", "hotmail.com", "hotmail.nl", "live.com", "live.nl",
    "msn.com", "ziggo.nl", "upcmail.nl",
}
_GOOGLE_DOMAINS = {"gmail.com", "googlemail.com"}


# =============================================================================
# Job queueing
# =============================================================================

async def queue_lead_for_enrichment(
    lead_id: str,
    workspace_id: str,
    priority: int = 5,
    enrichment_types: list[str] | None = None,
    supabase_client: Any = None,
) -> str | None:
    """Insert a lead into the enrichment_jobs queue.

    Args:
        lead_id: UUID of the lead to enrich.
        workspace_id: Workspace slug.
        priority: Job priority, lower = higher priority (1 = urgent, 10 = low).
        enrichment_types: List of enrichment step names to run. Defaults to
                          all six steps in sequence.
        supabase_client: Supabase client.

    Returns:
        Enrichment job UUID, or None on insert failure.
    """
    if enrichment_types is None:
        enrichment_types = [
            "website",
            "email_waterfall",
            "kvk",
            "company_enrichment",
            "website_intelligence",
            "contact_discovery",
            "data_verification",
            "scoring",
            "inbox_selection",
        ]

    record = {
        "workspace_id": workspace_id,
        "lead_id": lead_id,
        "status": "pending",
        "current_step": 1,
        "steps_completed": [],
        "priority": priority,
        "enrichment_types": enrichment_types,
        "retry_count": 0,
    }

    try:
        response = supabase_client.table("enrichment_jobs").insert(record).execute()
        job_id = response.data[0]["id"]
        logger.debug("Enrichment job queued: lead=%s job=%s", lead_id, job_id)
        return job_id
    except Exception as e:
        logger.error("Failed to queue enrichment job for lead %s: %s", lead_id, e)
        return None


async def queue_all_unenriched_leads(
    workspace_id: str,
    supabase_client: Any,
) -> int:
    """Queue all discovered, unenriched leads for enrichment.

    Finds all leads with status='discovered' and enrichment_version=0,
    queues them all at default priority.

    Args:
        workspace_id: Workspace slug.
        supabase_client: Supabase client.

    Returns:
        Number of leads queued.
    """
    try:
        response = (
            supabase_client.table("leads")
            .select("id")
            .eq("workspace_id", workspace_id)
            .eq("status", "discovered")
            .eq("enrichment_version", 0)
            .execute()
        )
    except Exception as e:
        logger.error("queue_all_unenriched_leads: failed to fetch leads: %s", e)
        return 0

    leads = response.data or []
    queued = 0
    for lead in leads:
        result = await queue_lead_for_enrichment(
            lead_id=lead["id"],
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )
        if result:
            queued += 1

    logger.info("Queued %d leads for enrichment in workspace %s", queued, workspace_id)
    return queued


# =============================================================================
# Job claiming
# =============================================================================

async def claim_next_enrichment_job(
    supabase_client: Any,
) -> dict | None:
    """Atomically claim the next pending enrichment job.

    Claims the job with the lowest priority value (most urgent), then
    earliest queued_at among ties.

    Args:
        supabase_client: Supabase client.

    Returns:
        Full job row dict if claimed, None if no pending jobs.
    """
    try:
        response = (
            supabase_client.table("enrichment_jobs")
            .select("*")
            .eq("status", "pending")
            .order("priority", desc=False)
            .order("created_at", desc=False)
            .limit(1)
            .execute()
        )

        if not response.data:
            return None

        job = response.data[0]
        job_id = job["id"]
        now_iso = datetime.now(timezone.utc).isoformat()

        update = (
            supabase_client.table("enrichment_jobs")
            .update({
                "status": "running",
                "started_at": now_iso,
            })
            .eq("id", job_id)
            .eq("status", "pending")  # Guard against concurrent workers
            .execute()
        )

        if not update.data:
            return None  # Another worker claimed it

        return update.data[0]

    except Exception as e:
        logger.error("claim_next_enrichment_job failed: %s", e)
        return None


# =============================================================================
# Job execution
# =============================================================================

async def run_enrichment_for_lead(
    job: dict,
    supabase_client: Any,
    anthropic_client: Any,
    warmr_client: WarmrClient,
) -> None:
    """Execute all enrichment steps for a single lead job.

    Runs steps in fixed order. Priority is boosted after email_waterfall
    if a valid email is found. Each step is independent — failure of one
    step does not prevent subsequent steps from running.

    Args:
        job: Enrichment job row dict from enrichment_jobs.
        supabase_client: Supabase client.
        anthropic_client: Initialised Anthropic client.
        warmr_client: Initialised WarmrClient.
    """
    lead_id: str = job["lead_id"]
    workspace_id: str = job.get("workspace_id", "")
    job_id: str = job["id"]
    enrichment_types: list[str] = job.get("enrichment_types") or [
        "website", "email_waterfall", "kvk",
        "company_enrichment", "website_intelligence",
        "contact_discovery", "data_verification",
        "scoring", "inbox_selection",
    ]

    logger.info("Enrichment started: lead=%s job=%s", lead_id, job_id)

    # Load lead to determine country (needed for KvK skip logic)
    lead_country = await _get_lead_field(lead_id, "country", supabase_client) or "NL"

    for step_name in enrichment_types:
        try:
            logger.debug("Running enrichment step: %s for lead %s", step_name, lead_id)
            await _run_step(
                step_name=step_name,
                lead_id=lead_id,
                workspace_id=workspace_id,
                lead_country=lead_country,
                supabase_client=supabase_client,
                anthropic_client=anthropic_client,
                warmr_client=warmr_client,
            )

            # Priority boost: after waterfall, if valid email → make urgent
            if step_name == "email_waterfall":
                email_status = await _get_lead_field(lead_id, "email_status", supabase_client)
                if email_status == "valid":
                    await _boost_job_priority(job_id, priority=2, supabase_client=supabase_client)

        except Exception as e:
            logger.warning(
                "Enrichment step %s failed for lead %s (non-fatal): %s",
                step_name, lead_id, e,
            )
            # Continue to next step — never let one step failure stop the pipeline

    await complete_enrichment_job(job_id, supabase_client)
    logger.info("Enrichment complete: lead=%s", lead_id)


async def _run_step(
    step_name: str,
    lead_id: str,
    workspace_id: str,
    lead_country: str,
    supabase_client: Any,
    anthropic_client: Any,
    warmr_client: WarmrClient,
) -> None:
    """Dispatch a single enrichment step to the correct function.

    Args:
        step_name: Step identifier string.
        lead_id: Lead UUID.
        workspace_id: Workspace slug.
        lead_country: ISO country code for KvK gating.
        supabase_client: Supabase client.
        anthropic_client: Anthropic client.
        warmr_client: WarmrClient.
    """
    if step_name == "website":
        lead_domain = await _get_lead_field(lead_id, "domain", supabase_client)
        if lead_domain:
            from scrapers.website_scraper import scrape_website
            website_data = await scrape_website(
                domain=lead_domain,
                lead_id=lead_id,
                workspace_id=workspace_id,
                supabase_client=supabase_client,
            )
            # Patch useful fields back to the lead row
            patch: dict = {}
            if website_data.get("cms") and website_data["cms"] != "unknown":
                patch["cms_detected"] = website_data["cms"]
            if website_data.get("has_instagram"):
                patch["has_instagram"] = True
            if website_data.get("has_online_booking"):
                patch["has_online_booking"] = True
            if website_data.get("has_whatsapp"):
                patch["has_whatsapp"] = True
            if website_data.get("phone"):
                patch["phone"] = website_data["phone"]
            if website_data.get("tracking_tools"):
                patch["tracking_tools"] = website_data["tracking_tools"]
            if patch:
                supabase_client.table("leads").update(patch).eq("id", lead_id).execute()

    elif step_name == "email_waterfall":
        from enrichment.email_waterfall import run_waterfall_for_lead
        await run_waterfall_for_lead(lead_id=lead_id, supabase_client=supabase_client)

    elif step_name == "kvk":
        if lead_country.upper() != "NL":
            logger.debug("Skipping KvK step for non-NL lead %s", lead_id)
            return
        lead_domain = await _get_lead_field(lead_id, "domain", supabase_client)
        lead_name = await _get_lead_field(lead_id, "company_name", supabase_client)
        lead_city = await _get_lead_field(lead_id, "city", supabase_client)
        if lead_name:
            from scrapers.kvk_scraper import enrich_company_kvk
            kvk_data = await enrich_company_kvk(
                domain=lead_domain or "",
                company_name=lead_name or "",
                city=lead_city or "",
                workspace_id=workspace_id,
                supabase_client=supabase_client,
            )
            if kvk_data:
                patch = {}
                if kvk_data.get("kvk_number"):
                    patch["kvk_number"] = kvk_data["kvk_number"]
                if kvk_data.get("kvk_sbi_code"):
                    patch["kvk_sbi_code"] = kvk_data["kvk_sbi_code"]
                if patch:
                    supabase_client.table("leads").update(patch).eq("id", lead_id).execute()

    elif step_name == "company_enrichment":
        from enrichment.company_enrichment import enrich_company
        await enrich_company(
            lead_id=lead_id,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
            anthropic_client=anthropic_client,
        )

    elif step_name == "website_intelligence":
        # Gate: only run if lead has a valid domain + email (avoid wasting Claude credits)
        domain = await _get_lead_field(lead_id, "domain", supabase_client)
        email_status = await _get_lead_field(lead_id, "email_status", supabase_client)
        if domain and email_status in ("valid", "risky", "catch_all"):
            # Pre-screen: is this a real website or a parked/placeholder domain?
            from enrichment.website_prescreener import is_real_website
            is_real, prescreen_reason = await is_real_website(domain)
            if not is_real:
                logger.info("Skipping website_intelligence: %s is not a real website (%s)", domain, prescreen_reason)
            else:
                from website_intelligence.analyzer import analyze_website
                sector = await _get_lead_field(lead_id, "sector", supabase_client) or ""
                await analyze_website(
                    lead_id=lead_id,
                    domain=domain,
                    sector=sector,
                    workspace_id=workspace_id,
                    supabase_client=supabase_client,
                    anthropic_client=anthropic_client,
                )
        else:
            logger.info("Skipping website_intelligence: no domain or email for lead %s", lead_id)

    elif step_name == "contact_discovery":
        from enrichment.contact_discovery import discover_contacts
        await discover_contacts(
            lead_id=lead_id,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
            anthropic_client=anthropic_client,
        )

    elif step_name == "data_verification":
        from enrichment.data_verification import verify_lead_data
        await verify_lead_data(
            lead_id=lead_id,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )

    elif step_name == "scoring":
        from scoring.lead_scoring import score_lead
        await score_lead(
            lead_id=lead_id,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )

    elif step_name == "inbox_selection":
        await select_and_store_preferred_inbox(
            lead_id=lead_id,
            supabase_client=supabase_client,
            warmr_client=warmr_client,
        )

    else:
        logger.warning("Unknown enrichment step: %s", step_name)


# =============================================================================
# Inbox selection
# =============================================================================

async def select_and_store_preferred_inbox(
    lead_id: str,
    supabase_client: Any,
    warmr_client: WarmrClient,
) -> str | None:
    """Select the optimal Warmr sending inbox for a lead and store it.

    Selection logic:
      1. Get ready inboxes from Warmr (15-minute cache)
      2. Detect recipient's email provider (google / microsoft / unknown)
      3. Filter to matching provider inboxes, sort by reputation DESC
      4. Sort remaining by occupancy rate ASC (least busy first)
      5. Take first result as preferred_inbox_id

    Args:
        lead_id: Lead UUID.
        supabase_client: Supabase client.
        warmr_client: WarmrClient.

    Returns:
        Selected inbox_id string, or None if no inboxes available.
    """
    # Get ready inboxes (cached)
    inboxes = await _get_cached_inboxes(supabase_client, warmr_client)

    if not inboxes:
        logger.warning("No ready Warmr inboxes — lead %s set to queued_no_inbox", lead_id)
        try:
            supabase_client.table("leads").update({
                "status": "queued_no_inbox",
            }).eq("id", lead_id).execute()
        except Exception as e:
            logger.error("Failed to update lead status to queued_no_inbox: %s", e)
        return None

    # Get lead email to determine recipient provider
    lead_email = await _get_lead_field(lead_id, "email", supabase_client) or ""
    recipient_provider = _detect_email_provider(lead_email)

    # Select optimal inbox
    selected_inbox = _select_best_inbox(inboxes, recipient_provider)

    if not selected_inbox:
        logger.warning("Inbox selection failed for lead %s", lead_id)
        return None

    inbox_id = selected_inbox.get("id") or selected_inbox.get("inbox_id")

    # Persist to leads table
    try:
        email_status = await _get_lead_field(lead_id, "email_status", supabase_client)
        new_status = "qualified" if email_status in ("valid", "risky") else "enriched"

        supabase_client.table("leads").update({
            "preferred_inbox_id": inbox_id,
            "status": new_status,
        }).eq("id", lead_id).execute()

        logger.info(
            "Inbox selected for lead %s: inbox=%s provider=%s status=%s",
            lead_id, inbox_id, recipient_provider, new_status,
        )
    except Exception as e:
        logger.error("Failed to store preferred inbox for lead %s: %s", lead_id, e)

    return inbox_id


def _detect_email_provider(email: str) -> str:
    """Detect whether an email address belongs to Google, Microsoft, or other.

    Args:
        email: Email address string.

    Returns:
        'google' | 'microsoft' | 'unknown'
    """
    if not email or "@" not in email:
        return "unknown"
    domain = email.split("@")[1].lower()
    if domain in _GOOGLE_DOMAINS:
        return "google"
    if domain in _MICROSOFT_DOMAINS:
        return "microsoft"
    return "unknown"


def _select_best_inbox(
    inboxes: list[dict],
    recipient_provider: str,
) -> dict | None:
    """Select the optimal inbox from the ready inbox list.

    Priority order:
      1. Provider-matching inboxes (google recipient → google inbox)
      2. Sorted by reputation_score DESC
      3. Sorted by occupancy rate ASC (daily_sent / daily_campaign_target)
      4. Fallback: best reputation regardless of provider

    Args:
        inboxes: List of ready inbox dicts from Warmr.
        recipient_provider: 'google' | 'microsoft' | 'unknown'.

    Returns:
        Best inbox dict, or None if list is empty.
    """
    if not inboxes:
        return None

    def _occupancy(inbox: dict) -> float:
        sent = inbox.get("daily_sent", 0) or 0
        target = inbox.get("daily_campaign_target", 1) or 1
        return sent / target

    def _sort_key(inbox: dict) -> tuple:
        reputation = float(inbox.get("reputation_score", 0) or 0)
        occ = _occupancy(inbox)
        return (-reputation, occ)  # Higher reputation first, lower occupancy first

    # Filter to provider-matching inboxes
    if recipient_provider != "unknown":
        provider_match = [
            i for i in inboxes
            if (i.get("provider") or "").lower() == recipient_provider
        ]
        if provider_match:
            return sorted(provider_match, key=_sort_key)[0]

    # Fallback: best inbox regardless of provider
    return sorted(inboxes, key=_sort_key)[0]


async def _get_cached_inboxes(
    supabase_client: Any,
    warmr_client: WarmrClient,
) -> list[dict]:
    """Fetch Warmr inbox list from cache or live API.

    Caches for 15 minutes in system_state to avoid flooding Warmr during
    bulk enrichment runs where inbox_selection runs for hundreds of leads.

    Args:
        supabase_client: Supabase client.
        warmr_client: WarmrClient.

    Returns:
        List of ready inbox dicts.
    """
    import json

    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # Try cache first
    try:
        response = (
            supabase_client.table("system_state")
            .select("value, expires_at")
            .eq("key", _INBOX_CACHE_KEY)
            .gte("expires_at", now_iso)
            .limit(1)
            .execute()
        )
        if response.data:
            cached = response.data[0].get("value", "[]")
            return json.loads(cached)
    except Exception:
        pass

    # Cache miss — fetch from Warmr
    try:
        inboxes = await warmr_client.get_ready_inboxes()
    except WarmrAPIError as e:
        logger.error("Failed to fetch Warmr inboxes: %s", e)
        return []

    # Store in cache
    expires = now + timedelta(minutes=_INBOX_CACHE_MINUTES)
    try:
        supabase_client.table("system_state").upsert({
            "key": _INBOX_CACHE_KEY,
            "value": json.dumps(inboxes),
            "expires_at": expires.isoformat(),
        }).execute()
    except Exception as e:
        logger.warning("Failed to cache inbox list: %s", e)

    return inboxes


# =============================================================================
# Job lifecycle
# =============================================================================

async def complete_enrichment_job(job_id: str, supabase_client: Any) -> None:
    """Mark an enrichment job as completed.

    Args:
        job_id: Enrichment job UUID.
        supabase_client: Supabase client.
    """
    try:
        supabase_client.table("enrichment_jobs").update({
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", job_id).execute()
    except Exception as e:
        logger.error("complete_enrichment_job failed for %s: %s", job_id, e)


async def fail_enrichment_job(
    job_id: str,
    error: str,
    supabase_client: Any,
) -> None:
    """Record a failure and requeue or permanently fail the job.

    Retries up to _MAX_RETRIES times. After that: status='failed'.

    Args:
        job_id: Enrichment job UUID.
        error: Error description string.
        supabase_client: Supabase client.
    """
    try:
        response = (
            supabase_client.table("enrichment_jobs")
            .select("retry_count")
            .eq("id", job_id)
            .single()
            .execute()
        )
        current_retries = 0
        if response.data:
            current_retries = response.data.get("retry_count", 0) or 0

        new_retries = current_retries + 1
        new_status = "pending" if new_retries < _MAX_RETRIES else "failed"

        supabase_client.table("enrichment_jobs").update({
            "status": new_status,
            "error_message": error[:2000],
            "retry_count": new_retries,
        }).eq("id", job_id).execute()

        if new_status == "pending":
            logger.warning("Enrichment job %s failed (attempt %d), requeueing", job_id, new_retries)
        else:
            logger.error("Enrichment job %s permanently failed after %d attempts", job_id, new_retries)
    except Exception as e:
        logger.error("fail_enrichment_job error for %s: %s", job_id, e)


# =============================================================================
# Worker loop
# =============================================================================

async def run_enrichment_worker(
    supabase_client: Any,
    anthropic_client: Any,
    warmr_client: WarmrClient,
) -> None:
    """Main enrichment worker loop — runs until cancelled.

    Respects MAX_CONCURRENT_ENRICHMENTS via asyncio.Semaphore. Sleeps
    _WORKER_SLEEP_SECONDS when no pending jobs are available.

    Args:
        supabase_client: Supabase client.
        anthropic_client: Anthropic client.
        warmr_client: WarmrClient.
    """
    max_concurrent = int(os.getenv("MAX_CONCURRENT_ENRICHMENTS", "5"))
    semaphore = asyncio.Semaphore(max_concurrent)

    logger.info("Enrichment worker started (max_concurrent=%d)", max_concurrent)

    while True:
        job = await claim_next_enrichment_job(supabase_client)

        if job is None:
            await asyncio.sleep(_WORKER_SLEEP_SECONDS)
            continue

        asyncio.create_task(
            _execute_with_semaphore(
                job=job,
                semaphore=semaphore,
                supabase_client=supabase_client,
                anthropic_client=anthropic_client,
                warmr_client=warmr_client,
            )
        )


async def _execute_with_semaphore(
    job: dict,
    semaphore: asyncio.Semaphore,
    supabase_client: Any,
    anthropic_client: Any,
    warmr_client: WarmrClient,
) -> None:
    """Run a single enrichment job under the concurrency semaphore.

    Args:
        job: Enrichment job row dict.
        semaphore: Shared concurrency semaphore.
        supabase_client: Supabase client.
        anthropic_client: Anthropic client.
        warmr_client: WarmrClient.
    """
    async with semaphore:
        try:
            await run_enrichment_for_lead(
                job=job,
                supabase_client=supabase_client,
                anthropic_client=anthropic_client,
                warmr_client=warmr_client,
            )
        except Exception as e:
            logger.exception("Enrichment job %s raised exception: %s", job.get("id"), e)
            await fail_enrichment_job(
                job_id=job["id"],
                error=str(e),
                supabase_client=supabase_client,
            )


# =============================================================================
# Internal helpers
# =============================================================================

async def _get_lead_field(
    lead_id: str,
    field: str,
    supabase_client: Any,
) -> Any:
    """Fetch a single field from the leads table.

    Args:
        lead_id: Lead UUID.
        field: Column name to fetch.
        supabase_client: Supabase client.

    Returns:
        Field value or None.
    """
    try:
        response = (
            supabase_client.table("leads")
            .select(field)
            .eq("id", lead_id)
            .single()
            .execute()
        )
        if response.data:
            return response.data.get(field)
    except Exception:
        pass
    return None


async def _boost_job_priority(
    job_id: str,
    priority: int,
    supabase_client: Any,
) -> None:
    """Update a job's priority in the queue.

    Args:
        job_id: Enrichment job UUID.
        priority: New priority value (lower = more urgent).
        supabase_client: Supabase client.
    """
    try:
        supabase_client.table("enrichment_jobs").update({
            "priority": priority,
        }).eq("id", job_id).execute()
    except Exception as e:
        logger.warning("Failed to boost priority for job %s: %s", job_id, e)
