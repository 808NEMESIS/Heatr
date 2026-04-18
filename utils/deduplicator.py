"""
utils/deduplicator.py — Three-level deduplication logic for Heatr.

Deduplication prevents duplicate scraping, double-enrichment, and most
critically, prevents a lead from receiving outreach twice — which damages
sender reputation and Warmr inbox warmth.

Three levels:
  Level 1 — Domain known:     companies_raw (scraping stage)
  Level 2 — Email known:      leads table (enrichment stage)
  Level 3 — Campaign active:  lead_campaign_history (Warmr push stage)

The 90-day cooldown in should_allow_warmr_push() is the final gate before
any lead touches Warmr. It combines all three levels.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


# Days a completed campaign must age before the same lead can be contacted again.
CAMPAIGN_COOLDOWN_DAYS: int = 90


def _now_utc() -> datetime:
    """Return current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


async def is_domain_known(
    domain: str,
    workspace_id: str,
    supabase_client: Any,
) -> bool:
    """Check whether a domain has already been scraped for this workspace.

    Queries the companies_raw table. Use this in scrapers before inserting a
    new company to avoid duplicate records.

    Args:
        domain: Clean domain string (e.g. 'example.nl') — no protocol, no slash.
        workspace_id: Workspace slug to scope the check.
        supabase_client: Initialised supabase-py client.

    Returns:
        True if the domain already exists in companies_raw, False otherwise.
    """
    if not domain:
        return False

    response = (
        supabase_client.table("companies_raw")
        .select("id")
        .eq("workspace_id", workspace_id)
        .eq("domain", domain.lower().strip())
        .limit(1)
        .execute()
    )

    return bool(response.data)


async def is_email_known(
    email: str,
    workspace_id: str,
    supabase_client: Any,
) -> bool:
    """Check whether an email address is already associated with a lead.

    Queries the leads table. Use this in enrichment to avoid creating
    duplicate leads for the same email address.

    Args:
        email: Email address string to check.
        workspace_id: Workspace slug to scope the check.
        supabase_client: Initialised supabase-py client.

    Returns:
        True if the email already exists in the leads table, False otherwise.
    """
    if not email or "@" not in email:
        return False

    response = (
        supabase_client.table("leads")
        .select("id")
        .eq("workspace_id", workspace_id)
        .eq("email", email.lower().strip())
        .limit(1)
        .execute()
    )

    return bool(response.data)


async def is_lead_in_active_campaign(
    lead_id: str,
    supabase_client: Any,
) -> bool:
    """Check whether a lead is currently enrolled in an active Warmr campaign.

    Queries lead_campaign_history for rows with status='active'. An active
    campaign means Warmr is currently sending sequences to this lead — pushing
    them again would create duplicate outreach.

    Args:
        lead_id: UUID string of the lead to check.
        supabase_client: Initialised supabase-py client.

    Returns:
        True if lead has any active campaign, False otherwise.
    """
    if not lead_id:
        return False

    response = (
        supabase_client.table("lead_campaign_history")
        .select("id")
        .eq("lead_id", lead_id)
        .eq("status", "active")
        .limit(1)
        .execute()
    )

    return bool(response.data)


async def should_allow_warmr_push(
    lead_id: str,
    supabase_client: Any,
) -> tuple[bool, str]:
    """Full deduplication check before pushing a lead to Warmr.

    Combines all three deduplication levels plus the 90-day cooldown window:

    1. Lead must have a valid, GDPR-safe email (from leads table).
    2. Lead must not be in an active campaign.
    3. Lead must not have completed a campaign within the last 90 days.
    4. Lead must not have been disqualified.

    Args:
        lead_id: UUID string of the lead to check.
        supabase_client: Initialised supabase-py client.

    Returns:
        Tuple of (allowed: bool, reason: str).
        If allowed=True, reason='ok'.
        If allowed=False, reason explains why (for logging/UI).
    """
    if not lead_id:
        return (False, "missing_lead_id")

    # --- Fetch lead record ---------------------------------------------------
    lead_response = (
        supabase_client.table("leads")
        .select("id, email, email_status, gdpr_safe, status, workspace_id")
        .eq("id", lead_id)
        .single()
        .execute()
    )

    if not lead_response.data:
        return (False, "lead_not_found")

    lead = lead_response.data

    # --- Check 1: Disqualified leads never go to Warmr -----------------------
    if lead.get("status") == "disqualified":
        return (False, "lead_disqualified")

    # --- Check 2: GDPR safety gate -------------------------------------------
    if not lead.get("gdpr_safe"):
        return (False, "gdpr_unsafe")

    # --- Check 3: Email must be valid or risky (not not_found or invalid) ----
    email_status = lead.get("email_status", "")
    if email_status not in ("valid", "risky"):
        return (False, f"email_status_{email_status or 'missing'}")

    # --- Check 4: Not in an active campaign right now ------------------------
    in_active = await is_lead_in_active_campaign(lead_id, supabase_client)
    if in_active:
        return (False, "already_in_active_campaign")

    # --- Check 5: 90-day cooldown after completed campaigns ------------------
    cutoff = _now_utc() - timedelta(days=CAMPAIGN_COOLDOWN_DAYS)

    history_response = (
        supabase_client.table("lead_campaign_history")
        .select("id, sent_at, status")
        .eq("lead_id", lead_id)
        .in_("status", ["completed", "stopped", "bounced"])
        .order("sent_at", desc=True)
        .limit(1)
        .execute()
    )

    if history_response.data:
        last_campaign = history_response.data[0]
        sent_at_str = last_campaign.get("sent_at", "")
        if sent_at_str:
            try:
                sent_at = datetime.fromisoformat(sent_at_str)
                if sent_at.tzinfo is None:
                    sent_at = sent_at.replace(tzinfo=timezone.utc)
                if sent_at > cutoff:
                    days_remaining = (sent_at + timedelta(days=CAMPAIGN_COOLDOWN_DAYS) - _now_utc()).days
                    return (
                        False,
                        f"cooldown_{days_remaining}_days_remaining",
                    )
            except ValueError:
                # Unparseable date — allow push, log warning
                pass

    return (True, "ok")


# =============================================================================
# Extended deduplication — company names, contacts, entity resolution
# =============================================================================

import re

_COMPANY_SUFFIXES = [
    " b.v.", " bv", " v.o.f.", " vof", " holding", " groep",
    " nederland", " nl", " eenmanszaak", " b.v", " maatschap",
]


def normalize_domain(domain: str) -> str:
    """Normalize a domain for comparison. Strips www., protocol, trailing slashes."""
    if not domain:
        return ""
    domain = domain.lower().strip()
    domain = domain.replace("https://", "").replace("http://", "")
    domain = domain.split("/")[0].split("?")[0]
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def normalize_company_name(name: str) -> str:
    """Normalize a Dutch company name for dedup comparison."""
    if not name:
        return ""
    name = name.lower().strip()
    for suffix in _COMPANY_SUFFIXES:
        if name.endswith(suffix):
            name = name[:-len(suffix)].rstrip()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


async def is_duplicate_entity(
    company_name: str,
    domain: str,
    city: str,
    workspace_id: str,
    supabase_client: Any,
) -> tuple[bool, str | None]:
    """
    Check if a company already exists as a lead.

    Matches on:
      1. Normalized domain (exact match)
      2. Normalized company name + same city

    Returns:
        (is_duplicate: bool, existing_lead_id: str | None)
    """
    norm_domain = normalize_domain(domain)
    norm_name = normalize_company_name(company_name)

    # Domain match (strongest signal)
    if norm_domain:
        res = (
            supabase_client.table("leads")
            .select("id")
            .eq("workspace_id", workspace_id)
            .eq("domain", norm_domain)
            .limit(1)
            .execute()
        )
        if res.data:
            return True, res.data[0]["id"]

    # Name + city match
    if norm_name and city:
        res = (
            supabase_client.table("leads")
            .select("id, company_name")
            .eq("workspace_id", workspace_id)
            .eq("city", city)
            .execute()
        )
        for row in (res.data or []):
            existing_norm = normalize_company_name(row.get("company_name") or "")
            if existing_norm and existing_norm == norm_name:
                return True, row["id"]

    return False, None


async def dedup_contacts(
    full_name: str,
    company_name: str,
    linkedin_url: str,
    workspace_id: str,
    supabase_client: Any,
) -> bool:
    """Check if a contact person already exists in lead_contacts."""
    if linkedin_url:
        res = (
            supabase_client.table("lead_contacts")
            .select("id")
            .eq("workspace_id", workspace_id)
            .eq("linkedin_url", linkedin_url)
            .limit(1)
            .execute()
        )
        if res.data:
            return True

    if full_name and company_name:
        res = (
            supabase_client.table("lead_contacts")
            .select("id, full_name")
            .eq("workspace_id", workspace_id)
            .execute()
        )
        target_name = full_name.lower().strip()
        for row in (res.data or []):
            if (row.get("full_name") or "").lower().strip() == target_name:
                return True

    return False
