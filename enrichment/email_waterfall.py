"""
enrichment/email_waterfall.py — 4-step email discovery orchestrator.

Single entry point for all email discovery. Nothing calls the individual
steps directly — they all go through this file.

Step sequence (stops at first GDPR-safe deliverable result):
  1. Website scraper  — emails on homepage / contact page
  2. Pattern generator — role + name-based SMTP-verified candidates
  3. Google Search     — snippet extraction (skipped if search is blocked)
  4. KvK API           — correspondence address (NL only)
  5. Not found         — marks lead accordingly, score penalty applied

Each step logs to enrichment_data. Lead is updated after each successful step.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from utils.playwright_helpers import classify_email_gdpr

logger = logging.getLogger(__name__)


# =============================================================================
# Public API
# =============================================================================

async def discover_email(
    lead_id: str,
    company_name: str,
    domain: str | None,
    city: str,
    country: str,
    first_name: str | None,
    last_name: str | None,
    tussenvoegsel: str | None,
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Run the full 4-step email waterfall for a single lead.

    IMPORTANT: Unlike a traditional waterfall that stops at first hit, this
    implementation collects ALL email candidates from ALL sources, then ranks
    them to pick the best one. This ensures we find personal emails even when
    a role email (info@) is found first on the website.

    Ranking order (best → worst):
      1. Personal email on company domain, verified (jan@bedrijf.nl, valid)
      2. Personal email on company domain, risky
      3. Role email on company domain, verified (info@bedrijf.nl, valid)
      4. Role email on company domain, risky
      5. Any verified email on company domain
      6. External email (catch-all, free provider, etc.)

    Args:
        lead_id: UUID of the lead row.
        company_name: Company name for Google Search and KvK queries.
        domain: Clean domain string or None.
        city: City for Google Search queries.
        country: ISO 2-letter country code. KvK step skipped for non-NL.
        first_name: Contact first name for pattern generation.
        last_name: Contact last name for pattern generation.
        tussenvoegsel: Dutch name particle for pattern generation.
        workspace_id: Workspace slug.
        supabase_client: Supabase client.

    Returns:
        Dict with keys:
          email (str|None), email_type (str), email_status (str),
          email_discovery_method (str), gdpr_safe (bool),
          all_emails (list[dict]).
    """
    gdpr_mode = os.getenv("GDPR_MODE", "strict")
    all_candidates: list[dict] = []  # Collect ALL found emails

    # ------------------------------------------------------------------
    # Step 1 — Website scraper
    # ------------------------------------------------------------------
    if domain:
        result = await _step_website(
            lead_id=lead_id,
            domain=domain,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )
        outcome = _check_step_result(result, gdpr_mode)
        if outcome:
            outcome["source_step"] = "website"
            all_candidates.append(outcome)
        await _log_waterfall_step(lead_id, workspace_id, 1, "website", outcome, supabase_client)

    # ------------------------------------------------------------------
    # Step 2 — Pattern generator + SMTP verification
    # ------------------------------------------------------------------
    if domain:
        result = await _step_patterns(
            domain=domain,
            first_name=first_name,
            last_name=last_name,
            tussenvoegsel=tussenvoegsel,
            supabase_client=supabase_client,
        )
        outcome = _check_step_result(result, gdpr_mode)
        if outcome:
            # Don't add if same email already found in step 1
            if not any(c["email"] == outcome["email"] for c in all_candidates):
                outcome["source_step"] = "pattern"
                all_candidates.append(outcome)
        await _log_waterfall_step(lead_id, workspace_id, 2, "pattern", outcome, supabase_client)

    # ------------------------------------------------------------------
    # Step 3 — Google Search fallback
    # ------------------------------------------------------------------
    from scrapers.google_search_scraper import (
        search_for_email,
        is_google_search_blocked,
    )
    from enrichment.email_verifier import get_best_email

    blocked = await is_google_search_blocked(supabase_client)
    if not blocked and company_name:
        try:
            candidates = await search_for_email(
                company_name=company_name,
                city=city,
                domain=domain or "",
                supabase_client=supabase_client,
            )
            if candidates:
                for cand_email in candidates:
                    if any(c["email"] == cand_email for c in all_candidates):
                        continue
                    # Verify each candidate
                    best_email, status = await get_best_email([cand_email], supabase_client)
                    if best_email:
                        email_type, gdpr_safe = classify_email_gdpr(best_email, mode=gdpr_mode)
                        if gdpr_safe or gdpr_mode != "strict":
                            all_candidates.append({
                                "email": best_email,
                                "email_type": email_type,
                                "email_status": status,
                                "email_discovery_method": "google_search",
                                "gdpr_safe": gdpr_safe,
                                "source_step": "google_search",
                            })
        except Exception as e:
            logger.warning("Waterfall step 3 (Google Search) failed: %s", e)
    await _log_waterfall_step(lead_id, workspace_id, 3, "google_search", None, supabase_client)

    # ------------------------------------------------------------------
    # Step 4 — KvK API (NL only)
    # ------------------------------------------------------------------
    if country.upper() == "NL":
        try:
            lead_kvk = await _get_lead_kvk_number(lead_id, supabase_client)
            if lead_kvk:
                from scrapers.kvk_scraper import find_email_in_kvk
                raw_email = await find_email_in_kvk(lead_kvk)
                if raw_email and not any(c["email"] == raw_email for c in all_candidates):
                    email_type, gdpr_safe = classify_email_gdpr(raw_email, mode=gdpr_mode)
                    if gdpr_safe or gdpr_mode != "strict":
                        best_email, status = await get_best_email([raw_email], supabase_client)
                        if best_email:
                            all_candidates.append({
                                "email": best_email,
                                "email_type": email_type,
                                "email_status": status,
                                "email_discovery_method": "kvk",
                                "gdpr_safe": gdpr_safe,
                                "source_step": "kvk",
                            })
        except Exception as e:
            logger.warning("Waterfall step 4 (KvK) failed: %s", e)
    await _log_waterfall_step(lead_id, workspace_id, 4, "kvk", None, supabase_client)

    # ------------------------------------------------------------------
    # Rank all candidates and pick the best
    # ------------------------------------------------------------------
    if all_candidates:
        best = _rank_and_pick_best(all_candidates, domain)
        best["all_emails"] = [
            {"email": c["email"], "type": c["email_type"], "status": c["email_status"], "source": c.get("source_step", "")}
            for c in all_candidates
        ]
        await _update_lead_email(lead_id, best, best.get("email_discovery_method", "unknown"), supabase_client)
        logger.info(
            "Email waterfall: lead=%s picked=%s (from %d candidates: %s)",
            lead_id, best["email"], len(all_candidates),
            [c["email"] for c in all_candidates],
        )
        return best

    # ------------------------------------------------------------------
    # No candidates found
    # ------------------------------------------------------------------
    not_found: dict = {
        "email": None,
        "email_type": "unknown",
        "email_status": "not_found",
        "email_discovery_method": "not_found",
        "gdpr_safe": False,
        "all_emails": [],
    }
    await _mark_lead_not_found(lead_id, supabase_client)
    logger.info("Email waterfall exhausted for lead %s — not found", lead_id)
    return not_found


# ---------------------------------------------------------------------------
# Email ranking
# ---------------------------------------------------------------------------

_ROLE_PREFIXES = {
    "info", "contact", "hallo", "receptie", "administratie",
    "boekhouding", "support", "sales", "team", "praktijk",
    "kantoor", "office", "admin", "klantenservice",
}


def _rank_and_pick_best(candidates: list[dict], lead_domain: str | None) -> dict:
    """Rank email candidates and return the best one.

    Ranking priority:
      1. Personal email on company domain, verified
      2. Personal email on company domain, risky
      3. Role email on company domain, verified
      4. Role email on company domain, risky
      5. Any other verified email
      6. Any other email
    """
    lead_domain = (lead_domain or "").lower()

    def score(c: dict) -> tuple:
        email = c.get("email", "")
        local = email.split("@")[0].lower() if "@" in email else ""
        email_domain = email.split("@")[-1].lower() if "@" in email else ""
        status = c.get("email_status", "")

        is_on_domain = email_domain == lead_domain if lead_domain else False
        is_personal = local not in _ROLE_PREFIXES and len(local) > 2
        is_verified = status == "valid"
        is_risky = status == "risky"

        return (
            is_on_domain,      # Company domain first
            is_personal,       # Personal > role
            is_verified,       # Verified > risky
            is_risky,          # Risky > unverified
            -len(email),       # Shorter emails tend to be more direct
        )

    candidates.sort(key=score, reverse=True)
    return dict(candidates[0])


async def run_waterfall_for_lead(
    lead_id: str,
    supabase_client: Any,
) -> dict:
    """Load a lead from Supabase and run the full email waterfall.

    Convenience function for the enrichment queue — loads the lead row
    and extracts all required fields before delegating to discover_email().

    Args:
        lead_id: UUID of the lead to enrich.
        supabase_client: Supabase client.

    Returns:
        Dict from discover_email() with email discovery result.
        Returns not_found result if lead is not found in DB.
    """
    try:
        response = (
            supabase_client.table("leads")
            .select(
                "id, company_name, domain, city, country, workspace_id, "
                "contact_first_name, contact_last_name, contact_tussenvoegsel, "
                "email, email_status, kvk_number"
            )
            .eq("id", lead_id)
            .single()
            .execute()
        )
    except Exception as e:
        logger.error("run_waterfall_for_lead: failed to load lead %s: %s", lead_id, e)
        return _not_found_result()

    if not response.data:
        logger.warning("run_waterfall_for_lead: lead %s not found", lead_id)
        return _not_found_result()

    lead = response.data

    # Skip waterfall if already has a valid/risky email
    existing_status = lead.get("email_status", "")
    if existing_status in ("valid", "risky") and lead.get("email"):
        logger.info("Lead %s already has email (%s) — skipping waterfall", lead_id, existing_status)
        return {
            "email": lead["email"],
            "email_type": "unknown",
            "email_status": existing_status,
            "email_discovery_method": "pre_existing",
            "gdpr_safe": True,
        }

    return await discover_email(
        lead_id=lead_id,
        company_name=lead.get("company_name", ""),
        domain=lead.get("domain"),
        city=lead.get("city", ""),
        country=lead.get("country", "NL"),
        first_name=lead.get("contact_first_name"),
        last_name=lead.get("contact_last_name"),
        tussenvoegsel=lead.get("contact_tussenvoegsel"),
        workspace_id=lead.get("workspace_id", ""),
        supabase_client=supabase_client,
    )


# =============================================================================
# Step implementations
# =============================================================================

async def _step_website(
    lead_id: str,
    domain: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict | None:
    """Run or reuse website scraper results for this lead.

    Checks enrichment_data first. If a completed website step exists:
    extracts emails from the stored record. If not: triggers website_scraper.

    Args:
        lead_id: Lead UUID.
        domain: Company domain.
        workspace_id: Workspace slug.
        supabase_client: Supabase client.

    Returns:
        Dict with 'email' and 'email_status' keys, or None if no email found.
    """
    from enrichment.email_verifier import get_best_email

    # Check if website step already ran
    try:
        existing = (
            supabase_client.table("enrichment_data")
            .select("email_candidate, succeeded")
            .eq("lead_id", lead_id)
            .eq("source", "website")
            .eq("enrichment_step", 1)
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        if existing.data and existing.data[0].get("succeeded"):
            candidate = existing.data[0].get("email_candidate")
            if candidate:
                best, status = await get_best_email([candidate], supabase_client)
                if best:
                    return {"email": best, "email_status": status}
    except Exception as e:
        logger.debug("Failed to check existing website enrichment: %s", e)

    # Run website scraper now
    try:
        from scrapers.website_scraper import scrape_website
        website_data = await scrape_website(
            domain=domain,
            lead_id=lead_id,
            workspace_id=workspace_id,
            supabase_client=supabase_client,
        )
        emails = website_data.get("emails", [])
        if emails:
            best, status = await get_best_email(emails, supabase_client)
            if best:
                return {"email": best, "email_status": status}
    except Exception as e:
        logger.warning("Website scraper step failed: %s", e)

    return None


async def _step_patterns(
    domain: str,
    first_name: str | None,
    last_name: str | None,
    tussenvoegsel: str | None,
    supabase_client: Any,
) -> dict | None:
    """Generate email patterns and verify via SMTP.

    Args:
        domain: Company domain.
        first_name: Contact first name or None.
        last_name: Contact last name or None.
        tussenvoegsel: Name particle or None.
        supabase_client: Supabase client.

    Returns:
        Dict with 'email' and 'email_status' keys, or None.
    """
    from enrichment.email_finder import generate_email_candidates
    from enrichment.email_verifier import get_best_email

    try:
        candidates = await generate_email_candidates(
            domain=domain,
            first_name=first_name,
            last_name=last_name,
            tussenvoegsel=tussenvoegsel,
        )
        if candidates:
            best, status = await get_best_email(candidates, supabase_client)
            if best:
                return {"email": best, "email_status": status}
    except Exception as e:
        logger.warning("Pattern generator step failed: %s", e)

    return None


# =============================================================================
# Lead update helpers
# =============================================================================

async def _update_lead_email(
    lead_id: str,
    outcome: dict,
    method: str,
    supabase_client: Any,
) -> None:
    """Write email discovery result to the leads table.

    Args:
        lead_id: Lead UUID.
        outcome: Dict with email, email_type, email_status, gdpr_safe.
        method: Discovery method label.
        supabase_client: Supabase client.
    """
    try:
        supabase_client.table("leads").update({
            "email": outcome.get("email"),
            "email_type": outcome.get("email_type"),
            "email_status": outcome.get("email_status"),
            "gdpr_safe": outcome.get("gdpr_safe", False),
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.error("Failed to update lead email for %s: %s", lead_id, e)


async def _mark_lead_not_found(lead_id: str, supabase_client: Any) -> None:
    """Set lead email_status to not_found and update enriched_at.

    Args:
        lead_id: Lead UUID.
        supabase_client: Supabase client.
    """
    try:
        supabase_client.table("leads").update({
            "email_status": "not_found",
            "email": None,
            "gdpr_safe": False,
            "enriched_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.error("Failed to mark lead %s not_found: %s", lead_id, e)


async def _log_waterfall_step(
    lead_id: str,
    workspace_id: str,
    step: int,
    source: str,
    outcome: dict | None,
    supabase_client: Any,
) -> None:
    """Append a waterfall step result to enrichment_data.

    Args:
        lead_id: Lead UUID.
        workspace_id: Workspace slug.
        step: Step number 1-4.
        source: Source label.
        outcome: Result dict or None if step found nothing.
        supabase_client: Supabase client.
    """
    try:
        supabase_client.table("enrichment_data").insert({
            "workspace_id": workspace_id,
            "lead_id": lead_id,
            "enrichment_step": step,
            "source": source,
            "email_candidate": outcome.get("email") if outcome else None,
            "email_status": outcome.get("email_status") if outcome else None,
            "succeeded": bool(outcome and outcome.get("email")),
            "raw_result": {"method": source, "outcome": outcome},
        }).execute()
    except Exception as e:
        logger.warning("Failed to log waterfall step %d for lead %s: %s", step, lead_id, e)


async def _get_lead_kvk_number(lead_id: str, supabase_client: Any) -> str | None:
    """Fetch the KvK number stored on a lead row.

    Args:
        lead_id: Lead UUID.
        supabase_client: Supabase client.

    Returns:
        KvK number string or None.
    """
    try:
        response = (
            supabase_client.table("leads")
            .select("kvk_number")
            .eq("id", lead_id)
            .single()
            .execute()
        )
        if response.data:
            return response.data.get("kvk_number")
    except Exception:
        pass
    return None


# =============================================================================
# Result helpers
# =============================================================================

def _check_step_result(
    step_result: dict | None,
    gdpr_mode: str,
) -> dict | None:
    """Validate a step result against GDPR rules.

    Args:
        step_result: Dict with 'email' and 'email_status' keys, or None.
        gdpr_mode: 'strict' or 'relaxed'.

    Returns:
        Full outcome dict if the email is usable, None otherwise.
    """
    if not step_result or not step_result.get("email"):
        return None

    email = step_result["email"]
    email_type, gdpr_safe = classify_email_gdpr(email, mode=gdpr_mode)

    if gdpr_mode == "strict" and not gdpr_safe:
        logger.debug("GDPR strict: skipping %s (type=%s)", email, email_type)
        return None

    return {
        "email": email,
        "email_type": email_type,
        "email_status": step_result.get("email_status", "unknown"),
        "email_discovery_method": step_result.get("email_discovery_method", "unknown"),
        "gdpr_safe": gdpr_safe,
    }


def _not_found_result() -> dict:
    """Return a standard not_found result dict."""
    return {
        "email": None,
        "email_type": "unknown",
        "email_status": "not_found",
        "email_discovery_method": "not_found",
        "gdpr_safe": False,
    }
