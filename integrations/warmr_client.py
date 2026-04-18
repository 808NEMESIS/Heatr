"""
integrations/warmr_client.py — Warmr API client.

All Heatr → Warmr communication goes through this class. Heatr never calls
Warmr endpoints directly from scrapers or enrichment code — always via here.

Warmr is the sending infrastructure. Heatr handles discovery + intelligence.
This client handles: inbox management, lead pushing, campaign operations.

Error handling: all methods raise WarmrAPIError on non-2xx responses.
Callers are responsible for catching WarmrAPIError and deciding whether to
retry, skip, or halt.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 100  # Max leads per bulk push call


# =============================================================================
# Custom exception
# =============================================================================

class WarmrAPIError(Exception):
    """Raised when the Warmr API returns a non-2xx response.

    Attributes:
        status_code: HTTP status code returned by Warmr.
        response_body: Response body string for debugging.
    """

    def __init__(self, message: str, status_code: int, response_body: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body

    def __str__(self) -> str:
        return f"{super().__str__()} (HTTP {self.status_code}): {self.response_body[:300]}"


# =============================================================================
# Warmr API client
# =============================================================================

class WarmrClient:
    """Async Warmr API client.

    Initialised from environment variables. All methods are async and use a
    shared httpx.AsyncClient for connection pooling.

    Usage:
        client = WarmrClient()
        inboxes = await client.get_ready_inboxes()
    """

    def __init__(
        self,
        api_url: str | None = None,
        api_key: str | None = None,
        supabase_client=None,
    ) -> None:
        """Initialise the Warmr client from env or explicit parameters.

        Args:
            api_url: Warmr API base URL. Defaults to WARMR_API_URL env var.
            api_key: Warmr API key. Defaults to WARMR_API_KEY env var.
            supabase_client: Optional Supabase client for writing back warmr_lead_id.
        """
        self.api_url = (api_url or os.getenv("WARMR_API_URL", "")).rstrip("/")
        self.api_key = api_key or os.getenv("WARMR_API_KEY", "")
        self._sb = supabase_client

        if not self.api_url:
            raise ValueError("WARMR_API_URL is not set")
        if not self.api_key:
            raise ValueError("WARMR_API_KEY is not set")

    def _headers(self) -> dict[str, str]:
        """Build standard request headers."""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> dict:
        """Execute an HTTP request and return parsed JSON response.

        Args:
            method: HTTP method string ('GET', 'POST', 'PATCH', etc.).
            path: API path, e.g. '/inboxes?status=ready'.
            **kwargs: Additional kwargs passed to httpx request.

        Returns:
            Parsed JSON response dict.

        Raises:
            WarmrAPIError: On any non-2xx HTTP response.
        """
        url = f"{self.api_url}{path}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.request(
                    method,
                    url,
                    headers=self._headers(),
                    **kwargs,
                )
            except httpx.RequestError as e:
                raise WarmrAPIError(
                    f"Warmr request failed: {e}", status_code=0
                ) from e

            if response.status_code < 200 or response.status_code >= 300:
                raise WarmrAPIError(
                    f"Warmr API error on {method} {path}",
                    status_code=response.status_code,
                    response_body=response.text,
                )

            try:
                return response.json()
            except Exception:
                return {}

    # =========================================================================
    # Inbox operations
    # =========================================================================

    async def get_ready_inboxes(self) -> list[dict]:
        """Fetch all inboxes with status=ready from Warmr.

        Returns:
            List of inbox dicts, each with: id, email, domain, provider,
            reputation_score, daily_sent, daily_campaign_target,
            daily_warmup_target.

        Raises:
            WarmrAPIError: On API error.
        """
        data = await self._request("GET", "/inboxes?status=ready")
        inboxes = data.get("inboxes", data) if isinstance(data, dict) else data
        if not isinstance(inboxes, list):
            return []
        logger.debug("Warmr: %d ready inboxes fetched", len(inboxes))
        return inboxes

    async def get_inbox_availability(self, inbox_id: str) -> dict:
        """Fetch current daily capacity remaining for a single inbox.

        Args:
            inbox_id: Warmr inbox UUID.

        Returns:
            Dict with at least: id, daily_remaining, reputation_score.

        Raises:
            WarmrAPIError: On API error.
        """
        return await self._request("GET", f"/inboxes/{inbox_id}/availability")

    # =========================================================================
    # Lead operations
    # =========================================================================

    async def push_lead(
        self,
        lead: dict,
        campaign_id: str,
        preferred_inbox_id: str | None = None,
    ) -> dict:
        """Push a single lead to Warmr for outreach.

        Args:
            lead: Full lead dict from Supabase leads table.
            campaign_id: Warmr campaign UUID to assign the lead to.
            preferred_inbox_id: Preferred sending inbox UUID or None.

        Returns:
            Warmr's response dict (includes Warmr lead ID).

        Raises:
            WarmrAPIError: On API error or duplicate lead rejection.
        """
        payload = self._build_lead_payload(lead, campaign_id, preferred_inbox_id)
        result = await self._request("POST", "/leads", json=payload)

        # Store Warmr's lead UUID back in Heatr for future correlation
        warmr_lead_id = result.get("id") or result.get("lead_id")
        heatr_lead_id = lead.get("id")
        if warmr_lead_id and heatr_lead_id and self._sb:
            try:
                self._sb.table("leads").update({
                    "warmr_lead_id": str(warmr_lead_id),
                }).eq("id", heatr_lead_id).execute()
            except Exception as exc:
                logger.warning("Failed to store warmr_lead_id for lead %s: %s", heatr_lead_id, exc)

        return result

    async def push_leads_bulk(
        self,
        leads: list[dict],
        campaign_id: str,
    ) -> dict:
        """Push multiple leads to Warmr in chunks of 100.

        Args:
            leads: List of lead dicts from Supabase.
            campaign_id: Warmr campaign UUID.

        Returns:
            Summary dict: { pushed: int, failed: int, duplicates: int }.
        """
        summary = {"pushed": 0, "failed": 0, "duplicates": 0}

        for i in range(0, len(leads), _CHUNK_SIZE):
            chunk = leads[i: i + _CHUNK_SIZE]
            payloads = [
                self._build_lead_payload(
                    lead,
                    campaign_id,
                    lead.get("preferred_inbox_id"),
                )
                for lead in chunk
            ]

            try:
                result = await self._request(
                    "POST",
                    "/leads/bulk",
                    json={"leads": payloads},
                )
                summary["pushed"] += result.get("pushed", len(chunk))
                summary["failed"] += result.get("failed", 0)
                summary["duplicates"] += result.get("duplicates", 0)
            except WarmrAPIError as e:
                logger.error(
                    "Bulk push chunk %d failed (HTTP %d): %s",
                    i // _CHUNK_SIZE,
                    e.status_code,
                    str(e)[:200],
                )
                summary["failed"] += len(chunk)

        logger.info(
            "Warmr bulk push: pushed=%d failed=%d duplicates=%d",
            summary["pushed"], summary["failed"], summary["duplicates"],
        )
        return summary

    # =========================================================================
    # Campaign operations
    # =========================================================================

    async def create_campaign(
        self,
        name: str,
        sequence_steps: list[dict],
        settings: dict,
    ) -> str:
        """Create a new campaign in Warmr.

        Args:
            name: Human-readable campaign name.
            sequence_steps: List of step dicts (subject, body, delay_days, etc.).
            settings: Campaign settings dict (from_name, timezone, etc.).

        Returns:
            Warmr campaign UUID string.

        Raises:
            WarmrAPIError: On API error.
        """
        payload = {
            "name": name,
            "steps": sequence_steps,
            "settings": settings,
        }
        result = await self._request("POST", "/campaigns", json=payload)
        campaign_id = result.get("id") or result.get("campaign_id")
        if not campaign_id:
            raise WarmrAPIError(
                "Warmr create_campaign returned no campaign ID",
                status_code=200,
                response_body=str(result),
            )
        logger.info("Warmr campaign created: id=%s name=%s", campaign_id, name)
        return str(campaign_id)

    async def get_campaign_stats(self, campaign_id: str) -> dict:
        """Fetch stats for a Warmr campaign.

        Args:
            campaign_id: Warmr campaign UUID.

        Returns:
            Stats dict: sent, opened, clicked, replied, bounced, unsubscribed, etc.

        Raises:
            WarmrAPIError: On API error.
        """
        return await self._request("GET", f"/campaigns/{campaign_id}/stats")

    async def pause_campaign(self, campaign_id: str) -> None:
        """Pause an active Warmr campaign.

        Args:
            campaign_id: Warmr campaign UUID.

        Raises:
            WarmrAPIError: On API error.
        """
        await self._request("POST", f"/campaigns/{campaign_id}/pause")
        logger.info("Warmr campaign paused: %s", campaign_id)

    # =========================================================================
    # Payload builder
    # =========================================================================

    def _build_lead_payload(
        self,
        lead: dict,
        campaign_id: str,
        preferred_inbox_id: str | None,
    ) -> dict:
        """Build the Warmr API lead payload from a Heatr lead dict.

        Includes all standard fields plus custom_fields for personalisation
        tokens in Warmr sequences.

        Args:
            lead: Heatr lead dict from the leads table.
            campaign_id: Campaign UUID to assign this lead to.
            preferred_inbox_id: Preferred inbox UUID or None.

        Returns:
            Warmr API-ready lead payload dict.
        """
        payload: dict = {
            "email": lead.get("email", ""),
            "first_name": lead.get("contact_first_name") or "",
            "last_name": lead.get("contact_last_name") or "",
            "campaign_id": campaign_id,
            "gdpr_footer_required": True,
            "custom_fields": {
                # Personalisation tokens used in Warmr sequences
                "opener": lead.get("personalized_opener") or "",
                "summary": lead.get("company_summary") or "",
                "company": lead.get("company_name") or "",
                "city": lead.get("city") or "",
                "industry": lead.get("industry") or "",
                "company_size": lead.get("company_size_estimate") or "",
                "sector": lead.get("sector") or "",
                "source": lead.get("source") or "heatr",
                # Scoring signals
                "heatr_score": str(lead.get("score") or 0),
                "icp_match": str(round(float(lead.get("icp_match") or 0), 2)),
                "website_score": str(lead.get("website_score") or ""),
                # Company signals
                "has_instagram": "ja" if lead.get("has_instagram") else "nee",
                "google_rating": str(lead.get("google_rating") or ""),
                "google_review_count": str(lead.get("google_review_count") or ""),
                "kvk_number": lead.get("kvk_number") or "",
                "domain": lead.get("domain") or "",
                # Heatr internal ID for webhook correlation
                "heatr_lead_id": str(lead.get("id") or ""),
                "workspace_id": str(lead.get("workspace_id") or ""),
                # Contact person (enriched by contact_discovery)
                "contact_title": lead.get("contact_title") or "",
                "contact_linkedin": lead.get("contact_linkedin_url") or "",
                "contact_why_chosen": lead.get("contact_why_chosen") or "",
                # Personalization context (enriched by website_intelligence)
                "positioning": lead.get("company_positioning") or "",
                "hooks": "|".join(lead.get("personalization_hooks") or []),
                "observations": "|".join(lead.get("personalization_observations") or []),
                # Data quality
                "fit_score": str(lead.get("fit_score") or 0),
                "reachability_score": str(lead.get("reachability_score") or 0),
                "data_quality": str(lead.get("data_quality_score") or 0),
            },
        }

        if preferred_inbox_id:
            payload["preferred_inbox_id"] = preferred_inbox_id

        return payload
