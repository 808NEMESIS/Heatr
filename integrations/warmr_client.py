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
        custom_subject: str | None = None,
        custom_body: str | None = None,
        content_type: str = "text/plain",
    ) -> dict:
        """Push a single lead to Warmr for outreach.

        Args:
            lead: Full lead dict from Supabase leads table.
            campaign_id: Warmr campaign UUID to assign the lead to.
            preferred_inbox_id: Preferred sending inbox UUID or None.
            custom_subject / custom_body: per-lead gerenderde content
                (sequence-dispatch-pad). Landen in payload.custom_fields
                zodat Warmr-side template-substitutie erbij kan. NB: Warmr
                rendert primair de frozen campaign-sequence; deze velden
                zijn beschikbaar als {{custom_subject}}/{{custom_body}}.
                Sprint 2-vondst: process_due_send gaf deze kwargs al jaren
                mee terwijl push_lead ze niet accepteerde — elke
                dispatch-send crashte silent op TypeError.

        Returns:
            Warmr's response dict (includes Warmr lead ID).

        Raises:
            WarmrAPIError: On API error or duplicate lead rejection.
        """
        payload = self._build_lead_payload(lead, campaign_id, preferred_inbox_id)
        if custom_subject or custom_body:
            payload.setdefault("custom_fields", {})
            if custom_subject:
                payload["custom_fields"]["custom_subject"] = custom_subject
            if custom_body:
                payload["custom_fields"]["custom_body"] = custom_body
                # Route C (2026-07-27): vlag de lead zodat Warmr's scheduler de body
                # OPAAK verstuurt (geen process_content → geen 200-tekens-truncatie,
                # spintax, her-substitutie, signature-append of link-wrap). Wat de
                # QA-gate goedkeurde gaat zo byte-voor-byte de deur uit (I8).
                payload["custom_fields"]["render_owner"] = "heatr"
                # DELIV-02 (audit 2026-07-24): plain-text-intentie EXPLICIET op de
                # wire zetten i.p.v. Warmrs onbekende default. Koude receptie-mail
                # gaat plain-text (geen HTML, geen tracking-pixel). Warmr-side handler
                # moet dit honoreren; verifieer de werkelijke MIME op een seed-/BCC-
                # test vóór de eerste koude send (Sami levert de bevestiging aan).
                payload["custom_fields"]["content_type"] = content_type
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

        Two contract details that matter for the Warmr-side handler:
          1. `campaign_id` is sent at the TOP LEVEL of the body, not just
             inside each per-lead payload. Warmr's BulkLeadIn schema reads
             body.campaign_id at top level — without it, the campaign_leads
             link is silently skipped and pushed leads never reach a
             scheduler.
          2. After a successful push, we write back per-lead bookkeeping
             on heatr_leads (warmr_lead_id, pushed_to_warmr_at, status,
             preferred_inbox_id). The response.inserted array (added to
             Warmr in May 2026) gives us {email, lead_id} per inserted
             row, which we map by email to the original heatr lead.

        Args:
            leads: List of lead dicts from Supabase. Must contain `id`,
                `email`, optionally `preferred_inbox_id`.
            campaign_id: Warmr campaign UUID.

        Returns:
            Summary dict: { pushed, failed, duplicates }.
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
                # `campaign_id` MUST be sent top-level on the body — Warmr's
                # BulkLeadIn model only reads it there. Without this the
                # campaign_leads link is silently skipped on Warmr's side.
                result = await self._request(
                    "POST",
                    "/leads/bulk",
                    json={"leads": payloads, "campaign_id": campaign_id},
                )
                summary["pushed"] += result.get("pushed", len(chunk))
                summary["failed"] += result.get("failed", 0)
                summary["duplicates"] += result.get("duplicates", 0)

                # Per-lead bookkeeping from response.inserted (added to
                # Warmr May 2026). Map by lowercased email back to the
                # original heatr_lead so we can write warmr_lead_id +
                # pushed_to_warmr_at + status + preferred_inbox_id.
                self._writeback_bulk_bookkeeping(chunk, result)
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

    def _writeback_bulk_bookkeeping(
        self,
        heatr_chunk: list[dict],
        warmr_response: dict,
    ) -> None:
        """Update heatr_leads with Warmr's bookkeeping after a bulk push.

        Looks up `inserted` on Warmr's response — list of {email, lead_id}.
        Older Warmr versions (pre-May-2026) don't return this field; in that
        case we log a single warning and skip writeback gracefully (no crash).

        Email matching is case-insensitive on both sides — Warmr lowercases
        on insert, but heatr leads occasionally carry uppercase letters.
        """
        if self._sb is None:
            return  # No Supabase client → no writeback possible (test mode)

        inserted = warmr_response.get("inserted")
        if not inserted:
            # Empty list = nothing actually inserted (all dups/suppressed).
            # Missing key = older Warmr without the inserted field.
            if "inserted" not in warmr_response:
                logger.warning(
                    "Warmr response missing 'inserted' field — heatr_leads "
                    "bookkeeping skipped (older Warmr version?). "
                    "warmr_lead_id will remain NULL for this push."
                )
            return

        email_to_warmr_id: dict[str, str] = {}
        for row in inserted:
            email = (row.get("email") or "").lower()
            wid = row.get("lead_id")
            if email and wid:
                email_to_warmr_id[email] = str(wid)

        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()

        missing_emails: list[str] = []
        for lead in heatr_chunk:
            heatr_id = lead.get("id")
            email = (lead.get("email") or "").lower()
            if not heatr_id or not email:
                continue
            wid = email_to_warmr_id.get(email)
            if not wid:
                # Lead in the input chunk but not in Warmr's inserted list:
                # likely a duplicate (already on heatr_leads.warmr_lead_id
                # from a prior push) or suppressed. Skip silently — operator
                # can see the aggregate `duplicates`/`suppressed` counts.
                missing_emails.append(email)
                continue
            try:
                self._sb.table("heatr_leads").update({
                    "warmr_lead_id":      wid,
                    "pushed_to_warmr_at": now_iso,
                    "status":             "pushed_to_warmr",
                    "preferred_inbox_id": lead.get("preferred_inbox_id"),
                }).eq("id", heatr_id).execute()
            except Exception as exc:
                logger.warning(
                    "Failed to write heatr_leads bookkeeping for %s: %s",
                    heatr_id, exc,
                )

        if missing_emails:
            logger.info(
                "Bulk push: %d lead(s) had no match in Warmr inserted (likely "
                "duplicates or suppressed): %s",
                len(missing_emails),
                ", ".join(missing_emails[:5]) + (" …" if len(missing_emails) > 5 else ""),
            )

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

    async def activate_campaign(self, campaign_id: str) -> dict:
        """Activeer een Warmr-campagne (draft/paused → active).

        create_campaign maakt de campagne in DRAFT; Warmr verstuurt ALLEEN
        active-campagnes. Zonder deze stap blijft alles draft staan en gaat er
        nooit een mail uit (root cause 2026-07-21). Warmr-endpoint:
        POST /campaigns/{id}/resume (vereist trigger_campaigns-permissie).
        """
        result = await self._request("POST", f"/campaigns/{campaign_id}/resume")
        logger.info("Warmr campaign geactiveerd: id=%s status=%s", campaign_id, result.get("status"))
        return result

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
        # Single source of truth voor "veilige first_name" — strip legacy
        # email-inference artefacten (bv. 'Ceciledebooij' uit gmail-local-part)
        from utils.lead_naming import safe_first_name
        from utils.signal_picker import pick_signaal_blok
        from config.sequence_templates import (
            primaire_dienstverlening_for_lead,
            sector_noemer_for_lead,
        )
        first_name_safe = safe_first_name(lead)

        # Test-mode: voeg BCC naar HEATR_TEST_BCC_EMAIL toe + [TEST] subject prefix.
        # Lead-flag is_test_lead wordt door /campaigns/launch via lead-row meegegeven.
        # Zonder env-var blijft BCC leeg → Warmr gedraagt zich normaal.
        is_test = bool(lead.get("is_test_lead"))
        # REROUTE WINT VAN BCC (Sami 2026-07-27, keuze A): staat de testmail-reroute-
        # guard aan (TEST_MODE) of is dit record al gererouteerd, dan MAG er geen
        # 017-BCC bij — de koude receptie-testfire gaat ALLEEN naar TEST_RECIPIENT,
        # nooit naar het originele to: of naar HEATR_TEST_BCC_EMAIL. Onderdrukt dan
        # ook de [TEST]-subjectprefix (die hangt aan dezelfde test_bcc-tak).
        from utils.testmail_guard import test_mode_active
        _reroute_active = test_mode_active() or bool(lead.get("_testmail_rerouted"))
        test_bcc = (
            "" if _reroute_active
            else ((os.getenv("HEATR_TEST_BCC_EMAIL") or "").strip() if is_test else "")
        )

        payload: dict = {
            "email": lead.get("email", ""),
            "first_name": first_name_safe,
            "last_name": lead.get("contact_last_name") or "",
            "campaign_id": campaign_id,
            "gdpr_footer_required": True,
            "is_test_lead": is_test,           # Surface naar Warmr voor logging
            **({"bcc": test_bcc, "subject_prefix": "[TEST] "} if test_bcc else {}),
            "custom_fields": {
                # Personalisation tokens used in Warmr sequences.
                # `_observation_text` (gezet door /campaigns/launch voor v1-template-leads)
                # wint van de Claude-gegenereerde `personalized_opener` — zo krijgt
                # elke lead exact het blok-paragraaf dat past bij hun signaal.
                "opener": lead.get("_observation_text") or lead.get("personalized_opener") or "",
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
                # v3.1 brug-tokens (signaal_blok via pick_signaal_blok, sector_noemer
                # uit SECTOR_NOEMER_MAP, primaire_dienstverlening uit treatment_focus
                # / industry / sector fallback). Heatr resolved server-side; Warmr
                # substitueert {{token}} per-lead in sequence-bodies.
                # bedrijfsnaam/stad zijn aliases voor bestaande company/city — in
                # v3.1 templates wordt expliciet {{bedrijfsnaam}} en {{stad}} gebruikt.
                "bedrijfsnaam": lead.get("company_name") or "",
                "stad": lead.get("city") or "",
                "primaire_dienstverlening": primaire_dienstverlening_for_lead(lead),
                "signaal_blok": pick_signaal_blok(lead),
                "sector_noemer": sector_noemer_for_lead(lead),
            },
        }

        if preferred_inbox_id:
            payload["preferred_inbox_id"] = preferred_inbox_id

        return payload
