"""
enrichment/data_verification.py — Cross-source data verification for Heatr.

Computes per-field confidence scores by comparing data from multiple sources
(Google Maps, website scraper, KvK, enrichment steps). Flags inconsistencies
and assigns an overall data_quality_score.

Called as step 7 in the enrichment pipeline, after all data sources have run.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class VerificationResult:
    """Result of cross-source data verification for a single lead."""
    company_match: float = 0.0       # 0.0-1.0: Maps name vs website vs KvK
    website_match: float = 0.0       # 0.0-1.0: domain consistency across sources
    contact_match: float = 0.0       # 0.0-1.0: contact person verified across sources
    email_confidence: float = 0.0    # 0.0-1.0: email quality + source reliability
    data_quality_score: float = 0.0  # 0.0-1.0: weighted average of all confidence
    inconsistency_flags: list[str] = field(default_factory=list)
    source_attribution: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "confidence_scores": {
                "company_match": round(self.company_match, 3),
                "website_match": round(self.website_match, 3),
                "contact_match": round(self.contact_match, 3),
                "email_confidence": round(self.email_confidence, 3),
            },
            "data_quality_score": round(self.data_quality_score, 3),
            "inconsistency_flags": self.inconsistency_flags,
            "source_attribution": self.source_attribution,
        }


# ---------------------------------------------------------------------------
# Weights for data_quality_score calculation
# ---------------------------------------------------------------------------
_WEIGHTS = {
    "company_match":   0.30,
    "website_match":   0.20,
    "contact_match":   0.20,
    "email_confidence": 0.30,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def verify_lead_data(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
) -> VerificationResult:
    """
    Run cross-source verification on a lead and store the results.

    Loads the lead row + enrichment_data rows, compares fields across sources,
    computes confidence scores, and writes results back to the leads table.

    Returns:
        VerificationResult with all confidence scores and flags.
    """
    result = VerificationResult()

    # Load lead
    lead_res = supabase_client.table("leads").select("*").eq(
        "id", lead_id,
    ).eq("workspace_id", workspace_id).maybe_single().execute()

    if not lead_res.data:
        logger.warning("verify_lead_data: lead %s not found", lead_id)
        return result

    lead = lead_res.data

    # Load enrichment data (all sources)
    enrich_res = supabase_client.table("enrichment_data").select("*").eq(
        "lead_id", lead_id,
    ).order("created_at", desc=False).execute()
    enrichment_rows = enrich_res.data or []

    # Build source data map
    sources = _build_source_map(lead, enrichment_rows)

    # --- Company name verification ---
    result.company_match = _verify_company_name(lead, sources, result)

    # --- Website/domain verification ---
    result.website_match = _verify_website(lead, sources, result)

    # --- Email confidence ---
    result.email_confidence = _verify_email(lead, sources, result)

    # --- Contact match (starts low, improved by contact_discovery later) ---
    result.contact_match = _verify_contact(lead, sources, result)

    # --- Overall data quality score ---
    result.data_quality_score = (
        result.company_match   * _WEIGHTS["company_match"]
        + result.website_match  * _WEIGHTS["website_match"]
        + result.contact_match  * _WEIGHTS["contact_match"]
        + result.email_confidence * _WEIGHTS["email_confidence"]
    )

    # --- Write results to DB ---
    try:
        update = result.to_dict()
        supabase_client.table("leads").update({
            "confidence_scores": update["confidence_scores"],
            "data_quality_score": update["data_quality_score"],
            "inconsistency_flags": update["inconsistency_flags"],
            "source_attribution": update["source_attribution"],
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.error("verify_lead_data: failed to write results for %s: %s", lead_id, e)

    logger.info(
        "verify_lead_data: lead=%s quality=%.2f company=%.2f website=%.2f email=%.2f contact=%.2f flags=%s",
        lead_id, result.data_quality_score, result.company_match,
        result.website_match, result.email_confidence, result.contact_match,
        result.inconsistency_flags,
    )

    return result


# ---------------------------------------------------------------------------
# Source data map
# ---------------------------------------------------------------------------

def _build_source_map(lead: dict, enrichment_rows: list[dict]) -> dict[str, dict]:
    """Build a lookup of field values per source for cross-referencing."""
    sources: dict[str, dict] = {}

    # Google Maps data (from companies_raw via lead creation)
    sources["google_maps"] = {
        "company_name": lead.get("company_name") or "",
        "domain": lead.get("domain") or "",
        "phone": lead.get("phone") or "",
        "city": lead.get("city") or "",
    }

    # Website scraper data
    for row in enrichment_rows:
        step = row.get("step") or row.get("source") or ""
        data = row.get("data") or row.get("result") or {}
        if isinstance(data, str):
            continue

        if "website" in step.lower():
            sources["website"] = {
                "company_name": data.get("title") or data.get("og_title") or "",
                "domain": data.get("domain") or "",
                "phone": data.get("phone") or "",
                "emails": data.get("emails") or [],
            }
        elif "kvk" in step.lower():
            sources["kvk"] = {
                "company_name": data.get("trade_name") or data.get("company_name") or "",
                "domain": data.get("domain") or "",
                "phone": data.get("phone") or "",
                "city": data.get("city") or data.get("address_city") or "",
                "kvk_number": data.get("kvk_number") or "",
            }

    return sources


# ---------------------------------------------------------------------------
# Individual verification functions
# ---------------------------------------------------------------------------

def _verify_company_name(
    lead: dict,
    sources: dict[str, dict],
    result: VerificationResult,
) -> float:
    """Compare company name across Google Maps, website title, and KvK."""
    maps_name = _normalize_company(sources.get("google_maps", {}).get("company_name", ""))
    website_name = _normalize_company(sources.get("website", {}).get("company_name", ""))
    kvk_name = _normalize_company(sources.get("kvk", {}).get("company_name", ""))

    result.source_attribution["company_name"] = "google_maps"

    if not maps_name:
        return 0.0

    scores: list[float] = []

    # Maps vs Website
    if website_name:
        ratio = SequenceMatcher(None, maps_name, website_name).ratio()
        scores.append(ratio)
        if ratio < 0.6:
            result.inconsistency_flags.append("company_name_mismatch_maps_vs_website")
        elif ratio >= 0.85:
            result.source_attribution["company_name"] = "google_maps+website"

    # Maps vs KvK
    if kvk_name:
        ratio = SequenceMatcher(None, maps_name, kvk_name).ratio()
        scores.append(ratio)
        if ratio < 0.6:
            result.inconsistency_flags.append("company_name_mismatch_maps_vs_kvk")
        elif ratio >= 0.85:
            result.source_attribution["company_name"] = "google_maps+kvk"

    # Website vs KvK
    if website_name and kvk_name:
        ratio = SequenceMatcher(None, website_name, kvk_name).ratio()
        scores.append(ratio)
        if ratio >= 0.85:
            result.source_attribution["company_name"] = "google_maps+website+kvk"

    if not scores:
        return 0.5  # Only one source — moderate confidence

    return round(sum(scores) / len(scores), 3)


def _verify_website(
    lead: dict,
    sources: dict[str, dict],
    result: VerificationResult,
) -> float:
    """Verify domain consistency across sources."""
    lead_domain = _normalize_domain(lead.get("domain") or "")
    result.source_attribution["domain"] = "google_maps"

    if not lead_domain:
        result.inconsistency_flags.append("no_domain")
        return 0.0

    confidence = 0.5  # Base: we have a domain from one source

    # Check if website scraper confirmed the domain
    ws_domain = _normalize_domain(sources.get("website", {}).get("domain", ""))
    if ws_domain:
        if ws_domain == lead_domain:
            confidence += 0.25
            result.source_attribution["domain"] = "google_maps+website"
        else:
            result.inconsistency_flags.append("domain_mismatch_maps_vs_website")
            confidence -= 0.2

    # Check if KvK has same domain
    kvk_domain = _normalize_domain(sources.get("kvk", {}).get("domain", ""))
    if kvk_domain:
        if kvk_domain == lead_domain:
            confidence += 0.25
            result.source_attribution["domain"] = result.source_attribution.get("domain", "") + "+kvk"
        else:
            result.inconsistency_flags.append("domain_mismatch_maps_vs_kvk")

    # Check phone consistency as additional signal
    maps_phone = _normalize_phone(sources.get("google_maps", {}).get("phone", ""))
    ws_phone = _normalize_phone(sources.get("website", {}).get("phone", ""))
    if maps_phone and ws_phone and maps_phone == ws_phone:
        confidence = min(confidence + 0.1, 1.0)
    elif maps_phone and ws_phone and maps_phone != ws_phone:
        result.inconsistency_flags.append("phone_mismatch_maps_vs_website")

    return round(max(0.0, min(1.0, confidence)), 3)


def _verify_email(
    lead: dict,
    sources: dict[str, dict],
    result: VerificationResult,
) -> float:
    """Compute email confidence from status, source, and domain match."""
    email = lead.get("email") or ""
    email_status = lead.get("email_status") or ""
    lead_domain = _normalize_domain(lead.get("domain") or "")

    if not email:
        result.source_attribution["email"] = "none"
        return 0.0

    # Base confidence from verification status
    status_scores = {
        "valid": 0.7,
        "risky": 0.4,
        "catch_all": 0.3,
        "catchall_risky": 0.2,
        "unverified": 0.15,
        "not_found": 0.0,
        "invalid": 0.0,
    }
    confidence = status_scores.get(email_status, 0.15)

    # Bonus: email domain matches lead domain
    email_domain = email.split("@")[-1].lower() if "@" in email else ""
    if email_domain and lead_domain and email_domain == lead_domain:
        confidence += 0.15
    elif email_domain and lead_domain and email_domain != lead_domain:
        result.inconsistency_flags.append("email_domain_mismatch")

    # Bonus: email found on the website directly
    ws_emails = sources.get("website", {}).get("emails", [])
    if email.lower() in [e.lower() for e in ws_emails]:
        confidence += 0.15
        result.source_attribution["email"] = "website"
    else:
        result.source_attribution["email"] = "pattern_or_search"

    # Penalty for generic/role emails in GDPR strict mode
    local_part = email.split("@")[0].lower() if "@" in email else ""
    if local_part in ("info", "contact", "hallo", "receptie", "administratie"):
        confidence = max(confidence - 0.05, 0.0)

    return round(min(1.0, confidence), 3)


def _verify_contact(
    lead: dict,
    sources: dict[str, dict],
    result: VerificationResult,
) -> float:
    """Compute contact person confidence.

    Starts low — improves when contact_discovery (Phase 4) runs later and
    writes contact data back to the lead.
    """
    contact_name = lead.get("contact_first_name") or lead.get("contact_name") or ""
    contact_source = lead.get("contact_source") or ""

    if not contact_name:
        result.source_attribution["contact"] = "none"
        return 0.0

    # Base confidence from source
    source_scores = {
        "website_team_page": 0.9,
        "kvk": 0.85,
        "linkedin": 0.75,
        "google_search": 0.5,
        "inferred": 0.3,
    }
    confidence = source_scores.get(contact_source, 0.3)

    # Bonus: contact name appears on website
    ws_text = sources.get("website", {}).get("company_name", "")
    if contact_name.lower() in ws_text.lower():
        confidence = min(confidence + 0.1, 1.0)

    result.source_attribution["contact"] = contact_source or "unknown"
    return round(confidence, 3)


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def _normalize_company(name: str) -> str:
    """Normalize company name for comparison."""
    if not name:
        return ""
    name = name.lower().strip()
    # Strip common Dutch suffixes
    for suffix in (" b.v.", " bv", " v.o.f.", " vof", " holding", " groep",
                    " nederland", " nl", " b.v", " eenmanszaak"):
        if name.endswith(suffix):
            name = name[:-len(suffix)].rstrip()
    # Remove punctuation
    name = re.sub(r"[^\w\s]", "", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _normalize_domain(domain: str) -> str:
    """Normalize domain for comparison."""
    if not domain:
        return ""
    domain = domain.lower().strip()
    domain = domain.replace("https://", "").replace("http://", "")
    domain = domain.split("/")[0]
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def _normalize_phone(phone: str) -> str:
    """Normalize phone number for comparison (digits only)."""
    if not phone:
        return ""
    return re.sub(r"[^\d]", "", phone)
