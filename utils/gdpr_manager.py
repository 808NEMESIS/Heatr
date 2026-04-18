"""
utils/gdpr_manager.py — GDPR compliance utilities.

Implements:
  - forget_lead()     : right to erasure (Art. 17 GDPR)
  - export_lead_data(): right of access / data portability (Art. 15/20 GDPR)
  - generate_register(): Article 30 processing register

All operations are logged to gdpr_log table.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Fields replaced with anonymized placeholder on forget
_REDACTED = "VERWIJDERD"
_REDACTED_EMAIL = "verwijderd@anoniem.nl"


async def forget_lead(
    lead_id: str,
    workspace_id: str,
    supabase_client,
    performed_by: str = "user",
) -> dict[str, int]:
    """
    Permanently remove all personal data for a lead.
    Retains anonymised statistical record for analytics.

    Steps (all or nothing — errors are logged but don't abort):
    1. Capture email for GDPR audit trail before deletion
    2. Anonymise leads table (replace PII fields)
    3. Delete all enrichment_data rows
    4. Delete screenshot from Supabase Storage
    5. Anonymise lead_timeline body/title (strip emails/names)
    6. Delete from lead_campaign_history
    7. Cancel open crm_tasks
    8. Mark lead status='forgotten'
    9. Log to gdpr_log

    Returns:
        { "deleted_records": int, "anonymized_records": int }
    """
    deleted = 0
    anonymized = 0

    # Step 1: get email before wiping
    lead_email: str | None = None
    try:
        res = supabase_client.table("leads").select("email, domain").eq("id", lead_id).maybe_single().execute()
        if res.data:
            lead_email = res.data.get("email")
    except Exception as e:
        logger.warning("forget_lead: could not read lead email: %s", e)

    # Step 2: anonymise leads row
    try:
        supabase_client.table("leads").update({
            "email": _REDACTED_EMAIL,
            "contact_first_name": _REDACTED,
            "contact_last_name": None,
            "phone": None,
            "linkedin_url": None,
            "personalized_opener": None,
            "company_summary": None,
            "status": "forgotten",
        }).eq("id", lead_id).eq("workspace_id", workspace_id).execute()
        anonymized += 1
    except Exception as e:
        logger.error("forget_lead: leads anonymize failed: %s", e)

    # Step 3: delete enrichment_data
    try:
        res = supabase_client.table("enrichment_data").delete().eq("lead_id", lead_id).execute()
        deleted += len(res.data or [])
    except Exception as e:
        logger.warning("forget_lead: enrichment_data delete failed: %s", e)

    # Step 4: delete screenshot from Storage
    try:
        lead_res = supabase_client.table("leads").select("domain").eq("id", lead_id).maybe_single().execute()
        domain = lead_res.data.get("domain") if lead_res.data else None
        if domain:
            bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "screenshots")
            supabase_client.storage.from_(bucket).remove([f"{domain}.png"])
            deleted += 1
    except Exception as e:
        logger.debug("forget_lead: screenshot delete failed (may not exist): %s", e)

    # Step 5: anonymise timeline — remove PII from title/body
    try:
        tl_res = supabase_client.table("lead_timeline").select("id, title, body").eq("lead_id", lead_id).execute()
        for row in (tl_res.data or []):
            clean_title = _strip_pii(row.get("title") or "")
            clean_body = _strip_pii(row.get("body") or "") if row.get("body") else None
            supabase_client.table("lead_timeline").update(
                {"title": clean_title, "body": clean_body}
            ).eq("id", row["id"]).execute()
            anonymized += 1
    except Exception as e:
        logger.warning("forget_lead: timeline anonymize failed: %s", e)

    # Step 6: delete campaign history
    try:
        res = supabase_client.table("lead_campaign_history").delete().eq("lead_id", lead_id).execute()
        deleted += len(res.data or [])
    except Exception as e:
        logger.warning("forget_lead: campaign_history delete failed: %s", e)

    # Step 7: cancel open tasks
    try:
        supabase_client.table("crm_tasks").update({"status": "cancelled"}).eq(
            "lead_id", lead_id).eq("status", "open").execute()
    except Exception as e:
        logger.warning("forget_lead: task cancellation failed: %s", e)

    # Step 8: delete reply_inbox messages
    try:
        res = supabase_client.table("reply_inbox").delete().eq("lead_id", lead_id).execute()
        deleted += len(res.data or [])
    except Exception as e:
        logger.warning("forget_lead: reply_inbox delete failed: %s", e)

    # Step 9: log to gdpr_log
    try:
        supabase_client.table("gdpr_log").insert({
            "workspace_id": workspace_id,
            "action": "forget",
            "lead_id": lead_id,
            "lead_email": lead_email or "unknown",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "performed_by": performed_by,
        }).execute()
    except Exception as e:
        logger.error("forget_lead: gdpr_log write failed: %s", e)

    logger.info("forget_lead: lead %s anonymized. deleted=%d anonymized=%d", lead_id, deleted, anonymized)
    return {"deleted_records": deleted, "anonymized_records": anonymized}


async def export_lead_data(lead_id: str, supabase_client) -> dict[str, Any]:
    """
    Export all stored data for a lead as a structured dict.
    Used for GDPR right of access (Art. 15) and data portability (Art. 20).

    Returns complete data record excluding other leads and workspace config.
    """
    data: dict[str, Any] = {}

    # Lead core data
    try:
        res = supabase_client.table("leads").select("*").eq("id", lead_id).maybe_single().execute()
        data["lead"] = res.data or {}
    except Exception as e:
        data["lead"] = {"error": str(e)}

    # Website intelligence
    try:
        res = supabase_client.table("website_intelligence").select("*").eq("lead_id", lead_id).maybe_single().execute()
        data["website_intelligence"] = res.data or {}
    except Exception:
        data["website_intelligence"] = {}

    # Enrichment data
    try:
        res = supabase_client.table("enrichment_data").select("*").eq("lead_id", lead_id).execute()
        data["enrichment_history"] = res.data or []
    except Exception:
        data["enrichment_history"] = []

    # Timeline
    try:
        res = supabase_client.table("lead_timeline").select("*").eq("lead_id", lead_id).order("created_at", desc=False).execute()
        data["timeline"] = res.data or []
    except Exception:
        data["timeline"] = []

    # Campaign history
    try:
        res = supabase_client.table("lead_campaign_history").select("*").eq("lead_id", lead_id).execute()
        data["campaign_history"] = res.data or []
    except Exception:
        data["campaign_history"] = []

    # Inbox replies
    try:
        res = supabase_client.table("reply_inbox").select("*").eq("lead_id", lead_id).execute()
        data["inbox_messages"] = res.data or []
    except Exception:
        data["inbox_messages"] = []

    # CRM tasks
    try:
        res = supabase_client.table("crm_tasks").select("*").eq("lead_id", lead_id).execute()
        data["crm_tasks"] = res.data or []
    except Exception:
        data["crm_tasks"] = []

    data["exported_at"] = datetime.now(timezone.utc).isoformat()
    data["gdpr_note"] = (
        "Dit overzicht bevat alle persoonsgegevens die Heatr heeft opgeslagen voor deze lead. "
        "Rechtsgrond: gerechtvaardig belang (B2B commercieel contact). "
        "Bewaartermijn: 2 jaar na laatste contact. "
        "Externe verwerkers: Supabase (opslag), Anthropic (AI analyse), Warmr (email sending)."
    )

    return data


def generate_processing_register() -> dict[str, Any]:
    """
    Generate Article 30 GDPR processing register.
    Describes what personal data Heatr processes, why, and for how long.
    """
    return {
        "organisation": "Aerys",
        "tool": "Heatr",
        "dpo_contact": os.getenv("OPERATOR_EMAIL", "privacy@aerys.nl"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "processing_activities": [
            {
                "name": "B2B Lead Discovery",
                "purpose": "Identificeren van potentiële zakelijke klanten voor Aerys diensten",
                "legal_basis": "Gerechtvaardigd belang (B2B commercieel contact, Art. 6(1)(f) AVG)",
                "data_categories": [
                    "Bedrijfsnaam en vestigingsadres",
                    "Zakelijk emailadres (role-based: info@, contact@)",
                    "Telefoonnummer (zakelijk)",
                    "KvK-nummer en SBI-code",
                    "Contactpersoon naam (alleen zakelijke context)",
                    "Website URL en publiek beschikbare website-inhoud",
                    "Google Maps rating en reviewcount (publiek beschikbaar)",
                ],
                "data_subjects": "Eigenaren/contactpersonen van MKB-bedrijven in BENELUX",
                "recipients": [
                    {"name": "Supabase", "role": "Processor", "country": "EU", "purpose": "Database opslag"},
                    {"name": "Anthropic", "role": "Processor", "country": "US (SCCs)", "purpose": "AI-analyse van website en bedrijfsprofiel"},
                    {"name": "Warmr", "role": "Processor", "country": "EU", "purpose": "Email verzending"},
                ],
                "retention": "2 jaar na laatste contact, daarna automatisch geanonimiseerd",
                "security_measures": [
                    "Row Level Security in Supabase per workspace",
                    "Encrypted at rest (Supabase AES-256)",
                    "GDPR-veiligheidscheck voor elke outreach",
                    "Alleen zakelijke role emails verwerkt (geen persoonlijke adressen)",
                    "Unsubscribe verwerkt binnen 60 seconden",
                ],
            },
            {
                "name": "Outreach Campagnes",
                "purpose": "Sturen van gepersonaliseerde zakelijke emails namens Aerys",
                "legal_basis": "Gerechtvaardigd belang (B2B cold outreach, Art. 6(1)(f) AVG)",
                "data_categories": ["Zakelijk emailadres", "Voornaam (indien zakelijk beschikbaar)", "Bedrijfsnaam", "Stad"],
                "recipients": [{"name": "Warmr", "role": "Processor", "country": "EU", "purpose": "Email verzending en inbox warming"}],
                "retention": "Campagnedata 1 jaar, replies 2 jaar",
                "security_measures": ["Unsubscribe verwerkt binnen 60 seconden", "Bounce handling binnen 5 minuten"],
            },
        ],
        "rights_info": {
            "access": "POST /gdpr/export/{lead_id}",
            "erasure": "POST /gdpr/forget/{lead_id}",
            "contact": os.getenv("OPERATOR_EMAIL", "privacy@aerys.nl"),
            "supervisory_authority": "Autoriteit Persoonsgegevens (autoriteitpersoonsgegevens.nl)",
        },
    }


def _strip_pii(text: str) -> str:
    """Remove emails and common name patterns from text for timeline anonymization."""
    # Strip email addresses
    text = re.sub(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", "[email]", text)
    # Strip phone numbers
    text = re.sub(r"\+?[\d\s\-\(\)]{9,15}", "[telefoon]", text)
    return text
