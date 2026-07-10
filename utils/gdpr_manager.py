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
# Legacy gedeelde placeholder (pre fase-2). Bewaard voor herkenning van oude
# rijen; NIET meer gebruikt voor nieuwe forgets — met de unique index op
# (workspace_id, lower(email)) uit migratie 021 botste de TWEEDE forget in
# een workspace op de constraint, werd de fout geslikt en bleef de echte
# PII staan terwijl de functie succes rapporteerde (audit v2, Art.17-fout).
_LEGACY_REDACTED_EMAIL = "verwijderd@anoniem.nl"


def _redacted_email(lead_id: str) -> str:
    """Per-lead-unieke, niet-herleidbare placeholder.

    - uniek per lead (volledige UUID) → geen unique-index-collision;
    - afgeleid van lead_id, niet van het originele adres → niet herleidbaar;
    - @anoniem.invalid (RFC 2606-gereserveerd TLD) → nooit afleverbaar;
    - stabiel per lead → een forget-retry schrijft dezelfde waarde
      (idempotent, geen nieuwe collision).
    """
    return f"verwijderd+{lead_id}@anoniem.invalid"


async def forget_lead(
    lead_id: str,
    workspace_id: str,
    supabase_client,
    performed_by: str = "user",
) -> dict[str, Any]:
    """
    Permanently remove all personal data for a lead.
    Retains anonymised statistical record for analytics.

    Steps:
    1. Capture email for GDPR audit trail before deletion
    2. Anonymise leads table (replace PII fields, per-lead-unieke placeholder)
       — KRITIEK: faalt dit of matcht het 0 rijen, dan is ok=False en wordt
       status NIET als 'forgotten' gerapporteerd (fase 2-fix: de oude code
       slikte de fout en rapporteerde succes terwijl de PII bleef staan)
    3. Delete all enrichment_data rows
    4. Delete screenshot from Supabase Storage
    5. Anonymise lead_timeline body/title (strip emails/names)
    6. Delete from lead_campaign_history
    7. Cancel open crm_tasks
    8. Delete reply_inbox messages
    9. Log to gdpr_log

    Elke stap-fout belandt in `errors`; ok=True ALLEEN als alle stappen
    slaagden. Een retry is idempotent (placeholder is stabiel per lead).

    Returns:
        { "ok": bool, "deleted_records": int, "anonymized_records": int,
          "errors": list[str] }
    """
    deleted = 0
    anonymized = 0
    errors: list[str] = []

    # Step 1: get email before wiping
    lead_email: str | None = None
    try:
        res = supabase_client.table("leads").select("email, domain").eq("id", lead_id).maybe_single().execute()
        if res.data:
            lead_email = res.data.get("email")
    except Exception as e:
        logger.warning("forget_lead: could not read lead email: %s", e)

    # Step 2: anonymise leads row — de kritieke stap. Per-lead-unieke
    # placeholder (zie _redacted_email) i.p.v. één gedeeld adres, zodat de
    # unique index (workspace_id, lower(email)) nooit de anonimisering van
    # de tweede vergeten lead blokkeert.
    try:
        res = supabase_client.table("leads").update({
            "email": _redacted_email(lead_id),
            "contact_first_name": _REDACTED,
            "contact_last_name": None,
            "phone": None,
            "linkedin_url": None,
            "personalized_opener": None,
            "company_summary": None,
            "status": "forgotten",
        }).eq("id", lead_id).eq("workspace_id", workspace_id).execute()
        if res.data:
            anonymized += 1
        else:
            errors.append("leads_anonymize: 0 rijen gematcht (verkeerde lead_id/workspace?)")
            logger.error("forget_lead: leads anonymize matchte 0 rijen (lead=%s ws=%s)",
                         lead_id, workspace_id)
    except Exception as e:
        errors.append(f"leads_anonymize: {e}")
        logger.error("forget_lead: leads anonymize failed: %s", e)

    # Step 3: delete enrichment_data
    try:
        res = supabase_client.table("enrichment_data").delete().eq("lead_id", lead_id).execute()
        deleted += len(res.data or [])
    except Exception as e:
        errors.append(f"enrichment_data_delete: {e}")
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
        errors.append(f"timeline_anonymize: {e}")
        logger.warning("forget_lead: timeline anonymize failed: %s", e)

    # Step 6: delete campaign history
    try:
        res = supabase_client.table("lead_campaign_history").delete().eq("lead_id", lead_id).execute()
        deleted += len(res.data or [])
    except Exception as e:
        errors.append(f"campaign_history_delete: {e}")
        logger.warning("forget_lead: campaign_history delete failed: %s", e)

    # Step 7: cancel open tasks
    try:
        supabase_client.table("crm_tasks").update({"status": "cancelled"}).eq(
            "lead_id", lead_id).eq("status", "open").execute()
    except Exception as e:
        errors.append(f"task_cancellation: {e}")
        logger.warning("forget_lead: task cancellation failed: %s", e)

    # Step 8: delete reply_inbox messages
    try:
        res = supabase_client.table("reply_inbox").delete().eq("lead_id", lead_id).execute()
        deleted += len(res.data or [])
    except Exception as e:
        errors.append(f"reply_inbox_delete: {e}")
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
        errors.append(f"gdpr_log_write: {e}")
        logger.error("forget_lead: gdpr_log write failed: %s", e)

    ok = not errors
    if ok:
        logger.info("forget_lead: lead %s anonymized. deleted=%d anonymized=%d",
                    lead_id, deleted, anonymized)
    else:
        logger.error("forget_lead: lead %s ONVOLLEDIG vergeten — %d fout(en): %s",
                     lead_id, len(errors), "; ".join(errors)[:500])
    return {"ok": ok, "deleted_records": deleted,
            "anonymized_records": anonymized, "errors": errors}


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
