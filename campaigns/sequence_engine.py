"""
campaigns/sequence_engine.py — Email sequence engine for Heatr.

Manages multi-step outreach sequences. Integrates with Warmr for actual
sending and SendingGuard for safety checks before every send.

n8n workflow 01-sequence-due-sends.json calls GET /sequences/due-sends
every 15 minutes to process pending sends.
"""

from __future__ import annotations

import logging
import os
import re
import random
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---- Constants ---------------------------------------------------------------
MAX_SEQUENCE_STEPS      = 4       # including initial email
MIN_WAIT_DAYS           = 2       # minimum delay between steps
SPAM_WORDS = {
    "free", "gratis", "gegarandeerd", "klik hier", "nu kopen",
    "100%", "risico vrij", "risicovrij", "win", "winnaar",
    "geld verdienen", "snel rijk",
}
RECONTACT_COOLDOWN_DAYS = int(os.getenv("RECONTACT_COOLDOWN_DAYS", "90"))


# ==============================================================================
# Validation
# ==============================================================================

def validate_sequence_config(steps: list[dict]) -> tuple[bool, list[str]]:
    """
    Validate a sequence configuration before creating a campaign.

    Args:
        steps: List of sequence step dicts with keys:
               subject, body, delay_days (0 for first step)

    Returns:
        (is_valid: bool, errors: list[str])
        errors is empty when is_valid is True.
    """
    errors: list[str] = []

    if not steps:
        errors.append("Sequence heeft minimaal 1 stap nodig.")
        return False, errors

    if len(steps) > MAX_SEQUENCE_STEPS:
        errors.append(
            f"Sequence heeft maximaal {MAX_SEQUENCE_STEPS} stappen "
            f"(inclusief eerste email). Je hebt {len(steps)} stappen."
        )

    for i, step in enumerate(steps):
        label = f"Stap {i + 1}"

        # Subject required
        subject = (step.get("subject") or "").strip()
        if not subject:
            errors.append(f"{label}: onderwerp is verplicht.")
        else:
            # Spam word check in subject
            subject_lower = subject.lower()
            found_spam = [w for w in SPAM_WORDS if w in subject_lower]
            if found_spam:
                errors.append(
                    f"{label}: onderwerp bevat spam-gevoelige woorden: {', '.join(found_spam)}."
                )

        # Body required + minimum length
        body = (step.get("body") or "").strip()
        if not body:
            errors.append(f"{label}: berichttekst is verplicht.")
        else:
            word_count = len(re.findall(r"\w+", body))
            if word_count < 50:
                errors.append(
                    f"{label}: berichttekst heeft minimaal 50 woorden "
                    f"(nu {word_count} woorden)."
                )

        # Wait days — first step exempt, follow-ups minimum MIN_WAIT_DAYS
        if i > 0:
            wait = int(step.get("delay_days") or step.get("wait_days") or 0)
            if wait < MIN_WAIT_DAYS:
                errors.append(
                    f"{label}: wachttijd moet minimaal {MIN_WAIT_DAYS} dagen zijn "
                    f"(nu {wait} dag(en)). Automatisch verhoogd bij uitvoering."
                )
                # Not a hard block, just a warning-as-error per spec
                # Caller can decide to auto-fix

    is_valid = len(errors) == 0
    return is_valid, errors


def auto_fix_sequence_config(steps: list[dict]) -> list[dict]:
    """
    Auto-fix correctable issues in a sequence config.
    Currently fixes: delay_days < MIN_WAIT_DAYS → set to MIN_WAIT_DAYS.
    Returns a new list — does not mutate input.
    """
    fixed = []
    for i, step in enumerate(steps):
        s = dict(step)
        if i > 0:
            wait = int(s.get("delay_days") or s.get("wait_days") or 0)
            if wait < MIN_WAIT_DAYS:
                s["delay_days"] = MIN_WAIT_DAYS
                logger.info("Sequence: auto-fixed step %d delay %d→%d days", i + 1, wait, MIN_WAIT_DAYS)
        fixed.append(s)
    return fixed


# ==============================================================================
# Variable injection + spintax
# ==============================================================================

def resolve_spintax(text: str) -> str:
    """
    Resolve {option1|option2|option3} spintax in text.
    Randomly picks one option per group.
    """
    def pick(match: re.Match) -> str:
        options = match.group(1).split("|")
        return random.choice(options).strip()

    return re.sub(r"\{([^{}]+)\}", pick, text)


def inject_variables(text: str, lead: dict) -> str:
    """
    Replace {{variable}} placeholders with lead data.

    Available variables:
      {{first_name}}  {{company}}  {{city}}  {{opener}}  {{sector}}
      {{website}}     {{score}}
    """
    replacements = {
        "{{first_name}}": lead.get("contact_first_name") or lead.get("company_name", "").split()[0],
        "{{company}}":    lead.get("company_name") or lead.get("domain") or "",
        "{{city}}":       lead.get("city") or "",
        "{{opener}}":     lead.get("personalized_opener") or "",
        "{{sector}}":     lead.get("sector") or "",
        "{{website}}":    f"https://{lead.get('domain')}" if lead.get("domain") else "",
        "{{score}}":      str(lead.get("website_score") or ""),
    }
    for placeholder, value in replacements.items():
        text = text.replace(placeholder, value or "")
    return text


def render_step(step: dict, lead: dict) -> dict:
    """
    Render a single sequence step for a specific lead.
    Applies variable injection and spintax resolution.

    Returns:
        { "subject": str, "body": str, "delay_days": int }
    """
    subject = inject_variables(resolve_spintax(step.get("subject") or ""), lead)
    body    = inject_variables(resolve_spintax(step.get("body") or ""), lead)
    return {
        "subject":    subject,
        "body":       body,
        "delay_days": int(step.get("delay_days") or 0),
    }


# ==============================================================================
# Due-send processing
# ==============================================================================

async def get_due_sends(workspace_id: str, supabase_client, limit: int = 50) -> list[dict]:
    """
    Return lead_campaign_history rows where next send is due.
    Called by n8n every 15 minutes.

    Returns list of pending send records with full lead + sequence data.
    """
    now = datetime.now(timezone.utc).isoformat()
    try:
        res = (
            supabase_client.table("lead_campaign_history")
            .select("*, leads(id, company_name, city, sector, email, status, gdpr_safe, "
                    "contact_first_name, domain, personalized_opener, snoozed_until, "
                    "next_contact_after, crm_stage)")
            .eq("workspace_id", workspace_id)
            .eq("status", "pending")
            .eq("is_active", True)
            .lte("next_send_at", now)
            .order("next_send_at", desc=False)
            .limit(limit)
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.error("get_due_sends failed: %s", e)
        return []


async def process_due_send(
    send_record: dict,
    supabase_client,
    warmr_client=None,
) -> dict[str, Any]:
    """
    Process a single pending send from lead_campaign_history.

    1. Run SendingGuard checks
    2. Render sequence step for this lead
    3. Push to Warmr (or skip if dry_run)
    4. Update lead_campaign_history: status, step_index, next_send_at
    5. Log to lead_timeline

    Returns:
        { "sent": bool, "reason": str, "lead_id": str }
    """
    from utils.sending_guard import SendingGuard
    from integrations.warmr_client import WarmrClient

    lead = send_record.get("leads") or {}
    lead_id     = lead.get("id") or send_record.get("lead_id")
    inbox_id    = send_record.get("inbox_id") or send_record.get("preferred_inbox_id")
    workspace_id = send_record.get("workspace_id")
    record_id   = send_record.get("id")

    # Safety check
    guard = SendingGuard()
    can_send, block_reason = await guard.check_can_send(
        lead_id=lead_id,
        inbox_id=inbox_id or "",
        workspace_id=workspace_id,
        supabase_client=supabase_client,
    )

    if not can_send:
        logger.info("Send blocked for lead %s: %s", lead_id, block_reason)
        _mark_send_blocked(record_id, block_reason, supabase_client)
        return {"sent": False, "reason": block_reason, "lead_id": lead_id}

    # Load sequence steps from campaign config
    sequence_steps = send_record.get("sequence_steps") or []
    step_index = int(send_record.get("step_index") or 0)

    if step_index >= len(sequence_steps):
        # Sequence complete
        await _complete_sequence(record_id, lead_id, workspace_id, supabase_client)
        return {"sent": False, "reason": "sequence_complete", "lead_id": lead_id}

    step = render_step(sequence_steps[step_index], lead)

    # Push to Warmr
    try:
        wc = warmr_client or WarmrClient()
        campaign_id = send_record.get("campaign_id")
        await wc.push_lead(
            lead,
            campaign_id=campaign_id,
            preferred_inbox_id=inbox_id,
            custom_subject=step["subject"],
            custom_body=step["body"],
        )
    except Exception as e:
        logger.error("Warmr push failed for lead %s: %s", lead_id, e)
        _mark_send_error(record_id, str(e), supabase_client)
        return {"sent": False, "reason": f"warmr_error: {e}", "lead_id": lead_id}

    # Advance sequence
    next_step_idx = step_index + 1
    is_last = next_step_idx >= len(sequence_steps)

    if is_last:
        next_send_at = None
        status = "sequence_complete"
    else:
        wait_days = int(sequence_steps[next_step_idx].get("delay_days") or MIN_WAIT_DAYS)
        wait_days = max(wait_days, MIN_WAIT_DAYS)
        next_send_at = (datetime.now(timezone.utc) + timedelta(days=wait_days)).isoformat()
        status = "pending"

    try:
        supabase_client.table("lead_campaign_history").update({
            "status": status,
            "step_index": next_step_idx,
            "next_send_at": next_send_at,
            "sent_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", record_id).execute()
    except Exception as e:
        logger.error("Failed to advance sequence record %s: %s", record_id, e)

    # Increment contact attempt count
    try:
        supabase_client.table("leads").update({
            "contact_attempt_count": (lead.get("contact_attempt_count") or 0) + 1,
        }).eq("id", lead_id).execute()
    except Exception:
        pass

    # Timeline entry
    _log_timeline_event(
        supabase_client, workspace_id, lead_id,
        "email_sent",
        f"Email verzonden: {step['subject']} (stap {step_index + 1})",
        metadata={"step_index": step_index, "campaign_id": send_record.get("campaign_id")},
    )

    return {"sent": True, "reason": "ok", "lead_id": lead_id}


async def _complete_sequence(
    record_id: str,
    lead_id: str,
    workspace_id: str,
    db,
) -> None:
    """Mark sequence as complete, set lead to no_response + cooldown."""
    recontact_after = (
        datetime.now(timezone.utc) + timedelta(days=RECONTACT_COOLDOWN_DAYS)
    ).isoformat()

    try:
        db.table("lead_campaign_history").update({
            "status": "sequence_complete",
            "is_active": False,
        }).eq("id", record_id).execute()
    except Exception as e:
        logger.warning("_complete_sequence: history update failed: %s", e)

    try:
        db.table("leads").update({
            "status": "no_response",
            "next_contact_after": recontact_after,
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.warning("_complete_sequence: leads update failed: %s", e)

    _log_timeline_event(
        db, workspace_id, lead_id, "sequence_completed",
        f"Sequence afgerond — heractivatie mogelijk na {RECONTACT_COOLDOWN_DAYS} dagen",
        metadata={"recontact_after": recontact_after},
    )


async def stop_all_sequences_for_lead(lead_id: str, workspace_id: str, db) -> int:
    """
    Cancel all active sequences for a lead (used on unsubscribe/forget).
    Returns count of stopped sequences.
    """
    try:
        res = db.table("lead_campaign_history").update({
            "status": "unsubscribed",
            "is_active": False,
        }).eq("lead_id", lead_id).eq("workspace_id", workspace_id).eq("is_active", True).execute()
        stopped = len(res.data or [])
        logger.info("stop_all_sequences: stopped %d sequences for lead %s", stopped, lead_id)
        return stopped
    except Exception as e:
        logger.error("stop_all_sequences failed for lead %s: %s", lead_id, e)
        return 0


def _mark_send_blocked(record_id: str, reason: str, db) -> None:
    try:
        db.table("lead_campaign_history").update({
            "status": "blocked",
            "block_reason": reason,
        }).eq("id", record_id).execute()
    except Exception:
        pass


def _mark_send_error(record_id: str, error: str, db) -> None:
    try:
        db.table("lead_campaign_history").update({
            "status": "error",
            "block_reason": error,
        }).eq("id", record_id).execute()
    except Exception:
        pass


def _log_timeline_event(db, workspace_id, lead_id, event_type, title, metadata=None) -> None:
    try:
        db.table("lead_timeline").insert({
            "workspace_id": workspace_id,
            "lead_id": lead_id,
            "event_type": event_type,
            "title": title,
            "metadata": metadata or {},
            "created_by": "sequence_engine",
        }).execute()
    except Exception as e:
        logger.debug("Timeline log failed: %s", e)


# ==============================================================================
# Snooze wake-up
# ==============================================================================

async def wake_snoozed_leads(workspace_id: str, supabase_client) -> int:
    """
    Move leads from 'later' stage back to previous stage when snooze expires.
    Called by n8n workflow 02-snooze-wakeup every 15 minutes.

    Returns count of woken leads.
    """
    now = datetime.now(timezone.utc).isoformat()
    try:
        res = (
            supabase_client.table("leads")
            .update({"crm_stage": "ontdekt", "snoozed_until": None})
            .eq("workspace_id", workspace_id)
            .eq("crm_stage", "later")
            .lte("snoozed_until", now)
            .execute()
        )
        woken = len(res.data or [])
        if woken:
            logger.info("wake_snoozed_leads: %d leads woken in workspace %s", woken, workspace_id)
        return woken
    except Exception as e:
        logger.error("wake_snoozed_leads failed: %s", e)
        return 0


async def reactivate_snoozed_tasks(workspace_id: str, supabase_client) -> int:
    """
    Reactivate snoozed tasks whose snooze_until has passed.
    Called by n8n workflow 02-snooze-wakeup every 15 minutes.
    """
    now = datetime.now(timezone.utc).isoformat()
    try:
        res = (
            supabase_client.table("crm_tasks")
            .update({"status": "open", "snoozed_until": None})
            .eq("workspace_id", workspace_id)
            .eq("status", "snoozed")
            .lte("snoozed_until", now)
            .execute()
        )
        count = len(res.data or [])
        return count
    except Exception as e:
        logger.error("reactivate_snoozed_tasks failed: %s", e)
        return 0
