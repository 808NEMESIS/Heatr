"""
utils/lead_activity.py — Per-lead activity rollup + status-derivatie.

Haalt 4 bronnen op (lead, sequences, replies, timeline) en consolideert:
  - last_outbound_at + which_step
  - last_inbound_at + classifier-category
  - current_status (manual override wint van derived)
  - status_changed_at
  - next_action_due (voor recontact_later)

Derived-status beslissingsboom (volgorde = prioriteit):
  1. unsubscribe_request OR email_status=unsubscribed → afgemeld
  2. classifier=not_interested → geen_interesse
  3. classifier=not_now (return_date) → recontact_later
  4. classifier=interested OR question → actief_gesprek
  5. classifier=wrong_person → verkeerde_contact
  6. sequence is_active=true → in_sequence
  7. sequence_complete + binnen 90d cooldown → cooldown
  8. cooldown voorbij → klaar_voor_recontact
  9. niets verstuurd ooit → niet_aangeschreven

Manual override (lead.manual_status_override) wint altijd boven derived.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

# All possible CRM statuses — gebruik in beide override + derived
CRM_STATUSES = {
    "afgemeld",
    "geen_interesse",
    "recontact_later",
    "actief_gesprek",
    "verkeerde_contact",
    "in_sequence",
    "cooldown",
    "klaar_voor_recontact",
    "niet_aangeschreven",
}

# Default cooldown na sequence_complete zonder reply
COOLDOWN_DAYS = 90


def parse_iso(s: str | None) -> datetime | None:
    """Public alias — gedeeld met API endpoints die ook ISO-datetime parsing nodig hebben."""
    return _parse_iso(s)


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def derive_status(
    lead: dict[str, Any],
    last_inbound_classification: dict | None,
    last_inbound_at: datetime | None,
    sequence_history: list[dict],
) -> tuple[str, str | None, datetime | None]:
    """Return (status, reason, status_changed_at).

    Manual override wint. Anders beslissingsboom.
    """
    # 1. Manual override wint
    override = lead.get("manual_status_override")
    if override and override in CRM_STATUSES:
        return (
            override,
            f"manual: {lead.get('manual_status_override_reason') or 'no reason given'}",
            _parse_iso(lead.get("manual_status_override_at")),
        )

    # 2. Email-status flag (unsubscribe permanent)
    if (lead.get("email_status") or "").lower() == "unsubscribed":
        return "afgemeld", "email_status=unsubscribed", last_inbound_at

    # 3. Reply-classifier
    if last_inbound_classification:
        cat = last_inbound_classification.get("category") or ""
        if cat == "unsubscribe_request":
            return "afgemeld", f"classifier: {cat}", last_inbound_at
        if cat == "not_interested":
            return "geen_interesse", f"classifier: {cat}", last_inbound_at
        if cat == "not_now":
            return "recontact_later", f"classifier: {cat}", last_inbound_at
        if cat in ("interested", "question"):
            return "actief_gesprek", f"classifier: {cat}", last_inbound_at
        if cat == "wrong_person":
            return "verkeerde_contact", f"classifier: {cat}", last_inbound_at

    # 4. Sequence-state
    active_seq = next((s for s in sequence_history if s.get("is_active")), None)
    if active_seq:
        return "in_sequence", "sequence in flight", _parse_iso(active_seq.get("created_at"))

    completed_seq = next((s for s in sequence_history if s.get("status") == "sequence_complete"), None)
    if completed_seq:
        completed_at = _parse_iso(completed_seq.get("sent_at")) or _parse_iso(completed_seq.get("created_at"))
        if completed_at:
            cooldown_end = completed_at + timedelta(days=COOLDOWN_DAYS)
            if datetime.now(timezone.utc) < cooldown_end:
                return "cooldown", f"sequence afgerond, cooldown tot {cooldown_end.date().isoformat()}", completed_at
            return "klaar_voor_recontact", f"cooldown afgelopen op {cooldown_end.date().isoformat()}", cooldown_end

    return "niet_aangeschreven", "geen sequence_history", None


async def get_lead_activity(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict[str, Any]:
    """Volledige activity-rollup voor één lead. Faalt-tolerant."""
    # 1. Lead row
    try:
        lead_res = (
            supabase_client.table("leads")
            .select(
                "id, company_name, domain, email, email_status, sector, city, "
                "score, archetype, contact_first_name, "
                "manual_status_override, manual_status_override_reason, "
                "manual_status_override_at, manual_status_override_by, "
                "recontact_after, created_at"
            )
            .eq("id", lead_id)
            .eq("workspace_id", workspace_id)
            .maybe_single()
            .execute()
        )
    except Exception as e:
        return {"error": f"lead fetch failed: {e}"}
    if not lead_res or not lead_res.data:
        return {"error": "lead not found"}
    lead = lead_res.data

    # 2. Sequence history (active + completed)
    sequence_history: list[dict] = []
    try:
        sh = (
            supabase_client.table("lead_campaign_history")
            .select("id, status, is_active, step_index, sent_at, created_at, next_send_at")
            .eq("lead_id", lead_id)
            .eq("workspace_id", workspace_id)
            .order("created_at", desc=True)
            .limit(20)
            .execute()
        )
        sequence_history = sh.data or []
    except Exception as e:
        logger.debug("activity: sequence history fetch failed: %s", e)

    last_outbound_at: datetime | None = None
    last_outbound_step: int | None = None
    outbound_count = 0
    for hist in sequence_history:
        sent_at = _parse_iso(hist.get("sent_at"))
        if sent_at:
            outbound_count += 1
            if last_outbound_at is None or sent_at > last_outbound_at:
                last_outbound_at = sent_at
                last_outbound_step = hist.get("step_index")

    # 3. Replies
    last_inbound_at: datetime | None = None
    last_inbound_classification: dict | None = None
    inbound_count = 0
    try:
        replies_res = (
            supabase_client.table("reply_inbox")
            .select("id, received_at, classification, classifier_summary, body_preview, subject")
            .eq("lead_id", lead_id)
            .eq("workspace_id", workspace_id)
            .order("received_at", desc=True)
            .limit(20)
            .execute()
        )
        replies = replies_res.data or []
        inbound_count = len(replies)
        if replies:
            last = replies[0]
            last_inbound_at = _parse_iso(last.get("received_at"))
            last_inbound_classification = {
                "category": last.get("classification"),
                "summary": last.get("classifier_summary"),
            }
    except Exception as e:
        logger.debug("activity: replies fetch failed: %s", e)

    # 4. Derive status
    status, status_reason, status_changed_at = derive_status(
        lead, last_inbound_classification, last_inbound_at, sequence_history,
    )

    # next_action_due voor recontact_later
    next_action_due = None
    if status == "recontact_later":
        # Eerst expliciete date op lead, anders 90 dagen na last_inbound
        recontact_after = _parse_iso(lead.get("recontact_after"))
        if recontact_after:
            next_action_due = recontact_after
        elif last_inbound_at:
            next_action_due = last_inbound_at + timedelta(days=90)

    now = datetime.now(timezone.utc)
    days_since_outbound = (now - last_outbound_at).days if last_outbound_at else None
    days_since_inbound = (now - last_inbound_at).days if last_inbound_at else None

    return {
        "lead_id": lead_id,
        "company_name": lead.get("company_name"),
        "domain": lead.get("domain"),
        "sector": lead.get("sector"),
        "archetype": lead.get("archetype"),
        "score": lead.get("score"),
        # Status
        "status": status,
        "status_reason": status_reason,
        "status_changed_at": status_changed_at.isoformat() if status_changed_at else None,
        "is_manual_override": bool(lead.get("manual_status_override")),
        "manual_override_reason": lead.get("manual_status_override_reason"),
        "manual_override_by": lead.get("manual_status_override_by"),
        # Activity
        "last_outbound_at": last_outbound_at.isoformat() if last_outbound_at else None,
        "last_outbound_step": last_outbound_step,
        "outbound_count": outbound_count,
        "last_inbound_at": last_inbound_at.isoformat() if last_inbound_at else None,
        "last_inbound_category": (last_inbound_classification or {}).get("category"),
        "inbound_count": inbound_count,
        "days_since_last_outbound": days_since_outbound,
        "days_since_last_inbound": days_since_inbound,
        "next_action_due": next_action_due.isoformat() if next_action_due else None,
    }
