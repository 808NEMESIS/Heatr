"""
campaigns/enrollment.py — tracking-only enrollments (ADR-001, fase 3 PR 9).

Warmr is eigenaar van de dripsequence; Heatr registreert per gepushte lead
één `lead_campaign_history`-rij met send_owner='warmr' en status='active'.
Deze rijen voeden — voor het eerst met echte data — de dedup
(`is_lead_in_active_campaign`), de 90-dagen-cooldown
(`campaign_cooldown_block`) en de volume-guards, ZONDER het tweede
verzendmodel te activeren: `get_due_sends` selecteert uitsluitend
send_owner='heatr' (de harde grens uit ADR-001).

Idempotentie: de partial-UNIQUE `uq_lch_enrollment`
(workspace_id, lead_id, campaign_id) uit migratie 025 maakt her-registratie
van dezelfde enrollment een no-op. Een bewuste her-benadering krijgt een
nieuwe Warmr-campagne (nieuwe campaign_id) en dus een nieuwe rij.

Fail-gedrag: de writes zijn fail-soft-met-luide-log — op het moment van
registratie is de push naar Warmr al gebeurd; een raise zou de operator een
vals "niet verstuurd" geven. Een gemiste registratie betekent: dedup/cooldown
blind voor precies deze enrollment (zichtbaar via de error-log en de
`enrollment_failed`-teller in de response).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Statussen die "in flight" betekenen — alleen deze mag de webhook-closure
# overschrijven (terminal-guard: een laat campaign.completed-event mag een
# eerdere replied/unsubscribed/bounced nooit terugdraaien; audit v2 scenario 4).
IN_FLIGHT_STATUSES = ("active", "pending", "paused")


def _is_unique_violation(exc: Exception) -> bool:
    text = str(exc)
    return getattr(exc, "code", None) == "23505" or "23505" in text or "duplicate key" in text.lower()


async def record_warmr_enrollments(
    supabase_client: Any,
    *,
    workspace_id: str,
    leads: list[dict],
    campaign_id: str,
    template_id: str | None = None,
    service_type: str | None = None,
    sequence_steps: list | None = None,
    inbox_id: str | None = None,
) -> dict:
    """Registreer per gepushte lead een tracking-enrollment (send_owner='warmr').

    Returns:
        {"created": int, "already_enrolled": int, "failed": int}
    """
    now = datetime.now(timezone.utc).isoformat()
    created = already = failed = 0
    for lead in leads:
        lead_id = lead.get("id")
        if not lead_id:
            continue
        row = {
            "workspace_id": workspace_id,
            "lead_id": lead_id,
            "campaign_id": campaign_id,
            "send_owner": "warmr",       # ADR-001: get_due_sends negeert deze rij
            "status": "active",          # NOOIT 'pending' — dat is Model B-vocabulaire
            "is_active": True,
            "step_index": 0,
            "sequence_steps": sequence_steps or [],
            "inbox_id": inbox_id,
            "template_id": template_id,
            "service_type": service_type,
            "enrolled_at": now,
        }
        try:
            supabase_client.table("lead_campaign_history").insert(row).execute()
            created += 1
        except Exception as e:
            if _is_unique_violation(e):
                already += 1
                continue
            failed += 1
            logger.error(
                "enrollment: REGISTRATIE MISLUKT (lead=%s campaign=%s): %s — "
                "dedup/cooldown zijn blind voor deze enrollment. Is migratie 025 gedraaid?",
                lead_id, campaign_id, e,
            )
    if created or already or failed:
        logger.info(
            "enrollment: campaign=%s → %d nieuw, %d al ingeschreven, %d mislukt",
            campaign_id, created, already, failed,
        )
    return {"created": created, "already_enrolled": already, "failed": failed}


def close_campaign_enrollments(
    supabase_client: Any,
    *,
    workspace_id: str,
    lead_id: str,
    mapped_status: str,
    campaign_id: str | None = None,
    completed: bool = False,
) -> int:
    """Sluit in-flight enrollments af op een terminale webhook-status.

    - Terminal-guard: alleen rijen met een IN_FLIGHT_STATUSES-status worden
      geraakt — een laat `campaign.completed` (→ no_response) overschrijft
      nooit een eerder `replied`/`unsubscribed`/`bounced` (scenario 4).
    - Campaign-scoping: als de payload een campaign_id levert, raakt de
      update alléén die campagne — een no_response van campagne X mag een
      replied van campagne Y niet vervuilen (audit v2 §C2).
    - `is_active=False` + `sent_at`-backfill: dit is wat F7 fixt — de rij
      verlaat de "actieve campagne"-set én wordt het anker voor de
      90-dagen-cooldown. sent_at is een benadering (afsluitmoment) tot de
      message-eventledger (PR 10) exacte send-tijden levert; conservatiever
      = langere cooldown-bescherming.

    Returns: aantal afgesloten rijen.
    """
    now = datetime.now(timezone.utc).isoformat()
    update: dict[str, Any] = {
        "status": mapped_status,
        "is_active": False,
        "last_event_at": now,
        ("completed_at" if completed else "stopped_at"): now,
    }
    q = (
        supabase_client.table("lead_campaign_history")
        .update(update)
        .eq("lead_id", lead_id)
        .eq("workspace_id", workspace_id)
        .in_("status", list(IN_FLIGHT_STATUSES))
    )
    if campaign_id:
        q = q.eq("campaign_id", campaign_id)
    res = q.execute()
    closed = len(res.data or [])

    if closed:
        # Cooldown-anker: campaign_cooldown_block filtert op is_active=False
        # én een gezette sent_at — backfill alleen waar die nog leeg is.
        backfill = (
            supabase_client.table("lead_campaign_history")
            .update({"sent_at": now})
            .eq("lead_id", lead_id)
            .eq("workspace_id", workspace_id)
            .eq("status", mapped_status)
            .is_("sent_at", "null")
        )
        if campaign_id:
            backfill = backfill.eq("campaign_id", campaign_id)
        backfill.execute()
    return closed
