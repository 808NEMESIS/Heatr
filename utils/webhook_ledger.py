"""
utils/webhook_ledger.py — inbound webhook-eventledger (fase 3 PR 10).

De at-least-once → exactly-once brug voor Warmr-events: elk event wordt
EERST geregistreerd (UNIQUE op event_id, migratie 026); een redelivery
botst op de unique en wordt geskipt vóór er ook maar één side-effect
(reply_inbox-rij, crm_task, statuswissel) gebeurt — audit v2 scenario 3.

Fail-richting is hier bewust OMGEKEERD aan de outbound-dispatcher:
inbound faalt SOFT. Een onbereikbare ledger mag de verwerking van een
reply/unsubscribe nooit blokkeren (dat zou compliance-events VERLIEZEN);
de dedup degradeert dan tijdelijk, met luide log. Outbound blijft
fail-closed (Besluit 3) — daar is niets doen veilig, hier niet.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Events die een bestaande lead VEREISEN — zonder resolvable lead gaan die
# naar dead_letter i.p.v. stil {"ok": true} (audit v2 scenario 14).
LEAD_BOUND_EVENTS = (
    "interested", "lead.interested", "replied", "lead.replied",
    "bounced", "lead.bounced", "unsubscribed", "lead.unsubscribed",
    "campaign.completed",
)


def make_event_id(payload: dict) -> str:
    """Warmr's event_id, of anders een synthetische: sha256 over de canonieke
    payload. Exacte redeliveries (zelfde body) dedupen daarmee ook zolang het
    Warmr-contract nog geen event_id levert."""
    explicit = payload.get("event_id") or payload.get("id")
    if explicit:
        return str(explicit)
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return "synth:" + hashlib.sha256(canonical.encode()).hexdigest()[:40]


def record_event(
    supabase_client: Any,
    *,
    event_id: str,
    workspace_id: str,
    event_type: str | None,
    payload: dict,
    lead_id: str | None = None,
    campaign_id: str | None = None,
) -> str:
    """Registreer het event. Returns 'new' | 'duplicate' | 'unavailable'."""
    row = {
        "event_id": event_id,
        "workspace_id": workspace_id,
        "event_type": event_type,
        "occurred_at": payload.get("occurred_at"),
        "sequence_no": payload.get("sequence_no"),
        "lead_id": lead_id,
        "campaign_id": campaign_id,
        "message_id": payload.get("message_id"),
        "payload": payload,
        "processing_status": "received",
    }
    try:
        supabase_client.table("webhook_events").insert(row).execute()
        return "new"
    except Exception as e:
        text = str(e)
        if getattr(e, "code", None) == "23505" or "23505" in text or "duplicate key" in text.lower():
            logger.info("webhook_ledger: duplicate delivery geskipt (event=%s type=%s)",
                        event_id, event_type)
            return "duplicate"
        logger.error(
            "webhook_ledger: EVENT-REGISTRATIE MISLUKT (event=%s): %s — inbound "
            "gaat door zonder dedup (fail-soft; is migratie 026 gedraaid?)",
            event_id, e,
        )
        return "unavailable"


def finalize_event(
    supabase_client: Any,
    event_id: str,
    status: str,
    error: str | None = None,
) -> None:
    """Zet de verwerkingsuitkomst ('processed' | 'dead_letter' | 'error').
    Fail-soft: de uitkomst zelf is al bepaald; alleen de administratie mist."""
    try:
        (
            supabase_client.table("webhook_events")
            .update({
                "processing_status": status,
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "error": (error or "")[:1000] or None,
            })
            .eq("event_id", event_id)
            .eq("processing_status", "received")
            .execute()
        )
    except Exception as e:
        logger.error("webhook_ledger: finalisatie mislukt (event=%s → %s): %s",
                     event_id, status, e)
