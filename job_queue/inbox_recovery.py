"""
job_queue/inbox_recovery.py — herstel voor queued_no_inbox-leads (remediation H5).

De 856 leads die op status='queued_no_inbox' stonden (geen ready inbox tijdens
enrichment) hadden GEEN automatisch herstelpad: enrichment_version=1 +
status≠'discovered' sloten ze uit van queue_all_unenriched_leads. Nu de
inboxen weer ready zijn, herpakt dit pad ALLEEN de inbox_selection-stap —
geen volledige re-enrichment.

Veilig by design: idempotent (leest steeds vers), workspace-aware,
capaciteit-bewust, en met dry_run die niets schrijft maar de verdeling
rapporteert. Geen bulkactivatie zonder voorafgaande dry-run-controle.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def recover_queued_no_inbox(
    supabase_client: Any,
    workspace_id: str,
    warmr_client: Any,
    *,
    dry_run: bool = True,
    limit: int = 2000,
) -> dict:
    """Herpak queued_no_inbox-leads door alleen inbox_selection te herdraaien.

    Returns:
        {dry_run, total, would_link, distribution:{inbox_id:count}, unlinked,
         reason_unlinked}
    """
    from job_queue.enrichment_queue import (
        _get_cached_inboxes, _detect_email_provider, _select_best_inbox,
    )

    leads = (supabase_client.table("leads")
             .select("id, email, email_status")
             .eq("workspace_id", workspace_id)
             .eq("status", "queued_no_inbox")
             .limit(limit).execute().data or [])

    inboxes = await _get_cached_inboxes(supabase_client, warmr_client)
    result = {
        "dry_run": dry_run, "total": len(leads), "would_link": 0,
        "distribution": {}, "unlinked": 0, "reason_unlinked": None,
    }
    if not inboxes:
        result["reason_unlinked"] = "geen ready inboxes"
        result["unlinked"] = len(leads)
        return result

    for lead in leads:
        provider = _detect_email_provider(lead.get("email") or "")
        chosen = _select_best_inbox(inboxes, provider)
        inbox_id = (chosen or {}).get("id") or (chosen or {}).get("inbox_id")
        if not inbox_id:
            result["unlinked"] += 1
            continue
        result["would_link"] += 1
        result["distribution"][inbox_id] = result["distribution"].get(inbox_id, 0) + 1
        if not dry_run:
            # Herpak ALLEEN inbox + status — geen re-enrichment. Idempotent:
            # een tweede run vindt de lead niet meer op status queued_no_inbox.
            email_status = lead.get("email_status")
            new_status = "qualified" if email_status in ("valid", "risky") else "enriched"
            try:
                supabase_client.table("leads").update({
                    "preferred_inbox_id": inbox_id,
                    "status": new_status,
                }).eq("id", lead["id"]).eq("workspace_id", workspace_id).eq(
                    "status", "queued_no_inbox").execute()
            except Exception as e:
                logger.error("inbox_recovery: update faalde voor lead %s: %s", lead["id"], e)
    return result
