"""
website_intelligence/hook_writer.py — persistentie van de receptie-haakje-ladder.

Schrijft de uitkomst van hook_detector.detect_receptie (Q4/Q7/Q2/P1 + ladder-haak)
naar de kolommen uit migratie 041 op de heatr_website_intelligence-rij van de lead.
Dit is de writer die de audit miste: er was niets dat hook_* écht in de productie-
flow produceerde. `run_receptie_for_lead` wordt aangeroepen door de website-
analyse-worker (job_queue/website_analysis_queue) en door de backfill.

Alles fail-soft: geen enkele fout mag de pijplijn stoppen (CLAUDE.md-conventie).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _collect_evidence(result: dict) -> dict | None:
    """Bewijs per as uit de eerste geslaagde run (voor transparantie/debug)."""
    for run in (result.get("runs") or []):
        ev = run.get("evidence")
        if ev:
            return ev
    return None


def receptie_row(result: dict, *, detected_at: str | None = None) -> dict[str, Any]:
    """Map een detect_receptie-uitkomst naar de heatr_website_intelligence-kolommen
    (migratie 041). PUUR — testbaar zonder DB."""
    return {
        "receptie_hook_code": result.get("hook_code"),
        "receptie_hook_ladder": result.get("hook_ladder") or [],
        "receptie_second_hook": result.get("second_hook"),
        "receptie_q4": result.get("q4"),
        "receptie_q7": result.get("q7"),
        "receptie_q2": result.get("q2"),
        "receptie_p1": result.get("p1"),
        "receptie_q4_gated": bool(result.get("q4_gated")),
        "receptie_form_present": bool(result.get("form_present")),
        "receptie_evidence": _collect_evidence(result),
        "receptie_detected_at": detected_at or datetime.now(timezone.utc).isoformat(),
    }


def persist_receptie(supabase, lead_id, workspace_id, result: dict, *,
                     detected_at: str | None = None) -> bool:
    """Schrijf de receptie-uitkomst naar de website_intelligence-rij van de lead.
    Fail-soft: logt en returned False bij een fout of een ontbrekende rij; raist
    nooit. Vereist dat migratie 041 gedraaid is (anders schrijft PostgREST een
    kolom-fout → False, pijplijn draait door)."""
    row = receptie_row(result, detected_at=detected_at)
    try:
        resp = (supabase.table("website_intelligence")
                .update(row)
                .eq("workspace_id", workspace_id)
                .eq("lead_id", lead_id)
                .execute())
        if not (resp.data or []):
            logger.warning("persist_receptie: geen WI-rij voor lead %s (nog niet geanalyseerd?)", lead_id)
            return False
        return True
    except Exception as e:
        logger.warning("persist_receptie: schrijven faalde voor lead %s: %s", lead_id, e)
        return False


async def run_receptie_for_lead(supabase, lead: dict, *, playwright=None, browser=None,
                                detected_at: str | None = None) -> dict | None:
    """Detecteer + persist de receptie-haak voor één lead. Fail-soft; returned de
    detect_receptie-uitkomst (of None bij een leeg domein / detectiefout). Geef
    een gedeelde playwright+browser mee voor batchgebruik."""
    from website_intelligence.hook_detector import detect_receptie
    domain = (lead.get("domain") or "").strip()
    if not domain:
        return None
    try:
        result = await detect_receptie(domain, playwright, browser=browser)
    except Exception as e:
        logger.warning("run_receptie_for_lead: detect faalde voor %s: %s", domain, e)
        return None
    persist_receptie(supabase, lead.get("id"), lead.get("workspace_id"), result,
                     detected_at=detected_at)
    return result
