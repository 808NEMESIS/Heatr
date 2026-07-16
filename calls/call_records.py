"""
calls/call_records.py — data-laag voor gesprekstranscripten + uitkomst (gate 1).

Een gesprek dat niet op een 'ja' eindigt krijgt hier een record (heatr_call_records)
en een menselijk gekozen uitkomst. Geen AI-classificatie van de uitkomst: de
operator opent tóch het checkup_data-formulier (tijdswinst nul), en de nuance
zit niet in het transcript ("even geen budget" kan "ik vertrouw je niet" zijn).
Een AI-voorstel zou bovendien op het verkeerde antwoord ankeren.

Twee menselijke gates leven hier / hierboven:
  - gate 1 = uitkomst kiezen (set_outcome) — altijd de operator.
  - gate 2 = rapport vrijgeven (endpoints) — altijd de operator.

Matching is fail-closed: precies één e-mailmatch of 'unmatched'. Geen fuzzy
matching op naam/bedrijf (een verkeerd gekoppeld transcript = andermans cijfers
naar de verkeerde kliniek).

`checkup_data` (op heatr_leads.checkup_data) — cijfers UIT het gesprek, LOS van
website intelligence (dat gaat over hun site en is er al vóór het gesprek):
    {
      "unanswered_inbound_per_week": 14,   # gemiste inbound/week
      "response_time_hours": 11,           # gemiddelde reactietijd (uur)
      "value_per_new_patient": 800,        # waarde van een nieuwe patiënt (EUR)
      "conversion_estimate": 0.25,         # inschatting conversie lead→patiënt
      "no_shows_per_month": 6,             # no-shows/maand
      "hours_reception_per_week": 5,       # receptie-uren/week aan telefoon/mail
      "reviews_per_month": 2,              # nieuwe reviews/maand
      "treatments_per_month": 80,          # behandelingen/maand
      "source": "call",                    # herkomst van de cijfers
      "captured_at": "2026-07-15"          # datum vastgelegd
    }
Alle velden optioneel; leeg checkup_data → geen rapport (report_status='skipped').
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Toegestane uitkomsten (heatr_call_records.outcome).
VALID_OUTCOMES = ("won", "timing", "no_value", "stalled", "hard_no")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def create_call_record(
    supabase_client: Any,
    workspace_id: str,
    *,
    transcript: str,
    call_date: str,
    participants: list | None = None,
    duration_minutes: int | None = None,
    zoom_meeting_id: str | None = None,
    transcript_source: str = "manual",
    lead_id: str | None = None,
    match_status: str = "unmatched",
) -> dict | None:
    """Maak een gesprekrecord aan (handmatig transcript of via Zoom).

    Args:
        supabase_client: heatr_-prefix Supabase-wrapper.
        workspace_id: workspace-slug — altijd gefilterd.
        transcript: het gesprekstranscript (verplicht, NOT NULL).
        call_date: ISO-timestamp van het gesprek.
        participants: optionele lijst deelnemers (jsonb).
        duration_minutes / zoom_meeting_id / transcript_source: bron-metadata.
        lead_id + match_status: koppeling (default unmatched — fail-closed).

    Returns:
        De aangemaakte rij, of None bij een insert-fout.
    """
    record = {
        "workspace_id": workspace_id,
        "transcript": transcript,
        "call_date": call_date,
        "participants": participants,
        "duration_minutes": duration_minutes,
        "zoom_meeting_id": zoom_meeting_id,
        "transcript_source": transcript_source,
        "lead_id": lead_id,
        "match_status": match_status,
    }
    try:
        res = supabase_client.table("call_records").insert(record).execute()
        return res.data[0] if res.data else None
    except Exception as e:
        logger.error("create_call_record faalde (ws=%s): %s", workspace_id, e)
        return None


async def get_call_record(supabase_client: Any, workspace_id: str, call_id: str) -> dict | None:
    """Haal één gesprekrecord op (workspace-scoped)."""
    try:
        res = (supabase_client.table("call_records").select("*")
               .eq("id", call_id).eq("workspace_id", workspace_id)
               .maybe_single().execute())
        return res.data or None
    except Exception as e:
        logger.error("get_call_record faalde (%s): %s", call_id, e)
        return None


async def list_call_records(
    supabase_client: Any, workspace_id: str, *,
    lead_id: str | None = None, report_status: str | None = None,
    outcome: str | None = None, match_status: str | None = None, limit: int = 50,
) -> list[dict]:
    """Lijst gesprekrecords met optionele filters, nieuwste eerst."""
    try:
        q = (supabase_client.table("call_records").select("*")
             .eq("workspace_id", workspace_id))
        if lead_id:
            q = q.eq("lead_id", lead_id)
        if report_status:
            q = q.eq("report_status", report_status)
        if outcome:
            q = q.eq("outcome", outcome)
        if match_status:
            q = q.eq("match_status", match_status)
        return q.order("call_date", desc=True).limit(limit).execute().data or []
    except Exception as e:
        logger.error("list_call_records faalde (ws=%s): %s", workspace_id, e)
        return []


async def list_unmatched(supabase_client: Any, workspace_id: str) -> list[dict]:
    """De fallback-lijst: gesprekken zonder gekoppelde lead (fail-closed matching)."""
    return await list_call_records(supabase_client, workspace_id, match_status="unmatched", limit=200)


async def match_call_record(
    supabase_client: Any, workspace_id: str, call_id: str, lead_id: str,
) -> dict | None:
    """Koppel een gesprek handmatig aan een lead (match_status='manually_matched').

    Handmatig, want automatische matching is fail-closed op precies één
    e-mailmatch; de rest komt hier terecht.
    """
    try:
        # Verifieer dat de lead in dezelfde workspace bestaat (nooit cross-tenant).
        lead = (supabase_client.table("leads").select("id")
                .eq("id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute())
        if not (lead.data or None):
            logger.warning("match_call_record: lead %s niet in ws %s", lead_id, workspace_id)
            return None
        res = (supabase_client.table("call_records").update({
            "lead_id": lead_id, "match_status": "manually_matched", "updated_at": _now(),
        }).eq("id", call_id).eq("workspace_id", workspace_id).execute())
        return res.data[0] if res.data else None
    except Exception as e:
        logger.error("match_call_record faalde (%s→%s): %s", call_id, lead_id, e)
        return None


async def set_outcome(
    supabase_client: Any, workspace_id: str, call_id: str, outcome: str, *,
    outcome_note: str | None = None, timing_target_date: str | None = None,
    checkup_data: dict | None = None,
) -> dict | None:
    """Gate 1 — de operator kiest de uitkomst en vult (optioneel) de cijfers.

    Zet outcome/outcome_note/outcome_set_at/timing_target_date op het record, en
    schrijft checkup_data + last_call_at + last_call_outcome naar de gekoppelde
    lead. Bepaalt de vervolgroute:
      - won     → crm_stage='gewonnen', geen rapport, geen cadans.
      - hard_no → crm_stage='verloren', geen rapport, geen cadans,
                  retarget_status='exhausted'.
      - overig  → rapport + cadans volgen (in latere fasen).

    Returns:
        De bijgewerkte call-record-rij, of None bij ongeldige uitkomst / fout.
    """
    if outcome not in VALID_OUTCOMES:
        logger.warning("set_outcome: ongeldige uitkomst %r", outcome)
        return None
    try:
        patch: dict[str, Any] = {
            "outcome": outcome,
            "outcome_note": outcome_note,
            "outcome_set_at": _now(),
            "updated_at": _now(),
        }
        if timing_target_date:
            patch["timing_target_date"] = timing_target_date
        if outcome == "hard_no":
            patch["retarget_status"] = "exhausted"

        res = (supabase_client.table("call_records").update(patch)
               .eq("id", call_id).eq("workspace_id", workspace_id).execute())
        record = res.data[0] if res.data else None
        if not record:
            return None

        # Lead bijwerken: checkup_data + call-metadata + crm_stage bij terminale uitkomst.
        lead_id = record.get("lead_id")
        if lead_id:
            lead_patch: dict[str, Any] = {
                "last_call_at": record.get("call_date") or _now(),
                "last_call_outcome": outcome,
            }
            if checkup_data:
                lead_patch["checkup_data"] = checkup_data
            if outcome == "won":
                lead_patch["crm_stage"] = "gewonnen"
            elif outcome == "hard_no":
                lead_patch["crm_stage"] = "verloren"
            try:
                (supabase_client.table("leads").update(lead_patch)
                 .eq("id", lead_id).eq("workspace_id", workspace_id).execute())
            except Exception as e:
                logger.error("set_outcome: lead-update faalde (%s): %s", lead_id, e)

        logger.info("set_outcome: call=%s outcome=%s lead=%s", call_id, outcome, lead_id)
        return record
    except Exception as e:
        logger.error("set_outcome faalde (%s): %s", call_id, e)
        return None
