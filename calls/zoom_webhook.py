"""
calls/zoom_webhook.py — Zoom-opname -> gesprekrecord (GEBOUWD MAAR UIT).

Uit tot ZOOM_WEBHOOK_SECRET gezet is (zoom_enabled() == False -> de endpoint
geeft 404). Zet 'm pas aan als de Zoom-app geregistreerd is; zie het Zoom-plan.

Fail-closed op twee assen:
  1. HMAC: elk event wordt geverifieerd tegen ZOOM_WEBHOOK_SECRET; mismatch ->
     geweigerd (geen "valid-looking"-tolerantie).
  2. Matching: exact één e-mailmatch (na filtering van AERYS_OWN_EMAILS) of
     'unmatched'. Nooit fuzzy op naam — een verkeerd gekoppeld transcript zijn
     andermans cijfers naar de verkeerde kliniek.

Zoom levert transcript + deelnemers + datum + duur. De check-up-cijfers blijven
mensenwerk (gate 1) — Zoom automatiseert de uitkomst NIET.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")


def zoom_enabled() -> bool:
    """De hele webhook staat uit tot het secret gezet is."""
    return bool(os.getenv("ZOOM_WEBHOOK_SECRET"))


def _secret() -> bytes:
    return (os.getenv("ZOOM_WEBHOOK_SECRET") or "").encode()


def verify_signature(timestamp: str, raw_body: bytes, signature: str) -> bool:
    """Verifieer x-zm-signature: v0=HMAC-SHA256(secret, 'v0:'+ts+':'+body).

    Fail-closed: lege timestamp/signature of mismatch -> False.
    """
    if not timestamp or not signature:
        return False
    message = b"v0:" + timestamp.encode() + b":" + raw_body
    digest = hmac.new(_secret(), message, hashlib.sha256).hexdigest()
    expected = f"v0={digest}"
    return hmac.compare_digest(expected, signature)


def url_validation_response(plain_token: str) -> dict:
    """Antwoord op Zoom's endpoint.url_validation-handshake."""
    encrypted = hmac.new(_secret(), plain_token.encode(), hashlib.sha256).hexdigest()
    return {"plainToken": plain_token, "encryptedToken": encrypted}


def parse_vtt(vtt: str) -> str:
    """Zet een WEBVTT-transcript om naar platte tekst.

    Gooit de WEBVTT-header, cue-nummers en tijdstempelregels (--> ) weg; houdt de
    gesproken regels over. Dedupliceert opeenvolgende lege regels.
    """
    out: list[str] = []
    for line in (vtt or "").splitlines():
        s = line.strip()
        if not s or s == "WEBVTT" or s.startswith("NOTE"):
            continue
        if "-->" in s:
            continue
        if s.isdigit():
            continue
        out.append(s)
    return "\n".join(out).strip()


def _own_emails() -> set[str]:
    raw = os.getenv("AERYS_OWN_EMAILS", "")
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


def extract_participant_emails(obj: dict) -> list[str]:
    """Verzamel deelnemer-e-mails uit het (recursief doorzochte) event-object.

    Best-effort: Zoom's recording-payload bevat host_email en soms een
    participants-lijst; niet altijd alle deelnemers (dat vergt een aparte
    Zoom-API-call, zie Zoom-plan). Eigen adressen worden er hier al uit gefilterd.
    """
    found: set[str] = set()

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for v in node:
                _walk(v)
        elif isinstance(node, str):
            for m in _EMAIL_RE.findall(node):
                found.add(m.lower())

    _walk(obj)
    own = _own_emails()
    return sorted(found - own)


def match_lead_by_emails(supabase_client: Any, workspace_id: str, emails: list[str]) -> tuple[str | None, str]:
    """Fail-closed matching: exact één lead over alle kandidaat-e-mails, of unmatched.

    Returns (lead_id, match_status). match_status in {matched, unmatched}.
    """
    if not emails:
        return None, "unmatched"
    try:
        res = (supabase_client.table("leads").select("id, email")
               .eq("workspace_id", workspace_id).in_("email", emails).execute())
        ids = {r["id"] for r in (res.data or []) if r.get("id")}
    except Exception as e:
        logger.error("match_lead_by_emails faalde (ws=%s): %s", workspace_id, e)
        return None, "unmatched"
    if len(ids) == 1:
        return next(iter(ids)), "matched"
    return None, "unmatched"


def _find_transcript_file(obj: dict) -> dict | None:
    """Vind het transcript-bestand (VTT) in recording_files."""
    for f in (obj.get("recording_files") or []):
        if not isinstance(f, dict):
            continue
        ft = (f.get("file_type") or "").upper()
        ext = (f.get("file_extension") or "").upper()
        if ft in ("TRANSCRIPT", "CC") or ext == "VTT":
            return f
    return None


async def _get_by_meeting_id(supabase_client: Any, workspace_id: str, meeting_id: str) -> dict | None:
    try:
        res = (supabase_client.table("call_records").select("id")
               .eq("workspace_id", workspace_id).eq("zoom_meeting_id", meeting_id)
               .maybe_single().execute())
        return res.data or None
    except Exception:
        return None


async def process_recording(payload: dict, workspace_id: str) -> dict:
    """Verwerk een Zoom recording/transcript-event tot een gesprekrecord.

    Never-raise (draait als background-task): retourneert een status-dict.
    Idempotent op zoom_meeting_id (UNIQUE + pre-check).
    """
    try:
        import httpx
        from config.database import get_heatr_supabase
        from calls.call_records import create_call_record

        obj = payload.get("object") or {}
        meeting_id = str(obj.get("uuid") or obj.get("id") or "").strip()
        if not meeting_id:
            return {"status": "skipped", "reason": "no_meeting_id"}

        sb = get_heatr_supabase()
        if await _get_by_meeting_id(sb, workspace_id, meeting_id):
            return {"status": "duplicate", "meeting_id": meeting_id}

        tfile = _find_transcript_file(obj)
        if not tfile or not tfile.get("download_url"):
            return {"status": "skipped", "reason": "no_transcript_file"}

        download_token = payload.get("download_token") or obj.get("download_token")
        headers = {"Authorization": f"Bearer {download_token}"} if download_token else {}
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.get(tfile["download_url"], headers=headers)
                r.raise_for_status()
                transcript = parse_vtt(r.text)
        except Exception as e:  # noqa: BLE001
            logger.error("Zoom VTT-download faalde (%s): %s", meeting_id, e)
            return {"status": "error", "reason": f"vtt_download: {str(e)[:80]}"}

        if not transcript:
            return {"status": "skipped", "reason": "empty_transcript"}

        emails = extract_participant_emails(obj)
        lead_id, match_status = match_lead_by_emails(sb, workspace_id, emails)

        rec = await create_call_record(
            sb, workspace_id,
            transcript=transcript,
            call_date=obj.get("start_time") or obj.get("recording_start") or "",
            participants=[{"email": e} for e in emails] or None,
            duration_minutes=obj.get("duration"),
            zoom_meeting_id=meeting_id,
            transcript_source="zoom",
            lead_id=lead_id,
            match_status=match_status,
        )
        if not rec:
            return {"status": "error", "reason": "insert_failed"}
        logger.info("Zoom-ingest: call %s (%s) meeting=%s", rec.get("id"), match_status, meeting_id)
        return {"status": "created", "call_id": rec.get("id"), "match_status": match_status}
    except Exception as e:  # noqa: BLE001 - never-raise in background
        logger.error("process_recording onverwacht gefaald: %s", e)
        return {"status": "error", "reason": f"unexpected: {str(e)[:80]}"}
