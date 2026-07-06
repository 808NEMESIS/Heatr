"""
utils/outbound_dispatcher.py — de verplichte doorgang voor outbound
side-effects (Sprint 2 Control Plane, invarianten I3 + I6 + I7).

Waarom één doorgang: de Sprint 1-audit vond 9 egress-paden met per-pad
geregelde gates. De compliance-gate is sindsdien gedeeld (I1), maar
idempotency, event-logging en reproduceerbaarheid waren per pad — of
ontbraken. De dispatcher is het ene punt waar:

  1. compliance als LAATSTE vangnet wordt afgedwongen (I1, defense in
     depth — callers gaten al, de dispatcher garandeert);
  2. de idempotency-key wordt afgedwongen (I6) — één keuzepunt i.p.v. 9;
  3. elke poging append-only wordt weggeschreven in heatr_outbound_log,
     óók geblokkeerde en geskipte pogingen (I7-fundament).

Key-conventies per pad (deterministisch uit de send-intentie):
  - send-to-warmr bulk:  warmr-bulk:{campaign_id}:{sha1(sorted lead_ids)}
  - review-email:        review-email:{lead_id}
  - campaign create:     campaign-create:{sha1(name+template+lead_ids)}
  - campaign bulk push:  campaign-push:{campaign_id}:{sha1(sorted lead_ids)}
  - sequence dispatch:   seq-send:{record_id}:step:{step_index}:epoch:{restart_epoch}
    (restart_epoch bumpt bij een bewuste operator-restart → nieuwe key →
    herzendbaar; accidentele dubbele dispatch = zelfde key = geblokkeerd)
  - operator-email:      briefing:{workspace}:{date} / alert:record-only

Degradatie-gedrag: als heatr_outbound_log nog niet bestaat (migratie 020
niet gedraaid) faalt de dispatcher OPEN met een luide error-log —
compliance blijft afgedwongen (in-memory), idempotency en ledger niet.
Bewuste keuze: pre-migratie alle sends bricken is erger dan tijdelijk
zonder dedup draaien; de log maakt de degradatie zichtbaar.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from utils.enrichment_check import compliance_check

logger = logging.getLogger(__name__)

VALID_KINDS = ("warmr_push", "warmr_bulk_push", "warmr_campaign_create", "operator_email")


class DispatchBlocked(Exception):
    """Side-effect geweigerd door de dispatcher (compliance). Bevat de reden."""


def ids_hash(lead_ids: list[str]) -> str:
    """Deterministische korte hash over een lead-selectie voor key-bouw."""
    return hashlib.sha1(",".join(sorted(lead_ids)).encode()).hexdigest()[:16]


@dataclass
class DispatchResult:
    executed: bool
    skipped_duplicate: bool = False
    result: Any = None
    previous: dict | None = None  # het eerdere completed-record bij een skip
    record_ids: list[str] = field(default_factory=list)


def _append_record(
    supabase_client: Any,
    *,
    workspace_id: str,
    idempotency_key: str,
    kind: str,
    status: str,
    actor: str,
    lead_id: str | None,
    lead_ids: list[str] | None,
    result: Any = None,
    error: str | None = None,
    metadata: dict | None = None,
) -> str | None:
    """Append-only write naar heatr_outbound_log. Faalt zacht met luide log."""
    row = {
        "workspace_id": workspace_id,
        "idempotency_key": idempotency_key,
        "kind": kind,
        "status": status,
        "actor": actor,
        "lead_id": lead_id,
        "lead_ids": lead_ids,
        "error": error[:2000] if error else None,
        "metadata": metadata or {},
    }
    if result is not None:
        try:
            row["result"] = json.loads(json.dumps(result, default=str))
        except (TypeError, ValueError):
            row["result"] = {"repr": repr(result)[:500]}
    try:
        res = supabase_client.table("outbound_log").insert(row).execute()
        return (res.data or [{}])[0].get("id")
    except Exception as e:
        logger.error(
            "outbound_dispatcher: LEDGER-WRITE MISLUKT (key=%s status=%s): %s — "
            "is migratie 020 gedraaid? Ledger/idempotency zijn NIET actief.",
            idempotency_key, status, e,
        )
        return None


def _find_completed(supabase_client: Any, workspace_id: str, idempotency_key: str) -> dict | None:
    """Zoek een eerder completed record voor deze key. None bij tabel-afwezig."""
    try:
        res = (
            supabase_client.table("outbound_log")
            .select("id, status, result, created_at, actor")
            .eq("workspace_id", workspace_id)
            .eq("idempotency_key", idempotency_key)
            .eq("status", "completed")
            .limit(1)
            .execute()
        )
        rows = res.data or []
        return rows[0] if rows else None
    except Exception as e:
        logger.error(
            "outbound_dispatcher: IDEMPOTENCY-CHECK MISLUKT (key=%s): %s — "
            "fail-open, dedup NIET afgedwongen. Is migratie 020 gedraaid?",
            idempotency_key, e,
        )
        return None


async def dispatch_outbound(
    *,
    kind: str,
    idempotency_key: str,
    actor: str,
    send: Callable[[], Awaitable[Any]],
    supabase_client: Any,
    workspace_id: str,
    lead: dict | None = None,
    leads: list[dict] | None = None,
    enforce_idempotency: bool = True,
    metadata: dict | None = None,
) -> DispatchResult:
    """Voer een outbound side-effect uit via de verplichte doorgang.

    Args:
        kind: één van VALID_KINDS.
        idempotency_key: deterministische key uit de send-intentie (zie
            module-docstring voor conventies).
        actor: wie triggert (user:…, service:…, scheduler:…) — audit-spoor.
        send: async callable zonder args die de daadwerkelijke side-effect
            uitvoert. Wordt ALLEEN aangeroepen als compliance + idempotency
            groen zijn.
        lead / leads: compliance-target(s). Prospect-gerichte kinds MOETEN
            er één leveren; operator_email mag zonder.
        enforce_idempotency: False = record-only (gebruikt door alerts,
            waar suppressie gevaarlijker is dan een dubbele melding).

    Raises:
        DispatchBlocked: compliance-fail op (één van) de target(s). Het
            geblokkeerde record staat dan al in de ledger.
        ValueError: onbekende kind of ontbrekende compliance-target.
        Exception: de send-exceptie zelf, ná het failed-record.
    """
    if kind not in VALID_KINDS:
        raise ValueError(f"Onbekende dispatch-kind: {kind!r} (geldig: {VALID_KINDS})")

    targets = leads if leads is not None else ([lead] if lead is not None else [])
    if kind != "operator_email" and not targets:
        raise ValueError(
            f"dispatch_outbound(kind={kind!r}) vereist lead of leads — "
            "prospect-gerichte sends zonder compliance-target zijn verboden (I1)."
        )

    lead_id = (lead or {}).get("id") if lead else None
    lead_ids = [l.get("id") for l in (leads or []) if l.get("id")] or None

    # 1. Compliance — laatste vangnet. Hoort nooit te triggeren (callers
    #    gaten al); als het triggert is er een gat en is dít de muur.
    for target in targets:
        compliant, reason = compliance_check(target)
        if not compliant:
            detail = f"lead={target.get('id')}: {reason}"
            _append_record(
                supabase_client,
                workspace_id=workspace_id, idempotency_key=idempotency_key,
                kind=kind, status="blocked_compliance", actor=actor,
                lead_id=lead_id, lead_ids=lead_ids, error=detail, metadata=metadata,
            )
            logger.error(
                "outbound_dispatcher: COMPLIANCE-BLOCK op dispatcher-niveau "
                "(key=%s, %s) — caller-gate heeft een gat!", idempotency_key, detail,
            )
            raise DispatchBlocked(detail)

    # 2. Idempotency — één keuzepunt (I6).
    if enforce_idempotency:
        previous = _find_completed(supabase_client, workspace_id, idempotency_key)
        if previous:
            rec = _append_record(
                supabase_client,
                workspace_id=workspace_id, idempotency_key=idempotency_key,
                kind=kind, status="skipped_duplicate", actor=actor,
                lead_id=lead_id, lead_ids=lead_ids,
                metadata={**(metadata or {}), "previous_record": previous.get("id"),
                          "previous_at": previous.get("created_at")},
            )
            logger.info(
                "outbound_dispatcher: duplicate geskipt (key=%s, eerder: %s door %s)",
                idempotency_key, previous.get("created_at"), previous.get("actor"),
            )
            return DispatchResult(
                executed=False, skipped_duplicate=True,
                previous=previous, record_ids=[r for r in [rec] if r],
            )

    # 3. Uitvoeren + append-only vastleggen (I7-fundament).
    try:
        result = await send()
    except Exception as e:
        _append_record(
            supabase_client,
            workspace_id=workspace_id, idempotency_key=idempotency_key,
            kind=kind, status="failed", actor=actor,
            lead_id=lead_id, lead_ids=lead_ids,
            error=f"{type(e).__name__}: {e}", metadata=metadata,
        )
        raise

    rec = _append_record(
        supabase_client,
        workspace_id=workspace_id, idempotency_key=idempotency_key,
        kind=kind, status="completed", actor=actor,
        lead_id=lead_id, lead_ids=lead_ids, result=result, metadata=metadata,
    )
    return DispatchResult(executed=True, result=result, record_ids=[r for r in [rec] if r])
