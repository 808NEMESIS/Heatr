"""
utils/outbound_dispatcher.py — de verplichte doorgang voor outbound
side-effects (Sprint 2 Control Plane, invarianten I3 + I6 + I7;
aangescherpt in Werkpakket A "Outbound Safety Foundation", 2026-07-10).

Waarom één doorgang: de Sprint 1-audit vond 9 egress-paden met per-pad
geregelde gates. De compliance-gate is sindsdien gedeeld (I1), maar
idempotency, event-logging en reproduceerbaarheid waren per pad — of
ontbraken. De dispatcher is het ene punt waar:

  1. compliance als LAATSTE vangnet wordt afgedwongen (I1, defense in
     depth — callers gaten al, de dispatcher garandeert);
  2. de idempotency-key wordt afgedwongen (I6) via een ATOMISCHE
     in_flight-reservering: de INSERT op heatr_outbound_log is de
     eigenaarstoewijzing, gedekt door de partial-UNIQUE-index
     uq_outbound_log_active_key (migratie 022). Wie de insert wint mag
     versturen; wie een unique-conflict krijgt, verstuurt NIET;
  3. elke poging in heatr_outbound_log belandt, óók geblokkeerde en
     geskipte pogingen (I7-fundament). De reserveringsrij wordt in-place
     gefinaliseerd (in_flight → completed | failed_retryable |
     failed_terminal); alle overige rijen zijn append-only.

Key-conventies per pad (deterministisch uit de send-intentie):
  - send-to-warmr bulk:  warmr-bulk:{campaign_id}:{sha1(sorted lead_ids)}
  - review-email:        review-email:{lead_id}
  - campaign create:     campaign-create:{sha1(name+template+lead_ids)}
  - campaign bulk push:  campaign-push:{campaign_id}:{sha1(sorted lead_ids)}
  - sequence dispatch:   seq-send:{record_id}:step:{step_index}:epoch:{restart_epoch}
    (restart_epoch bumpt bij een bewuste operator-restart → nieuwe key →
    herzendbaar; accidentele dubbele dispatch = zelfde key = geblokkeerd)
  - operator-email:      briefing:{workspace}:{date} / alert:record-only

Master-kill-switch (WP-A stap 7): élk prospect-kind wordt hier centraal
gegate't op ENABLE_PROSPECT_SENDS (fallback: de legacy
ENABLE_CAMPAIGN_SENDS, die vóór WP-A alleen /campaigns/launch dekte —
audit v2 P0-4). operator_email heeft een eigen switch
(ENABLE_INTERNAL_NOTIFICATIONS, default aan) zodat een campagne-stop
geen interne alerts dempt.

Degradatie-gedrag (Werkpakket A, Besluit 3 — GEWIJZIGD t.o.v. Sprint 2):
prospect-gerichte kinds falen CLOSED als de ledger onbeschikbaar is —
geen reservering = geen send (DispatchLedgerUnavailable). Alleen
operator_email faalt zacht (een gemiste interne melding is erger dan
een dubbele). De oude fail-open-keuze verborg precies de bug die hem
motiveerde: de ledger-writes faalden maandenlang stil door een
tabelnaam-prefix-fout, en elke send draaide zonder dedup.

Residueel risico (gedocumenteerd, buiten scope WP-A): een Warmr-timeout
ná acceptatie finaliseert de rij als failed_retryable terwijl de mail
mogelijk wél bezorgd wordt; een retry kan dan dubbel bezorgen. De
definitieve fix is een Idempotency-Key-header richting Warmr
(actieplan fase 3+). metadata.external_state_unknown markeert deze
gevallen voor de runbook-check.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from utils.enrichment_check import compliance_check
from utils.suppression import check_suppressed

logger = logging.getLogger(__name__)

VALID_KINDS = ("warmr_push", "warmr_bulk_push", "warmr_campaign_create", "operator_email")
# Kinds die een prospect kunnen bereiken → fail-closed + (commit 3) kill-switch.
PROSPECT_KINDS = ("warmr_push", "warmr_bulk_push", "warmr_campaign_create")
# Statussen die meetellen in de partial-UNIQUE (migratie 022): één actieve
# eigenaar per key. failed_* valt erbuiten zodat een retry opnieuw kan reserveren.
_ACTIVE_STATUSES = ("in_flight", "completed")


def _inflight_stale_minutes() -> int:
    """Na hoeveel minuten een in_flight-reservering als gecrasht geldt."""
    try:
        return max(1, int(os.getenv("OUTBOUND_INFLIGHT_STALE_MINUTES", "15")))
    except ValueError:
        return 15


def _prospect_sends_enabled() -> bool:
    """Centrale master-kill-switch voor prospect-gerichte sends (WP-A stap 7).

    ENABLE_PROSPECT_SENDS is leidend; ontbreekt die, dan geldt de legacy
    ENABLE_CAMPAIGN_SENDS (die vóór WP-A alleen /campaigns/launch gate'te —
    audit v2 P0-4). Fail-closed: default false, en elke niet-"true"-waarde
    (typefout incluis) blokkeert.
    """
    value = os.getenv("ENABLE_PROSPECT_SENDS")
    if value is None:
        value = os.getenv("ENABLE_CAMPAIGN_SENDS", "false")
    return value.strip().lower() == "true"


def _internal_notifications_enabled() -> bool:
    """Aparte switch voor operator_email (briefings/alerts): een campagne-stop
    mag kritieke interne meldingen niet dempen. Default aan."""
    return os.getenv("ENABLE_INTERNAL_NOTIFICATIONS", "true").strip().lower() == "true"


class DispatchBlocked(Exception):
    """Side-effect geweigerd door de dispatcher (compliance). Bevat de reden."""


class DispatchHalted(DispatchBlocked):
    """Side-effect geweigerd door de master-kill-switch (geen compliance-fout).

    Subclass van DispatchBlocked zodat bestaande callers de block-afhandeling
    hergebruiken: geen send, nette reden, geen kale 500.
    """


class DispatchLedgerUnavailable(DispatchBlocked):
    """Ledger onbeschikbaar bij een prospect-send → fail-closed (Besluit 3).

    Subclass van DispatchBlocked zodat bestaande callers (sequence_engine,
    endpoints) de block-afhandeling hergebruiken: geen send, wel een nette
    reden i.p.v. een kale 500.
    """


def ids_hash(lead_ids: list[str]) -> str:
    """Deterministische korte hash over een lead-selectie voor key-bouw."""
    return hashlib.sha1(",".join(sorted(lead_ids)).encode()).hexdigest()[:16]


@dataclass
class DispatchResult:
    executed: bool
    skipped_duplicate: bool = False
    result: Any = None
    previous: dict | None = None  # het eerdere actieve record bij een skip
    record_ids: list[str] = field(default_factory=list)


def _log_decision(*, decision: str, kind: str, idempotency_key: str,
                  workspace_id: str, actor: str, lead_id: str | None,
                  lead_count: int, reason: str | None = None) -> None:
    """Structured decision-log (actieplan §4.2): key=value, geen mailinhoud/PII.

    Dit is de metrics-bron voor de runbook-queries (outbound_dispatch_total
    per decision) zolang er geen Prometheus is.
    """
    logger.info(
        "outbound_dispatch decision=%s kind=%s key=%s workspace_id=%s actor=%s "
        "lead_id=%s lead_count=%d reason=%s",
        decision, kind, idempotency_key, workspace_id, actor,
        lead_id or "-", lead_count, (reason or "-")[:300],
    )


def _serialize_result(result: Any) -> Any:
    try:
        return json.loads(json.dumps(result, default=str))
    except (TypeError, ValueError):
        return {"repr": repr(result)[:500]}


def _is_unique_violation(exc: Exception) -> bool:
    """Postgres 23505 via PostgREST/supabase-py — meerdere verschijningsvormen."""
    if getattr(exc, "code", None) == "23505":
        return True
    text = str(exc)
    return "23505" in text or "duplicate key" in text.lower()


def _classify_failure(exc: Exception) -> str:
    """failed_terminal bij een definitieve 4xx (fout in de request zelf);
    failed_retryable bij netwerk/timeout/5xx/onbekend. 408/429 zijn retrybaar."""
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int) and 400 <= status_code < 500 and status_code not in (408, 429):
        return "failed_terminal"
    return "failed_retryable"


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
    """Append-only write van een informatieve rij (blocked_*, skipped_*,
    record-only completed). Faalt zacht met luide log — deze rijen zijn
    audit-informatie, geen eigenaarstoewijzing; de reservering (fail-closed
    voor prospect-kinds) loopt via _reserve()."""
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
        row["result"] = _serialize_result(result)
    try:
        res = supabase_client.table("outbound_log").insert(row).execute()
        return (res.data or [{}])[0].get("id")
    except Exception as e:
        logger.error(
            "outbound_dispatcher: LEDGER-WRITE MISLUKT (key=%s status=%s): %s — "
            "audit-rij verloren; controleer migratie 020/022 en de "
            "outbound_log-prefixregistratie in config/database.py.",
            idempotency_key, status, e,
        )
        return None


def _find_active(supabase_client: Any, workspace_id: str, idempotency_key: str) -> dict | None:
    """Zoek het actieve record (in_flight/completed) voor deze key.

    Raises bij een ledger-leesfout — de caller beslist fail-closed/soft.
    """
    res = (
        supabase_client.table("outbound_log")
        .select("id, status, result, created_at, actor")
        .eq("workspace_id", workspace_id)
        .eq("idempotency_key", idempotency_key)
        .in_("status", list(_ACTIVE_STATUSES))
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


def _is_stale_inflight(record: dict) -> bool:
    """True als een in_flight-reservering ouder is dan de stale-TTL (worker
    gecrasht tussen reservering en finalisatie)."""
    if record.get("status") != "in_flight":
        return False
    raw = record.get("created_at") or ""
    try:
        created = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - created > timedelta(minutes=_inflight_stale_minutes())


def _reserve(
    supabase_client: Any,
    *,
    workspace_id: str,
    idempotency_key: str,
    kind: str,
    actor: str,
    lead_id: str | None,
    lead_ids: list[str] | None,
    metadata: dict | None,
) -> tuple[str | None, dict | None]:
    """Atomische eigenaarstoewijzing: INSERT in_flight, gedekt door de
    partial-UNIQUE (migratie 022).

    Returns:
        (reservation_id, None)  — wij zijn eigenaar, verstuur.
        (None, existing_row)    — een ander actief record bestaat, verstuur NIET.

    Raises:
        Exception — ledger onbeschikbaar (géén unique-conflict); de caller
        beslist fail-closed (prospect) of fail-soft (operator).
    """
    row = {
        "workspace_id": workspace_id,
        "idempotency_key": idempotency_key,
        "kind": kind,
        "status": "in_flight",
        "actor": actor,
        "lead_id": lead_id,
        "lead_ids": lead_ids,
        "metadata": metadata or {},
    }
    try:
        res = supabase_client.table("outbound_log").insert(row).execute()
        rec_id = (res.data or [{}])[0].get("id")
        if not rec_id:
            raise RuntimeError("reservering-insert gaf geen record-id terug")
        return rec_id, None
    except Exception as e:
        if not _is_unique_violation(e):
            raise
    # Unique-conflict: er bestaat al een actief record voor deze key.
    existing = _find_active(supabase_client, workspace_id, idempotency_key)
    return None, (existing or {})


def _finalize(
    supabase_client: Any,
    *,
    reservation_id: str,
    status: str,
    result: Any = None,
    error: str | None = None,
) -> bool:
    """Finaliseer de reserveringsrij (CAS op status='in_flight'). Faalt zacht
    met luide log: de send is dan al gebeurd — een raise zou de caller een
    vals 'niet verstuurd' geven."""
    update: dict[str, Any] = {"status": status}
    if result is not None:
        update["result"] = _serialize_result(result)
    if error:
        update["error"] = error[:2000]
    try:
        res = (
            supabase_client.table("outbound_log")
            .update(update)
            .eq("id", reservation_id)
            .eq("status", "in_flight")
            .execute()
        )
        if not (res.data or []):
            logger.error(
                "outbound_dispatcher: FINALISATIE MISTE de reserveringsrij "
                "(id=%s → %s) — rij was niet meer in_flight; onderzoek handmatig.",
                reservation_id, status,
            )
            return False
        return True
    except Exception as e:
        logger.error(
            "outbound_dispatcher: FINALISATIE MISLUKT (id=%s → %s): %s — de send "
            "is WEL uitgevoerd; de in_flight-rij blokkeert deze key tot de "
            "stale-TTL (%d min). Zie runbook outbound_safety.",
            reservation_id, status, e, _inflight_stale_minutes(),
        )
        return False


def _release_stale(supabase_client: Any, record: dict) -> bool:
    """Neem een gecrashte in_flight-reservering over: CAS → failed_retryable.
    Alleen de winnaar van deze CAS mag opnieuw reserveren."""
    try:
        res = (
            supabase_client.table("outbound_log")
            .update({"status": "failed_retryable",
                     "error": "stale in_flight-reservering overgenomen (worker-crash?)"})
            .eq("id", record.get("id"))
            .eq("status", "in_flight")
            .execute()
        )
        return bool(res.data or [])
    except Exception as e:
        logger.error(
            "outbound_dispatcher: stale-takeover mislukt (id=%s): %s",
            record.get("id"), e,
        )
        return False


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
            uitvoert. Wordt ALLEEN aangeroepen als compliance groen is én de
            in_flight-reservering gewonnen is.
        lead / leads: compliance-target(s). Prospect-gerichte kinds MOETEN
            er één leveren; operator_email mag zonder.
        enforce_idempotency: False = record-only (gebruikt door alerts,
            waar suppressie gevaarlijker is dan een dubbele melding).

    Raises:
        DispatchBlocked: compliance-fail op (één van) de target(s). Het
            geblokkeerde record staat dan al in de ledger.
        DispatchLedgerUnavailable: ledger onbereikbaar bij een prospect-kind
            → fail-closed, send NIET uitgevoerd (Besluit 3).
        ValueError: onbekende kind of ontbrekende compliance-target.
        Exception: de send-exceptie zelf, ná finalisatie van de reservering.
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
    lead_count = len(targets)

    def _decide(decision: str, reason: str | None = None) -> None:
        _log_decision(
            decision=decision, kind=kind, idempotency_key=idempotency_key,
            workspace_id=workspace_id, actor=actor, lead_id=lead_id,
            lead_count=lead_count, reason=reason,
        )

    # 0. Master-kill-switch — absoluut en centraal (WP-A stap 7): élk
    #    prospect-pad (launch, follow-up, ad-hoc push, review-mail) stopt
    #    hier, niet alleen /campaigns/launch. Interne meldingen hebben een
    #    eigen switch zodat een campagne-stop geen alerts dempt.
    if kind in PROSPECT_KINDS and not _prospect_sends_enabled():
        detail = ("prospect-sends staan uit "
                  "(ENABLE_PROSPECT_SENDS/ENABLE_CAMPAIGN_SENDS != true)")
        _append_record(
            supabase_client,
            workspace_id=workspace_id, idempotency_key=idempotency_key,
            kind=kind, status="blocked_killswitch", actor=actor,
            lead_id=lead_id, lead_ids=lead_ids, error=detail, metadata=metadata,
        )
        _decide("blocked_killswitch", detail)
        raise DispatchHalted(detail)
    if kind == "operator_email" and not _internal_notifications_enabled():
        detail = "interne meldingen staan uit (ENABLE_INTERNAL_NOTIFICATIONS=false)"
        _append_record(
            supabase_client,
            workspace_id=workspace_id, idempotency_key=idempotency_key,
            kind=kind, status="blocked_killswitch", actor=actor,
            lead_id=lead_id, lead_ids=lead_ids, error=detail, metadata=metadata,
        )
        _decide("blocked_killswitch", detail)
        raise DispatchHalted(detail)

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
            _decide("blocked_compliance", detail)
            logger.error(
                "outbound_dispatcher: COMPLIANCE-BLOCK op dispatcher-niveau "
                "(key=%s, %s) — caller-gate heeft een gat!", idempotency_key, detail,
            )
            raise DispatchBlocked(detail)

    # 1b. Platformbrede suppressie (fase 2 PR 7, audit v2 P0-2) — de
    #     cross-workspace lijst is de tweede linie bovenop de per-lead
    #     compliance_check. I/O-gate ná de pure checks, vóór de
    #     reservering. FAIL-CLOSED bij een leesfout (Besluit 3): een
    #     onbereikbare suppressielijst mag nooit stilletjes "niets
    #     gesuppressed" betekenen.
    if kind in PROSPECT_KINDS:
        target_emails = [t.get("email") for t in targets if t.get("email")]
        if target_emails:
            try:
                suppressed = check_suppressed(supabase_client, target_emails)
            except Exception as e:
                _decide("suppression_unavailable", str(e))
                logger.error(
                    "outbound_dispatcher: SUPPRESSIE-CHECK ONBESCHIKBAAR (key=%s): %s — "
                    "prospect-send FAIL-CLOSED geblokkeerd. Is migratie 024 gedraaid?",
                    idempotency_key, e,
                )
                raise DispatchLedgerUnavailable(
                    f"suppressie-check onbeschikbaar — prospect-send geblokkeerd: {e}"
                ) from e
            if suppressed:
                # Privacy-contract: alleen DAT het adres gesuppressed is —
                # nooit door welke tenant/campagne (utils/suppression.py).
                detail = (
                    f"{len(suppressed)} adres(sen) globally_suppressed: "
                    f"{', '.join(sorted(suppressed)[:5])}"
                )
                _append_record(
                    supabase_client,
                    workspace_id=workspace_id, idempotency_key=idempotency_key,
                    kind=kind, status="blocked_suppression", actor=actor,
                    lead_id=lead_id, lead_ids=lead_ids, error=detail, metadata=metadata,
                )
                _decide("blocked_suppression", detail)
                raise DispatchBlocked(detail)

    # 2. Idempotency — atomische in_flight-reservering (I6, migratie 022).
    reservation_id: str | None = None
    if enforce_idempotency:
        for attempt in (1, 2):  # 2e poging alleen na een stale-takeover
            try:
                reservation_id, existing = _reserve(
                    supabase_client,
                    workspace_id=workspace_id, idempotency_key=idempotency_key,
                    kind=kind, actor=actor, lead_id=lead_id, lead_ids=lead_ids,
                    metadata=metadata,
                )
            except Exception as e:
                if kind in PROSPECT_KINDS:
                    _decide("ledger_unavailable", str(e))
                    logger.error(
                        "outbound_dispatcher: LEDGER ONBESCHIKBAAR (key=%s): %s — "
                        "prospect-send FAIL-CLOSED geblokkeerd (Besluit 3). "
                        "Controleer migratie 020/022 + outbound_log-prefix.",
                        idempotency_key, e,
                    )
                    raise DispatchLedgerUnavailable(
                        f"ledger onbeschikbaar — prospect-send geblokkeerd: {e}"
                    ) from e
                # operator_email: fail-soft — melding gaat vóór dedup.
                _decide("ledger_unavailable_soft", str(e))
                logger.error(
                    "outbound_dispatcher: LEDGER ONBESCHIKBAAR (key=%s): %s — "
                    "operator_email gaat door zonder reservering (fail-soft).",
                    idempotency_key, e,
                )
                break

            if reservation_id:
                break  # wij zijn eigenaar

            # Conflict met een bestaand actief record.
            existing = existing or {}
            if attempt == 1 and _is_stale_inflight(existing):
                if _release_stale(supabase_client, existing):
                    continue  # takeover gewonnen → opnieuw reserveren
                # takeover verloren: een ander is bezig — behandel als skip
            status_label = existing.get("status") or "onbekend"
            rec = _append_record(
                supabase_client,
                workspace_id=workspace_id, idempotency_key=idempotency_key,
                kind=kind, status="skipped_duplicate", actor=actor,
                lead_id=lead_id, lead_ids=lead_ids,
                metadata={**(metadata or {}), "previous_record": existing.get("id"),
                          "previous_status": status_label,
                          "previous_at": existing.get("created_at")},
            )
            _decide("skipped_duplicate", f"previous_status={status_label}")
            logger.info(
                "outbound_dispatcher: duplicate geskipt (key=%s, eerder: %s status=%s door %s)",
                idempotency_key, existing.get("created_at"), status_label,
                existing.get("actor"),
            )
            return DispatchResult(
                executed=False, skipped_duplicate=True,
                previous=existing, record_ids=[r for r in [rec] if r],
            )

    # 3. Uitvoeren + reservering finaliseren (I7-fundament).
    try:
        result = await send()
    except Exception as e:
        failure_status = _classify_failure(e)
        error_text = f"{type(e).__name__}: {e}"
        fail_meta = dict(metadata or {})
        if failure_status == "failed_retryable" and getattr(e, "status_code", None) in (None, 0, 408):
            # Timeout/netwerk: de externe staat is onbekend — Warmr kan de
            # send WEL geaccepteerd hebben. Runbook-check vóór handmatige retry.
            fail_meta["external_state_unknown"] = True
            error_text += " [external_state_unknown]"
        if reservation_id:
            _finalize(supabase_client, reservation_id=reservation_id,
                      status=failure_status, error=error_text)
        else:
            _append_record(
                supabase_client,
                workspace_id=workspace_id, idempotency_key=idempotency_key,
                kind=kind, status=failure_status, actor=actor,
                lead_id=lead_id, lead_ids=lead_ids,
                error=error_text, metadata=fail_meta,
            )
        _decide(failure_status, error_text)
        raise

    if reservation_id:
        _finalize(supabase_client, reservation_id=reservation_id,
                  status="completed", result=result)
        record_ids = [reservation_id]
    else:
        rec = _append_record(
            supabase_client,
            workspace_id=workspace_id, idempotency_key=idempotency_key,
            kind=kind, status="completed", actor=actor,
            lead_id=lead_id, lead_ids=lead_ids, result=result, metadata=metadata,
        )
        record_ids = [r for r in [rec] if r]
    _decide("executed")
    return DispatchResult(executed=True, result=result, record_ids=record_ids)
