"""
utils/launch_readiness.py — per-lead launch readiness-check.

Capability 1 uit de Capability Sprint (2026-07-05): geen black-box launch
meer. Eén functie die ALLE bestaande gates composeert tot een uitlegbaar
verdict: ready / blocked / needs_review, met concrete redenen per check.

Composeert bestaande bronnen van waarheid (bouwt geen nieuwe regels):
  - compliance_check        (utils/enrichment_check — GDPR + status, absoluut)
  - check_lead_completeness (utils/enrichment_check — hard/soft velden)
  - is_sendable             (utils/email_sendability — email-status gate)
  - MIN_SCORE_FOR_WARMR + MIN_ICP_MATCH_FOR_WARMR (env, zelfde defaults
    als scoring/lead_scoring.py)
  - cooldown/snooze-velden  (next_contact_after, snoozed_until — zelfde
    semantiek als utils/sending_guard.py)

Severities:
  block  → lead mag NIET gelanceerd (verdict "blocked")
  review → operator moet kijken (verdict "needs_review")
  ok     → check groen

Test-lead beleid (consistent met bestaande gates): is_test_lead bypasst
completeness- en score-checks, maar NOOIT compliance en nooit de
email-aanwezigheidscheck.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

from utils.email_sendability import is_sendable
from utils.enrichment_check import check_lead_completeness, compliance_check

_REVIEW_EMAIL_STATUSES = ("risky", "catch_all", "catchall", "catchall_risky")


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def assess_launch_readiness(lead: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    """Beoordeel één lead op launch-gereedheid.

    Returns:
        {
            "lead_id": str,
            "verdict": "ready" | "blocked" | "needs_review",
            "is_test_lead": bool,
            "checks": [ {key, ok, severity, detail}, ... ],
            "blockers": [detail, ...],   # samenvatting van block-checks
            "reviews": [detail, ...],    # samenvatting van review-checks
        }
    """
    now = now or datetime.now(timezone.utc)
    is_test = bool(lead.get("is_test_lead"))
    checks: list[dict[str, Any]] = []

    def add(key: str, ok: bool, severity: str, detail: str) -> None:
        checks.append({"key": key, "ok": ok, "severity": severity if not ok else "ok", "detail": detail})

    # 1. Compliance (GDPR + status) — absoluut, geen test-bypass
    compliant, comp_reason = compliance_check(lead)
    add("compliance", compliant, "block",
        "GDPR-safe en status toegestaan" if compliant else comp_reason or "compliance-fail")

    # 2. Email aanwezig + sendable — test-lead bypasst status-check binnen
    #    is_sendable, maar een lead zonder email blijft geblokkeerd
    sendable, send_reason = is_sendable(
        lead.get("email"), lead.get("email_status"), is_test_lead=is_test,
        verification_method=lead.get("email_verification_method"),
    )
    add("email", sendable, "block",
        f"Email sendable ({send_reason})" if sendable else f"Email niet sendable: {send_reason}")
    if sendable and (lead.get("email_status") or "") in _REVIEW_EMAIL_STATUSES:
        add("email_risk", False, "review",
            f"Email-status is {lead.get('email_status')!r} — hoger bounce-risico, check handmatig")

    # 3. Enrichment-completeness — hard-required = block (test-lead bypasst)
    completeness = check_lead_completeness(lead)
    if completeness["missing_required"]:
        ok = is_test  # test-lead: zichtbaar maar niet blocking
        add("completeness", ok, "block",
            f"Mist verplichte velden: {', '.join(completeness['missing_required'])}"
            + (" (test-lead bypass)" if is_test else ""))
    else:
        add("completeness", True, "block", "Alle verplichte enrichment-velden gevuld")
    if completeness["missing_recommended"]:
        add("personalisation", False, "review",
            f"Mist personalisatie-data: {', '.join(completeness['missing_recommended'])}")

    # 4. Score-drempels — zelfde envs + defaults als scoring/lead_scoring.py
    min_score = int(os.getenv("MIN_SCORE_FOR_WARMR", 65))
    min_icp = float(os.getenv("MIN_ICP_MATCH_FOR_WARMR", 0.6))
    score = lead.get("score") or 0
    icp = lead.get("icp_match") or 0.0
    score_ok = (score >= min_score and icp >= min_icp) or is_test
    add("score", score_ok, "block",
        f"score={score} (min {min_score}), icp_match={icp} (min {min_icp})"
        + (" — test-lead bypass" if is_test and not (score >= min_score and icp >= min_icp) else ""))

    # 5. Website-analyse aanwezig — review, niet blocking (sequence werkt
    #    zonder, maar personalisatie is dan zwakker)
    if lead.get("website_score") is None:
        add("website_analysis", False, "review",
            "Geen website-analyse — opener/observaties missen website-signalen")
    else:
        add("website_analysis", True, "review", f"Website-analyse aanwezig (score {lead.get('website_score')})")

    # 6. Eerder benaderd — review (operator beslist over her-benadering)
    pushed_at = lead.get("pushed_to_warmr_at")
    attempts = lead.get("contact_attempt_count") or 0
    if pushed_at or attempts > 0:
        add("previously_contacted", False, "review",
            f"Al eerder benaderd (pushed_at={pushed_at or '—'}, pogingen={attempts})")
    else:
        add("previously_contacted", True, "review", "Nog niet eerder benaderd")

    # 7. Cooldown/snooze actief — block (zelfde semantiek als sending_guard:
    #    een bewuste snooze of recontact-cooldown is een harde afspraak)
    cooldown_until = None
    for field in ("next_contact_after", "snoozed_until"):
        ts = _parse_ts(lead.get(field))
        if ts and ts > now and (cooldown_until is None or ts > cooldown_until):
            cooldown_until = ts
    if cooldown_until:
        add("cooldown", False, "block", f"Cooldown/snooze actief tot {cooldown_until.date().isoformat()}")
    else:
        add("cooldown", True, "block", "Geen cooldown actief")

    blockers = [c["detail"] for c in checks if not c["ok"] and c["severity"] == "block"]
    reviews = [c["detail"] for c in checks if not c["ok"] and c["severity"] == "review"]
    verdict = "blocked" if blockers else ("needs_review" if reviews else "ready")

    return {
        "lead_id": lead.get("id"),
        "verdict": verdict,
        "is_test_lead": is_test,
        "checks": checks,
        "blockers": blockers,
        "reviews": reviews,
    }
