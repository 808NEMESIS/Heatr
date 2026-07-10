"""
utils/email_sendability.py — Single source of truth voor "is dit email sendable".

Heatr's email-verifier produceert deze status-waarden:
  - verified / valid     → 100% sendable
  - role_email           → sendable (info@/contact@/etc, niet per persoon)
  - catchall             → onzeker, mogelijk wel sendable maar bouncerisico
  - risky                → externe verifier-output, betekent typisch
                           "syntactisch ok maar SMTP-check inconclusief"
  - not_found / invalid  → NIET sendable
  - bounced              → NIET sendable (eerder gebounced)
  - unsubscribed         → NIET sendable (legaal verboden)
  - pending / null       → niet gecontroleerd, default niet sendable

Pad A (huidige flow): `risky` als sendable accepteren omdat 24% van leads
in deze bucket zit. Tracker via `allow_risky=True`.

Pad B: zet `allow_risky=False` zodra eigen SMTP-verifier alle `risky` leads
heeft geherclassificeerd naar verified/catchall/not_found.
"""
from __future__ import annotations

import os

# Statussen die ALTIJD sendable zijn (geen bounce-risico)
_ALWAYS_SENDABLE = {"verified", "valid"}

# Statussen die ALTIJD geweigerd worden (legaal of practical)
_NEVER_SENDABLE = {"not_found", "invalid", "bounced", "unsubscribed", "blocked"}

# Suppressie-subset: deze winnen van ELKE bypass, ook is_test_lead. Een
# unsubscribe/bounce is een belofte aan de ontvanger resp. een
# deliverability-feit — geen gate die een testvlag mag omzeilen
# (fase 2-hotfix, audit v2 P0-2: de test-lead-bypass negeerde suppressie).
_SUPPRESSED_STATUSES = {"bounced", "unsubscribed", "blocked"}

# Statussen die afhankelijk van flag zijn
_RISKY_STATUSES = {"risky", "catchall"}


def sendability_config() -> dict:
    """Publieke sendability-definitie voor UI-consumptie.

    Sprint 1-audit §8 vond dat de frontend een EIGEN kopie van deze lijsten
    had (RISKY_EMAIL_STATUSES in CampagneLaunch.tsx) die afweek van de
    server-definitie — ander risicobeeld voor de operator dan de gate
    hanteert. De frontend leest sindsdien deze config (GET
    /config/sendability); er bestaat geen tweede definitie meer (I5).
    """
    allow_risky = os.getenv("HEATR_ALLOW_RISKY_EMAILS", "true").lower() == "true"
    return {
        "always_sendable": sorted(_ALWAYS_SENDABLE),
        "never_sendable": sorted(_NEVER_SENDABLE),
        "risky_statuses": sorted(_RISKY_STATUSES),
        "allow_risky": allow_risky,
    }


def is_sendable(
    email: str | None,
    email_status: str | None,
    allow_risky: bool | None = None,
    allow_role_emails: bool = True,
    is_test_lead: bool = False,
) -> tuple[bool, str]:
    """Return (sendable, reason).

    Args:
        email: Email-adres (None/empty → niet sendable, ook bij test-mode)
        email_status: Status uit verifier
        allow_risky: True = accept 'risky'/'catchall' (Pad A — pragmatisch).
                    None = lees env-var HEATR_ALLOW_RISKY_EMAILS (default true).
        allow_role_emails: info@/contact@/etc als sendable (default true,
                          want voor MKB-zorg vaak enige optie).
        is_test_lead: True = gate-bypass voor verificatie-statussen (risky/
                     not_found/pending), NIET voor suppressie (bounced/
                     unsubscribed/blocked — die winnen altijd). Voor
                     smoke-test pipeline. Stuur via _build_lead_payload BCC
                     zodat het naar HEATR_TEST_BCC_EMAIL mirrort.

    Returns:
        (sendable: bool, reason: str)
    """
    if not email or "@" not in email:
        # Test-leads MOETEN nog steeds een geldig email-adres hebben.
        # Zonder email valt er niets te sturen, zelfs niet in test-mode.
        return False, "no_email"

    status = (email_status or "").strip().lower()

    # Suppressie wint van de test-bypass: een test-gemarkeerde lead die
    # unsubscribed/bounced is, blijft geblokkeerd (fase 2-hotfix).
    if status in _SUPPRESSED_STATUSES:
        return False, f"status:{status}"

    if is_test_lead:
        return True, "test_lead_bypass"

    if allow_risky is None:
        allow_risky = os.getenv("HEATR_ALLOW_RISKY_EMAILS", "true").lower() == "true"

    if status in _NEVER_SENDABLE:
        return False, f"status:{status}"

    if status in _ALWAYS_SENDABLE:
        return True, "verified"

    if status in _RISKY_STATUSES:
        if allow_risky:
            return True, f"risky_accepted:{status}"
        return False, f"risky_rejected:{status}"

    # Role-email detection (info@/contact@/etc) als status onbekend is
    role_prefixes = ("info@", "contact@", "hallo@", "praktijk@", "kliniek@",
                     "receptie@", "secretariaat@", "afspraak@", "support@")
    if email.lower().startswith(role_prefixes):
        if allow_role_emails:
            return True, "role_email"
        return False, "role_email_rejected"

    if not status or status in ("pending", "no_status_set"):
        return False, "not_verified"

    # Onbekende status → conservatief weigeren
    return False, f"unknown_status:{status}"


def filter_sendable_leads(
    leads: list[dict],
    allow_risky: bool | None = None,
) -> tuple[list[dict], list[dict]]:
    """Split leads in (sendable, unsendable) lijsten.

    Voor bulk-launch: gebruik dit om geweigerde leads apart te tonen aan operator.
    Honors per-lead `is_test_lead` flag voor gate-bypass.
    """
    sendable: list[dict] = []
    unsendable: list[dict] = []
    for lead in leads:
        ok, reason = is_sendable(
            lead.get("email"),
            lead.get("email_status"),
            allow_risky=allow_risky,
            is_test_lead=bool(lead.get("is_test_lead")),
        )
        if ok:
            lead["_sendability_reason"] = reason
            sendable.append(lead)
        else:
            lead["_sendability_reason"] = reason
            unsendable.append(lead)
    return sendable, unsendable
