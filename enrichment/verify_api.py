"""
enrichment/verify_api.py — externe e-mailverificatie (remediation C1-vervolg).

De SMTP-verifier kan niet werken vanaf een host met dichte poort 25 (audit
V2 + diagnostiek: IPv4:25 geblokkeerd, doelgroep IPv4-only NL-MX). Deze
module verifieert via een externe API (Bouncer, EU/GDPR) en levert een
ECHT oordeel voor de risky/not_checked-bucket.

Provider-agnostisch: één interface `verify_via_api(email)`, mapping per
provider. Verstuurt NIETS — verificatie leest alleen de mailserver-status.

Status-mapping naar onze granulaire enum (email_verifier.coarse_email_status
zet die door naar de coarse email_status binnen de migratie-023 CHECK):
  deliverable   → valid
  undeliverable → invalid
  risky         → risky            (echte adres-twijfel; verzendbaar met gate)
  catch-all     → catchall_risky
  unknown       → not_checked      (fail-closed)
"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_BOUNCER_URL = "https://api.usebouncer.com/v1.1/email/verify"

# Bouncer 'status' → onze granulaire verificatie-status
_BOUNCER_STATUS = {
    "deliverable": "valid",
    "undeliverable": "invalid",
    "risky": "risky",
    "unknown": "not_checked",
}


def provider() -> str:
    """Actieve provider uit env ('bouncer' | 'none')."""
    return (os.getenv("EMAIL_VERIFY_PROVIDER") or "none").strip().lower()


def is_enabled() -> bool:
    return provider() == "bouncer" and bool(os.getenv("BOUNCER_API_KEY", "").strip())


async def verify_via_api(email: str, *, timeout: float = 20.0) -> dict:
    """Verifieer één adres via de externe API.

    Returns:
        {status, method, provider_status, catch_all, raw_reason, score}
        status = onze granulaire enum; method = 'bouncer_api' | 'disabled' | 'error'.
    Verstuurt geen mail.
    """
    if not email or "@" not in email:
        return {"status": "invalid", "method": "format_check"}
    prov = provider()
    if prov != "bouncer":
        return {"status": "not_checked", "method": "disabled"}
    key = os.getenv("BOUNCER_API_KEY", "").strip()
    if not key:
        return {"status": "not_checked", "method": "disabled"}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(
                _BOUNCER_URL,
                params={"email": email, "timeout": 15},
                headers={"x-api-key": key},
            )
        if r.status_code == 401:
            logger.error("verify_api: Bouncer 401 — ongeldige API-key.")
            return {"status": "not_checked", "method": "error", "raw_reason": "auth_401"}
        if r.status_code == 402:
            logger.error("verify_api: Bouncer 402 — geen tegoed meer.")
            return {"status": "not_checked", "method": "error", "raw_reason": "no_credits_402"}
        if r.status_code == 429:
            logger.warning("verify_api: Bouncer 429 — rate-limited.")
            return {"status": "not_checked", "method": "rate_limited", "raw_reason": "rate_limited"}
        r.raise_for_status()
        data: dict[str, Any] = r.json()
    except Exception as e:
        logger.warning("verify_api: Bouncer-call faalde voor %s: %s", email, e)
        return {"status": "not_checked", "method": "error", "raw_reason": str(e)[:120]}

    bstatus = (data.get("status") or "unknown").lower()
    domain = data.get("domain") or {}
    accept_all = str(domain.get("acceptAll") or "").lower() == "yes"

    # catch-all wint: een deliverable op een accept-all-domein is onzeker.
    if accept_all and bstatus in ("deliverable", "risky", "unknown"):
        status = "catchall_risky"
    else:
        status = _BOUNCER_STATUS.get(bstatus, "not_checked")

    return {
        "status": status,
        "method": "bouncer_api",
        "provider_status": bstatus,
        "catch_all": accept_all,
        "raw_reason": data.get("reason"),
        "score": data.get("score"),
    }
