"""
enrichment/email_verifier.py — MX + SMTP + catch-all email verification.

Never sends an actual email. Uses the SMTP RCPT TO handshake only to test
deliverability. All SMTP connections close with QUIT before the DATA command.

Verification flow per address:
  1. MX lookup via dnspython — no MX → invalid immediately
  2. Catch-all check (if CATCHALL_CHECK_ENABLED=true) — cached per domain 7 days
  3. SMTP RCPT TO handshake against lowest-priority MX host

Status values returned:
  valid         — 250 response to RCPT TO
  invalid       — 5xx permanent rejection
  risky         — 4xx temporary rejection, timeout, or connection refused
  catchall_risky — domain accepts all addresses (deliverability unknown)
  not_checked   — verification skipped (e.g. rate limit, MX timeout)
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import smtplib
import socket
from datetime import datetime, timedelta, timezone
from typing import Any

import dns.resolver
import dns.exception

from utils.rate_limiter import wait_for_token

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EHLO_DOMAIN = "heatr-verify.com"
_MAIL_FROM = f"verify@{_EHLO_DOMAIN}"
_CATCHALL_TTL_DAYS = 7
_CATCHALL_PREFIX = "xzq7k2m9"

# SMTP response code categories
_SMTP_VALID_CODES = {250}
_SMTP_INVALID_CODES = {550, 551, 552, 553, 554, 501, 503, 521}
# Temporary/greylist codes — NIET 'risky' (adres-kwaliteit) maar
# 'temporary_failure' (infra/greylisting). 450 hoort hier: het is een
# tijdelijke weigering, geen definitieve bounce. Her-verificatie (fase 2.5)
# probeert deze later opnieuw; inline seconden-retry verslaat greylisting niet.
_SMTP_TEMPORARY_CODES = {421, 450, 451, 452}

# Granulaire verificatie-statussen (remediation C1). De eerste drie zijn
# ADRES-kwaliteit; de laatste drie zijn INFRA-fouten die NIET als
# verzendbaar-twijfelachtig mogen tellen.
_ADDRESS_STATUSES = {"valid", "invalid", "risky", "catchall_risky"}
_INFRA_STATUSES = {"timeout", "connection_error", "temporary_failure"}

# Granulaire status → coarse email_status dat binnen de migratie-023 CHECK
# valt. Infra-fouten worden 'not_checked' (al toegestaan én al fail-closed in
# is_sendable); de precieze reden leeft in email_verification_method.
_COARSE_EMAIL_STATUS = {
    "valid": "valid",
    "invalid": "invalid",
    "risky": "risky",
    "catchall_risky": "catchall_risky",
    "not_found": "not_found",
    "not_checked": "not_checked",
    "timeout": "not_checked",
    "connection_error": "not_checked",
    "temporary_failure": "not_checked",
}


def coarse_email_status(granular_status: str) -> str:
    """Map een granulaire verificatie-status naar de coarse email_status die
    de migratie-023 CHECK toestaat. Onbekend → 'not_checked' (fail-closed)."""
    return _COARSE_EMAIL_STATUS.get(granular_status, "not_checked")


# =============================================================================
# Public API
# =============================================================================

async def verify_email(
    email: str,
    supabase_client: Any,
) -> tuple[str, str]:
    """Verify a single email address via MX check and SMTP handshake.

    Args:
        email: Email address to verify.
        supabase_client: Supabase client (used for domain cache and rate limits).

    Returns:
        Tuple of (email_status, verification_method) where email_status is one of:
        'valid' | 'invalid' | 'risky' | 'catchall_risky' | 'not_checked'
        and verification_method is one of:
        'smtp' | 'mx_check' | 'catchall_detection' | 'timeout' | 'cache' | 'rate_limited'
    """
    if not email or "@" not in email:
        return ("invalid", "format_check")

    email = email.lower().strip()
    domain = email.split("@")[1]
    timeout = int(os.getenv("EMAIL_VERIFY_TIMEOUT", "10"))

    # Externe verify-API eerst (remediation C1-vervolg): werkt óók vanaf een
    # host met dichte poort 25. Alleen als geconfigureerd (BOUNCER_API_KEY).
    # Verstuurt niets. Bij een API-fout (timeout/402/error) valt-ie NIET terug
    # op de kapotte SMTP-verifier maar geeft de fail-closed status door.
    try:
        from enrichment.verify_api import is_enabled as _api_enabled, verify_via_api
        if _api_enabled():
            res = await verify_via_api(email)
            return (res["status"], res.get("method", "bouncer_api"))
    except Exception as e:
        logger.warning("verify_email: externe API-pad faalde voor %s: %s", email, e)
        # val NIET stil terug op SMTP als de API bedoeld was — fail-closed
        return ("not_checked", "error")

    # --- Rate limit: globale SMTP-verificatie-bucket -------------------------
    # De 'smtp_verify'-key bestaat nu in RATE_LIMITS (recovery-fix). Een
    # exceptie hier is dus GEEN ontbrekende-key meer maar echte rate-limit-
    # uitputting (RuntimeError na 120s) of een DB-fout — log luid met de
    # werkelijke oorzaak i.p.v. alles stil als 'rate_limited' te maskeren.
    try:
        await wait_for_token("smtp_verify", supabase_client)
    except Exception as e:
        logger.error("SMTP verify rate-limit/wait faalde voor domein %s: %s: %s",
                     domain, type(e).__name__, e)
        return ("not_checked", "rate_limited")

    # --- Step 1: MX record check ---------------------------------------------
    mx_hosts = await _get_mx_records(domain)
    if not mx_hosts:
        logger.debug("No MX records for domain %s", domain)
        return ("invalid", "mx_check")

    # --- Step 2: Catch-all check (cached) ------------------------------------
    catchall_enabled = os.getenv("CATCHALL_CHECK_ENABLED", "true").lower() == "true"
    if catchall_enabled:
        cached = await _get_cached_catchall(domain, supabase_client)
        if cached is True:
            return ("catchall_risky", "cache")
        elif cached is None:
            # Not cached — run detection
            is_catchall = await _check_catchall(domain, mx_hosts, timeout)
            await _store_catchall_cache(domain, is_catchall, supabase_client)
            if is_catchall:
                return ("catchall_risky", "catchall_detection")
        # cached is False → not a catchall, proceed to SMTP check

    # --- Step 3: SMTP handshake for the actual address -----------------------
    status, method = await _smtp_verify(email, mx_hosts, timeout)
    return (status, method)


async def verify_email_list(
    emails: list[str],
    supabase_client: Any,
) -> list[dict]:
    """Verify a list of email candidates, stopping at the first valid result.

    Args:
        emails: List of candidate email strings to verify in order.
        supabase_client: Supabase client.

    Returns:
        List of dicts, one per email, each with keys:
        'email', 'status', 'method'. Stops verifying after first 'valid'.
    """
    results: list[dict] = []
    for email in emails:
        try:
            status, method = await verify_email(email, supabase_client)
            results.append({"email": email, "status": status, "method": method})
            if status == "valid":
                break  # Waterfall stops at first confirmed valid
        except Exception as e:
            logger.warning("verify_email raised for %s: %s", email, e)
            results.append({"email": email, "status": "not_checked", "method": "error"})
    return results


async def get_best_email(
    candidates: list[str],
    supabase_client: Any,
) -> tuple[str | None, str]:
    """Verify candidates and return the best address found.

    Best = first 'valid'. Fallback = first 'risky' or 'catchall_risky'.
    Returns (None, 'not_found') if nothing usable is found.

    Args:
        candidates: Ordered list of candidate email strings to try.
        supabase_client: Supabase client.

    Returns:
        Tuple of (best_email, status). best_email is None if nothing found.
    """
    if not candidates:
        return (None, "not_found", "none")

    results = await verify_email_list(candidates, supabase_client)

    # Voorkeursvolgorde: bevestigd valid > adres-twijfel (risky/catchall) >
    # infra-onbekend (timeout/connection_error/temporary_failure). Een
    # infra-onbekend adres wordt WEL teruggegeven zodat de waterfall het
    # opslaat, maar met zijn infra-status → coarse 'not_checked' → NIET
    # verzendbaar (fail-closed). Alleen 'valid' en echte 'risky' (met
    # method='smtp') zijn later verzendbaar.
    first_address: tuple[str, str, str] | None = None
    first_infra: tuple[str, str, str] | None = None

    for result in results:
        status = result["status"]
        email = result["email"]
        method = result.get("method", "smtp")
        if status == "valid":
            return (email, "valid", method)
        if status in ("risky", "catchall_risky") and first_address is None:
            first_address = (email, status, method)
        elif status in _INFRA_STATUSES and first_infra is None:
            first_infra = (email, status, method)

    if first_address:
        return first_address
    if first_infra:
        return first_infra
    return (None, "not_found", "not_found")


# =============================================================================
# MX lookup
# =============================================================================

async def _get_mx_records(domain: str) -> list[str]:
    """Fetch MX records for a domain and return hosts ordered by priority.

    Uses asyncio.get_event_loop().run_in_executor to avoid blocking the event
    loop on DNS I/O (dnspython is synchronous).

    Args:
        domain: Domain string, e.g. 'example.nl'.

    Returns:
        List of MX hostnames ordered by priority (lowest number = highest priority).
        Empty list if no MX records or DNS error.
    """
    loop = asyncio.get_event_loop()
    try:
        records = await loop.run_in_executor(
            None, lambda: dns.resolver.resolve(domain, "MX")
        )
        # Sort by preference (lower = higher priority)
        sorted_records = sorted(records, key=lambda r: r.preference)
        return [str(r.exchange).rstrip(".") for r in sorted_records]
    except (dns.exception.DNSException, Exception) as e:
        logger.debug("MX lookup failed for %s: %s", domain, e)
        return []


# =============================================================================
# Catch-all detection
# =============================================================================

async def _check_catchall(
    domain: str,
    mx_hosts: list[str],
    timeout: int,
) -> bool:
    """Test whether a domain accepts all addresses (catch-all).

    Sends RCPT TO with a known-nonexistent address. If accepted: catch-all.

    Args:
        domain: Domain to test.
        mx_hosts: Pre-resolved MX hosts list (lowest priority first).
        timeout: SMTP connection timeout in seconds.

    Returns:
        True if catch-all detected, False otherwise.
    """
    fake_local = f"{_CATCHALL_PREFIX}_{secrets.token_hex(6)}"
    fake_email = f"{fake_local}@{domain}"
    status, _ = await _smtp_verify(fake_email, mx_hosts, timeout)
    return status == "valid"


async def _get_cached_catchall(
    domain: str,
    supabase_client: Any,
) -> bool | None:
    """Check the domain_cache table for a non-expired catch-all result.

    Args:
        domain: Domain string.
        supabase_client: Supabase client.

    Returns:
        True if cached as catchall, False if cached as not-catchall, None if not cached.
    """
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        response = (
            supabase_client.table("domain_cache")
            .select("is_catchall, expires_at")
            .eq("domain", domain)
            .gte("expires_at", now_iso)
            .single()
            .execute()
        )
        if response.data:
            return bool(response.data["is_catchall"])
    except Exception:
        pass
    return None


async def _store_catchall_cache(
    domain: str,
    is_catchall: bool,
    supabase_client: Any,
) -> None:
    """Store catch-all detection result in domain_cache with a 7-day TTL.

    Args:
        domain: Domain string.
        is_catchall: Whether the domain is a catch-all.
        supabase_client: Supabase client.
    """
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=_CATCHALL_TTL_DAYS)
    try:
        supabase_client.table("domain_cache").upsert({
            "domain": domain,
            "is_catchall": is_catchall,
            "has_mx": True,
            "checked_at": now.isoformat(),
            "expires_at": expires.isoformat(),
        }).execute()
    except Exception as e:
        logger.warning("Failed to cache catchall result for %s: %s", domain, e)


# =============================================================================
# SMTP handshake
# =============================================================================

async def _smtp_verify(
    email: str,
    mx_hosts: list[str],
    timeout: int,
) -> tuple[str, str]:
    """Run an SMTP RCPT TO handshake to verify an address.

    Connects to the first available MX host on port 25. Sends EHLO, MAIL FROM,
    RCPT TO, then QUIT — never DATA.

    Args:
        email: Full email address to verify.
        mx_hosts: Ordered list of MX hostnames (highest priority first).
        timeout: Connection + read timeout in seconds.

    Returns:
        Tuple of (status, method).
    """
    loop = asyncio.get_event_loop()
    try:
        status, method = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: _smtp_verify_sync(email, mx_hosts, timeout),
            ),
            timeout=timeout + 2,  # Outer async timeout slightly larger
        )
        return (status, method)
    except asyncio.TimeoutError:
        # INFRA-fout, GEEN adres-oordeel (remediation C1).
        return ("timeout", "timeout")
    except Exception as e:
        logger.debug("SMTP verify exception for %s: %s", email, e)
        return ("connection_error", "exception")


def _smtp_verify_sync(
    email: str,
    mx_hosts: list[str],
    timeout: int,
) -> tuple[str, str]:
    """Synchronous SMTP verification (runs in thread executor).

    Tries each MX host in order. Returns on first connection that gives a
    definitive answer. Falls back to 'risky' if all hosts fail.

    Args:
        email: Email to verify.
        mx_hosts: MX hosts ordered by priority.
        timeout: Per-host timeout in seconds.

    Returns:
        Tuple of (status, method).
    """
    # We onthouden of ÉÉN host een RCPT-antwoord gaf (dan is de infra ok en
    # is een verder falen adres-gerelateerd) vs. dat we nooit een handshake
    # kregen (pure infra-fout → connection_error/timeout, NIET 'risky').
    saw_timeout = False
    for mx_host in mx_hosts[:3]:  # Try at most 3 MX hosts
        try:
            with smtplib.SMTP(timeout=timeout) as smtp:
                smtp.connect(mx_host, 25)
                smtp.ehlo(_EHLO_DOMAIN)
                smtp.mail(_MAIL_FROM)
                code, _ = smtp.rcpt(email)
                smtp.quit()

                if code in _SMTP_VALID_CODES:
                    return ("valid", "smtp")
                elif code in _SMTP_INVALID_CODES:
                    return ("invalid", "smtp")
                elif code in _SMTP_TEMPORARY_CODES:
                    # 421/450/451/452 = greylist/tijdelijk → INFRA, geen
                    # adres-oordeel. Her-verificatie (2.5) probeert later opnieuw.
                    return ("temporary_failure", "smtp")
                else:
                    # Onbekende code, maar we KREGEN een RCPT-antwoord → de
                    # infra werkt; behandel als adres-twijfel (risky), niet infra.
                    logger.debug("Unknown SMTP code %d for %s", code, email)
                    return ("risky", "smtp")

        except socket.timeout:
            logger.debug("SMTP timeout for MX %s", mx_host)
            saw_timeout = True
            continue
        except (smtplib.SMTPConnectError, smtplib.SMTPServerDisconnected,
                ConnectionRefusedError, OSError) as e:
            logger.debug("SMTP connect/OS error for MX %s: %s", mx_host, e)
            continue
        except Exception as e:
            logger.debug("SMTP unexpected error for MX %s: %s", mx_host, e)
            continue

    # Geen enkele host gaf een RCPT-antwoord → PURE INFRA-fout. NOOIT 'risky'
    # (dat zou een niet-bestaand/gegokt adres verzendbaar maken). C1-kern.
    return ("timeout", "timeout") if saw_timeout else ("connection_error", "connection_error")
