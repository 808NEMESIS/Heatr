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
import random
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
_SMTP_INVALID_CODES = {550, 551, 552, 553, 554, 450, 501, 503, 521}
_SMTP_RISKY_CODES = {421, 452, 451}


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

    # --- Rate limit: max 3 SMTP checks per domain per hour -------------------
    try:
        await wait_for_token("smtp_verify", supabase_client)
    except Exception:
        logger.warning("SMTP rate limit hit for domain %s", domain)
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
        return (None, "not_found")

    best_risky: str | None = None
    best_risky_status: str = "risky"

    results = await verify_email_list(candidates, supabase_client)

    for result in results:
        status = result["status"]
        email = result["email"]
        if status == "valid":
            return (email, "valid")
        if status in ("risky", "catchall_risky") and best_risky is None:
            best_risky = email
            best_risky_status = status

    if best_risky:
        return (best_risky, best_risky_status)

    return (None, "not_found")


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
        return ("risky", "timeout")
    except Exception as e:
        logger.debug("SMTP verify exception for %s: %s", email, e)
        return ("risky", "exception")


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
                elif code in _SMTP_RISKY_CODES:
                    return ("risky", "smtp")
                else:
                    # Unknown code — treat as risky
                    logger.debug("Unknown SMTP code %d for %s", code, email)
                    return ("risky", "smtp")

        except smtplib.SMTPConnectError:
            logger.debug("SMTP connect error for MX %s", mx_host)
            continue
        except smtplib.SMTPServerDisconnected:
            logger.debug("SMTP server disconnected for MX %s", mx_host)
            continue
        except socket.timeout:
            logger.debug("SMTP timeout for MX %s", mx_host)
            continue
        except ConnectionRefusedError:
            logger.debug("SMTP connection refused for MX %s", mx_host)
            continue
        except OSError as e:
            logger.debug("SMTP OS error for MX %s: %s", mx_host, e)
            continue
        except Exception as e:
            logger.debug("SMTP unexpected error for MX %s: %s", mx_host, e)
            continue

    return ("risky", "timeout")
