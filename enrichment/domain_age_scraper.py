"""
enrichment/domain_age_scraper.py — Hoe lang bestaat het domein al?

Datapunt voor Warmr Sequence v1.0 Mail 1 BLOK A:
    "De site (...) draait al zo'n [website_age_years] jaar mee."

RDAP (Registration Data Access Protocol) > WHOIS:
  - JSON over HTTPS, geen TCP/43 parse-hel
  - Geen auth, publieke gratis API
  - IANA bootstrap resolver: rdap.org/domain/{name} → redirect naar juiste server
  - Voor .nl specifiek: rdap.sidn.nl/domain/{name}

Cost: €0. Wel rate-limited (SIDN ~20/min, andere registries meestal 10/s).

Cache-tabel heatr_domain_age_cache (migration 006):
  - Primary key: domain
  - TTL effectief 365 dagen (domain-age verandert zelden)
  - Gedeeld over workspaces (domain-registratie is globaal, geen privacy issue)

Never raises — returnt None op elke fout.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from utils.rate_limiter import wait_for_token

logger = logging.getLogger(__name__)

_CACHE_TTL_DAYS = 365
_RDAP_TIMEOUT = 10.0

# Registry-specific endpoints. rdap.org is de bootstrap resolver voor gTLDs.
# Voor .nl (SIDN) de directe endpoint omdat SIDN niet altijd in de bootstrap staat.
_RDAP_ENDPOINTS: list[str] = [
    "https://rdap.org/domain/{domain}",
    "https://rdap.sidn.nl/domain/{domain}",
    "https://rdap.verisign.com/com/v1/domain/{domain}",
]


def _normalize_domain(domain: str) -> str:
    """Strip protocol + www. Lowercase. Strip trailing slash."""
    if not domain:
        return ""
    d = domain.strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.split("/", 1)[0].split("?", 1)[0]
    return d


def _pick_registration_event(events: list[dict]) -> datetime | None:
    """RDAP 'events' array bevat o.a. registration, last changed, expiration.
    We willen 'registration' — de oorspronkelijke datum."""
    for ev in events or []:
        if (ev.get("eventAction") or "").lower() == "registration":
            raw = ev.get("eventDate") or ""
            try:
                # RDAP eventDate is RFC3339/ISO8601
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                continue
    return None


async def _fetch_rdap_registration(domain: str) -> datetime | None:
    """Try RDAP endpoints in order. First success wins."""
    async with httpx.AsyncClient(timeout=_RDAP_TIMEOUT, follow_redirects=True) as client:
        for tmpl in _RDAP_ENDPOINTS:
            url = tmpl.format(domain=domain)
            try:
                r = await client.get(url, headers={"Accept": "application/rdap+json"})
            except Exception as e:
                logger.debug("RDAP fetch error (%s): %s", url, e)
                continue
            if r.status_code >= 400:
                logger.debug("RDAP %s → %d", url, r.status_code)
                continue
            try:
                body = r.json()
            except Exception:
                continue
            events = body.get("events") or []
            dt = _pick_registration_event(events)
            if dt is not None:
                return dt
    return None


async def fetch_domain_age_years(
    domain: str,
    supabase_client: Any,
) -> dict[str, Any]:
    """Return {registered_at: datetime|None, years_old: int|None, from_cache: bool}.

    Flow:
      1. Normalize domain
      2. Check heatr_domain_age_cache (valid if checked_at within TTL)
      3. Rate-limit wait
      4. RDAP lookup (try multiple endpoints)
      5. Store result in cache (upsert)
    """
    out: dict[str, Any] = {
        "domain": _normalize_domain(domain),
        "registered_at": None,
        "years_old": None,
        "from_cache": False,
    }
    if not out["domain"]:
        return out

    d = out["domain"]

    # Cache hit?
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=_CACHE_TTL_DAYS)).isoformat()
        res = (
            supabase_client.table("domain_age_cache")
            .select("registered_at, years_old, checked_at")
            .eq("domain", d)
            .gte("checked_at", cutoff)
            .maybe_single()
            .execute()
        )
        if res.data:
            registered_raw = res.data.get("registered_at")
            out["registered_at"] = _parse_iso_safe(registered_raw) if registered_raw else None
            out["years_old"] = res.data.get("years_old")
            out["from_cache"] = True
            return out
    except Exception as e:
        logger.debug("domain_age_scraper: cache lookup failed for %s: %s", d, e)

    # Rate-limited RDAP fetch
    try:
        await wait_for_token("rdap", supabase_client)
    except Exception as e:
        logger.warning("domain_age_scraper: rate-limit wait failed: %s", e)
        return out

    try:
        reg_dt = await _fetch_rdap_registration(d)
    except Exception as e:
        logger.warning("domain_age_scraper: RDAP fetch crashed for %s: %s", d, e)
        reg_dt = None

    if reg_dt is None:
        # Store negative result too so we don't retry every single run
        try:
            supabase_client.table("domain_age_cache").upsert({
                "domain": d,
                "registered_at": None,
                "years_old": None,
                "source": "rdap_miss",
            }, on_conflict="domain").execute()
        except Exception:
            pass
        return out

    years = int((datetime.now(timezone.utc) - reg_dt).days // 365)
    out["registered_at"] = reg_dt
    out["years_old"] = years

    try:
        supabase_client.table("domain_age_cache").upsert({
            "domain": d,
            "registered_at": reg_dt.isoformat(),
            "years_old": years,
            "source": "rdap",
        }, on_conflict="domain").execute()
    except Exception as e:
        logger.debug("domain_age_scraper: cache store failed for %s: %s", d, e)

    logger.info("domain_age_scraper: %s registered=%s age=%d yrs", d, reg_dt.date(), years)
    return out


def _parse_iso_safe(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat((s or "").replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
