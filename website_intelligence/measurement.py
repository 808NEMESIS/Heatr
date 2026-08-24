"""website_intelligence/measurement.py — het ENIGE pad van domein naar geldige meting.

Les van 2026-08 (vier vastgelopen rondes): 15 bestanden hadden elk hun eigen
fetch-logica, en `check_conversion` initialiseert elke boolean op False — waardoor
een geblokkeerde/lege pagina eruitziet als een gemeten "niet aanwezig". Deze module
centraliseert de drie beslissingen die dat voorkomen:

  1. BRUIKBAARHEID — een respons telt alleen als meting bij status 200-299, geen
     challenge-wall, en ≥1 positief content-signaal (anders: geen verdict).
  2. FALLBACK — httpx eerst; onbruikbaar én een renderer beschikbaar → Playwright
     (RenderedFetcher, bestaande anti-detectie) op de gerenderde DOM.
  3. HERKOMST — elke meting draagt een provenance-contract {method, status,
     body_size, content_seen, detector_version, measured_at}, zodat een consument
     (of een audit maanden later) zonder herfetch kan zien wat de waarde waard is.

Drie-waarden-principe voor consumenten: een veld is pas "False" (afwezig) als
`usable` True is; anders is de waarheid ONBEKEND — nooit een claim op bouwen.
"""
from __future__ import annotations

import datetime as _dt
import logging

import httpx

logger = logging.getLogger(__name__)

# Bump bij ELKE patroonwijziging in conversion_checker (platforms/regex): zo is in
# de opgeslagen provenance te zien met welke detector-generatie een waarde ontstond.
DETECTOR_VERSION = 2      # v2 = NL-boekpatronen + wa.me-veld (2026-08-14)

_UA = {"User-Agent": "Mozilla/5.0 (compatible; AerysBot/1.0; +https://aeryssolution.nl)"}
_WALL = ("just a moment", "checking your browser", "enable javascript",
         "attention required", "cf-browser-verification", "cf_chl_opt")
_TIMEOUT = 12.0
_MIN_BODY = 400


def richness(cd: dict | None) -> int:
    """Aantal POSITIEVE content-signalen in een check_conversion-resultaat.
    0 = geen herkenbare pagina-inhoud gezien (geblokkeerd / JS-shell / echt kaal):
    negatieve velden zijn dan betekenisloos."""
    if not cd:
        return 0
    sig = [
        bool(cd.get("cta_texts")),
        cd.get("has_phone_clickable") is True,
        cd.get("has_whatsapp") is True,
        cd.get("has_contact_form") is True,
        (cd.get("form_field_count") or 0) > 0,
        cd.get("has_chatbot") is True,
        bool(cd.get("booking_platform")),
        (cd.get("conversion_score") or 0) > 0,
        cd.get("has_cta_above_fold") is True,
        any(d.get("passed") is True for d in (cd.get("details") or [])),
    ]
    return sum(1 for s in sig if s)


async def fetch_httpx(domain: str) -> tuple[int | None, str]:
    """Kale httpx-fetch. Geeft óók 4xx/5xx terug (mét body) zodat de bruikbaarheids-
    gate de status kan zien — nooit `if r.text:` als succes-criterium (de 403-les)."""
    url = domain if domain.startswith("http") else f"https://{domain}"
    for u in (url, url.replace("https://", "http://")):
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True, headers=_UA) as c:
                r = await c.get(u)
                return r.status_code, (r.text or "")
        except Exception:
            continue
    return None, ""


async def usable_measurement(domain: str, status, text: str, sector: str):
    """Is deze respons een geldige meting? Returned (usable, result, richness, reden);
    result = check_conversion-output (None bij status/leeg/wall)."""
    from website_intelligence.conversion_checker import check_conversion

    if status is None:
        return False, None, 0, "fetch_error"
    if not (200 <= status <= 299):
        return False, None, 0, f"http_{status}"
    if not text or not text.strip() or len(text.strip()) < _MIN_BODY:
        return False, None, 0, "empty_body"
    if any(w in text.lower()[:4000] for w in _WALL):
        return False, None, 0, "challenge_wall"
    result = await check_conversion(domain, text, sector)
    rich = richness(result)
    if rich == 0:
        return False, result, 0, "no_content_signals"
    return True, result, rich, "ok"


def provenance(*, method: str | None, status, body_size: int, content_seen: bool,
               reason: str, now: _dt.datetime | None = None) -> dict:
    """Het herkomst-contract dat élke opgeslagen meting moet dragen."""
    ts = (now or _dt.datetime.now(_dt.timezone.utc)).isoformat()
    return {"method": method, "status": status, "body_size": body_size,
            "content_seen": content_seen, "reason": reason,
            "detector_version": DETECTOR_VERSION, "measured_at": ts}


async def measure_conversion(domain: str, sector: str, *, renderer=None) -> dict:
    """Domein → geldige conversie-meting via het ene pad (httpx → Playwright-fallback).

    Returned {usable, result, richness, provenance}. usable=False → result mag NOOIT
    als meting worden opgeslagen; de provenance legt vast waarom niet. `renderer` is
    een open RenderedFetcher (de aanroeper beheert de browser-levensduur)."""
    status, text = await fetch_httpx(domain)
    usable, result, rich, reason = await usable_measurement(domain, status, text, sector)
    method = "httpx"
    if not usable and renderer is not None:
        r_status, r_text = await renderer.fetch(domain)
        r_usable, r_result, r_rich, r_reason = await usable_measurement(
            domain, r_status, r_text, sector)
        if r_usable or (r_status is not None and not usable):
            status, text, usable, result, rich, reason = (
                r_status, r_text, r_usable, r_result, r_rich, r_reason)
            method = "playwright"
    import re as _re
    title = ""
    if usable and text:
        m = _re.search(r"<title[^>]*>(.*?)</title>", text, _re.I | _re.S)
        if m:
            title = _re.sub(r"\s+", " ", _re.sub(r"<[^>]+>", " ", m.group(1))).strip()[:160]
    return {
        "usable": usable, "result": result if usable else None, "richness": rich,
        "title": title,
        "provenance": provenance(method=method if status is not None else None,
                                 status=status, body_size=len(text or ""),
                                 content_seen=usable, reason=reason),
    }
