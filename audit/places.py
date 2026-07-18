"""
audit/places.py — reviews via de Google Places API (Tier 2).

Veel klinieken hebben honderd Google-reviews en tonen er nul. De finding wordt dan
niet "u heeft geen reviews" maar "u heeft 87 reviews met een 4,7 gemiddelde en toont
er geen enkele" — een compliment verpakt als probleem. Places is de BRON;
site-detectie (reviews_zichtbaar) blijft aanvulling.

De pure functies (parse_place_details, build_review_finding) zijn zonder key/HTTP
te testen. De live-call gaat via een injecteerbare `fetcher` — default = echte
HTTP door de rate-limiter (bucket 'google_places'). GEEN key gezet
(GOOGLE_PLACES_API_KEY) -> get_place_reviews geeft {"error": "no_places_key"}.

NB (no-touch-tijdens-backfill): de 'google_places'-bucket moet nog aan
rate_limiter.RATE_LIMITS worden toegevoegd — dat is een wijziging aan een bestaand
bestand en gebeurt samen met de key, ná de backfill. Tot dan draait er geen
live-call (geen key), dus dit blokkeert niets.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable
from urllib.parse import quote

logger = logging.getLogger(__name__)

_PLACES_BUCKET = "google_places"
_FIND_URL = ("https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
             "?input={q}&inputtype=textquery&fields=place_id&key={key}")
_DETAILS_URL = ("https://maps.googleapis.com/maps/api/place/details/json"
                "?place_id={pid}&fields=rating,user_ratings_total,name&key={key}")


def parse_place_details(data: dict) -> dict:
    """Pure: Place Details-JSON -> {rating, review_count, name}. Leeg bij onbruikbaar."""
    res = (data or {}).get("result") or {}
    if not res:
        return {}
    return {
        "rating": res.get("rating"),
        "review_count": res.get("user_ratings_total"),
        "name": res.get("name"),
    }


def build_review_finding(places: dict, on_site_shown: bool, sector: str = "") -> dict:
    """Pure: de social-proof-finding op basis van de Places-data + wat de site toont.

    - Sterke reputatie die NIET getoond wordt -> compliment-als-probleem (warn/fail),
      hoge waarde, mail_safe.
    - Sterke reputatie die WEL getoond wordt -> pass.
    - Zwakke/afwezige reputatie -> feitelijke fail.
    """
    from audit.checks import _f
    rating = places.get("rating")
    n = places.get("review_count")
    cid, cat, mx = "reviews_via_places", "social_proof", 5
    if rating is None or n is None:
        return _f(cid, cat, "not_measurable", 0, mx)
    strong = rating >= 4.3 and n >= 20
    if strong and not on_site_shown:
        return _f(cid, cat, "fail", 0, mx,
                  bewijs=f"Google: {rating} met {n} reviews; site toont er geen",
                  severity="high", mail_safe=True,
                  mail_zin=f"U heeft {n} Google-reviews met een {rating} gemiddelde, "
                           f"maar op uw site is daar niets van te zien.")
    if strong and on_site_shown:
        return _f(cid, cat, "pass", mx, mx,
                  bewijs=f"Google: {rating} met {n} reviews, ook op de site getoond",
                  mail_zin=f"Uw sterke reputatie ({rating}, {n} reviews) staat ook op de site.")
    return _f(cid, cat, "fail", 0, mx,
              bewijs=f"Google: {rating if rating is not None else '-'} met {n} reviews",
              mail_zin=f"Uw Google-reputatie ({rating}, {n} reviews) is nog niet sterk genoeg om mee te leiden.")


async def _real_fetcher(url: str, sb: Any) -> dict:
    """Live HTTP door de rate-limiter. Alleen gebruikt als er een key is."""
    import httpx
    try:
        from utils.rate_limiter import wait_for_token
        await wait_for_token(_PLACES_BUCKET, sb)
    except Exception as e:  # bucket nog niet geregistreerd / limiter-fout -> niet hard falen
        logger.debug("places: rate-limiter overgeslagen (%s)", e)
    async with httpx.AsyncClient(timeout=12.0) as cl:
        r = await cl.get(url)
        return r.json() if r.status_code == 200 else {}


async def get_place_reviews(
    lead: dict, sb: Any, *,
    api_key: str | None = None,
    fetcher: Callable[[str, Any], Awaitable[dict]] | None = None,
) -> dict:
    """Resolve de place + haal rating/review-count op. Never-raise.

    api_key default = env GOOGLE_PLACES_API_KEY. fetcher injecteerbaar voor tests.
    Returns {rating, review_count, name, place_id} of {"error": ...}.
    """
    key = api_key or os.getenv("GOOGLE_PLACES_API_KEY")
    if not key:
        return {"error": "no_places_key"}
    fetch = fetcher or _real_fetcher
    try:
        q = " ".join(x for x in [lead.get("company_name"), lead.get("city")] if x).strip()
        if not q:
            return {"error": "no_query"}
        find = await fetch(_FIND_URL.format(q=quote(q), key=key), sb)
        cands = (find or {}).get("candidates") or []
        if not cands:
            return {"error": "place_not_found"}
        pid = cands[0].get("place_id")
        if not pid:
            return {"error": "no_place_id"}
        details = await fetch(_DETAILS_URL.format(pid=quote(pid), key=key), sb)
        out = parse_place_details(details)
        out["place_id"] = pid
        return out or {"error": "no_details"}
    except Exception as e:  # noqa: BLE001
        logger.error("get_place_reviews faalde: %s", e)
        return {"error": f"unexpected: {str(e)[:80]}"}
