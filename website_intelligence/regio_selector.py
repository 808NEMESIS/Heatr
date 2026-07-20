"""
website_intelligence/regio_selector.py — outreach spec 5.

Levert per lead twee mail 2-tokens, nul externe API:
  {{concurrent_signaal}} — de grootste verdedigbare achterstand t.o.v. een
      praktijk binnen RADIUS_KM (pool = de leads-database zelf, same-sector,
      met coördinaten uit spec 4). Extern anoniem ("in jouw regio"); intern
      onderbouwd met echte getallen.
  {{detail_2}} — een tweede observatie over de EIGEN site, uit een ANDERE laag
      dan de opener (spec 5b: opener leidt op reviews/social → detail_2 pakt
      conversie/techniek, geen dubbeling).

Ontbreekt de data (geen coords, lege pool, geen verdedigbare kloof) → None, en
de sequence-engine kiest de degradation-variant van mail 2.

Ontwerpnoot: de blauwdruk noemde "laag 5" (competitor_analyzer) als pool. Die
slaat geen coördinaten/pool op; de leads-database heeft ná spec 4 wél coords +
scores + reviews, dus dát is de pool. Bewuste, onderbouwde afwijking.
"""
from __future__ import annotations

import logging

from utils.geo import haversine_km

logger = logging.getLogger(__name__)

RADIUS_KM_DEFAULT = 15.0            # stad; landelijk hoger via config (later)
_MIN_REVIEWS_ABS = 30              # concurrent moet zelf substantie hebben
_MIN_RATED_REVIEWS = 20           # rating pas geloofwaardig vanaf X reviews
_MIN_OWN_REVIEWS = 5              # eigen review-basis vóór "waar jullie er N hebben"
                                 # (0/null = scrape-gat of te hard voor koude mail)


def _fmt_rating(r: float) -> str:
    return f"{r:.1f}".replace(".", ",")


def select_concurrent_gap(lead: dict, pool: list[dict], *, brug: str) -> str | None:
    """Grootste verdedigbare achterstand → anonieme NL-zin, of None.

    pool = radius-gefilterde same-sector leads (excl. self), elk met
    google_review_count / google_rating / has_online_booking.
    Salience-ranking is een heuristiek (grofweg de omvang van de kloof); de
    zin noemt echte getallen zodat de claim nooit verzonnen is.
    """
    my_rev = lead.get("google_review_count") or 0
    my_rat = lead.get("google_rating") or 0.0
    cands: list[tuple[float, str]] = []

    # reviews-achterstand (publiek trust-signaal, beide bruggen). Alleen bij een
    # ECHTE eigen basis (>= _MIN_OWN_REVIEWS): "waar jullie er 0 hebben" is te hard
    # voor een koude mail én 0/null is vaak een scrape-gat, geen echte nul.
    best_rev = max((c.get("google_review_count") or 0 for c in pool), default=0)
    if my_rev >= _MIN_OWN_REVIEWS and best_rev >= _MIN_REVIEWS_ABS and best_rev >= 2 * my_rev:
        cands.append((float(best_rev - my_rev),
                      f"een praktijk op een paar kilometer met {best_rev} reviews, "
                      f"waar jullie er {my_rev} hebben"))

    # rating-achterstand
    rated = [c for c in pool
             if (c.get("google_review_count") or 0) >= _MIN_RATED_REVIEWS and c.get("google_rating")]
    best_rat = max((c["google_rating"] for c in rated), default=0.0)
    if best_rat and my_rat and best_rat >= my_rat + 0.3:
        cands.append(((best_rat - my_rat) * 50,
                      f"een praktijk vlakbij die op {_fmt_rating(best_rat)} staat, "
                      f"net boven jullie {_fmt_rating(my_rat)}"))

    # workflow-brug: online-afspreken-kloof (reageersnelheid/bereikbaarheid)
    if brug == "workflow" and not lead.get("has_online_booking") \
            and any(c.get("has_online_booking") for c in pool):
        cands.append((40.0,
                      "een praktijk vlakbij waar patiënten direct online een afspraak "
                      "maken, iets wat bij jullie nog niet kan"))

    if not cands:
        return None
    cands.sort(key=lambda x: x[0], reverse=True)
    return cands[0][1]


def pick_detail_2(lead: dict) -> str | None:
    """Tweede EIGEN-site-observatie uit een andere laag dan de opener (spec 5b).

    Opener leidt op reviews/social → detail_2 pakt conversie (afspreken/WhatsApp)
    of techniek (snelheid). NAAMWOORD-frase, gekapitaliseerd — loopt soepel in het
    mail 2-frame "{{detail_2}} liet me niet los" (geen botsende werkwoorden).
    """
    if not lead.get("has_online_booking"):
        return "De ontbrekende afspraakknop op je site"
    if not lead.get("has_whatsapp"):
        return "Het ontbreken van een WhatsApp-knop op je site"
    ws = lead.get("website_score")
    if ws is not None and ws < 50:
        return "De laadtijd van je site"
    return None


def radius_pool(lead: dict, candidates: list[dict], *, radius_km: float = RADIUS_KM_DEFAULT) -> list[dict]:
    """Filter same-sector candidates op Haversine <= radius, excl. self, met coords."""
    lat, lng = lead.get("lat"), lead.get("lng")
    if lat is None or lng is None:
        return []
    out = []
    for c in candidates:
        if c.get("id") == lead.get("id"):
            continue
        clat, clng = c.get("lat"), c.get("lng")
        if clat is None or clng is None:
            continue
        if haversine_km(lat, lng, clat, clng) <= radius_km:
            out.append(c)
    return out


async def compute_regio_signals(
    lead: dict, supabase_client, *, workspace_id: str, brug: str,
    radius_km: float = RADIUS_KM_DEFAULT,
) -> dict:
    """Live: {"concurrent_signaal": str|None, "detail_2": str|None}.

    detail_2 hangt niet op coords; concurrent_signaal wel (pool via radius).
    Read-only op leads — raakt de lopende website-backfill niet.
    """
    detail_2 = pick_detail_2(lead)
    concurrent = None
    if lead.get("lat") is not None and lead.get("lng") is not None:
        try:
            cand = (supabase_client.table("leads")
                    .select("id, lat, lng, google_review_count, google_rating, has_online_booking")
                    .eq("workspace_id", workspace_id).eq("sector", lead.get("sector"))
                    .not_.is_("lat", "null").execute()).data or []
            pool = radius_pool(lead, cand, radius_km=radius_km)
            concurrent = select_concurrent_gap(lead, pool, brug=brug)
        except Exception as e:
            logger.warning("compute_regio_signals: pool-query faalde (%s) — geen concurrent-zin", e)
    return {"concurrent_signaal": concurrent, "detail_2": detail_2}
