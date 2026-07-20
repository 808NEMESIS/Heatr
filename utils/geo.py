"""
utils/geo.py — coördinaten uit Google-Maps-URLs + afstand (outreach spec 4/5).

Nul externe API: coördinaten zitten al in leads.google_maps_url en afstand is
pure wiskunde (Haversine). Geverifieerd 2026-07-20: 424/424 cosmetiek-leads
hebben het !3d!4d-paar (de ECHTE bedrijfslocatie); het /@lat,lng-paar is het
kaartbeeld-centrum (stadsniveau) en is dus NIET bruikbaar voor een radius binnen
de stad.
"""
from __future__ import annotations

import re
from math import asin, cos, radians, sin, sqrt

# Voorkeur: !3d<lat>!4d<lng> = de daadwerkelijke place-marker. Fallback /@ alleen
# als het place-paar ontbreekt (kaartbeeld-centrum, minder precies).
_PLACE_RE = re.compile(r"!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)")
_VIEW_RE = re.compile(r"/@(-?\d+\.\d+),(-?\d+\.\d+)")


def extract_place_coords(maps_url: str | None) -> tuple[float, float] | None:
    """(lat, lng) van de bedrijfslocatie uit een Google-Maps-URL, of None.

    Prefereert het !3d!4d-place-paar; valt terug op /@ (kaartbeeld) als het
    place-paar ontbreekt.
    """
    if not maps_url:
        return None
    m = _PLACE_RE.search(maps_url)
    if not m:
        m = _VIEW_RE.search(maps_url)
    if not m:
        return None
    try:
        return float(m.group(1)), float(m.group(2))
    except (TypeError, ValueError):
        return None


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Afstand in km tussen twee coördinaten (groot-cirkel, nul API)."""
    dlat, dlng = radians(lat2 - lat1), radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    return 2 * 6371.0 * asin(sqrt(a))
