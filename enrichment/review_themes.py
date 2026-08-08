"""enrichment/review_themes.py — specifieke, gegronde review-thema's per lead.

Voor de value-first mail-1-opener (Sami 2026-08-08): niet oordelen over dingen
waar je geen verstand van hebt, maar teruggeven wat de EIGEN klanten zeggen. Dat
is oprecht + niet-faketbaar (Hormozi: bewijs dat je écht keek).

Bron = echte Google-reviews. Twee routes:
  1. Places API (schoon, robuust) — alleen als GOOGLE_PLACES_API_KEY gezet is.
  2. Playwright-scrape (gratis, fragieler) — enrichment/review_analyzer.

De tekst wordt gededupt, positieve reviews eruit, en Claude Haiku mine't er 2
SPECIFIEKE, herhaalde lof-thema's uit — GEGROND, verzint niks. Kritische reviews
worden geteld (zichtbaar voor de operator) maar niet in de opener gebruikt.
"""
from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger(__name__)

_MODEL = os.getenv("REVIEW_THEME_MODEL", "claude-haiku-4-5-20251001")
_POSITIVE_MIN_RATING = 4


def build_maps_search_url(company_name: str, city: str | None) -> str:
    """Google-Maps-zoek-URL voor een praktijk. Landt meestal direct op de plek."""
    q = " ".join(x for x in [company_name, city] if x).strip()
    from urllib.parse import quote_plus
    return f"https://www.google.com/maps/search/{quote_plus(q)}/"


def dedup_reviews(reviews: list[dict]) -> list[dict]:
    """Ontdubbel op reviewtekst (de scraper levert soms 2-3x dezelfde)."""
    seen: set[str] = set()
    out: list[dict] = []
    for r in reviews:
        t = (r.get("text") or "").strip()
        if t and t not in seen:
            seen.add(t)
            out.append(r)
    return out


def split_by_sentiment(reviews: list[dict]) -> tuple[list[dict], list[dict]]:
    """(positief, kritisch) op rating >= _POSITIVE_MIN_RATING."""
    pos, crit = [], []
    for r in reviews:
        try:
            rt = int(r.get("rating") or 0)
        except (TypeError, ValueError):
            rt = 0
        (pos if rt >= _POSITIVE_MIN_RATING else crit).append(r)
    return pos, crit


def _parse_theme_lines(text: str) -> list[str]:
    """Claude-output → max 2 schone thema-frases (kleine letter, geen opsomteken)."""
    out: list[str] = []
    for ln in (text or "").splitlines():
        s = ln.strip().lstrip("-•*0123456789. ").strip().strip('"').rstrip(".")
        if s:
            out.append(s)
    return out[:2]


async def mine_themes_from_reviews(
    reviews: list[dict], company_name: str, anthropic_client=None,
) -> list[str]:
    """Mine 2 specifieke, herhaalde lof-thema's uit POSITIEVE reviews. Puur
    gegrond (alleen wat er staat). Leeg → [] (aanroeper personaliseert dan niet)."""
    pos, _ = split_by_sentiment(dedup_reviews(reviews))
    texts = [(r.get("text") or "").strip() for r in pos if (r.get("text") or "").strip()]
    if len(texts) < 2:                       # te weinig materiaal → niet forceren
        return []

    client = anthropic_client
    if client is None:
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            return []
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=key)

    joined = "\n\n".join(f"[{r.get('rating')}*] {r.get('text','').strip()}" for r in pos)[:6000]
    prompt = (
        f"Hieronder echte positieve Google-reviews van praktijk '{company_name}'. Geef 2 "
        "SPECIFIEKE dingen waar klanten herhaaldelijk over te spreken zijn, elk als korte "
        "kleine-letter Nederlandse frase die in een zin past (bv. 'hoe op hun gemak mensen "
        "zich voelen vanaf het eerste consult'). Baseer je UITSLUITEND op wat er staat, "
        "generaliseer niet, verzin niks. Geef PRECIES twee regels, elk één frase, zonder "
        "opsomtekens.\n\n" + joined
    )
    try:
        m = await client.messages.create(model=_MODEL, max_tokens=180,
                                         messages=[{"role": "user", "content": prompt}])
        return _parse_theme_lines(m.content[0].text or "")
    except Exception as e:
        logger.warning("mine_themes_from_reviews: Claude-call faalde: %s", e)
        return []


async def extract_review_themes(
    company_name: str, city: str | None, *,
    max_reviews: int = 10, anthropic_client=None, maps_url: str | None = None,
) -> dict:
    """Volledige pijplijn: scrape reviews → dedup → mine 2 thema's. Never-raise.

    Returns {"themes": [..], "positive_count": int, "critical_count": int}.
    themes is [] als er te weinig/geen bruikbaar materiaal is (dan personaliseren
    we niet nep)."""
    from enrichment.review_analyzer import scrape_google_reviews

    url = maps_url or build_maps_search_url(company_name, city)
    try:
        raw = await scrape_google_reviews(url, max_reviews=max_reviews)
    except Exception as e:
        logger.warning("extract_review_themes: scrape faalde voor %s: %s", company_name, e)
        raw = []
    uniq = dedup_reviews(raw)
    pos, crit = split_by_sentiment(uniq)
    themes = await mine_themes_from_reviews(uniq, company_name, anthropic_client=anthropic_client)
    return {"themes": themes, "positive_count": len(pos), "critical_count": len(crit)}
