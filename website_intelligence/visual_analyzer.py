"""
website_intelligence/visual_analyzer.py — Layer 2: Visual website analysis via Claude Sonnet Vision.

Takes a Playwright screenshot, uploads to Supabase Storage, sends to Claude Sonnet
for visual quality assessment. Max 25 points.

Cost-control layers (PR2.5):
  1. SCREENSHOT_ENABLED env kill-switch (bestaand)
  2. Conditional skip: technical_score >= VISION_SKIP_TECHNICAL_THRESHOLD (default 20)
     → site is technisch al te goed voor Aerys-websitebouw-verkoop, Vision voegt
     weinig toe aan de opener/review-email. Skip + visual_score=None.
  3. Screenshot-hash cache via utils/vision_cache — zelfde PNG-bytes →
     dezelfde response, hergebruikt over leads en re-enrichments.

Deze laag is de duurste (~€0.014/lead, 87% van totaalkost). Conditional + cache
brengt dat structureel terug naar ~€0.005 gemiddeld bij normale flow en naar
~€0 bij re-enrichment.
"""
from __future__ import annotations

import logging
import os
from typing import Any

from utils.vision_cache import get_cached_vision, store_vision_result
from utils.rate_limiter import wait_for_token

# Gewogen visuele dimensies — gelijk aan config/scoring_weights.py VISUAL (max 25).
# Eén reproduceerbaar oordeel per as i.p.v. één gestalt-"overall".
_VISUAL_WEIGHTS = {
    "algemene_indruk": 10,
    "professionele_fotografie": 5,
    "typografie": 5,
    "kleurgebruik": 5,
}

logger = logging.getLogger(__name__)

SCREENSHOT_ENABLED = os.getenv("SCREENSHOT_ENABLED", "true").lower() == "true"
STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "screenshots")

# Boven deze technical_score gaan we Vision overslaan — site is al te gezond
# voor de Aerys-websitebouw-pitch. Override via env.
_VISION_SKIP_TECH_THRESHOLD = int(os.getenv("VISION_SKIP_TECHNICAL_THRESHOLD", "20"))


async def analyze_visual(
    domain: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
    sector: str = "",
    technical_score: int = 0,
    screenshot_desktop_b64: str | None = None,
    screenshot_mobile_b64: str | None = None,
) -> dict[str, Any]:
    """
    Claude Sonnet Vision op de desktop + mobiel screenshot uit capture_site.

    Returns dict met de 4 gewogen dimensies (algemene_indruk, professionele_
    fotografie, typografie, kleurgebruik; elk 1-10), overall_score (0-10, =
    algemene_indruk), top_strengths, top_improvements, en visual_score (0-25,
    gewogen som — None als Vision niet draaide of de parse faalde, zodat de
    normalisatie de laag correct uitsluit).

    Skips Vision when:
      - SCREENSHOT_ENABLED=false
      - technical_score >= _VISION_SKIP_TECH_THRESHOLD (site technisch al sterk)
      - geen desktop-screenshot aangereikt
    Cache-hit (zelfde desktop+mobiel-bytes): gecached resultaat, geen API-call.
    """
    result: dict[str, Any] = {
        "overall_score": None,
        "visual_score": None,
        "dimensions": {},
        "top_strengths": [],
        "top_improvements": [],
        "skipped_reason": None,
        "cache_hit": False,
    }

    if not SCREENSHOT_ENABLED:
        logger.info("Visual analysis skipped — SCREENSHOT_ENABLED=false")
        result["skipped_reason"] = "disabled"
        return result

    # Conditional gate: skip Vision if technical already strong
    if technical_score >= _VISION_SKIP_TECH_THRESHOLD:
        logger.info(
            "visual_analyzer: skipping Vision for %s — technical_score=%d >= %d (no Aerys-pitch opportunity)",
            domain, technical_score, _VISION_SKIP_TECH_THRESHOLD,
        )
        result["skipped_reason"] = f"technical_score_above_threshold:{technical_score}"
        return result

    # Screenshots komen uit capture_site (het enige capture-pad). Minimaal de
    # desktop is nodig; mobiel wordt meegestuurd als 'ie er is.
    if not screenshot_desktop_b64:
        result["skipped_reason"] = "no_screenshot"
        return result

    # Cache-key = gecombineerde desktop+mobiel-bytes (beide beelden bepalen het
    # oordeel, dus beide in de sleutel). Gelijke hash -> geen nieuwe call.
    cache_key = (screenshot_desktop_b64 or "") + "|" + (screenshot_mobile_b64 or "")
    try:
        cached = await get_cached_vision(cache_key, supabase_client)
    except Exception as e:
        logger.debug("visual_analyzer: cache lookup errored: %s", e)
        cached = None

    if cached:
        cached["cache_hit"] = True
        return cached

    # Claude Sonnet Vision analysis
    sector_context = {
        "alternatieve_geneeskunde": (
            "alternatieve / complementaire zorg praktijken in Nederland "
            "(acupuncturisten, osteopaten, natuurgeneeskundigen, haptotherapeuten)"
        ),
        "cosmetische_behandelaars": (
            "cosmetische klinieken en behandelaars in Nederland "
            "(botox, fillers, laser, huidtherapie, schoonheidssalons)"
        ),
    }.get(sector, "Nederlands MKB")

    prompt = (
        f"Je bent een senior webdesigner gespecialiseerd in {sector_context}.\n"
        "Je krijgt twee screenshots van dezelfde website: eerst DESKTOP, dan MOBIEL.\n"
        "Beoordeel de site op vier onderdelen, elk een geheel getal 1-10. Weeg\n"
        "desktop én mobiel mee — mobiel telt zwaar, de meeste bezoekers zijn mobiel.\n\n"
        "1. ALGEMENE INDRUK — modern en professioneel, desktop én mobiel? Oogt het responsive?\n"
        "2. PROFESSIONELE FOTOGRAFIE — echte, professionele foto's of goedkope stock?\n"
        "3. TYPOGRAFIE — leesbaar, modern, consistente hiërarchie?\n"
        "4. KLEURGEBRUIK — past bij de sector en coherent toegepast?\n\n"
        "Return ALLEEN JSON (geen andere tekst):\n"
        '{"algemene_indruk": N, "professionele_fotografie": N, "typografie": N, '
        '"kleurgebruik": N, "sterkste_punten": ["...", "...", "..."], '
        '"verbeterpunten": ["...", "...", "..."]}\n'
        "Elke N is een geheel getal 1-10. Wees direct en eerlijk, in het Nederlands."
    )

    try:
        # Vision-calls door de claude_sonnet rate-limiter (bucket bestond, nul
        # consumers). RuntimeError bij >120s -> valt in de except hieronder =>
        # visual_score None (fail-soft, normalisatie sluit de laag uit).
        await wait_for_token("claude_sonnet", supabase_client)

        desktop_img = _fit_for_vision(screenshot_desktop_b64)
        mobile_img = _fit_for_vision(screenshot_mobile_b64)
        content: list = [
            {"type": "text", "text": "DESKTOP:"},
            {"type": "image", "source": {"type": "base64", "media_type": "image/webp", "data": desktop_img}},
        ]
        if mobile_img:
            content.append({"type": "text", "text": "MOBIEL:"})
            content.append({"type": "image", "source": {"type": "base64", "media_type": "image/webp", "data": mobile_img}})
        content.append({"type": "text", "text": prompt})

        message = await anthropic_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            messages=[{"role": "user", "content": content}],
        )

        response_text = message.content[0].text
        parsed = _parse_vision_response(response_text)
        if not parsed.get("dimensions"):
            # Onparseerbaar -> geen betrouwbare score. Laat visual_score None
            # zodat de normalisatie de laag uitsluit i.p.v. een 0 door te rekenen.
            result["skipped_reason"] = "parse_failed"
            return result
        parsed["visual_score"] = _calculate_visual_score(parsed)
        parsed["skipped_reason"] = None
        parsed["cache_hit"] = False
        result = parsed

        # Cache op de gecombineerde desktop+mobiel-key.
        try:
            usage = getattr(message, "usage", None)
            in_tok = getattr(usage, "input_tokens", 0) or 0
            out_tok = getattr(usage, "output_tokens", 0) or 0
            await store_vision_result(
                cache_key, domain, result, in_tok, out_tok, supabase_client,
            )
        except Exception as e:
            logger.debug("visual_analyzer: cache store failed for %s: %s", domain, e)

    except Exception as e:
        logger.error("Claude Vision analysis failed for %s: %s", domain, e)

    return result


_VISION_MAX_DIM = 7999  # Claude Vision weigert beelden >8000px per dimensie


def _fit_for_vision(b64: str | None) -> str | None:
    """Verklein een WebP-b64 zodat de langste zijde <=7999px is (Claude-limiet).

    De full-page screenshots in Storage blijven onaangetast; alleen het beeld dat
    naar de Vision-API gaat wordt indien nodig geschaald. Claude verkleint intern
    toch naar ~1568px, dus dit kost geen detail dat Vision zou gebruiken.
    """
    if not b64:
        return b64
    from io import BytesIO
    import base64 as _b64
    from PIL import Image
    Image.MAX_IMAGE_PIXELS = 500_000_000  # zie utils.playwright_helpers.encode_webp
    try:
        raw = _b64.b64decode(b64)
        with Image.open(BytesIO(raw)) as img:
            w, h = img.size
            if max(w, h) <= _VISION_MAX_DIM:
                return b64
            scale = _VISION_MAX_DIM / max(w, h)
            img = img.convert("RGB").resize((max(1, int(w * scale)), max(1, int(h * scale))))
            out = BytesIO()
            img.save(out, format="WEBP", quality=80, method=6)
            return _b64.b64encode(out.getvalue()).decode("ascii")
    except Exception:
        return b64  # laat de API desnoods zelf oordelen


def _calculate_visual_score(parsed: dict[str, Any]) -> int:
    """Gewogen som van de 4 dimensies -> 0-25 punten (config/scoring_weights VISUAL).

    Elke dimensie is 1-10; punten = (dim/10) * max_points. Ontbrekende dimensies
    tellen als 0 (JSON hoort alle vier te leveren).
    """
    dims = parsed.get("dimensions") or {}
    pts = 0.0
    for name, maxpts in _VISUAL_WEIGHTS.items():
        v = dims.get(name)
        if v is not None:
            pts += (v / 10.0) * maxpts
    return int(round(min(25.0, pts)))


def _parse_vision_response(text: str) -> dict[str, Any]:
    """Parse de JSON-Vision-respons naar de 4 dimensies + strengths/improvements.

    JSON i.p.v. proza-regex: reproduceerbaarder, en de audit (stap 2) steunt op
    reproduceerbaarheid. overall_score = de algemene_indruk-dimensie (0-10), voor
    de opportunity_classifier `visual_score < 4`-check.
    """
    import json
    import re

    result: dict[str, Any] = {
        "dimensions": {},
        "top_strengths": [],
        "top_improvements": [],
        "overall_score": None,
        "raw_analysis": text,
    }

    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-z]*\n?", "", t)
        t = re.sub(r"\n?```$", "", t)
    try:
        data = json.loads(t)
    except (json.JSONDecodeError, ValueError):
        return result  # geen dimensions -> caller behandelt als parse_failed

    def _clamp(v: Any) -> int | None:
        try:
            return max(1, min(10, int(round(float(v)))))
        except (TypeError, ValueError):
            return None

    for name in _VISUAL_WEIGHTS:
        cv = _clamp(data.get(name))
        if cv is not None:
            result["dimensions"][name] = cv
    result["overall_score"] = result["dimensions"].get("algemene_indruk")
    result["top_strengths"] = [str(x).strip() for x in (data.get("sterkste_punten") or []) if str(x).strip()][:3]
    result["top_improvements"] = [str(x).strip() for x in (data.get("verbeterpunten") or []) if str(x).strip()][:3]
    return result
