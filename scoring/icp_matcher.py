"""
scoring/icp_matcher.py — ICP (Ideal Customer Profile) matching.

Checks a lead against the sector's ICP definition: keywords, disqualifiers,
Google rating, KvK SBI match.

Returns a 0.0-1.0 ICP match score stored in leads.icp_match.
"""
from __future__ import annotations

import logging
from typing import Any

from config.sectors import get_sector

logger = logging.getLogger(__name__)

# Per sectors.py (2026-04-21 herziening): typical_company_size is niet meer
# gedefinieerd per sector. We werken met één conservatieve range die alle
# behandelaars-sectoren dekt: 1-persoons solo praktijken tot 15-person klinieken.
_FALLBACK_COMPANY_SIZE_RANGE = (1, 15)

# ICP-keyword-saturatie: bij dit aantal DISTINCTE keyword-matches telt de
# keyword-component volledig (0.35). Absoluut i.p.v. lijstlengte-afhankelijk —
# de oude `matches / (len(keywords)*0.3)` eiste 42+ matches bij een 137-woorden-
# sector en ~4 bij een 13-woorden-sector: sector-oneerlijk én overal te laag.
_KEYWORD_SATURATION = 5


def _collect_icp_signals(sector_config: dict) -> list[str]:
    """Flatten alle subcategory icp_signals + global lead_keywords tot één lijst."""
    signals: list[str] = list(sector_config.get("lead_keywords") or [])
    for sub in (sector_config.get("subcategories") or {}).values():
        signals.extend(sub.get("icp_signals") or [])
    return signals


def _parse_employee_count(lead: dict) -> int | None:
    """Leid een indicatief medewerker-aantal af uit de kolommen die ECHT bestaan
    (`kvk_employee_count_range`, `company_size_estimate`).

    De oude code las `employee_count`/`estimated_size` — phantom-kolommen die
    nergens geschreven worden → iedereen kreeg 'unknown size'. Returnt None als
    er geen parsebare data is; dan telt de size-component niet mee in de noemer.
    """
    import re
    for field in ("kvk_employee_count_range", "company_size_estimate"):
        raw = lead.get(field)
        if not raw:
            continue
        nums = re.findall(r"\d+", str(raw))
        if nums:
            # Ondergrens van de range als indicatie ("2-10" → 2, "1-5 mdw" → 1).
            return int(nums[0])
    return None


def compute_icp_match(lead: dict, sector_config: dict) -> dict:
    """Pure ICP-berekening — GEEN DB. Kern van `match_icp`; los testbaar en
    herbruikbaar in de rescore-dry-run.

    De denominator (`evaluable_max`) telt ALLEEN componenten mee die voor deze
    lead daadwerkelijk evalueerbaar zijn. SBI (KvK opt-in) en company-size worden
    overgeslagen als de benodigde data structureel ontbreekt, zodat een onmeetbaar
    signaal de score niet omlaag drukt. Dit is de kern-fix: voorheen was
    `max_score` altijd exact 1.0 (alle componenten onvoorwaardelijk opgeteld),
    dus delen door `max_score` was een no-op en de score capte rond 0.45.

    Returns:
        {"icp_match": float 0.0-1.0, "signals": list[str], "evaluable_max": float}
    """
    signals: list[str] = []
    score = 0.0
    max_score = 0.0

    company_text = _build_company_text(lead).lower()

    # --- Disqualifiers (instant disqualify) — ALLEEN sector-globaal ---
    # Subcategory-disqualifiers zijn DISAMBIGUATIE (hoort deze lead in déze
    # subcategorie?), GEEN sector-uitsluiting. Ze in de globale check meenemen
    # was de bug (2026-07-22): de schoonheidssalons-subcat had "medisch"/"arts"
    # om medische klinieken uit díe bucket te houden — maar globaal toegepast
    # disqualificeerde dat 51% van de cosmetische ICP (elke kliniek met
    # "medische behandelingen" in de Claude-samenvatting → icp_match 0). Alleen
    # sector-brede disqualifiers zijn echte non-ICP-uitsluitingen.
    disqualifiers: list[str] = list(sector_config.get("disqualifiers") or [])
    for kw in disqualifiers:
        if kw.lower() in company_text:
            return {"icp_match": 0.0, "signals": ["disqualified"], "evaluable_max": 0.0}

    # --- ICP keywords (0.35) — saturatie op absoluut aantal matches ---
    max_score += 0.35
    icp_keywords = _collect_icp_signals(sector_config)
    if icp_keywords:
        matches = sum(1 for kw in icp_keywords if kw.lower() in company_text)
        keyword_ratio = min(matches / _KEYWORD_SATURATION, 1.0)
        score += keyword_ratio * 0.35
        if matches:
            signals.append(f"keywords:{matches}")

    # --- KvK SBI match (0.20) — CONDITIONEEL: alleen als evalueerbaar ---
    # Kolom-fix: de enrichment schrijft `kvk_sbi_code` (niet het phantom
    # `sbi_code`). Alleen meetellen als de sector SBI-codes kent én de lead
    # SBI-data heeft — anders is het signaal onmeetbaar (KvK opt-in uit).
    sbi_codes = sector_config.get("sbi_codes") or []
    lead_sbi = (lead.get("kvk_sbi_code") or lead.get("sbi_code") or "").replace(".", "")
    if sbi_codes and lead_sbi:
        max_score += 0.20
        if any(code.startswith(lead_sbi) or lead_sbi.startswith(code) for code in sbi_codes):
            score += 0.20
            signals.append("sbi_match")

    # --- Company size fit (0.15) — CONDITIONEEL: alleen als parsebaar ---
    employee_count = _parse_employee_count(lead)
    if employee_count is not None:
        max_score += 0.15
        lo, hi = _FALLBACK_COMPANY_SIZE_RANGE
        if lo <= employee_count <= hi:
            score += 0.15
            signals.append("size_fit")

    # --- Google rating (0.15) — onvoorwaardelijk (afwezigheid = echt signaal) ---
    max_score += 0.15
    rating = lead.get("google_rating") or 0
    if rating >= 4.5:
        score += 0.15
        signals.append("high_rating")
    elif rating >= 4.0:
        score += 0.10
    elif rating >= 3.5:
        score += 0.05

    # --- Has website (0.10) — onvoorwaardelijk ---
    max_score += 0.10
    if lead.get("domain"):
        score += 0.10
        signals.append("has_website")

    # --- Has valid email (0.05) — onvoorwaardelijk ---
    max_score += 0.05
    if lead.get("email_status") in ("valid", "risky"):
        score += 0.05
        signals.append("has_email")

    icp_match = round(min(score / max_score, 1.0), 3) if max_score > 0 else 0.0
    return {"icp_match": icp_match, "signals": signals, "evaluable_max": round(max_score, 3)}


async def match_icp(
    lead_id: str,
    sector: str,
    workspace_id: str,
    supabase_client: Any,
) -> float:
    """
    Compute ICP match score for a lead and persist it to leads.icp_match.

    Thin wrapper rond `compute_icp_match` (pure): sector-config resolven, lead
    laden, berekenen, opslaan. Returns 0.0-1.0.
    """
    try:
        sector_config = get_sector(sector)
    except ValueError:
        logger.warning("icp_matcher: unknown sector %s", sector)
        return 0.0

    lead_res = supabase_client.table("leads").select("*").eq(
        "id", lead_id,
    ).eq("workspace_id", workspace_id).maybe_single().execute()

    if not lead_res.data:
        return 0.0

    result = compute_icp_match(lead_res.data, sector_config)
    icp_match = result["icp_match"]
    _store_icp_match(supabase_client, lead_id, icp_match)

    logger.info("icp_matcher: lead=%s sector=%s match=%.2f signals=%s",
                lead_id, sector, icp_match, result["signals"])
    return icp_match


def _build_company_text(lead: dict) -> str:
    """Build searchable text from all lead fields for keyword matching."""
    parts = [
        lead.get("company_name") or "",
        lead.get("company_summary") or "",
        lead.get("industry") or "",
        lead.get("google_category") or "",
        lead.get("company_positioning") or "",
    ]
    return " ".join(p for p in parts if p)


def _store_icp_match(supabase_client: Any, lead_id: str, icp_match: float) -> None:
    """Write ICP match score back to leads table."""
    try:
        supabase_client.table("leads").update({
            "icp_match": icp_match,
        }).eq("id", lead_id).execute()
    except Exception as e:
        logger.debug("Failed to store icp_match: %s", e)
