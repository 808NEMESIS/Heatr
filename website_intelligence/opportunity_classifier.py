"""
website_intelligence/opportunity_classifier.py — Classify service opportunities.

Determines which Aerys services to pitch based on website analysis scores.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def classify_opportunities(
    total_score: int,
    technical_result: dict,
    conversion_result: dict,
    sector_result: dict,
    visual_score: int | None = None,
) -> dict[str, Any]:
    """
    Classify which services to pitch based on website analysis.

    Returns:
        {
            "opportunity_types": ["website_rebuild", "conversie_optimalisatie", ...],
            "priority": "urgent" | "high" | "medium" | "low",
            "reasons": {"website_rebuild": "score < 40", ...},
        }
    """
    types: list[str] = []
    reasons: dict[str, str] = {}

    cms = technical_result.get("cms")
    conversion_score = conversion_result.get("conversion_score", 0)

    # --- Website rebuild ---
    if total_score < 40:
        types.append("website_rebuild")
        reasons["website_rebuild"] = f"Totaal score {total_score}/100 — onder 40"
    elif visual_score is not None and visual_score < 4:
        types.append("website_rebuild")
        reasons["website_rebuild"] = f"Visuele score {visual_score}/10 — onder 4"

    # --- Conversie optimalisatie ---
    if total_score >= 40 and conversion_score < 15:
        types.append("conversie_optimalisatie")
        reasons["conversie_optimalisatie"] = f"Conversie score {conversion_score}/30 — onder 15"
    if not conversion_result.get("has_whatsapp"):
        if "conversie_optimalisatie" not in types:
            types.append("conversie_optimalisatie")
            reasons.setdefault("conversie_optimalisatie", "Geen WhatsApp integratie")
    if not conversion_result.get("has_online_booking"):
        if "conversie_optimalisatie" not in types:
            types.append("conversie_optimalisatie")
            reasons.setdefault("conversie_optimalisatie", "Geen online booking")

    # --- Chatbot ---
    if not conversion_result.get("has_chatbot"):
        types.append("chatbot")
        reasons["chatbot"] = "Geen chatbot of live chat gedetecteerd"

    # --- AI audit (always after website conversation) ---
    types.append("ai_audit")
    reasons["ai_audit"] = "Standaard aanbeveling na websitegesprek"

    # --- Priority ---
    if total_score < 30:
        priority = "urgent"
    elif total_score < 50:
        priority = "high"
    elif len(types) >= 3:
        priority = "medium"
    else:
        priority = "low"

    return {
        "opportunity_types": types,
        "priority": priority,
        "reasons": reasons,
    }
