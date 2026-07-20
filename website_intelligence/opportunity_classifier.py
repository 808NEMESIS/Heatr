"""
website_intelligence/opportunity_classifier.py — Classify service opportunities.

Determines which Aerys services to pitch based on website analysis scores.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Offer-type → categorie voor de sector-poort (allowed_offers). 'chatbot' = een
# on-site AI-chat die op een medische/pijn-site patiëntvragen afhandelt → valt
# bewust onder 'automatisering' (compliance-risico), dus geblokkeerd voor alt-
# zorg/chiro. 'ai_audit' = de automatiserings-upsell.
_OFFER_CATEGORY: dict[str, str] = {
    "website_rebuild": "website_rebuild",
    "conversie_optimalisatie": "conversie_optimalisatie",
    "chatbot": "automatisering",
    "ai_audit": "automatisering",
}


def classify_opportunities(
    total_score: int,
    technical_result: dict,
    conversion_result: dict,
    sector_result: dict,
    visual_score: int | None = None,
    sector: str | None = None,
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

    # --- Sector-poort (2026-07-20): nooit een offer buiten allowed_offers ---
    # cosmetiek = volledige funnel (incl. automatisering); alt-zorg/chiro = alleen
    # website. Onbekende sector → website-only (veilige default).
    from config.sectors import get_allowed_offers
    allowed = get_allowed_offers(sector)
    kept = [t for t in types if _OFFER_CATEGORY.get(t, "automatisering") in allowed]
    for t in [x for x in types if x not in kept]:
        reasons.pop(t, None)
    # Site sterk (weinig website-pijn) bij een website-only-sector → val terug op
    # de sterkste TOEGESTANE website-angle i.p.v. door te schuiven naar
    # automatisering (dan zou de lijst leeg blijven omdat chatbot/ai_audit eruit
    # gefilterd zijn).
    if not kept and allowed:
        fb = "conversie_optimalisatie" if "conversie_optimalisatie" in allowed else "website_rebuild"
        kept = [fb]
        reasons[fb] = "Sterkste toegestane website-angle (geen automatisering voor deze sector)"
    types = kept

    # --- Priority (op de gefilterde lijst) ---
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
