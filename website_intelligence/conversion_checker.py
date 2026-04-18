"""
website_intelligence/conversion_checker.py — Layer 3: Conversion analysis.

Checks CTA presence, booking flows, chat widgets, WhatsApp, contact forms.
Max 30 points per CLAUDE.md spec.
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Booking platforms to detect
_BOOKING_PLATFORMS = [
    "calendly.com", "acuityscheduling.com", "simplybook.me",
    "reservio.com", "setmore.com", "squareup.com/appointments",
    "tidycal.com", "zcal.co", "booksy.com", "treatwell.nl",
    "planyo.com",
]

# Chat widget scripts to detect
_CHAT_WIDGETS = {
    "Intercom": ["intercom.com", "intercomcdn.com"],
    "Drift": ["drift.com", "js.driftt.com"],
    "Tidio": ["tidio.co", "tidiochat.com"],
    "Landbot": ["landbot.io"],
    "Trengo": ["trengo.com", "trengo.eu"],
    "LiveChat": ["livechatinc.com"],
    "Zendesk": ["zopim.com", "zdassets.com"],
    "HubSpot Chat": ["js.hs-scripts.com"],
    "Crisp": ["crisp.chat"],
}


async def check_conversion(
    domain: str,
    page_html: str,
    sector: str,
    supabase_client: Any = None,
) -> dict[str, Any]:
    """
    Run all Layer 3 conversion checks on a page.

    Args:
        domain: Website domain.
        page_html: Full HTML of the homepage.
        sector: Sector key for sector-specific weighting.

    Returns dict with:
        has_cta_above_fold, cta_texts, has_phone_clickable, has_whatsapp,
        has_online_booking, booking_platform, has_chatbot, chatbot_platform,
        has_contact_form, form_field_count, conversion_score (0-30), details
    """
    result: dict[str, Any] = {
        "has_cta_above_fold": False,
        "cta_texts": [],
        "has_phone_clickable": False,
        "has_whatsapp": False,
        "has_online_booking": False,
        "booking_platform": None,
        "has_chatbot": False,
        "chatbot_platform": None,
        "has_contact_form": False,
        "form_field_count": 0,
        "conversion_score": 0,
        "details": [],
    }

    if not page_html:
        return result

    html_lower = page_html.lower()
    score = 0

    # --- CTA above fold (5 pts) ---
    # Heuristic: look for button/a elements with action words in first 5000 chars
    above_fold = html_lower[:5000]
    cta_patterns = [
        r"<(?:a|button)[^>]*>([^<]*(?:afspraak|boek|bel|contact|offerte|plan|start|gratis|probeer|aanvra)[^<]*)<",
        r"<(?:a|button)[^>]*>([^<]*(?:appointment|book|call|contact|quote|schedule|free|try|request)[^<]*)<",
    ]
    cta_texts = []
    for pattern in cta_patterns:
        matches = re.findall(pattern, above_fold, re.IGNORECASE)
        cta_texts.extend(m.strip() for m in matches if len(m.strip()) > 2)

    if cta_texts:
        result["has_cta_above_fold"] = True
        result["cta_texts"] = cta_texts[:5]
        score += 5
        result["details"].append({"check": "cta_above_fold", "passed": True, "value": cta_texts[:3]})
    else:
        result["details"].append({"check": "cta_above_fold", "passed": False})

    # --- Phone clickable (3 pts) ---
    if 'href="tel:' in html_lower or "href='tel:" in html_lower:
        result["has_phone_clickable"] = True
        score += 3
        result["details"].append({"check": "phone_clickable", "passed": True})
    else:
        result["details"].append({"check": "phone_clickable", "passed": False})

    # --- WhatsApp (4 pts) ---
    whatsapp_patterns = ["wa.me/", "api.whatsapp.com", "whatsapp.com/send", "whatsapp-widget"]
    if any(p in html_lower for p in whatsapp_patterns):
        result["has_whatsapp"] = True
        score += 4
        result["details"].append({"check": "whatsapp", "passed": True})
    else:
        result["details"].append({"check": "whatsapp", "passed": False})

    # --- Online booking (6 pts) ---
    for platform in _BOOKING_PLATFORMS:
        if platform.lower() in html_lower:
            result["has_online_booking"] = True
            result["booking_platform"] = platform.split(".")[0].capitalize()
            score += 6
            result["details"].append({"check": "online_booking", "passed": True, "value": result["booking_platform"]})
            break
    else:
        # Check for generic booking indicators
        booking_keywords = ["booking", "reserveren", "afspraak maken", "boek nu", "plan je afspraak"]
        if any(kw in html_lower for kw in booking_keywords):
            result["has_online_booking"] = True
            result["booking_platform"] = "custom"
            score += 6
            result["details"].append({"check": "online_booking", "passed": True, "value": "custom"})
        else:
            result["details"].append({"check": "online_booking", "passed": False})

    # --- Chatbot / live chat (4 pts) ---
    for platform, patterns in _CHAT_WIDGETS.items():
        if any(p.lower() in html_lower for p in patterns):
            result["has_chatbot"] = True
            result["chatbot_platform"] = platform
            score += 4
            result["details"].append({"check": "chatbot", "passed": True, "value": platform})
            break
    else:
        result["details"].append({"check": "chatbot", "passed": False})

    # --- Contact form (3 pts, max 5 fields) ---
    form_count = html_lower.count("<form")
    if form_count > 0:
        result["has_contact_form"] = True
        # Count input fields in the page
        input_count = len(re.findall(r"<(?:input|select|textarea)", html_lower))
        result["form_field_count"] = input_count
        if input_count <= 5:
            score += 3
            result["details"].append({"check": "contact_form", "passed": True, "value": f"{input_count} fields"})
        else:
            score += 1  # Has form but too many fields
            result["details"].append({"check": "contact_form", "passed": True, "value": f"{input_count} fields (>5)"})
    else:
        result["details"].append({"check": "contact_form", "passed": False})

    # --- CTA text strength (5 pts) — scored based on specificity ---
    if cta_texts:
        # Simple heuristic: specific > generic
        strong_ctas = [t for t in cta_texts if any(w in t.lower() for w in
                       ["gratis", "vrijblijvend", "offerte", "afspraak", "waardebepaling", "kennismak"])]
        if strong_ctas:
            score += 5
            result["details"].append({"check": "cta_strength", "passed": True, "value": strong_ctas[:2]})
        else:
            score += 2  # Generic CTA present
            result["details"].append({"check": "cta_strength", "passed": True, "value": "generic"})

    result["conversion_score"] = min(score, 30)
    return result
