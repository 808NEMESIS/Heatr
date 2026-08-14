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
# ── Boekdetectie: TWEE gescheiden soorten ────────────────────────────────────
# KIND 1 — externe boekPLATFORMS (domein-substring). Internationaal + NL-markt.
# Zorgdomein/Zorgmail BEWUST NIET: dat zijn verwijs- (huisarts→specialist) en
# beveiligde-berichtenplatforms, geen publieke patiënt-zelf-boekwidget (Sami 2026-08-12).
_BOOKING_PLATFORMS = [
    # internationaal (bestaand)
    "calendly.com", "acuityscheduling.com", "simplybook.me",
    "reservio.com", "setmore.com", "squareup.com/appointments",
    "tidycal.com", "zcal.co", "booksy.com", "treatwell.nl",
    "planyo.com",
    # NL-markt (inventaris meetrapport 2026-08-12; crossuite/clientomgeving gemeten)
    "crossuite.com", "clientomgeving.nl", "onlineafspraken.nl",
    "supersaas.com", "supersaas.nl", "skedify.", "timify.com",
    "salonized.com", "appointer.", "boekjeafspraak.",
    # handcheck-gaten (Sami 2026-08-13): kliniek-EPD's met publieke boekflow
    "clinicminds.com", "mijndiad.nl",
]

# KIND 2 — tekst-/URL-patronen (self-hosted boekpagina's + NL-knopteksten). PATROON,
# geen exacte string: "Maak een afspraak" / "afspraak maken" / "afspraak-maken" raken alle.
# Alleen href-waarden en klikbare elementen tellen — GEEN losse zinstekst (vals-positief-gate).
_BOOKING_URL_RE = re.compile(
    r'(?:href|src)=["\'][^"\']*'
    # [/-]boeken vangt /boeken én -boeken (bv. /nl/online-boeken) — patroon, geen exacte string
    r'(?:[/-]boeken|/boek-afspraak|/afspraak-maken|/afspraak-inplannen|/online-afspraak'
    r'|/reserveren|/plan-afspraak|afspraak-maken|maak-een-afspraak|clientomgeving\.nl)'
    r'[^"\']*["\']', re.I)
# Knopteksten: lidwoord optioneel ("Maak Afspraak" — MIDI) en geneste elementen
# tussen de tag en de tekst toegestaan (<a><span>Maak een afspraak</span></a> —
# fusion/elementor-thema's). Vereist wél een a/button-context: losse zinstekst
# ("u kunt een afspraak maken") telt nooit (Laarman-guardrail).
_BOOKING_ANCHOR_RE = re.compile(
    r'<(?:a|button)[^>]*>(?:\s*<[^>]+>)*\s*[^<]{0,40}?'
    r'(?:maak (?:een |je )?afspraak|afspraak maken|boek (?:direct|nu|hier|een afspraak|online|meteen)'
    r'|online (?:een )?afspraak|afspraak inplannen|plan (?:een|je) afspraak|reserveer nu)',
    re.I)
# WhatsApp-boekpad (Osteopathie Anna: wa.me onder de hero) — APART veld, flipt
# has_online_booking niet: appen is een boekPAD, geen online-boeksysteem.
_WHATSAPP_LINK_RE = re.compile(r"wa\.me/|api\.whatsapp\.com|whatsapp://", re.I)


def detect_booking(page_html: str):
    """Return (has_booking, platform_label, evidence). Houdt 'boeking aanwezig' (de CLAIM)
    los van 'welk platform' (CONTEXT). Prioriteit: bekend platform → self-hosted boek-URL →
    boekKNOP in een klikbaar element. Losse zinstekst ('maak een afspraak' in een alinea)
    telt niet — dat voorkomt vals-positieven op contactpagina's."""
    low = page_html.lower()
    for platform in _BOOKING_PLATFORMS:
        if platform.lower() in low:
            return True, platform.split("/")[0].split(".")[0].capitalize(), platform
    m = _BOOKING_URL_RE.search(page_html)
    if m:
        return True, "self-hosted", m.group(0)[:90]
    m = _BOOKING_ANCHOR_RE.search(page_html)
    if m:
        return True, "custom", re.sub(r"\s+", " ", m.group(0))[:90]
    return False, None, None

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
    # Expliciete tap-link (wa.me/nummer) = een BOEKPAD (Osteopathie Anna: "Snel een
    # afspraak? App ..."). Apart veld naast has_whatsapp (dat ook widgets telt) en
    # naast has_online_booking (appen is geen online-boeksysteem, flipt die niet).
    result["has_whatsapp_link"] = bool(_WHATSAPP_LINK_RE.search(page_html))
    if any(p in html_lower for p in whatsapp_patterns):
        result["has_whatsapp"] = True
        score += 4
        result["details"].append({"check": "whatsapp", "passed": True})
    else:
        result["details"].append({"check": "whatsapp", "passed": False})

    # --- Online booking (6 pts) ---
    # 'boeking aanwezig' (de claim) los van 'welk platform' (context) — zie detect_booking.
    has_booking, booking_label, booking_evidence = detect_booking(page_html)
    if has_booking:
        result["has_online_booking"] = True
        result["booking_platform"] = booking_label
        score += 6
        result["details"].append({"check": "online_booking", "passed": True,
                                   "value": booking_label, "evidence": booking_evidence})
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
    # RECOVERY-FIX: tel velden PER <form>, niet pagina-breed. Voorheen telde dit
    # alle inputs op de pagina (zoekbalk + nieuwsbrief + filters) → het echte
    # contactformulier werd onterecht als ">5 velden" gestraft. We kiezen het
    # meest waarschijnlijke contactformulier (bevat email/textarea), anders het
    # kleinste form (vermijdt straf op een groot zoek/filter-form).
    forms = re.findall(r"<form\b[^>]*>.*?</form>", html_lower, re.DOTALL)
    if forms:
        result["has_contact_form"] = True

        def _field_count(form_html: str) -> int:
            return len(re.findall(r"<(?:input|select|textarea)", form_html))

        contact_forms = [f for f in forms if ('type="email"' in f or "<textarea" in f)]
        chosen = max(contact_forms, key=_field_count) if contact_forms else min(forms, key=_field_count)
        input_count = _field_count(chosen)
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

    # --- Booking system classification (for Warmr Sequence v1.0, Mail 2) ---
    # Enum: "online" | "contact-form-only" | "phone-only" | "unknown"
    # Priorities matcht de ladder waarop de mail reageert: online boeking wint
    # altijd; anders contactformulier; anders telefoon; anders onbekend.
    if result.get("has_online_booking"):
        result["booking_system"] = "online"
    elif result.get("has_contact_form"):
        result["booking_system"] = "contact-form-only"
    elif result.get("has_phone_clickable"):
        result["booking_system"] = "phone-only"
    else:
        result["booking_system"] = "unknown"

    return result
