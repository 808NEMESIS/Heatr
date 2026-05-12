"""
utils/lead_naming.py — Single source of truth voor "veilige first_name extraction".

Live bug: contact_discovery._infer_contact_from_email parsde
'ceciledebooij@gmail.com' → first_name='Ceciledebooij'. Mail 1 begon met
'Hoi Ceciledebooij' — onmiddellijke 'dit is bot'-trigger.

Deze helper detecteert die patronen en retourneert een veilige string of "".
Wordt gebruikt door:
  - integrations/warmr_client.py (Warmr payload)
  - campaigns/sequence_engine.inject_variables (Mail body rendering)
"""
from __future__ import annotations

import re


_CONFIDENCE_RE = re.compile(r"confidence:\s*(\d+)\s*%", re.IGNORECASE)
_MIN_GREETING_CONFIDENCE = 30


def extract_contact_confidence(lead: dict) -> int:
    """Parse confidence-percentage uit lead.contact_why_chosen.

    Format zoals geschreven door enrichment/contact_discovery.py:
      'Plastisch Chirurg bij <Bedrijf>, gevonden op teampagina (confidence: 5%)'

    Returns:
        Integer 0-100. Als geen confidence-string aanwezig (oudere leads,
        handmatige imports), retourneer 100 — fail-open, niet onderdrukken.
    """
    why = lead.get("contact_why_chosen") or ""
    m = _CONFIDENCE_RE.search(why)
    if not m:
        return 100
    try:
        return max(0, min(100, int(m.group(1))))
    except (TypeError, ValueError):
        return 100


def safe_first_name(lead: dict) -> str:
    """Return een veilige first_name voor outbound, of "" als geen betrouwbare keuze.

    Regels (eerste match wint):
      1. Lege/None contact_first_name → ""
      2. first_name == email-local-part (case-insensitive) → "" (legacy email-inference)
      3. first_name >12 chars zonder spatie → "" (waarschijnlijk samengevoegd)
      4. Anders: behoud as-is
    """
    raw = (lead.get("contact_first_name") or "").strip()
    if not raw:
        return ""
    email = (lead.get("email") or "").strip()
    if "@" in email:
        local = email.split("@")[0]
        if raw.lower() == local.lower():
            return ""
    if len(raw) > 12 and " " not in raw:
        return ""
    return raw


def display_first_name(lead: dict, fallback: str = "daar") -> str:
    """Voor mail-greeting: 'Hoi {{first_name}}'. Geeft fallback als safe leeg is.

    Confidence-gate (Test 1' bevinding 2026-05-06): contact_discovery vist
    soms een receptionist/scrum-master uit de teampagina (bv. 'Tallechien
    Tempelman', 5% confidence) terwijl de echte beslisser ('Drs. Khoe',
    plastisch chirurg) in contact_name staat. 'Hoi Tallechien' boven een
    chirurg-tone'd opener leest als bot. Daarom: confidence < 30% → fallback.

    Caller kan tussen 'daar' (informeel) en '' (forceert template-fallback) kiezen.
    """
    if extract_contact_confidence(lead) < _MIN_GREETING_CONFIDENCE:
        return fallback
    safe = safe_first_name(lead)
    return safe or fallback
