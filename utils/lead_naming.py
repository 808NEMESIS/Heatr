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

# Junk-'namen' (sample-bevinding 2026-07-24): generieke woorden i.p.v. een naam.
_GENERIC_NAME_RE = re.compile(
    r"^(afspraak|contact|informatie|info|kliniek|clinic|praktijk|receptie|balie|"
    r"admin|noreply|no-reply|team|mail|e-?mail|hallo|hello|webshop|order|orders|"
    r"sales|support|aanmelden|nieuwsbrief|boeking|booking|klantenservice|"
    r"welkom|website|home)$", re.IGNORECASE)
# Een echte voornaam bevat geen business-/sectorwoord. Vangt domein-/merkfragmenten
# als 'Glowclinicnl' zonder eponieme namen (Joost, Frodo, Liem) te raken.
_BUSINESS_WORD_RE = re.compile(
    r"clinic|kliniek|skin|beauty|laser|huid|praktijk|studio|medisch|cosmetic|"
    r"aesthetic|esthetic|derma|salon|\.nl|\.com|\.be", re.IGNORECASE)


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
    # Junk-namen afvangen (sample-bevinding 2026-07-24): liever geen naam dan een
    # neppe. Een lege return laat de greeting terugvallen op 'Hallo,'.
    if raw.endswith(".") or len(raw) <= 1:
        return ""                                    # initiaal ('A.') / te kort
    if any(c.isdigit() for c in raw):
        return ""                                    # bevat cijfers
    if _GENERIC_NAME_RE.match(raw):
        return ""                                    # generiek woord ('Afspraak')
    if _BUSINESS_WORD_RE.search(raw):
        return ""                                    # domein-/merkfragment ('Glowclinicnl')
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
