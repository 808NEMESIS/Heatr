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
    r"welkom|website|home|"
    # titels die als 'voornaam' doorglippen als de titel-strip faalt (name-sweep
    # 2026-08-20: extractor leverde letterlijk 'Dokter' als first_name)
    r"dokter|dr|drs|prof|arts|mevrouw|meneer|mevr|dhr)$", re.IGNORECASE)
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
        # Local-part-regel: blokkeert legacy e-mail-inferentie ('Ceciledebooij').
        # UITZONDERING (2026-08-19): een naam die aantoonbaar ONAFHANKELIJK van de
        # e-mail is gevonden (name-sweep-marker in why_chosen: teampagina/bedrijfsnaam,
        # resolver gebruikt nooit e-mail) én toevallig de local-part matcht
        # (voornaam@domein) is juist de stérkste bevestiging — niet onderdrukken.
        independent = "[name-sweep" in (lead.get("contact_why_chosen") or "")
        if raw.lower() == local.lower() and not independent:
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


# SEO-title-vervuiling in company_name (2026-07-24): "NAAM | Descriptor | ..." —
# een title-tag als bedrijfsnaam is de sterkste geautomatiseerd-tell. Splits op de
# eerste separator (pipe/bullet of spatie-omringd streepje/dubbelepunt).
_COMPANY_SEP_RE = re.compile(r"\s*[|•·»«]\s*|\s+[-–—:]\s+")
_COMPANY_WEIRD_RE = re.compile(r"[|•·»«/\\@]|\s{2,}")


def clean_company_name(raw: str | None) -> tuple[str, bool]:
    """Schoon de bedrijfsnaam van SEO-title-vervuiling. Returned (naam, needs_review).

    Neemt het deel vóór de eerste pipe/spatie-streepje/dubbelepunt. needs_review=True
    → nog steeds title-tag-achtig (>4 woorden of rare tekens) óf leeg → handmatige
    correctie nodig, GEEN mail met een title-tag als naam (Sami 2026-07-24)."""
    if not raw or not raw.strip():
        return ("", True)
    name = _COMPANY_SEP_RE.split(raw.strip(), maxsplit=1)[0].strip()
    if not name:
        return (raw.strip(), True)
    needs_review = bool(_COMPANY_WEIRD_RE.search(name)) or len(name.split()) > 4
    return (name, needs_review)


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
