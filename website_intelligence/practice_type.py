"""
website_intelligence/practice_type.py — praktijktype-indicatie voor fit-weging.

Aanleiding (Sami's live-check, 2026-07-22): drie van vijf "stuurbare" testrun-
leads bleken schoonheidssalons/huidinstituten of een eenmanszaak-op-gmail, geen
medisch-esthetische klinieken — precies de randgevallen die niet ongemarkeerd
als founding-referentiecase mogen dienen.

Dit is een INDICATIE, geen gate: de classificatie sluit niets automatisch uit,
maar maakt het onderscheid zichtbaar op de beoordelingskaart (Sami beslist per
geval). Deterministisch en transparant: elke uitspraak komt met de gematchte
signalen.
"""
from __future__ import annotations

import re

# Freemail-domeinen: sterk signaal voor eenmanszaak (weegt mee als vlag).
FREEMAIL_DOMAINS = {
    "gmail.com", "hotmail.com", "hotmail.nl", "outlook.com", "outlook.nl",
    "live.nl", "live.com", "icloud.com", "yahoo.com", "yahoo.nl",
    "ziggo.nl", "kpnmail.nl", "casema.nl", "planet.nl", "home.nl",
}

# Woordgrens-regexes: "salon" mag niet op "salonized" matchen.
_SALON_SIGNALS = [
    r"\bschoonheidssalon\b", r"\bbeautysalon\b", r"\bbeauty\s?salon\b",
    r"\bschoonheidsspecialist(?:e)?\b", r"\bhuidinstituut\b", r"\bsalon\b",
    r"\bbeautystudio\b", r"\bpedicure\b", r"\bmanicure\b", r"\bwimper",
    r"\bnagelstudio\b", r"\bgezichtsbehandeling",
]
_KLINIEK_SIGNALS = [
    r"\bkliniek\b", r"\bclinic\b", r"\bmedisch[\-\s]?esthetisch",
    r"\bcosmetisch(?:e)? arts", r"\bplastisch(?:e)? chirurg", r"\bbig[\-\s]?geregistreerd",
    r"\barts(?:en)?\b", r"\binjectables?\b", r"\bbotox\b", r"\bfillers?\b",
    r"\bdermatolo", r"\bhaartransplant", r"\bchirurgie\b", r"\blaserkliniek\b",
]


def _matches(patterns: list[str], text: str) -> list[str]:
    return [p.strip("\\b") for p in patterns if re.search(p, text, re.IGNORECASE)]


# --- Praktijkomvang (Sami 2026-07-23): gevestigde speler = buiten founding -----
_SIZE_PATTERNS = {
    "meerdere_vestigingen": r"\b(?:meerdere |onze |verschillende )?(?:vestiging(?:en)?|locaties|filialen)\b|vestiging(?:en)? in .{0,40}\b(?:en|&)\b",
    "team_specialisten": r"team van (?:artsen|specialisten|professionals)|ons (?:deskundig[e]? )?team|multidisciplinair",
    "keurmerk": r"\bzkn\b|iso[\s-]?900|keurmerk|geaccrediteerd|gecertificeerd|nvcg|geregistreerd bij",
    "media": r"bekend van (?:tv|rtl|sbs)|zoals gezien op|in de media verschenen|\brtl\s?\d\b|\bsbs\s?\d\b|geïnterviewd|reportage",
}


def assess_practice_size(text: str, current_year: int = 2026) -> dict:
    """Fit-signaal praktijkomvang: gevestigde speler (meerdere vestigingen, team,
    keurmerken, media, lang bestaand) hoort waarschijnlijk NIET in het founding-
    aanbod (te groot, heeft vaak al een bureau). Markering, geen gate — mens
    beslist. Amstelzijde-fixture: multidisciplinair team, opgericht 2009."""
    t = (text or "").lower()
    signals = [name for name, pat in _SIZE_PATTERNS.items() if re.search(pat, t, re.IGNORECASE)]
    established_year = None
    m = re.search(r"(?:opgericht|sinds|gevestigd)[^\d]{0,12}(20\d{2}|19\d{2})", t)
    if m:
        yr = int(m.group(1))
        if yr <= current_year - 8:  # 8+ jaar = gevestigd
            established_year = yr
            signals.append("lang_bestaand")
    likely = len(signals) >= 2 or "meerdere_vestigingen" in signals or "media" in signals
    label = ("gevestigde speler (buiten founding?): " + ", ".join(signals)) if likely else "kleinschalig / onbepaald"
    return {"signals": signals, "established_year": established_year,
            "likely_established_player": likely, "label": label}


_TITLE_RE = re.compile(
    r"\b(prof\.?\s*dr\.?|prof\.?|dr\.?|drs\.?|ir\.?|mr\.?|ing\.?|dokter)\b", re.IGNORECASE)


def suggest_aanhef_register(name_context: str | None) -> dict:
    """Aanhef-register (Sami 2026-07-23): bij een BIG-arts/oprichter met titel is
    'Hoi {voornaam}' te familiair voor koude NL-outreach. Detecteer de titel en
    stel een formeler register vóór — mens beslist. Geen automatische wijziging."""
    ctx = name_context or ""
    m = _TITLE_RE.search(ctx)
    if not m:
        return {"register": "informeel", "title": None, "note": ""}
    title = re.sub(r"\s+", " ", m.group(1).strip())
    return {"register": "formeel", "title": title,
            "note": f"titel '{title}' gedetecteerd — overweeg formele aanhef (Beste dr./mevr. + achternaam) i.p.v. Hoi voornaam"}


def classify_practice_type(
    company_name: str | None,
    company_summary: str | None = None,
    email: str | None = None,
) -> dict:
    """Classificeer het praktijktype op naam + samenvatting + e-maildomein.

    Returns:
        {
          "practice_type": "medisch_esthetisch" | "salon_huidinstituut"
                           | "gemengd" | "onbepaald",
          "kliniek_signals": [..], "salon_signals": [..], "flags": [..],
          "label": korte NL-weergave voor de beoordelingskaart,
        }
    """
    text = f"{company_name or ''} {company_summary or ''}"
    kliniek = _matches(_KLINIEK_SIGNALS, text)
    salon = _matches(_SALON_SIGNALS, text)

    flags: list[str] = []
    domain = (email or "").rsplit("@", 1)[-1].lower().strip()
    if domain in FREEMAIL_DOMAINS:
        flags.append(f"freemail-adres ({domain}) — eenmanszaak-signaal")

    if kliniek and not salon:
        ptype = "medisch_esthetisch"
    elif salon and not kliniek:
        ptype = "salon_huidinstituut"
    elif salon and kliniek:
        # beide aanwezig: naam weegt zwaarder dan samenvatting — "Huidinstituut X"
        # met botox-aanbod blijft een huidinstituut als referentiecase.
        name_salon = _matches(_SALON_SIGNALS, company_name or "")
        name_kliniek = _matches(_KLINIEK_SIGNALS, company_name or "")
        if name_salon and not name_kliniek:
            ptype = "salon_huidinstituut"
        elif name_kliniek and not name_salon:
            ptype = "gemengd"
        else:
            ptype = "gemengd"
    else:
        ptype = "onbepaald"

    labels = {
        "medisch_esthetisch": "medisch-esthetische kliniek",
        "salon_huidinstituut": "schoonheidssalon / huidinstituut (zwakkere fit)",
        "gemengd": "gemengd profiel (kliniek- én salon-signalen)",
        "onbepaald": "praktijktype onbepaald",
    }
    label = labels[ptype]
    if flags:
        label += " · " + flags[0]
    return {"practice_type": ptype, "kliniek_signals": kliniek[:5],
            "salon_signals": salon[:5], "flags": flags, "label": label}
