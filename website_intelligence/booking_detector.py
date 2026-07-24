"""
website_intelligence/booking_detector.py — betrouwbare online-boeken-detectie
(remediation C3).

DE CANONIEKE DEFINITIE (besluit 2026-07-22, na Sami's KVEG/Amstelzijde-
inconsistentie): "online kunnen boeken" = er bestaat een online ingang mét
AFSPRAAK-INTENTIE. Twee soorten, allebei tellen ze:
  - `self_booking`  — echte zelf-boek-widget/agenda waar de cliënt zelf een
    slot kiest (platform-domein, boek-iframe).
  - `request_form`  — aanvraag-pad: consult-/afspraak-aanvraagformulier of
    terugbelverzoek ("bel mij terug"). De cliënt kiest geen slot, maar er ís
    een online afspraak-ingang.
Signaal 1 ("kwam alleen een telefoonnummer tegen") mag ALLEEN overeind blijven
als GEEN van beide bestaat — anders is de zin aantoonbaar onwaar. Een
GENERIEKE contactpagina zonder afspraak-intentie (geen afspraak-/consult-/
terugbel-taal) telt NIET als boekingang — anders vervalt elke lead, want
iedere site heeft een contactpagina. KVEG (bel-mij-terug-formulier) en
Amstelzijde (consult-plannen-formulier) krijgen onder deze definitie hetzelfde
oordeel: has_booking/request_form.

Verder:
  - kijkt naar platform-domeinen, iframes, links (href/aria-label/tekst),
    scripts én NL-keyword-CTA's — niet alleen zichtbare platte tekst;
  - normaliseert HTML vóór matching;
  - geeft een CONFIDENCE-model terug: een negatieve conclusie
    ('no_booking') is ALLEEN toegestaan bij een geslaagde, volledige fetch
    en géén enkele indicator. Bij fetch-fout of te weinig HTML → 'unknown'
    (nooit 'no_booking').

Puur + testbaar: geen I/O. De fetch (httpx + Playwright-fallback) leeft in
de caller (conversion_checker / A3-verificatie / hook_detector).
"""
from __future__ import annotations

import re

# Bekende boekingsplatformen (domein-fragmenten in href/iframe/script src).
BOOKING_PLATFORMS = [
    "calendly.com", "acuityscheduling.com", "app.acuityscheduling",
    "simplybook.me", "simplybook.it", "reservio.com", "setmore.com",
    "squareup.com/appointments", "book.squareup", "tidycal.com", "zcal.co",
    "booksy.com", "treatwell.nl", "treatwell.com", "salonized.com",
    "planyo.com", "doctena.nl", "doctena.com", "afspraken.nl",
    "onlineafspraken.nl", "zorgdomein.nl", "efysic.nl", "spikke.nl",
    "abclick.nl", "cal.com", "youcanbook.me", "timify.com",
    "appointmentplus", "meetings.hubspot.com",
    # testrun 2026-07-22: aeverclinics.zenoti.com miste → vals "geen boeking"
    "zenoti.com",
    # Q4-steekproef 2026-07-24: doctorsatsoap gebruikte clinicminds (NL cosmetisch
    # boekplatform) → vals Q4/Q2-hit. Toegevoegd aan de canonieke lijst.
    "clinicminds",
]

# NL + EN keyword-varianten die op een boekings-CTA wijzen. Woordgrens-match.
_BOOKING_KEYWORDS = [
    r"maak (?:een )?afspraak", r"afspraak maken", r"boek (?:een )?afspraak",
    r"online (?:af)?boeken", r"online afspraak", r"direct boeken",
    r"plan (?:een )?consult", r"plan (?:een )?afspraak", r"reserveer",
    r"reserveren", r"afspraak inplannen", r"inplannen", r"book (?:an )?appointment",
    r"book now", r"schedule (?:an )?appointment", r"maak online een afspraak",
    r"afspraak online",
    # testrun 2026-07-22: drie bewezen missers — "nu boeken"-knoppen (Monalisa),
    # "consult aanvragen" (Hairworld) en "consult plannen" (Amstelzijde) gaven
    # vals "geen boeking" → een onware signaal-1-mail.
    r"nu boeken", r"consult (?:aanvragen|plannen)", r"consultatie (?:aanvragen|plannen)",
    # KVEG-fix (2026-07-22): terugbel-/aanvraag-paden zijn óók een online
    # afspraak-ingang (request_form) — "bel mij terug" werd gemist waardoor
    # KVEG en Amstelzijde tegengesteld beoordeeld werden.
    r"bel (?:mij|me) terug", r"terugbelverzoek", r"terugbelafspraak",
    r"wij bellen (?:u|je) terug", r"(?:afspraak|consult|behandeling) aanvragen",
    r"vraag (?:een )?(?:afspraak|consult|behandeling) aan",
    r"request a callback", r"call (?:me|you) back",
]
_BOOKING_KW_RE = re.compile("|".join(_BOOKING_KEYWORDS), re.IGNORECASE)

# href-fragmenten die op een boek-bestemming wijzen. WOORDGRENS-match: een bare
# substring "book" matchte ook "facebook.com" → elke kliniek met een Facebook-
# link kreeg vals 'has_booking' (bewezen 2026-07-22 op hasci.nl / echthaar.nl).
# `\bbook` matcht wél "/booking", "book-now", "booksy.com", maar NIET "facebook".
# `consult` toegevoegd (testrun 2026-07-22): /consult-aanvragen/ en
# /consultatie-form/ zijn afspraak-paden die anders gemist werden.
_BOOK_HREF_RE = re.compile(r"\b(?:book|boek|afspraak|appoint|reserv|consult)", re.IGNORECASE)

# Minimale HTML-lengte om een 'no_booking' te durven concluderen.
_MIN_HTML_FOR_NEGATIVE = 500


def _norm(html: str) -> str:
    return re.sub(r"\s+", " ", (html or "")).lower()


def detect_booking(html: str | None, *, fetch_ok: bool) -> dict:
    """Detecteer online-boeken in HTML.

    Args:
        html: ruwe (bij voorkeur JS-gerenderde) HTML.
        fetch_ok: True als de fetch geslaagd is en volledige HTML gaf.

    Returns:
        {value: 'has_booking'|'no_booking'|'unknown',
         confidence: 'high'|'low',
         evidence: [str, ...],
         platform: str|None,
         kind: 'self_booking'|'request_form'|None}

    `kind` (definitie-besluit 2026-07-22): self_booking = widget/agenda waar de
    cliënt zelf een slot kiest (platform/iframe); request_form = afspraak-
    intentie-pad zonder slot-keuze (aanvraag-/terugbelformulier, boek-link —
    kan op een subpagina alsnog een widget zijn, maar dat is vanaf de homepage
    niet vast te stellen; voor signaal 1 maakt het niet uit: beide blokkeren).
    """
    text = _norm(html)

    # 1. Platform-domein in href/iframe/script src of waar dan ook in de HTML.
    for plat in BOOKING_PLATFORMS:
        if plat in text:
            return {"value": "has_booking", "confidence": "high", "kind": "self_booking",
                    "evidence": [f"platform:{plat}"], "platform": plat}

    # 2. Iframe/widget src die op boeken wijst (zonder bekend platform).
    for m in re.finditer(r'<iframe[^>]+src=["\']([^"\']+)["\']', text):
        src = m.group(1)
        if any(w in src for w in ("book", "afspraak", "appoint", "reserv", "calendar", "schedule")):
            return {"value": "has_booking", "confidence": "high", "kind": "self_booking",
                    "evidence": [f"iframe:{src[:80]}"], "platform": None}

    # 3. Link met href/aria-label/tekst die op boeken wijst.
    for m in re.finditer(r'<a\b[^>]*>.*?</a>', text):
        chunk = m.group(0)
        href = re.search(r'href=["\']([^"\']*)["\']', chunk)
        aria = re.search(r'aria-label=["\']([^"\']*)["\']', chunk)
        hay = " ".join(x.group(1) for x in (href, aria) if x) + " " + re.sub(r"<[^>]+>", " ", chunk)
        if _BOOKING_KW_RE.search(hay) or (href and _BOOK_HREF_RE.search(href.group(1))):
            return {"value": "has_booking", "confidence": "high", "kind": "request_form",
                    "evidence": [f"link:{hay.strip()[:80]}"], "platform": None}

    # 4. Button of losse CTA-tekst.
    for m in re.finditer(r'<button\b[^>]*>(.*?)</button>', text):
        if _BOOKING_KW_RE.search(re.sub(r"<[^>]+>", " ", m.group(1))):
            return {"value": "has_booking", "confidence": "high", "kind": "request_form",
                    "evidence": [f"button:{m.group(1)[:60]}"], "platform": None}

    # 5. Keyword ergens in de zichtbare tekst (zwakker, maar telt als indicator).
    if _BOOKING_KW_RE.search(re.sub(r"<[^>]+>", " ", text)):
        return {"value": "has_booking", "confidence": "low", "kind": "request_form",
                "evidence": ["keyword_in_text"], "platform": None}

    # --- Geen enkele indicator gevonden ---
    if not fetch_ok or len(text) < _MIN_HTML_FOR_NEGATIVE:
        # C3-kern: een mislukte/lege fetch mag NOOIT 'no_booking' worden.
        return {"value": "unknown", "confidence": "low", "kind": None,
                "evidence": ["fetch_failed_or_insufficient_html"], "platform": None}

    return {"value": "no_booking", "confidence": "high", "kind": None,
            "evidence": [], "platform": None}
