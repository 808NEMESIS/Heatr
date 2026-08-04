"""
utils/legal_form.py — rechtsvorm-risico voor koude B2B-mail (AVG-02).

Audit-bevinding (2026-07-24): voor natuurlijke personen (zzp/eenmanszaak, groot
deel van de ICP: 1-15 medewerkers, eigenaar=beslisser) geldt het spamverbod voor
natuurlijke personen (Telecommunicatiewet art. 11.7 lid 1) — anders dan voor een
rechtspersoon (BV/NV/stichting), die vrij benaderbaar is.

CORRECTIE (2026-08-04, na online-onderzoek + Sami-beslissing): art. 11.7 **lid 3**
Tw kent óók de natuurlijke-persoon-ondernemer een uitzondering toe. Koude B2B-mail
mag ZONDER toestemming als de afzender contactgegevens gebruikt die de ontvanger
"heeft bestemd en openbaar gemaakt voor dit doel", en overeenkomstig dat doel. In
de praktijk: een e-mailadres dat de ondernemer ZELF op de eigen website publiceert
voor zijn dienstverlening (waterval-stap 1 → email_discovery_source == 'website').
Die grond werkt onafhankelijk van de rechtsvorm — een rechtspersoon mag toch al,
een natuurlijk persoon mag mét zo'n gepubliceerd adres. De eerdere aanname ("de
published-address-uitzondering is een rechtsPERSOON-concept") was dus onjuist.

Beleid nu (Sami 2026-08-04): "bekijk of ze een e-mail voor hun dienstverlening op
de site gebruiken; zo ja, gewoon mailen." → een op de eigen site gepubliceerd adres
opent de poort, óók bij eenmanszaak/VOF. Zonder zo'n adres blijft natuurlijk persoon/
onbepaald geblokkeerd (fail-closed).

JURIDISCH VOORBEHOUD: de ACM leest "overeenkomstig het doel" streng; een contact-
adres gebruikt voor koude sales is pleitbaar maar niet risicovrij. Dit staat onder
Spoor J (legal sign-off) vóór productie-verzending. Deze gate raakt de globale
gdpr_safe NIET (bestaande 493-flow blijft); sends blijven achter de kill-switch.
"""
from __future__ import annotations

import re

# Eenmanszaak/zzp/VOF/maatschap = natuurlijk persoon (geen aparte rechtspersoon).
_NATUURLIJK_RE = re.compile(
    r"eenmanszaak|zzp|zelfstandige|vennootschap onder firma|\bvof\b|maatschap|"
    r"commanditaire vennootschap|\bcv\b|natuurlijk persoon", re.IGNORECASE)
# Rechtspersonen: BV/NV/stichting/coöperatie/vereniging.
_RECHTSPERSOON_RE = re.compile(
    r"besloten vennootschap|\bb\.?v\.?\b|naamloze vennootschap|\bn\.?v\.?\b|"
    r"stichting|co[öo]peratie|vereniging|holding|rechtspersoon", re.IGNORECASE)

# Gratis, zekere rechtspersoon-afleiding uit bedrijfsnaam/footer (Sami 2026-07-25,
# zonder betaalde KvK-API). POSITIEF-ONLY: "B.V."/"N.V."/besloten vennootschap =
# rechtspersoon; afwezigheid zegt NIETS (een BV zet 'BV' niet altijd in de handels-
# naam) → dan blijft het onbepaald, nooit 'natuurlijk_persoon'. Streng: standalone
# BV/B.V.-token, niet een substring.
# Dotted/uitgeschreven vorm: hoofdletter-ongevoelig (B.V./b.v./Besloten Vennootschap).
_RP_SPELLED_RE = re.compile(
    r"\bb\.v\.|\bn\.v\.|besloten vennootschap|naamloze vennootschap|co[öo]perati", re.IGNORECASE)
# Kale 'BV'/'NV'-token: ALLEEN hoofdletters (ruis van lowercase 'bv' in woorden/URLs voorkomen).
_RP_BARE_RE = re.compile(r"\bBV\b|\bNV\b")


def derive_rechtspersoon(text: str | None) -> str | None:
    """Return 'rechtspersoon' als de tekst (bedrijfsnaam of footer) een B.V./N.V.-
    signaal draagt, anders None (= niet af te leiden, NIET natuurlijk persoon).
    Kale 'BV'/'NV' alleen in hoofdletters om ruis te beperken; 'B.V.'/'besloten
    vennootschap' hoofdletter-ongevoelig."""
    if not text:
        return None
    return "rechtspersoon" if (_RP_SPELLED_RE.search(text) or _RP_BARE_RE.search(text)) else None


def classify_legal_form(lead: dict, page_text: str | None = None) -> str:
    """'rechtspersoon' | 'natuurlijk_persoon' | 'onbepaald'.

    Bronnen, in volgorde: (1) kvk_legal_form (handmatig ingevoerd of ooit via API),
    (2) gratis B.V.-afleiding uit de bedrijfsnaam, (3) idem uit de meegegeven footer/
    page_text. Leeg/onbekend → 'onbepaald'. Fail-closed: nooit optimistisch."""
    raw = (lead.get("kvk_legal_form") or "").strip()
    if raw:
        if _NATUURLIJK_RE.search(raw):
            return "natuurlijk_persoon"
        if _RECHTSPERSOON_RE.search(raw):
            return "rechtspersoon"
        return "onbepaald"
    # Geen expliciete rechtsvorm → probeer de gratis, zekere B.V.-afleiding.
    if derive_rechtspersoon(lead.get("company_name")) or derive_rechtspersoon(page_text):
        return "rechtspersoon"
    return "onbepaald"


# Herkomst-vlag (leads.email_discovery_source) die telt als "door de ondernemer
# zelf op de eigen site gepubliceerd voor zijn dienstverlening" (art. 11.7 lid 3).
# ALLEEN waterval-stap 1 (de eigen website); een gegokt info@-patroon, een Google-
# snippet of een KvK-adres is GEEN bewijs van publicatie-voor-dit-doel.
_PUBLISHED_SITE_SOURCES = {"website"}


def email_published_on_own_site(lead: dict) -> bool:
    """True als het e-mailadres van de lead op hun EIGEN website is gevonden
    (waterval-stap 1). Dat is de art. 11.7 lid 3-grond: een adres dat de
    ondernemer zelf heeft bestemd en openbaar gemaakt voor contact over zijn
    dienstverlening. Andere herkomsten (gok-patroon, google_search, kvk) tellen
    NIET — die bewijzen geen publicatie-voor-dit-doel."""
    return (lead.get("email_discovery_source") or "").strip().lower() in _PUBLISHED_SITE_SOURCES


def receptie_avg_safe(lead: dict, page_text: str | None = None) -> tuple[bool, str]:
    """Cold-mail-gate voor de receptie-campagne (AVG-02).

    Twee zelfstandige gronden voor "veilig":
      1. bevestigde RECHTSPERSOON (BV/NV/stichting) — vrij benaderbaar; of
      2. een op de EIGEN SITE gepubliceerd zakelijk adres (art. 11.7 lid 3) —
         geldt óók voor eenmanszaak/VOF/onbepaald.
    Zonder één van beide: natuurlijk persoon/onbepaald blokt (fail-closed).

    `page_text` (optioneel) laat de gratis B.V.-afleiding ook de footer meenemen.
    Returns (ok, reason). Bewust GEEN raise: de send-gate leest de reason."""
    lf = classify_legal_form(lead, page_text=page_text)
    if lf == "rechtspersoon":
        return True, "rechtspersoon"
    # Art. 11.7 lid 3 Tw: zelf-gepubliceerd zakelijk adres op de eigen site =
    # "bestemd en openbaar gemaakt voor dit doel" → koude B2B-mail toegestaan,
    # ongeacht de rechtsvorm (dekt dus ook eenmanszaak/VOF en 'onbepaald').
    if email_published_on_own_site(lead):
        return True, "gepubliceerd_zakelijk_adres_art_11_7_lid_3"
    if lf == "natuurlijk_persoon":
        return False, "natuurlijk_persoon_geen_gepubliceerd_adres"
    return False, "rechtsvorm_onbepaald_geen_gepubliceerd_adres"
