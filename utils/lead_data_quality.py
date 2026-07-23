"""
utils/lead_data_quality.py — pre-render / pre-enrichment datakwaliteit-gate.

Aanleiding (Sami's testrun-check, 2026-07-22): twee van tien leads hadden een
datafout die niets met het website-signaal te maken had, maar wél de mail zou
bereiken:
  - "de huidkliniek" had het e-mailadres van z'n WEBBUREAU (sales@interip.nl),
    niet van de kliniek;
  - generieke inbox op een vreemd domein glipt zo door.

Deze gate draait VÓÓR de voornaam-enrichment: bij een verkeerd/vreemd adres is
naam zoeken zinloos. Het is een MARKERING (handmatig checken), geen harde
uitsluiting — deterministisch en transparant.

Puur, geen I/O.
"""
from __future__ import annotations

import re

from website_intelligence.practice_type import FREEMAIL_DOMAINS

# Generieke/rol-inboxen (geen persoon). local-part vóór de @.
_GENERIC_LOCALPARTS = {
    "info", "contact", "sales", "hello", "hallo", "mail", "office", "admin",
    "webshop", "receptie", "reception", "praktijk", "kliniek", "afspraak",
    "welkom", "no-reply", "noreply", "support", "team", "secretariaat",
}


def _norm_domain(value: str | None) -> str:
    """Domein uit een URL/domein-veld: protocol/www/pad/poort eraf, lowercase."""
    if not value:
        return ""
    v = str(value).strip().lower()
    v = re.sub(r"^https?://", "", v)
    v = v.split("/")[0].split("?")[0].split("#")[0]
    v = v.split(":")[0]
    if v.startswith("www."):
        v = v[4:]
    return v.strip()


def _registrable(domain: str) -> str:
    """Grof registrable-domein (laatste 2 labels, of 3 bij .co.uk-achtig).

    Genoeg om 'mail.kveg.nl' en 'kveg.nl' als hetzelfde bedrijf te zien, zonder
    een volledige public-suffix-lijst. Bewust conservatief."""
    if not domain:
        return ""
    parts = domain.split(".")
    if len(parts) <= 2:
        return domain
    if parts[-2] in {"co", "com", "net", "org"} and len(parts[-1]) == 2:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


def check_contact_data_quality(email: str | None, domain: str | None) -> dict:
    """Beoordeel of het contact-e-mailadres bij de website hoort.

    Returns:
        {
          "ok": bool,                 # False = handmatig checken vóór enrichment
          "flags": [str, ...],        # mensleesbare markeringen
          "email_domain": str,
          "site_domain": str,
          "block_name_enrichment": bool,  # naam zoeken heeft geen zin
        }
    """
    flags: list[str] = []
    email = (email or "").strip().lower()
    site = _norm_domain(domain)

    if not email or "@" not in email:
        return {"ok": False, "flags": ["geen e-mailadres"], "email_domain": "",
                "site_domain": site, "block_name_enrichment": True}

    local, _, edom = email.partition("@")
    edom = _norm_domain(edom)
    is_freemail = edom in FREEMAIL_DOMAINS
    is_generic = local in _GENERIC_LOCALPARTS
    same_company = bool(site) and _registrable(edom) == _registrable(site)

    block = False
    if is_freemail:
        # freemail = eenmanszaak-signaal (elders al gewogen); geen bureau-vlag,
        # naam zoeken blijft zinvol (de eigenaar mailt vanaf z'n privé-adres).
        flags.append(f"freemail-adres ({edom}) — eenmanszaak-signaal")
    elif site and not same_company:
        # e-maildomein wijkt af van het websitedomein en is geen freemail →
        # sterk signaal voor een bureau/derde-partij-adres. Naam zoeken op de
        # kliniek-site is dan zinloos: het adres hoort er niet bij.
        who = "generieke/bureau-inbox" if is_generic else "vreemd domein"
        flags.append(
            f"e-maildomein ({edom}) wijkt af van websitedomein ({site}) — "
            f"mogelijk bureau/derde ({who}), handmatig checken")
        block = True
    elif not site:
        flags.append("geen websitedomein om tegen te vergelijken")

    return {"ok": not block, "flags": flags, "email_domain": edom,
            "site_domain": site, "block_name_enrichment": block}
