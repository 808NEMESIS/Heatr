"""campaigns/receptie_copywriter.py — gegronde Claude-copy voor receptie mail-1.

Hybride (Sami 2026-08-07): de HARDE feiten blijven deterministisch en worden
INGESPOTEN — de geverifieerde haak-observatie (config/hook_templates, uit
website_intelligence) en de reviewcijfers. Claude verzint dus NOOIT een claim;
het weeft alleen die feiten tot natuurlijke, doorlopende tekst. De bestaande
render-gate (receptie_mail_sendable) + een cijfer-fabricatie-check zijn het
vangnet: faalt de output, dan geeft de generator None terug en valt de aanroeper
terug op de deterministische template (fail-closed).

Twee hoeken:
  - "conversie" (default): opent met de haak, brug naar onzichtbaar afhaken +
    hun reviews als bewijs dat ze klanten bereiken, één zachte vraag (site-review).
  - "reviews": voor cosmetisch met weinig reviews — observatie over review-aantal
    vs. echte blije klanten + review-automatisering, één vraag. NOOIT voor
    alternatieve_geneeskunde (automatisering daar bewust uitgesloten).
"""
from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger(__name__)

_MODEL = os.getenv("RECEPTIE_COPY_MODEL", "claude-haiku-4-5-20251001")


def _allowed_number_tokens(review_count: int | None, rating: float | None) -> set[str]:
    """De ENIGE cijfers die in de body mogen staan (anti-fabricatie): het review-
    aantal en de rating (punt- én komma-notatie). 'twee minuten' e.d. zijn woorden."""
    ok: set[str] = set()
    if review_count:
        ok.add(str(int(review_count)))
    if rating:
        r = f"{float(rating):.1f}"
        ok.add(r); ok.add(r.replace(".", ","))
        ok.add(str(int(float(rating))))          # heel getal, mocht 't zo vallen
    return ok


def _has_fabricated_number(text: str, allowed: set[str]) -> bool:
    """True als er een getal in staat dat niet in `allowed` zit — dan heeft Claude
    een cijfer verzonnen (bv. '300 klanten'). Getallen die deelstring zijn van een
    toegestaan getal (bv. rating-delen) worden afgevangen door de exacte set-check."""
    for tok in re.findall(r"\d+(?:[.,]\d+)?", text):
        if tok not in allowed:
            return True
    return False


def _prompt(*, greeting: str, company: str, city: str, hook_claim: str,
            review_count: int | None, rating: float | None, angle: str) -> str:
    reviews_fact = ""
    if review_count and rating:
        reviews_fact = f"{review_count} Google-reviews met een gemiddelde van {float(rating):.1f}".replace(".", ",")
    elif review_count:
        reviews_fact = f"{review_count} Google-reviews"

    if angle == "reviews":
        opdracht = (
            "Schrijf een koude e-mail waarin je opvalt dat de praktijk relatief weinig "
            "reviews heeft ten opzichte van hoe verzorgd/druk de praktijk oogt, en dat "
            "lang niet elke tevreden klant er uit zichzelf een achterlaat. Reviews zijn "
            "goud voor zichtbaarheid en vertrouwen. Sluit af met dat Aerys het vragen om "
            "reviews automatiseert, en één zachte vraag of je in twee minuten mag laten "
            "zien hoe dat werkt.")
        feiten = f"- Reviewcijfers (ENIGE getallen die je mag noemen): {reviews_fact or 'onbekend'}"
    else:
        opdracht = (
            "Schrijf een koude e-mail die OPENT met de geverifieerde observatie hieronder "
            "(je mag 'm natuurlijk herformuleren maar de FEITELIJKE strekking exact laten), "
            "dan uitlegt waarom dat juist een twijfelaar laat afhaken, dat je zoiets nooit "
            "terugziet (die persoon meldt zich niet), en dat het zonde is omdat hun reviews "
            "laten zien dat ze wél klanten binnenkrijgen en overtuigen. Sluit af met één "
            "zachte vraag of je in twee minuten op hun eigen site mag laten zien waar het "
            "misgaat.")
        feiten = (f"- Geverifieerde observatie (opener, feiten exact laten): {hook_claim}\n"
                  f"- Reviewcijfers (ENIGE getallen die je mag noemen): {reviews_fact or 'onbekend'}")

    return (
        "Je bent Sami, eigenaar van Aerys' Solution (websites + conversie voor praktijken). "
        "Je mailt de eigenaar van een praktijk koud, van ondernemer tot ondernemer.\n\n"
        f"{opdracht}\n\n"
        f"Praktijk: {company}" + (f" in {city}" if city else "") + "\n"
        f"{feiten}\n\n"
        "HARDE REGELS:\n"
        "- Begin met exact deze begroeting op de eerste regel: " + f'"{greeting}"\n'
        "- Nederlands, spreektaal, van mens tot mens. Geen marketing-/kliniekjargon.\n"
        "- Gebruik UITSLUITEND de feiten hierboven. Verzin NIETS over hun site, resultaten "
        "of klanten. Noem GEEN enkel getal behalve de reviewcijfers hierboven.\n"
        "- Maximaal ongeveer 90 woorden. Precies ÉÉN vraagteken in de hele mail.\n"
        "- Begin geen zin met 'Ik'. Geen em-dash (—) of en-dash (–); gebruik komma of punt.\n"
        "- Geen tijdsclaim zoals 'ik keek gisteren' of 'vanmorgen bekeek ik'.\n"
        "- GEEN afsluiting/ondertekening/naam en GEEN privacy- of afmeldregel toevoegen; "
        "stop na de vraag. Die worden er los onder gezet.\n\n"
        "Geef alleen de mailtekst terug, niets eromheen."
    )


async def generate_mail1_body(
    lead: dict,
    *,
    hook_claim: str,
    greeting: str,
    privacy_notice: str,
    unsubscribe: str,
    angle: str = "conversie",
    anthropic_client=None,
    warmr_owns_unsubscribe: bool = False,
) -> str | None:
    """Genereer een volledige, gegronde mail-1-body via Claude. Returned de body
    (incl. begroeting + ondertekening + tokens) als 'ie de gate + cijfer-check
    passeert, anders None (aanroeper valt terug op de deterministische template)."""
    from config.receptie_sequence import receptie_mail_sendable

    company = (lead.get("company_name") or "jullie praktijk").strip()
    city = (lead.get("city") or "").strip()
    review_count = lead.get("google_review_count")
    rating = lead.get("google_rating")

    if angle == "conversie" and not (hook_claim or "").strip():
        return None                                   # geen geverifieerde haak → geen gegronde opener

    client = anthropic_client
    if client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            return None
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=api_key)

    prompt = _prompt(greeting=greeting, company=company, city=city, hook_claim=hook_claim,
                     review_count=review_count, rating=rating, angle=angle)
    try:
        resp = await client.messages.create(
            model=_MODEL, max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        core = (resp.content[0].text or "").strip()
    except Exception as e:
        logger.warning("receptie_copywriter: Claude-call faalde: %s", e)
        return None

    if not core:
        return None
    # Anti-fabricatie: geen getallen buiten de reviewcijfers.
    if _has_fabricated_number(core, _allowed_number_tokens(review_count, rating)):
        logger.info("receptie_copywriter: verworpen — verzonnen getal in output (lead=%s)", lead.get("id"))
        return None
    # Claude mag de begroeting al bevatten; niet dubbel zetten.
    if not core.lower().startswith(greeting.lower()[:6]):
        core = f"{greeting}\n\n{core}"

    body = (f"{core}\n\n"
            "Groet,\nSami Jansema\nAerys Solution, aeryssolution.nl\n\n"
            f"{privacy_notice}\n{unsubscribe}")
    body = re.sub(r"\n{3,}", "\n\n", body).strip()

    ok, reason = receptie_mail_sendable(
        body, privacy_notice=privacy_notice, unsubscribe=unsubscribe,
        warmr_owns_unsubscribe=warmr_owns_unsubscribe)
    if not ok:
        logger.info("receptie_copywriter: verworpen door gate (%s, lead=%s)", reason, lead.get("id"))
        return None
    return body


# ─────────────────────────────────────────────────────────────────────────────
# Value-first mail-1 (Sami 2026-08-08) — DETERMINISTISCHE canonieke boog.
#
# Boog: review-positief (echte thema's) → ongezien lek → vergelijk buurpraktijk →
# Founding Five-aanbod (gratis concept/analyse) → Loom-belofte + shrink-ask.
# Bron: docs/besluit_template_herschrijf.md. Praktijknaam altijd verweven.
# Claude raakt hier NIETS aan — de thema's zijn de enige AI-output (elders gegrond
# geminet). Zo kan de mailbody zelf nooit een claim verzinnen.
# ─────────────────────────────────────────────────────────────────────────────

_VF_CONCEPTSITE = (
    "Hoi,\n\n"
    "Ik las even door de reviews van {naam} en wat steeds terugkomt is {themes}. "
    "Daar komen mensen op af, dus de basis zit goed.\n\n"
    "{leak}\n\n"
    "Klinieken die dat wél strak hebben, winnen 'm nu van de buurpraktijk nog vóór "
    "de eerste telefoon.\n\n"
    "Ik doe nu iets eenmaligs: voor de eerste vijf klinieken maak ik gratis een concept "
    "voor hun nieuwe site. Zeg je ja, dan laat ik je in een korte Loom precies zien wat "
    "ik voor {naam} in gedachten heb. Nergens aan vast.\n\n"
    "Zal ik er een voor {naam} maken?\n\n"
    "Groet,\nSami Jansema\nAerys Solution, aeryssolution.nl\n\n"
    "{privacy}\n{unsub}"
)

_VF_WORKFLOW = (
    "Hoi,\n\n"
    "De site van {naam} ziet er goed uit, en in de reviews lees ik steeds terug {themes}. "
    "Dat zegt genoeg over hoe jullie het doen.\n\n"
    "Waar het bij de meeste praktijken stilletjes weglekt zit erachter: de beller buiten "
    "kantoortijd die geen voicemail inspreekt, het appje dat pas morgen wordt gezien. Net "
    "de patiënt die je al bijna binnen had.\n\n"
    "De praktijk die als eerste reageert houdt die twijfelaar, en de meeste rond {naam} "
    "vangen dat nu nog niet slim op.\n\n"
    "Voor de eerste vijf breng ik gratis in kaart hoeveel daar bij {naam} blijft liggen. "
    "Zeg je ja, dan loop ik je in een Loom door wat ik zie. Kost je niks.\n\n"
    "Zal ik er eens naar kijken voor {naam}?\n\n"
    "Groet,\nSami Jansema\nAerys Solution, aeryssolution.nl\n\n"
    "{privacy}\n{unsub}"
)

_VF_SUBJECT = {"conceptsite": "{naam}, iets aan jullie site", "workflow": "{naam}, één ding"}

_SOFTEN_MODEL = os.getenv("DESIGN_HOOK_MODEL", "claude-haiku-4-5-20251001")
# Woorden die nooit in een koude mail naar de eigenaar mogen (beledigend).
_HARSH = ("amateuristisch", "amateur", "chaotisch", "slecht", "lelijk", "rommelig", "prutswerk")


async def soften_design_flaw(raw: str, *, company: str = "", anthropic_client=None) -> str | None:
    """Herschrijf een interne Vision-observatie tot ÉÉN nette, constructieve, zelf-
    verifieerbare mail-zin (de {leak} van de conceptsite-brug). Geen beledigende
    woorden, geen em-dash, geen vraag. None bij twijfel → aanroeper valt terug."""
    raw = (raw or "").strip()
    if not raw:
        return None
    client = anthropic_client
    if client is None:
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            return None
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=key)
    prompt = (
        "Je bent Sami, websitebouwer. Hieronder een interne design-observatie over de "
        f"site van praktijk '{company}'. Herschrijf 'm tot ÉÉN KORTE Nederlandse zin "
        "(maximaal ongeveer 20 woorden) voor een koude mail aan de eigenaar: benoem "
        "ALLEEN het concrete, zelf-verifieerbare symptoom, GEEN oplossing of advies erbij "
        "(dat bewaren we voor het gesprek). Nooit beledigend (geen woorden als "
        "amateuristisch, slecht, lelijk, chaotisch). Begin met iets als 'Wat meteen "
        "opvalt' of 'Als je 'm op je telefoon opent'. Geen em-dash, geen vraagteken, "
        "geen aanhef of ondertekening.\n\nObservatie: " + raw
    )
    try:
        m = await client.messages.create(model=_SOFTEN_MODEL, max_tokens=120,
                                         messages=[{"role": "user", "content": prompt}])
        out = (m.content[0].text or "").strip().strip('"')
    except Exception as e:
        logger.warning("soften_design_flaw: Claude-call faalde: %s", e)
        return None
    if not out or "?" in out or "—" in out or "–" in out:
        return None
    if any(w in out.lower() for w in _HARSH):
        return None                                   # nog te bot → liever niks
    return out


def _themes_phrase(themes: list[str]) -> str:
    clean = [str(t).strip() for t in (themes or []) if str(t).strip()][:2]
    return " en ".join(clean)


def build_value_first_mail1(
    lead: dict, *,
    bridge: str,
    themes: list[str],
    privacy_notice: str,
    unsubscribe: str,
    leak_line: str | None = None,
    warmr_owns_unsubscribe: bool = False,
) -> dict | None:
    """Bouw de value-first mail-1 uit de canonieke template + echte review-thema's.

    Returns {subject, body, bridge} als 'ie de gate passeert, anders None (aanroeper
    valt terug op de bestaande receptie-mail). Fail-closed:
      - < 2 echte thema's → None (nooit nep-personaliseren);
      - conceptsite zonder gegronde leak_line → None (nooit een verzonnen lek forceren).
    """
    from utils.lead_naming import clean_company_name
    from config.receptie_sequence import receptie_mail_sendable

    naam, needs_review = clean_company_name(lead.get("company_name"))
    naam = (naam or "").strip()
    if not naam or needs_review:
        return None
    themes_phrase = _themes_phrase(themes)
    if len(themes_phrase.split(" en ")) < 2 or not themes_phrase:
        return None                                    # geen twee echte thema's

    if bridge == "conceptsite":
        if not (leak_line or "").strip():
            return None                                # geen gegronde leak → niet forceren
        body = _VF_CONCEPTSITE.format(naam=naam, themes=themes_phrase,
                                      leak=leak_line.strip(), privacy=privacy_notice, unsub=unsubscribe)
    elif bridge == "workflow":
        body = _VF_WORKFLOW.format(naam=naam, themes=themes_phrase,
                                   privacy=privacy_notice, unsub=unsubscribe)
    else:
        return None

    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    ok, reason = receptie_mail_sendable(
        body, privacy_notice=privacy_notice, unsubscribe=unsubscribe,
        warmr_owns_unsubscribe=warmr_owns_unsubscribe)
    if not ok:
        logger.info("build_value_first_mail1: gate wees af (%s, lead=%s)", reason, lead.get("id"))
        return None
    return {"subject": _VF_SUBJECT[bridge].format(naam=naam), "body": body, "bridge": bridge}
