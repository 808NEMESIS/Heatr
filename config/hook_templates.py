"""
config/hook_templates.py — deterministische mail-1 haakjes (signaal-ladder).

Canonieke bron: docs/haakje_mapping_mail1.md (Hormozi-haakjes). Eén scherp,
feitelijk haakje per lead, gekozen op het EERSTE website-signaal dat LIVE vuurt
(website_intelligence/hook_detector.py). Vervangt de Claude-opener +
build_site_observatie voor mail 1: het haakje toont één ongezien lek dat aan
geldverlies koppelt, niet aan smaak.

Hyperpersonalisatie-regels (2026-07-22, Sami — v2):
- **Klinieknaam één keer, vroeg in de body.** De eerste zin van het haakje noemt
  de kliniek bij naam, gekoppeld aan de concrete observatie ("hoe ik bij
  {kliniek} een afspraak maak"). NIET het generieke "een paar klinieken in
  {stad}". De naam komt daarna in de body niet meer terug ("je praktijk").
- **Stad alleen waar het iets toevoegt** — als observatie (patiënt in hun markt,
  concurrentiedichtheid), nooit als kale merge-token. Valt weg als er geen stad
  is; de zin blijft dan kloppen.
- **Tokens spreiden.** Nooit naam + stad + reviews in één zin: naam in zin 1,
  stad (indien gebruikt) in een latere zin, reviews pas in {{zonde_brug}}
  (2e alinea) als onderbouwing. Het haakje noemt de reviews dus NIET.
- **"zonde" is gereserveerd** voor de zonde-brug (2e alinea) — de haakje-zinnen
  gebruiken het woord niet, anders staat het twee alinea's op rij.

Overige regels:
- Het haakje rendert DIRECT als `{{haakje}}` (niet via de opener-QA-keten) — het
  mág over de site praten omdat het deterministisch/live-geverifieerd is.
- Em-/en-dashes bewust vermeden (outreach spec 1, 2026-07-20).
- Variant A/B: bij batch-selectie ALTERNEREND per signaal toegewezen en als
  `hook_variant` opgeslagen (twee leads met hetzelfde signaal krijgen nooit
  dezelfde variant naast elkaar); de render leest die kolom. `pick_variant`
  (hash) is alleen de fallback als er geen opgeslagen variant is.

Tokens in de varianten: {kliniek}, {loc} (= "in {stad}"), {jaar} (sig 5),
{vision_element} (sig 6).
"""
from __future__ import annotations

import hashlib
import re

SIGNAL_NAMES: dict[int, str] = {
    1: "no_online_booking",
    2: "cta_below_fold",
    3: "slow_tti",
    4: "tap_targets",
    5: "stale_footer",
    6: "vision_element",
}

# signaal -> (variant_A, variant_B). Klinieknaam in zin 1, gekoppeld aan de
# observatie. Geen review-verwijzing (die zit in zonde_brug), geen "zonde".
HOOK_VARIANTS: dict[int, tuple[str, str]] = {
    1: (
        "Ik wilde op mijn telefoon even kijken hoe ik bij {kliniek} een afspraak "
        "maak, en kwam alleen een telefoonnummer tegen. De meeste mensen bellen "
        "'s avonds niet meer; die willen op dat moment even boeken, en zijn anders "
        "zo weer weg.",
        "Op zoek naar de boekknop van {kliniek} vond ik alleen een telefoonnummer. "
        "Wie 's avonds {loc} op de bank zit te twijfelen, vindt bellen net die "
        "drempel te veel, en gaat door naar de volgende praktijk.",
    ),
    2: (
        "Ik wilde op mijn telefoon een afspraak maken bij {kliniek}, en moest een "
        "paar schermen scrollen voor ik de boekknop vond. Dat is precies het "
        "moment waarop iemand die twijfelt afhaakt.",
        "Op mijn telefoon kostte het me flink zoeken voor ik doorhad waar ik bij "
        "{kliniek} kon boeken. Klein ding, maar {loc} staat de volgende praktijk "
        "een tik verderop, en juist op zo'n zoekmoment haakt een twijfelaar af.",
    ),
    3: (
        "Ik opende de site van {kliniek} op mijn telefoon en 'ie deed er een paar "
        "tellen over voor er iets stond. Dat klinkt klein, maar het is genoeg voor "
        "iemand om terug te tikken naar de volgende praktijk.",
        "Toen ik de site van {kliniek} op mobiel opende, keek ik een paar seconden "
        "naar een leeg scherm. In die paar seconden ben je de ongeduldige bezoeker "
        "al kwijt, en die zie je nooit terug in je cijfers.",
    ),
    4: (
        "Op de site van {kliniek} zat ik op mijn telefoon een paar keer mis te "
        "tikken; de knoppen staan wat dicht op elkaar. Net dat kleine gedoe doet "
        "iemand afhaken bij het boeken.",
        "Ik merkte op mobiel dat de knoppen op de site van {kliniek} wat klein en "
        "dicht op elkaar staan; een paar keer raakte ik de verkeerde. Onbenullig, "
        "tot iemand die snel een afspraak wil het net te veel gedoe vindt.",
    ),
    5: (
        "Klein detail dat me opviel: onderaan de site van {kliniek} staat nog "
        "{jaar}. Technisch niks mis mee, maar een bezoeker die twijfelt leest dat "
        "als \"zijn ze hier nog actief?\", en dat is nooit de indruk die je wilt.",
        "Onderaan de site van {kliniek} zag ik nog {jaar} staan. Zelf weet je dat "
        "de praktijk volop draait, maar een nieuwe bezoeker weet dat niet, en zo'n "
        "jaartal zaait net dat kleine twijfeltje op het verkeerde moment.",
    ),
    6: (
        "Wat me op de site van {kliniek} meteen opviel is {vision_element}. Op "
        "zich een detail, maar het is het soort ding dat iemand die twijfelt "
        "onbewust laat afhaken.",
        "Eén ding sprong er voor mij meteen uit op de site van {kliniek}: "
        "{vision_element}. Kleinigheid, maar precies zoiets bepaalt in een paar "
        "seconden of een bezoeker je vertrouwt of doorklikt.",
    ),
}

# {loc}-fallback per (signaal, variant) als er geen stad is. Default "" (token
# valt weg, witruimte wordt gecollapsed); (2,'B') heeft een woord nodig omdat de
# zin anders grammaticaal breekt.
_LOC_FALLBACK: dict[tuple[int, str], str] = {(2, "B"): "hier"}

# Vision-prompt voor signaal 6 (Guardrail 2: score < 60 telt als GEEN).
VISION_PROMPT = """\
Je krijgt een mobiele screenshot van de homepage van een kliniek. Noem het ÉNE
visuele element dat een bezoeker het eerst als verouderd of onprofessioneel zou
registreren. Antwoord met een korte, concrete zelfstandige omschrijving die in een
zin past (bijv. "een fotocarrousel die automatisch doorschuift", "stockfoto's van
modellen in plaats van de eigen praktijk", "tekst die over een drukke
achtergrondfoto valt"). Geen algemene oordelen als "gedateerd" of
"onprofessioneel". Als er niks noemenswaardigs opvalt, antwoord exact met: GEEN.

Geef daarnaast een zekerheidsscore 0-100 voor hoe sterk/opvallend het signaal is.

Formaat exact:
ELEMENT: <omschrijving of GEEN>
SCORE: <0-100>

Max 12 woorden voor het element."""


def pick_variant(signal: int, seed: str | None) -> str:
    """Fallback-variantkeuze (hash van seed+signaal) als er GEEN opgeslagen
    `hook_variant` is. Batch-selectie hoort varianten alternerend toe te wijzen
    en op te slaan; deze hash garandeert alleen determinisme, geen spreiding."""
    if not seed:
        return "A"
    h = int(hashlib.sha1(f"{seed}:{signal}".encode("utf-8")).hexdigest(), 16)
    return "A" if h % 2 == 0 else "B"


def build_haakje(
    signal: int | None,
    seed: str | None = None,
    *,
    kliniek: str | None = None,
    variant: str | None = None,
    stad: str | None = None,
    singular: bool = False,
    jaar: int | str | None = None,
    vision_element: str | None = None,
) -> str:
    """Bouw de haakje-zin voor een gevuurd ladder-signaal.

    - `kliniek` (bedrijfsnaam) komt in de EERSTE zin, gekoppeld aan de
      observatie. Zonder naam valt de template terug op "jullie praktijk".
    - `stad` wordt alleen gebruikt waar de variant er een echte observatie van
      maakt ({loc} = "in {stad}"); ontbreekt de stad, dan valt het weg of pakt
      de variant z'n fallback — de zin blijft kloppen.
    - `singular` blijft geaccepteerd voor de aanroep-compatibiliteit; de huidige
      varianten spreken via de klinieknaam en het generieke 'je' en branchen er
      niet meer op.

    Returns "" als er geen (bruikbaar) signaal is. Signaal 5 zonder `jaar` en
    signaal 6 zonder `vision_element` leveren "".
    """
    if signal not in HOOK_VARIANTS:
        return ""
    v = (variant or pick_variant(signal, seed)).upper()
    if v not in ("A", "B"):
        v = "A"
    text = HOOK_VARIANTS[signal][0 if v == "A" else 1]

    naam = (kliniek or "").strip() or "jullie praktijk"
    loc = f"in {str(stad).strip()}" if stad and str(stad).strip() else _LOC_FALLBACK.get((signal, v), "")
    text = text.replace("{kliniek}", naam).replace("{loc}", loc)
    # lege {loc} laat dubbele spaties/spatie-voor-komma achter → opruimen
    text = re.sub(r"\s{2,}", " ", text).replace(" ,", ",").strip()

    if signal == 5:
        if not jaar:
            return ""
        text = text.replace("{jaar}", str(jaar))
    if signal == 6:
        if not vision_element:
            return ""
        text = text.replace("{vision_element}", str(vision_element).strip())
    return text


def build_zonde_brug(reviews: int | None = None, rating: float | None = None) -> str:
    """Review-onderbouwing van 'dat is zonde', na het haakje (2e alinea).

    De review-getallen komen HIER terug (niet in het haakje). Ontbreken ze, dan
    valt de zin terug op een getal-loze variant — nooit verzonnen cijfers.
    """
    core = "je krijgt ze binnen, en op dat ene moment laat de site een deel weer los."
    try:
        r = int(reviews) if reviews is not None else 0
    except (TypeError, ValueError):
        r = 0
    try:
        star = float(rating) if rating is not None else 0.0
    except (TypeError, ValueError):
        star = 0.0
    if r >= 3 and star > 0:
        sterren = f"{star:.1f}".replace(".", ",")
        return f"Bij {r} reviews en een gemiddelde van {sterren} is dat zonde: {core}"
    if r >= 3:
        return f"Bij {r} reviews is dat zonde: {core}"
    return f"En dat is zonde: {core}"
