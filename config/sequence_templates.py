"""
config/sequence_templates.py — Production-ready sequence templates.

v1.0 sequence: 3-mail cold outreach voor NL cosmetische klinieken,
prijs-vrij, converteert naar Efficiency Audit (€497 — niet in mail body).

Selector logica: Mail 1 heeft 5 observation blocks (A-E). Caller kiest blok
op basis van lead-data via `pick_observation_block()`. Renderer in
`campaigns/sequence_engine.py` doet variabele-injectie + spintax.
"""
from __future__ import annotations

from typing import Any


# Per-archetype: welke observation-blokken zijn TONE-PASSEND.
# Een blok dat hier NIET in staat wordt geskipt — fallback wordt gekozen.
# Reden: een holistisch-spirituele praktijk wil je niet aanvliegen op "online
# booking is conversie-killer"; een medisch_cosmetisch wil je niet bestoken
# met ads-jargon.
_ARCHETYPE_ALLOWED_BLOCKS: dict[str, set[str]] = {
    # cosmetisch
    "medisch_cosmetisch":         {"website", "reviews", "operational", "fallback"},  # ADS skip — te commercieel
    "esthetisch_medisch":         {"website", "reviews", "ads", "operational", "fallback"},  # alle
    "premium_beauty":             {"website", "reviews", "operational", "fallback"},  # ADS skip — persoonlijke brand
    "volume_beauty":              {"website", "reviews", "ads", "operational", "fallback"},  # alle
    # alternatief
    "lichaamswerk_pragmatisch":   {"website", "reviews", "operational", "fallback"},  # ADS skip — vakprofielen draaien zelden ads
    "holistisch_spiritueel":      {"website", "reviews", "fallback"},  # ADS+OPERATIONAL skip — anti-conversie tone
    "therapeutisch_mentaal":      {"website", "reviews", "fallback"},  # OPERATIONAL skip — boekings-druk past niet bij tone
    "welzijn_praktisch":          {"website", "reviews", "ads", "operational", "fallback"},  # alle
}


def pick_observation_block(lead: dict[str, Any]) -> str:
    """Selecteer Mail-1 observatie-blok op signaal-sterkte (eerste match wint).

    Archetype-aware: een blok dat tone-mismatched is voor het archetype wordt
    overgeslagen. Bv. holistisch_spiritueel slaat 'operational' (online-booking
    pijn) over want die praktijken kiezen vaak bewust voor persoonlijk contact.

    Returns one of: 'website', 'reviews', 'ads', 'operational', 'fallback'.
    """
    archetype = lead.get("archetype")
    allowed = _ARCHETYPE_ALLOWED_BLOCKS.get(archetype) if archetype else None
    # Geen archetype bekend → alle blokken toegestaan (legacy gedrag)
    def _allowed(block: str) -> bool:
        return allowed is None or block in allowed

    age = lead.get("website_age_years") or 0
    visual = lead.get("visual_score") or lead.get("website_score") or 100
    if age >= 4 and visual < 60 and _allowed("website"):
        return "website"

    rating = lead.get("google_rating") or 0
    latest_review_days = lead.get("days_since_last_review")
    if latest_review_days is None and lead.get("latest_review_date"):
        # Compute on-the-fly als enrichment-step het rauwe veld vulde maar de derived niet
        from enrichment.google_reviews_scraper import days_since
        latest_review_days = days_since(lead.get("latest_review_date"))
    # Strict: alleen kiezen als we daadwerkelijk weten dat het oud is
    # (None blijft skip — geen valse signalen)
    if 4.0 <= rating <= 4.4 and latest_review_days is not None and latest_review_days > 28 and _allowed("reviews"):
        return "reviews"

    if lead.get("meta_ads_active") and lead.get("ad_focus") and _allowed("ads"):
        return "ads"

    if not lead.get("has_online_booking") and not lead.get("booking_system") and _allowed("operational"):
        return "operational"

    return "fallback"


# ---------------------------------------------------------------------------
# Mail 1 — observation-driven openers
# Mail 1 body is uniform `{{opener}}` — Warmr substitueert per-lead via custom_fields.
# `_OBSERVATION_PARAGRAPHS` levert alleen de blok-specifieke paragraaf
# (geen greeting, geen signature — die zitten in het uniforme Mail 1 frame).
# ---------------------------------------------------------------------------
_OBSERVATION_PARAGRAPHS: dict[str, str] = {
    "website": (
        "Ik kwam {{company}} tegen via Google en heb kort op de site gekeken — die draait al een paar jaar mee, "
        "te zien aan de structuur. In de cosmetische hoek zien we dat sites van die leeftijd vaak op één punt achter "
        "lopen: bezoekers vanaf mobiel haken af voor ze bij 'afspraak maken' zijn. Bij andere klinieken in {{city}} "
        "zien we dat 60-70% van het verkeer mobiel is.\n\n"
        "Geen verkooppraatje — gewoon een observatie. Herken je dit, of zit het inmiddels strakker in elkaar?"
    ),
    "reviews": (
        "Ik kwam {{company}} tegen — Google rating {{score}} met goede beoordelingen. Wat me opviel: de meest "
        "recente review is alweer een paar weken oud. In jullie hoek zien we dat een review-cadans van >1 per maand "
        "ranking en click-through op Google Maps significant verbetert; meestal een kwestie van een geautomatiseerde "
        "vraag na de behandeling.\n\n"
        "Doen jullie hier al iets mee, of staat het op de 'ooit'-lijst?"
    ),
    "ads": (
        "Ik zag dat {{company}} actief Meta Ads draait. Dat is op zich goed — ad-actieve klinieken zitten meestal in "
        "groeifase. Wat ik bij vergelijkbare praktijken vaak zie is dat de landingsroute (ad → site → afspraak) één "
        "of twee onnodige stappen heeft, waardoor cost-per-booking 2-3x hoger uitvalt dan nodig.\n\n"
        "Ben benieuwd: meet je de cost-per-boeking, of stuur je puur op CTR/CPC?"
    ),
    "operational": (
        "Ik kwam {{company}} tegen en zag iets opmerkelijks: geen online boekingsknop op de site. In jullie hoek is "
        "dat nu de #1 friction-point — patiënten willen 's avonds vanaf de bank een slot kiezen, niet bellen op "
        "kantoortijden. Bij andere klinieken in {{city}} loopt 30-40% van de boekingen via de online flow.\n\n"
        "Bewuste keuze, of staat het gewoon nog op de lijst?"
    ),
    "fallback": (
        "Ik kwam {{company}} tegen tijdens onderzoek naar cosmetische klinieken in {{city}}. Wat me opviel is dat "
        "jullie online aanwezigheid ruimte heeft om scherper te worden — ik kijk specifiek naar conversie van "
        "websitebezoek naar boeking.\n\n"
        "Dit is geen verkooppraatje, gewoon een observatie. Open om er kort over te sparren?"
    ),
}


def _paragraphs_for_sector(sector: str | None) -> dict[str, str]:
    """Return blok-paragraphs voor de juiste sector. Default = cosmetisch."""
    if sector == "alternatieve_geneeskunde":
        return _OBSERVATION_PARAGRAPHS_ALT
    return _OBSERVATION_PARAGRAPHS


def get_observation_text(block: str, lead: dict | None = None) -> str:
    """Return de blok-paragraaf voor lead's observation block.

    Sector-aware: kiest cosmetisch- of alt-zorg-paragrafen op basis van
    `lead.sector`. Vervolgens substitutie van {{company}}/{{city}}/{{score}}
    voor preview-rendering. In productie doet Warmr de Warmr-substitutie via
    custom_fields.

    Voor BLOK B (reviews): als competitor-aggregaten beschikbaar zijn op de lead,
    voeg een concrete competitor-sentence toe — verhoogt de pijn-druk significant
    ("er zijn 3 concurrenten in jullie stad met een hogere rating").
    """
    sector = (lead or {}).get("sector") if lead else None
    paragraphs = _paragraphs_for_sector(sector)
    text = paragraphs.get(block, paragraphs["fallback"])
    if not lead:
        return text

    # Optionele BLOK B verrijking met concurrent-context
    if block == "reviews":
        higher = lead.get("local_competitors_higher_rating") or 0
        in_db = lead.get("local_competitors_in_db") or 0
        if higher >= 1 and in_db >= 2:
            extra = (
                f"\n\nVoor context: in {{{{city}}}} zien we {higher} van de {in_db} "
                f"vergelijkbare klinieken die we volgen met een hogere Google-rating. "
                f"Dat scheelt elk kwartaal een paar nieuwe patiënten."
            )
            # Splice voor de afsluitende vraag (laatste paragraaf)
            parts = text.rsplit("\n\n", 1)
            text = parts[0] + extra + "\n\n" + parts[1] if len(parts) == 2 else text + extra

    return (
        text
        .replace("{{company}}", str(lead.get("company_name") or lead.get("domain") or ""))
        .replace("{{city}}", str(lead.get("city") or ""))
        .replace("{{score}}", str(lead.get("google_rating") or lead.get("website_score") or ""))
    )


# Uniform Mail 1 frame — geldig voor alle leads. Per-lead variatie via {{opener}}.
_MAIL1_UNIFORM = (
    "Hoi {{first_name}},\n\n"
    "{{opener}}\n\n"
    "— Sami Jansema"
)


# ---------------------------------------------------------------------------
# ALT-ZORG observation paragraphs — sector-aangepaste tone
# ---------------------------------------------------------------------------
# Verschil met cosmetisch:
#   - geen "klinieken" / "in de cosmetische hoek"
#   - geen "60-70% mobiel verkeer" (te corporate voor holistisch-spiritueel)
#   - geen "cost-per-booking" / "conversie" (vermijd harde commerciële taal)
#   - wél: vakinhoudelijk-respectvol, "praktijken", refereer aan vertrouwen +
#     persoonlijke relatie ipv conversie
#
# Tone is een gemiddelde — voor extreme archetypes (holistisch_spiritueel
# vs welzijn_praktisch) zou je nog finer kunnen splitsen, maar dat doen we
# pas als A/B-loop het rechtvaardigt.
_OBSERVATION_PARAGRAPHS_ALT: dict[str, str] = {
    "website": (
        "Ik kwam {{company}} tegen tijdens onderzoek naar praktijken in {{city}} en "
        "heb kort op de site gekeken — die heeft duidelijk een aantal jaren in de "
        "spreekkamer doorstaan. Wat me opviel: nieuwe patiënten die je vinden via "
        "Google krijgen weinig houvast op wat een eerste sessie bij jou letterlijk "
        "inhoudt — terwijl dat juist in jullie vak het verschil maakt tussen "
        "boeken of nog twijfelen.\n\n"
        "Bewuste keuze om dat zo open te houden, of komt het er gewoon nog niet van?"
    ),
    "reviews": (
        "Ik kwam {{company}} tegen — Google rating {{score}}, met patiënten die "
        "duidelijk waarderen wat je doet. Wat me opviel: de meest recente review "
        "staat er alweer een paar weken. In jullie vak is een geregelde review-"
        "stroom vaak het verschil tussen wel/niet gevonden worden door zoekers — "
        "meestal een kwestie van een korte vraag na een sessie, niet meer.\n\n"
        "Doen jullie hier al iets bewust mee?"
    ),
    "ads": (
        "Ik zag dat {{company}} actief Meta Ads draait — niet vanzelfsprekend in "
        "jullie hoek. Wat ik bij vergelijkbare praktijken vaak zie is dat de route "
        "van advertentie naar daadwerkelijke afspraak een paar onnodige stappen "
        "heeft, waardoor het rendement uit de ads merkbaar lager ligt dan zou "
        "kunnen.\n\n"
        "Ben benieuwd: kijk je vooral naar klikken, of meet je ook hoeveel mensen "
        "uiteindelijk de stap maken?"
    ),
    "operational": (
        "Ik kwam {{company}} tegen en zag iets praktisch: geen mogelijkheid om "
        "online een afspraak in te plannen. In jullie vak is dat steeds vaker het "
        "punt waar iemand afhaakt — patiënten die 's avonds googlen willen niet "
        "wachten tot ze de volgende dag kunnen bellen.\n\n"
        "Bewuste keuze (ik snap dat het persoonlijk contact je iets waard is) of "
        "staat het op de lijst?"
    ),
    "fallback": (
        "Ik kwam {{company}} tegen tijdens onderzoek naar praktijken in {{city}}. "
        "Wat me opviel: jullie online aanwezigheid heeft ruimte om scherper te "
        "worden — ik kijk specifiek naar hoe nieuwe patiënten de stap van Google "
        "naar afspraak maken.\n\n"
        "Dit is geen verkooppraatje, gewoon een observatie. Open om er kort over "
        "te sparren?"
    ),
}


# ---------------------------------------------------------------------------
# Mail 2 — audit pitch (PRIJS-VRIJ, met P.P.S. naar website-only pad)
# ---------------------------------------------------------------------------
_MAIL2_BODY = (
    "Hoi {{first_name}},\n\n"
    "Vorige week stuurde ik een observatie over {{company}}. Geen reactie nodig — ik begrijp dat het druk is.\n\n"
    "Voor de helderheid: wat ik doe is een korte meet-sessie waarbij ik concreet in kaart breng waar bezoekers "
    "afhaken op je site, en wat dat per maand kost in misgelopen boekingen. Geen powerpoint, geen lange offerte — "
    "een uur sessie en een document met drie acties die je zelf of via ons kan doorvoeren.\n\n"
    "Werkt het beter als ik je een 15-minuten kennismaking voorstel, of zal ik het document direct sturen?\n\n"
    "— Sami Jansema\n\n"
    "P.P.S. Mocht je niet op zoek zijn naar een audit maar wél overwegen je site te vernieuwen: "
    "ik stuur je graag een paar recente projecten ter inspiratie."
)


# ---------------------------------------------------------------------------
# Mail 3 — break-up / soft close
# ---------------------------------------------------------------------------
_MAIL3_BODY = (
    "Hoi {{first_name}},\n\n"
    "Laatste mail in deze reeks — beloofd.\n\n"
    "Als de timing niet goed is: helemaal geen probleem. Mocht het over een aantal maanden alsnog relevant worden, "
    "weet je me te vinden. En zo niet, laat me het weten dan haal ik {{company}} uit mijn lijst.\n\n"
    "Dank voor je tijd.\n\n"
    "— Sami Jansema"
)


# DEPRECATED: replaced by v3.1 brug-templates (v3_1_website / v3_1_workflow /
# v3_1_ai_audit). Production launch-flow gebruikt sinds 2026-05-07 pick_brug()
# server-side; v1-template-IDs in /campaigns/launch worden ge-rerouted naar
# v3.1 auto-mode. Kept for migration safety until 2026-06-06 — alleen verwijderen
# nadat de eerste cohort live is en geen rollback meer nodig.
def get_v1_sequence(observation_block: str = "fallback") -> list[dict]:
    """Return de complete v1.0 sequence als renderbare steps.

    Mail 1 body is altijd het uniforme `_MAIL1_UNIFORM` frame met `{{opener}}`
    placeholder. Per-lead variatie loopt via Warmr's custom_fields.opener — dat
    wordt door `_build_lead_payload` gevuld met `get_observation_text(block, lead)`.

    De `observation_block` parameter wordt nog wel doorgegeven aan de validator
    voor minimum-woordcount check op de gerenderde tekst (preview path), maar de
    body die naar Warmr gaat is altijd het uniforme frame.
    """
    return [
        {
            "subject": "{{company}} — één observatie",
            "body": _MAIL1_UNIFORM,
            "delay_days": 0,
            "thread": "new",
            # Hint voor render_step / preview — niet door Warmr gebruikt
            "_observation_block": observation_block if observation_block in _OBSERVATION_PARAGRAPHS else "fallback",
        },
        {
            "subject": "Re: {{company}} — één observatie",
            "body": _MAIL2_BODY,
            "delay_days": 3,
            "thread": "reply",
        },
        {
            "subject": "Re: {{company}} — één observatie",
            "body": _MAIL3_BODY,
            "delay_days": 5,
            "thread": "reply",
        },
    ]


# Alt-zorg-specifieke Mail 2 — geen "audit" jargon, geen "meet-sessie"
_MAIL2_BODY_ALT = (
    "Hoi {{first_name}},\n\n"
    "Vorige week stuurde ik een observatie over {{company}}. Geen reactie nodig — "
    "ik begrijp dat het druk is in de praktijk.\n\n"
    "Wat ik in dit soort gesprekken meestal doe: een uur samen kijken naar je "
    "site, vanuit het oogpunt van iemand die net begint te zoeken in jouw vak. "
    "Geen powerpoint, geen offerte van vier kantjes. Een document met drie "
    "concrete dingen die je zelf, of via ons, kan aanpakken.\n\n"
    "Werkt een korte kennismaking beter, of zal ik het document gewoon sturen?\n\n"
    "— Sami Jansema\n\n"
    "P.P.S. Mocht je niet zoeken naar zo'n sessie maar wél overwegen je site te "
    "vernieuwen — ik stuur je graag een paar voorbeelden van praktijken waarmee "
    "we eerder hebben gewerkt."
)

_MAIL3_BODY_ALT = (
    "Hoi {{first_name}},\n\n"
    "Laatste mail in deze reeks — beloofd.\n\n"
    "Als de timing niet goed is: helemaal geen probleem. Mocht het over een "
    "aantal maanden alsnog relevant worden, weet je me te vinden. En zo niet, "
    "laat me het weten dan haal ik {{company}} uit mijn lijst.\n\n"
    "Dank voor je tijd.\n\n"
    "— Sami Jansema"
)


# DEPRECATED: replaced by v3.1 brug-templates. Zelfde reden als get_v1_sequence
# hierboven — kept until 2026-06-06.
def get_v1_alt_sequence() -> list[dict]:
    """v1.0 alternatieve zorg sequence — uniform Mail 1 frame + alt-zorg Mail 2/3."""
    return [
        {
            "subject": "{{company}} — een observatie",
            "body": _MAIL1_UNIFORM,
            "delay_days": 0,
            "thread": "new",
        },
        {
            "subject": "Re: {{company}} — een observatie",
            "body": _MAIL2_BODY_ALT,
            "delay_days": 4,           # iets langere cadans, alt-zorg-praktijken zijn niet-haastig
            "thread": "reply",
        },
        {
            "subject": "Re: {{company}} — een observatie",
            "body": _MAIL3_BODY_ALT,
            "delay_days": 6,
            "thread": "reply",
        },
    ]


# ===========================================================================
# v3.1 — Bridge-based templates (toegevoegd 2026-05-07)
# ===========================================================================
# Drie bruggen ipv sector-pad: 'website', 'workflow', 'ai_audit'. Brug-keuze
# via pick_brug(lead_signals). v1.0 templates blijven naast deze staan voor
# backwards-compat met api/main.py launch endpoint + frontend default + tests
# (refactor naar v3.1-only is aparte sessie). Productie-activatie van v3.1
# vereist dat /campaigns/launch caller pick_brug aanroept en de juiste
# v3_1_<brug>-template kiest — niet automatisch.
#
# Tokens: {{first_name}}, {{bedrijfsnaam}}, {{stad}}, {{primaire_dienstverlening}},
#         {{signaal_blok}}, {{sector_noemer}}, {{LOOM_LINK}}, {{VIDEO_LINK}}
# Substitutie: campaigns/sequence_engine.inject_variables.

# Sector-noemer mapping voor {{sector_noemer}} token.
# Default fallback: "ondernemers".
SECTOR_NOEMER_MAP: dict[str, str] = {
    "cosmetische_behandelaars":  "cliniek-ondernemers",
    "alternatieve_geneeskunde":  "praktijken",
    "techniek_ambacht":          "ambachtelijke bedrijven",
    "zakelijke_dienstverlening": "zakelijke dienstverleners",
}


# NB: de eerdere _SIGNAAL_BLOK_SHORT-dict + signal_block_short()-functie zijn
# verwijderd op 2026-05-07. De fallback-strings ("een site die al een paar
# jaar meedraait", "wat ik in jullie online aanwezigheid zag") pasten niet in
# de v3.1 zin-frames. Vervangen door utils/signal_picker.pick_signaal_blok
# met 6-tier prioriteits-keten — zie utils/signal_picker.py.


def primaire_dienstverlening_for_lead(lead: dict) -> str:
    """Resolve {{primaire_dienstverlening}} fallback-keten.

    1. treatment_focus (joined) — bv. 'Ooglidcorrectie, Injectables, Plastische Chirurgie'
    2. industry — Claude-haiku output uit company_enrichment
    3. sector letterlijk — laatste redmiddel
    """
    treatment = lead.get("treatment_focus")
    if isinstance(treatment, list) and treatment:
        return ", ".join(str(t) for t in treatment if t)
    industry = (lead.get("industry") or "").strip()
    if industry:
        return industry
    return (lead.get("sector") or "").replace("_", " ").strip() or "jullie werk"


def sector_noemer_for_lead(lead: dict) -> str:
    """Resolve {{sector_noemer}} via SECTOR_NOEMER_MAP. Default 'ondernemers'."""
    return SECTOR_NOEMER_MAP.get(lead.get("sector") or "", "ondernemers")


# ---------------------------------------------------------------------------
# v3.1 mail-bodies — 3 bruggen × 3 mails
# ---------------------------------------------------------------------------
# TODO {{LOOM_LINK}} en {{VIDEO_LINK}} blijven leeg in v1 — handmatig invullen
# per send. Auto-generatie kan later via een Loom-API integratie + per-lead
# video-pre-render (uit-scope nu).

# v3.2 Mail 1 (2026-05-07): opener herzien naar "ik keek naar ondernemers in X"
# frame met sector_impact_frame + stad_of_sector tokens. Mail 2/3 blijft v3.1.
# "een gemiddelde patiëntervaring" → "gemiddeld werk" (universele variant ipv
# aparte sector_position_frame-mapping; user-spec gaf de keuze).
_V3_1_BRUG_WEBSITE_MAIL_1 = (
    "Hoi {{first_name}},\n\n"
    "Ik keek naar ondernemers in {{stad_of_sector}} met lokale impact en het "
    "viel me op dat {{bedrijfsnaam}} {{sector_impact_frame}}. {{signaal_blok}} "
    "bouw je niet op met gemiddeld werk.\n\n"
    "Maar ik val maar gelijk met de deur in huis: jullie homepage hangt "
    "waarschijnlijk niet helemaal in lijn met die ervaring. Hoe hoog is de "
    "urgentie eigenlijk voor een bezoeker die op jullie site landt? Het verschil "
    "tussen \"interessant, ooit eens kijken\" en \"ik wil dit nu boeken\" valt "
    "of staat met die eerste indruk.\n\n"
    "Herkennen jullie dat zelf ook?\n\n"
    "Bij Aerys bouwen we sites voor {{sector_noemer}} die deze stap willen "
    "zetten. Ik kan een korte Loom maken waarin ik 1 of 2 dingen laat zien die "
    "ik specifiek voor {{bedrijfsnaam}} zou aanpakken. Geen template-Loom, "
    "echt voor jullie.\n\n"
    "Zin in?\n\n"
    "Sami Jansema\n"
    "Founder, Aerys Solution\n"
    "aeryssolution.nl | Groningen"
)

_V3_1_BRUG_WEBSITE_MAIL_2 = (
    "Hoi {{first_name}},\n\n"
    "Geen reactie op mijn vorige? Geen man overboord, ik snap het. Maar ik "
    "bleef er even over nadenken voor {{bedrijfsnaam}} en heb een Loom van 3 "
    "minuten opgenomen. Specifiek, niet generiek, zo zou ik 'm zelf ook niet "
    "willen ontvangen.\n\n"
    "{{LOOM_LINK}}\n\n"
    "Wat denk je? Zinvol, of zit ik mis?\n\n"
    "Sami\n"
    "Aerys Solution"
)

_V3_1_BRUG_WEBSITE_MAIL_3 = (
    "Hoi {{first_name}},\n\n"
    "Laatste van mijn kant, beloofd. Ik nam toch nog een persoonlijk videootje "
    "op, omdat ik vermoed dat 'ie hangt voor {{bedrijfsnaam}}.\n\n"
    "{{VIDEO_LINK}}\n\n"
    "Plus, mocht je liever zelf even kijken: ik heb een mini-audit klaarliggen. "
    "Eén pagina, geen sales, gewoon wat ik zou doen. Stuur 'm graag op als je "
    "'m wil. Anders alle goeds met {{bedrijfsnaam}}, ik laat 'm hier.\n\n"
    "Sami\n"
    "Aerys Solution"
)


# v3.2 Mail 1 (2026-05-07): zie comment boven _V3_1_BRUG_WEBSITE_MAIL_1.
_V3_1_BRUG_WORKFLOW_MAIL_1 = (
    "Hoi {{first_name}},\n\n"
    "Ik keek naar ondernemers in {{stad_of_sector}} met lokale impact en het "
    "viel me op dat {{bedrijfsnaam}} {{sector_impact_frame}}. {{signaal_blok}} "
    "zegt iets, die schaal komt er niet vanzelf.\n\n"
    "Maar ik val maar gelijk met de deur in huis: bij dit soort groei loopt er "
    "meestal werk op de achtergrond mee dat niemand ooit ter sprake brengt. "
    "Vragen die het team de honderdste keer beantwoordt. Bevestigingen die "
    "handmatig rondgaan. No-shows die nabellen kosten.\n\n"
    "We merken dat veel bedrijven in deze fase niet toekomen aan het "
    "standaardiseren van die processen. Te druk met de voorkant, achterkant "
    "blijft handwerk.\n\n"
    "Komt dit jullie bekend voor?\n\n"
    "Bij Aerys zetten we precies dit soort handwerk om in workflows die op de "
    "achtergrond meelopen. Ik kan een korte Loom maken met 2 voorbeelden uit "
    "andere {{sector_noemer}}-projecten en hoe die voor {{bedrijfsnaam}} zouden "
    "werken.\n\n"
    "Zin in?\n\n"
    "Sami Jansema\n"
    "Founder, Aerys Solution\n"
    "aeryssolution.nl | Groningen"
)

_V3_1_BRUG_WORKFLOW_MAIL_2 = (
    "Hoi {{first_name}},\n\n"
    "Geen reactie op mijn vorige? Geen man overboord. Maar ik bleef er even "
    "over nadenken voor {{bedrijfsnaam}} en maakte een Loom van 3 minuten. Twee "
    "voorbeelden, niet generiek, hoe ze concreet bij jullie zouden draaien.\n\n"
    "{{LOOM_LINK}}\n\n"
    "Wat denk je? Zit er iets in voor {{bedrijfsnaam}}, of niet jullie ding?\n\n"
    "Sami\n"
    "Aerys Solution"
)

_V3_1_BRUG_WORKFLOW_MAIL_3 = (
    "Hoi {{first_name}},\n\n"
    "Laatste van mijn kant, beloofd. Ik nam toch een persoonlijk videootje op "
    "waarin ik concreet uitleg wat ik voor {{bedrijfsnaam}} zou opzetten en "
    "waarom dat nu relevant is voor {{sector_noemer}}.\n\n"
    "{{VIDEO_LINK}}\n\n"
    "Plus, als je liever zelf rondkijkt: mini-audit van 1 pagina, gratis, geen "
    "sales. Stuur 'm graag op. Anders veel succes met alles wat eraan komt voor "
    "{{bedrijfsnaam}}, ik laat 'm hier.\n\n"
    "Sami\n"
    "Aerys Solution"
)


# v3.2 Mail 1 (2026-05-07): zie comment boven _V3_1_BRUG_WEBSITE_MAIL_1.
_V3_1_BRUG_AI_AUDIT_MAIL_1 = (
    "Hoi {{first_name}},\n\n"
    "Ik keek naar ondernemers in {{stad_of_sector}} met lokale impact en het "
    "viel me op dat {{bedrijfsnaam}} {{sector_impact_frame}}. {{signaal_blok}}. "
    "Sterk gebouwd.\n\n"
    "Maar ik val maar gelijk met de deur in huis: bij bedrijven die op deze "
    "schaal opereren valt vaak iets op dat de eigenaar zelf niet meer ziet. "
    "Werk dat een paar jaar geleden niks kostte, kost er nu uren. En meestal is "
    "er nooit een moment geweest om te zeggen: hier moeten we eens naar "
    "kijken.\n\n"
    "Komt dit jullie bekend voor?\n\n"
    "Bij Aerys doen we daar een gratis AI Audit voor van 15 minuten. Of als je "
    "liever eerst even ziet wat ik bedoel: ik kan ook een korte Loom maken "
    "specifiek voor {{bedrijfsnaam}}.\n\n"
    "Wat past beter, audit of Loom?\n\n"
    "Sami Jansema\n"
    "Founder, Aerys Solution\n"
    "aeryssolution.nl | Groningen"
)

_V3_1_BRUG_AI_AUDIT_MAIL_2 = (
    "Hoi {{first_name}},\n\n"
    "Geen reactie op mijn vorige? Geen man overboord. Ik nam de Loom-route en "
    "maakte er één van 3 minuten, specifiek voor {{bedrijfsnaam}}.\n\n"
    "{{LOOM_LINK}}\n\n"
    "Eerste minuut is één observatie over jullie site, daarna twee dingen "
    "die wij kunnen oppakken plus één gratis tip om zelf alvast mee te "
    "beginnen.\n\n"
    "Audit-aanbod staat ook nog. Of allebei niet, ook prima om te horen.\n\n"
    "Wat denk je?\n\n"
    "Sami\n"
    "Aerys Solution"
)

_V3_1_BRUG_AI_AUDIT_MAIL_3 = (
    "Hoi {{first_name}},\n\n"
    "Laatste van mijn kant, beloofd. Ik nam toch een persoonlijk videootje op "
    "met een specifiek idee voor {{bedrijfsnaam}}.\n\n"
    "{{VIDEO_LINK}}\n\n"
    "Plus, mocht je het liever op papier hebben: mini-audit van 1 pagina, "
    "gratis. Eén concreet ding waar AI voor jullie zou helpen, geen pitch. Wil "
    "je 'm hebben? Anders alle goeds met {{bedrijfsnaam}}, ik laat 'm hier.\n\n"
    "Sami\n"
    "Aerys Solution"
)


def _v3_1_steps(brug: str) -> list[dict]:
    """Bouw default_steps voor v3.1 brug-template. Cadence: [0, 3, 5]."""
    bodies = {
        "website":  (_V3_1_BRUG_WEBSITE_MAIL_1,  _V3_1_BRUG_WEBSITE_MAIL_2,  _V3_1_BRUG_WEBSITE_MAIL_3),
        "workflow": (_V3_1_BRUG_WORKFLOW_MAIL_1, _V3_1_BRUG_WORKFLOW_MAIL_2, _V3_1_BRUG_WORKFLOW_MAIL_3),
        "ai_audit": (_V3_1_BRUG_AI_AUDIT_MAIL_1, _V3_1_BRUG_AI_AUDIT_MAIL_2, _V3_1_BRUG_AI_AUDIT_MAIL_3),
    }
    m1, m2, m3 = bodies[brug]
    subj_base = "{{bedrijfsnaam}} — even kort"
    return [
        {"subject": subj_base,         "body": m1, "delay_days": 0, "thread": "new"},
        {"subject": "Re: " + subj_base, "body": m2, "delay_days": 3, "thread": "reply"},
        {"subject": "Re: " + subj_base, "body": m3, "delay_days": 5, "thread": "reply"},
    ]


# ---------------------------------------------------------------------------
# pick_brug — routing op basis van enrichment-signalen
# ---------------------------------------------------------------------------

def pick_brug(lead_signals: dict) -> str:
    """Kiest welke v3.1 mail-template-set te gebruiken.

    Returns: 'website', 'workflow', or 'ai_audit'.
    Default voor twijfelgevallen of dunne data: 'ai_audit'.

    Routeert op de GENORMALISEERDE leads.website_score (Sami-keuze, outreach-
    reparatie 2026-07-18). De oude visual_score-tak is verwijderd: die was dood
    op de call-site (de leads-rij heeft geen visual_score-kolom) en verwachtte
    bovendien een 0-100-schaal terwijl website_intelligence.visual_score 0-25 is.

    Herijkt 2026-07-21: de drempel staat op de mediaan (WEBSITE_SCORE_HIGH, 49)
    van de post-dekking-verdeling. NB: voor Fase A is deze routing sinds de
    workflow-brug-schrapping (faseA_brug_for → altijd conceptsite) niet meer
    bepalend; pick_brug voedt nog wel de gedeprecieerde v3.1-flow.
    """
    from config.scoring_thresholds import WEBSITE_SCORE_HIGH
    site_score = lead_signals.get("website_score")
    if site_score is None:
        site_score = 100  # geen analyse -> geen website-punten (conservatief)
    website_age = lead_signals.get("website_age_years") or 0
    cms = (lead_signals.get("cms_detected") or "").strip()

    website_score = 0
    if site_score < WEBSITE_SCORE_HIGH:
        website_score += 40
    if website_age >= 4:
        website_score += 30
    if cms in ("Wix", "Squarespace", "older WordPress"):
        website_score += 20

    review_count = lead_signals.get("google_review_count") or 0
    treatment_count = len(lead_signals.get("treatment_focus") or [])
    locations = lead_signals.get("locations_count") or 1

    workflow_score = 0
    if review_count >= 30:
        workflow_score += 30
    if treatment_count >= 3:
        workflow_score += 30
    if locations >= 2:
        workflow_score += 30

    if website_score >= 70 and website_score > workflow_score:
        return "website"
    if workflow_score >= 70 and workflow_score > website_score:
        return "workflow"
    return "ai_audit"


SEQUENCE_TEMPLATES: dict[str, dict] = {
    # DEPRECATED: v1_cosmetisch_audit + v1_alternatieve_zorg vervangen door
    # v3_1_website / v3_1_workflow / v3_1_ai_audit. /campaigns/launch +
    # /campaigns/preview routen v1-IDs server-side naar v3.1 auto-mode (per-
    # lead pick_brug). Kept for migration safety until 2026-06-06; testing-
    # callers (test_sequence_gate.py) blijven werken zolang ze hier staan.
    "v1_cosmetisch_audit": {
        "id": "v1_cosmetisch_audit",
        "name": "v1.0 — Cosmetische klinieken (audit funnel)",
        "version": "1.0",
        "segment": "NL cosmetische klinieken, 3-6 behandelaars",
        "sector": "cosmetische_behandelaars",
        "language": "nl",
        "cadence_days": [0, 3, 5],
        "primary_cta": "15-min kennismaking → Efficiency Audit",
        "constraints": [
            "Geen prijzen in body",
            "Mail 2 + 3 in reply-thread",
            "Stop-trigger op elke reply",
            "Verzendtijd 08:30-11:00 lokaal, geen weekend/maandag-ochtend",
        ],
        "min_personalization_score": 70,
        "max_sends_per_day_first_month": 40,
        "observation_blocks": list(_OBSERVATION_PARAGRAPHS.keys()),
        # Default sequence rendert met 'fallback' opener — caller bepaalt blok per lead
        "default_steps": get_v1_sequence("fallback"),
    },
    "v1_alternatieve_zorg": {
        "id": "v1_alternatieve_zorg",
        "name": "v1.0 — Alternatieve zorg (vakgesprek funnel)",
        "version": "1.0",
        "segment": "NL alternatieve zorg-praktijken, 1-4 behandelaars",
        "sector": "alternatieve_geneeskunde",
        "language": "nl",
        "cadence_days": [0, 4, 6],
        "primary_cta": "Korte kennismaking → site-sessie",
        "constraints": [
            "Geen 'kliniek' of 'cosmetisch' jargon",
            "Geen 'conversie'/'ROI' woorden — vermijd commerciële taal",
            "Vakinhoudelijk-respectvol, ruimte laten in tone",
            "Mail 2 + 3 in reply-thread, langere cadans (alt-zorg = niet-haastig)",
            "Stop-trigger op elke reply",
        ],
        "min_personalization_score": 65,  # iets lager dan cosmetisch — alt-zorg leads vaak iets dunner data
        "max_sends_per_day_first_month": 30,
        "observation_blocks": list(_OBSERVATION_PARAGRAPHS_ALT.keys()),
        "default_steps": get_v1_alt_sequence(),
    },
    # v3.1 — bridge-based (sector-agnostic, brug gekozen via pick_brug)
    "v3_1_website": {
        "id": "v3_1_website",
        "name": "v3.1 — Brug 1: Website-redesign",
        "version": "3.1",
        "brug": "website",
        "language": "nl",
        "cadence_days": [0, 3, 5],
        "primary_cta": "Korte Loom met website-observaties",
        "constraints": [
            "Geen prijzen in body",
            "Mail 2 + 3 in reply-thread",
            "Stop-trigger op elke reply",
            "{{LOOM_LINK}} en {{VIDEO_LINK}} handmatig invullen pre-send",
        ],
        "min_personalization_score": 70,
        "max_sends_per_day_first_month": 40,
        "default_steps": _v3_1_steps("website"),
    },
    "v3_1_workflow": {
        "id": "v3_1_workflow",
        "name": "v3.1 — Brug 2: Workflow-automatisering",
        "version": "3.1",
        "brug": "workflow",
        "language": "nl",
        "cadence_days": [0, 3, 5],
        "primary_cta": "Korte Loom met 2 workflow-voorbeelden",
        "constraints": [
            "Geen prijzen in body",
            "Mail 2 + 3 in reply-thread",
            "Stop-trigger op elke reply",
            "{{LOOM_LINK}} en {{VIDEO_LINK}} handmatig invullen pre-send",
        ],
        "min_personalization_score": 70,
        "max_sends_per_day_first_month": 40,
        "default_steps": _v3_1_steps("workflow"),
    },
    "v3_1_ai_audit": {
        "id": "v3_1_ai_audit",
        "name": "v3.1 — Brug 3: AI Audit (default voor twijfel)",
        "version": "3.1",
        "brug": "ai_audit",
        "language": "nl",
        "cadence_days": [0, 3, 5],
        "primary_cta": "Gratis AI Audit (15 min) of korte Loom",
        "constraints": [
            "Geen prijzen in body",
            "Mail 2 + 3 in reply-thread",
            "Stop-trigger op elke reply",
            "{{LOOM_LINK}} en {{VIDEO_LINK}} handmatig invullen pre-send",
        ],
        "min_personalization_score": 65,
        "max_sends_per_day_first_month": 40,
        "default_steps": _v3_1_steps("ai_audit"),
    },
}


# DEPRECATED: gebruik pick_brug() + template_for_brug() voor v3.1 productie-flow.
# Deze function blijft bestaan voor scripts/render_warmr_payload.py + test_sequence_gate.py
# tot 2026-06-06. /campaigns/launch + /campaigns/preview gebruiken hem niet meer.
def template_for_sector(sector: str | None) -> str | None:
    """Return de default v1.0 template_id voor een sector. None = geen match.

    NB: alleen v1.0 templates worden gemapt — v3.1 is brug-based, gebruik
    `pick_brug(lead_signals)` daarvoor.
    """
    if not sector:
        return None
    for tmpl_id, tmpl in SEQUENCE_TEMPLATES.items():
        if not tmpl_id.startswith("v1_"):
            continue
        if tmpl.get("sector") == sector:
            return tmpl_id
    return None


def template_for_brug(brug: str) -> str:
    """Map brug-keuze ('website'|'workflow'|'ai_audit') naar v3.1 template_id."""
    return f"v3_1_{brug}"


# ═══════════════════════════════════════════════════════════════════════════
# FASE A — Founding Five (besluit v2 + docs/outreach_implementatie_faseA.md)
# ═══════════════════════════════════════════════════════════════════════════
# Koude mail = observatie ({{opener}}) + één vraag + Founding-Five-aanbod. Twee
# bruggen. Loom = belofte-ná-ja (nooit vooraf gestuurd → geen {{LOOM_LINK}} hier).
# Copy WOORDELIJK uit de blauwdruk; alleen ontwrapt tot e-mail-paragrafen (de
# mid-zin-regelafbrekingen in de bron zijn plak-artefacten). Greeting via
# {{begroeting}} (spec 2), plekken via {{vrije_plekken}} (spec 3, live geteld).

FOUNDING_FIVE_TOTAL = 5
_FASE_A_BRUGGEN = ("conceptsite", "workflow")
_FASE_A_DELAYS = [0, 3, 5]
_FASE_A_SUBJECT = "{{bedrijfsnaam}}, één ding dat me opviel"

# ─── CONCEPTSITE ───────────────────────────────────────────────────────────
_CS_MAIL1 = (
    "{{begroeting}}\n\n"
    "{{opener}}\n\n"
    "Klinieken die dit wél strak hebben staan, winnen 'm nu van de buurpraktijk "
    "nog voor de eerste telefoon. Puur omdat de site meteen vertrouwen wekt.\n\n"
    "Ik doe nu iets eenmaligs: voor de eerste vijf klinieken maak ik kosteloos "
    "een concept voor hun nieuwe site. Zeg je ja, dan laat ik je in een Loom, een "
    "persoonlijke videoboodschap, precies zien wat ik voor {{bedrijfsnaam}} in "
    "gedachten heb.\n\n"
    "Het concept en die video zijn gratis, daar zit je nergens aan vast. En "
    "spreekt het je aan, dan bouwen we 'm echt, voor deze eerste vijf tegen een "
    "gereduceerd tarief in ruil voor je verhaal als referentie. Maar dat is "
    "helemaal aan jou.\n\n"
    "Zal ik er een voor {{bedrijfsnaam}} maken?\n\n"
    "Groet,\nSami Jansema\nAerys Solution · aeryssolution.nl"
)
_CS_MAIL2_BOTH = (
    "{{begroeting}}\n\n"
    "Uit enthousiasme was ik eigenlijk al begonnen. {{detail_2}} liet me niet "
    "los, dus ik zat al wat te schetsen voor {{bedrijfsnaam}}.\n\n"
    "En ik zag iets in jouw regio: {{concurrent_signaal}}. Dat is precies waar "
    "twijfelaars nu op afgaan. De mensen die tussen jullie kiezen, landen op dit "
    "moment bij hen, elke week opnieuw, zonder dat jij het merkt.\n\n"
    "Zal ik dat concept afmaken en je in een korte video laten zien hoe je dat "
    "terugpakt? Kost je niks, en spreekt het aan dan bouwen we 'm echt.\n\nSami"
)
_CS_MAIL2_DETAIL = (
    "{{begroeting}}\n\n"
    "Uit enthousiasme was ik eigenlijk al begonnen. {{detail_2}} liet me niet "
    "los, dus ik zat al wat te schetsen voor {{bedrijfsnaam}}.\n\n"
    "Dat soort dingen zie ik vaker, en het kost stilletjes de bezoekers die "
    "twijfelen, precies degenen die je bijna al binnen had.\n\n"
    "Zal ik dat concept afmaken en je in een korte video laten zien hoe je het "
    "terugpakt? Kost je niks, en spreekt het aan dan bouwen we 'm echt.\n\nSami"
)
_CS_MAIL2_CONCURRENT = (
    "{{begroeting}}\n\n"
    "Ik bleef nog even hangen bij {{bedrijfsnaam}}, dus ik ben alvast wat gaan "
    "schetsen.\n\n"
    "En ik zag iets in jouw regio: {{concurrent_signaal}}. Dat is precies waar "
    "twijfelaars nu op afgaan. De mensen die tussen jullie kiezen, landen op dit "
    "moment bij hen, zonder dat jij het merkt.\n\n"
    "Zal ik dat concept afmaken en je in een korte video laten zien hoe je het "
    "terugpakt? Kost je niks, en spreekt het aan dan bouwen we 'm echt.\n\nSami"
)
_CS_MAIL2_NONE = (
    "{{begroeting}}\n\n"
    "Ik bleef nog even hangen bij {{bedrijfsnaam}}, dus ik ben alvast wat gaan "
    "schetsen voor je nieuwe site.\n\n"
    "Ik wil 'm alleen niet zomaar afmaken zonder dat je 't wilt. Zal ik doorgaan "
    "en je in een korte video laten zien wat ik in gedachten heb? Kost je niks, "
    "en spreekt het aan dan bouwen we 'm echt.\n\nSami"
)
_CS_MAIL3_HEAD = (
    "{{begroeting}}\n\n"
    "Laatste van mijn kant voor nu, beloofd.\n\n"
    "Waarom ik aandring: negen van de tien mensen die op je site landen en "
    "twijfelen, zijn binnen een paar seconden weg naar de volgende praktijk. Je "
    "ziet het nooit, want ze bellen je niet om te zeggen dat ze afhaakten. De "
    "meeste klinieksites laten dat gewoon gebeuren, en dat is precies je kans: "
    "een site die die twijfelaar wél vasthoudt, heeft bijna niemand in deze "
    "markt nu al.\n\n"
    "Maar dat raam sluit. Elke maand stappen er praktijken over, en wie straks "
    "pas begint, haalt in plaats van voorloopt.\n\n"
)

# ─── WORKFLOW ──────────────────────────────────────────────────────────────
_WF_MAIL1 = (
    "{{begroeting}}\n\n"
    "{{opener}}\n\n"
    "De voorkant staat goed. Waar het bij de meeste praktijken stilletjes "
    "weglekt, zit erachter: de beller buiten kantoortijd die geen voicemail "
    "inspreekt, het appje dat pas de volgende dag wordt gezien. Precies de "
    "patiënt die je al bijna binnen had.\n\n"
    "Ik doe nu iets eenmaligs: voor de eerste vijf klinieken breng ik kosteloos "
    "in kaart hoeveel daar bij hen blijft liggen. Zeg je ja, dan loop ik je in "
    "een Loom, een persoonlijke videoboodschap, door wat ik zie en wat het je "
    "waarschijnlijk kost.\n\n"
    "Die analyse is gratis, daar zit je nergens aan vast. En wil je die gemiste "
    "contacten daarna echt opvangen, dan zet ik dat voor deze eerste vijf op "
    "tegen een gereduceerd tarief in ruil voor je verhaal als referentie. Maar "
    "dat is helemaal aan jou.\n\n"
    "Zal ik er voor {{bedrijfsnaam}} eens naar kijken?\n\n"
    "Groet,\nSami Jansema\nAerys Solution · aeryssolution.nl"
)
_WF_MAIL2_BOTH = (
    "{{begroeting}}\n\n"
    "Uit enthousiasme was ik eigenlijk al gaan kijken. {{detail_2}} liet me niet "
    "los, dus ik ben je opzet alvast wat gaan doorlopen.\n\n"
    "En ik zag iets in jouw regio: {{concurrent_signaal}}. Daar zit 'm nu de "
    "winst: de praktijk die als eerste reageert, houdt de twijfelaar. Elke "
    "gemiste beller buiten kantoortijd is er nu eentje die ergens anders wél "
    "wordt teruggebeld, zonder dat jij het terugziet in je agenda.\n\n"
    "Zal ik in een korte video laten zien hoeveel er bij {{bedrijfsnaam}} blijft "
    "liggen en hoe je dat opvangt? Kost je niks, en wil je het echt oppakken dan "
    "zetten we 'm op.\n\nSami"
)
_WF_MAIL2_DETAIL = (
    "{{begroeting}}\n\n"
    "Uit enthousiasme was ik eigenlijk al gaan kijken. {{detail_2}} liet me niet "
    "los, dus ik ben je opzet alvast wat gaan doorlopen.\n\n"
    "Dat soort dingen kost stilletjes de bellers buiten kantoortijd, precies de "
    "patiënten die je al bijna binnen had.\n\n"
    "Zal ik in een korte video laten zien hoeveel er bij {{bedrijfsnaam}} blijft "
    "liggen en hoe je dat opvangt? Kost je niks, en wil je het echt oppakken dan "
    "zetten we 'm op.\n\nSami"
)
_WF_MAIL2_CONCURRENT = (
    "{{begroeting}}\n\n"
    "Ik bleef nog even hangen bij {{bedrijfsnaam}}, dus ik ben je opzet alvast "
    "wat gaan doorlopen.\n\n"
    "En ik zag iets in jouw regio: {{concurrent_signaal}}. Daar zit 'm nu de "
    "winst: de praktijk die als eerste reageert, houdt de twijfelaar. Elke "
    "gemiste beller is er nu eentje die ergens anders wél wordt teruggebeld.\n\n"
    "Zal ik laten zien hoeveel er bij {{bedrijfsnaam}} blijft liggen en hoe je "
    "dat opvangt? Kost je niks, en wil je het oppakken dan zetten we 'm op.\n\nSami"
)
_WF_MAIL2_NONE = (
    "{{begroeting}}\n\n"
    "Ik bleef nog even hangen bij {{bedrijfsnaam}}, dus ik ben je opzet alvast "
    "wat gaan doorlopen.\n\n"
    "Wat me opvalt bij de meeste praktijken: de bellers en appjes buiten "
    "kantoortijd blijven liggen, precies de patiënten die je al bijna binnen "
    "had.\n\n"
    "Zal ik in een korte video laten zien hoe je dat opvangt? Kost je niks, en "
    "wil je het oppakken dan zetten we 'm op.\n\nSami"
)
_WF_MAIL3_HEAD = (
    "{{begroeting}}\n\n"
    "Laatste van mijn kant voor nu, beloofd.\n\n"
    "Waarom ik aandring: bijna geen enkele praktijk in deze markt vangt die "
    "gemiste contacten nu al slim op. Wie er vroeg bij is pakt de patiënten die "
    "de rest laat lopen. Over een jaar doet iedereen het en is het gewoon "
    "bijhouden.\n\n"
    "Maar dat raam sluit. Elke maand stappen er praktijken over, en wie straks "
    "pas begint, haalt in plaats van voorloopt.\n\n"
)

# Plekken-paragraaf (spec 3, live). >=2 toont het getal, ==1 laatste plek,
# ==0 laat de Founding-Five-alinea vallen (puur first-mover-urgentie).
_FF_MULTI = (
    "Die {{vrije_plekken}} plekken tegen gereduceerd tarief zijn er zo niet "
    "meer. Spreekt het je aan, dan is dit het moment.\n\n"
    "Alle goeds met {{bedrijfsnaam}}.\n\nSami"
)
_FF_LAST = (
    "De laatste plek tegen gereduceerd tarief is zo vergeven. Spreekt het je "
    "aan, dan is dit het moment.\n\n"
    "Alle goeds met {{bedrijfsnaam}}.\n\nSami"
)
_FF_SOLDOUT = (
    "Mocht je er later toch eens naar willen kijken, laat het gerust weten. "
    "Alle goeds met {{bedrijfsnaam}}.\n\nSami"
)

_FASE_A: dict[str, dict] = {
    "conceptsite": {
        "mail1": _CS_MAIL1,
        "mail2": {"both": _CS_MAIL2_BOTH, "detail_2": _CS_MAIL2_DETAIL,
                  "concurrent": _CS_MAIL2_CONCURRENT, "none": _CS_MAIL2_NONE},
        "mail3_head": _CS_MAIL3_HEAD,
    },
    "workflow": {
        "mail1": _WF_MAIL1,
        "mail2": {"both": _WF_MAIL2_BOTH, "detail_2": _WF_MAIL2_DETAIL,
                  "concurrent": _WF_MAIL2_CONCURRENT, "none": _WF_MAIL2_NONE},
        "mail3_head": _WF_MAIL3_HEAD,
    },
}


def faseA_brug_for(brug_pick: str) -> str:
    """Map pick_brug-output → Fase A-brug.

    Workflow-brug GESCHRAPT (herijking-keuze 2026-07-21): vuurde de facto nooit
    (locaties≥2-eis) → alle ICP-leads krijgen de conceptsite-brug. De workflow-
    copy blijft in dit bestand maar is onbereikbaar geworden (opruimen kan later)."""
    return "conceptsite"


def _mail2_variant_key(lead: dict) -> str:
    has_d = bool((lead.get("detail_2") or "").strip())
    has_c = bool((lead.get("concurrent_signaal") or "").strip())
    if has_d and has_c:
        return "both"
    if has_d:
        return "detail_2"
    if has_c:
        return "concurrent"
    return "none"


def _mail3_plekken(free_slots: int) -> str:
    if free_slots <= 0:
        return _FF_SOLDOUT
    if free_slots == 1:
        return _FF_LAST
    return _FF_MULTI


def resolve_faseA_step(brug: str, step_index: int, lead: dict, *, free_slots: int) -> dict:
    """Kies de juiste (ongerenderde) Fase A-step: subject + body + delay.

    Variant-keuze: mail 2 op token-beschikbaarheid (degradatie), mail 3 op de
    live plekken-teller. Rendering doet render_step (met een lead-copy die
    'vrije_plekken' bevat)."""
    b = _FASE_A[brug if brug in _FASE_A_BRUGGEN else "conceptsite"]
    if step_index == 0:
        body, subject = b["mail1"], _FASE_A_SUBJECT
    elif step_index == 1:
        body, subject = b["mail2"][_mail2_variant_key(lead)], "Re: " + _FASE_A_SUBJECT
    elif step_index == 2:
        body, subject = b["mail3_head"] + _mail3_plekken(free_slots), "Re: " + _FASE_A_SUBJECT
    else:
        raise IndexError(f"Fase A heeft 3 mails; step_index={step_index}")
    return {
        "subject": subject,
        "body": body,
        "delay_days": _FASE_A_DELAYS[step_index],
        "thread": "new" if step_index == 0 else "reply",
    }
