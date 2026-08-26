"""campaigns/frictie_copywriter.py — Frame A/C copy-engine (heatr-copy skill).

Vervangt de afgekeurde value-first/compliment-boog. DETERMINISTISCH (geen Claude →
nul fabricatie-risico op de zin die het meest moet kloppen).

Twee frames:

  A — Frictie-reveal.  Gebruikt ALLEEN de geen-online-boeking-frictie: de enige
      leak met een consequentie die de lezer logisch kan volgen (telefoon-niet-
      klikbaar en CTA-onder-de-vouw dragen een ánder verhaal en krijgen later een
      eigen frame — Regel 0 geldt óók voor logische dekking, niet alleen datadekking).
      Niche-gesplitst (Regel 7):
        cosmetisch — tegenwerping vooraf weggenomen ("je telefoon gaat genoeg, het
                     gaat om de mensen die níet bellen"); uitkomst = concreet beeld
                     (afspraak om half elf 's avonds), geen feature.
        alt./chiro — bellen is de hóógste drempel voor een twijfelaar → hij stelt uit;
                     NOOIT een groei- of concurrentieframe.

  C — Kale ask.  Geen diagnose; de controlevariant en de fallback als er geen (verse)
      frictie is.

Vers-gate (Sami 2026-08-11): een frictieclaim ouder dan `max_age_days` (of zonder
bekende `analyzed_at`) mag NIET verstuurd worden — render valt dan terug op Frame C
i.p.v. een mogelijk achterhaalde claim de deur uit te doen ("geen bewering over de
buitenwereld zonder verse verificatie in de database").

Schaarste = capaciteit met reason why, en concept/bouw zijn GESPLITST (Sami
2026-08-11): "vijf" slaat op de bouwcapaciteit ("ik neem er vijf tegelijk aan, want
ik bouw ze zelf"), concepten zijn onbeperkt en gratis vooraf. Zo blijft de reason why
waar bij overinschrijving. Loom nooit koud (Regel 8): belofte na 'ja', geen link.
`copy_selfcheck` draait de 10-punts pass/fail-lijst geautomatiseerd.
"""
from __future__ import annotations

import datetime as _dt
import re
from urllib.parse import urlparse

_COSM = "cosmetisch"
_ALT = "alt"


def niche_for_sector(sector: str | None) -> str:
    """Map de Heatr-sector op de twee copy-niches (Regel 7). Chiro valt bij alt."""
    return _COSM if (sector or "") == "cosmetische_behandelaars" else _ALT


# ── frictie-leak (alleen geen-boeking; gedekt door conversion_checks) ─────────
_LEAK_BOOKING = "Er is geen knop om een afspraak te maken. Wie wil boeken, moet bellen."


def select_leak(conversion_details: dict | None) -> tuple[str, str] | None:
    """Returned ('has_online_booking', leak) alleen als online boeken ontbreekt.
    Telefoon-niet-klikbaar / CTA-onder-de-vouw geven bewust None (eigen frame nodig).

    `reverify_uncertain`=True (gezet door de pre-send herverificatie als twee fetches
    het oneens waren of faalden) → None: geen frictieclaim bij een onzekere meting."""
    cd = conversion_details or {}
    if cd.get("reverify_uncertain"):
        return None
    if cd.get("has_online_booking") is False:
        return "has_online_booking", _LEAK_BOOKING
    return None


def select_second_finding(conversion_details: dict | None) -> tuple[str, str] | None:
    """Kies een NIET-generieke tweede vondst voor mail 2 (naast de geen-boeking van
    mail 1). Alleen telefoon-niet-klikbaar of formulier >5 velden — CTA<vouw/WhatsApp/
    chatbot zijn te generiek. None → mail 2 wordt overgeslagen (→ mail 3)."""
    cd = conversion_details or {}
    if cd.get("reverify_uncertain"):
        return None
    if cd.get("has_phone_clickable") is False:
        return "has_phone_clickable", _SECOND_FINDINGS[0][1]
    if (cd.get("form_field_count") or 0) > 5:
        return "form_field_count", _SECOND_FINDINGS[1][1]
    return None


def is_friction_fresh(analyzed_at, *, max_age_days: int = 30, now: _dt.date | None = None) -> bool:
    """True als de website-analyse (analyzed_at) niet ouder is dan max_age_days.
    Onbekende/onparseerbare datum → False (conservatief: geen verse verificatie =
    geen claim)."""
    if not analyzed_at:
        return False
    try:
        d = _dt.date.fromisoformat(str(analyzed_at)[:10])
    except ValueError:
        return False
    today = now or _dt.date.today()
    return (today - d).days <= max_age_days


def friction_reverified_today(conversion_details, *, now: _dt.date | None = None) -> bool:
    """True als conversion_details vandaag opnieuw is geverifieerd (checked_at, gezet
    door scripts/reverify_conversion_checks.py). De send-gate weigert een frictieclaim
    zonder dit — de ochtend-herverificatie is zo een gate, geen procedure."""
    ca = (conversion_details or {}).get("checked_at")
    if not ca:
        return False
    try:
        return _dt.date.fromisoformat(str(ca)[:10]) == (now or _dt.date.today())
    except ValueError:
        return False


# ── niche-copyblokken (alle vier waardevariabelen als vaste constanten, Regel 2)
_COSM_OPENER = "Open {domein} eens op je telefoon, als nieuwe patiënt."
_COSM_CONSEQ = (
    "Je telefoon gaat waarschijnlijk genoeg. Het gaat om de mensen die níet bellen: die "
    "zaten er al, via Google, je reviews, mond-tot-mond. Voor een gemiste oproep krijg "
    "je een melding. Hiervoor niet."
)
_COSM_OFFER = (
    "Ik bouw sites voor cosmetische klinieken waar iemand om half elf 's avonds zijn "
    "afspraak zet, zonder dat er iemand hoeft op te nemen. Drie weken van akkoord tot "
    "live. Het kost je één gesprek en je bestaande foto's, de teksten schrijf ik."
)
_ALT_OPENER = "Open {domein} op je telefoon, als iemand die twijfelt."
_ALT_CONSEQ = (
    "Voor een twijfelaar is bellen de hoogste drempel: hij moet zijn klacht uitleggen aan "
    "iemand die hij niet kent, terwijl hij nog niet weet of dit iets voor hem is. Dus "
    "stelt hij het uit."
)
# Losse punchline (Sami 2026-08-11): staat alleen, zodat 'ie stilvalt i.p.v. wegvalt.
_ALT_PUNCH = "Jij ziet dat niet gebeuren."
_ALT_OFFER = (
    "Ik bouw sites waar dat antwoord al op de pagina staat, zodat mensen weten waar ze "
    "aan beginnen voor ze bellen. Drie weken van akkoord tot live. Het kost je één "
    "gesprek en je bestaande foto's, de teksten schrijf ik."
)
_CAPACITY = (
    "Ik neem er vijf tegelijk aan, want ik bouw ze zelf. Het eerste ontwerp maak ik "
    "vooraf en gratis: in vier minuten Loom zie je je huidige {pad} naast het nieuwe. "
    "Bevalt het niet, dan houd je het ontwerp en hoor je niets meer."
)
_CTA = 'Antwoord met "ja" en hij staat er binnen twee werkdagen.'
_SIGN = "Groet,\nSami Jansema\nAerys Solution, aeryssolution.nl"

# ── mail 2 (bewijs, geen herhaling) — Regel 5 ────────────────────────────────
# Eén concrete, NIET-generieke tweede vondst uit conversion_checks. Prioriteit:
# telefoon-niet-klikbaar > formulier >5 velden. CTA<vouw/WhatsApp/chatbot tellen
# NIET (te generiek — Sami's eigen kritiek): zonder tweede vondst wordt mail 2
# overgeslagen (→ direct mail 3), geen inhoudsloze bump.
_SECOND_FINDINGS: list[tuple[str, str]] = [
    ("has_phone_clickable",
     "je telefoonnummer kun je op mobiel niet aantikken, wie je wil bellen moet het eerst overtypen."),
    ("form_field_count",
     "je contactformulier vraagt meer dan vijf velden voordat iemand iets kan versturen."),
]
_MAIL2 = (
    "Nog iets wat me op {domein} opviel: {second}\n\n"
    "Datzelfde laat ik zien in het gratis ontwerp dat ik aanbood, naast je huidige "
    "pagina. " + _CTA
)
# ── mail 3 (krimp de ask) — Regel 5 ──────────────────────────────────────────
_MAIL3 = (
    "Laatste bericht van mijn kant. Ik hoef geen ja op een heel traject.\n\n"
    "Zeg \"stuur maar\" en het ontwerp staat er binnen twee werkdagen. Je kijkt "
    "wanneer het jou uitkomt, en bevalt het niet, dan hoor je niets meer van me."
)

_NICHE_COPY = {
    #      opener,       consequentie,  offer,       pad,           losse punchline
    _COSM: (_COSM_OPENER, _COSM_CONSEQ, _COSM_OFFER, "mobiele pad", None),
    _ALT: (_ALT_OPENER, _ALT_CONSEQ, _ALT_OFFER, "pad", _ALT_PUNCH),
}


def _domain(lead: dict) -> str:
    """Bare host uit lead.domain/website (of e-maildomein als fallback). '' als onbekend."""
    raw = (lead.get("domain") or lead.get("website") or "").strip()
    if raw:
        host = urlparse(raw if "//" in raw else f"//{raw}").netloc or raw
        host = host.split("/")[0].strip().lower()
        if host.startswith("www."):
            host = host[4:]
        if host:
            return host
    email = (lead.get("email") or "").strip()
    return email.split("@")[-1].lower() if "@" in email else ""


def _greeting(lead: dict) -> str:
    from utils.lead_naming import display_first_name
    first = display_first_name(lead, fallback="")
    return f"Hoi {first}," if first else "Hallo,"


_LEGAL_SUFFIX = ("b.v.", "bv", "n.v.", "nv", "v.o.f.", "vof")
_DANGLING = ("den", "de", "van", "het", "'t", "ter", "ten", "&")


def display_company_name(naam: str) -> str:
    """Display-variant voor mail/onderwerp: zonder juridisch achtervoegsel
    ("Kliniek Ebbelaar B.V." → "Kliniek Ebbelaar"). ALLEEN display — de AVG-
    rechtsvorm-inferentie leest de ruwe naam en blijft onaangeroerd."""
    toks = naam.split()
    while toks and toks[-1].lower().rstrip(".,") in [x.rstrip(".") for x in _LEGAL_SUFFIX]:
        toks = toks[:-1]
    return " ".join(toks) or naam


def _subject(pattern: str, naam: str) -> str:
    """Onderwerpregel van max zes woorden (Regel 4); naam ingekort als nodig.
    Nooit eindigen op een bungel-token ("…CadanCe Huidinstituut Den" — 2026-08-26)."""
    toks = display_company_name(naam).split()
    while toks:
        if toks[-1].lower() in _DANGLING:
            toks = toks[:-1]; continue
        subj = pattern.format(naam=" ".join(toks))
        if len(subj.split()) <= 6:
            return subj
        toks = toks[:-1]
    return pattern.format(naam=naam.split()[0] if naam.split() else naam)


def _assemble(*, greeting: str, blocks: list[str], privacy_notice: str, unsubscribe: str) -> str:
    parts = [greeting, *blocks, _SIGN, f"{privacy_notice}\n{unsubscribe}".strip()]
    body = "\n\n".join(p for p in parts if p and p.strip())
    return re.sub(r"\n{3,}", "\n\n", body).strip()


def build_frictie_mail1(
    lead: dict, *, niche: str, leak: tuple[str, str],
    privacy_notice: str, unsubscribe: str, warmr_owns_unsubscribe: bool = False,
) -> dict | None:
    """Frame A. Returned {subject, body, frame, leak_source, naam} of None."""
    from utils.lead_naming import clean_company_name
    from config.receptie_sequence import receptie_mail_sendable

    naam, needs_review = clean_company_name(lead.get("company_name"))
    naam = (naam or "").strip()
    if not naam or needs_review:
        return None
    domein = _domain(lead)
    if not domein:
        return None
    source, _leak_line = leak                          # Frame A gebruikt de vaste leak
    opener_t, conseq, offer, pad, punch = _NICHE_COPY[niche]

    blocks = [opener_t.format(domein=domein), _LEAK_BOOKING, conseq]
    if punch:
        blocks.append(punch)                           # losse punchline (eigen regel)
    blocks += [offer, _CAPACITY.format(pad=pad), _CTA]
    body = _assemble(greeting=_greeting(lead), blocks=blocks,
                     privacy_notice=privacy_notice, unsubscribe=unsubscribe)
    ok, _reason = receptie_mail_sendable(body, privacy_notice=privacy_notice,
                                         unsubscribe=unsubscribe,
                                         warmr_owns_unsubscribe=warmr_owns_unsubscribe)
    if not ok:
        return None
    return {"subject": _subject("{naam} op mobiel", naam), "body": body,
            "frame": "A", "leak_source": source, "naam": naam}


def build_kale_ask_mail1(
    lead: dict, *, niche: str,
    privacy_notice: str, unsubscribe: str, warmr_owns_unsubscribe: bool = False,
) -> dict | None:
    """Frame C — kale ask, geen diagnose. Vereist niets behalve een schone naam."""
    from utils.lead_naming import clean_company_name
    from config.receptie_sequence import receptie_mail_sendable

    naam, needs_review = clean_company_name(lead.get("company_name"))
    naam = display_company_name((naam or "").strip())
    if not naam or needs_review:
        return None
    # Hormozi-boog (Sami 2026-08-26, docs/besluit_template_herschrijf.md):
    # uitkomst i.p.v. mechaniek + expliciete waarom-gratis + risk-reversal +
    # capaciteits-schaarste. Binnen de eigen regels: nul claims over hún site.
    wat = "cosmetische klinieken" if niche == _COSM else "zorgpraktijken"
    uitkomst = ("zo dat iemand om half elf 's avonds zijn afspraak zet, zonder dat er "
                "iemand hoeft op te nemen" if niche == _COSM else
                "zo dat mensen meteen weten waar ze aan beginnen en direct kunnen boeken")
    blocks = [
        f"Dit najaar bouw ik voor vijf {wat} een nieuwe site. {naam} heb ik op mijn lijstje gezet.",
        ("Het eerste ontwerp maak ik vooraf en gratis: in vier minuten Loom zie je je "
         f"eigen homepage opnieuw opgezet, {uitkomst}. Daarna pas beslis je."),
        ("Waarom gratis: dit worden mijn eerste vijf in deze hoek, en die wil ik "
         "kunnen laten zien. Jij krijgt het ontwerp, hij gaat mijn portfolio in. "
         "Bevalt het niet, dan houd je het ontwerp en hoor je niets meer van me."),
        ("Bevalt het wel: drie weken van akkoord tot live. Het kost je één gesprek en "
         "je bestaande foto's, de teksten schrijf ik."),
        "Ik neem er vijf tegelijk aan, want ik bouw ze zelf.",
        _CTA,
    ]
    body = _assemble(greeting=_greeting(lead), blocks=blocks,
                     privacy_notice=privacy_notice, unsubscribe=unsubscribe)
    ok, _reason = receptie_mail_sendable(body, privacy_notice=privacy_notice,
                                         unsubscribe=unsubscribe,
                                         warmr_owns_unsubscribe=warmr_owns_unsubscribe)
    if not ok:
        return None
    return {"subject": _subject("gratis ontwerp voor {naam}", naam), "body": body,
            "frame": "C", "leak_source": None, "naam": naam}


def _build_followup(lead, *, block, subject_pattern, niche, privacy_notice, unsubscribe,
                    warmr_owns_unsubscribe):
    """Gedeelde assemblage + gate voor mail 2/3 (geen frictie-observatie/waardevars)."""
    from utils.lead_naming import clean_company_name
    from config.receptie_sequence import receptie_mail_sendable

    naam, needs_review = clean_company_name(lead.get("company_name"))
    naam = (naam or "").strip()
    if not naam or needs_review:
        return None, naam
    body = _assemble(greeting=_greeting(lead), blocks=[block],
                     privacy_notice=privacy_notice, unsubscribe=unsubscribe)
    ok, _r = receptie_mail_sendable(body, privacy_notice=privacy_notice, unsubscribe=unsubscribe,
                                    warmr_owns_unsubscribe=warmr_owns_unsubscribe)
    if not ok:
        return None, naam
    return {"subject": _subject(subject_pattern, naam), "body": body, "naam": naam}, naam


def build_frictie_mail2(
    lead: dict, *, niche: str, second: tuple[str, str],
    privacy_notice: str, unsubscribe: str, warmr_owns_unsubscribe: bool = False,
) -> dict | None:
    """Frame A, mail 2 — bewijs: één concrete tweede vondst van hún site, zelfde ask."""
    source, sentence = second
    block = _MAIL2.format(domein=_domain(lead), second=sentence)
    mail, naam = _build_followup(lead, block=block, subject_pattern="{naam} op mobiel",
                                 niche=niche, privacy_notice=privacy_notice, unsubscribe=unsubscribe,
                                 warmr_owns_unsubscribe=warmr_owns_unsubscribe)
    if mail is None:
        return None
    mail.update(frame="2", leak_source=source,
                selfcheck=copy_selfcheck(mail["body"], subject=mail["subject"], niche=niche,
                                         domain=_domain(lead), name=naam, frame="2",
                                         require_value_vars=False,
                                         privacy_notice=privacy_notice, unsubscribe=unsubscribe))
    return mail


def build_frictie_mail3(
    lead: dict, *, niche: str,
    privacy_notice: str, unsubscribe: str, warmr_owns_unsubscribe: bool = False,
) -> dict | None:
    """Frame A, mail 3 — krimp de ask (geen nieuwe claim, dus geen vers-gate nodig)."""
    mail, naam = _build_followup(lead, block=_MAIL3, subject_pattern="{naam} op mobiel",
                                 niche=niche, privacy_notice=privacy_notice, unsubscribe=unsubscribe,
                                 warmr_owns_unsubscribe=warmr_owns_unsubscribe)
    if mail is None:
        return None
    mail.update(frame="3", leak_source=None,
                selfcheck=copy_selfcheck(mail["body"], subject=mail["subject"], niche=niche,
                                         domain=_domain(lead), name=naam, frame="3",
                                         require_value_vars=False,
                                         privacy_notice=privacy_notice, unsubscribe=unsubscribe))
    return mail


def render_frictie_mail(
    lead: dict, *, sector: str | None, conversion_details: dict | None,
    privacy_notice: str, unsubscribe: str, warmr_owns_unsubscribe: bool = False,
    analyzed_at=None, max_age_days: int = 30, now: _dt.date | None = None,
) -> dict | None:
    """High-level: kies frame (A bij een VERSE geen-boeking-frictie, anders C) en
    render mail-1 mét zelfcontrole. Vers-gate: stale/onbekende analyzed_at → geen
    frictieclaim → Frame C (mail['stale_friction']=True). Returned de mail-dict of
    None als zelfs Frame C niet bouwt (naam onbruikbaar)."""
    niche = niche_for_sector(sector)
    leak = select_leak(conversion_details)
    stale = False
    # Verssignaal = checked_at (pre-send herverificatie) vóór analyzed_at (oorspronkelijke
    # analyse): een vandaag-herverifieerde lead is vers, ook als de Vision-analyse oud is.
    freshness_ts = (conversion_details or {}).get("checked_at") or analyzed_at
    if leak is not None and not is_friction_fresh(freshness_ts, max_age_days=max_age_days, now=now):
        leak, stale = None, True                       # verse-gate: geen claim zonder verse data
    mail = None
    if leak is not None:
        mail = build_frictie_mail1(lead, niche=niche, leak=leak,
                                   privacy_notice=privacy_notice, unsubscribe=unsubscribe,
                                   warmr_owns_unsubscribe=warmr_owns_unsubscribe)
    if mail is None:
        mail = build_kale_ask_mail1(lead, niche=niche, privacy_notice=privacy_notice,
                                    unsubscribe=unsubscribe,
                                    warmr_owns_unsubscribe=warmr_owns_unsubscribe)
    if mail is None:
        return None
    mail["niche"] = niche
    mail["stale_friction"] = stale
    mail["selfcheck"] = copy_selfcheck(
        mail["body"], subject=mail["subject"], niche=niche, domain=_domain(lead),
        frame=mail["frame"], name=mail.get("naam", ""),
        privacy_notice=privacy_notice, unsubscribe=unsubscribe)
    return mail


# ── geautomatiseerde zelfcontrole (de 10-punts pass/fail-lijst) ───────────────
_TASTE = ("2010", "stockfoto", "stock foto", "witruimte", "designelement", "ouderwets",
          "gedateerd", "lelijk", "kleurgebruik", "typografie", "amateur", "rommelig")
_BLACKLIST = ("wat steeds terugkomt", "de basis zit goed", "nergens aan vast",
              "winnen 'm van de buurpraktijk", "winnen van de buurpraktijk",
              "iets eenmaligs", "wat ik in gedachten heb",
              "ik hoop dat deze mail je goed bereikt", "even sparren", "vrijblijvend")
_OFFER_SUPERLATIVE = ("state of the art", "op maat", "supermodern", "professionele site")
_UNPROVABLE = ("meer aanvragen", "meer klanten", "meer patiënten", "de meeste praktijken",
               "%", "concurrent")
_GROWTH_FORBIDDEN_ALT = ("buurpraktijk", "concurrent", "meer aanvragen", "meer klanten",
                         "nieuwe aanvragen", "groei", "winnen")
_LOOM_COLD = ("loom.com", "http://", "https://", "bijlage")
# Uitkomst-ankers (Regel 2, punt 4): concreet beeld i.p.v. feature.
_VALUE_OUTCOME = ("hoeft op te nemen", "op de pagina", "waar ze aan beginnen", "opnieuw opgezet")


def _pitch(body: str, privacy_notice: str, unsubscribe: str) -> str:
    """De PERSUASIEVE body waar de tellingen op gelden: begroeting t/m CTA, ZONDER
    de vaste ondertekening en de compliance-footer (boilerplate telt niet mee)."""
    core = body.split("\nGroet,")[0].split("\nSami")[0]
    for foot in (privacy_notice, unsubscribe):
        if foot:
            core = core.replace(foot, "")
    return core


def copy_selfcheck(body: str, *, subject: str, niche: str, domain: str = "", name: str = "",
                   frame: str = "A", require_value_vars: bool = True,
                   privacy_notice: str = "", unsubscribe: str = "") -> dict:
    """Draai de 10-punts pass/fail-lijst uit de skill. Returned
    {passed: bool, fails: [int], detail: {int: reden}, words: int, iks: int}."""
    pitch = _pitch(body, privacy_notice, unsubscribe)
    low = pitch.lower()
    fails: dict[int, str] = {}

    # 1 — geen LOSSTAAND cijfer in de pitch (Frame A/C dragen geen numerieke claim).
    #     Woordgrenzen sparen merknamen met een cijfer erin (Skin8); domein + naam
    #     (beide DB-velden) worden gestript, zodat "Clinic 4 All" mag maar "40%" valt.
    scan = pitch
    for tok in (domain, name):
        if tok:
            scan = scan.replace(tok, "")
    nums = re.findall(r"\b\d+(?:[.,]\d+)?\b", scan)
    if nums:
        fails[1] = f"ongedekt getal: {nums}"
    # 2 — observatie binnen 30s zelf te checken: Frame A moet de frictie-leak dragen.
    if frame == "A" and _LEAK_BOOKING not in body:
        fails[2] = "geen herkenbare frictie-observatie"
    # 3 — smaak i.p.v. frictie
    if any(w in low for w in _TASTE):
        fails[3] = "smaakoordeel aanwezig"
    # 4 — alle vier waardevariabelen (alleen in de pitch/mail-1; mail 2/3 zijn bewijs/
    #     shrink-ask en dragen die last niet)
    if require_value_vars:
        missing = []
        if not any(o in low for o in _VALUE_OUTCOME): missing.append("uitkomst")
        if "gratis" not in low: missing.append("geloof")
        if "drie weken" not in low: missing.append("tijd")
        if "bestaande foto" not in low: missing.append("inspanning")
        if missing:
            fails[4] = f"waardevariabele mist: {missing}"
    # 5 — blacklist-zin/patroon + meer dan één vraagteken
    hit = [p for p in (*_BLACKLIST, *_OFFER_SUPERLATIVE) if p in low]
    if hit:
        fails[5] = f"blacklist: {hit}"
    elif pitch.count("?") > 1:
        fails[5] = f"meer dan één vraag ({pitch.count('?')})"
    # 6 — > 160 woorden in de pitch
    wc = len(pitch.split())
    if wc > 160:
        fails[6] = f"{wc} woorden"
    # 7 — meer dan zeven keer "ik"
    ik = len(re.findall(r"\bik\b", low))
    if ik > 7:
        fails[7] = f'{ik}x "ik"'
    # 8 — houdt stand bij iemand die zijn cijfers kent (geen onbewijsbare claim)
    unprov = [p for p in _UNPROVABLE if p in low]
    if unprov:
        fails[8] = f"onbewijsbare claim: {unprov}"
    # 9 — frame past bij niche (alt/chiro: geen groei-/concurrentieframe)
    if niche == _ALT:
        g = [p for p in _GROWTH_FORBIDDEN_ALT if p in low]
        if g:
            fails[9] = f"groei/concurrentie naar alt. zorg: {g}"
    # 10 — Loom koud aangeboden (link/bijlage in mail 1)
    if any(p in low for p in _LOOM_COLD):
        fails[10] = "Loom-link/bijlage in mail 1"
    # Regel 4 — onderwerpregel max zes woorden
    if subject and len(subject.split()) > 6:
        fails[4] = (fails.get(4, "") + "; " if fails.get(4) else "") + f"onderwerp > 6 woorden ({subject})"

    return {"passed": not fails, "fails": sorted(fails), "detail": fails,
            "words": wc, "iks": ik}
