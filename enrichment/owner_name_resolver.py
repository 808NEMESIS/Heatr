"""enrichment/owner_name_resolver.py — eigenaarsvoornaam via bron-prioriteit.

Herbouw van de voornaam-enrichment (Sami 2026-07-28). De oude flow pakte structureel
het e-mail-local-part of een domeinfragment ("Hoi Glowclinicnl") — een verkeerde naam
is erger dan geen naam, want die bewijst dat je scrapet zonder te kijken. Deze resolver
zoekt de EIGENAARSvoornaam via drie bronnen in prioriteit, met bron-attributie, en zegt
eerlijk ONBEPAALD (None) als geen bron betrouwbaar matcht.

Bronnen (stop bij de eerste betrouwbare match):
  1. company_name  — bedrijfsnaam bevat een herkenbare persoons-VOORNAAM (gazetteer);
  2. over_ons_role — naam náást een expliciet eigenaar/behandelaar-rolsignaal, via
                     owner_extractor.extract_team_from_page_text (Claude Haiku);
  3. linkedin      — owner/founder gekoppeld aan het bedrijf (DATA NU NIET BESCHIKBAAR:
                     geen contact_linkedin op de leads; structureel inpluggbaar).

Harde regels: NOOIT domein, e-mail-local-part of initiaal als voornaam (safe_first_name-
gate). Bij twijfel / meerdere gelijkwaardige kandidaten → onbepaald, niet gokken.

resolve_owner_first_name is PUUR + testbaar: de caller levert het owner_extractor-team
(de render + Claude-call gebeurt buiten deze module).
"""
from __future__ import annotations

import re

from enrichment.owner_extractor import strip_name_titles
from utils.lead_naming import safe_first_name

# Curated gazetteer van veelvoorkomende NL-voornamen (lowercase). BEWUST conservatief:
# een gemiste naam → onbepaald (veilig), nooit een verkeerde. Niet uitputtend — dat is
# ok, Source 2 vangt de rest, en de blind-check bewaakt de precisie.
_FIRST_NAMES = frozenset({
    # mannelijk
    "joost", "jan", "kees", "piet", "hans", "henk", "gerard", "peter", "paul", "mark",
    "marco", "bas", "bart", "tom", "tim", "rob", "ronald", "richard", "michiel", "maarten",
    "martijn", "sander", "stefan", "sven", "dennis", "jeroen", "erik", "eric", "frank", "freek",
    "guus", "gijs", "daan", "sem", "lucas", "luuk", "finn", "milan", "thijs", "ruben",
    "niels", "koen", "kevin", "dylan", "jesse", "sami", "youssef", "mohammed", "ahmed", "ali",
    "david", "daniel", "simon", "thomas", "willem", "wouter", "arjen", "arjan", "roy", "remco",
    "vincent", "victor", "oscar", "hugo", "frodo", "liem", "raymond", "patrick", "steven", "ivan",
    "chris", "christiaan", "matthijs", "casper", "jasper", "robbert", "robert", "leon", "bram", "ben",
    "abdel", "hassan", "karim", "omar", "samir",
    # vrouwelijk
    "anna", "anne", "emma", "sophie", "sanne", "lisa", "laura", "eva", "julia", "fleur",
    "iris", "lotte", "maud", "roos", "nina", "sara", "sarah", "yara", "noa", "lieke",
    "femke", "esther", "linda", "monique", "marloes", "marije", "marieke", "mieke", "wendy", "kim",
    "manon", "amber", "demi", "britt", "isa", "tess", "evi", "sofie", "nadia", "fatima",
    "karin", "karen", "ellen", "ingrid", "hanneke", "annelies", "annemarie", "jolanda", "petra", "wilma",
    "sandra", "cindy", "chantal", "denise", "vera", "carla", "claudia", "nicole", "natasja", "saskia",
    "renske", "renate", "suzanne", "susan", "cecile", "cecilia", "elise", "eline", "florence", "gwen",
    "hannah", "hanna", "isabel", "isabella", "janneke", "jasmijn", "judith", "katja", "maaike", "melanie",
    "merel", "mirjam", "myrthe", "nienke", "romy", "rosa", "sabine", "selin", "tessa", "veronique",
    "yasmin", "zoe", "zainab", "aisha",
})

# Merk-/plaats-/soortwoorden die NOOIT een voornaam mogen zijn (belt tegen false
# positives, ook als een van deze ooit toevallig in de gazetteer zou glippen).
_NON_NAME = frozenset({
    "glow", "allure", "beauty", "skin", "clinic", "kliniek", "laser", "huid", "praktijk",
    "studio", "medisch", "medical", "cosmetic", "cosmetics", "aesthetic", "aesthetics",
    "esthetic", "derma", "salon", "center", "centrum", "amsterdam", "rotterdam", "utrecht",
    "den", "haag", "eindhoven", "groningen", "clinics", "care", "health", "face", "body",
    "queens", "queen", "royal", "luxe", "pure", "perfect", "institute", "instituut",
})


def first_name_from_company(company_name: str | None) -> str | None:
    """Source 1: een herkenbare persoons-voornaam in de bedrijfsnaam, anders None.

    Match ALLEEN op een token dat in de gazetteer staat én geen merk/plaats/soortwoord
    is. Zo wordt 'Joost Kroon' → 'Joost', maar 'Glow Clinic'/'Laser Queens' → None."""
    if not company_name:
        return None
    cleaned = strip_name_titles(str(company_name))
    for tok in re.findall(r"[A-Za-zÀ-ÿ]{2,}", cleaned):
        low = tok.lower()
        if low in _NON_NAME:
            continue
        if low in _FIRST_NAMES:
            return tok if tok[:1].isupper() else tok.capitalize()
    return None


# Rol-hiërarchie: eigenaar (3) > arts/medisch (2) > behandelaar (1) > overig (0).
_ROLE_TIERS = (
    (3, ("eigenaar", "oprichter", "mede-eigenaar", "dga", "directeur-eigenaar", "praktijkhouder",
         "praktijkeigenaar", "kliniekhouder", "kliniekhoudster", "founder", "co-founder", "owner")),
    (2, ("arts", "dermatoloog", "huidarts", "chirurg", "big", "cosmetisch", "plastisch")),
    (1, ("huidtherapeut", "therapeut", "behandelaar", "specialist", "verpleegkundig")),
)


def _role_tier(role: str | None, is_owner: bool) -> int:
    r = (role or "").lower()
    for tier, kws in _ROLE_TIERS:
        if any(k in r for k in kws):
            return tier
    return 3 if is_owner else 0


def pick_owner(team: list[dict] | None) -> tuple[dict | None, str]:
    """Source 2-selectie: (owner | None, reden).

    Hoogste rol wint (eigenaar > arts > behandelaar). Gelijke top-rol zonder duidelijk
    hoogste confidence → onbepaald (twijfel), niet gokken.
    reden ∈ {ok, geen_team, geen_rolsignaal, meerdere_gelijk}."""
    if not team:
        return None, "geen_team"
    ranked = [(_role_tier(t.get("role"), bool(t.get("is_owner"))), (t.get("confidence") or 0.0), t)
              for t in team]
    top = max(tier for tier, _, _ in ranked)
    if top == 0:
        return None, "geen_rolsignaal"                     # namen zonder eigenaar/behandelaar-rol
    top_members = sorted((t for tier, _, t in ranked if tier == top),
                         key=lambda t: -(t.get("confidence") or 0.0))
    if len(top_members) > 1:
        gap = (top_members[0].get("confidence") or 0.0) - (top_members[1].get("confidence") or 0.0)
        if gap < 0.15:                                     # geen duidelijke winnaar
            return None, "meerdere_gelijk"
    return top_members[0], "ok"


def resolve_owner_first_name(lead: dict, team: list[dict] | None = None) -> tuple[str | None, str, str]:
    """(first_name | None, source, detail). source ∈ {company_name, over_ons_role, linkedin, none}.

    Stop bij de eerste bron die een naam levert die de safe_first_name-gate passeert
    (nooit domein/e-mail-local-part/initiaal). team = owner_extractor-output of None.
    detail licht een onbepaald-uitkomst toe (voor de recovery-meting)."""
    company = lead.get("company_name")

    # Source 1 — persoonsnaam in de bedrijfsnaam.
    fn = first_name_from_company(company)
    if fn:
        v = safe_first_name({"contact_first_name": fn, "company_name": company})  # geen email → geen local-part-afkeur
        if v:
            return v, "company_name", "ok"

    # Source 2 — over-ons/team + expliciet rolsignaal.
    owner, reason = pick_owner(team)
    if owner:
        ofn = strip_name_titles(owner.get("first_name") or owner.get("name") or "")
        v = safe_first_name({"contact_first_name": ofn, "company_name": company})
        if v:
            return v, "over_ons_role", f"role={owner.get('role') or '?'}"

    # Source 3 — LinkedIn owner/founder. DATA NIET BESCHIKBAAR (geen contact_linkedin op
    # de leads). Bewust niet gefabriceerd; inpluggen zodra die data bestaat.

    # Onbepaald: leg de reden vast voor de meting.
    detail = reason if team is not None else ("geen_over_ons_pagina" if fn is None else reason)
    return None, "none", detail
