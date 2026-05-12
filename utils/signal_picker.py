"""
utils/signal_picker.py — Resolver voor v3.1 token {{signaal_blok}}.

Vervangt de eerdere `signal_block_short`-fallback (die "een site die al een
paar jaar meedraait" en "wat ik in jullie online aanwezigheid zag" returnde —
beide passen niet in de v3.1 zin-frames):

  - Brug 1: "Mensen onthouden {{bedrijfsnaam}} van {{signaal_blok}}"
  - Brug 2: "Ik zag {{signaal_blok}} en dacht: deze groeit op een serieus tempo"
  - Brug 3: "{{signaal_blok}}. Sterk gebouwd." (zinbegin)

Prioriteit-keten: kies de hoogste tier waarvoor de lead voldoende data heeft.
Elk return-string is geverifieerd grammaticaal-correct in alle drie frames
(Brug 3 zinbegin-cap wordt door inject_variables gepatcht voor Tier 5/6).
"""
from __future__ import annotations


def pick_signaal_blok(lead: dict) -> str:
    """Resolve {{signaal_blok}} via 6-tier prioriteits-keten.

    Tiers (hoogste eerst):
      1. review-quality         — sterkste reputatie-signaal
      2. treatment/dienst-spread — toont volume + breedte
      3. ad-investering         — toont actieve groei
      4. bedrijfsleeftijd       — gevestigde positie
      5. lokale-zichtbaarheid   — naam in stad
      6. generieke fallback     — laatste redmiddel

    Alle tier-strings zijn handmatig getest in v3.1 zin-frames. Tiers 5+6
    starten met kleine letter (lower-case article) en worden door
    `inject_variables` gecapitalized voor zinbegin-frames (Brug 3).
    """
    # Tier 1 — review-quality
    rating = lead.get("google_rating")
    count = lead.get("google_review_count")
    try:
        if count and rating and int(count) >= 30 and float(rating) >= 4.5:
            return f"{int(count)} reviews met een {rating}-rating"
    except (TypeError, ValueError):
        pass

    # Tier 2 — treatment/dienst-spread (eigennamen → werkt in alle 3 zin-frames)
    treatments = lead.get("treatment_focus") or []
    if isinstance(treatments, list):
        clean = [str(t).strip() for t in treatments if t and str(t).strip()]
        if len(clean) >= 3:
            joined = ", ".join(clean[:3])
            return f"{joined} in jullie aanbod"

    # Tier 3 — ad-investering
    if lead.get("meta_ads_active"):
        # ad_focus is de ge-enrichte kolom; meta_ads_focus is alias voor user-spec compat
        ad_focus = (lead.get("ad_focus") or lead.get("meta_ads_focus") or "").strip()
        if ad_focus:
            return f"Meta Ads-campagnes met focus op {ad_focus}"
        return "Meta Ads-campagnes die actief draaien"

    # Tier 4 — bedrijfsleeftijd (positief frame, NIET website-leeftijd)
    age = lead.get("company_age_years")
    city = (lead.get("city") or "").strip()
    try:
        age_int = int(age) if age is not None else 0
    except (TypeError, ValueError):
        age_int = 0
    if age_int >= 5 and city:
        return f"{age_int} jaar geschiedenis in {city}"

    # Tier 5 — lokale-zichtbaarheid (kleine letter; cap-first wordt door
    # inject_variables gedaan voor zinbegin)
    if city:
        return f"de naam die jullie in {city} hebben opgebouwd"

    # Tier 6 — generieke fallback (kleine letter; idem cap-first)
    return "het werk dat jullie leveren"
