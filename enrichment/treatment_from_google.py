"""
enrichment/treatment_from_google.py — Map Google Maps category → treatment_focus[].

Google Maps gives elke business 1+ category strings (`Beauty salon`, `Plastic
surgeon`, `Acupuncturist`, etc). Voor de meeste leads is dit voldoende om een
ruwe `treatment_focus[]` lijst te vullen ZONDER Claude-call (€0).

De Claude treatment_classifier blijft de "rich" pad — deze mapper is de
goedkope eerste-orde fill voor leads zonder Claude-output.

Resultaat wordt op `lead.treatment_focus` geschreven als lege list.
"""
from __future__ import annotations

import logging
from typing import Iterable

logger = logging.getLogger(__name__)

# Mapping: lowercase Google-category → list of treatment-focus tags.
# Eerste match wint. Multi-tag is OK ("Beauty salon" → cosmetisch + beauty).
# Houd dit conservatief — beter under-tag dan misclassificatie.
_CATEGORY_MAP: list[tuple[str, list[str]]] = [
    # Cosmetisch — medisch
    ("plastic surgeon",        ["plastic_surgery"]),
    ("plastische chirurg",     ["plastic_surgery"]),
    ("cosmetic surgeon",       ["plastic_surgery", "cosmetic_medicine"]),
    ("cosmetisch arts",        ["cosmetic_medicine", "injectables"]),
    ("dermatologist",          ["dermatology"]),
    ("dermatoloog",            ["dermatology"]),
    ("hair transplantation",   ["hair_transplant"]),
    ("haartransplantatie",     ["hair_transplant"]),
    ("medical spa",            ["medspa", "injectables"]),
    ("aesthetic clinic",       ["cosmetic_medicine", "injectables"]),
    ("esthetische kliniek",    ["cosmetic_medicine"]),
    ("laser hair removal",     ["laser_hair_removal", "ontharing"]),
    ("permanent makeup",       ["pmu"]),
    ("permanente make-up",     ["pmu"]),

    # Cosmetisch — beauty
    ("beauty salon",           ["beauty_salon", "huidverzorging"]),
    ("schoonheidssalon",       ["beauty_salon", "huidverzorging"]),
    ("nail salon",             ["nail_salon"]),
    ("nagelstudio",            ["nail_salon"]),
    ("eyelash salon",          ["eyelash"]),
    ("waxing hair removal",    ["ontharing"]),
    ("skin care clinic",       ["huidverzorging", "huidtherapie"]),
    ("huidtherapeut",          ["huidtherapie"]),
    ("body sculpting",         ["bodycontouring"]),

    # Cosmetisch — tand
    ("cosmetic dentist",       ["cosmetische_tandheelkunde"]),
    ("dentist",                ["tandheelkunde"]),
    ("tandarts",               ["tandheelkunde"]),

    # Alternatief — lichaamswerk
    ("chiropractor",           ["chiropractie", "manueel"]),
    ("osteopath",              ["osteopathie", "manueel"]),
    ("osteopaat",              ["osteopathie", "manueel"]),
    ("physiotherapist",        ["fysiotherapie"]),
    ("fysiotherapeut",         ["fysiotherapie"]),
    ("manual therapy",         ["manueel"]),
    ("manuele therapie",       ["manueel"]),
    ("craniosacral",           ["craniosacraal"]),

    # Alternatief — energetisch
    ("acupuncturist",          ["acupunctuur"]),
    ("acupunctuur",            ["acupunctuur"]),
    ("reiki",                  ["reiki", "energetisch"]),
    ("reflexology",            ["reflexologie"]),
    ("reflexoloog",            ["reflexologie"]),
    ("reflextherapie",         ["reflexologie"]),
    ("ayurveda",               ["ayurveda"]),
    ("shiatsu",                ["shiatsu"]),

    # Alternatief — natuur
    ("homeopath",              ["homeopathie"]),
    ("homeopaat",              ["homeopathie"]),
    ("naturopath",             ["natuurgeneeskunde"]),
    ("natuurgeneeskunde",      ["natuurgeneeskunde"]),
    ("orthomolecular",         ["orthomoleculair"]),
    ("orthomoleculair",        ["orthomoleculair"]),

    # Alternatief — mentaal/lichaam
    ("hypnotherapist",         ["hypnotherapie"]),
    ("hypnotherapeut",         ["hypnotherapie"]),
    ("haptotherapy",           ["haptotherapie"]),
    ("haptotherapeut",         ["haptotherapie"]),
    ("mindfulness",            ["mindfulness"]),

    # Welzijn
    ("dietitian",              ["dietiek", "voedingsadvies"]),
    ("dietist",                ["dietiek", "voedingsadvies"]),
    ("nutritionist",           ["voedingsadvies"]),
    ("voedingsdeskundige",     ["voedingsadvies"]),

    # Massage (kan beide kanten op)
    ("massage therapist",      ["massage"]),
    ("massagepraktijk",        ["massage"]),
    ("sports massage",         ["sportmassage"]),
]


def map_google_category(google_category: str | None) -> list[str]:
    """Return treatment-focus tags inferred from Google category.

    Empty list als no match — caller kan dan fallback naar Claude classifier.
    Multi-tag mogelijk: 'Skin care clinic' → ['huidverzorging', 'huidtherapie'].
    """
    if not google_category:
        return []
    norm = google_category.lower().strip()
    matched: list[str] = []
    for keyword, tags in _CATEGORY_MAP:
        if keyword in norm:
            for tag in tags:
                if tag not in matched:
                    matched.append(tag)
    return matched


def merge_treatment_focus(
    existing: Iterable[str] | None,
    inferred: Iterable[str],
) -> list[str]:
    """Merge bestaande tags met inferred. Behoud volgorde, dedup."""
    out: list[str] = []
    for v in (existing or []):
        if v and v not in out:
            out.append(v)
    for v in inferred:
        if v and v not in out:
            out.append(v)
    return out
