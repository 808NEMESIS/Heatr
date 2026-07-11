"""
campaigns/observation_opener.py — de koude-mail-opener (fase A PR A3).

Kernbeslissing (2026-07-11): de teardown is GEEN koude stap 1. De koude mail
bevat één rake, menselijke observatie uit de website-analyse + één oprechte
vraag — geen link, geen cijfer, geen rapport. De mail moet de reply verdienen;
pas ná een positieve reply gaat de teardown de deur uit (consent).

Bewust DETERMINISTISCH (mens-geschreven template + het concrete signaal
ingevuld), niet per-lead-AI-proza: dat laatste is precies de generieke
cold-mail-lucht waar de markt in verzuipt (strategie-doc). Een échte,
specifieke observatie is authentieker én controleerbaar/testbaar/gratis.

Prioriteit op HERKENBAARHEID, niet frequentie: 'geen online afspraken' en
'telefoonnummer niet aantikbaar op mobiel' zijn concreet en waardevol voor
een behandelpraktijk; 'geen chatbot' klinkt als een pitch (en iedereen mist
'm, dus niet onderscheidend) → nooit als opener.
"""
from __future__ import annotations

import hashlib

# Signaal → observatievarianten. Twee harde regels: (1) begint NOOIT met "Ik"
# (CLAUDE.md); (2) altijd GEHEDGED ("zag ik zo snel geen…", "leek niet…") —
# nooit een stellige bewering die fout kan zijn bij een valse-negatief in de
# detectie. Bescheiden-en-juist verslaat stellig-en-mogelijk-fout in koude mail.
# Meerdere varianten → deterministische rotatie op lead-id (niet elke mail gelijk).
_OBSERVATIONS: dict[str, list[dict]] = {
    # hoogste herkenbaarheid + waarde voor een behandelpraktijk
    "online_booking": [
        {"obs": "op jullie site zag ik zo snel geen manier om online een afspraak in te plannen",
         "q": "bewust telefonisch gehouden, of staat het op de lijst?"},
        {"obs": "een online-afsprakenoptie kon ik op {domein} niet meteen vinden",
         "q": "regelen jullie dat liever persoonlijk?"},
    ],
    "phone_clickable": [
        {"obs": "op mijn telefoon kon ik jullie nummer niet aantikken om direct te bellen",
         "q": "krijgen jullie veel bezoek via mobiel?"},
        {"obs": "jullie telefoonnummer leek op mobiel niet klikbaar",
         "q": "valt dat jullie op, of hoor ik het nu voor het eerst?"},
    ],
    "whatsapp": [
        {"obs": "een WhatsApp-knop voor een snelle vraag kon ik niet vinden op de site",
         "q": "bewuste keuze, of iets voor later?"},
        {"obs": "op jullie site zag ik geen WhatsApp-optie",
         "q": "merken jullie dat mensen liever bellen?"},
    ],
    # zwakkere fallback (bijna iedereen 'faalt' 'm → minder onderscheidend)
    "cta_above_fold": [
        {"obs": "bovenaan jullie site viel me niet meteen een duidelijke 'maak een afspraak'-knop op",
         "q": "vinden bezoekers 'm volgens jullie makkelijk?"},
    ],
}

# Volgorde = prioriteit op herkenbaarheid/waarde. chatbot + contact_form bewust
# NIET erin (te zwak/pitch-achtig als opener).
_PRIORITY = ["online_booking", "phone_clickable", "whatsapp", "cta_above_fold"]

# check-key in conversion_details → signaal-key hierboven (zelfde namen)
_CONV_KEY = set(_PRIORITY)


# ── Ondertekening ────────────────────────────────────────────────────────────
# Twee versies: LICHT voor de koude mail-1 (minimale links = betere
# deliverability + past bij de zachte toon), VOL vanaf de reply/mail 2 als de
# ontvanger warm is. Telefoon per opgave 2026-07-11: 0620761632.
SENDER_NAME = "Sami"

SIGNATURE_LIGHT = (
    "Groet,\n"
    "Sami\n\n"
    "Sami Jansema · Aerys' Solution · 06 20761632"
)

SIGNATURE_FULL = (
    "Groet,\n"
    "Sami Jansema\n\n"
    "CEO · Aerys' Solution\n"
    "Groningen, Nederland\n"
    "aeryssolution.nl · info@aeryssolution.nl · 06 20761632\n"
    "Plan een gesprek in: {calendar_url}\n\n"
    "\"Van interactie naar impact. Automatisering die voor je werkt.\""
)


def _failed_conversion_signals(wi: dict) -> set[str]:
    out = set()
    for c in (wi.get("conversion_details") or {}).get("details", []):
        if c.get("check") in _CONV_KEY and not c.get("passed"):
            out.add(c["check"])
    return out


def _variant_index(lead_id: str, n: int) -> int:
    """Deterministische variant-keuze per lead (geen randomness → reproduceerbaar)."""
    h = int(hashlib.sha1((lead_id or "").encode()).hexdigest(), 16)
    return h % n if n else 0


def pick_observation(lead: dict, wi: dict) -> dict | None:
    """Kies de sterkste bruikbare observatie. Return None als er geen
    herkenbaar conversie-signaal is (→ lead niet personaliseerbaar, mail-1
    wordt geblokkeerd; fail-closed zoals de personalisatie-gate)."""
    failed = _failed_conversion_signals(wi)
    for sig in _PRIORITY:
        if sig in failed:
            variants = _OBSERVATIONS[sig]
            v = variants[_variant_index(lead.get("id", ""), len(variants))]
            obs = v["obs"].replace("{domein}", lead.get("domain") or "jullie site")
            return {"signal": sig, "observation": obs, "question": v["q"]}
    return None


def render_mail1(lead: dict, wi: dict, *, signature: str = SIGNATURE_LIGHT) -> str | None:
    """Assembleer de koude mail-1 (kort, geen link/cijfer/prijs, begint niet
    met 'Ik'). Default = lichte ondertekening (deliverability + zachte toon).
    Return None als niet personaliseerbaar (fail-closed)."""
    pick = pick_observation(lead, wi)
    if not pick:
        return None
    first = lead.get("contact_first_name")
    greet = f"Hoi {first}," if first else "Hoi,"
    obs = pick["observation"]
    q = pick["question"]
    obs_sentence = obs[0].upper() + obs[1:]
    q_sentence = q[0].upper() + q[1:]
    return (
        f"{greet}\n\n"
        f"{obs_sentence}. {q_sentence}\n\n"
        f"{signature}"
    )
