"""
config/retarget_cadence.py — wanneer en hoe vaak een afgesloten gesprek
opnieuw benaderd wordt.

Deze getallen zijn een GOK. Ze staan bewust in config, niet in de flow, omdat
ze bijgesteld moeten worden zodra `/analytics/calls` (de leerlus) genoeg data
heeft — analoog aan hoe `scoring/feedback_processor.py` de ICP-gewichten voedt.

Per uitkomst (zie heatr_call_records.outcome):
  - days:            aantal dagen na rapport (sent/skipped) tot de retarget.
  - max_attempts:    hoeveel keer we retargeten (twee is genoeg; drie keer ben
                     je die vent).
  - use_target_date: als de prospect zélf een moment noemde
                     (timing_target_date), gebruik díe i.p.v. `days` — de
                     prospect weet z'n eigen agenda beter dan onze schatting.
  - event_triggered: tijd overtuigt niemand die de waarde niet ziet; de echte
                     trigger is een gebeurtenis. Voor nu alleen de datum-fallback;
                     de hook `check_retarget_events` staat open voor later.
  - None:            uit de flow (won = klaar, hard_no = niet meer benaderen).
"""
from __future__ import annotations

from typing import Any

RETARGET_CADENCE: dict[str, dict[str, Any] | None] = {
    "timing":   {"days": 75,  "max_attempts": 2, "use_target_date": True},
    "no_value": {"days": 180, "max_attempts": 1, "event_triggered": True},
    "stalled":  {"days": 10,  "max_attempts": 2},
    "hard_no":  None,   # uit de flow
    "won":      None,   # uit de flow
}


def cadence_for(outcome: str | None) -> dict[str, Any] | None:
    """Return de cadans-config voor een uitkomst, of None als die uit de flow is.

    Args:
        outcome: waarde uit heatr_call_records.outcome (timing/no_value/stalled/
            hard_no/won) of None.

    Returns:
        De config-dict, of None (geen retarget: won/hard_no/onbekende uitkomst).
    """
    if not outcome:
        return None
    return RETARGET_CADENCE.get(outcome)


def check_retarget_events(lead_id: str) -> str | None:
    """Detecteer een gebeurtenis die een retarget rechtvaardigt vóór de geplande
    datum (bv. website-score-verandering, concurrentbeweging).

    Bewust nog niet geïmplementeerd — tijd is de fallback in deze fase. Return
    None betekent: geen event, gebruik de datum-cadans. De hook staat hier zodat
    de flow later een echte event-trigger kan inpluggen zonder herschrijven.

    Args:
        lead_id: UUID van de lead.

    Returns:
        Een korte reden-string als er een event is, anders None.
    """
    return None
