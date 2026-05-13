"""
utils/enrichment_check.py — Pre-launch completeness check.

Voor elk lead: verifieer dat de enrichment-pipeline kritieke velden heeft
gevuld vóór we een sequence-launch toelaten. Voorkomt:

  - Z5: 0% archetype-coverage initieel — leads zonder archetype krijgen
    verkeerde sequence-tone (cosmetisch ipv alt-zorg of vice versa)
  - Z1: sequence-tekst niet gereviewd — leads zonder personalized_opener
    en zonder static fallback krijgen lege Mail-1

Hard-required velden (launch wordt geweigerd als één ontbreekt):
  - archetype                 (sequence-routing wint of valt hier)
  - score                     (lead-quality basisaanname)
  - sector                    (template-keuze afhankelijk)

Soft-recommended velden (warning, geen block):
  - personalized_opener of static fallback (sowieso afgevangen door
    is_quality_opener / static-block, maar zichtbaar maken)
  - contact_first_name        (anders 'Hoi daar'-fallback)
  - treatment_focus           (sequence-context, nice-to-have)

Test-lead bypass: leads met is_test_lead=True passeren altijd.
"""
from __future__ import annotations

from typing import Any

# Velden die ALTIJD gevuld moeten zijn voor productie-launch
HARD_REQUIRED_FIELDS = ("archetype", "score", "sector")

# Velden die we WAARSCHUWEN over maar niet blokkeren
SOFT_RECOMMENDED_FIELDS = (
    "personalized_opener",
    "contact_first_name",
    "treatment_focus",
)


def _is_filled(lead: dict, field: str) -> bool:
    """True als veld een betekenisvolle waarde heeft (niet None, niet leeg, niet 0)."""
    val = lead.get(field)
    if val is None:
        return False
    if isinstance(val, str) and not val.strip():
        return False
    if isinstance(val, list) and not val:
        return False
    if field == "score" and val == 0:
        # Score=0 is niet enriched (zou >0 moeten zijn na scoring step)
        return False
    return True


def check_lead_completeness(lead: dict[str, Any]) -> dict[str, Any]:
    """Check één lead. Returns:
        {
            "lead_id": str,
            "company_name": str,
            "is_complete": bool,        # alle hard-required gevuld?
            "is_test_lead": bool,       # bypass-flag aanwezig
            "missing_required": list[str],
            "missing_recommended": list[str],
            "blocked_reason": str | None,  # alleen gezet als is_complete=False EN niet test-lead
        }
    """
    is_test = bool(lead.get("is_test_lead"))
    missing_required = [f for f in HARD_REQUIRED_FIELDS if not _is_filled(lead, f)]
    missing_recommended = [f for f in SOFT_RECOMMENDED_FIELDS if not _is_filled(lead, f)]

    is_complete = len(missing_required) == 0
    blocked_reason = None
    if not is_complete and not is_test:
        blocked_reason = (
            f"Mist verplichte velden: {', '.join(missing_required)}. "
            f"Re-enqueue voor enrichment via /admin/re-enqueue-stale-leads."
        )

    return {
        "lead_id": lead.get("id"),
        "company_name": lead.get("company_name"),
        "is_complete": is_complete,
        "is_test_lead": is_test,
        "missing_required": missing_required,
        "missing_recommended": missing_recommended,
        "blocked_reason": blocked_reason,
    }


def filter_launchable_leads(
    leads: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Split leads in (launchable, blocked, warnings).

    - launchable: hard-required velden compleet OF is_test_lead=True
    - blocked:    hard-required velden ontbreken EN niet test-lead
    - warnings:   launchable maar soft-recommended velden missen (informatief)

    Test-leads landen ALTIJD in launchable, ook als alles ontbreekt
    behalve email (is_sendable handelt dat af).
    """
    launchable: list[dict] = []
    blocked: list[dict] = []
    warnings: list[dict] = []

    for lead in leads:
        check = check_lead_completeness(lead)
        if check["is_complete"] or check["is_test_lead"]:
            lead["_completeness"] = check
            launchable.append(lead)
            if check["missing_recommended"]:
                warnings.append(check)
        else:
            lead["_completeness"] = check
            blocked.append(lead)

    return launchable, blocked, warnings
