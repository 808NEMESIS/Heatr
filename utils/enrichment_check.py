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

# Statussen waarbij een lead NOOIT verstuurd mag worden — geen enkele bypass,
# ook niet voor test-leads. Unsubscribe/forget is een compliance-belofte aan
# de ontvanger; disqualified is een bewuste operator-beslissing; bounced is
# een deliverability-feit (audit v2 P0-2 / fase 2-hotfix: ontbrak hier,
# waardoor een hard-bounced lead via /campaigns/launch launchbaar bleef —
# dat pad heeft géén email_status-gate).
BLOCKED_STATUSES = ("unsubscribed", "forgotten", "disqualified", "bounced")


def compliance_check(lead: dict[str, Any]) -> tuple[bool, str | None]:
    """GDPR + status compliance-gate. Return (allowed, reason).

    Dit is een HARDE gate — in tegenstelling tot de completeness-check is er
    geen is_test_lead-bypass. Een unsubscribed test-lead blijft geblokkeerd:
    de unsubscribe-belofte geldt ook in smoke-tests. Smoke-test-leads moeten
    daarom gdpr_safe=true + status buiten BLOCKED_STATUSES hebben (eigen
    mailadres, dus zelf te zetten in de setup-SQL).

    Gebruikt door: filter_launchable_leads (campaign launch + preview),
    /leads/send-to-warmr en /leads/{id}/send-review-email.
    """
    if not lead.get("gdpr_safe"):
        return False, f"gdpr_safe={lead.get('gdpr_safe')!r} — lead is niet GDPR-veilig"
    status = lead.get("status")
    if status in BLOCKED_STATUSES:
        return False, f"status={status!r} — verzending permanent geblokkeerd"
    return True, None

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

    # Compliance eerst — deze block kent GEEN test-lead-bypass.
    compliance_ok, compliance_reason = compliance_check(lead)

    is_complete = len(missing_required) == 0
    blocked_reason = None
    if not compliance_ok:
        blocked_reason = f"GDPR/status-block: {compliance_reason}"
    elif not is_complete and not is_test:
        blocked_reason = (
            f"Mist verplichte velden: {', '.join(missing_required)}. "
            f"Re-enqueue voor enrichment via /admin/re-enqueue-stale-leads."
        )

    return {
        "lead_id": lead.get("id"),
        "company_name": lead.get("company_name"),
        "is_complete": is_complete,
        "is_test_lead": is_test,
        "compliance_blocked": not compliance_ok,
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
        # Compliance-block wint van ALLES — ook van de test-lead-bypass.
        if check["compliance_blocked"]:
            lead["_completeness"] = check
            blocked.append(lead)
        elif check["is_complete"] or check["is_test_lead"]:
            lead["_completeness"] = check
            launchable.append(lead)
            if check["missing_recommended"]:
                warnings.append(check)
        else:
            lead["_completeness"] = check
            blocked.append(lead)

    return launchable, blocked, warnings
