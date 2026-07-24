"""
config/receptie_sequence.py — receptie-mailsequentie (Fase 3b).

Componeert mail 1/2/3 uit de receptie-haakje-ladder (hook_detector →
hook_templates.build_haakje) op het verzendmoment. Structuur volgt Sami's spec
(2026-07-24):

  Mail 1 (dag 0)  begroeting + haakje + onzichtbaarheid + zonde-brug (reviews) +
                  minimale ask. GEEN product/oplossing/call genoemd.
  Mail 2 (dag 3)  ALLEEN als er een tweede WÁRE haak uit een ánder thema is
                  (second_hook); anders overgeslagen → direct mail 3.
  Mail 3 (dag 5)  capaciteit-schaarste, haak-specifiek ('doorlopende lek').

Twee compliance-tokens zijn HARDE gates (Sami 2026-07-24): {{privacy_notice}}
(AVG art. 14, herkomst uit externe bronnen) en {{unsubscribe}} (afmeldlink).
Zolang die leeg zijn weigert `receptie_mail_sendable` de mail — dat is het
gewenste gedrag; Sami levert de teksten/URL apart aan.

F4 (hard): geen eerste-persoons-tijdsclaim of geënsceneerde context in de HELE
mail. Em-/en-dash verboden (QA-gate). Max één vraag per mail.
"""
from __future__ import annotations

import os
import re
from typing import Any

from config.hook_templates import build_haakje, build_zonde_brug


def receptie_compliance_tokens(lead: dict) -> tuple[str, str]:
    """Privacyzin (AVG art.14) + afmeldlink uit env — Sami levert die aan. Leeg →
    de render-gate (receptie_mail_sendable) blokkeert de send (gewenst gedrag).

    RECEPTIE_PRIVACY_NOTICE: de herkomst-/privacyzin met link.
    RECEPTIE_UNSUBSCRIBE_TEMPLATE: afmeldregel met {email}/{id}-placeholders."""
    privacy = (os.getenv("RECEPTIE_PRIVACY_NOTICE") or "").strip()
    tmpl = (os.getenv("RECEPTIE_UNSUBSCRIBE_TEMPLATE") or "").strip()
    unsub = ""
    if tmpl:
        unsub = (tmpl.replace("{email}", lead.get("email") or "")
                     .replace("{id}", str(lead.get("id") or "")))
    return privacy, unsub

_SUBJECT = "{{bedrijfsnaam}}, één ding dat me opviel"

# Mail 1 — dag 0. {{haakje}} = mail-1 haak, {{zonde_brug}} = review-onderbouwing.
_RC_MAIL1 = (
    "{{begroeting}}\n\n"
    "{{haakje}}\n\n"
    "Het vervelende is dat je dat nooit ziet: ze belt je niet om te zeggen dat "
    "ze afhaakte. {{zonde_brug}}\n\n"
    "Zal ik je in twee minuten laten zien wat ik bedoel? Ik neem dan even je "
    "eigen site door en laat precies zien waar het misloopt. Kost je niks en je "
    "zit nergens aan vast.\n\n"
    "Groet,\n"
    "Sami Jansema\n"
    "Aerys Solution, aeryssolution.nl\n\n"
    "{{privacy_notice}}\n"
    "{{unsubscribe}}"
)

# Mail 2 — dag 3. Alleen bij een tweede thema-haak. {{haakje_2}} = second_hook.
# {{overgang}} is CONDITIONEEL (Sami 2026-07-24): versterkende combinaties (het gat
# wordt onzichtbaar dóór het tweede signaal, bv. Q4+Q7: niet kunnen vastleggen én
# niet meten) krijgen de causale zin; losse combinaties iets neutraals zónder
# causale claim (geen prijs maakt geen-meting niet lastiger).
_RC_MAIL2 = (
    "{{begroeting}}\n\n"
    "{{overgang}} {{haakje_2}}\n\n"
    "Zal ik het je even laten zien? Twee minuten, op je eigen site.\n\n"
    "Sami\n\n"
    "{{privacy_notice}}\n"
    "{{unsubscribe}}"
)

def receptie_shells() -> list[tuple[str, str]]:
    """(subject, body-shell) per receptie-mail (1/2/3), voor de Warmr campaign-
    create. De ECHTE body wordt live geresolved door _render_receptie_marker op
    het verzendmoment (met haakje/tokens/gates); dit is enkel de shell die de
    campaign-create accepteert. Cadence 0/3/5 zit in receptie_faseA_steps."""
    return [
        (_SUBJECT, _RC_MAIL1),
        ("Re: " + _SUBJECT, _RC_MAIL2),
        ("Re: " + _SUBJECT, _RC_MAIL3),
    ]


_MAIL2_OVERGANG_VERSTERKEND = (
    "Nog één ding dat me opviel, en dit maakt het eerste lastiger dan het lijkt."
)
_MAIL2_OVERGANG_NEUTRAAL = "Nog één ding dat me opviel, op een heel ander punt."
# Versterkende paren: het tweede signaal maakt het eerste gat onzichtbaar/erger.
# Q4 (niet kunnen vastleggen) + Q7 (niet meten) = het lek is onzichtbaar.
_VERSTERKENDE_PAREN = frozenset([frozenset({"Q4", "Q7"})])

# Mail 3 — dag 5. Capaciteit-schaarste, geen deadline/teller. {{lek_doorloop}}
# is haak-specifiek zodat de urgentie uit het mail-1-lek komt (niet generiek).
_RC_MAIL3 = (
    "{{begroeting}}\n\n"
    "Laatste van mijn kant, beloofd.\n\n"
    "Ik doe dit met een handjevol praktijken tegelijk, vijf om precies te zijn, "
    "omdat ik er bij elke persoonlijk naast zit. Dat is geen verkooptruc, het is "
    "gewoon wat ik aankan.\n\n"
    "Ondertussen loopt het door: {{lek_doorloop}}. Dat is precies waarom ik het "
    "zonde vind bij een praktijk die er verder goed voor staat.\n\n"
    "Wil je die twee minuten alsnog, laat het weten. Anders alle goeds met "
    "{{bedrijfsnaam}}.\n\n"
    "Sami\n\n"
    "{{privacy_notice}}\n"
    "{{unsubscribe}}"
)

# Haak-specifiek 'doorlopende lek' voor mail 3 (het lek dat de kliniek zelf niet ziet).
_LEK_DOORLOOP = {
    "Q4": "elke avond dat niemand kan vastleggen, gaan er mensen weg die je nooit "
          "terugziet in je cijfers",
    "Q7": "je blijft mensen mislopen zonder dat het ergens in je cijfers opduikt",
    "Q2": "wie op het moment zelf wil boeken en dat niet kan, is vaak weg voordat "
          "je 'm gesproken hebt",
    "P1": "wie niet weet wat het ongeveer kost, klikt weg voordat je het merkt",
}

# F4: geen geclaimde persoonlijke actie / tijdstip in de HELE mail.
_F4_TIME_CLAIM = re.compile(
    r"gisteravond|vanavond|vanochtend|vannacht|\bik keek\b|\bik zat\b|\bik wilde\b|"
    r"\bik opende\b|\bik probeerde\b|op mijn telefoon|'s avonds op de bank",
    re.IGNORECASE,
)


def receptie_mail_sendable(body: str, *, privacy_notice: str, unsubscribe: str) -> tuple[bool, str]:
    """Harde render-gate. Weigert zolang een compliance-token leeg is of er iets
    onopgelost/verboden in de body staat. Fail-closed: bij twijfel niet-sendable."""
    if not (privacy_notice or "").strip():
        return False, "privacy_notice_missing"      # AVG art. 14 — Sami levert aan
    if not (unsubscribe or "").strip():
        return False, "unsubscribe_missing"          # afmeldlink — Sami verifieert
    if not body or not body.strip():
        return False, "empty_body"
    if "{{" in body or re.search(r"{[a-z_]+}", body):
        return False, "unresolved_token"
    if "—" in body or "–" in body:
        return False, "em_dash"
    if _F4_TIME_CLAIM.search(body):
        return False, "f4_time_claim"
    if "Hoi ," in body or "Hoi ,\n" in body or body.startswith("Hoi ,"):
        return False, "bare_greeting"                # voornaam-gate: nooit kale Hoi,
    # 'max één vraag' geldt voor de PITCH, niet voor de compliance-footer (een
    # afmeldregel als "Geen mail meer?" mag een vraagteken hebben).
    core = body
    for foot in (privacy_notice, unsubscribe):
        if foot:
            core = core.replace(foot, "")
    if core.count("?") > 1:
        return False, "multiple_questions"
    return True, "ok"


def render_receptie_mail(
    step: int,
    lead: dict,
    *,
    hook_code: str | None,
    second_hook: str | None = None,
    hook_variant: str | None = None,
    second_variant: str | None = None,
    privacy_notice: str = "",
    unsubscribe: str = "",
    seed: str | None = None,
) -> dict[str, Any]:
    """Render één receptie-mail (step 1/2/3) op leaddata. Returned altijd een dict
    met {step, subject, body, sendable, block_reason, hook_code, second_hook,
    skipped}. Verstuurt NIETS; puur compositie + gate.

    Mail 2 zonder second_hook → skipped=True (geen mail 2, ga naar mail 3)."""
    from utils.lead_naming import display_first_name
    first = display_first_name(lead, fallback="")
    company = (lead.get("company_name") or "").strip()
    # Voornaam-gate (Sami 2026-07-24): een geldige voornaam → "Hoi {naam},";
    # geen (of junk, afgevangen in safe_first_name) → "Hallo," — nooit "Hoi ,".
    begroeting = f"Hoi {first}," if first else "Hallo,"

    if step == 2 and not second_hook:
        return {"step": 2, "skipped": True, "subject": None, "body": None,
                "sendable": False, "block_reason": "no_second_hook",
                "hook_code": hook_code, "second_hook": None}

    haakje = build_haakje(hook_code, seed or lead.get("id"), kliniek=company or None,
                          variant=hook_variant, stad=lead.get("city"))
    zonde = build_zonde_brug(lead.get("google_review_count"), lead.get("google_rating"))
    haakje_2 = build_haakje(second_hook, seed or lead.get("id"), kliniek=company or None,
                            variant=second_variant, stad=lead.get("city")) if second_hook else ""
    lek = _LEK_DOORLOOP.get(hook_code or "", "")
    # mail-2 overgang: causaal alleen bij een versterkend paar (bv. Q4+Q7).
    overgang = (_MAIL2_OVERGANG_VERSTERKEND
                if frozenset({hook_code, second_hook}) in _VERSTERKENDE_PAREN
                else _MAIL2_OVERGANG_NEUTRAAL)

    body = {1: _RC_MAIL1, 2: _RC_MAIL2, 3: _RC_MAIL3}.get(step, _RC_MAIL1)
    subject = _SUBJECT.replace("{{bedrijfsnaam}}", company)
    if step in (2, 3):
        subject = "Re: " + subject
    repl = {
        "{{begroeting}}": begroeting, "{{bedrijfsnaam}}": company,
        "{{haakje}}": haakje, "{{zonde_brug}}": zonde, "{{haakje_2}}": haakje_2,
        "{{overgang}}": overgang, "{{lek_doorloop}}": lek,
        "{{privacy_notice}}": privacy_notice, "{{unsubscribe}}": unsubscribe,
    }
    for k, v in repl.items():
        body = body.replace(k, v or "")
    body = re.sub(r"\n{3,}", "\n\n", body).strip()

    ok, reason = receptie_mail_sendable(body, privacy_notice=privacy_notice, unsubscribe=unsubscribe)
    return {"step": step, "skipped": False, "subject": subject, "body": body,
            "sendable": ok, "block_reason": reason,
            "hook_code": hook_code, "second_hook": second_hook if step == 2 else None}
