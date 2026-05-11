"""
campaigns/reply_drafter.py — Claude Haiku schrijft een draft-antwoord op een
inbound reply. Sami leest, edit, kopieert naar mailclient en stuurt zelf.

Heatr verstuurt NIETS automatisch — dit is alleen een suggestie. Per v1.0 spec:
"Sami handmatig binnen 4 werkuren. Geen templates, geen auto-responses."
Dit module schrijft *individueel*, niet uit templates, en de draft is altijd
handmatig te overrulen.

Per category een aangepaste tone:
  - interested    → bevestigen + concrete next step (kennismaking voorstel)
  - question      → korte feitelijke beantwoording, geen sales-druk
  - not_now       → erkennen + rustige re-entry voorstel ("kom ik in september terug")
  - wrong_person  → bedanken + vragen of doorsturen mogelijk is
  - unsubscribe   → één zin: "uitgeschreven, dank voor de melding"
  - not_interested→ neutraal afsluiten, geen tegenargumenten
  - auto_reply    → geen draft (skip — niet zinvol om OOO te beantwoorden)
  - other         → korte open vraag-bevestiging

Cost: 1 Haiku call per draft (~€0.0005), gecached op (reply_id, last_classified_at).
Re-clicks na cache-hit kosten €0.

Guarded via utils.cost_guard daily-budget alleen (geen per-lead ceiling — replies
zijn niet gerelateerd aan een specifieke enrichment-run).
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from config.pricing import get_legacy_per_m_eur
from utils.claude_cache import log_api_cost

logger = logging.getLogger(__name__)

_HAIKU_MODEL = "claude-haiku-4-5-20251001"
# Pricing centraal in config/pricing.py.
_PRICING = get_legacy_per_m_eur(_HAIKU_MODEL)
_COST_PER_M_INPUT = _PRICING["input_per_m_eur"]
_COST_PER_M_OUTPUT = _PRICING["output_per_m_eur"]

# Categories die we niet beantwoorden (return None — UI toont "geen draft nodig")
_SKIP_CATEGORIES = {"auto_reply", "unsubscribe_request"}

# Cal.com / scheduling URL — verplicht via env. Geen default-URL meer omdat een
# fake URL prospects naar 404 stuurt en vertrouwen breekt. Als env niet gezet:
# `_SCHEDULING_URL = ""` → auto-injection wordt overgeslagen, Claude moet zelf
# een vervolgstap formuleren in de draft.
_SCHEDULING_URL = (os.getenv("HEATR_SCHEDULING_URL") or "").strip()

# Heuristics voor quote-trimmen — gangbare email-client markers waarna alleen
# quoted text volgt. Eerste match wint, alles daarna wordt gestripped.
_QUOTE_MARKERS = [
    r"^On\s+.{1,80}\s+wrote:\s*$",
    r"^Op\s+.{1,80}\s+schreef\s+.{1,80}:\s*$",
    r"^From:\s+.+",
    r"^Van:\s+.+",
    r"^-----Original\s+Message-----",
    r"^-----Oorspronkelijk\s+bericht-----",
    r"^>+\s",  # plain quote-prefix lines
]
_QUOTE_REGEXES = [re.compile(p, re.MULTILINE | re.IGNORECASE) for p in _QUOTE_MARKERS]


def trim_quoted_thread(body: str) -> str:
    """Strip alles vanaf de eerste quote-marker. Bewaart de top — wat de
    prospect daadwerkelijk schreef — en gooit hun gequoteerde versie van onze
    eerdere mails weg.

    Returns getrimde body. Als trim 0 chars overlaat (alleen quote, geen reply
    erboven) → return origineel. Korte replies tot ~15 chars worden bewaard
    zonder trim om "Yes, please." en "OK!"-style replies niet te beschadigen.
    """
    if not body:
        return body
    earliest = len(body)
    for rx in _QUOTE_REGEXES:
        m = rx.search(body)
        if m and m.start() < earliest:
            earliest = m.start()
    if earliest == len(body):
        return body.strip()
    trimmed = body[:earliest].strip()
    # Edge-cases waar trim niets bruikbaars overlaat:
    # - empty (puur quoted reply zonder eigen tekst)
    # - <8 chars (waarschijnlijk een fragment, geen volledige zin)
    if len(trimmed) < 8:
        return body.strip()
    return trimmed


def hours_since(received_at: str | datetime | None) -> int | None:
    """Return integer aantal uren tussen `received_at` en nu (UTC)."""
    if not received_at:
        return None
    dt: datetime | None = None
    if isinstance(received_at, datetime):
        dt = received_at
    else:
        try:
            dt = datetime.fromisoformat(str(received_at).replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = datetime.now(timezone.utc) - dt
    return max(int(delta.total_seconds() // 3600), 0)


def _timing_instruction(hours: int | None) -> str:
    """Korte instructie voor Claude over hoe te reageren op de wachttijd."""
    if hours is None:
        return ""
    if hours <= 2:
        return "Geen vermelding van wachttijd nodig — antwoord komt binnen 2 uur."
    if hours <= 24:
        return "Antwoord komt binnen een werkdag — geen apologize nodig."
    if hours <= 72:
        return f"Antwoord na ~{hours}u — kort acknowledgen kan, maar niet overdrijven."
    days = hours // 24
    return f"Antwoord na ~{days} dagen — open met korte erkenning ('Dank voor je geduld' of vergelijkbaar), niet uitgebreid sorry zeggen."


def _ensure_scheduling_link(draft: str, category: str) -> tuple[str, bool]:
    """Voor `interested`-drafts: voeg scheduling-link toe als HEATR_SCHEDULING_URL
    is gezet én niet al in de draft staat. Geen URL gezet → skipt silently
    (Claude's draft is al een geldig antwoord zonder link).
    """
    if category != "interested" or not draft or not _SCHEDULING_URL:
        return draft, False
    if _SCHEDULING_URL.lower() in draft.lower() or "cal.com" in draft.lower():
        return draft, False
    addition = f"\n\nP.S. Direct plannen kan via {_SCHEDULING_URL}."
    return draft.rstrip() + addition, True


# Per-category tone-instructie. Sami's signature wordt apart geappend.
_CATEGORY_GUIDANCE: dict[str, str] = {
    "interested": (
        "De prospect toont interesse. Bevestig kort (1 zin), stel ÉÉN concrete "
        "vervolgstap voor: een 15-min kennismaking via Cal.com link. Niet sales-y, "
        "niet enthousiast — gewoon doorzakken naar planning. Max 50 woorden."
    ),
    "question": (
        "De prospect heeft een feitelijke vraag gesteld. Beantwoord direct en kort. "
        "Eindig met een open vraag of de info voldoende is — geen verkoop-pitch. "
        "Max 60 woorden."
    ),
    "not_now": (
        "De prospect zegt: 'niet nu'. Erken dat zonder druk. Stel voor om over "
        "X maanden terug te komen (X afhankelijk van wat ze zelf noemden, anders "
        "3 maanden default). Geen tegenargumenten. Max 40 woorden."
    ),
    "wrong_person": (
        "We hebben de verkeerde persoon te pakken. Bedank kort, vraag of ze ons "
        "naar de juiste contactpersoon kunnen doorverwijzen. Max 35 woorden."
    ),
    "not_interested": (
        "De prospect is niet geïnteresseerd. Erken dat neutraal, bedank voor de "
        "tijd, sluit af. Geen 'jammer' of 'misschien later' — gewoon nette afsluiting. "
        "Max 30 woorden."
    ),
    "other": (
        "Onduidelijke reply. Schrijf een korte vriendelijke open vraag om te "
        "verduidelijken wat ze bedoelen. Max 35 woorden."
    ),
}

_SYSTEM_PROMPT = (
    "Je bent Sami Jansema, oprichter van Aerys. Je schrijft persoonlijke "
    "antwoorden op replies van prospects. Geen templates, geen sales-jargon, "
    "geen 'fijn dat je reageert'-fluff. Direct, menselijk, kort.\n\n"
    "Regels:\n"
    "- Begin met de voornaam + komma\n"
    "- Geen formele aanhef ('Geachte', 'Beste')\n"
    "- Sluit af met '— Sami'\n"
    "- Houd je strikt aan de woordlimiet uit de category-guidance\n"
    "- Als de reply expliciet om iets vraagt: beantwoord dat eerst\n"
    "- Geen verkooppraatje toevoegen waar de prospect daar niet om vroeg"
)


async def draft_reply(
    reply_inbox_row: dict[str, Any],
    lead: dict[str, Any] | None,
    classification: dict[str, Any],
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
    original_emails: list[dict] | None = None,
) -> dict[str, Any]:
    """Generate een draft-antwoord op een inbound reply.

    Args:
        reply_inbox_row: Row uit heatr_reply_inbox (id, body_text, from_name, etc.)
        lead: Lead row (company_name, contact_first_name, sector, etc.)
        classification: Output van reply_classifier (category, summary, etc.)
        original_emails: Optioneel — eerder verstuurde mails in de thread voor context
        workspace_id: Workspace key
        supabase_client: Supabase client (voor cost-log + cache)
        anthropic_client: Anthropic AsyncAnthropic instance

    Returns: {
        "draft": str | None,        # None als category in skip-list
        "category": str,
        "skip_reason": str | None,
        "cached": bool,
        "cost_eur": float,
    }
    """
    category = (classification or {}).get("category") or "other"

    # Skip categories — geen draft nodig
    if category in _SKIP_CATEGORIES:
        return {
            "draft": None,
            "category": category,
            "skip_reason": f"category '{category}' wordt niet automatisch beantwoord",
            "cached": False,
            "cost_eur": 0.0,
        }

    reply_id = reply_inbox_row.get("id")
    raw_body = (reply_inbox_row.get("body_text") or "").strip()
    if len(raw_body) < 3:
        return {
            "draft": None, "category": category,
            "skip_reason": "reply body te kort",
            "cached": False, "cost_eur": 0.0,
        }
    # Quote-trim: gooi gequoteerde versie van onze eerdere mails weg
    reply_body = trim_quoted_thread(raw_body)
    reply_body_capped = reply_body[:2000]
    # Wachttijd-context
    hrs = hours_since(reply_inbox_row.get("received_at"))

    from_name = (reply_inbox_row.get("from_name") or "").strip()
    from_name_parts = from_name.split() if from_name else []
    first_name = (
        (lead or {}).get("contact_first_name")
        or (from_name_parts[0] if from_name_parts else None)
        or "daar"
    )
    company_name = (lead or {}).get("company_name") or reply_inbox_row.get("from_name") or "uw praktijk"

    # Cache key: reply_id + classification + last_modified
    cache_key = hashlib.sha256(
        f"draft::{reply_id}::{category}::{classification.get('summary', '')[:200]}".encode("utf-8")
    ).hexdigest()
    try:
        cached = (
            supabase_client.table("claude_cache")
            .select("response_text")
            .eq("cache_key", cache_key)
            .maybe_single()
            .execute()
        )
        if cached and cached.data and cached.data.get("response_text"):
            try:
                parsed = json.loads(cached.data["response_text"])
                parsed["cached"] = True
                parsed["cost_eur"] = 0.0
                return parsed
            except (json.JSONDecodeError, ValueError):
                pass
    except Exception as e:
        logger.debug("draft_reply cache lookup failed: %s", e)

    # Build prompt
    guidance = _CATEGORY_GUIDANCE.get(category, _CATEGORY_GUIDANCE["other"])

    thread_context = ""
    if original_emails:
        # Toon laatste 2 mails uit de thread voor context
        for em in original_emails[-2:]:
            subj = em.get("subject", "")
            body = (em.get("body") or em.get("body_text") or "")[:600]
            thread_context += f"\n[Eerder verzonden — onderwerp: {subj}]\n{body}\n"

    timing = _timing_instruction(hrs)
    timing_block = f"\nWachttijd-instructie: {timing}\n" if timing else ""

    user_prompt = (
        f"Categorie van deze reply: **{category}**\n\n"
        f"Tone-guidance:\n{guidance}\n"
        f"{timing_block}\n"
        f"Lead-context:\n"
        f"- Bedrijf: {company_name}\n"
        f"- Voornaam contact: {first_name}\n"
        f"- Sector: {(lead or {}).get('sector', 'onbekend')}\n"
        f"- Archetype: {(lead or {}).get('archetype', 'onbekend')}\n"
        f"- Reply-classifier samenvatting: {classification.get('summary') or '(geen)'}\n"
        f"{thread_context}\n"
        f"De inbound reply waarop je antwoordt (quote-trimmed):\n"
        f'"""\n{reply_body_capped}\n"""\n\n'
        f"Schrijf nu het antwoord. Alleen de mail-body, geen onderwerpregel, "
        f"geen extra commentaar eromheen."
    )

    # Inject evidence-based reply-rubric uit config/opener_principles.md
    # ("Reply draft principles" sectie). Anthropic prompt-cache (ephemeral)
    # zorgt dat de rubric server-side wordt cached — geen extra cost na warmup.
    from config.principles_loader import get_principles
    principles_text = get_principles()
    if principles_text:
        # Pak alleen de "Reply draft principles" sectie — body voor system prompt
        idx = principles_text.lower().find("reply draft principles")
        rubric_subset = principles_text[idx:idx + 4000] if idx >= 0 else ""
        full_system = (
            f"{_SYSTEM_PROMPT}\n\n"
            f"=== EVIDENCE-BASED REPLY RUBRIC ===\n"
            f"{rubric_subset or principles_text[:4000]}\n"
            f"=== EINDE RUBRIC ===\n"
        )
    else:
        full_system = _SYSTEM_PROMPT

    try:
        response = await anthropic_client.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=300,
            temperature=0.3,  # Lichte creativiteit — niet 0.0 anders worden alle replies klonen
            system=[{
                "type": "text",
                "text": full_system,
                "cache_control": {"type": "ephemeral"},  # System prompt wordt cached over calls
            }],
            messages=[{"role": "user", "content": user_prompt}],
        )
    except Exception as e:
        logger.warning("draft_reply Claude call failed: %s", e)
        return {
            "draft": None, "category": category,
            "skip_reason": f"claude error: {e}",
            "cached": False, "cost_eur": 0.0,
        }

    draft_text = response.content[0].text.strip() if response.content else ""
    # Trim eventuele markdown / quote-fence
    if draft_text.startswith("```"):
        draft_text = re.sub(r"^```[a-z]*\n?", "", draft_text)
        draft_text = re.sub(r"\n?```$", "", draft_text)
    # Voor `interested`: garandeer dat scheduling-link erin staat
    draft_text, link_appended = _ensure_scheduling_link(draft_text, category)

    # Cost accounting
    input_tokens = getattr(response.usage, "input_tokens", 0) or 0
    output_tokens = getattr(response.usage, "output_tokens", 0) or 0
    cost_eur = round(
        (input_tokens * _COST_PER_M_INPUT + output_tokens * _COST_PER_M_OUTPUT) / 1_000_000, 6,
    )
    try:
        await log_api_cost(
            model=_HAIKU_MODEL, input_tokens=input_tokens, output_tokens=output_tokens,
            cost_eur=cost_eur, workspace_id=workspace_id, supabase_client=supabase_client,
            context="reply_draft", lead_id=(lead or {}).get("id"),
        )
    except Exception as e:
        logger.debug("reply_draft cost log failed: %s", e)

    result = {
        "draft": draft_text,
        "category": category,
        "skip_reason": None,
        "cached": False,
        "cost_eur": cost_eur,
        "scheduling_link_auto_added": link_appended,
        "hours_since_received": hrs,
    }

    # Cache
    try:
        supabase_client.table("claude_cache").upsert({
            "cache_key": cache_key,
            "model": _HAIKU_MODEL,
            "prompt_hash": cache_key,
            "response_text": json.dumps({k: v for k, v in result.items() if k != "cost_eur"}),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }, on_conflict="cache_key").execute()
    except Exception as e:
        logger.debug("reply_draft cache store failed: %s", e)

    logger.info(
        "reply_draft: lead=%s category=%s draft_chars=%d cost=EUR %.6f",
        (lead or {}).get("id"), category, len(draft_text), cost_eur,
    )
    return result
