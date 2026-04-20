"""
integrations/reply_classifier.py — Classify incoming Warmr replies with Claude Haiku.

Replaces manual inbox triage. Every reply gets:
  - A category: interested | not_now | not_interested | wrong_person |
                unsubscribe_request | auto_reply | question | other
  - Optional extracted data:
      - return_date: when to recontact (parsed from "kom terug in Q3", "september", etc.)
      - referred_to: if "stuur naar Jan" — the name
      - unsubscribe: explicit opt-out
  - Suggested action for Heatr (auto-snooze, auto-unsubscribe, flag for review)

Cost: 1 Haiku call per reply (~€0.0002)
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger(__name__)


# Reply categories
CATEGORY_INTERESTED     = "interested"
CATEGORY_NOT_NOW        = "not_now"
CATEGORY_NOT_INTERESTED = "not_interested"
CATEGORY_WRONG_PERSON   = "wrong_person"
CATEGORY_UNSUBSCRIBE    = "unsubscribe_request"
CATEGORY_AUTO_REPLY     = "auto_reply"
CATEGORY_QUESTION       = "question"
CATEGORY_OTHER          = "other"

ALL_CATEGORIES = [
    CATEGORY_INTERESTED, CATEGORY_NOT_NOW, CATEGORY_NOT_INTERESTED,
    CATEGORY_WRONG_PERSON, CATEGORY_UNSUBSCRIBE, CATEGORY_AUTO_REPLY,
    CATEGORY_QUESTION, CATEGORY_OTHER,
]


_CLASSIFIER_SYSTEM_PROMPT = """Je classificeert inkomende email-replies op cold outreach.
Return ALLEEN JSON met deze structuur:
{"category":"...","return_date":"YYYY-MM-DD or null","referred_to":"name or null","unsubscribe":true/false,"summary":"1 zin","sentiment":"positive/neutral/negative"}

Categorieën:
- interested: positieve reactie, wil meer info of bellen
- not_now: geïnteresseerd maar niet nu (extraheer return_date als mogelijk)
- not_interested: beleefd/expliciet nee
- wrong_person: "stuur naar X" of "ik ben niet de juiste persoon" (extraheer referred_to)
- unsubscribe_request: expliciet uitschrijven/verwijderen (unsubscribe=true)
- auto_reply: out-of-office / vakantie (extraheer return_date uit tekst)
- question: stelt een vraag, wil meer details voor beslissing
- other: onduidelijk / niet classificeerbaar"""


async def classify_reply(
    reply_text: str,
    reply_from: str,
    lead_company: str,
    supabase_client: Any,
    anthropic_client: Any,
) -> dict[str, Any]:
    """
    Classify a single reply with Claude Haiku.

    Returns dict with category + optional extracted fields.
    """
    if not reply_text or len(reply_text.strip()) < 3:
        return {"category": CATEGORY_OTHER, "summary": "Empty reply"}

    # Trim very long replies (quoted email threads)
    reply_for_llm = reply_text.strip()[:2000]

    import anthropic

    async_client = anthropic.AsyncAnthropic(api_key=anthropic_client.api_key)

    try:
        response = await async_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            temperature=0,
            system=[{
                "type": "text",
                "text": _CLASSIFIER_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            messages=[{
                "role": "user",
                "content": (
                    f"Reply van {reply_from} (over {lead_company}):\n\n"
                    f"{reply_for_llm}\n\n"
                    f"Classificeer."
                ),
            }],
        )

        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)

        parsed = json.loads(text)

        # Log cost
        try:
            usage = response.usage
            cost = _estimate_cost(usage.input_tokens, usage.output_tokens)
            supabase_client.table("api_cost_log").insert({
                "workspace_id": "aerys",
                "model": "claude-haiku-4-5-20251001",
                "prompt_tokens": usage.input_tokens,
                "response_tokens": usage.output_tokens,
                "cost_eur": round(cost, 6),
                "context": "reply_classify",
            }).execute()
        except Exception:
            pass

        # Validate category
        category = parsed.get("category") or CATEGORY_OTHER
        if category not in ALL_CATEGORIES:
            category = CATEGORY_OTHER

        # Parse return_date
        return_date = parsed.get("return_date")
        if return_date:
            return_date = _normalize_date(return_date)

        return {
            "category": category,
            "return_date": return_date,
            "referred_to": parsed.get("referred_to"),
            "unsubscribe": bool(parsed.get("unsubscribe", False)),
            "summary": parsed.get("summary", ""),
            "sentiment": parsed.get("sentiment", "neutral"),
        }

    except Exception as e:
        logger.warning("classify_reply failed: %s", e)
        return {"category": CATEGORY_OTHER, "summary": f"Parse error: {str(e)[:60]}"}


async def process_reply(
    reply_id: str,
    reply_text: str,
    reply_from: str,
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
) -> dict[str, Any]:
    """
    Full pipeline: classify reply → apply automatic actions → log.

    Automatic actions per category:
      - interested:          flag in inbox, high priority, stop sequence
      - not_now + date:      auto-snooze tot return_date
      - not_interested:      status='disqualified', stop sequence, save snapshot
      - wrong_person:        flag for manual review with referred_to name
      - unsubscribe_request: status='unsubscribed', permanent block
      - auto_reply + date:   snooze tot return_date, keep in sequence
      - question:            high-priority inbox flag
      - other:               inbox, manual review
    """
    # Load lead
    lead_res = supabase_client.table("leads").select("*").eq(
        "id", lead_id,
    ).maybe_single().execute()
    if not lead_res.data:
        return {"error": "lead not found"}

    lead = lead_res.data
    company_name = lead.get("company_name") or ""

    # Classify
    classification = await classify_reply(
        reply_text=reply_text,
        reply_from=reply_from,
        lead_company=company_name,
        supabase_client=supabase_client,
        anthropic_client=anthropic_client,
    )

    category = classification.get("category")
    return_date = classification.get("return_date")

    # Store classification on the reply row (reply_inbox)
    try:
        supabase_client.table("reply_inbox").update({
            "classification": category,
            "classification_summary": classification.get("summary", ""),
            "classification_sentiment": classification.get("sentiment", ""),
        }).eq("id", reply_id).execute()
    except Exception as e:
        logger.debug("reply_inbox update: %s", e)

    # Apply automatic actions
    actions_taken: list[str] = []

    if category == CATEGORY_UNSUBSCRIBE or classification.get("unsubscribe"):
        # Permanent block
        try:
            supabase_client.table("leads").update({
                "status": "unsubscribed",
                "unsubscribed_at": datetime.now(timezone.utc).isoformat(),
                "unsubscribe_source": "reply_classifier",
                "gdpr_safe": False,
            }).eq("id", lead_id).execute()
            await _stop_all_sequences(lead_id, supabase_client)
            actions_taken.append("marked_unsubscribed")
        except Exception as e:
            logger.error("unsubscribe action failed: %s", e)

    elif category == CATEGORY_NOT_INTERESTED:
        try:
            supabase_client.table("leads").update({
                "status": "disqualified",
                "disqualification_reason": "not_interested_reply",
                "next_contact_after": (datetime.now(timezone.utc) + timedelta(days=365)).isoformat(),
            }).eq("id", lead_id).execute()
            await _stop_all_sequences(lead_id, supabase_client)
            actions_taken.append("marked_disqualified")
        except Exception:
            pass

    elif category == CATEGORY_NOT_NOW and return_date:
        try:
            supabase_client.table("leads").update({
                "status": "snoozed",
                "snoozed_until": return_date,
                "next_contact_after": return_date,
                "crm_stage": "later",
            }).eq("id", lead_id).execute()
            await _pause_sequences(lead_id, supabase_client)
            actions_taken.append(f"snoozed_until_{return_date}")
        except Exception:
            pass

    elif category == CATEGORY_AUTO_REPLY and return_date:
        # Auto-reply (out-of-office) — pause sequence until return
        try:
            supabase_client.table("leads").update({
                "snoozed_until": return_date,
            }).eq("id", lead_id).execute()
            await _pause_sequences(lead_id, supabase_client, until=return_date)
            actions_taken.append(f"auto_reply_snooze_{return_date}")
        except Exception:
            pass

    elif category == CATEGORY_WRONG_PERSON:
        referred_to = classification.get("referred_to")
        try:
            supabase_client.table("leads").update({
                "status": "needs_review",
                "disqualification_reason": f"referred_to:{referred_to}" if referred_to else "wrong_person",
            }).eq("id", lead_id).execute()
            await _pause_sequences(lead_id, supabase_client)
            actions_taken.append("flagged_wrong_person")
        except Exception:
            pass

    elif category == CATEGORY_INTERESTED:
        try:
            supabase_client.table("leads").update({
                "status": "replied",
                "crm_stage": "gereageerd",
            }).eq("id", lead_id).execute()
            await _stop_all_sequences(lead_id, supabase_client)
            actions_taken.append("marked_interested")
        except Exception:
            pass

    elif category == CATEGORY_QUESTION:
        # Keep sequence but flag as hot
        try:
            supabase_client.table("leads").update({
                "status": "replied",
                "crm_stage": "gereageerd",
            }).eq("id", lead_id).execute()
            actions_taken.append("flagged_question")
        except Exception:
            pass

    # Log to timeline
    try:
        supabase_client.table("lead_timeline").insert({
            "workspace_id": workspace_id,
            "lead_id": lead_id,
            "event_type": "reply_classified",
            "title": f"Reply classified as {category}",
            "body": classification.get("summary", ""),
            "metadata": {
                "classification": classification,
                "actions": actions_taken,
                "reply_from": reply_from,
            },
            "created_by": "reply_classifier",
        }).execute()
    except Exception:
        pass

    logger.info(
        "process_reply: lead=%s category=%s actions=%s",
        lead_id, category, actions_taken,
    )

    return {
        "classification": classification,
        "actions_taken": actions_taken,
    }


async def _stop_all_sequences(lead_id: str, supabase_client: Any) -> None:
    """Stop all active outreach sequences for this lead."""
    try:
        supabase_client.table("lead_campaign_history").update({
            "status": "stopped",
            "is_active": False,
        }).eq("lead_id", lead_id).eq("is_active", True).execute()
    except Exception:
        pass


async def _pause_sequences(
    lead_id: str,
    supabase_client: Any,
    until: str | None = None,
) -> None:
    """Pause sequences — will resume on `until` or require manual restart."""
    update: dict[str, Any] = {"status": "paused"}
    if until:
        update["next_send_at"] = until
    try:
        supabase_client.table("lead_campaign_history").update(update).eq(
            "lead_id", lead_id
        ).eq("is_active", True).execute()
    except Exception:
        pass


def _normalize_date(date_str: str) -> str | None:
    """
    Convert date strings to YYYY-MM-DD.

    Accepts: "2026-09-01", "september", "Q3", "over 3 maanden", etc.
    """
    if not date_str or date_str.lower() in ("null", "none", ""):
        return None

    s = date_str.strip().lower()
    now = datetime.now(timezone.utc)

    # Already ISO-like
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return m.group(0)

    # Dutch months
    months = {
        "januari": 1, "jan": 1, "februari": 2, "feb": 2, "maart": 3, "mrt": 3,
        "april": 4, "apr": 4, "mei": 5, "juni": 6, "jun": 6, "juli": 7, "jul": 7,
        "augustus": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
        "oktober": 10, "okt": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
    }
    for mname, mnum in months.items():
        if mname in s:
            year = now.year if mnum > now.month else now.year + 1
            return f"{year:04d}-{mnum:02d}-01"

    # Quarters
    quarters = {
        "q1": (1, 15), "q2": (4, 15), "q3": (7, 15), "q4": (10, 15),
        "kwartaal 1": (1, 15), "kwartaal 2": (4, 15), "kwartaal 3": (7, 15), "kwartaal 4": (10, 15),
    }
    for qname, (month, day) in quarters.items():
        if qname in s:
            year = now.year if month > now.month else now.year + 1
            return f"{year:04d}-{month:02d}-{day:02d}"

    # "over X maanden" / "in X maanden"
    m = re.search(r"(\d+)\s*(maand|maanden|week|weken|dag|dagen)", s)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        days = n * (30 if unit.startswith("maand") else 7 if unit.startswith("week") else 1)
        return (now + timedelta(days=days)).strftime("%Y-%m-%d")

    # "volgende maand / volgend kwartaal"
    if "volgende maand" in s:
        next_month = now.replace(day=1) + timedelta(days=32)
        return next_month.replace(day=1).strftime("%Y-%m-%d")
    if "volgend kwartaal" in s or "volgend q" in s:
        return (now + timedelta(days=90)).strftime("%Y-%m-%d")

    return None


def _estimate_cost(input_tokens: int, output_tokens: int) -> float:
    """Haiku pricing per million tokens (EUR)."""
    return (input_tokens * 0.74 + output_tokens * 3.68) / 1_000_000
