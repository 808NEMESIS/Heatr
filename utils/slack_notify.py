"""utils/slack_notify.py — operationele Slack-meldingen (afmelding + reactie).

Fail-soft en config-driven: zonder SLACK_WEBHOOK_URL is alles een no-op, en een
Slack-fout mag NOOIT de webhook-verwerking (compliance-kritiek) laten crashen.
Alle publieke functies vangen hun eigen excepties en retourneren een bool.

Transport (notify_slack) is generiek; de formatters (reactie-rapport, afmelding)
zijn puur en los getest. De Warmr-webhook roept notify_reply/notify_unsubscribe
aan; die halen de lead + website-intelligence op en posten het bericht.
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_SNIPPET_MAX = 400


def slack_enabled() -> bool:
    """True als er een Slack-webhook is geconfigureerd."""
    return bool(os.getenv("SLACK_WEBHOOK_URL"))


async def notify_slack(text: str, blocks: list | None = None) -> bool:
    """Post naar de Slack incoming-webhook. Fail-soft: geen URL → no-op (False);
    fouten worden gelogd, nooit geraised. `text` is de fallback/notificatie-tekst,
    `blocks` optioneel Block Kit voor rijke opmaak."""
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return False
    payload: dict[str, Any] = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    try:
        import httpx
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.post(url, json=payload)
            if r.status_code >= 400:
                logger.warning("slack_notify: post faalde (%s): %s", r.status_code, r.text[:120])
                return False
            return True
    except Exception as e:
        logger.warning("slack_notify: post-fout: %s", e)
        return False


def _lead_url(lead_id: str) -> str | None:
    base = os.getenv("HEATR_BASE_URL")
    return f"{base.rstrip('/')}/heatr/leads/{lead_id}" if base else None


def _snippet(payload: dict) -> str:
    body = (payload.get("body_text") or payload.get("body") or "").strip()
    body = " ".join(body.split())               # collapse whitespace
    return (body[:_SNIPPET_MAX] + "…") if len(body) > _SNIPPET_MAX else body


# ── Reactie-rapport ───────────────────────────────────────────────────────────

def format_reply_message(
    lead: dict, wi: dict, payload: dict, kind: str, lead_id: str = "",
) -> tuple[str, list]:
    """Bouw (fallback_text, blocks) voor een binnengekomen reactie. `kind` =
    'interested' (positief) of 'replied' (generiek). Puur — geen I/O."""
    company = lead.get("company_name") or "Onbekend bedrijf"
    positief = kind in ("interested", "lead.interested")
    icon = "🎯" if positief else "💬"
    label = "Geïnteresseerd" if positief else "Reactie"
    header = f"{icon} {label}: {company}"

    contact = lead.get("contact_first_name") or "—"
    city = lead.get("city") or "—"
    sector = (lead.get("sector") or "—")
    lead_score = lead.get("score")
    ws_score = wi.get("total_score")
    prio = wi.get("priority")
    email = lead.get("email") or payload.get("from_email") or "—"

    ws_line = f"{ws_score}/100" + (f" · prio {prio}" if prio else "") if ws_score is not None else "—"
    facts = (f"*Contact:* {contact}   *Plaats:* {city}   *Sector:* {sector}\n"
             f"*Lead-score:* {lead_score if lead_score is not None else '—'}   "
             f"*Website-score:* {ws_line}\n"
             f"*E-mail:* {email}")

    opener = (lead.get("personalized_opener") or "").strip()
    snippet = _snippet(payload)
    subject = (payload.get("subject") or "").strip()

    # fallback-tekst (notificatie + clients zonder Block Kit)
    parts = [header, facts.replace("*", "")]
    if snippet:
        parts.append(f"Reactie: {snippet}")
    fallback = "\n".join(parts)

    blocks: list = [
        {"type": "header", "text": {"type": "plain_text", "text": header[:150], "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": facts}},
    ]
    if subject or snippet:
        reply_md = (f"*Onderwerp:* {subject}\n" if subject else "") + (f">{snippet}" if snippet else "")
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": reply_md[:2900]}})
    if opener:
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
                      "text": f"*Verzonden opener:*\n_{opener[:600]}_"}})
    url = _lead_url(lead_id)
    if url:
        blocks.append({"type": "context", "elements": [
            {"type": "mrkdwn", "text": f"<{url}|Open lead in Heatr →>"}]})
    return fallback, blocks


async def notify_reply(db, lead_id: str, workspace_id: str, payload: dict, kind: str) -> bool:
    """Haal lead + WI op en post een reactie-rapport naar Slack. Fail-soft."""
    if not slack_enabled() or not lead_id:
        return False
    try:
        lead = ((db.table("leads")
                 .select("company_name, contact_first_name, city, sector, score, "
                         "email, domain, personalized_opener")
                 .eq("id", lead_id).eq("workspace_id", workspace_id)
                 .limit(1).execute()).data or [{}])[0]
        wi = ((db.table("website_intelligence")
               .select("total_score, priority")
               .eq("lead_id", lead_id).eq("workspace_id", workspace_id)
               .limit(1).execute()).data or [{}])[0]
        text, blocks = format_reply_message(lead, wi, payload, kind, lead_id=lead_id)
        return await notify_slack(text, blocks)
    except Exception as e:
        logger.warning("notify_reply: rapport bouwen/posten faalde (lead=%s): %s", lead_id, e)
        return False


# ── Afmelding ─────────────────────────────────────────────────────────────────

def format_unsubscribe_message(lead: dict, payload: dict, lead_id: str = "") -> tuple[str, list]:
    """Bouw (fallback_text, blocks) voor een afmelding. Puur — geen I/O."""
    company = lead.get("company_name") or "Onbekend bedrijf"
    email = lead.get("email") or payload.get("from_email") or payload.get("email") or "—"
    city = lead.get("city") or "—"
    header = f"🔕 Afmelding: {company}"
    facts = (f"*E-mail:* {email}   *Plaats:* {city}\n"
             f"Sequence gestopt en adres platformbreed gesuppressed.")
    fallback = f"{header}\n{facts}".replace("*", "")
    blocks: list = [
        {"type": "section", "text": {"type": "mrkdwn", "text": f"{header}\n{facts}"}},
    ]
    url = _lead_url(lead_id)
    if url:
        blocks.append({"type": "context", "elements": [
            {"type": "mrkdwn", "text": f"<{url}|Open lead in Heatr →>"}]})
    return fallback, blocks


async def notify_unsubscribe(db, lead_id: str, workspace_id: str, payload: dict) -> bool:
    """Haal lead op en post een afmeld-melding naar Slack. Fail-soft."""
    if not slack_enabled() or not lead_id:
        return False
    try:
        lead = ((db.table("leads")
                 .select("company_name, city, email")
                 .eq("id", lead_id).eq("workspace_id", workspace_id)
                 .limit(1).execute()).data or [{}])[0]
        text, blocks = format_unsubscribe_message(lead, payload, lead_id=lead_id)
        return await notify_slack(text, blocks)
    except Exception as e:
        logger.warning("notify_unsubscribe: melding posten faalde (lead=%s): %s", lead_id, e)
        return False
