"""
utils/lead_thread.py — Bouw chronologische email-thread per lead.

Mergt twee bronnen:

1. **Sent**: `heatr_lead_timeline` events met event_type='email_sent' geven de
   timestamps. Per event: `metadata.campaign_id` + `metadata.step_index` joint
   naar `heatr_lead_campaign_history.sequence_steps[step_index]` voor subject +
   body content (frozen-at-launch JSONB).

2. **Received**: `heatr_reply_inbox` per row = één ontvangen reply met
   subject + body + classification.

Returns: chronologisch array (oudste eerst) met items van vorm:
    {
        "direction": "sent" | "received",
        "timestamp": ISO-string,
        "subject": str,
        "body": str,
        "step_index": int | None,         # alleen voor sent
        "campaign_id": str | None,        # alleen voor sent
        "classification": str | None,     # alleen voor received
        "classifier_summary": str | None, # alleen voor received
        "from_email": str | None,         # alleen voor received
    }

Faalt-tolerant: missende tabellen of velden geven lege thread terug ipv crash.
Body text-only: geen HTML-rendering in v1 — voorkomt XSS-risk en past bij
v1.0 simpel-houden filosofie.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _strip_html_to_text(text: str | None) -> str:
    """Heel simpele HTML→text strip — geen library, geen XSS-window.

    Voor v1: vervang <br>/<p> met newlines, strip alle andere tags via regex,
    decode common HTML entities. Ja, niet perfect — dat is OK voor preview-doel.
    """
    if not text:
        return ""
    import re
    out = text
    out = re.sub(r"<br\s*/?>", "\n", out, flags=re.IGNORECASE)
    out = re.sub(r"</p\s*>", "\n\n", out, flags=re.IGNORECASE)
    out = re.sub(r"<[^>]+>", "", out)  # strip alle resterende tags
    out = (out
           .replace("&nbsp;", " ")
           .replace("&amp;", "&")
           .replace("&lt;", "<")
           .replace("&gt;", ">")
           .replace("&quot;", '"')
           .replace("&#39;", "'"))
    # Cap op redelijke lengte (voorkom 50KB-quoted-thread in UI)
    return out.strip()[:5000]


async def build_lead_thread(
    lead_id: str,
    workspace_id: str,
    db: Any,
) -> dict[str, Any]:
    """Return {"thread": [...], "lead_id": str, "counts": {...}}.

    Lege thread bij nog-niet-gemailde lead. Lege thread bij missende tabellen
    (graceful degradation).
    """
    items: list[dict] = []

    # 1. Sent: lead_timeline events met type='email_sent' + 'review_email_sent'
    timeline_rows: list[dict] = []
    try:
        res = (
            db.table("lead_timeline")
            .select("id, event_type, title, body, metadata, created_at")
            .eq("lead_id", lead_id)
            .eq("workspace_id", workspace_id)
            .in_("event_type", ["email_sent", "review_email_sent"])
            .order("created_at", desc=False)
            .execute()
        )
        timeline_rows = res.data or []
    except Exception as e:
        logger.debug("build_lead_thread: timeline fetch failed: %s", e)

    # 2. Sequence steps voor body-lookup (campaign_id + step_index → frozen subject/body)
    campaign_history_by_id: dict[str, dict] = {}
    if timeline_rows:
        # Verzamel unieke campaign_ids uit metadata
        campaign_ids = list({
            (row.get("metadata") or {}).get("campaign_id")
            for row in timeline_rows
            if (row.get("metadata") or {}).get("campaign_id")
        })
        if campaign_ids:
            try:
                ch_res = (
                    db.table("lead_campaign_history")
                    .select("warmr_campaign_id, sequence_steps, step_index, sent_at")
                    .in_("warmr_campaign_id", campaign_ids)
                    .eq("lead_id", lead_id)
                    .execute()
                )
                for row in (ch_res.data or []):
                    cid = row.get("warmr_campaign_id")
                    if cid:
                        campaign_history_by_id[cid] = row
            except Exception as e:
                logger.debug("build_lead_thread: campaign_history fetch failed: %s", e)

    for row in timeline_rows:
        meta = row.get("metadata") or {}
        cid = meta.get("campaign_id")
        step_idx = meta.get("step_index")

        # Standaard: gebruik timeline title als fallback subject + lege body
        subject = row.get("title") or "(geen onderwerp)"
        body = row.get("body") or ""

        # Verrijkt: pak frozen sequence_steps content als beschikbaar
        if cid and cid in campaign_history_by_id:
            steps = campaign_history_by_id[cid].get("sequence_steps") or []
            idx = step_idx if isinstance(step_idx, int) else 0
            if 0 <= idx < len(steps):
                step = steps[idx]
                subject = step.get("subject") or subject
                body = step.get("body") or body

        items.append({
            "direction": "sent",
            "timestamp": row.get("created_at"),
            "subject": subject,
            "body": body[:5000],
            "step_index": step_idx,
            "campaign_id": cid,
            "classification": None,
            "classifier_summary": None,
            "from_email": None,
        })

    # 3. Received: reply_inbox rows
    try:
        rep_res = (
            db.table("reply_inbox")
            .select("id, subject, body, body_text, received_at, classification, "
                    "classification_summary, from_email")
            .eq("lead_id", lead_id)
            .eq("workspace_id", workspace_id)
            .order("received_at", desc=False)
            .execute()
        )
        for row in (rep_res.data or []):
            # body_text wint als beschikbaar, anders strip body (HTML)
            body_text = row.get("body_text") or _strip_html_to_text(row.get("body"))
            items.append({
                "direction": "received",
                "timestamp": row.get("received_at"),
                "subject": row.get("subject") or "(geen onderwerp)",
                "body": body_text[:5000] if body_text else "",
                "step_index": None,
                "campaign_id": None,
                "classification": row.get("classification"),
                "classifier_summary": row.get("classification_summary"),
                "from_email": row.get("from_email"),
            })
    except Exception as e:
        logger.debug("build_lead_thread: reply_inbox fetch failed: %s", e)

    # 4. Merge chronologisch (oudste eerst)
    def _ts_key(item: dict) -> str:
        return item.get("timestamp") or ""
    items.sort(key=_ts_key)

    counts = {
        "total": len(items),
        "sent": sum(1 for i in items if i["direction"] == "sent"),
        "received": sum(1 for i in items if i["direction"] == "received"),
    }

    return {
        "lead_id": lead_id,
        "thread": items,
        "counts": counts,
    }
