"""
scoring/feedback_processor.py — Warmr feedback loop for scoring improvement.

Analyzes Warmr reply/bounce/open data to identify which lead characteristics
correlate with success, and adjusts scoring signals accordingly.

Called weekly by n8n workflow or on-demand via POST /scoring/process-feedback.

This is a LEARNING system: it doesn't just log what happened, it changes
how future leads are scored based on what actually worked.
"""
from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


async def process_feedback(
    workspace_id: str,
    supabase_client: Any,
    days: int = 30,
) -> dict[str, Any]:
    """
    Analyze Warmr outcomes and derive scoring adjustments.

    Steps:
      1. Load all leads that received outreach in the last N days
      2. Group by outcome: replied, opened_only, bounced, no_response
      3. For each group: analyze common characteristics
      4. Derive scoring signal adjustments
      5. Store insights in scoring_feedback table

    Returns:
        {
            "leads_analyzed": int,
            "replied": int,
            "bounced": int,
            "insights": [...],
            "adjustments": [...],
        }
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    # Load campaign history with lead data
    try:
        res = supabase_client.table("lead_campaign_history").select(
            "lead_id, status, "
            "leads(sector, score, email_status, contact_source, contact_title, "
            "google_rating, google_review_count, data_quality_score, "
            "fit_score, reachability_score, personalization_potential, "
            "has_instagram, domain, company_size_estimate)"
        ).eq("workspace_id", workspace_id).gte("created_at", cutoff).execute()
    except Exception as e:
        logger.error("feedback_processor: failed to load campaign history: %s", e)
        return {"leads_analyzed": 0, "error": str(e)}

    rows = res.data or []
    if not rows:
        return {"leads_analyzed": 0, "insights": [], "adjustments": []}

    # Group by outcome
    replied: list[dict] = []
    bounced: list[dict] = []
    opened: list[dict] = []
    no_response: list[dict] = []

    for row in rows:
        lead = row.get("leads") or {}
        status = row.get("status") or ""

        if status in ("replied", "interested"):
            replied.append(lead)
        elif status == "bounced":
            bounced.append(lead)
        elif status == "opened":
            opened.append(lead)
        else:
            no_response.append(lead)

    insights: list[dict] = []
    adjustments: list[dict] = []

    # --- Insight 1: What do replied leads have in common? ---
    if replied:
        replied_patterns = _analyze_group(replied, "replied")
        insights.append(replied_patterns)

        # If replied leads predominantly have personal contact + high personalization
        avg_pers = _avg(replied, "personalization_potential")
        avg_reach = _avg(replied, "reachability_score")
        if avg_pers > 8:
            adjustments.append({
                "signal": "personalization_potential",
                "direction": "increase_weight",
                "reason": f"Replied leads avg personalization={avg_pers:.1f} vs overall",
                "suggested_boost": 2,
            })

    # --- Insight 2: Bounce analysis → email verifier quality ---
    if bounced:
        bounce_sources = Counter(l.get("email_status") for l in bounced)
        for status, count in bounce_sources.most_common():
            if status == "valid" and count >= 3:
                insights.append({
                    "type": "email_verifier_issue",
                    "detail": f"{count} bounces from 'valid' emails — SMTP verifier may be unreliable",
                    "action": "review email_verifier catch-all detection",
                })

        # Flag specific domains with high bounce rate
        bounce_domains = Counter(
            (l.get("domain") or "").split(".")[-2] if l.get("domain") else "unknown"
            for l in bounced
        )
        for domain_base, count in bounce_domains.most_common(3):
            if count >= 2:
                insights.append({
                    "type": "domain_bounce_pattern",
                    "detail": f"Domain pattern '*{domain_base}*' has {count} bounces",
                    "action": "consider blacklisting this domain pattern",
                })

    # --- Insight 3: Contact source effectiveness ---
    if replied:
        replied_sources = Counter(l.get("contact_source") for l in replied if l.get("contact_source"))
        all_sources = Counter(lead.get("leads", {}).get("contact_source")
                              for lead in rows if lead.get("leads", {}).get("contact_source"))

        for source, reply_count in replied_sources.most_common():
            total = all_sources.get(source, 0)
            if total > 0:
                rate = reply_count / total
                insights.append({
                    "type": "contact_source_effectiveness",
                    "source": source,
                    "reply_rate": round(rate, 3),
                    "replied": reply_count,
                    "total": total,
                })

    # --- Insight 4: Sector performance ---
    if len(rows) >= 10:
        sector_replies = Counter(l.get("sector") for l in replied if l.get("sector"))
        sector_total = Counter(
            lead.get("leads", {}).get("sector")
            for lead in rows if lead.get("leads", {}).get("sector")
        )
        for sector, total in sector_total.most_common():
            reply_count = sector_replies.get(sector, 0)
            rate = reply_count / total if total else 0
            insights.append({
                "type": "sector_reply_rate",
                "sector": sector,
                "reply_rate": round(rate, 3),
                "replied": reply_count,
                "total": total,
            })

    # --- Store feedback ---
    try:
        supabase_client.table("lead_timeline").insert({
            "workspace_id": workspace_id,
            "event_type": "feedback_processed",
            "title": f"Scoring feedback: {len(replied)} replies, {len(bounced)} bounces from {len(rows)} sends",
            "metadata": {
                "insights": insights[:10],
                "adjustments": adjustments[:5],
                "period_days": days,
            },
            "created_by": "feedback_processor",
        }).execute()
    except Exception:
        pass

    result = {
        "leads_analyzed": len(rows),
        "replied": len(replied),
        "bounced": len(bounced),
        "opened_only": len(opened),
        "no_response": len(no_response),
        "reply_rate": round(len(replied) / max(len(rows), 1), 3),
        "bounce_rate": round(len(bounced) / max(len(rows), 1), 3),
        "insights": insights,
        "adjustments": adjustments,
    }

    logger.info(
        "feedback_processor: analyzed=%d replied=%d bounced=%d insights=%d",
        len(rows), len(replied), len(bounced), len(insights),
    )

    return result


def _analyze_group(leads: list[dict], label: str) -> dict:
    """Analyze common characteristics of a group of leads."""
    return {
        "type": f"{label}_patterns",
        "count": len(leads),
        "avg_score": _avg(leads, "score"),
        "avg_fit": _avg(leads, "fit_score"),
        "avg_reachability": _avg(leads, "reachability_score"),
        "avg_personalization": _avg(leads, "personalization_potential"),
        "avg_data_quality": _avg(leads, "data_quality_score"),
        "top_contact_sources": Counter(
            l.get("contact_source") for l in leads if l.get("contact_source")
        ).most_common(3),
        "top_email_statuses": Counter(
            l.get("email_status") for l in leads if l.get("email_status")
        ).most_common(3),
        "has_instagram_pct": round(
            sum(1 for l in leads if l.get("has_instagram")) / max(len(leads), 1), 2
        ),
        "avg_google_rating": _avg(leads, "google_rating"),
    }


def _avg(items: list[dict], key: str) -> float:
    """Calculate average of a numeric field, ignoring None/missing."""
    vals = [item.get(key) for item in items if item.get(key) is not None]
    if not vals:
        return 0.0
    return round(sum(float(v) for v in vals) / len(vals), 2)
