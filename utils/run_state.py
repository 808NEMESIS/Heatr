"""
utils/run_state.py — per-lead run-state voor de Control Plane Inspect-laag.

Read-only composer over bestaande Heatr-data (zie
docs/audits/sprint2_runstate_inventory.md voor de bronnen-map). Toont wat
er ÍS; wat een event-log zou vereisen staat expliciet in `gaps` zodat de
UI eerlijk markeert wat "vereist run-history-fundament (I7)" is.

Pipeline-afleiding is bewust grofmazig: Heatr heeft geen per-stap-events,
wel timestamps (created_at → enriched → scored → pushed) en gekoppelde
tabellen (campaign_history, reply_inbox). Geen aannames verzinnen die de
data niet draagt.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Eerlijke markering van wat de inspect-laag NIET kan tonen zonder event-log
KNOWN_GAPS = [
    "Per-attempt retry-history (anthropic_retry logt alleen naar de logger) — vereist run-history-fundament (I7)",
    "Step-timestamps binnen een enrichment-run (steps_completed heeft geen tijden) — vereist run-history-fundament (I7)",
]


def _safe_rows(db: Any, query_fn) -> list[dict]:
    """Voer een query uit; lege lijst bij elke fout (inspect mag nooit 500'en
    op één ontbrekende tabel — de rest van het beeld blijft bruikbaar)."""
    try:
        res = query_fn()
        return res.data or []
    except Exception as e:
        logger.debug("run_state: deelquery faalde (%s)", e)
        return []


def build_lead_run_state(lead: dict, workspace_id: str, db: Any) -> dict[str, Any]:
    """Componeer de volledige run-state voor één lead (read-only)."""
    lead_id = lead["id"]

    # ── Gekoppelde data ────────────────────────────────────────────────
    jobs = _safe_rows(db, lambda: (
        db.table("enrichment_jobs")
        .select("id, status, current_step, steps_completed, retry_count, error_message, created_at, completed_at")
        .eq("lead_id", lead_id).eq("workspace_id", workspace_id)
        .order("created_at", desc=True).limit(5).execute()
    ))
    campaign_rows = _safe_rows(db, lambda: (
        db.table("lead_campaign_history")
        .select("id, campaign_id, inbox_id, step_index, status, is_active, next_send_at, sent_at, block_reason, restart_epoch, created_at, sequence_steps")
        .eq("lead_id", lead_id).eq("workspace_id", workspace_id)
        .order("created_at", desc=True).limit(10).execute()
    ))
    replies = _safe_rows(db, lambda: (
        db.table("reply_inbox")
        .select("id, classification, received_at, created_at")
        .eq("lead_id", lead_id).eq("workspace_id", workspace_id)
        .order("created_at", desc=True).limit(10).execute()
    ))
    side_effects = _safe_rows(db, lambda: (
        db.table("outbound_log")
        .select("id, idempotency_key, kind, status, actor, error, created_at, metadata")
        .eq("workspace_id", workspace_id).eq("lead_id", lead_id)
        .order("created_at", desc=True).limit(25).execute()
    ))
    blocked = _safe_rows(db, lambda: (
        db.table("blocked_sends")
        .select("reason, inbox_id, blocked_at, created_at")
        .eq("lead_id", lead_id).eq("workspace_id", workspace_id)
        .order("created_at", desc=True).limit(10).execute()
    ))
    cost_rows = _safe_rows(db, lambda: (
        db.table("api_cost_log")
        .select("cost_eur, context, cache_hit")
        .eq("lead_id", lead_id).eq("workspace_id", workspace_id)
        .execute()
    ))
    timeline = _safe_rows(db, lambda: (
        db.table("lead_timeline")
        .select("event_type, title, created_at, created_by")
        .eq("lead_id", lead_id).eq("workspace_id", workspace_id)
        .order("created_at", desc=True).limit(20).execute()
    ))

    # ── Cost-aggregatie ────────────────────────────────────────────────
    by_context: dict[str, float] = {}
    for r in cost_rows:
        ctx = (r.get("context") or "unknown").split(":", 1)[0]
        by_context[ctx] = by_context.get(ctx, 0.0) + float(r.get("cost_eur") or 0)
    cost = {
        "total_eur": round(sum(by_context.values()), 6),
        "call_count": len(cost_rows),
        "cache_hits": sum(1 for r in cost_rows if r.get("cache_hit")),
        "by_context": {k: round(v, 6) for k, v in sorted(by_context.items(), key=lambda kv: -kv[1])},
    }

    # ── Grofmazige pipeline-afleiding ──────────────────────────────────
    last_reply_at = replies[0].get("received_at") or replies[0].get("created_at") if replies else None
    sent_at = lead.get("pushed_to_warmr_at") or (
        campaign_rows[0].get("sent_at") if campaign_rows else None
    )
    pipeline = [
        {"step": "scraped", "reached": True, "at": lead.get("created_at"),
         "detail": lead.get("source") or "onbekende bron"},
        {"step": "enriched", "reached": (lead.get("enrichment_version") or 0) > 0,
         "at": lead.get("enriched_at"),
         "detail": f"enrichment v{lead.get('enrichment_version') or 0}"},
        {"step": "scored", "reached": lead.get("scored_at") is not None,
         "at": lead.get("scored_at"),
         "detail": f"score {lead.get('score')}, icp {lead.get('icp_match')}"},
        {"step": "sent", "reached": bool(sent_at), "at": sent_at,
         "detail": f"{len(campaign_rows)} campagne-koppeling(en)" if campaign_rows else "nog niet verstuurd"},
        {"step": "replied", "reached": bool(replies), "at": last_reply_at,
         "detail": (replies[0].get("classification") or "ongeclassificeerd") if replies else "geen reply"},
    ]
    current = next((p["step"] for p in reversed(pipeline) if p["reached"]), "scraped")

    # ── Readiness (bestaande composer) ─────────────────────────────────
    from utils.launch_readiness import assess_launch_readiness
    readiness = assess_launch_readiness(lead)

    return {
        "lead_id": lead_id,
        "company_name": lead.get("company_name"),
        "status": lead.get("status"),
        "crm_stage": lead.get("crm_stage"),
        "pipeline": pipeline,
        "current_step": current,
        "readiness": readiness,
        "jobs": jobs,
        "campaign_history": [
            {**{k: v for k, v in row.items() if k != "sequence_steps"},
             "sequence_step_count": len(row.get("sequence_steps") or [])}
            for row in campaign_rows
        ],
        "side_effects": side_effects,
        "blocked_sends": blocked,
        "cost": cost,
        "timeline": timeline,
        "gaps": KNOWN_GAPS,
    }
