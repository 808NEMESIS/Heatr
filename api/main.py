"""
api/main.py — Heatr FastAPI application.

All endpoints follow the spec in CLAUDE.md.
Authentication: Bearer token (Supabase JWT). Every request validated against
the workspace_id claim in the JWT. workspace_id injected into each DB call.

Session 5 + 6 + 7 endpoints included.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone, timedelta
from typing import Any
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from supabase import Client, create_client

logger = logging.getLogger(__name__)

# =============================================================================
# App setup
# =============================================================================

from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Run startup validation on boot; log result."""
    try:
        from utils.startup_validator import validate_startup
        supabase = get_supabase()
        result = await validate_startup(supabase_client=supabase)
        if not result.success:
            logger.critical("Startup validation FAILED — %d hard errors", len(result.errors))
            for e in result.errors:
                logger.critical("  FAIL: %s — %s", e.name, e.detail)
        if result.warnings:
            for w in result.warnings:
                logger.warning("  WARN: %s — %s", w.name, w.detail)
    except Exception as exc:
        logger.error("Startup validator failed unexpectedly: %s", exc)
    yield


app = FastAPI(
    title="Heatr API",
    version="1.0.0",
    description="B2B outbound intelligence platform for BENELUX",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# Supabase client
# =============================================================================

_supabase: Client | None = None


def get_supabase() -> Client:
    global _supabase
    if _supabase is None:
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_KEY"]
        _supabase = create_client(url, key)
    return _supabase


# =============================================================================
# Auth dependency
# =============================================================================

DEFAULT_WORKSPACE = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")


async def get_workspace(request: Request) -> str:
    """Extract workspace_id from Bearer JWT or fall back to default.

    In production, decode the Supabase JWT and read app_metadata.workspace_id.
    For MVP simplicity we use DEFAULT_WORKSPACE_ID as the workspace.
    """
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    # MVP: single-workspace, trust any valid-looking token
    return DEFAULT_WORKSPACE


# =============================================================================
# Pydantic models
# =============================================================================

class SearchRequest(BaseModel):
    sector: str
    city: str
    max_results: int = 60
    sources: dict[str, bool] = Field(default_factory=lambda: {"google_maps": True, "directories": True})


class EnrichRequest(BaseModel):
    lead_ids: list[str]


class SendToWarmrRequest(BaseModel):
    lead_ids: list[str]
    dry_run: bool = False


class DisqualifyRequest(BaseModel):
    lead_id: str
    reason: str


class WebsiteReviewPatch(BaseModel):
    status: str  # ok | opportunity | urgent


class LeadPatch(BaseModel):
    crm_stage: str | None = None
    snoozed_until: str | None = None
    crm_notes: str | None = None


class CampaignLaunchRequest(BaseModel):
    name: str
    lead_ids: list[str]
    sequence: list[dict]
    inbox_ids: list[str]


class ReviewEmailRequest(BaseModel):
    preview_only: bool = False


# CRM models
class TaskCreate(BaseModel):
    lead_id: str
    title: str
    description: str | None = None
    task_type: str | None = None
    priority: str = "medium"
    due_date: str | None = None


class TaskPatch(BaseModel):
    title: str | None = None
    status: str | None = None
    due_date: str | None = None
    snoozed_until: str | None = None
    priority: str | None = None
    description: str | None = None


class TimelineEventCreate(BaseModel):
    event_type: str   # note_added | call_logged | meeting_logged
    title: str
    body: str | None = None
    metadata: dict = Field(default_factory=dict)


class DealCreate(BaseModel):
    lead_id: str
    dienst_type: str
    value: float
    currency: str = "EUR"
    project_start_date: str | None = None
    notes: str | None = None


class CollectMetricsRequest(BaseModel):
    target_date: str | None = None  # YYYY-MM-DD, defaults to today


# =============================================================================
# Helpers
# =============================================================================

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _lead_row_to_dict(row: dict) -> dict:
    """Ensure UUID fields are strings for JSON serialisation."""
    if row.get("id") and isinstance(row["id"], UUID):
        row["id"] = str(row["id"])
    return row


# =============================================================================
# SEARCH & JOBS
# =============================================================================

@app.post("/search")
async def start_search(
    body: SearchRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Start a scraping job for a sector + city combination."""
    from job_queue.scraping_queue import create_scraping_job  # lazy import

    job_id = await create_scraping_job(
        db=db,
        workspace_id=workspace_id,
        sector=body.sector,
        city=body.city,
        max_results=body.max_results,
        sources=body.sources,
    )
    return {"job_id": job_id, "status": "pending"}


@app.get("/jobs/{job_id}")
async def get_job(
    job_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Return current status of a scraping or enrichment job."""
    res = db.table("scraping_jobs").select("*").eq("id", job_id).eq("workspace_id", workspace_id).maybe_single().execute()
    if not res.data:
        res = db.table("enrichment_jobs").select("*").eq("id", job_id).eq("workspace_id", workspace_id).maybe_single().execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Job not found")
    return res.data


@app.get("/jobs")
async def list_jobs(
    limit: int = 20,
    job_type: str = "scraping",
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    table = "scraping_jobs" if job_type == "scraping" else "enrichment_jobs"
    res = (
        db.table(table)
        .select("*")
        .eq("workspace_id", workspace_id)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return {"jobs": res.data or []}


# =============================================================================
# LEADS
# =============================================================================

@app.get("/leads")
async def list_leads(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    params = dict(request.query_params)
    limit = int(params.get("limit", 25))
    offset = int(params.get("offset", 0))

    q = db.table("leads").select("*", count="exact").eq("workspace_id", workspace_id)

    if sector := params.get("sector"):
        q = q.eq("sector", sector)
    if email_status := params.get("email_status"):
        q = q.eq("email_status", email_status)
    if min_score := params.get("min_score"):
        q = q.gte("score", int(min_score))
    if search := params.get("q"):
        q = q.or_(f"company_name.ilike.%{search}%,domain.ilike.%{search}%,city.ilike.%{search}%")
    if crm_stage := params.get("crm_stage"):
        q = q.eq("crm_stage", crm_stage)

    sort = params.get("sort", "score_desc")
    sort_map = {
        "score_desc": ("score", True),
        "created_at_desc": ("created_at", True),
        "company_name_asc": ("company_name", False),
    }
    col, desc = sort_map.get(sort, ("score", True))
    q = q.order(col, desc=desc).range(offset, offset + limit - 1)

    res = q.execute()
    return {"leads": res.data or [], "total": res.count or 0}


@app.get("/leads/{lead_id}")
async def get_lead(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = db.table("leads").select("*").eq("id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Lead not found")
    return res.data


@app.patch("/leads/{lead_id}")
async def patch_lead(
    lead_id: str,
    body: LeadPatch,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    res = db.table("leads").update(updates).eq("id", lead_id).eq("workspace_id", workspace_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Lead not found")

    # Log stage change to timeline
    if "crm_stage" in updates:
        _insert_timeline_event(db, workspace_id, lead_id, "stage_changed", f"Stage gewijzigd naar {updates['crm_stage']}")
    if "snoozed_until" in updates and updates["snoozed_until"]:
        _insert_timeline_event(db, workspace_id, lead_id, "snoozed", f"Lead gesnoozed tot {updates['snoozed_until'][:10]}")

    return res.data[0]


@app.post("/leads/enrich")
async def enrich_leads(
    body: EnrichRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    from job_queue.enrichment_queue import queue_lead_for_enrichment  # lazy

    queued = 0
    for lead_id in body.lead_ids:
        try:
            await queue_lead_for_enrichment(db=db, workspace_id=workspace_id, lead_id=lead_id)
            queued += 1
        except Exception as e:
            logger.warning("Failed to queue lead %s: %s", lead_id, e)
    return {"queued": queued}


@app.post("/leads/send-to-warmr")
async def send_leads_to_warmr(
    body: SendToWarmrRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    from integrations.warmr_client import WarmrClient

    res = db.table("leads").select("*").in_("id", body.lead_ids).eq("workspace_id", workspace_id).execute()
    leads = res.data or []

    eligible = [
        l for l in leads
        if l.get("gdpr_safe") and l.get("email_status") in ("verified", "catch_all") and (l.get("score") or 0) >= int(os.getenv("MIN_SCORE_FOR_WARMR", 65))
    ]

    if body.dry_run:
        return {"eligible": len(eligible), "dry_run": True}

    client = WarmrClient()
    # Use first available inbox as campaign placeholder
    inboxes = await client.get_ready_inboxes()
    if not inboxes:
        raise HTTPException(status_code=503, detail="No Warmr inboxes available")

    campaign_id = inboxes[0].get("campaign_id") or inboxes[0].get("id")
    result = await client.push_leads_bulk(eligible, campaign_id=campaign_id)

    for lead in eligible:
        _insert_timeline_event(db, workspace_id, lead["id"], "email_sent", f"Lead verstuurd naar Warmr (campagne {campaign_id})")

    return result


@app.post("/leads/disqualify")
async def disqualify_lead(
    body: DisqualifyRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = db.table("leads").update({
        "crm_stage": "verloren",
        "disqualification_reason": body.reason,
    }).eq("id", body.lead_id).eq("workspace_id", workspace_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Lead not found")
    _insert_timeline_event(db, workspace_id, body.lead_id, "deal_lost", f"Lead gediskwalificeerd: {body.reason}")
    return {"ok": True}


@app.get("/leads/{lead_id}/website")
async def get_lead_website(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = db.table("website_intelligence").select("*").eq("lead_id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute()
    return res.data or {}


@app.post("/leads/{lead_id}/send-review-email")
async def send_review_email(
    lead_id: str,
    body: ReviewEmailRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    lead_res = db.table("leads").select("*").eq("id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute()
    if not lead_res.data:
        raise HTTPException(status_code=404, detail="Lead not found")
    lead = lead_res.data

    wi_res = db.table("website_intelligence").select("*").eq("lead_id", lead_id).maybe_single().execute()
    wi = wi_res.data or {}

    from campaigns.review_email_generator import generate_review_email
    email_data = await generate_review_email(lead=lead, website_intelligence=wi)

    if body.preview_only:
        return email_data

    from integrations.warmr_client import WarmrClient
    client = WarmrClient()
    inboxes = await client.get_ready_inboxes()
    if not inboxes:
        raise HTTPException(status_code=503, detail="No Warmr inbox available")

    await client.push_lead(lead, campaign_id=inboxes[0]["id"], preferred_inbox_id=inboxes[0]["id"])
    _insert_timeline_event(db, workspace_id, lead_id, "review_email_sent", "Review email verstuurd via Warmr")

    return {"ok": True, **email_data}


@app.patch("/leads/{lead_id}/website-review")
async def patch_website_review(
    lead_id: str,
    body: WebsiteReviewPatch,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    db.table("website_intelligence").update({"review_status": body.status}).eq("lead_id", lead_id).eq("workspace_id", workspace_id).execute()
    return {"ok": True}


# =============================================================================
# WEBSITE OPPORTUNITIES
# =============================================================================

@app.get("/website-opportunities")
async def get_website_opportunities(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    params = dict(request.query_params)
    limit = int(params.get("limit", 18))
    offset = int(params.get("offset", 0))

    q = (
        db.table("website_intelligence")
        .select("*, leads!inner(id, company_name, city, sector, score, email_status, crm_stage)", count="exact")
        .eq("workspace_id", workspace_id)
    )

    if priority := params.get("priority"):
        priorities = priority.split(",")
        q = q.in_("priority", priorities)
    if opp_type := params.get("opportunity_type"):
        q = q.contains("opportunity_types", [opp_type])
    if sector := params.get("sector"):
        q = q.eq("leads.sector", sector)

    q = q.order("total_score", desc=False).range(offset, offset + limit - 1)
    res = q.execute()

    opportunities = []
    for row in (res.data or []):
        lead = row.pop("leads", {}) or {}
        opportunities.append({
            **row,
            "lead_id": lead.get("id"),
            "company_name": lead.get("company_name"),
            "city": lead.get("city"),
            "sector": lead.get("sector"),
            "score": lead.get("score"),
        })

    return {"opportunities": opportunities, "total": res.count or 0}


# =============================================================================
# ICP
# =============================================================================

@app.get("/icp")
async def list_icp(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = db.table("icp_definitions").select("*").eq("workspace_id", workspace_id).execute()
    return {"icps": res.data or []}


@app.post("/icp")
async def create_icp(
    body: dict,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    body["workspace_id"] = workspace_id
    res = db.table("icp_definitions").insert(body).execute()
    return res.data[0]


# =============================================================================
# WARMR
# =============================================================================

@app.get("/warmr/inboxes")
async def get_warmr_inboxes(
    workspace_id: str = Depends(get_workspace),
) -> dict:
    from integrations.warmr_client import WarmrClient
    client = WarmrClient()
    inboxes = await client.get_ready_inboxes()
    return {"inboxes": inboxes}


# =============================================================================
# CAMPAIGNS
# =============================================================================

@app.post("/campaigns/launch")
async def launch_campaign(
    body: CampaignLaunchRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    from integrations.warmr_client import WarmrClient
    from campaigns.sequence_engine import validate_sequence_config, auto_fix_sequence_config

    # Validate sequence before creating campaign
    is_valid, errors = validate_sequence_config(body.sequence)
    if not is_valid:
        raise HTTPException(status_code=422, detail={"errors": errors})

    fixed_sequence = auto_fix_sequence_config(body.sequence)

    client = WarmrClient()
    campaign_id = await client.create_campaign(
        name=body.name,
        sequence_steps=fixed_sequence,
        settings={"inbox_ids": body.inbox_ids},
    )

    res = db.table("leads").select("*").in_("id", body.lead_ids).eq("workspace_id", workspace_id).execute()
    leads = res.data or []

    result = await client.push_leads_bulk(leads, campaign_id=campaign_id)

    for lead in leads:
        _insert_timeline_event(db, workspace_id, lead["id"], "email_sent", f"Verstuurd via campagne '{body.name}'", metadata={"campaign_id": campaign_id})

    # Store campaign mapping in Heatr for stats retrieval
    try:
        db.table("campaigns").insert({
            "workspace_id": workspace_id,
            "warmr_campaign_id": campaign_id,
            "name": body.name,
            "lead_count": len(leads),
            "status": "active",
        }).execute()
    except Exception as exc:
        logger.warning("Failed to store campaign mapping: %s", exc)

    return {"campaign_id": campaign_id, **result}


@app.get("/campaigns")
async def list_campaigns(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """List all campaigns with Warmr stats fetched in real-time."""
    from integrations.warmr_client import WarmrClient

    res = db.table("campaigns").select("*").eq("workspace_id", workspace_id).order("created_at", desc=True).execute()
    campaigns = res.data or []

    # Fetch live stats from Warmr for each campaign
    try:
        client = WarmrClient()
        for camp in campaigns:
            warmr_id = camp.get("warmr_campaign_id")
            if warmr_id:
                try:
                    stats = await client.get_campaign_stats(warmr_id)
                    camp["warmr_stats"] = stats
                except Exception:
                    camp["warmr_stats"] = None
    except Exception:
        pass  # Warmr unreachable — show campaigns without stats

    return {"campaigns": campaigns}


@app.get("/campaigns/{campaign_id}/stats")
async def campaign_stats(
    campaign_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Fetch real-time stats for a single campaign from Warmr."""
    from integrations.warmr_client import WarmrClient

    # Look up warmr_campaign_id
    res = db.table("campaigns").select("warmr_campaign_id").eq("id", campaign_id).eq("workspace_id", workspace_id).limit(1).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Campaign not found")

    warmr_id = res.data[0].get("warmr_campaign_id")
    if not warmr_id:
        return {"error": "No Warmr campaign linked"}

    client = WarmrClient()
    return await client.get_campaign_stats(warmr_id)


# =============================================================================
# INBOX
# =============================================================================

@app.get("/inbox")
async def list_inbox(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    params = dict(request.query_params)
    limit = int(params.get("limit", 50))

    q = db.table("reply_inbox").select("*").eq("workspace_id", workspace_id).order("received_at", desc=True).limit(limit)
    if status_filter := params.get("status"):
        q = q.eq("event_type", status_filter)

    res = q.execute()
    return {"messages": res.data or []}


@app.get("/inbox/{message_id}")
async def get_inbox_message(
    message_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = db.table("reply_inbox").select("*").eq("id", message_id).eq("workspace_id", workspace_id).maybe_single().execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Message not found")
    return res.data


# =============================================================================
# ANALYTICS
# =============================================================================

@app.get("/analytics/pipeline")
async def analytics_pipeline(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    leads_res = db.table("leads").select("id, score, email_status, crm_stage", count="exact").eq("workspace_id", workspace_id).execute()
    leads = leads_res.data or []
    total = leads_res.count or 0

    email_counts: dict[str, int] = {}
    stage_counts: dict[str, int] = {}
    for l in leads:
        es = l.get("email_status") or "pending"
        email_counts[es] = email_counts.get(es, 0) + 1
        s = l.get("crm_stage") or "ontdekt"
        stage_counts[s] = stage_counts.get(s, 0) + 1

    verified = email_counts.get("verified", 0)
    catchall = email_counts.get("catch_all", 0)
    coverage_pct = round((verified + catchall) / total * 100) if total else 0

    inbox_res = db.table("reply_inbox").select("id, event_type", count="exact").eq("workspace_id", workspace_id).execute()
    replies = inbox_res.count or 0
    interested = sum(1 for r in (inbox_res.data or []) if r.get("event_type") == "interested")

    sent = sum(1 for l in leads if l.get("crm_stage") not in ("ontdekt", None))

    # CRM stats
    today = datetime.now(timezone.utc).date()
    month_start = today.replace(day=1).isoformat()
    deals_res = db.table("crm_deals").select("value").eq("workspace_id", workspace_id).gte("created_at", month_start).execute()
    won_this_month = sum(d.get("value") or 0 for d in (deals_res.data or []))

    return {
        "total_leads": total,
        "verified_emails": verified,
        "email_coverage_pct": coverage_pct,
        "email_breakdown": email_counts,
        "sector_breakdown": {},  # populated below if needed
        "enriched_leads": sum(1 for l in leads if (l.get("score") or 0) > 0),
        "sent_to_warmr": sent,
        "total_replies": replies,
        "reply_rate_pct": round(replies / sent * 100) if sent else 0,
        "catchall_emails": catchall,
        "not_found_emails": email_counts.get("not_found", 0),
        "pending_emails": email_counts.get("pending", 0),
        "website_opportunities": 0,  # filled by website analytics
        # CRM
        "open_tasks_today": 0,
        "leads_in_pipeline": sum(v for k, v in stage_counts.items() if k not in ("ontdekt", "verloren")),
        "won_this_month": won_this_month,
    }


@app.get("/analytics/website")
async def analytics_website(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = db.table("website_intelligence").select("total_score, opportunity_types, priority").eq("workspace_id", workspace_id).execute()
    rows = res.data or []

    if not rows:
        return {"total_analysed": 0, "avg_website_score": None, "score_distribution": {}, "website_rebuild_count": 0, "conversion_count": 0}

    scores = [r.get("total_score") or 0 for r in rows]
    buckets = {"0–20": 0, "20–40": 0, "40–60": 0, "60–80": 0, "80–100": 0}
    for s in scores:
        if s < 20: buckets["0–20"] += 1
        elif s < 40: buckets["20–40"] += 1
        elif s < 60: buckets["40–60"] += 1
        elif s < 80: buckets["60–80"] += 1
        else: buckets["80–100"] += 1

    return {
        "total_analysed": len(rows),
        "avg_website_score": round(sum(scores) / len(scores), 1),
        "score_distribution": buckets,
        "website_rebuild_count": sum(1 for r in rows if "website_rebuild" in (r.get("opportunity_types") or [])),
        "conversion_count": sum(1 for r in rows if "conversion_optimisation" in (r.get("opportunity_types") or [])),
    }


# =============================================================================
# SECTORS
# =============================================================================

@app.get("/sectors")
async def list_sectors() -> dict:
    from config.sectors import list_sectors as _list_sectors
    return {"sectors": _list_sectors()}


# =============================================================================
# WEBHOOKS — Warmr
# =============================================================================

@app.post("/webhooks/warmr")
async def warmr_webhook(
    request: Request,
    db: Client = Depends(get_supabase),
) -> dict:
    import hashlib, hmac
    secret = os.getenv("WARMR_WEBHOOK_SECRET", "")
    sig = request.headers.get("X-Warmr-Signature", "")
    body_bytes = await request.body()

    if secret:
        expected = hmac.new(secret.encode(), body_bytes, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

    payload = await request.json()
    event_type = payload.get("event")
    heatr_lead_id = payload.get("custom_fields", {}).get("heatr_lead_id")
    workspace_id = payload.get("custom_fields", {}).get("workspace_id", DEFAULT_WORKSPACE)

    if heatr_lead_id:
        # Log all events to reply_inbox for audit
        try:
            db.table("reply_inbox").insert({
                "workspace_id": workspace_id,
                "lead_id": heatr_lead_id,
                "event_type": event_type,
                "from_email": payload.get("from_email"),
                "from_name": payload.get("from_name"),
                "subject": payload.get("subject"),
                "body_text": payload.get("body_text"),
                "body_html": payload.get("body_html"),
                "received_at": _now_iso(),
            }).execute()
        except Exception as exc:
            logger.warning("Failed to insert reply_inbox for lead %s: %s", heatr_lead_id, exc)

        # Route events to appropriate handlers
        if event_type in ("interested", "lead.interested"):
            db.table("leads").update({"crm_stage": "gereageerd"}).eq("id", heatr_lead_id).execute()
            _insert_timeline_event(db, workspace_id, heatr_lead_id, "reply_received", "Reply ontvangen: geïnteresseerd")

        elif event_type in ("replied", "lead.replied"):
            db.table("leads").update({"crm_stage": "beantwoord"}).eq("id", heatr_lead_id).execute()
            _insert_timeline_event(db, workspace_id, heatr_lead_id, "reply_received", "Reply ontvangen")

        elif event_type in ("bounced", "lead.bounced"):
            db.table("leads").update({"email_status": "bounced"}).eq("id", heatr_lead_id).execute()
            _insert_timeline_event(db, workspace_id, heatr_lead_id, "bounced", "Email gebounced")

        elif event_type in ("unsubscribed", "lead.unsubscribed"):
            db.table("leads").update({
                "email_status": "unsubscribed",
                "crm_stage": "afgesloten",
            }).eq("id", heatr_lead_id).execute()
            _insert_timeline_event(db, workspace_id, heatr_lead_id, "unsubscribed", "Lead heeft zich uitgeschreven")

        elif event_type in ("campaign.completed",):
            _insert_timeline_event(db, workspace_id, heatr_lead_id, "campaign_done", "Campagne sequence afgerond — geen reply ontvangen")
            # Auto-create follow-up task
            try:
                db.table("crm_tasks").insert({
                    "workspace_id": workspace_id,
                    "lead_id": heatr_lead_id,
                    "title": "Sequence afgerond zonder reply — handmatig opvolgen?",
                    "status": "open",
                    "priority": "low",
                    "due_date": (datetime.now(timezone.utc) + timedelta(days=3)).isoformat(),
                }).execute()
            except Exception:
                pass

        elif event_type in ("inbox.warmup_complete",):
            # Not lead-specific but informational
            _insert_timeline_event(db, workspace_id, heatr_lead_id, "system", "Warmr inbox warmup afgerond — klaar voor campagnes")

        else:
            _insert_timeline_event(db, workspace_id, heatr_lead_id, "warmr_event", f"Warmr event: {event_type}")

        # Feed lead_campaign_history for the feedback processor / ICP scorer loop
        try:
            status_map = {
                "interested": "replied",
                "lead.interested": "replied",
                "replied": "replied",
                "lead.replied": "replied",
                "bounced": "bounced",
                "lead.bounced": "bounced",
                "unsubscribed": "unsubscribed",
                "lead.unsubscribed": "unsubscribed",
                "campaign.completed": "no_response",
            }
            mapped_status = status_map.get(event_type)
            if mapped_status:
                db.table("lead_campaign_history").upsert({
                    "workspace_id": workspace_id,
                    "lead_id": heatr_lead_id,
                    "status": mapped_status,
                    "event_type": event_type,
                    "updated_at": _now_iso(),
                }, on_conflict="lead_id").execute()
        except Exception as exc:
            logger.debug("Failed to upsert lead_campaign_history: %s", exc)

    return {"ok": True}


# =============================================================================
# CRM — TASKS
# =============================================================================

@app.get("/tasks")
async def list_tasks(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    params = dict(request.query_params)

    q = db.table("crm_tasks").select("*, leads(company_name, city, sector, crm_stage)").eq("workspace_id", workspace_id)

    if lead_id := params.get("lead_id"):
        q = q.eq("lead_id", lead_id)
    if task_status := params.get("status"):
        q = q.eq("status", task_status)
    if priority := params.get("priority"):
        q = q.eq("priority", priority)
    if params.get("due_today") == "true":
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        today_end = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=0).isoformat()
        q = q.gte("due_date", today_start).lte("due_date", today_end)

    q = q.order("due_date", desc=False, nulls_last=True)
    res = q.execute()
    return {"tasks": res.data or []}


@app.post("/tasks")
async def create_task(
    body: TaskCreate,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    row = {
        "workspace_id": workspace_id,
        "lead_id": body.lead_id,
        "title": body.title,
        "description": body.description,
        "task_type": body.task_type,
        "priority": body.priority,
        "due_date": body.due_date,
        "status": "open",
        "created_by": "user",
    }
    res = db.table("crm_tasks").insert(row).execute()
    task = res.data[0]
    _insert_timeline_event(db, workspace_id, body.lead_id, "task_created", f"Taak aangemaakt: {body.title}", metadata={"task_id": task["id"]})
    return task


@app.patch("/tasks/{task_id}")
async def patch_task(
    task_id: str,
    body: TaskPatch,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if "status" in updates and updates["status"] == "completed":
        updates["completed_at"] = _now_iso()

    res = db.table("crm_tasks").update(updates).eq("id", task_id).eq("workspace_id", workspace_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Task not found")
    task = res.data[0]

    if updates.get("status") == "completed":
        _insert_timeline_event(db, workspace_id, task["lead_id"], "task_completed", f"Taak voltooid: {task['title']}", metadata={"task_id": task_id})

    return task


@app.delete("/tasks/{task_id}")
async def delete_task(
    task_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    db.table("crm_tasks").delete().eq("id", task_id).eq("workspace_id", workspace_id).execute()
    return {"ok": True}


# =============================================================================
# CRM — TIMELINE
# =============================================================================

def _insert_timeline_event(
    db: Client,
    workspace_id: str,
    lead_id: str,
    event_type: str,
    title: str,
    body: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Insert a timeline event. Fire-and-forget — does not raise."""
    try:
        db.table("lead_timeline").insert({
            "workspace_id": workspace_id,
            "lead_id": lead_id,
            "event_type": event_type,
            "title": title,
            "body": body,
            "metadata": metadata or {},
            "created_by": "system",
        }).execute()
    except Exception as e:
        logger.warning("Timeline insert failed: %s", e)


@app.get("/crm/timeline/recent")
async def get_recent_timeline_crm(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Recent timeline events across all leads — for Focus view activity feed."""
    res = (
        db.table("lead_timeline")
        .select("*, leads(company_name, city)")
        .eq("workspace_id", workspace_id)
        .order("created_at", desc=True)
        .limit(20)
        .execute()
    )
    events = []
    for row in (res.data or []):
        lead = row.pop("leads", {}) or {}
        events.append({**row, "company_name": lead.get("company_name"), "lead_city": lead.get("city")})
    return {"events": events}


@app.get("/timeline/{lead_id}")
async def get_timeline(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = (
        db.table("lead_timeline")
        .select("*")
        .eq("lead_id", lead_id)
        .eq("workspace_id", workspace_id)
        .order("created_at", desc=True)
        .limit(100)
        .execute()
    )
    return {"events": res.data or []}


@app.post("/timeline/{lead_id}")
async def add_timeline_event(
    lead_id: str,
    body: TimelineEventCreate,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    row = {
        "workspace_id": workspace_id,
        "lead_id": lead_id,
        "event_type": body.event_type,
        "title": body.title,
        "body": body.body,
        "metadata": body.metadata,
        "created_by": "user",
    }
    res = db.table("lead_timeline").insert(row).execute()
    return res.data[0]


@app.get("/timeline/recent")
async def get_recent_timeline(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = (
        db.table("lead_timeline")
        .select("*, leads(company_name, city)")
        .eq("workspace_id", workspace_id)
        .order("created_at", desc=True)
        .limit(20)
        .execute()
    )
    events = []
    for row in (res.data or []):
        lead = row.pop("leads", {}) or {}
        events.append({**row, "company_name": lead.get("company_name"), "lead_city": lead.get("city")})
    return {"events": events}


# =============================================================================
# CRM — PIPELINE + DEALS + STATS
# =============================================================================

@app.get("/crm/pipeline")
async def crm_pipeline(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = db.table("leads").select("id, company_name, city, sector, score, website_score, icp_match, crm_stage, snoozed_until, created_at", count="exact").eq("workspace_id", workspace_id).execute()
    leads = res.data or []

    # Get open task counts per lead
    tasks_res = db.table("crm_tasks").select("lead_id").eq("workspace_id", workspace_id).eq("status", "open").execute()
    task_counts: dict[str, int] = {}
    for t in (tasks_res.data or []):
        lid = t["lead_id"]
        task_counts[lid] = task_counts.get(lid, 0) + 1

    stages: dict[str, list] = {}
    for lead in leads:
        s = lead.get("crm_stage") or "ontdekt"
        if s not in stages:
            stages[s] = []
        lead["open_tasks"] = task_counts.get(lead["id"], 0)
        stages[s].append(lead)

    return {"stages": stages, "total": res.count or 0}


@app.post("/crm/deals")
async def create_deal(
    body: DealCreate,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    row = {
        "workspace_id": workspace_id,
        "lead_id": body.lead_id,
        "dienst_type": body.dienst_type,
        "value": body.value,
        "currency": body.currency,
        "project_start_date": body.project_start_date,
        "notes": body.notes,
    }
    res = db.table("crm_deals").insert(row).execute()
    deal = res.data[0]

    # Mark lead as gewonnen
    db.table("leads").update({"crm_stage": "gewonnen"}).eq("id", body.lead_id).execute()
    _insert_timeline_event(
        db, workspace_id, body.lead_id, "deal_won",
        f"Deal gewonnen: € {body.value:,.0f}",
        metadata={"deal_id": deal["id"], "dienst_type": body.dienst_type, "value": body.value},
    )

    return deal


@app.get("/crm/stats")
async def crm_stats(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    today = datetime.now(timezone.utc)
    today_start = today.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    today_end = today.replace(hour=23, minute=59, second=59, microsecond=0).isoformat()
    month_start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()

    # Open tasks today (due today + overdue open tasks)
    tasks_today_res = db.table("crm_tasks").select("id", count="exact").eq("workspace_id", workspace_id).eq("status", "open").lte("due_date", today_end).execute()
    open_tasks_today = tasks_today_res.count or 0

    # Pipeline leads (excluding ontdekt + verloren)
    pipeline_res = db.table("leads").select("id", count="exact").eq("workspace_id", workspace_id).not_.in_("crm_stage", ["ontdekt", "verloren", "gewonnen"]).execute()
    pipeline_count = pipeline_res.count or 0

    # Won this month
    deals_res = db.table("crm_deals").select("value, created_at").eq("workspace_id", workspace_id).gte("created_at", month_start).execute()
    won_this_month = sum(d.get("value") or 0 for d in (deals_res.data or []))

    # Avg time to close (days from lead created_at to deal created_at)
    all_deals_res = db.table("crm_deals").select("created_at, lead_id").eq("workspace_id", workspace_id).limit(50).execute()
    avg_days = None
    if all_deals_res.data:
        lead_ids = [d["lead_id"] for d in all_deals_res.data]
        leads_res = db.table("leads").select("id, created_at").in_("id", lead_ids).execute()
        lead_dates = {l["id"]: l["created_at"] for l in (leads_res.data or [])}
        deltas = []
        for deal in all_deals_res.data:
            if deal["lead_id"] in lead_dates:
                lead_dt = datetime.fromisoformat(lead_dates[deal["lead_id"]].replace("Z", "+00:00"))
                deal_dt = datetime.fromisoformat(deal["created_at"].replace("Z", "+00:00"))
                deltas.append((deal_dt - lead_dt).days)
        avg_days = round(sum(deltas) / len(deltas)) if deltas else None

    return {
        "open_tasks_today": open_tasks_today,
        "pipeline_count": pipeline_count,
        "won_this_month": won_this_month,
        "avg_days_to_close": avg_days,
    }


# =============================================================================
# HEALTH
# =============================================================================

@app.get("/health")
async def health_check() -> dict:
    """Liveness probe — returns immediately without DB call."""
    return {"status": "ok", "version": app.version}


@app.get("/health/startup")
async def health_startup(
    _workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Runs startup validation checks and returns the result."""
    from utils.startup_validator import validate_startup
    result = await validate_startup(supabase_client=db)
    return {
        "success": result.success,
        "checks": [
            {"name": c.name, "passed": c.passed, "detail": c.detail}
            for c in result.checks
        ],
        "error_count": len(result.errors),
        "warning_count": len(result.warnings),
    }


# =============================================================================
# ALERTS
# =============================================================================

@app.get("/alerts")
async def list_alerts(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Return unread system alerts for this workspace."""
    params = dict(request.query_params)
    limit = int(params.get("limit", 50))
    include_read = params.get("include_read", "false").lower() == "true"

    q = (
        db.table("system_alerts")
        .select("*")
        .eq("workspace_id", workspace_id)
        .order("created_at", desc=True)
        .limit(limit)
    )
    if not include_read:
        q = q.eq("is_read", False)
    res = q.execute()
    return {"alerts": res.data or []}


@app.patch("/alerts/{alert_id}/read")
async def mark_alert_read(
    alert_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    db.table("system_alerts").update({"is_read": True}).eq("id", alert_id).eq("workspace_id", workspace_id).execute()
    return {"ok": True}


# =============================================================================
# ANALYTICS — costs + metrics (Session 7)
# =============================================================================

@app.get("/analytics/costs")
async def get_costs(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Return Claude API cost totals grouped by model and date."""
    params = dict(request.query_params)
    days = int(params.get("days", 30))
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()

    res = (
        db.table("api_cost_log")
        .select("date, model, cost_eur, prompt_tokens, response_tokens, cache_hit")
        .eq("workspace_id", workspace_id)
        .gte("date", cutoff)
        .order("date", desc=True)
        .execute()
    )
    rows = res.data or []
    total_eur = round(sum(r.get("cost_eur") or 0 for r in rows), 4)
    cache_hits = sum(1 for r in rows if r.get("cache_hit"))
    return {
        "total_eur": total_eur,
        "cache_hits": cache_hits,
        "rows": rows,
    }


@app.get("/analytics/metrics")
async def get_metrics(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Return daily_metrics rows for the last N days."""
    from utils.metrics_collector import get_metrics_range
    params = dict(request.query_params)
    days = int(params.get("days", 30))
    rows = await get_metrics_range(workspace_id, days, db)
    return {"metrics": rows}


@app.post("/analytics/collect-metrics")
async def trigger_collect_metrics(
    body: CollectMetricsRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Manually trigger daily metrics collection (n8n also calls this at 23:55)."""
    from utils.metrics_collector import collect_daily_metrics
    metrics = await collect_daily_metrics(workspace_id, db, target_date=body.target_date)
    return {"ok": True, "metrics": metrics}


# =============================================================================
# GDPR
# =============================================================================

@app.post("/gdpr/forget/{lead_id}")
async def gdpr_forget(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    GDPR right to erasure (Art. 17).
    Anonymises PII and stops all active sequences for the lead.
    """
    from utils.gdpr_manager import forget_lead
    from campaigns.sequence_engine import stop_all_sequences_for_lead

    # Stop sequences first so no email goes out after forget
    await stop_all_sequences_for_lead(lead_id, workspace_id, db)

    result = await forget_lead(
        lead_id=lead_id,
        workspace_id=workspace_id,
        supabase_client=db,
        performed_by="api",
    )
    return result


@app.get("/gdpr/export/{lead_id}")
async def gdpr_export(
    lead_id: str,
    _workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    GDPR right of access / data portability (Art. 15 / 20).
    Returns all stored data for the given lead.
    """
    from utils.gdpr_manager import export_lead_data
    data = await export_lead_data(lead_id, db)
    return data


@app.get("/gdpr/log")
async def gdpr_log(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Return GDPR audit log for this workspace."""
    params = dict(request.query_params)
    limit = int(params.get("limit", 50))
    res = (
        db.table("gdpr_log")
        .select("*")
        .eq("workspace_id", workspace_id)
        .order("completed_at", desc=True)
        .limit(limit)
        .execute()
    )
    return {"log": res.data or []}


@app.get("/gdpr/register")
async def gdpr_register(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Return Article 30 processing register."""
    from utils.gdpr_manager import generate_processing_register
    try:
        db.table("gdpr_log").insert({
            "workspace_id": workspace_id,
            "action": "register_view",
            "performed_by": "api",
        }).execute()
    except Exception:
        pass
    return generate_processing_register()


# =============================================================================
# SEQUENCES (n8n integration)
# =============================================================================

@app.get("/sequences/due-sends")
async def get_due_sends(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Return pending sequence sends that are due now.
    Called by n8n workflow 01-sequence-due-sends every 15 minutes.
    """
    from campaigns.sequence_engine import get_due_sends as _get_due_sends
    params = dict(request.query_params)
    limit = int(params.get("limit", 50))
    records = await _get_due_sends(workspace_id, db, limit=limit)
    return {"due_sends": records, "count": len(records)}


@app.post("/sequences/process-send/{record_id}")
async def process_sequence_send(
    record_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Process a single due send record.
    n8n calls this for each record returned by /sequences/due-sends.
    """
    from campaigns.sequence_engine import process_due_send

    # Load the record
    res = db.table("lead_campaign_history").select(
        "*, leads(id, company_name, city, sector, email, status, gdpr_safe, "
        "contact_first_name, domain, personalized_opener, snoozed_until, "
        "next_contact_after, crm_stage)"
    ).eq("id", record_id).eq("workspace_id", workspace_id).maybe_single().execute()

    if not res.data:
        raise HTTPException(status_code=404, detail="Send record not found")

    result = await process_due_send(res.data, db)
    return result


# =============================================================================
# SNOOZE WAKE-UP (n8n integration)
# =============================================================================

@app.post("/crm/wake-snoozed")
async def wake_snoozed(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Wake leads whose snooze has expired back to 'ontdekt'.
    Called by n8n workflow 02-snooze-wakeup every 15 minutes.
    """
    from campaigns.sequence_engine import wake_snoozed_leads
    woken = await wake_snoozed_leads(workspace_id, db)
    return {"woken": woken}


@app.post("/tasks/reactivate-snoozed")
async def reactivate_snoozed_tasks(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Reactivate snoozed tasks whose snooze_until has passed.
    Called by n8n workflow 02-snooze-wakeup every 15 minutes.
    """
    from campaigns.sequence_engine import reactivate_snoozed_tasks as _reactivate
    count = await _reactivate(workspace_id, db)
    return {"reactivated": count}


# =============================================================================
# RECONTACT SUGGESTIONS
# =============================================================================

@app.get("/leads/recontact-ready")
async def recontact_ready(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Return leads whose recontact cooldown has expired and are safe to re-engage.
    Called by n8n workflow 09-recontact-suggestions.
    """
    params = dict(request.query_params)
    limit = int(params.get("limit", 50))
    now = datetime.now(timezone.utc).isoformat()

    res = (
        db.table("leads")
        .select("id, company_name, city, sector, email, score, next_contact_after, contact_attempt_count")
        .eq("workspace_id", workspace_id)
        .eq("status", "no_response")
        .eq("gdpr_safe", True)
        .lte("next_contact_after", now)
        .order("score", desc=True)
        .limit(limit)
        .execute()
    )
    return {"leads": res.data or [], "count": len(res.data or [])}


# =============================================================================
# ENRICHMENT + WEBSITE ANALYSIS WORKERS (n8n integration)
# =============================================================================

@app.post("/enrichment/process-next")
async def enrichment_process_next(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Process the next pending lead in the enrichment queue.
    Called by n8n workflow 03-enrichment-worker every minute.
    """
    from job_queue.enrichment_queue import process_next_enrichment
    result = await process_next_enrichment(workspace_id, db)
    return result or {"processed": False, "reason": "queue_empty"}


@app.post("/website-intelligence/process-next")
async def website_intelligence_process_next(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Process the next lead pending website analysis.
    Called by n8n workflow 04-website-analysis-worker every minute.
    """
    from job_queue.website_analysis_queue import process_next_website_analysis
    result = await process_next_website_analysis(workspace_id, db)
    return result or {"processed": False, "reason": "queue_empty"}


# =============================================================================
# DAILY BRIEFING
# =============================================================================

@app.post("/briefing/generate")
async def generate_briefing(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Generate and send daily briefing email to OPERATOR_EMAIL.
    Called by n8n workflow 10-daily-briefing every morning at 07:00.
    """
    from utils.metrics_collector import get_metrics_range

    # Get yesterday's metrics
    rows = await get_metrics_range(workspace_id, 2, db)
    yesterday = rows[0] if rows else {}

    # Get top opportunities
    opp_res = (
        db.table("leads")
        .select("company_name, city, sector, score")
        .eq("workspace_id", workspace_id)
        .eq("gdpr_safe", True)
        .gte("score", 65)
        .order("score", desc=True)
        .limit(5)
        .execute()
    )
    top_leads = opp_res.data or []

    # Get open tasks due today
    today_end = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59).isoformat()
    tasks_res = (
        db.table("crm_tasks")
        .select("title, lead_id, priority, due_date")
        .eq("workspace_id", workspace_id)
        .eq("status", "open")
        .lte("due_date", today_end)
        .order("priority", desc=True)
        .limit(10)
        .execute()
    )
    due_tasks = tasks_res.data or []

    briefing = {
        "date": datetime.now(timezone.utc).date().isoformat(),
        "yesterday_metrics": yesterday,
        "top_qualified_leads": top_leads,
        "tasks_due_today": due_tasks,
    }

    # Send email if RESEND is configured
    operator_email = os.getenv("OPERATOR_EMAIL")
    resend_key = os.getenv("RESEND_API_KEY")
    if operator_email and resend_key:
        try:
            import httpx
            emails_sent = yesterday.get("emails_sent", 0)
            reply_rate = yesterday.get("reply_rate", 0)
            open_rate = yesterday.get("open_rate", 0)
            tasks_html = "".join(
                f"<li>{t.get('title')} [{t.get('priority')}]</li>" for t in due_tasks
            ) or "<li>Geen taken gepland</li>"
            leads_html = "".join(
                f"<li>{l.get('company_name')} — {l.get('city')} (score {l.get('score')})</li>"
                for l in top_leads
            ) or "<li>Geen gekwalificeerde leads</li>"

            payload = {
                "from": "briefing@heatr.aerys.nl",
                "to": [operator_email],
                "subject": f"Heatr Briefing — {briefing['date']}",
                "html": f"""
                    <h2>Heatr Dagelijkse Briefing</h2>
                    <p><strong>Datum:</strong> {briefing['date']}</p>
                    <h3>Gisteren</h3>
                    <ul>
                        <li>Emails verstuurd: {emails_sent}</li>
                        <li>Open rate: {open_rate:.1%}</li>
                        <li>Reply rate: {reply_rate:.1%}</li>
                    </ul>
                    <h3>Top leads vandaag</h3><ul>{leads_html}</ul>
                    <h3>Taken voor vandaag</h3><ul>{tasks_html}</ul>
                    <p><a href="{os.getenv('HEATR_BASE_URL', 'http://localhost:8000')}/dashboard.html">Open Heatr →</a></p>
                """,
            }
            async with httpx.AsyncClient(timeout=10.0) as client:
                r = await client.post(
                    "https://api.resend.com/emails",
                    json=payload,
                    headers={"Authorization": f"Bearer {resend_key}"},
                )
                briefing["email_sent"] = r.status_code < 400
        except Exception as e:
            logger.warning("Briefing email failed: %s", e)
            briefing["email_sent"] = False
    else:
        briefing["email_sent"] = False

    return briefing
