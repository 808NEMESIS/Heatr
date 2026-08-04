"""
api/main.py — Heatr FastAPI application.

All endpoints follow the spec in CLAUDE.md.
Authentication: Bearer token (Supabase JWT). Every request validated against
the workspace_id claim in the JWT. workspace_id injected into each DB call.

Session 5 + 6 + 7 endpoints included.
"""

from __future__ import annotations

import json
import logging
import os
import secrets as _secrets
from datetime import date, datetime, timezone, timedelta
from typing import Any
from uuid import UUID

# Laad .env bij startup zodat env-wijzigingen bij een herstart altijd meekomen.
# Vóór alle os.getenv-lezingen. Geen .env (bv. in prod, env via platform) →
# load_dotenv is een stille no-op. override=False: echte env-vars winnen.
try:
    from dotenv import load_dotenv

    load_dotenv(override=False)
    # .env.local — gitignored local-overrides-bestand (cascade: .env < .env.local).
    # override=True zodat een lokaal geplakte secret (bv. GOOGLE_PLACES_API_KEY)
    # de basis-.env wint. Stille no-op als het bestand er niet is.
    import os as _os
    _local_env = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), ".env.local")
    load_dotenv(_local_env, override=True)
except ImportError:  # dotenv is een dep (requirements.txt); defensief voor kale runs
    pass

import jwt as _jwt
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request, Response, status
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

# CORS-origins uit env (HEATR_ALLOWED_ORIGINS, komma-gescheiden). Default "*"
# voor dev; in prod zet je de unified-origin zodat het niet wagenwijd openstaat.
_cors_origins_raw = os.getenv("HEATR_ALLOWED_ORIGINS", "").strip()
_cors_origins = [o.strip() for o in _cors_origins_raw.split(",") if o.strip()] or ["*"]

# GUARD (recovery-fix): `allow_origins=["*"]` mét `allow_credentials=True` is een
# ongeldige én gevaarlijke combinatie (browsers weigeren 'm, maar de headers
# stonden wagenwijd open). Bij een wildcard zetten we credentials daarom UIT;
# alleen een expliciete origin-allowlist mag credentials voeren.
_cors_wildcard = "*" in _cors_origins
if _cors_wildcard:
    logger.warning(
        "CORS staat op wildcard '*' (HEATR_ALLOWED_ORIGINS niet gezet) — "
        "credentials uitgeschakeld. Zet HEATR_ALLOWED_ORIGINS in productie."
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=not _cors_wildcard,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# Supabase client
# =============================================================================

_supabase = None


def get_supabase():
    """Return a Supabase client wrapped with Heatr table prefix.

    All .table("leads") calls automatically become .table("heatr_leads")
    when HEATR_TABLE_PREFIX is set (default: "heatr_").
    """
    global _supabase
    if _supabase is None:
        from config.database import get_heatr_supabase
        _supabase = get_heatr_supabase()
    return _supabase


# =============================================================================
# Auth dependency
# =============================================================================

DEFAULT_WORKSPACE = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")


def _service_key_workspace(request: Request) -> str | None:
    """X-API-Key path — service-to-service (worker, n8n, scripts).

    Constant-time compare against HEATR_API_KEY. Returns DEFAULT_WORKSPACE on match.
    """
    incoming = request.headers.get("X-API-Key", "")
    expected = os.getenv("HEATR_API_KEY", "")
    if not incoming or not expected or len(expected) < 32:
        return None
    if _secrets.compare_digest(incoming, expected):
        return DEFAULT_WORKSPACE
    return None


def _jwt_workspace(request: Request) -> str | None:
    """Supabase JWT path — voor browser-clients.

    Decodes Supabase HS256-signed JWT, reads workspace_id from app_metadata.

    FAIL-CLOSED (fase 4 PR 12, audit v2 P1-1/scenario 11): een geldig
    getekende JWT ZONDER workspace_id-claim wordt geweigerd — voorheen
    kreeg zo'n token stilzwijgend DEFAULT_WORKSPACE en daarmee volledige
    lees/schrijftoegang tot de aerys-data. De oude fallback bestaat alleen
    nog achter de expliciete cutover-flag HEATR_JWT_WORKSPACE_FALLBACK
    (default uit; zie runbook voor de provisioning-SQL die bestaande
    Supabase-users de claim geeft).
    """
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:].strip()
    if not token:
        return None
    secret = os.getenv("SUPABASE_JWT_SECRET", "")
    if not secret:
        return None
    try:
        payload = _jwt.decode(
            token, secret, algorithms=["HS256"], audience="authenticated",
        )
    except _jwt.InvalidTokenError:
        return None
    app_meta = payload.get("app_metadata") or {}
    claimed = app_meta.get("workspace_id")
    if claimed:
        return claimed
    # Geen claim: alleen doorlaten als de expliciete cutover/dev-flag aanstaat.
    if os.getenv("HEATR_JWT_WORKSPACE_FALLBACK", "false").strip().lower() == "true":
        logger.warning(
            "auth: JWT zonder workspace_id-claim toegelaten via "
            "HEATR_JWT_WORKSPACE_FALLBACK → %s (sub=%s). Provision de claim "
            "en zet de flag uit (zie runbook).",
            DEFAULT_WORKSPACE, payload.get("sub", "?"),
        )
        return DEFAULT_WORKSPACE
    logger.warning(
        "auth: JWT GEWEIGERD — geen app_metadata.workspace_id-claim (sub=%s). "
        "Fail-closed (fase 4 PR 12).", payload.get("sub", "?"),
    )
    return None


def _legacy_dev_token(request: Request) -> str | None:
    """Migratie-pad: oude `Bearer dev-token` accepteren als LEGACY_DEV_TOKEN_ALLOWED=true.

    Standaard UIT. Zet alleen aan tijdens frontend-cutover (max 24h aanbevolen).
    """
    if os.getenv("LEGACY_DEV_TOKEN_ALLOWED", "false").lower() != "true":
        return None
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:].strip()
        # GUARD (recovery-fix): accepteer alléén het letterlijke `dev-token`,
        # niet "elke string >= 5 tekens". Voorheen was de legacy-flag een
        # volledige auth-bypass voor willekeurige Bearer-waarden.
        if _secrets.compare_digest(token, "dev-token"):
            return DEFAULT_WORKSPACE
    return None


async def get_workspace(request: Request) -> str:
    """Resolve workspace_id from request auth.

    Order: X-API-Key (service) → Supabase JWT (browser) → LEGACY (env-gated).
    No match → 401.
    """
    ws = (
        _service_key_workspace(request)
        or _jwt_workspace(request)
        or _legacy_dev_token(request)
    )
    if not ws:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Auth failed — provide X-API-Key (service) or valid Supabase Bearer JWT (user)",
        )
    return ws


async def require_service_key(request: Request) -> str:
    """Stricter dependency: alleen service-key (X-API-Key) accepteren.

    Gebruik op endpoints die geen browser-toegang horen te hebben (campagne-launch,
    bulk-deletes, system-overrides). Browser-JWTs worden hier geweigerd.
    """
    ws = _service_key_workspace(request)
    if not ws:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Service-only endpoint — vereist X-API-Key header met geldig HEATR_API_KEY",
        )
    return ws


def identify_principal(request: Request) -> dict[str, str]:
    """Inspect request en return wie deze actie uitvoert.

    Used voor audit-log van campagne-launches en andere gevoelige acties.
    Returns: {created_by, created_via, request_ip}
    """
    # IP — eerste van X-Forwarded-For, anders client.host
    ip = (request.headers.get("X-Forwarded-For") or "").split(",")[0].strip() or (
        request.client.host if request.client else ""
    )

    # Service-key path
    if request.headers.get("X-API-Key"):
        # Optionele service-naming via X-Service-Name header
        service = request.headers.get("X-Service-Name", "default")
        return {"created_by": f"service:{service}", "created_via": "service_key", "request_ip": ip}

    # JWT path
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:].strip()
        secret = os.getenv("SUPABASE_JWT_SECRET", "")
        if secret and token != "dev-token":
            try:
                payload = _jwt.decode(
                    token, secret, algorithms=["HS256"], audience="authenticated",
                )
                email = payload.get("email") or payload.get("sub") or "unknown"
                return {"created_by": f"user:{email}", "created_via": "user_jwt", "request_ip": ip}
            except _jwt.InvalidTokenError:
                pass
        # Legacy fallback
        return {"created_by": "legacy_dev", "created_via": "legacy_dev", "request_ip": ip}

    return {"created_by": "unknown", "created_via": "unknown", "request_ip": ip}


# =============================================================================
# Pydantic models
# =============================================================================

class SearchRequest(BaseModel):
    sector: str
    city: str
    subcategory_keys: list[str] = []                # ['injectables_anti_aging', 'huidtherapie', ...]
    custom_query: str | None = None                  # override → 1 job met deze exact query
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
    sequence: list[dict] = []  # leeg = AUTO mode: per-lead pick_brug() → v3.1 brug-template
    template_id: str | None = None  # v3_1_* forceert single brug; v1-IDs worden server-side naar v3.1 ge-rerouted
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


# --- Check-up follow-up (calls) ---------------------------------------------
class CallCreate(BaseModel):
    transcript: str
    call_date: str
    participants: list | None = None
    duration_minutes: int | None = None
    zoom_meeting_id: str | None = None
    lead_id: str | None = None          # optioneel: direct koppelen (anders unmatched)


class CallMatch(BaseModel):
    lead_id: str


class CallOutcome(BaseModel):
    outcome: str                        # won | timing | no_value | stalled | hard_no
    outcome_note: str | None = None
    timing_target_date: str | None = None
    checkup_data: dict | None = None


class CallReportPatch(BaseModel):
    action: str                         # approve | discard
    report_html: str | None = None      # bewerkte versie bij approve (gate 2)


class CallSendRequest(BaseModel):
    dry_run: bool = False               # render+upload, geen Warmr-push (test zonder send)


class CallRetargetRequest(BaseModel):
    dry_run: bool = False               # genereer+QA, geen Warmr-push
    force: bool = False                 # negeer retarget_due_at (operator-override)


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
    """Start scraping job(s) voor een sector+stad combinatie.

    Drie modes (eerste die past wint):
      1. `custom_query` → 1 job met die exact query
      2. `subcategory_keys` → N jobs, één per subcategorie (eerste keyword als query)
      3. Geen van beide → 1 job met sector-label als query (legacy gedrag)

    Per job: dedupe op (sector, query, city, source) binnen 7 dagen.
    Returns: {jobs: [{job_id, query, source}, ...], total_jobs, planned_lead_volume}
    """
    from job_queue.scraping_queue import create_scraping_job
    from config.sectors import get_sector, get_subcategory_keywords

    # 1. Build de lijst (query, sector_key) tuples
    queries: list[tuple[str, str]] = []  # (query, sub_key_or_empty)

    if body.custom_query and body.custom_query.strip():
        queries.append((body.custom_query.strip(), ""))
    elif body.subcategory_keys:
        try:
            sector_cfg = get_sector(body.sector)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        for sub_key in body.subcategory_keys:
            if sub_key not in sector_cfg["subcategories"]:
                raise HTTPException(
                    status_code=400,
                    detail=f"Subcategorie '{sub_key}' bestaat niet voor sector '{body.sector}'",
                )
            keywords = get_subcategory_keywords(body.sector, sub_key)
            if not keywords:
                continue
            # Eerste keyword = beste signaal (meest specifiek). Combineer NIET — dat
            # geeft slechtere Google Maps hits dan één scherpe term.
            queries.append((keywords[0], sub_key))
    else:
        # Legacy: sector-label only
        try:
            sector_cfg = get_sector(body.sector)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        queries.append((sector_cfg["label"].lower(), ""))

    if not queries:
        raise HTTPException(status_code=422, detail="Geen geldige zoekopdracht — kies subcategorie of vul custom_query")

    # 2. Build sources list
    enabled_sources: list[str] = []
    if body.sources.get("google_maps", True):
        enabled_sources.append("google_maps")
    if body.sources.get("directories"):
        enabled_sources.append("directory")
    if not enabled_sources:
        enabled_sources = ["google_maps"]

    # 3. Fan-out: één job per (query × source)
    jobs_created: list[dict] = []
    for query, sub_key in queries:
        for source in enabled_sources:
            try:
                job_id = await create_scraping_job(
                    job_type=source,
                    sector_key=body.sector,
                    query=query,
                    location=body.city,
                    country="NL",
                    workspace_id=workspace_id,
                    supabase_client=db,
                )
                jobs_created.append({
                    "job_id": job_id,
                    "query": query,
                    "subcategory": sub_key or None,
                    "source": source,
                })
            except Exception as e:
                logger.warning("create_scraping_job failed for %r/%s: %s", query, source, e)

    return {
        "jobs": jobs_created,
        "total_jobs": len(jobs_created),
        "planned_lead_volume_estimate": len(jobs_created) * body.max_results,
    }


@app.get("/sectors/full")
async def list_sectors_full() -> dict:
    """Return rich sector taxonomy met subcategorieën + voorbeeld keywords.

    Frontend gebruikt dit om de zoek-UI te bouwen (sector dropdown + multi-select
    subcategorieën + live query-preview).
    """
    from config.sectors import SECTORS
    out = []
    for key, cfg in SECTORS.items():
        subs = []
        for sub_key, sub_cfg in (cfg.get("subcategories") or {}).items():
            keywords = sub_cfg.get("lead_keywords") or []
            subs.append({
                "key": sub_key,
                "label": sub_cfg.get("label") or sub_key,
                "primary_keyword": keywords[0] if keywords else "",
                "all_keywords": keywords[:6],   # cap voor UI tooltip
            })
        out.append({
            "key": key,
            "label": cfg.get("label") or key,
            "subcategories": subs,
        })
    return {"sectors": out}


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
    leads = res.data or []
    # Receptie-haak per lead bij-mergen (cohort-badge in de lijst): één batch-lookup
    # op de WI-rij (migratie 041). Voegt alleen receptie_hook_code toe; non-breaking.
    if leads:
        ids = [l["id"] for l in leads if l.get("id")]
        # In batches: .in_() zet alle id's in de querystring; boven ~500 UUIDs
        # wordt de URL te lang → PostgREST 400 'JSON could not be generated'.
        hooks: dict[str, str | None] = {}
        for i in range(0, len(ids), 200):
            wi = (db.table("website_intelligence").select("lead_id, receptie_hook_code")
                  .eq("workspace_id", workspace_id).in_("lead_id", ids[i:i + 200]).execute().data or [])
            hooks.update({w["lead_id"]: w.get("receptie_hook_code") for w in wi})
        for l in leads:
            l["receptie_hook_code"] = hooks.get(l.get("id"))
    return {"leads": leads, "total": res.count or 0}


# --- Literale /leads/<naam>-GET-routes MOETEN vóór /leads/{lead_id} staan ---
# Anders vangt de parametrische route ze af (lead_id="recontact-ready") en 500t
# get_lead op .maybe_single() met 0 rijen. Niet naar onderen verplaatsen.
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


@app.get("/leads/recontact-ready-signals")
async def recontact_ready_with_signals(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Trigger-based recontact list — only leads with fresh change signals."""
    from scoring.recontact_signals import get_recontact_ready
    params = dict(request.query_params)
    limit = int(params.get("limit", 25))
    leads = await get_recontact_ready(workspace_id, db, limit=limit)
    return {"leads": leads, "count": len(leads)}


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


# =============================================================================
# Inbound website-leads (Aerys Praktijk-Check / contact) — service-to-service
# =============================================================================

class InboundLead(BaseModel):
    source: str = "website"
    contact: dict = Field(default_factory=dict)   # name, email, phone, company
    consent: dict = Field(default_factory=dict)   # business, contactOptIn
    data: dict = Field(default_factory=dict)      # scan-antwoorden + euro-schatting


# Vrije mailproviders → geen bedrijfsdomein afleiden.
_FREEMAIL = {
    "gmail.com", "googlemail.com", "hotmail.com", "hotmail.nl", "outlook.com",
    "live.nl", "live.com", "yahoo.com", "yahoo.nl", "icloud.com", "me.com", "ziggo.nl", "kpnmail.nl",
}

# Leesbare labels voor de scan-antwoorden in de CRM-notitie.
_INBOUND_LABELS: dict[str, tuple[str, dict[str, str]]] = {
    "niche": ("Praktijk", {"cosmetisch": "Cosmetische kliniek", "chiro": "Chiropractie/houding", "anders": "Anders"}),
    "appts": ("Afspraken/week", {"lt40": "< 40", "40-80": "40-80", "80-150": "80-150", "gt150": "> 150"}),
    "intake_channel": ("Instroom", {"telefoon": "Vooral telefonisch", "online": "Online zelf boeken", "beide": "Half/half"}),
    "rate": ("Tarief", {"lt75": "< 75", "75-150": "75-150", "150-300": "150-300", "gt300": "> 300"}),
    "missed_calls": ("Gemiste oproepen", {"niets": "Beller weg", "voicemail": "Voicemail", "terugbellijst": "Terugbel-lijst"}),
    "reminders": ("Herinneringen", {"nee": "Geen", "handmatig": "Handmatig", "automatisch": "Automatisch"}),
    "followup": ("Nazorg", {"nee": "Geen", "soms": "Soms", "gestructureerd": "Vast proces"}),
}


def _inbound_summary(data: dict) -> str:
    """Leesbare samenvatting van de scan voor crm_notes + timeline."""
    lines: list[str] = []
    for key, (label, opts) in _INBOUND_LABELS.items():
        v = data.get(key)
        if v:
            lines.append(f"- {label}: {opts.get(str(v), v)}")
    lo, hi, hrs = data.get("totalLow"), data.get("totalHigh"), data.get("hours")
    if lo is not None and hi is not None:
        bedrag = f"EUR {int(lo):,}-{int(hi):,}/jaar".replace(",", ".")
        lines.append(f"- Geschat weglek: {bedrag} (~{hrs} u/week handwerk)")
    fr = data.get("frustration")
    if fr:
        lines.append(f'- In eigen woorden: "{fr}"')
    return "\n".join(lines) if lines else "(geen scan-details meegestuurd)"


async def _enrich_inbound_website(domain: str) -> str:
    """Snelle website-enrichment: favicon + tech-stack detectie.

    Retourneert leesbare samenvattingsregel voor crm_notes; failsafe (lege string bij error).
    """
    if not domain:
        return ""

    try:
        import httpx

        # Timeout kort (1s totaal) om de inbound-response niet te vertragen.
        async with httpx.AsyncClient(timeout=1.0) as client:
            # HEAD-request naar domein (tech-stack markers in headers/redirect)
            try:
                resp = await client.head(f"https://{domain}", follow_redirects=True)
                server_header = (resp.headers.get("Server") or "").lower()
                x_powered_by = (resp.headers.get("X-Powered-By") or "").lower()

                # Tech detectie
                tech_marks = []
                if "wix" in server_header or "wix" in x_powered_by:
                    tech_marks.append("Wix")
                if "wordpress" in server_header or "wp-" in x_powered_by:
                    tech_marks.append("WordPress")
                if any(m in server_header for m in ["shopify", "shopifycdn"]):
                    tech_marks.append("Shopify")
                if "cloudflare" in server_header:
                    tech_marks.append("Cloudflare")

                # Favicon (URL afleiding)
                favicon_url = f"https://{domain}/favicon.ico"

                # Bouw samenvattingsregel
                parts = [f"Website: https://{domain}"]
                if tech_marks:
                    parts.append(f"Tech: {', '.join(tech_marks)}")
                parts.append(f"Favicon: {favicon_url}")
                return " · ".join(parts)
            except (httpx.TimeoutException, httpx.ConnectError):
                # Timeout/geen connectie: fallback
                return f"Website: https://{domain} (niet bereikbaar)"
    except ImportError:
        pass  # httpx niet beschikbaar; fallback
    except Exception as e:
        logger.debug(f"Enrichment for {domain} failed: {e}")

    return f"Website: https://{domain}"


@app.post("/leads/inbound")
async def inbound_lead(
    body: InboundLead,
    workspace_id: str = Depends(require_service_key),
    db: Client = Depends(get_supabase),
) -> dict:
    """Warme inbound lead van de Aerys-website (Praktijk-Check/contact).

    Auth: X-API-Key (HEATR_API_KEY) -> DEFAULT_WORKSPACE. De lead landt als warme
    lead in de CRM (crm_stage 'gereageerd'), NIET in de koude Warmr-flow:
    email_status='valid' (self-provided) valt buiten de Warmr-eligibility.
    Bestaat de e-mail/het domein al, dan mergen we op de bestaande lead.

    Enrichment: quick website-scan (tech-stack, favicon) synchrone integratie;
    deep enrichment (KvK, company-data) async via job_queue.
    """
    from utils.deduplicator import is_duplicate_entity, normalize_domain

    contact = body.contact or {}
    email = (contact.get("email") or "").lower().strip()
    name = (contact.get("name") or "").strip()
    company = (contact.get("company") or "").strip() or name or "Onbekende praktijk"
    phone = (contact.get("phone") or "").strip() or None
    data = body.data or {}

    domain = ""
    if "@" in email:
        d = email.split("@", 1)[1].strip()
        if d and d not in _FREEMAIL:
            domain = normalize_domain(d)

    sector = {
        "cosmetisch": "cosmetische_behandelaars",
        "chiro": "alternatieve_geneeskunde",
    }.get(data.get("niche"), "overig")

    parts = name.split()
    first = parts[0] if parts else None
    last = " ".join(parts[1:]) if len(parts) > 1 else None
    opt_in = bool((body.consent or {}).get("contactOptIn"))

    summary = _inbound_summary(data)

    # Quick enrichment: tech-stack + favicon
    enrichment = await _enrich_inbound_website(domain) if domain else ""

    note_parts = [f"Inbound via website ({body.source}).", summary]
    if enrichment:
        note_parts.append(enrichment)
    note = "\n".join(note_parts)
    now = datetime.now(timezone.utc).isoformat()

    # Dedup: e-mail eerst (sterkste inbound-signaal), dan domein/naam.
    existing_id: str | None = None
    if email:
        r = db.table("leads").select("id").eq("workspace_id", workspace_id).eq("email", email).limit(1).execute()
        if r.data:
            existing_id = r.data[0]["id"]
    if not existing_id and (domain or company):
        is_dup, dup_id = await is_duplicate_entity(company, domain, "", workspace_id, db)
        if is_dup:
            existing_id = dup_id

    if existing_id:
        update = {"crm_stage": "gereageerd", "crm_notes": note, "updated_at": now}
        if phone:
            update["phone"] = phone
        db.table("leads").update(update).eq("id", existing_id).eq("workspace_id", workspace_id).execute()
        lead_id, action = existing_id, "merged"
    else:
        ins = db.table("leads").insert({
            "workspace_id": workspace_id,
            "company_name": company,
            "domain": domain or None,
            "sector": sector,
            "country": "NL",
            "contact_name": name or None,
            "contact_first_name": first,
            "contact_last_name": last,
            "email": email or None,
            "email_status": "valid",
            "email_type": "personal",
            "gdpr_safe": opt_in,
            "phone": phone,
            "score": 90,
            "status": "qualified",
            "crm_stage": "gereageerd",
            "crm_notes": note,
        }).execute()
        lead_id = ins.data[0]["id"] if ins.data else None
        action = "created"

    if lead_id:
        _insert_timeline_event(
            db, workspace_id, lead_id, "note_added",
            "Inbound Praktijk-Check ontvangen", summary,
        )

        # Queue deep enrichment (async: KvK, company-data, scoring)
        try:
            from job_queue.enrichment_queue import queue_lead_for_enrichment
            await queue_lead_for_enrichment(supabase_client=db, workspace_id=workspace_id, lead_id=lead_id)
        except Exception as e:
            logger.debug(f"Failed to queue deep enrichment for {lead_id}: {e}")

    return {"ok": True, "lead_id": lead_id, "action": action}


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
            await queue_lead_for_enrichment(supabase_client=db, workspace_id=workspace_id, lead_id=lead_id)
            queued += 1
        except Exception as e:
            logger.warning("Failed to queue lead %s: %s", lead_id, e)
    return {"queued": queued}


@app.post("/leads/send-to-warmr")
async def send_leads_to_warmr(
    body: SendToWarmrRequest,
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    from integrations.warmr_client import WarmrClient

    res = db.table("leads").select("*").in_("id", body.lead_ids).eq("workspace_id", workspace_id).execute()
    leads = res.data or []

    # Compliance (gdpr_safe + status niet in BLOCKED_STATUSES) via dezelfde
    # gate als campaign-launch en review-email — één bron van waarheid.
    #
    # E-mail-gate via de CANONIEKE is_sendable (outreach-reparatie 2026-07-18).
    # De oude literal ("verified","catch_all") matchte op waardes die niet in de
    # data bestaan (echte waardes: valid/catchall_risky/...) → 0 leads passeerden.
    # allow_risky=False + allow_role_emails=False = de eerste-campagne-keuze
    # "alleen valid" (Sami 2026-07-18): 518 leads. Verruimen naar catchall_risky
    # (+174, allemaal bouncer_api-geverifieerd) = deze twee args weghalen zodat
    # het canonieke risky-regime (methode-eis + HEATR_ALLOW_RISKY_EMAILS) geldt.
    # Opener-eis: mail 1 is "één observatie" — zonder opener geen mail (zelfde
    # keuze als HARD_REQUIRED_FIELDS bij /campaigns/launch).
    from utils.enrichment_check import compliance_check
    from utils.email_sendability import is_sendable
    eligible = [
        l for l in leads
        if compliance_check(l)[0]
        and is_sendable(
            l.get("email"), l.get("email_status"),
            allow_risky=False, allow_role_emails=False,
            verification_method=l.get("email_verification_method"),
        )[0]
        and (l.get("personalized_opener") or "").strip()
        and (l.get("score") or 0) >= int(os.getenv("MIN_SCORE_FOR_WARMR", 65))
    ]

    # 90-dagen-cooldown afdwingen (recovery-fix): filter leads die binnen 90
    # dagen een sequence afrondden/stopten — dezelfde gedeelde gate als
    # SendingGuard, zodat re-enrollment geen dubbele outreach oplevert.
    from utils.deduplicator import campaign_cooldown_block
    _kept, _cooled = [], 0
    for _l in eligible:
        if await campaign_cooldown_block(_l["id"], db):
            _cooled += 1
            continue
        _kept.append(_l)
    eligible = _kept

    if body.dry_run:
        return {"eligible": len(eligible), "cooldown_blocked": _cooled, "dry_run": True}
    if not eligible:
        return {"pushed": 0, "failed": 0, "duplicates": 0, "eligible": 0}

    client = WarmrClient()
    # Use first available inbox as campaign placeholder
    inboxes = await client.get_ready_inboxes()
    if not inboxes:
        raise HTTPException(status_code=503, detail="No Warmr inboxes available")

    campaign_id = inboxes[0].get("campaign_id") or inboxes[0].get("id")

    # Outbound via de dispatcher (I3/I6/I7): idempotency-key over de exacte
    # selectie — dezelfde leads nogmaals naar dezelfde campagne = duplicate.
    from utils.outbound_dispatcher import dispatch_outbound, ids_hash
    principal = identify_principal(request)
    disp = await dispatch_outbound(
        kind="warmr_bulk_push",
        idempotency_key=f"warmr-bulk:{campaign_id}:{ids_hash([l['id'] for l in eligible])}",
        actor=principal.get("created_by", "unknown"),
        leads=eligible,
        send=lambda: client.push_leads_bulk(eligible, campaign_id=campaign_id),
        supabase_client=db,
        workspace_id=workspace_id,
        metadata={"endpoint": "/leads/send-to-warmr"},
    )
    if disp.skipped_duplicate:
        return {
            "pushed": 0, "skipped_duplicate": True,
            "previous_push_at": (disp.previous or {}).get("created_at"),
            "detail": "Exact dezelfde selectie is al naar deze campagne gepusht.",
        }
    result = disp.result

    # ADR-001 (fase 3 PR 9): tracking-enrollment per gepushte lead — zelfde
    # bookkeeping als /campaigns/launch, zodat dedup/cooldown ook dit pad zien.
    from campaigns.enrollment import record_warmr_enrollments
    await record_warmr_enrollments(
        db, workspace_id=workspace_id, leads=eligible,
        campaign_id=campaign_id, service_type="adhoc_push", sequence_steps=[],
    )

    # Launch = voorbereiden (Warmr houdt de campagne in DRAFT tot activatie).
    # GEEN 'email_sent' hier — dat was misleidend (er ging niets uit). Het echte
    # send-moment schrijft 'email_sent' in de activate-endpoint.
    for lead in eligible:
        _insert_timeline_event(db, workspace_id, lead["id"], "campaign_prepared",
                               f"Voorbereid in Warmr-draft (campagne {campaign_id})")

    return result


@app.post("/leads/disqualify")
async def disqualify_lead(
    body: DisqualifyRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    # status="disqualified" is wat de centrale compliance-gate
    # (utils/enrichment_check.compliance_check) leest — zonder deze status
    # bleef een "gediskwalificeerde" lead gewoon launchbaar (Sprint 1-audit,
    # sectie 1: alleen crm_stage werd gezet).
    res = db.table("leads").update({
        "status": "disqualified",
        "crm_stage": "verloren",
        "disqualification_reason": body.reason,
    }).eq("id", body.lead_id).eq("workspace_id", workspace_id).execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="Lead not found")

    # Stop lopende sequences — anders krijgt de lead mail 2/3 nog via het
    # dispatch-pad (zelfde patroon als reply_classifier's disqualify-flow).
    try:
        db.table("lead_campaign_history").update({
            "status": "stopped",
            "is_active": False,
        }).eq("lead_id", body.lead_id).eq("workspace_id", workspace_id).eq("is_active", True).execute()
    except Exception as e:
        logger.warning("disqualify: kon sequences niet stoppen voor %s: %s", body.lead_id, e)

    _insert_timeline_event(db, workspace_id, body.lead_id, "deal_lost", f"Lead gediskwalificeerd: {body.reason}")
    return {"ok": True}


@app.get("/leads/{lead_id}/website")
async def get_lead_website(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    res = db.table("website_intelligence").select("*").eq("lead_id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute()
    wi = res.data or {}
    if wi.get("total_score") or wi.get("conversion_score"):
        # score_vs_market uit EIGEN data (geen Places-key): 2 assen — website (total_score,
        # → Aerys-rebuild) en automations (conversion_score, → conversie-opt + Curio-audit).
        from scoring.market_benchmark import enrich_wi_with_benchmarks
        enrich_wi_with_benchmarks(db, workspace_id, lead_id, wi)
    return wi


@app.get("/leads/{lead_id}/contacts")
async def get_lead_contacts(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Contactpersonen van één lead (LeadDetail Contacts-tab). Read-only.

    Mapt de DB-kolommen (full_name/title/…) naar het frontend Contact-contract
    (name/role/…). email blijft leeg: we hebben per contact alleen een pattern,
    geen geverifieerd adres — dat zou een onwaar 'email' suggereren.
    """
    res = (
        db.table("lead_contacts")
        .select("*")
        .eq("lead_id", lead_id)
        .eq("workspace_id", workspace_id)
        .order("is_primary", desc=True)
        .order("confidence", desc=True)
        .execute()
    )
    contacts = [
        {
            "id": c.get("id"),
            "name": (c.get("full_name")
                     or " ".join(filter(None, [c.get("first_name"), c.get("tussenvoegsel"), c.get("last_name")])).strip()
                     or None),
            "role": c.get("title") or None,
            "source": c.get("source") or None,
            "linkedin_url": c.get("linkedin_url") or None,
            "is_primary": c.get("is_primary"),
            "confidence": c.get("confidence"),
            "why_chosen": c.get("why_chosen") or None,
        }
        for c in (res.data or [])
    ]
    return {"contacts": contacts}


@app.get("/leads/{lead_id}/receptie")
async def get_lead_receptie(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Receptie-preview voor één lead: de gepersisteerde haak (migratie 041, Q4/Q7/Q2/P1 +
    axis-states) plus de gerenderde mail 1/2/3. Read-only — rendert via de pure
    build_receptie_preview, verstuurt niets (LeadDetail-Receptie-tab)."""
    from config.receptie_sequence import build_receptie_preview
    lead_res = (db.table("leads").select("*")
                .eq("id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute())
    if not lead_res.data:
        raise HTTPException(status_code=404, detail="lead niet gevonden")
    wi_res = (db.table("website_intelligence").select("*")
              .eq("lead_id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute())
    return build_receptie_preview(lead_res.data, wi_res.data)


@app.get("/leads/{lead_id}/audit")
async def get_lead_audit(
    lead_id: str,
    tier: int | None = None,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Laatste (hoogste versie) audit-rapport voor een lead uit heatr_audit_reports —
    read-only, geen re-run. Optioneel ?tier= voor de laatste tier-N. {} als er geen is.
    Bevat categories + score_total (die de POST weglaat) zodat de UI de per-categorie-
    breakdown kan tonen zonder opnieuw te draaien."""
    q = (db.table("audit_reports").select("*")
         .eq("lead_id", lead_id).eq("workspace_id", workspace_id))
    if tier in (1, 2):
        q = q.eq("tier", tier)
    res = q.order("version", desc=True).limit(1).execute()
    return (res.data[0] if res.data else {})


@app.post("/leads/{lead_id}/audit")
async def run_lead_audit(
    lead_id: str,
    tier: int = 1,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Prospect-facing audit voor één lead. Append-only naar heatr_audit_reports.

    tier=1 (default): gratis checks, geen Places — leest bestaande data + één
    lichte homepage-fetch. tier=2 (op verzoek, bij leadreactie): + Places als
    reviews-bron + bevroren stad/niche-benchmark. Geen nieuwe crawl in beide
    tiers. Raakt website_intelligence-scoring niet aan.

    Tier 2 skipt herberekening als dom_text_hash gelijk is aan de vorige
    audit-run (rescan-skip uit het ontwerp) — dan komt het bestaande rapport
    terug met skipped=true.
    """
    if tier not in (1, 2):
        raise HTTPException(status_code=422, detail="tier moet 1 of 2 zijn")
    from audit.scorer import score_lead, persist_audit_report
    lead = (db.table("leads").select("*").eq("id", lead_id)
            .eq("workspace_id", workspace_id).maybe_single().execute()).data
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    if not (lead.get("domain") or "").strip():
        raise HTTPException(status_code=409, detail="Lead heeft geen domein om te auditen.")

    places = None
    benchmark = None
    if tier == 2:
        # Rescan-skip: zelfde dom_text_hash als de vorige tier-2-run -> niets
        # herberekenen, bestaand rapport teruggeven.
        wi = (db.table("website_intelligence").select("dom_text_hash")
              .eq("lead_id", lead_id).eq("workspace_id", workspace_id)
              .maybe_single().execute()).data or {}
        current_hash = wi.get("dom_text_hash")
        prev = (db.table("audit_reports").select("*")
                .eq("lead_id", lead_id).eq("workspace_id", workspace_id).eq("tier", 2)
                .order("version", desc=True).limit(1).execute()).data or []
        if prev and current_hash and prev[0].get("content_hash") == current_hash:
            p = prev[0]
            return {"ok": True, "skipped": "unchanged_content_hash",
                    "version": p["version"], "score_normalized": p["score_normalized"],
                    "score_capped_by": p.get("score_capped_by"),
                    "benchmark": p.get("benchmark")}

        from audit.places import get_place_reviews
        places = await get_place_reviews(lead, db)
        if places.get("error") == "no_places_key":
            raise HTTPException(status_code=503,
                                detail="Tier 2 vereist GOOGLE_PLACES_API_KEY (nog niet gezet).")
        if places.get("error"):
            # Place niet gevonden is geen blocker: audit draait door, reviews-check
            # wordt not_measurable. Wel teruggeven wat er misging.
            logger.warning("audit tier2: places-fout voor %s: %s", lead_id, places["error"])
            places = None

    report = await score_lead(lead, db, tier=tier, places=places)

    if tier == 2:
        from audit.benchmark import compute_benchmark
        benchmark = await compute_benchmark(lead, report["score_normalized"], db)

    saved = await persist_audit_report(report, db, benchmark=benchmark)
    return {"ok": True, "tier": tier, "version": (saved or {}).get("version"),
            "score_normalized": report["score_normalized"],
            "score_capped_by": report["score_capped_by"],
            "is_empty_site": report["is_empty_site"],
            "scored_layers": report["scored_layers"],
            "benchmark": benchmark,
            "findings": report["findings"]}


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

    # GDPR + status gate vóór Warmr-push. Preview-mode (hierboven) bereikt
    # deze code niet — review-preview blijft werken zonder push-permissie.
    from utils.enrichment_check import compliance_check
    compliant, compliance_reason = compliance_check(lead)
    if not compliant:
        raise HTTPException(status_code=403, detail=f"Lead niet sendable: {compliance_reason}")

    from integrations.warmr_client import WarmrClient
    client = WarmrClient()
    inboxes = await client.get_ready_inboxes()
    if not inboxes:
        raise HTTPException(status_code=503, detail="No Warmr inbox available")

    # Via de dispatcher (I3/I6/I7). Key per lead: één review-email per lead —
    # een tweede klik wordt een nette 409 i.p.v. een dubbele Warmr-push
    # (Sprint 1-audit §7: dit endpoint had geen enkele idempotency).
    from utils.outbound_dispatcher import dispatch_outbound
    disp = await dispatch_outbound(
        kind="warmr_push",
        idempotency_key=f"review-email:{lead_id}",
        actor="operator",
        lead=lead,
        send=lambda: client.push_lead(lead, campaign_id=inboxes[0]["id"], preferred_inbox_id=inboxes[0]["id"]),
        supabase_client=db,
        workspace_id=workspace_id,
        metadata={"endpoint": "/leads/{id}/send-review-email"},
    )
    if disp.skipped_duplicate:
        raise HTTPException(
            status_code=409,
            detail=f"Review-email is al verstuurd voor deze lead "
                   f"({(disp.previous or {}).get('created_at', 'eerder')}). "
                   f"Herzenden vereist een bewuste restart via de Control Plane.",
        )
    _insert_timeline_event(db, workspace_id, lead_id, "review_email_sent", "Review email verstuurd via Warmr")

    return {"ok": True, **email_data}


@app.get("/leads/{lead_id}/launch-readiness")
async def lead_launch_readiness(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Per-lead launch readiness: ready / blocked / needs_review met redenen.

    Composeert alle bestaande gates (compliance, email-sendability,
    enrichment-completeness, score-drempels, cooldown) tot één uitlegbaar
    verdict — zie utils/launch_readiness.py. Geen black-box launch meer:
    de operator ziet vóór een send precies welke check blokkeert.
    """
    lead_res = db.table("leads").select("*").eq("id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute()
    if not lead_res.data:
        raise HTTPException(status_code=404, detail="Lead not found")

    from utils.launch_readiness import assess_launch_readiness
    return assess_launch_readiness(lead_res.data)


@app.get("/leads/{lead_id}/run-state")
async def lead_run_state(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Control Plane Inspect-laag: volledige run-state van één lead.

    Read-only compositie van pipeline-positie, readiness, jobs,
    campagne-historie, side-effects (outbound-ledger), blocks, cost en
    timeline — zie utils/run_state.py. `gaps` markeert eerlijk wat zonder
    event-log niet toonbaar is (I7-fundament).
    """
    lead_res = db.table("leads").select("*").eq("id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute()
    if not lead_res.data:
        raise HTTPException(status_code=404, detail="Lead not found")

    from utils.run_state import build_lead_run_state
    return build_lead_run_state(lead_res.data, workspace_id, db)


@app.get("/control/outbound")
async def control_outbound_ledger(
    limit: int = 50,
    status: str | None = None,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Control Plane: het outbound-ledger (heatr_outbound_log).

    Elke side-effect-poging — in_flight, completed, failed_retryable,
    failed_terminal, blocked_compliance, skipped_duplicate — via de
    dispatcher (reserveringsmodel sinds migratie 022 / WP-A).
    """
    try:
        q = (
            db.table("outbound_log")
            .select("id, idempotency_key, kind, status, actor, lead_id, error, created_at, metadata")
            .eq("workspace_id", workspace_id)
            .order("created_at", desc=True)
            .limit(min(limit, 200))
        )
        if status:
            q = q.eq("status", status)
        res = q.execute()
        return {"records": res.data or [], "count": len(res.data or [])}
    except Exception as e:
        return {
            "records": [], "count": 0,
            "warning": (
                f"outbound_log niet leesbaar ({type(e).__name__}) — ledger "
                "onbeschikbaar; prospect-sends zijn fail-closed geblokkeerd. "
                "Controleer migratie 020/022 en de outbound_log-registratie "
                "in config/database.py (zie runbook outbound_safety)."
            ),
        }


# =============================================================================
# CONTROL PLANE — operator-acties (Sprint 2, laag 2)
#
# Twee harde regels (I4 + I6):
#   1. Idempotent, óf expliciet unsafe met confirm=true in de body.
#   2. Elke actie schrijft zichzelf weg als timeline-event met principal.
# Unsafe-acties zijn pas veilig doordat de dispatcher-idempotency-key
# (seq-send:{record}:{step}:{epoch}) dubbele sends tegenhoudt; restart bumpt
# bewust de epoch en is daarmee de ENIGE route naar een herzending.
# =============================================================================

def _get_campaign_record(record_id: str, workspace_id: str, db: Client) -> dict:
    res = (
        db.table("lead_campaign_history").select("*")
        .eq("id", record_id).eq("workspace_id", workspace_id)
        .maybe_single().execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Campaign-record niet gevonden")
    return res.data


@app.post("/control/leads/{lead_id}/retry-enrichment")
async def control_retry_enrichment(
    lead_id: str,
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Retry enrichment voor één lead. IDEMPOTENT: bestaande pending/running
    jobs worden niet gedupliceerd (zelfde dedup als re-enqueue-stale-leads);
    per-lead cost-cap blijft de kosten-vangrail."""
    existing = (
        db.table("enrichment_jobs").select("id")
        .eq("lead_id", lead_id).eq("workspace_id", workspace_id)
        .in_("status", ["pending", "running"]).limit(1).execute()
    )
    if existing.data:
        return {"queued": False, "reason": "al een pending/running enrichment-job", "job_id": existing.data[0]["id"]}

    from job_queue.enrichment_queue import queue_lead_for_enrichment
    job_id = await queue_lead_for_enrichment(
        lead_id=lead_id, workspace_id=workspace_id, supabase_client=db, priority=2,
    )
    principal = identify_principal(request)
    _insert_timeline_event(
        db, workspace_id, lead_id, "control_retry_enrichment",
        "Enrichment-retry via Control Plane",
        metadata={"by": principal.get("created_by"), "job_id": job_id},
    )
    return {"queued": bool(job_id), "job_id": job_id}


@app.post("/control/campaign-records/{record_id}/pause")
async def control_pause_record(
    record_id: str,
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Pauzeer één sequence-record. Idempotent (paused blijft paused)."""
    record = _get_campaign_record(record_id, workspace_id, db)
    db.table("lead_campaign_history").update({"status": "paused"}).eq("id", record_id).eq("workspace_id", workspace_id).execute()
    principal = identify_principal(request)
    _insert_timeline_event(
        db, workspace_id, record["lead_id"], "control_sequence_paused",
        f"Sequence gepauzeerd (stap {record.get('step_index')})",
        metadata={"by": principal.get("created_by"), "record_id": record_id},
    )
    return {"record_id": record_id, "status": "paused"}


@app.post("/control/campaign-records/{record_id}/resume")
async def control_resume_record(
    record_id: str,
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Hervat een gepauzeerd sequence-record. Idempotent. next_send_at blijft
    staan (of nu, als hij in het verleden lag) — geen stap wordt overgeslagen."""
    record = _get_campaign_record(record_id, workspace_id, db)
    db.table("lead_campaign_history").update({"status": "pending"}).eq("id", record_id).eq("workspace_id", workspace_id).execute()
    principal = identify_principal(request)
    _insert_timeline_event(
        db, workspace_id, record["lead_id"], "control_sequence_resumed",
        f"Sequence hervat (stap {record.get('step_index')})",
        metadata={"by": principal.get("created_by"), "record_id": record_id},
    )
    return {"record_id": record_id, "status": "pending"}


@app.post("/control/campaign-records/{record_id}/force-next")
async def control_force_next(
    record_id: str,
    body: dict,
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """UNSAFE: forceer de eerstvolgende stap NU (overschrijft de wachttijd).

    Vereist {"confirm": true}. Veiligheidsnet: de dispatcher-key
    seq-send:{record}:{step}:{epoch} voorkomt dat een al-verzonden stap
    nogmaals gaat — forceren kan de timing breken, nooit een dubbele send
    veroorzaken."""
    if not body.get("confirm"):
        raise HTTPException(status_code=400, detail=(
            "force-next is unsafe (overschrijft wachttijd). Stuur {\"confirm\": true} om te bevestigen."
        ))
    record = _get_campaign_record(record_id, workspace_id, db)
    if not record.get("is_active"):
        raise HTTPException(status_code=409, detail="Record is niet actief — niets te forceren.")
    now_iso = datetime.now(timezone.utc).isoformat()
    db.table("lead_campaign_history").update({
        "status": "pending", "next_send_at": now_iso,
    }).eq("id", record_id).eq("workspace_id", workspace_id).execute()
    principal = identify_principal(request)
    _insert_timeline_event(
        db, workspace_id, record["lead_id"], "control_force_next",
        f"Stap {record.get('step_index')} geforceerd (wachttijd overschreven)",
        metadata={"by": principal.get("created_by"), "record_id": record_id, "confirmed": True},
    )
    return {"record_id": record_id, "next_send_at": now_iso, "step_index": record.get("step_index")}


@app.post("/control/campaign-records/{record_id}/restart")
async def control_restart_record(
    record_id: str,
    body: dict,
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """UNSAFE: herstart de sequence vanaf stap X — kan een lead een TWEEDE
    mail voor dezelfde stap opleveren.

    Vereist {"confirm": true, "step_index": N}. Werkt ALLEEN doordat de
    restart bewust restart_epoch bumpt: de dispatcher-key krijgt een nieuwe
    epoch en staat de herzending toe, terwijl accidentele duplicaten
    (zelfde epoch) geblokkeerd blijven. Compliance wordt op dispatch-tijd
    opnieuw afgedwongen (SendingGuard → compliance_check) — een inmiddels
    unsubscribed/disqualified lead komt er ook via restart niet door."""
    if not body.get("confirm"):
        raise HTTPException(status_code=400, detail=(
            "restart is unsafe (kan een stap opnieuw versturen). "
            "Stuur {\"confirm\": true, \"step_index\": N} om te bevestigen."
        ))
    step_index = body.get("step_index")
    if not isinstance(step_index, int) or step_index < 0:
        raise HTTPException(status_code=400, detail="step_index (int >= 0) is verplicht")

    record = _get_campaign_record(record_id, workspace_id, db)
    step_count = len(record.get("sequence_steps") or [])
    if step_index >= step_count:
        raise HTTPException(status_code=400, detail=f"step_index {step_index} buiten bereik (sequence heeft {step_count} stappen)")

    new_epoch = int(record.get("restart_epoch") or 0) + 1
    now_iso = datetime.now(timezone.utc).isoformat()
    db.table("lead_campaign_history").update({
        "status": "pending", "is_active": True,
        "step_index": step_index, "next_send_at": now_iso,
        "restart_epoch": new_epoch,
    }).eq("id", record_id).eq("workspace_id", workspace_id).execute()
    principal = identify_principal(request)
    _insert_timeline_event(
        db, workspace_id, record["lead_id"], "control_sequence_restart",
        f"Sequence herstart vanaf stap {step_index} (epoch {new_epoch})",
        metadata={"by": principal.get("created_by"), "record_id": record_id,
                  "step_index": step_index, "restart_epoch": new_epoch, "confirmed": True},
    )
    return {"record_id": record_id, "step_index": step_index, "restart_epoch": new_epoch, "next_send_at": now_iso}


@app.get("/leads/{lead_id}/thread")
async def lead_email_thread(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Chronologisch email-thread voor één lead.

    Mergt verstuurde mails (uit lead_timeline + lead_campaign_history) met
    ontvangen replies (uit reply_inbox). Returns array sorted oudste-eerst.
    Faalt-tolerant: missende tabellen geven lege thread terug.
    """
    from utils.lead_thread import build_lead_thread
    return await build_lead_thread(lead_id, workspace_id, db)


@app.post("/leads/{lead_id}/test-mode")
async def toggle_test_mode(
    lead_id: str,
    body: dict,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Toggle is_test_lead flag op een lead.

    Body: {"is_test_lead": true|false}
    Test-mode triggers: is_sendable() bypass + BCC naar HEATR_TEST_BCC_EMAIL +
    [TEST] prefix in subject. Voor smoke-test pipeline zonder risico.
    """
    new_value = bool(body.get("is_test_lead"))
    try:
        res = (
            db.table("leads")
            .update({"is_test_lead": new_value})
            .eq("id", lead_id)
            .eq("workspace_id", workspace_id)
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"toggle failed: {e}")
    if not res.data:
        raise HTTPException(status_code=404, detail="Lead not found")

    # I4: test-mode-toggle logde voorheen niets (bewuste keuze in de top-4
    # sprint, maar tegen de operator-event-invariant).
    try:
        _insert_timeline_event(
            db, workspace_id, lead_id, "test_mode_toggled",
            f"Test-mode {'AAN' if new_value else 'UIT'} gezet",
        )
    except Exception as e:
        logger.warning("test-mode: timeline-event mislukt voor %s: %s", lead_id, e)

    return {"lead_id": lead_id, "is_test_lead": new_value}


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

    # Zonder expliciete FK in Supabase kunnen we geen inner-join doen via PostgREST.
    # Twee-query pattern: eerst WI rows, dan leads batched per lead_id.
    q = (
        db.table("website_intelligence")
        .select("*", count="exact")
        .eq("workspace_id", workspace_id)
    )

    if priority := params.get("priority"):
        priorities = priority.split(",")
        q = q.in_("priority", priorities)
    if opp_type := params.get("opportunity_type"):
        q = q.contains("opportunity_types", [opp_type])

    q = q.order("total_score", desc=False).range(offset, offset + limit - 1)
    res = q.execute()
    wi_rows = res.data or []

    lead_ids = [r.get("lead_id") for r in wi_rows if r.get("lead_id")]
    leads_by_id: dict[str, dict] = {}
    if lead_ids:
        try:
            leads_res = (
                db.table("leads")
                .select("id, company_name, city, sector, score, email_status, crm_stage, status")
                .eq("workspace_id", workspace_id)
                .in_("id", lead_ids)
                .execute()
            )
            for l in leads_res.data or []:
                leads_by_id[l["id"]] = l
        except Exception:
            leads_by_id = {}

    sector_filter = params.get("sector")
    opportunities = []
    for row in wi_rows:
        lead = leads_by_id.get(row.get("lead_id") or "", {})
        if sector_filter and lead.get("sector") != sector_filter:
            continue
        if lead.get("status") == "archived":
            continue
        opportunities.append({
            **row,
            "lead_id": lead.get("id") or row.get("lead_id"),
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

@app.get("/sequences/templates")
async def list_sequence_templates() -> dict:
    """Return alle voorgedefinieerde sequence-templates (campaign-launcher dropdown)."""
    from config.sequence_templates import SEQUENCE_TEMPLATES
    return {
        "templates": [
            {
                "id": t["id"],
                "name": t["name"],
                "version": t["version"],
                "segment": t.get("segment"),
                "sector": t.get("sector"),
                "language": t["language"],
                "cadence_days": t["cadence_days"],
                "primary_cta": t["primary_cta"],
                "constraints": t["constraints"],
                "observation_blocks": t.get("observation_blocks", []),
                "min_personalization_score": t.get("min_personalization_score"),
                "step_count": len(t["default_steps"]),
                "preview_steps": [
                    {"subject": s["subject"], "delay_days": s["delay_days"], "body_preview": s["body"][:160] + "…" if len(s["body"]) > 160 else s["body"]}
                    for s in t["default_steps"]
                ],
            }
            for t in SEQUENCE_TEMPLATES.values()
        ]
    }


@app.get("/sequences/templates/{template_id}")
async def get_sequence_template(template_id: str) -> dict:
    """Return één template volledig (incl. body-tekst), klaar voor campaign-launcher voorvulling."""
    from config.sequence_templates import SEQUENCE_TEMPLATES
    t = SEQUENCE_TEMPLATES.get(template_id)
    if not t:
        raise HTTPException(status_code=404, detail=f"Template '{template_id}' niet gevonden")
    return t


def _personalization_score_0_100(lead: dict) -> int:
    """Schaal personalization_potential (0-15) naar 0-100.

    Gebruikt voor de v1.0 sequence-gate: ≥70 = auto, 50-69 = review, <50 = skip.
    """
    raw = lead.get("personalization_potential") or 0
    try:
        return int(round((float(raw) / 15.0) * 100))
    except (TypeError, ValueError):
        return 0


def _gate_leads_for_template(
    leads: list[dict], template: dict | None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Split leads in (auto, review, skip) op basis van template min_personalization_score.

    Drempels per v1.0 spec:
      score >= threshold       → auto (mag mee in launch)
      score >= threshold-20    → review (handmatige goedkeuring vereist)
      anders                    → skip
    """
    if not template:
        return leads, [], []
    threshold = template.get("min_personalization_score")
    if not threshold:
        return leads, [], []
    auto, review, skip = [], [], []
    review_floor = max(threshold - 20, 0)
    for lead in leads:
        # is_test_lead bypass: test-leads slaan de personalization-score-gate
        # over. faf8abd claimde dat deze bypass werkte maar raakte alleen de
        # completeness-check; de score-drempel hield test-leads alsnog tegen.
        # Test-leads krijgen wél nog hun _pers_score_0_100 berekend zodat
        # downstream-rendering (debug-output, logs) ze niet als "0" toont.
        if lead.get("is_test_lead"):
            lead["_pers_score_0_100"] = _personalization_score_0_100(lead)
            auto.append(lead)
            continue
        score = _personalization_score_0_100(lead)
        lead["_pers_score_0_100"] = score
        if score >= threshold:
            auto.append(lead)
        elif score >= review_floor:
            review.append(lead)
        else:
            skip.append(lead)
    return auto, review, skip


def _resolve_template_for_lead(
    lead: dict,
    body_template_id: str | None,
    body_sequence: list[dict],
) -> tuple[str | None, dict | None, list[dict]]:
    """Resolve sequence-template + steps voor één lead in v3.1-launch flow.

    Priority:
      1. body.sequence non-empty   → custom override (geen template-binding)
      2. body.template_id = v3_1_* → forced single brug voor hele cohort
      3. body.template_id = v1_*, leeg, of unknown → AUTO mode: per-lead
         pick_brug() → v3_1_<brug>. v1-IDs worden hier server-side ge-rerouted
         naar v3.1 — geen frontend-default-flip nodig om v3.1 te activeren.
    """
    from config.sequence_templates import (
        SEQUENCE_TEMPLATES, pick_brug, faseA_brug_for, _FASE_A_DELAYS,
        resolve_faseA_step, FOUNDING_FIVE_TOTAL,
    )

    if body_sequence:
        return None, None, body_sequence

    if body_template_id and body_template_id.startswith("v3_1_"):
        t = SEQUENCE_TEMPLATES.get(body_template_id)
        if t:
            return body_template_id, t, t["default_steps"]

    # Receptie-brug (Q4/Q7/Q2/P1-ladder): expliciete forced-brug voor de hele
    # cohort, net als de v3_1_-forced-mode. render_faseA_marker leest per lead de
    # receptie_hook_code en past de compliance-gates toe; leads zonder hook of
    # zonder tokens/rechtspersoon worden bij het renderen geblokkeerd/overgeslagen.
    if body_template_id == "faseA_receptie":
        from config.sequence_templates import receptie_faseA_steps
        return "faseA_receptie", None, receptie_faseA_steps()

    # AUTO mode (default sinds 2026-07-21): Fase A-steps. Elke step draagt een
    # subject+body-SHELL (met {{tokens}}, net als v3.1) zodat Warmr's campaign-
    # create ze accepteert — ÉN faseA_brug/faseA_step, zodat process_due_send de
    # body LIVE herresolvet bij verzending (render_faseA_marker → push_lead
    # custom_subject/body): plekken-teller/opener/voornaam op het verzendmoment.
    # De shell gebruikt de start-plekken (FOUNDING_FIVE_TOTAL); de echte waarde
    # komt live per send. brug = conceptsite (workflow geschrapt).
    brug = faseA_brug_for(pick_brug(lead))
    steps = []
    for i, d in enumerate(_FASE_A_DELAYS):
        shell = resolve_faseA_step(brug, i, lead, free_slots=FOUNDING_FIVE_TOTAL)
        steps.append({
            "faseA_brug": brug, "faseA_step": i,
            "subject": shell["subject"], "body": shell["body"],
            "delay_days": d, "thread": shell["thread"],
        })
    return f"faseA_{brug}", None, steps
    return None, None, []


@app.post("/campaigns/preview")
async def preview_campaign(
    body: CampaignLaunchRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Dry-run: render sequence per lead, return preview. Verstuurt NIETS.

    v3.1 actief sinds 2026-05-07: per-lead pick_brug() bepaalt welke v3.1
    brug-template (website/workflow/ai_audit) wordt gerenderd. v1.0-template_ids
    worden server-side ge-rerouted naar v3.1 auto-mode.
    """
    from campaigns.sequence_engine import validate_sequence_config, render_step
    from config.sequence_templates import SEQUENCE_TEMPLATES

    # Snelle 404 als caller een onbekende v3.1-key vraagt — guards explicit-mode.
    if body.template_id and body.template_id.startswith("v3_1_"):
        if body.template_id not in SEQUENCE_TEMPLATES:
            raise HTTPException(status_code=404, detail=f"Template '{body.template_id}' niet gevonden")

    res = db.table("leads").select("*").in_("id", body.lead_ids).eq("workspace_id", workspace_id).execute()
    leads = res.data or []

    # Pre-launch enrichment-completeness check (preview-versie: blokkeert niet,
    # toont alleen welke leads bij echte launch worden geweigerd)
    from utils.enrichment_check import filter_launchable_leads
    launchable_leads, blocked_leads, completeness_warnings = filter_launchable_leads(leads)
    leads = launchable_leads

    # Per-lead template-resolutie. Voor de top-level gate gebruiken we het meest
    # voorkomende v3.1-template uit de cohort als representatief threshold-bron.
    per_lead_template: dict[str, dict] = {}  # lead_id → template-dict (v3.1-gate)
    per_lead_steps: dict[str, tuple] = {}    # lead_id → (template_id, steps)  incl. Fase A-markers
    template_id_counts: dict[str, int] = {}
    for lead in leads:
        t_id, t_obj, steps = _resolve_template_for_lead(lead, body.template_id, body.sequence)
        per_lead_steps[lead["id"]] = (t_id, steps)
        if t_id and t_obj:
            per_lead_template[lead["id"]] = {"template_id": t_id, "template": t_obj}
            template_id_counts[t_id] = template_id_counts.get(t_id, 0) + 1

    # Dominant template-id voor de top-level response — gebruikt voor gate-threshold
    dominant_template_id = max(template_id_counts, key=template_id_counts.get) if template_id_counts else None
    dominant_template = SEQUENCE_TEMPLATES.get(dominant_template_id) if dominant_template_id else None

    # Sequence-validatie op de dominante template (custom sequence krijgt zijn eigen)
    if body.sequence:
        is_valid, errors = validate_sequence_config(body.sequence)
    elif dominant_template:
        is_valid, errors = validate_sequence_config(dominant_template["default_steps"])
    else:
        is_valid, errors = True, []

    # Personalization gate werkt op dominante template's threshold
    auto_leads, review_leads, skip_leads = _gate_leads_for_template(leads, dominant_template)

    # Render preview per lead met hun eigen brug-template
    from campaigns.sequence_engine import render_faseA_marker
    sample = (auto_leads or leads)[:5]
    previews = []
    for lead in sample:
        lead_id = lead.get("id")
        template_id_for_lead, steps = per_lead_steps.get(lead_id, (None, body.sequence or []))
        # Fase A-markers live resolven (zelfde pad als send → dry-render == verzending);
        # v3.1/custom-steps statisch renderen.
        rendered = []
        for s in steps:
            if s.get("faseA_brug"):
                rendered.append(await render_faseA_marker(s, lead, db, workspace_id))
            else:
                rendered.append(render_step(s, lead))
        _brug = None
        if template_id_for_lead:
            _brug = template_id_for_lead.replace("v3_1_", "").replace("faseA_", "")
        previews.append({
            "lead_id": lead_id,
            "company_name": lead.get("company_name"),
            "first_name": lead.get("contact_first_name"),
            "domain": lead.get("domain"),
            "template_id": template_id_for_lead,
            "brug": _brug,
            "personalization_score": lead.get("_pers_score_0_100", 0),
            "steps": rendered,
        })

    template_id = dominant_template_id  # voor onderstaande response-blok

    def _summary(items: list[dict]) -> list[dict]:
        return [
            {
                "lead_id": l.get("id"),
                "company_name": l.get("company_name"),
                "personalization_score": l.get("_pers_score_0_100", 0),
            }
            for l in items[:50]
        ]

    return {
        "template_id": template_id,
        "valid": is_valid,
        "errors": errors,
        "lead_count": len(body.lead_ids),
        "preview_count": len(previews),
        "previews": previews,
        "personalization_gate": {
            "threshold": (dominant_template or {}).get("min_personalization_score"),
            "auto_count": len(auto_leads),
            "review_count": len(review_leads),
            "skip_count": len(skip_leads),
            "auto_sample": _summary(auto_leads),
            "review_sample": _summary(review_leads),
            "skip_sample": _summary(skip_leads),
        },
        "completeness_check": {
            "launchable_count": len(launchable_leads),
            "blocked_count": len(blocked_leads),
            "warning_count": len(completeness_warnings),
            "blocked_sample": [
                {
                    "lead_id": b.get("id"),
                    "company_name": b.get("company_name"),
                    "missing_required": b.get("_completeness", {}).get("missing_required", []),
                }
                for b in blocked_leads[:10]
            ],
            "warning_sample": completeness_warnings[:10],
        },
    }


@app.post("/campaigns/launch")
async def launch_campaign(
    body: CampaignLaunchRequest,
    request: Request,
    workspace_id: str = Depends(require_service_key),  # service-only — browser kan NOOIT versturen
    db: Client = Depends(get_supabase),
) -> dict:
    # DRAFT-PREP, GEEN SEND. Launch maakt in Warmr ALTIJD een DRAFT-campagne aan en
    # pusht de leads erin — Warmr verstuurt draft-campagnes niet. Er is hier dus geen
    # kill-switch: de enige send-trigger is POST /campaigns/{id}/activate, en dáár zit
    # de kill-switch (ENABLE_PROSPECT_SENDS) + allowlist + per-ontvanger her-verificatie.
    # Zo blijft launch veilig-vrij (previewbaar/voorbereidbaar) en is activeren de ene muur.

    from integrations.warmr_client import WarmrClient
    from campaigns.sequence_engine import validate_sequence_config, auto_fix_sequence_config
    from config.sequence_templates import SEQUENCE_TEMPLATES

    # Vroege 404 voor onbekende v3.1-keys — anders silent fallback naar AUTO mode.
    if body.template_id and body.template_id.startswith("v3_1_"):
        if body.template_id not in SEQUENCE_TEMPLATES:
            raise HTTPException(status_code=404, detail=f"Template '{body.template_id}' niet gevonden")

    res = db.table("leads").select("*").in_("id", body.lead_ids).eq("workspace_id", workspace_id).execute()
    leads = res.data or []

    # Pre-launch enrichment-completeness check — voorkomt halve-data sends.
    # Test-leads (is_test_lead=true) bypassen deze check voor smoke-test flow.
    from utils.enrichment_check import filter_launchable_leads
    launchable_leads, blocked_leads, _completeness_warnings = filter_launchable_leads(leads)
    if blocked_leads and not launchable_leads:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Geen lead heeft volledige enrichment voor sequence-launch",
                "blocked_count": len(blocked_leads),
                "blocked_sample": [
                    {
                        "lead_id": l.get("id"),
                        "company_name": l.get("company_name"),
                        "missing_required": l.get("_completeness", {}).get("missing_required", []),
                    }
                    for l in blocked_leads[:5]
                ],
                "hint": "Re-enqueue voor enrichment via /admin/re-enqueue-stale-leads, óf markeer als test-lead.",
            },
        )
    if blocked_leads:
        logger.warning(
            "campaigns/launch: %d/%d leads geweigerd door completeness check",
            len(blocked_leads), len(leads),
        )
    leads = launchable_leads

    # E-mail-sendability-gate (outreach-reparatie 2026-07-18). Dit pad had GÉÉN
    # email-gate (de omgekeerde fout van /leads/send-to-warmr, dat op niet-
    # bestaande waardes matchte en niemand doorliet) — een hard-bounced status
    # werd al via BLOCKED_STATUSES gevangen, maar invalid/not_found/not_checked
    # niet. Zelfde canonieke is_sendable + zelfde eerste-campagne-keuze "alleen
    # valid" als /leads/send-to-warmr; verruimen = de twee args weghalen.
    # Test-leads behouden hun bypass via is_test_lead (suppressie wint altijd).
    from utils.email_sendability import is_sendable as _is_sendable
    _email_blocked: list[dict] = []
    _email_ok: list[dict] = []
    for _l in leads:
        _ok, _reason = _is_sendable(
            _l.get("email"), _l.get("email_status"),
            allow_risky=False, allow_role_emails=False,
            is_test_lead=bool(_l.get("is_test_lead")),
            verification_method=_l.get("email_verification_method"),
        )
        (_email_ok if _ok else _email_blocked).append(_l)
    if _email_blocked:
        logger.warning(
            "campaigns/launch: %d/%d leads geweigerd door email-sendability",
            len(_email_blocked), len(leads),
        )
    leads = _email_ok
    if not leads:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Geen enkele lead heeft een verzendbaar e-mailadres (gate: alleen 'valid')",
                "email_blocked": len(_email_blocked),
                "hint": "Controleer email_status; verruimen kan door de risky-regime-args in deze gate te versoepelen.",
            },
        )

    # 90-dagen-cooldown afdwingen vóór (re-)enrollment (recovery-fix). Dezelfde
    # gedeelde gate als SendingGuard: een lead die binnen 90 dagen een sequence
    # afrondde/stopte wordt geweigerd → geen dubbele koud-outreach.
    from utils.deduplicator import campaign_cooldown_block
    _cooldown_blocked: list[dict] = []
    _launchable: list[dict] = []
    for _l in leads:
        if await campaign_cooldown_block(_l["id"], db):
            _cooldown_blocked.append(_l)
        else:
            _launchable.append(_l)
    if _cooldown_blocked:
        logger.warning(
            "campaigns/launch: %d/%d leads geweigerd door 90-dagen-cooldown",
            len(_cooldown_blocked), len(leads),
        )
    leads = _launchable
    if not leads:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Alle leads vallen binnen de 90-dagen-cooldown na een eerdere campagne",
                "cooldown_blocked": len(_cooldown_blocked),
                "hint": "Wacht tot de cooldown afloopt of kies andere leads.",
            },
        )

    # Actieve-campagne-block (fase 3 PR 9, audit v2 F1/F2): een lead die al
    # in een lopende enrollment zit (is_active=true) mag niet nogmaals
    # gelanceerd worden — óók niet onder een andere campagnenaam. Batch-query
    # i.p.v. per-lead; werkt op de tracking-rijen uit record_warmr_enrollments.
    try:
        _active_res = (
            db.table("lead_campaign_history").select("lead_id")
            .eq("workspace_id", workspace_id)
            .in_("lead_id", [l["id"] for l in leads])
            .eq("is_active", True)
            .execute()
        )
        _active_ids = {r["lead_id"] for r in (_active_res.data or [])}
    except Exception as e:
        # Fail-closed (Besluit 3): dedup-data onbereikbaar = niet launchen.
        logger.error("campaigns/launch: active-campagne-check faalde — fail-closed: %s", e)
        raise HTTPException(status_code=503, detail=(
            "Actieve-campagne-check onbeschikbaar (lead_campaign_history niet "
            "leesbaar) — launch geblokkeerd. Is migratie 025 gedraaid?"
        ))
    if _active_ids:
        _in_campaign = [l for l in leads if l["id"] in _active_ids]
        leads = [l for l in leads if l["id"] not in _active_ids]
        logger.warning(
            "campaigns/launch: %d lead(s) geweigerd — al in een actieve campagne: %s",
            len(_in_campaign), sorted(_active_ids)[:10],
        )
    if not leads:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Alle leads zitten al in een actieve campagne",
                "in_active_campaign": len(_active_ids),
                "hint": "Wacht tot de lopende sequence is afgerond (webhook sluit de enrollment) of stop die expliciet.",
            },
        )

    # v3.1 routing: per lead → resolve template (auto pick_brug of forced/custom).
    # Bucket leads per resolved-template_id zodat we per bucket één Warmr-campaign
    # kunnen aanmaken met de juiste sequence-template.
    buckets: dict[str, dict] = {}  # bucket_key → {"template_id", "template", "steps", "leads"}
    custom_bucket_key = "__custom__"
    for lead in leads:
        t_id, t_obj, steps = _resolve_template_for_lead(lead, body.template_id, body.sequence)
        bucket_key = t_id or custom_bucket_key
        if bucket_key not in buckets:
            buckets[bucket_key] = {
                "template_id": t_id,
                "template": t_obj,
                "steps": steps,
                "leads": [],
            }
        buckets[bucket_key]["leads"].append(lead)

    if not buckets:
        raise HTTPException(status_code=422, detail="Geen leads om te versturen na completeness-check")

    # Per-bucket: validate sequence + filter via personalization-gate
    all_review: list[dict] = []
    all_skip: list[dict] = []
    for bucket_key, b in list(buckets.items()):
        _is_faseA = bool(b["steps"]) and b["steps"][0].get("faseA_brug")
        if _is_faseA:
            # Fase A-markers zijn canonieke, pre-gevalideerde templates; hun body
            # wordt pas live bij send/preview geresolved → geen v3.1-body-validatie
            # of auto-fix (die op {{opener}}-bodies werkt, niet op markers).
            b["fixed_sequence"] = b["steps"]
        else:
            is_valid, errors = validate_sequence_config(b["steps"])
            if not is_valid:
                raise HTTPException(status_code=422, detail={"bucket": bucket_key, "errors": errors})
            b["fixed_sequence"] = auto_fix_sequence_config(b["steps"])
        # Personalization gate — alleen 'auto'-leads in deze bucket gaan mee
        auto_leads, review_leads, skip_leads = _gate_leads_for_template(b["leads"], b["template"])
        all_review.extend(review_leads)
        all_skip.extend(skip_leads)
        b["auto_leads"] = auto_leads
        b["review_leads"] = review_leads
        b["skip_leads"] = skip_leads

    total_auto = sum(len(b["auto_leads"]) for b in buckets.values())
    if total_auto == 0:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Geen leads halen de personalisatie-drempel",
                "review_count": len(all_review),
                "skip_count": len(all_skip),
                "hint": "Verlaag drempel via custom sequence, of verrijking de leads opnieuw.",
            },
        )

    # Per niet-leeg bucket: maak Warmr-campaign + push leads
    client = WarmrClient()
    principal = identify_principal(request)  # vóór de loop — dispatcher-actor
    sub_results: list[dict] = []
    sub_campaign_ids: list[str] = []
    audit_lead_ids: list[str] = []
    audit_sequences: dict[str, list] = {}
    for bucket_key, b in buckets.items():
        bucket_leads = b["auto_leads"]
        if not bucket_leads:
            continue
        # Naam per bucket: prefix met brug-key zodat dashboard ze als één run kan groeperen
        bucket_label = bucket_key if bucket_key != custom_bucket_key else "custom"
        camp_name = f"{body.name} · {bucket_label}" if len(buckets) > 1 else body.name

        # Beide side-effects via de dispatcher (I3/I6/I7). Create-key over
        # naam+template+selectie: dubbel launchen van exact dezelfde intentie
        # maakt geen tweede campagne; push-key over campagne+selectie.
        from utils.outbound_dispatcher import dispatch_outbound, ids_hash
        bucket_ids_hash = ids_hash([l["id"] for l in bucket_leads if l.get("id")])
        create_disp = await dispatch_outbound(
            kind="warmr_campaign_create",
            idempotency_key=f"campaign-create:{camp_name}:{b['template_id']}:{bucket_ids_hash}",
            actor=principal.get("created_by", "unknown"),
            leads=bucket_leads,
            send=lambda: client.create_campaign(
                name=camp_name,
                sequence_steps=b["fixed_sequence"],
                settings={"inbox_ids": body.inbox_ids, "template_id": b["template_id"]},
            ),
            supabase_client=db,
            workspace_id=workspace_id,
            metadata={"endpoint": "/campaigns/launch", "bucket": bucket_label},
        )
        if create_disp.executed:
            camp_id = create_disp.result
        else:
            # Duplicate: hergebruik de campagne-id uit het eerdere ledger-record
            camp_id = (create_disp.previous or {}).get("result")
        if not isinstance(camp_id, str) or not camp_id:
            raise HTTPException(status_code=409, detail=(
                f"Campagne '{camp_name}' is al eerder met exact deze selectie gelanceerd "
                f"({(create_disp.previous or {}).get('created_at', 'eerder')}); "
                "campagne-id kon niet uit het ledger worden hersteld. Kies een andere naam."
            ))
        sub_campaign_ids.append(camp_id)
        audit_sequences[bucket_label] = b["fixed_sequence"]

        push_disp = await dispatch_outbound(
            kind="warmr_bulk_push",
            idempotency_key=f"campaign-push:{camp_id}:{bucket_ids_hash}",
            actor=principal.get("created_by", "unknown"),
            leads=bucket_leads,
            send=lambda: client.push_leads_bulk(bucket_leads, campaign_id=camp_id),
            supabase_client=db,
            workspace_id=workspace_id,
            metadata={"endpoint": "/campaigns/launch", "bucket": bucket_label},
        )
        push_result = push_disp.result if push_disp.executed else {
            "pushed": 0, "failed": 0, "duplicates": len(bucket_leads),
            "skipped_duplicate": True,
        }
        # ADR-001 (fase 3 PR 9): tracking-only enrollment per gepushte lead.
        # send_owner='warmr' + status='active' — voedt dedup/cooldown/guards,
        # wordt door get_due_sends genegeerd (Warmr dript zelf). Idempotent
        # (uq_lch_enrollment), dus ook veilig bij een dispatcher-duplicate-skip.
        from campaigns.enrollment import record_warmr_enrollments
        enroll_result = await record_warmr_enrollments(
            db, workspace_id=workspace_id, leads=bucket_leads,
            campaign_id=camp_id, template_id=b["template_id"],
            service_type=bucket_label, sequence_steps=b["fixed_sequence"],
        )
        sub_results.append({
            "bucket": bucket_label,
            "template_id": b["template_id"],
            "campaign_id": camp_id,
            "lead_count": len(bucket_leads),
            "enrollments": enroll_result,
            **push_result,
        })
        audit_lead_ids.extend([l.get("id") for l in bucket_leads if l.get("id")])

        for lead in bucket_leads:
            _insert_timeline_event(
                db, workspace_id, lead["id"], "email_sent",
                f"Verstuurd via campagne '{camp_name}'",
                metadata={"campaign_id": camp_id, "template_id": b["template_id"]},
            )

    gate_summary = {
        "auto_sent": total_auto,
        "review_skipped": len(all_review),
        "skip_skipped": len(all_skip),
        "buckets": {
            (b["template_id"] or custom_bucket_key): {
                "auto": len(b["auto_leads"]),
                "review": len(b["review_leads"]),
                "skip": len(b["skip_leads"]),
                "threshold": (b["template"] or {}).get("min_personalization_score"),
            }
            for b in buckets.values()
        },
    }
    result = {
        "campaigns": sub_results,                    # multi-bucket details
        "campaign_id": sub_campaign_ids[0] if sub_campaign_ids else None,  # backwards-compat single id
        "campaign_ids": sub_campaign_ids,            # alle bucket-campaigns
        "personalization_gate": gate_summary,
        "completeness_check": {
            "blocked_count": len(blocked_leads),
            "blocked_sample": [
                {
                    "lead_id": l.get("id"),
                    "company_name": l.get("company_name"),
                    "missing_required": l.get("_completeness", {}).get("missing_required", []),
                }
                for l in blocked_leads[:10]
            ],
        },
    }

    # Audit-trail: persist run-snapshot in heatr_campaigns. Bij multi-bucket één rij
    # per bucket-campaign zodat dashboard correct linkt.
    principal = identify_principal(request)
    for sub in sub_results:
        try:
            db.table("campaigns").insert({
                "workspace_id": workspace_id,
                "warmr_campaign_id": sub["campaign_id"],
                "name": body.name,
                "template_id": sub.get("template_id"),
                "lead_count": sub["lead_count"],
                "lead_ids": [],  # per-bucket subset; full lead_ids in bovenstaande audit_lead_ids
                "inbox_ids": body.inbox_ids,
                "sequence_snapshot": audit_sequences.get(sub["bucket"]) or [],
                "personalization_gate": gate_summary.get("buckets", {}).get(sub["template_id"] or custom_bucket_key) or {},
                "created_by": principal.get("created_by"),
                "created_via": principal.get("created_via"),
                "request_ip": principal.get("request_ip"),
                "status": "active",
                "launched_at": datetime.now(timezone.utc).isoformat(),
            }).execute()
        except Exception as exc:
            logger.warning("Failed to store campaign audit-row for bucket %s: %s", sub["bucket"], exc)

    return result


@app.post("/campaigns/{campaign_id}/activate")
async def activate_campaign_endpoint(
    campaign_id: str,
    workspace_id: str = Depends(require_service_key),  # service-only — browser kan NOOIT activeren/versturen
    db: Client = Depends(get_supabase),
) -> dict:
    """Activeer een voorbereide (draft) campagne — HET verzendmoment (fase 3).

    Send-model 2026-07-21: launch bereidt voor (draft, verstuurt nooit), preview
    reviewt, ACTIVATE is de enige "go". Vier borgingen vóór Warmr's /resume:
      1. service-key (deze dependency) — nooit vanuit de browser.
      2. kill-switch aan (ENABLE_PROSPECT_SENDS).
      3. per-ontvanger her-verificatie: is_sendable + compliance + allowlist. Faalt
         er één → 409, niks geactiveerd. KRITIEK: ná activatie verstuurt Warmr
         autonoom, buiten Heatr's per-send-gates om — dít is de laatste muur.
      4. audit-log + recipient-samenvatting in de respons.
    """
    from utils.outbound_dispatcher import _prospect_sends_enabled
    from utils.email_sendability import is_sendable
    from utils.enrichment_check import compliance_check
    from integrations.warmr_client import WarmrClient

    if not _prospect_sends_enabled():
        raise HTTPException(status_code=409,
                            detail="Kill-switch dicht (ENABLE_PROSPECT_SENDS != true) — activeren geblokkeerd.")

    enr = (db.table("lead_campaign_history").select("lead_id")
           .eq("workspace_id", workspace_id).eq("campaign_id", campaign_id).execute()).data or []
    lead_ids = [r["lead_id"] for r in enr if r.get("lead_id")]
    if not lead_ids:
        raise HTTPException(status_code=404, detail="Geen enrollments voor deze campagne.")
    leads = (db.table("leads").select("*").eq("workspace_id", workspace_id)
             .in_("id", lead_ids).execute()).data or []

    allowlist = [e.strip().lower() for e in (os.getenv("HEATR_SEND_ALLOWLIST") or "").split(",") if e.strip()]
    rejected = []
    for l in leads:
        ok, reason = is_sendable(l.get("email"), l.get("email_status"), allow_risky=False,
                                 allow_role_emails=False, is_test_lead=bool(l.get("is_test_lead")),
                                 verification_method=l.get("email_verification_method"))
        comp_ok, comp_reason = compliance_check(l)
        em = (l.get("email") or "").strip().lower()
        allow_ok = (not allowlist) or (em in allowlist)
        if not (ok and comp_ok and allow_ok):
            rejected.append({"lead_id": l.get("id"), "email": l.get("email"),
                             "reason": reason if not ok else (comp_reason if not comp_ok else "niet op HEATR_SEND_ALLOWLIST")})
    if rejected:
        raise HTTPException(status_code=409, detail={
            "error": "Activatie geblokkeerd — ontvanger(s) faalden de her-verificatie.",
            "rejected": rejected,
            "hint": "Verwijder/repareer deze leads of pas de allowlist aan; niets geactiveerd."})

    logger.warning("CAMPAIGN ACTIVATE: campaign=%s recipients=%d ws=%s (kill-switch open, her-verificatie groen)",
                   campaign_id, len(leads), workspace_id)
    wc = WarmrClient()
    result = await wc.activate_campaign(campaign_id)
    # 'email_sent' op HET activatiemoment (het echte send-punt), niet bij launch.
    # NB: nog steeds "overgedragen aan Warmr", niet "afgeleverd" — echte bezorg-
    # bevestiging vereist een Warmr-webhook (openstaande observability-taak).
    for l in leads:
        _insert_timeline_event(db, workspace_id, l["id"], "email_sent",
                               f"Campagne {campaign_id} geactiveerd — Warmr verstuurt")
    return {"ok": True, "campaign_id": campaign_id, "activated_recipients": len(leads),
            "recipients": [l.get("email") for l in leads], "warmr": result}


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


@app.get("/campaigns/{campaign_id}/audit")
async def campaign_audit(
    campaign_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Return volledige audit-trail van een campagne: wie, wanneer, met welke
    leads/sequence/template/gate-uitkomst. Voor compliance + debugging."""
    try:
        res = (
            db.table("campaigns")
            .select(
                "id, workspace_id, warmr_campaign_id, name, template_id, status, "
                "lead_count, lead_ids, inbox_ids, sequence_snapshot, "
                "personalization_gate, created_by, created_via, request_ip, "
                "created_at, launched_at, completed_at"
            )
            .eq("id", campaign_id)
            .eq("workspace_id", workspace_id)
            .maybe_single()
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Audit fetch failed: {e}")
    if not res or not res.data:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return res.data


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
        # Kolom-fix (2026-07-14): 'event_type' bestaat niet op heatr_reply_inbox
        # (migratie 004) — elke ?status=-aanroep gaf een PostgREST-error. De
        # echte filterkolom is 'classification'.
        q = q.eq("classification", status_filter)

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


# Classificatie-waarden zoals de classifier ze schrijft (reply_classifier.py
# VALID_CATEGORIES) — de bron voor het ?classification=-filter hieronder.
_REPLY_CLASSIFICATIONS = {
    "interested", "not_now", "not_interested", "wrong_person",
    "unsubscribe_request", "auto_reply", "question", "other",
}
_REPLY_PREVIEW_CHARS = 200


@app.get("/reply-inbox")
async def list_reply_inbox(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Unified inbox voor de frontend (Inbox.tsx): alle Warmr-replies, nieuwste
    eerst, in het contract dat de pagina verwacht.

    Contract-fix (2026-07-14): de frontend riep dit endpoint al aan, maar het
    bestond niet — alleen GET /inbox met andere veldnamen ({messages}, body,
    from_email). Dit endpoint levert {replies: [...]} met per rij:
      id, lead_id, subject, body_preview (HTML-gestript + afgekapt),
      sender_email (← from_email), classification, received_at,
      company_name (← join heatr_leads).

    Query-params:
      limit: max aantal rijen (default 100, cap 200).
      classification: filter op één classifier-waarde, of 'unclassified'
        voor rijen die nog niet geclassificeerd zijn (classification IS NULL).
    """
    from utils.lead_thread import _strip_html_to_text

    params = dict(request.query_params)
    limit = min(int(params.get("limit", 100)), 200)

    q = (
        db.table("reply_inbox")
        .select("id, lead_id, subject, body, body_html, from_email, "
                "classification, received_at")
        .eq("workspace_id", workspace_id)
    )
    if cls_filter := params.get("classification"):
        if cls_filter == "unclassified":
            q = q.is_("classification", "null")
        elif cls_filter in _REPLY_CLASSIFICATIONS:
            q = q.eq("classification", cls_filter)
        else:
            raise HTTPException(
                status_code=422,
                detail=f"Onbekende classification-filter: {cls_filter!r}",
            )
    res = q.order("received_at", desc=True).limit(limit).execute()
    rows = res.data or []

    # Bedrijfsnamen via een tweede batch-query i.p.v. een embedded join:
    # heatr_reply_inbox.lead_id heeft GEEN FK naar heatr_leads (migratie 004),
    # dus PostgREST kan niet embedden (PGRST200 — live smoke 2026-07-14).
    company_by_lead: dict[str, str] = {}
    lead_ids = sorted({r["lead_id"] for r in rows if r.get("lead_id")})
    if lead_ids:
        try:
            lres = (db.table("leads").select("id, company_name")
                    .eq("workspace_id", workspace_id)
                    .in_("id", lead_ids).execute())
            company_by_lead = {
                l["id"]: l.get("company_name") for l in (lres.data or [])
            }
        except Exception as e:
            # Naam-lookup is verrijking, geen kern — inbox blijft werken.
            logger.warning("reply-inbox: company_name-lookup faalde: %s", e)

    replies = []
    for row in rows:
        preview = _strip_html_to_text(row.get("body_html") or row.get("body"))
        replies.append({
            "id": row.get("id"),
            "lead_id": row.get("lead_id"),
            "subject": row.get("subject"),
            "body_preview": preview[:_REPLY_PREVIEW_CHARS] or None,
            "sender_email": row.get("from_email"),
            "classification": row.get("classification"),
            "received_at": row.get("received_at"),
            "company_name": company_by_lead.get(row.get("lead_id")),
        })
    return {"replies": replies}


# =============================================================================
# CHECK-UP FOLLOW-UP (calls) — JWT-paden (CRUD + gate 1 uitkomst + gate 2 rapport).
# Verzenden en retarget zitten op de service-key (verderop).
# =============================================================================

@app.post("/calls")
async def create_call(
    body: CallCreate,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Maak handmatig een gesprekrecord aan. Met lead_id → direct gekoppeld
    (manually_matched), anders unmatched (fail-closed)."""
    from calls.call_records import create_call_record
    match_status = "manually_matched" if body.lead_id else "unmatched"
    record = await create_call_record(
        db, workspace_id, transcript=body.transcript, call_date=body.call_date,
        participants=body.participants, duration_minutes=body.duration_minutes,
        zoom_meeting_id=body.zoom_meeting_id, transcript_source="manual",
        lead_id=body.lead_id, match_status=match_status,
    )
    if not record:
        raise HTTPException(status_code=500, detail="create_failed")
    return record


@app.get("/calls")
async def list_calls(
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Lijst gesprekken met optionele filters (report_status/outcome/match_status)."""
    from calls.call_records import list_call_records
    p = dict(request.query_params)
    calls = await list_call_records(
        db, workspace_id, lead_id=p.get("lead_id"), report_status=p.get("report_status"),
        outcome=p.get("outcome"), match_status=p.get("match_status"),
        limit=min(int(p.get("limit", 50)), 200),
    )
    # Verrijk met company_name (batch) — geen FK-embed, zoals /reply-inbox.
    lead_ids = sorted({c["lead_id"] for c in calls if c.get("lead_id")})
    if lead_ids:
        try:
            lres = (db.table("leads").select("id, company_name")
                    .eq("workspace_id", workspace_id).in_("id", lead_ids).execute())
            by_id = {l["id"]: l.get("company_name") for l in (lres.data or [])}
            for c in calls:
                c["company_name"] = by_id.get(c.get("lead_id"))
        except Exception as e:
            logger.warning("list_calls: company_name-lookup faalde: %s", e)
    return {"calls": calls}


@app.get("/calls/unmatched")
async def list_calls_unmatched(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """De fallback-lijst: gesprekken zonder gekoppelde lead."""
    from calls.call_records import list_unmatched
    return {"calls": await list_unmatched(db, workspace_id)}


@app.get("/calls/{call_id}")
async def get_call(
    call_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Volledig gesprekrecord."""
    from calls.call_records import get_call_record
    record = await get_call_record(db, workspace_id, call_id)
    if not record:
        raise HTTPException(status_code=404, detail="call not found")
    return record


@app.patch("/calls/{call_id}/match")
async def match_call(
    call_id: str,
    body: CallMatch,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Koppel een gesprek handmatig aan een lead (fail-closed: alleen exact)."""
    from calls.call_records import match_call_record
    record = await match_call_record(db, workspace_id, call_id, body.lead_id)
    if not record:
        raise HTTPException(status_code=400, detail="match_failed (lead niet in workspace?)")
    return record


@app.patch("/calls/{call_id}/outcome")
async def set_call_outcome(
    call_id: str,
    body: CallOutcome,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Gate 1 — de operator kiest de uitkomst + vult (optioneel) checkup_data.
    won→gewonnen, hard_no→verloren (geen rapport/cadans)."""
    from calls.call_records import set_outcome, VALID_OUTCOMES
    if body.outcome not in VALID_OUTCOMES:
        raise HTTPException(status_code=422, detail=f"outcome moet uit {VALID_OUTCOMES}")
    record = await set_outcome(
        db, workspace_id, call_id, body.outcome,
        outcome_note=body.outcome_note, timing_target_date=body.timing_target_date,
        checkup_data=body.checkup_data,
    )
    if not record:
        raise HTTPException(status_code=404, detail="call not found")
    return record


@app.post("/calls/{call_id}/generate-report")
async def generate_call_report(
    call_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Genereer het check-up rapport (Sonnet + QA-gate). Vereist een gekoppelde
    lead met checkup_data — anders report_status='skipped'. Faalt de QA-gate,
    dan report_status='pending' + reden (fail-closed, niet opgeslagen)."""
    from calls.call_records import get_call_record
    from calls.report_generator import generate_checkup_report, validate_report_sendable

    record = await get_call_record(db, workspace_id, call_id)
    if not record:
        raise HTTPException(status_code=404, detail="call not found")
    lead_id = record.get("lead_id")
    if not lead_id:
        raise HTTPException(status_code=400, detail="call niet gekoppeld aan lead")

    lead = (db.table("leads").select("*").eq("id", lead_id)
            .eq("workspace_id", workspace_id).maybe_single().execute()).data or {}
    checkup = lead.get("checkup_data") or {}
    if not checkup:
        # Regel 4: geen rapport zonder cijfers → alleen de cadans loopt.
        db.table("call_records").update({"report_status": "skipped"}).eq("id", call_id).eq("workspace_id", workspace_id).execute()
        return {"report_status": "skipped", "reason": "no_checkup_data"}

    wi = (db.table("website_intelligence").select("*").eq("lead_id", lead_id)
          .eq("workspace_id", workspace_id).maybe_single().execute()).data or {}

    import anthropic
    anth = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    out = await generate_checkup_report(
        lead=lead, checkup_data=checkup, transcript=record.get("transcript") or "",
        website_intelligence=wi, anthropic_client=anth, supabase_client=db,
    )
    if out.get("error"):
        raise HTTPException(status_code=502, detail=f"generatie faalde: {out['error']}")

    ok, reason = validate_report_sendable(out["report_html"], checkup, wi, findings=out["report_findings"])
    if not ok:
        logger.warning("checkup: QA-gate afgekeurd voor call %s: %s", call_id, reason)
        return {"report_status": "pending", "qa_failed": reason}

    db.table("call_records").update({
        "report_findings": out["report_findings"], "report_html": out["report_html"],
        "report_status": "generated", "updated_at": _now_iso(),
    }).eq("id", call_id).eq("workspace_id", workspace_id).execute()
    return {"report_status": "generated", "report_findings": out["report_findings"],
            "report_html": out["report_html"], "cost_eur": out["cost_eur"]}


@app.patch("/calls/{call_id}/report")
async def patch_call_report(
    call_id: str,
    body: CallReportPatch,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Gate 2 — de operator geeft vrij of gooit weg.

    approve: report_status='approved' (+ eventueel bewerkte report_html). De
    QA-gate draait óók hier opnieuw (fail-closed: ook een menselijke edit mag
    geen ongegrond getal / pitch / streepje bevatten). discard: terug naar
    'pending', rapport weggegooid.
    """
    from calls.call_records import get_call_record
    from calls.report_generator import validate_report_sendable

    record = await get_call_record(db, workspace_id, call_id)
    if not record:
        raise HTTPException(status_code=404, detail="call not found")

    if body.action == "discard":
        db.table("call_records").update({
            "report_status": "pending", "report_html": None, "report_findings": None,
            "updated_at": _now_iso(),
        }).eq("id", call_id).eq("workspace_id", workspace_id).execute()
        return {"report_status": "pending"}

    if body.action != "approve":
        raise HTTPException(status_code=422, detail="action moet 'approve' of 'discard' zijn")

    final_html = body.report_html if body.report_html is not None else record.get("report_html")
    if not final_html:
        raise HTTPException(status_code=400, detail="geen rapport om vrij te geven")

    lead_id = record.get("lead_id")
    lead = (db.table("leads").select("checkup_data").eq("id", lead_id)
            .eq("workspace_id", workspace_id).maybe_single().execute()).data or {} if lead_id else {}
    wi = (db.table("website_intelligence").select("*").eq("lead_id", lead_id)
          .eq("workspace_id", workspace_id).maybe_single().execute()).data or {} if lead_id else {}
    ok, reason = validate_report_sendable(final_html, lead.get("checkup_data") or {}, wi,
                                          findings=record.get("report_findings"))
    if not ok:
        raise HTTPException(status_code=422, detail=f"QA-gate afgekeurd: {reason}")

    db.table("call_records").update({
        "report_html": final_html, "report_status": "approved", "updated_at": _now_iso(),
    }).eq("id", call_id).eq("workspace_id", workspace_id).execute()
    return {"report_status": "approved"}


def _schedule_retarget_fields(outcome: str, timing_target_date: str | None) -> dict:
    """Bepaal retarget_due_at/status uit de cadans-config (config/retarget_cadence).

    Date-based (timing/stalled) -> concrete due_at; event-triggered (no_value) ->
    geen datum, status 'scheduled' (de event-hook beslist); geen cadans -> niets.
    Puur; geen side-effects.
    """
    from config.retarget_cadence import cadence_for
    cad = cadence_for(outcome)
    if not cad:
        return {}
    if cad.get("event_triggered"):
        return {"retarget_status": "scheduled", "retarget_due_at": None}
    days = int(cad.get("days") or 0)
    due = None
    if cad.get("use_target_date") and timing_target_date:
        due = timing_target_date
    elif days > 0:
        due = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    return {"retarget_status": "scheduled", "retarget_due_at": due}


@app.post("/calls/{call_id}/send-report")
async def send_call_report(
    call_id: str,
    body: CallSendRequest,
    workspace_id: str = Depends(require_service_key),
    db: Client = Depends(get_supabase),
) -> dict:
    """Verstuur het goedgekeurde check-up rapport (PDF-link via Warmr).

    Service-only (nooit browser: dit is een send). Dubbel op slot:
      1. feature-gate CHECKUP_REPORT_ENABLED,
      2. de dispatcher zelf gate't op ENABLE_PROSPECT_SENDS.

    Fail-closed precondities: report_status MOET 'approved' zijn (menselijke gate
    2 gepasseerd), de call moet gekoppeld zijn aan een lead mét e-mail. Warmr kan
    geen bijlage -> de PDF wordt gerenderd (Chromium), geüpload naar Storage
    (signed URL) en als link meegegeven. report_status wordt pas 'sent' ná een
    geslaagde push (dat is de verifieerbare voorwaarde voor de latere retarget).
    """
    if os.getenv("CHECKUP_REPORT_ENABLED", "false").lower() != "true":
        raise HTTPException(
            status_code=403,
            detail="Check-up sends staan uit (CHECKUP_REPORT_ENABLED=false).",
        )

    from calls.call_records import get_call_record
    from calls.report_pdf import render_report_pdf, upload_report_pdf
    from calls.report_generator import build_cover_mail

    record = await get_call_record(db, workspace_id, call_id)
    if not record:
        raise HTTPException(status_code=404, detail="call not found")
    if record.get("report_status") != "approved":
        raise HTTPException(
            status_code=409,
            detail=f"Rapport is niet vrijgegeven (report_status={record.get('report_status')}). "
                   f"Gate 2 (vrijgeven) moet eerst.",
        )
    if record.get("report_sent_at"):
        raise HTTPException(status_code=409, detail="Rapport is al verstuurd.")

    lead_id = record.get("lead_id")
    if not lead_id:
        raise HTTPException(status_code=409, detail="Gesprek is niet aan een lead gekoppeld.")
    lead = (db.table("leads").select("*").eq("id", lead_id)
            .eq("workspace_id", workspace_id).maybe_single().execute()).data
    if not lead:
        raise HTTPException(status_code=404, detail="Gekoppelde lead niet gevonden.")
    if not (lead.get("email") or "").strip():
        raise HTTPException(status_code=409, detail="Lead heeft geen e-mailadres.")

    report_html = record.get("report_html")
    if not report_html:
        raise HTTPException(status_code=409, detail="Geen report_html om te versturen.")

    # PDF renderen + hosten (signed URL). Faalt dit -> status ongewijzigd.
    try:
        pdf_bytes = await render_report_pdf(report_html, lead.get("company_name") or "")
        report_url = await upload_report_pdf(pdf_bytes, call_id, db)
    except Exception as e:  # noqa: BLE001
        logger.error("send-report: PDF-pijplijn faalde (call=%s): %s", call_id, e)
        raise HTTPException(status_code=500, detail=f"PDF-pijplijn faalde: {e}")

    cover = build_cover_mail(lead, report_url)

    # dry_run: alles behalve de push (test de render+URL+covertekst zonder te sturen).
    if body.dry_run:
        return {"dry_run": True, "report_url": report_url, "cover": cover,
                "pdf_bytes": len(pdf_bytes)}

    from integrations.warmr_client import WarmrClient
    from utils.outbound_dispatcher import dispatch_outbound, DispatchHalted
    client = WarmrClient()
    inboxes = await client.get_ready_inboxes()
    if not inboxes:
        raise HTTPException(status_code=503, detail="Geen Warmr-inbox beschikbaar.")
    # Warmr-side check-up-campagne indien geconfigureerd; anders de ready inbox
    # (zoals send-review-email). De cover-body draagt de report_url (Warmr kan
    # geen bijlage); push_lead zet 'm in payload.custom_fields.custom_body.
    campaign_id = os.getenv("CHECKUP_WARMR_CAMPAIGN_ID") or inboxes[0]["id"]

    try:
        disp = await dispatch_outbound(
            kind="warmr_push",
            idempotency_key=f"checkup-report:{call_id}",
            actor="operator",
            lead=lead,
            send=lambda: client.push_lead(
                lead, campaign_id=campaign_id, preferred_inbox_id=inboxes[0]["id"],
                custom_subject=cover["subject"], custom_body=cover["body"],
            ),
            supabase_client=db,
            workspace_id=workspace_id,
            metadata={"endpoint": "/calls/{id}/send-report", "call_id": call_id},
        )
    except DispatchHalted as e:
        raise HTTPException(status_code=403, detail=f"Send geweigerd: {e}")
    if disp.skipped_duplicate:
        raise HTTPException(status_code=409, detail="Rapport is al verstuurd (dispatcher-dedup).")
    if not disp.executed:
        raise HTTPException(status_code=502, detail="Warmr-push niet uitgevoerd.")

    updates = {"report_status": "sent", "report_sent_at": _now_iso(), "updated_at": _now_iso()}
    updates.update(_schedule_retarget_fields(record.get("outcome") or "", record.get("timing_target_date")))
    db.table("call_records").update(updates).eq("id", call_id).eq("workspace_id", workspace_id).execute()
    _insert_timeline_event(db, workspace_id, lead_id, "checkup_report_sent",
                           "Check-up rapport verstuurd via Warmr")
    return {"report_status": "sent", "report_url": report_url,
            "retarget_status": updates.get("retarget_status")}


def _retarget_max_attempts(outcome: str) -> int:
    """Max pogingen voor een uitkomst: cadans-config, geplafonneerd door de env-cap."""
    from config.retarget_cadence import cadence_for
    cad = cadence_for(outcome) or {}
    per_outcome = int(cad.get("max_attempts") or 0)
    cap = int(os.getenv("RETARGET_MAX_ATTEMPTS", "2"))
    return min(per_outcome, cap) if per_outcome else cap


@app.post("/calls/{call_id}/retarget")
async def retarget_call(
    call_id: str,
    body: CallRetargetRequest,
    workspace_id: str = Depends(require_service_key),
    db: Client = Depends(get_supabase),
) -> dict:
    """Verstuur een retarget-mail voor een afgesloten gesprek (cadans).

    Service-only + dubbel op slot (CHECKUP_REPORT_ENABLED + dispatcher). De
    variant (with_report/no_report) wordt HARD gekozen op report_status; de
    QA-gate is fail-closed (o.a. hard rule 1: geen rapport-verwijzing tenzij
    report_status='sent'). Na een geslaagde push: poging+1, en bij het bereiken
    van max_attempts -> retarget_status='exhausted', anders opnieuw ingepland.
    """
    if os.getenv("CHECKUP_REPORT_ENABLED", "false").lower() != "true":
        raise HTTPException(status_code=403, detail="Check-up sends staan uit (CHECKUP_REPORT_ENABLED=false).")

    from calls.call_records import get_call_record
    from calls.retarget_generator import generate_retarget_mail, validate_retarget_sendable

    record = await get_call_record(db, workspace_id, call_id)
    if not record:
        raise HTTPException(status_code=404, detail="call not found")
    if record.get("retarget_status") != "scheduled":
        raise HTTPException(status_code=409,
                            detail=f"Geen geplande retarget (retarget_status={record.get('retarget_status')}).")

    due = record.get("retarget_due_at")
    if not body.force and due and due > _now_iso():
        raise HTTPException(status_code=409, detail=f"Retarget nog niet due (gepland {due}).")

    outcome = record.get("outcome") or ""
    max_attempts = _retarget_max_attempts(outcome)
    attempt = int(record.get("retarget_attempt") or 0)
    if attempt >= max_attempts:
        db.table("call_records").update({"retarget_status": "exhausted", "updated_at": _now_iso()}) \
            .eq("id", call_id).eq("workspace_id", workspace_id).execute()
        raise HTTPException(status_code=409, detail="Max pogingen bereikt (exhausted).")

    lead_id = record.get("lead_id")
    if not lead_id:
        raise HTTPException(status_code=409, detail="Gesprek is niet aan een lead gekoppeld.")
    lead = (db.table("leads").select("*").eq("id", lead_id)
            .eq("workspace_id", workspace_id).maybe_single().execute()).data
    if not lead:
        raise HTTPException(status_code=404, detail="Gekoppelde lead niet gevonden.")
    if not (lead.get("email") or "").strip():
        raise HTTPException(status_code=409, detail="Lead heeft geen e-mailadres.")

    mail = await generate_retarget_mail(record, lead, supabase_client=db)
    if mail.get("error") or not mail.get("body"):
        raise HTTPException(status_code=502, detail=f"Retarget-generatie faalde: {mail.get('error')}")
    ok, reason = validate_retarget_sendable(mail["body"], mail["variant"], record.get("report_status"))
    if not ok:
        raise HTTPException(status_code=422, detail=f"Retarget QA-gate afgekeurd: {reason}")

    if body.dry_run:
        return {"dry_run": True, "variant": mail["variant"], "attempt_would_be": attempt + 1,
                "subject": mail["subject"], "body": mail["body"]}

    from integrations.warmr_client import WarmrClient
    from utils.outbound_dispatcher import dispatch_outbound, DispatchHalted
    client = WarmrClient()
    inboxes = await client.get_ready_inboxes()
    if not inboxes:
        raise HTTPException(status_code=503, detail="Geen Warmr-inbox beschikbaar.")
    campaign_id = os.getenv("CHECKUP_WARMR_CAMPAIGN_ID") or inboxes[0]["id"]

    try:
        disp = await dispatch_outbound(
            kind="warmr_push",
            idempotency_key=f"retarget:{call_id}:{attempt + 1}",
            actor="operator",
            lead=lead,
            send=lambda: client.push_lead(
                lead, campaign_id=campaign_id, preferred_inbox_id=inboxes[0]["id"],
                custom_subject=mail["subject"], custom_body=mail["body"],
            ),
            supabase_client=db,
            workspace_id=workspace_id,
            metadata={"endpoint": "/calls/{id}/retarget", "call_id": call_id,
                      "variant": mail["variant"], "attempt": attempt + 1},
        )
    except DispatchHalted as e:
        raise HTTPException(status_code=403, detail=f"Send geweigerd: {e}")
    if disp.skipped_duplicate:
        raise HTTPException(status_code=409, detail="Deze retarget-poging is al verstuurd (dedup).")
    if not disp.executed:
        raise HTTPException(status_code=502, detail="Warmr-push niet uitgevoerd.")

    new_attempt = attempt + 1
    updates = {"retarget_attempt": new_attempt, "retarget_last_sent_at": _now_iso(),
               "updated_at": _now_iso()}
    if new_attempt >= max_attempts:
        updates["retarget_status"] = "exhausted"
        updates["retarget_due_at"] = None
    else:
        updates.update(_schedule_retarget_fields(outcome, record.get("timing_target_date")))
        # use_target_date is verbruikt; de volgende poging is puur op interval.
        if updates.get("retarget_due_at") == record.get("timing_target_date"):
            from config.retarget_cadence import cadence_for
            days = int((cadence_for(outcome) or {}).get("days") or 0)
            updates["retarget_due_at"] = (
                (datetime.now(timezone.utc) + timedelta(days=days)).isoformat() if days else None)
    db.table("call_records").update(updates).eq("id", call_id).eq("workspace_id", workspace_id).execute()
    _insert_timeline_event(db, workspace_id, lead_id, "retarget_sent",
                           f"Retarget verstuurd (poging {new_attempt}, {mail['variant']})")
    return {"retarget_status": updates["retarget_status"] if "retarget_status" in updates else "scheduled",
            "attempt": new_attempt, "variant": mail["variant"]}


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

    # Bouncer schrijft 'valid'/'catchall_risky'; het oude SMTP-pad schreef 'verified'/'catch_all'.
    verified = email_counts.get("valid", 0) + email_counts.get("verified", 0)
    catchall = email_counts.get("catch_all", 0) + email_counts.get("catchall_risky", 0)
    coverage_pct = round((verified + catchall) / total * 100) if total else 0

    # heatr_reply_inbox heeft geen 'event_type' kolom — gebruik 'status'/'classification' als die bestaat
    # of tel gewoon alle rijen. Faal silent op schema-mismatch zodat de endpoint niet crasht.
    try:
        inbox_res = db.table("reply_inbox").select("id", count="exact").eq("workspace_id", workspace_id).execute()
        replies = inbox_res.count or 0
    except Exception:
        replies = 0

    sent = sum(1 for l in leads if l.get("crm_stage") not in ("ontdekt", None))

    # CRM stats — heatr_crm_deals table bestaat mogelijk nog niet. Faal silent.
    today = datetime.now(timezone.utc).date()
    month_start = today.replace(day=1).isoformat()
    try:
        deals_res = db.table("crm_deals").select("value").eq("workspace_id", workspace_id).gte("created_at", month_start).execute()
        won_this_month = sum(d.get("value") or 0 for d in (deals_res.data or []))
    except Exception:
        won_this_month = 0

    # Website-kansen: geanalyseerde sites met een lage score (<50 = pitchbaar, sluit 0/niet-geanalyseerd uit).
    try:
        wo_res = (
            db.table("website_intelligence")
            .select("id", count="exact")
            .eq("workspace_id", workspace_id)
            .gt("total_score", 0)
            .lt("total_score", 50)
            .execute()
        )
        website_opps = wo_res.count or 0
    except Exception:
        website_opps = 0

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
        "website_opportunities": website_opps,
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
        # Schuld-fix 2026-07-18: matchte op "conversion_optimisation", maar de
        # classifier schrijft "conversie_optimalisatie" (opportunity_classifier.py:47)
        # -> teller stond altijd op 0. De data is de bron van waarheid.
        "conversion_count": sum(1 for r in rows if "conversie_optimalisatie" in (r.get("opportunity_types") or [])),
    }


@app.get("/analytics/calls")
async def analytics_calls(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Check-up funnel + leerlus.

    De leerlus voedt (handmatig, voorlopig) config/retarget_cadence: welke uitkomst
    en welke poging leveren replies, en welke bevinding-typen zitten in de replies.
    Reply-tijdstip is bij benadering (updated_at op het moment dat de webhook de
    flow op 'replied' zette; daarna muteert de record niet meer).
    """
    res = (db.table("call_records").select(
        "outcome, match_status, report_status, report_findings, report_sent_at, "
        "retarget_status, retarget_attempt, retarget_last_sent_at, updated_at")
        .eq("workspace_id", workspace_id).execute())
    rows = res.data or []
    total = len(rows)

    def _tally(key: str) -> dict:
        out: dict[str, int] = {}
        for r in rows:
            out[r.get(key) or "onbekend"] = out.get(r.get(key) or "onbekend", 0) + 1
        return out

    # Records die daadwerkelijk de outbound-lus in gingen (rapport verstuurd of
    # minstens één retarget) — de noemer voor reply-rates.
    engaged = [r for r in rows if r.get("report_status") == "sent" or int(r.get("retarget_attempt") or 0) > 0]

    reply_per_outcome: dict[str, dict] = {}
    for r in engaged:
        o = r.get("outcome") or "onbekend"
        b = reply_per_outcome.setdefault(o, {"engaged": 0, "replied": 0})
        b["engaged"] += 1
        if r.get("retarget_status") == "replied":
            b["replied"] += 1
    for o, b in reply_per_outcome.items():
        b["reply_rate_pct"] = round(b["replied"] / b["engaged"] * 100, 1) if b["engaged"] else 0.0

    # Reply per poging: bij welke retarget-poging kwam de reply.
    reply_per_attempt: dict[str, int] = {}
    for r in rows:
        if r.get("retarget_status") == "replied":
            a = str(r.get("retarget_attempt") or 0)
            reply_per_attempt[a] = reply_per_attempt.get(a, 0) + 1

    # Dagen-tot-reply per uitkomst (benadering: updated_at - report_sent_at).
    from datetime import datetime as _dt
    def _parse(ts):
        try:
            return _dt.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return None
    days_acc: dict[str, list] = {}
    for r in rows:
        if r.get("retarget_status") != "replied":
            continue
        start = _parse(r.get("report_sent_at") or r.get("retarget_last_sent_at"))
        end = _parse(r.get("updated_at"))
        if start and end and end >= start:
            days_acc.setdefault(r.get("outcome") or "onbekend", []).append((end - start).total_seconds() / 86400)
    days_to_reply = {o: round(sum(v) / len(v), 1) for o, v in days_acc.items() if v}

    # Bevinding-typen in replies: welke pijn, benoemd, correleert met een reply.
    finding_types_replied: dict[str, int] = {}
    for r in rows:
        if r.get("retarget_status") != "replied":
            continue
        for f in (r.get("report_findings") or []):
            if isinstance(f, dict) and f.get("title"):
                t = str(f["title"]).strip()
                finding_types_replied[t] = finding_types_replied.get(t, 0) + 1

    return {
        "total_calls": total,
        "funnel": {
            "by_outcome": _tally("outcome"),
            "unmatched": sum(1 for r in rows if r.get("match_status") == "unmatched"),
            "reports": _tally("report_status"),
            "retargets": _tally("retarget_status"),
        },
        "learning": {
            "engaged_total": len(engaged),
            "reply_rate_per_outcome": reply_per_outcome,
            "reply_per_attempt": reply_per_attempt,
            "days_to_reply_per_outcome": days_to_reply,
            "finding_types_in_replies": dict(sorted(
                finding_types_replied.items(), key=lambda kv: kv[1], reverse=True)),
        },
    }


@app.get("/analytics/enrichment-cost")
async def analytics_enrichment_cost(
    days: int = 7,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Enrichment spend overview — cost_guard burn-rate dashboard.

    Reads heatr_api_cost_log. Returns:
      - today_eur: vandaag tot nu
      - daily_budget_eur: current cap (env ENRICHMENT_DAILY_BUDGET_EUR)
      - budget_remaining_eur: cap - today_eur (0 als overschreden)
      - budget_pct_used: 0..100
      - window_eur: som over ?days=7 (default)
      - by_context: {context: eur} per enrichment-stap
      - block_events: aantal 'BLOCKED:*' entries in window
    """
    from datetime import datetime, timezone, timedelta
    from utils.cost_guard import _daily_budget_eur, check_monthly_budget

    today = datetime.now(timezone.utc).date()
    today_start = f"{today.isoformat()}T00:00:00+00:00"
    window_start = (today - timedelta(days=max(days - 1, 0))).isoformat() + "T00:00:00+00:00"
    month_start = today.replace(day=1).isoformat() + "T00:00:00+00:00"

    try:
        today_rows = db.table("api_cost_log").select("cost_eur, context").eq("workspace_id", workspace_id).gte("created_at", today_start).execute().data or []
    except Exception:
        today_rows = []
    try:
        window_rows = db.table("api_cost_log").select("cost_eur, context, created_at").eq("workspace_id", workspace_id).gte("created_at", window_start).execute().data or []
    except Exception:
        window_rows = []

    today_eur = sum(float(r.get("cost_eur") or 0) for r in today_rows)
    window_eur = sum(float(r.get("cost_eur") or 0) for r in window_rows)

    by_context: dict[str, float] = {}
    for r in window_rows:
        ctx = (r.get("context") or "").split(":", 1)[0] or "unknown"
        by_context[ctx] = by_context.get(ctx, 0.0) + float(r.get("cost_eur") or 0)

    block_events = sum(
        1 for r in window_rows if (r.get("context") or "").startswith("BLOCKED:")
    )

    cap = _daily_budget_eur()
    remaining = max(cap - today_eur, 0.0)
    pct = round((today_eur / cap * 100) if cap > 0 else 0, 1)

    # Monthly spend + budget + 50%-approval-check
    try:
        month_rows = db.table("api_cost_log").select("cost_eur").eq("workspace_id", workspace_id).gte("created_at", month_start).execute().data or []
        month_eur = sum(float(r.get("cost_eur") or 0) for r in month_rows)
    except Exception:
        month_eur = 0.0
    monthly_status = await check_monthly_budget(workspace_id, db)

    return {
        "today_eur": round(today_eur, 6),
        "daily_budget_eur": cap,
        "budget_remaining_eur": round(remaining, 6),
        "budget_pct_used": pct,
        "budget_warning": pct >= 80,
        "window_days": days,
        "window_eur": round(window_eur, 6),
        "month_eur": round(month_eur, 6),
        "monthly_budget_eur": monthly_status["monthly_budget_eur"],
        "monthly_base_eur": monthly_status.get("monthly_base_eur"),
        "monthly_override_eur": monthly_status.get("monthly_override_eur", 0.0),
        "override_info": monthly_status.get("override_info") or None,
        "month_pct_used": monthly_status["pct_used"],
        "month_remaining_eur": round(max(monthly_status["monthly_budget_eur"] - monthly_status["month_eur"], 0), 6),
        "tier_50_hit": monthly_status["tier_50_hit"],
        "tier_100_hit": monthly_status["tier_100_hit"],
        "approved_over_50": monthly_status["approved_over_50"],
        "enrichment_allowed": monthly_status["allowed"],
        "approval_required": monthly_status["tier_50_hit"] and not monthly_status["approved_over_50"] and not monthly_status["tier_100_hit"],
        "block_reason": monthly_status["block_reason"],
        "by_context": {k: round(v, 6) for k, v in sorted(by_context.items(), key=lambda kv: -kv[1])},
        "block_events_in_window": block_events,
    }


@app.post("/analytics/enrichment-approve-continue")
async def analytics_approve_continue(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """User klikt 'doorgaan' na 50% monthly budget waarschuwing.

    Zet approval-flag in heatr_system_state met TTL = einde huidige maand.
    Na deze call mag enrichment doordraaien tot 100% monthly budget (hard stop).
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    # End of month
    if now.month == 12:
        end_of_month = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        end_of_month = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)

    try:
        db.table("system_state").upsert({
            "key": f"enrichment_approved_over_50:{workspace_id}",
            "value": {"approved_at": now.isoformat(), "by": "user"},
            "expires_at": end_of_month.isoformat(),
            "updated_at": now.isoformat(),
        }, on_conflict="key").execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save approval: {e}")

    return {"approved": True, "expires_at": end_of_month.isoformat(), "message": "Enrichment doorgaan tot 100% monthly cap bereikt is."}


@app.post("/analytics/enrichment-raise-monthly-cap")
async def analytics_raise_monthly_cap(
    body: dict,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """User klikt 'verhoog maand-budget met €X' op hard-stop banner.

    body: {"extra_eur": float}  — hoeveel euro extra boven de base cap.
    TTL = einde huidige maand (next month reset).
    Cumulatief bij herhaalde calls: nieuwe extra_eur vervangt oude.
    """
    from datetime import datetime, timezone
    try:
        extra = float(body.get("extra_eur", 0))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="extra_eur moet een getal zijn")
    if extra <= 0 or extra > 500:
        raise HTTPException(status_code=400, detail="extra_eur moet tussen €0.01 en €500 liggen")

    now = datetime.now(timezone.utc)
    if now.month == 12:
        end_of_month = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        end_of_month = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)

    try:
        db.table("system_state").upsert({
            "key": f"enrichment_monthly_override:{workspace_id}",
            "value": {"extra_eur": extra, "approved_at": now.isoformat(), "by": "user"},
            "expires_at": end_of_month.isoformat(),
            "updated_at": now.isoformat(),
        }, on_conflict="key").execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save override: {e}")

    return {
        "extra_eur": extra,
        "expires_at": end_of_month.isoformat(),
        "message": f"Maand-budget verhoogd met €{extra:.2f} tot einde maand. Enrichment mag doorgaan.",
    }


@app.post("/analytics/enrichment-pause")
async def analytics_pause_enrichment(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """User kiest om enrichment te stoppen — verwijdert approval-flag.

    Na deze call blokkeert cost_guard opnieuw bij > 50% monthly budget.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    try:
        db.table("system_state").delete().eq("key", f"enrichment_approved_over_50:{workspace_id}").execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to pause: {e}")
    return {"paused": True, "at": now.isoformat(), "message": "Enrichment gepauzeerd. Nieuwe Claude calls worden geblokkeerd tot herstart van de maand."}


@app.get("/analytics/funnel")
async def analytics_funnel(
    weeks: int = 8,
    group_by: str = "archetype",   # 'archetype' | 'sector' | 'archetype+sector'
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Funnel-cohort analyse per groep × week.

    Per cohort 7 stages:
      imported → has_email → has_verified_email → sent → opened (proxy) →
      replied → interested

    `group_by` opties:
      - 'archetype' (default)
      - 'sector'
      - 'archetype+sector' (gecombineerd)
      - 'none' (één cohort over alles)

    Default 8 weken historie.
    """
    from datetime import datetime, timezone, timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(weeks=max(weeks, 1))).isoformat()

    # 1. Fetch leads in window
    try:
        leads_res = (
            db.table("leads")
            .select("id, archetype, sector, email, email_status, created_at")
            .eq("workspace_id", workspace_id)
            .gte("created_at", cutoff)
            .execute()
        )
        leads = leads_res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"funnel fetch failed: {e}")

    lead_ids = [l["id"] for l in leads]

    # 2. Bulk-fetch send-history en replies
    sent_lead_ids: set[str] = set()
    if lead_ids:
        try:
            hist = (
                db.table("lead_campaign_history")
                .select("lead_id, sent_at")
                .in_("lead_id", lead_ids)
                .eq("workspace_id", workspace_id)
                .execute()
            )
            for h in (hist.data or []):
                if h.get("sent_at"):
                    sent_lead_ids.add(h["lead_id"])
        except Exception:
            pass

    replied_lead_ids: set[str] = set()
    interested_lead_ids: set[str] = set()
    if lead_ids:
        try:
            replies = (
                db.table("reply_inbox")
                .select("lead_id, classification")
                .in_("lead_id", lead_ids)
                .eq("workspace_id", workspace_id)
                .execute()
            )
            for r in (replies.data or []):
                replied_lead_ids.add(r["lead_id"])
                if r.get("classification") == "interested":
                    interested_lead_ids.add(r["lead_id"])
        except Exception:
            pass

    # 3. Bucket per cohort
    def _key(lead: dict) -> str:
        if group_by == "archetype":
            return lead.get("archetype") or "(unknown)"
        if group_by == "sector":
            return lead.get("sector") or "(unknown)"
        if group_by == "archetype+sector":
            return f"{lead.get('archetype') or '?'} / {lead.get('sector') or '?'}"
        return "all"

    def _week_key(iso: str | None) -> str:
        if not iso:
            return "(unknown)"
        try:
            dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
            iso_year, iso_week, _ = dt.isocalendar()
            return f"{iso_year}-W{iso_week:02d}"
        except (ValueError, TypeError):
            return "(unknown)"

    cohorts: dict[str, dict] = {}  # (key, week) → counts dict
    for lead in leads:
        ck = (_key(lead), _week_key(lead.get("created_at")))
        if ck not in cohorts:
            cohorts[ck] = {
                "group": ck[0], "week": ck[1],
                "imported": 0, "has_email": 0, "has_verified_email": 0,
                "sent": 0, "replied": 0, "interested": 0,
            }
        c = cohorts[ck]
        c["imported"] += 1
        if lead.get("email"):
            c["has_email"] += 1
            if (lead.get("email_status") or "").lower() in ("verified", "valid"):
                c["has_verified_email"] += 1
        if lead["id"] in sent_lead_ids:
            c["sent"] += 1
        if lead["id"] in replied_lead_ids:
            c["replied"] += 1
        if lead["id"] in interested_lead_ids:
            c["interested"] += 1

    cohort_list = sorted(cohorts.values(), key=lambda x: (x["week"], x["group"]))

    # 4. Aggregated totals (over all cohorts)
    totals = {
        "imported": len(leads),
        "has_email": sum(1 for l in leads if l.get("email")),
        "has_verified_email": sum(1 for l in leads if (l.get("email_status") or "").lower() in ("verified", "valid")),
        "sent": len(sent_lead_ids),
        "replied": len(replied_lead_ids),
        "interested": len(interested_lead_ids),
    }
    # Conversion rates
    conversions = {
        "email_pct": round(totals["has_email"] / totals["imported"] * 100, 1) if totals["imported"] else 0,
        "verified_pct_of_email": round(totals["has_verified_email"] / totals["has_email"] * 100, 1) if totals["has_email"] else 0,
        "sent_pct_of_verified": round(totals["sent"] / totals["has_verified_email"] * 100, 1) if totals["has_verified_email"] else 0,
        "reply_rate_pct": round(totals["replied"] / totals["sent"] * 100, 1) if totals["sent"] else 0,
        "interested_pct_of_replies": round(totals["interested"] / totals["replied"] * 100, 1) if totals["replied"] else 0,
    }

    return {
        "weeks": weeks,
        "group_by": group_by,
        "totals": totals,
        "conversions": conversions,
        "cohorts": cohort_list,
    }


@app.get("/analytics/cost-attribution")
async def analytics_cost_attribution(
    days: int = 30,
    group_by: str = "archetype",   # 'archetype' | 'sector' | 'context' | 'archetype+sector'
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Cost-attribution per groep over `days` window.

    Joint api_cost_log met leads om per archetype/sector te tonen:
      - total_cost_eur
      - lead_count (unieke leads in groep)
      - cost_per_lead
      - replies + interested binnen groep
      - cost_per_reply, cost_per_interested
    """
    from datetime import datetime, timezone, timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(days, 1))).isoformat()

    # 1. Cost log binnen window
    try:
        cost_res = (
            db.table("api_cost_log")
            .select("lead_id, cost_eur, context, created_at")
            .eq("workspace_id", workspace_id)
            .gte("created_at", cutoff)
            .execute()
        )
        cost_rows = cost_res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"cost-attribution fetch failed: {e}")

    if not cost_rows:
        return {"days": days, "group_by": group_by, "groups": [], "totals": {"cost_eur": 0}}

    lead_ids = list({r["lead_id"] for r in cost_rows if r.get("lead_id")})

    # 2. Lead-metadata
    lead_meta: dict[str, dict] = {}
    if lead_ids:
        try:
            leads_res = (
                db.table("leads")
                .select("id, archetype, sector")
                .in_("id", lead_ids)
                .execute()
            )
            for l in (leads_res.data or []):
                lead_meta[l["id"]] = l
        except Exception:
            pass

    # 3. Replies → reply-counts per lead
    reply_lead_ids: set[str] = set()
    interested_lead_ids: set[str] = set()
    if lead_ids:
        try:
            r_res = (
                db.table("reply_inbox")
                .select("lead_id, classification")
                .in_("lead_id", lead_ids)
                .eq("workspace_id", workspace_id)
                .execute()
            )
            for row in (r_res.data or []):
                reply_lead_ids.add(row["lead_id"])
                if row.get("classification") == "interested":
                    interested_lead_ids.add(row["lead_id"])
        except Exception:
            pass

    # 4. Group-key resolver
    def _key(lead_id: str | None, context: str | None) -> str:
        if group_by == "context":
            return (context or "(none)").split(":", 1)[0]
        meta = lead_meta.get(lead_id or "", {}) if lead_id else {}
        if group_by == "archetype":
            return meta.get("archetype") or "(unknown)"
        if group_by == "sector":
            return meta.get("sector") or "(unknown)"
        if group_by == "archetype+sector":
            return f"{meta.get('archetype') or '?'} / {meta.get('sector') or '?'}"
        return "all"

    # 5. Aggregate
    groups: dict[str, dict] = {}
    for row in cost_rows:
        gk = _key(row.get("lead_id"), row.get("context"))
        g = groups.setdefault(gk, {"group": gk, "cost_eur": 0.0, "call_count": 0, "leads": set(), "replies": 0, "interested": 0})
        g["cost_eur"] += float(row.get("cost_eur") or 0)
        g["call_count"] += 1
        if row.get("lead_id"):
            g["leads"].add(row["lead_id"])

    # Replies/interested per group
    for lead_id in reply_lead_ids:
        gk = _key(lead_id, None)
        if gk in groups:
            groups[gk]["replies"] += 1
    for lead_id in interested_lead_ids:
        gk = _key(lead_id, None)
        if gk in groups:
            groups[gk]["interested"] += 1

    # Finalize
    out_groups = []
    for g in groups.values():
        lead_count = len(g["leads"])
        cost = round(g["cost_eur"], 4)
        out_groups.append({
            "group": g["group"],
            "cost_eur": cost,
            "call_count": g["call_count"],
            "lead_count": lead_count,
            "replies": g["replies"],
            "interested": g["interested"],
            "cost_per_lead_eur": round(cost / lead_count, 4) if lead_count else None,
            "cost_per_reply_eur": round(cost / g["replies"], 4) if g["replies"] else None,
            "cost_per_interested_eur": round(cost / g["interested"], 4) if g["interested"] else None,
        })
    out_groups.sort(key=lambda x: -x["cost_eur"])

    totals = {
        "cost_eur": round(sum(r["cost_eur"] for r in out_groups), 4),
        "call_count": sum(r["call_count"] for r in out_groups),
        "lead_count": sum(r["lead_count"] for r in out_groups),
        "replies": sum(r["replies"] for r in out_groups),
        "interested": sum(r["interested"] for r in out_groups),
    }

    return {
        "days": days,
        "group_by": group_by,
        "totals": totals,
        "groups": out_groups,
    }


@app.get("/analytics/export/leads.csv")
async def analytics_export_leads_csv(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
):
    """Direct CSV-dump van alle workspace-leads voor offline analyse.

    Geen pagination — beoogd voor 1-time export naar Excel/Google Sheets.
    Returns text/csv response stream.
    """
    import csv
    import io
    from fastapi.responses import StreamingResponse

    # Probeer eerst met de "rich" kolomset; bij ontbrekende kolommen valt de
    # query terug op een minimale set zodat export blijft werken.
    rich_select = (
        "id, company_name, domain, email, email_status, phone, city, sector, "
        "archetype, archetype_confidence, score, fit_score, data_quality_score, "
        "personalization_potential, reachability_score, website_score, "
        "google_rating, google_review_count, latest_review_date, "
        "has_instagram, has_online_booking, meta_ads_active, "
        "kvk_number, kvk_sbi_code, contact_first_name, contact_last_name, "
        "manual_status_override, recontact_after, "
        "imported_source, imported_at, created_at, updated_at"
    )
    minimal_select = (
        "id, company_name, domain, email, email_status, phone, city, sector, "
        "score, google_rating, google_review_count, "
        "kvk_number, contact_first_name, contact_last_name, created_at"
    )
    leads: list[dict] = []
    try:
        res = (
            db.table("leads").select(rich_select)
            .eq("workspace_id", workspace_id).order("created_at", desc=True).execute()
        )
        leads = res.data or []
    except Exception:
        try:
            res = (
                db.table("leads").select(minimal_select)
                .eq("workspace_id", workspace_id).order("created_at", desc=True).execute()
            )
            leads = res.data or []
        except Exception as e2:
            raise HTTPException(status_code=500, detail=f"export failed: {e2}")

    # Headers afgeleid uit eerste lead om kolom-mismatch te vermijden
    if leads:
        headers = list(leads[0].keys())
    else:
        headers = ["id", "company_name", "domain", "email", "city", "sector", "created_at"]

    def stream():
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)
        for lead in leads:
            writer.writerow({k: lead.get(k, "") for k in headers})
            yield buf.getvalue()
            buf.seek(0); buf.truncate(0)

    filename = f"heatr-leads-{datetime.now(timezone.utc).date().isoformat()}.csv"
    return StreamingResponse(
        stream(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _auth_mode_summary() -> dict:
    """Welke auth-paden zijn geconfigureerd. Booleans only — geen secrets.

    Bewust unauthenticated beschikbaar (via /healthz): als de auth-config
    stuk is, is /health/startup zelf onbereikbaar (vereist auth) en is dit
    de enige manier om de misconfiguratie te zien zonder server-logs.
    """
    service_key = os.getenv("HEATR_API_KEY", "")
    jwt_secret = os.getenv("SUPABASE_JWT_SECRET", "")
    legacy = os.getenv("LEGACY_DEV_TOKEN_ALLOWED", "false").lower() == "true"
    modes = []
    if service_key and len(service_key) >= 32:
        modes.append("service_key")
    if jwt_secret:
        modes.append("supabase_jwt")
    if legacy:
        modes.append("legacy_dev_token")
    return {
        "service_key_configured": bool(service_key) and len(service_key) >= 32,
        "supabase_jwt_configured": bool(jwt_secret),
        "legacy_dev_token_allowed": legacy,
        "active_modes": modes,
        "warning": (
            "GEEN enkel auth-pad geconfigureerd — alle requests krijgen 401. "
            "Zet HEATR_API_KEY (service) of SUPABASE_JWT_SECRET (browser), of "
            "tijdelijk LEGACY_DEV_TOKEN_ALLOWED=true tijdens frontend-cutover."
        ) if not modes else (
            "legacy_dev_token staat AAN — elke Bearer-token wordt geaccepteerd. "
            "Uitschakelen zodra frontend-next Supabase JWT gebruikt."
        ) if legacy else None,
    }


@app.get("/healthz")
async def healthz() -> dict:
    """Externe uptime monitoring endpoint. Geen auth, geen DB-call.

    Voor UptimeRobot / BetterUptime / Statuspage. Returnt timestamp om
    cache-busting niet nodig te maken op caller-side. Bevat auth-mode
    booleans zodat een kapotte auth-config diagnosticeerbaar is zonder
    ingelogde sessie (zie _auth_mode_summary).
    """
    return {
        "status": "ok",
        "service": "heatr-api",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "auth": _auth_mode_summary(),
    }


@app.get("/admin/missing-field-counts")
async def admin_missing_field_counts(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Hoeveel leads missen welk veld — voor re-enqueue UI dropdown.

    Levert quick counts zodat de admin zelf prioriteit kan kiezen
    (bv. "archetype heeft 855 missing, treatment_focus 812").
    """
    fields = [
        "archetype", "treatment_focus", "company_summary", "kvk_number",
        "latest_review_date", "domain_age_years", "personalized_opener",
        "contact_first_name",
    ]
    counts: dict[str, int | None] = {}
    for f in fields:
        try:
            res = (
                db.table("leads")
                .select("id", count="exact")
                .eq("workspace_id", workspace_id)
                .is_(f, "null")
                .limit(1)
                .execute()
            )
            counts[f] = res.count or 0
        except Exception:
            counts[f] = None  # kolom bestaat niet of query faalde
    return {"missing_field_counts": counts}


class ReEnqueueRequest(BaseModel):
    """Re-enqueue criteria — alle leeg = match alle leads in workspace."""
    missing_field: str | None = "archetype"  # standaard: leads zonder archetype
    sector: str | None = None
    status_in: list[str] | None = None  # bv. ['discovered', 'enriched']
    exclude_status: list[str] | None = ["unsubscribed", "forgotten"]
    enrichment_types: list[str] | None = None  # None = alle default steps
    priority: int = 5
    dry_run: bool = True   # default veilig: toon match-count, geen writes
    limit: int = 500       # safety cap
    confirm_phrase: str | None = None  # moet "ENQUEUE" zijn voor execute


@app.post("/admin/re-enqueue-stale-leads")
async def admin_re_enqueue_stale_leads(
    body: ReEnqueueRequest,
    workspace_id: str = Depends(get_workspace),  # browser ok, type-to-confirm in UI
    db: Client = Depends(get_supabase),
) -> dict:
    """Bulk-enqueue leads die nog niet (volledig) door de pipeline zijn geweest.

    Use-cases:
      - Migratie 014 net toegepast → leads zonder archetype re-enrichten
      - Worker is dagen offline geweest → alle stale-leads opnieuw doen
      - Specifieke step toevoegen voor sub-segment

    UX-veiligheid: default dry_run=True. Voor execute moet body.confirm_phrase
    exact "ENQUEUE" zijn — vangt accidental dubbelklikken / per-ongeluk-POST.
    Cost-guard (€20/maand) is de uiteindelijke veiligheidsnet.
    """
    # Type-to-confirm guard — alleen bij echte execute
    if not body.dry_run and body.confirm_phrase != "ENQUEUE":
        raise HTTPException(
            status_code=400,
            detail="Niet uitgevoerd: voor execute moet confirm_phrase exact 'ENQUEUE' zijn (case-sensitive). Beschermt tegen accidental triggers.",
        )
    # 1. Build filter-query
    try:
        q = db.table("leads").select(
            "id, company_name, sector, status, archetype, email_status, "
            "kvk_number, score, created_at"
        ).eq("workspace_id", workspace_id)

        if body.missing_field:
            q = q.is_(body.missing_field, "null")
        if body.sector:
            q = q.eq("sector", body.sector)
        if body.status_in:
            q = q.in_("status", body.status_in)
        if body.exclude_status:
            for s in body.exclude_status:
                q = q.neq("status", s)

        q = q.order("created_at", desc=True).limit(min(body.limit, 2000))
        res = q.execute()
        candidates = res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"candidate query failed: {e}")

    if not candidates:
        return {
            "matched_count": 0,
            "enqueued_count": 0,
            "dry_run": body.dry_run,
            "sample": [],
            "message": "Geen leads matchten de filter-criteria",
        }

    # 2. Skip leads die AL een pending/running job hebben
    existing_active: set[str] = set()
    try:
        lead_ids = [c["id"] for c in candidates]
        active = (
            db.table("enrichment_jobs")
            .select("lead_id")
            .in_("lead_id", lead_ids)
            .in_("status", ["pending", "running"])
            .execute()
        )
        existing_active = {row["lead_id"] for row in (active.data or [])}
    except Exception:
        pass

    fresh = [c for c in candidates if c["id"] not in existing_active]

    sample = [
        {"id": c["id"], "company_name": c.get("company_name"), "sector": c.get("sector")}
        for c in fresh[:10]
    ]

    if body.dry_run:
        return {
            "matched_count": len(candidates),
            "fresh_count": len(fresh),
            "skipped_already_in_queue": len(existing_active),
            "dry_run": True,
            "sample": sample,
            "message": f"Dry-run: {len(fresh)} leads zouden geënqueued worden. Roep nogmaals met dry_run=false om uit te voeren.",
        }

    # 3. Daadwerkelijk enqueue
    from job_queue.enrichment_queue import queue_lead_for_enrichment

    enqueued = 0
    failures: list[dict] = []
    for c in fresh:
        try:
            job_id = await queue_lead_for_enrichment(
                lead_id=c["id"],
                workspace_id=workspace_id,
                priority=body.priority,
                enrichment_types=body.enrichment_types,
                supabase_client=db,
            )
            if job_id:
                enqueued += 1
        except Exception as e:
            failures.append({"lead_id": c["id"], "error": str(e)[:200]})

    return {
        "matched_count": len(candidates),
        "fresh_count": len(fresh),
        "skipped_already_in_queue": len(existing_active),
        "enqueued_count": enqueued,
        "failed_count": len(failures),
        "first_failures": failures[:5],
        "dry_run": False,
        "sample": sample,
        "message": f"{enqueued} jobs aangemaakt. Worker pakt ze op vanaf eerstvolgende cyclus.",
    }


@app.get("/analytics/email-status-breakdown")
async def analytics_email_status_breakdown(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Diagnose-endpoint voor de email-verifier pipeline.

    Wat normaal stuk gaat: SMTP-verifier kan niet draaien (port 25 geblokkeerd),
    catchall-detectie wordt te aggressief, of de step is helemaal niet
    aangeroepen. Deze endpoint maakt zichtbaar in welk emmer leads belanden.

    Buckets:
      - verified / valid
      - catchall (mailserver accepteert alles)
      - not_found (SMTP zegt: bestaat niet)
      - bounced (Warmr meldde bounce)
      - unsubscribed
      - pending (nog niet gecontroleerd)
      - role_email (info@/contact@/etc — niet per persoon verifieerbaar)
      - missing (geen email-veld)
      - other / null

    Per bucket: count + voorbeelden + percentage.
    """
    try:
        leads_res = (
            db.table("leads")
            .select("id, email, email_status, sector, archetype")
            .eq("workspace_id", workspace_id)
            .execute()
        )
        leads = leads_res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"breakdown fetch failed: {e}")

    if not leads:
        return {"total": 0, "buckets": {}}

    role_prefixes = ("info@", "contact@", "hallo@", "praktijk@", "kliniek@", "receptie@", "secretariaat@")

    buckets: dict[str, dict] = {}
    by_sector: dict[str, dict[str, int]] = {}
    by_archetype: dict[str, dict[str, int]] = {}

    for lead in leads:
        email = (lead.get("email") or "").lower().strip()
        status = (lead.get("email_status") or "").lower().strip()

        if not email:
            bucket = "missing"
        elif status in ("verified", "valid"):
            bucket = "verified"
        elif status == "catchall":
            bucket = "catchall"
        elif status == "not_found":
            bucket = "not_found"
        elif status == "bounced":
            bucket = "bounced"
        elif status == "unsubscribed":
            bucket = "unsubscribed"
        elif status == "pending":
            bucket = "pending"
        elif email.startswith(role_prefixes):
            bucket = "role_email_unverified"
        elif not status:
            bucket = "no_status_set"
        else:
            bucket = f"other:{status}"

        if bucket not in buckets:
            buckets[bucket] = {"count": 0, "examples": []}
        buckets[bucket]["count"] += 1
        if len(buckets[bucket]["examples"]) < 3 and email:
            buckets[bucket]["examples"].append(email)

        # Cross-tab: bucket × sector / archetype
        sec = lead.get("sector") or "(unknown)"
        arc = lead.get("archetype") or "(unknown)"
        by_sector.setdefault(sec, {})[bucket] = by_sector.setdefault(sec, {}).get(bucket, 0) + 1
        by_archetype.setdefault(arc, {})[bucket] = by_archetype.setdefault(arc, {}).get(bucket, 0) + 1

    total = len(leads)
    bucket_list = sorted(
        [{"bucket": k, "count": v["count"], "pct": round(v["count"] / total * 100, 1), "examples": v["examples"]}
         for k, v in buckets.items()],
        key=lambda x: -x["count"],
    )

    # Diagnostic hints — wat zegt deze breakdown over je pipeline?
    hints = []
    sendable = (buckets.get("verified", {}).get("count", 0) +
                buckets.get("role_email_unverified", {}).get("count", 0))
    if sendable == 0:
        hints.append("CRITICAL: 0 sendable emails. Check of email_verifier daadwerkelijk draait + SMTP-port 25 toegankelijk is.")
    if buckets.get("no_status_set", {}).get("count", 0) > total * 0.5:
        hints.append("Veel leads zonder email_status — verifier-step wordt overgeslagen of crashed silent. Check enrichment_jobs voor failures.")
    if buckets.get("catchall", {}).get("count", 0) > total * 0.3:
        hints.append("Hoge catchall-rate — overweegging extra check via Hunter.io of skip catchall in send-flow.")
    if buckets.get("not_found", {}).get("count", 0) > total * 0.4:
        hints.append("Veel not_found — overweeg de email-waterval te tunen (meer pattern-tries, of Google Search fallback agressiever).")

    return {
        "total": total,
        "buckets": bucket_list,
        "by_sector": by_sector,
        "by_archetype": by_archetype,
        "diagnostic_hints": hints,
    }


@app.get("/analytics/enrichment-coverage")
async def analytics_enrichment_coverage(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Per enrichment-stap: welk percentage van leads heeft de output?

    Mapt elke pipeline-step naar een veld dat zou moeten ingevuld zijn als de
    step succesvol gedraaid heeft. Toont waar de pipeline silent breaks
    veroorzaakt — bv. 95% leads enrichted maar slechts 12% archetype-tagged
    onthult dat archetype-classifier silent fault.
    """
    try:
        leads_res = (
            db.table("leads")
            .select(
                "id, email, email_status, kvk_number, kvk_sbi_code, "
                "company_summary, personalized_opener, archetype, "
                "treatment_focus, has_instagram, latest_review_date, "
                "website_age_years, score, contact_first_name, "
                "review_recency_checked_at, archetype_classified_at"
            )
            .eq("workspace_id", workspace_id)
            .execute()
        )
        leads = leads_res.data or []
    except Exception as e:
        # Fallback minimal als kolommen ontbreken
        try:
            leads_res = (
                db.table("leads")
                .select("id, email, email_status, kvk_number, score, contact_first_name, archetype")
                .eq("workspace_id", workspace_id)
                .execute()
            )
            leads = leads_res.data or []
        except Exception as e2:
            raise HTTPException(status_code=500, detail=f"coverage fetch failed: {e2}")

    total = len(leads)
    if total == 0:
        return {"total": 0, "steps": []}

    # Mapping: step → predicate(lead) → True als step succesvol uitgevoerd
    # NB: kvk_lookup/kvk_sbi zijn opt-in (betaalde API) en worden alleen meegerekend
    # als KVK_API_KEY env-var gezet is. Zonder key wordt deze step bewust geskipt.
    import os as _os
    kvk_enabled = bool(_os.getenv("KVK_API_KEY"))

    step_predicates = {
        "email_waterfall": lambda l: bool(l.get("email")),
        "email_verified": lambda l: (l.get("email_status") or "").lower() in ("verified", "valid"),
        "company_enrichment": lambda l: bool(l.get("company_summary")),
        "personalized_opener": lambda l: bool(l.get("personalized_opener")),
        "owner_extract": lambda l: bool(l.get("contact_first_name")),
        "treatment_classifier": lambda l: bool(l.get("treatment_focus")),
        "review_recency": lambda l: bool(l.get("review_recency_checked_at") or l.get("latest_review_date")),
        "archetype_classifier": lambda l: bool(l.get("archetype")),
        "domain_age": lambda l: l.get("website_age_years") is not None,
        "scoring": lambda l: l.get("score") is not None and (l.get("score") or 0) > 0,
    }
    if kvk_enabled:
        step_predicates["kvk_lookup"] = lambda l: bool(l.get("kvk_number"))
        step_predicates["kvk_sbi"] = lambda l: bool(l.get("kvk_sbi_code"))

    steps_out = []
    for step_name, pred in step_predicates.items():
        try:
            done_count = sum(1 for l in leads if pred(l))
        except (KeyError, TypeError):
            done_count = 0
        steps_out.append({
            "step": step_name,
            "completed_count": done_count,
            "missing_count": total - done_count,
            "coverage_pct": round(done_count / total * 100, 1),
        })

    # Sort: lowest coverage first (= grootste pijn)
    steps_out.sort(key=lambda x: x["coverage_pct"])

    # Diagnostic hints
    hints = []
    for step in steps_out:
        if step["coverage_pct"] < 20 and step["step"] not in ("email_verified",):
            hints.append(f"{step['step']}: slechts {step['coverage_pct']}% coverage — step draait niet of crasht silent.")

    return {
        "total": total,
        "steps": steps_out,
        "diagnostic_hints": hints,
    }


@app.get("/analytics/ops-health")
async def analytics_ops_health(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Operationele pijplijn-gezondheid in één blik: queue-diepte, vastgelopen
    jobs, worker-heartbeat en expliciete STALL-detectie (utils.pipeline_ops).

    Returns {status: healthy|degraded|stalled, alerts: [...], snapshot: {...}}.
    Complementair aan /analytics/queue-health (retry/step-detail) en
    /analytics/pipeline (conversie). Cron-baar als stall-alarm.
    """
    from utils.pipeline_ops import ops_health
    return await ops_health(workspace_id, db)


@app.get("/analytics/queue-health")
async def analytics_queue_health(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Snapshot van enrichment-queue gezondheid.

    Toont per status hoeveel jobs, plus retry-distributie en welke steps het
    vaakst falen. Voor diagnose van "waarom blijven leads in 'discovered'?".
    """
    from datetime import datetime, timezone, timedelta

    try:
        jobs_res = (
            db.table("enrichment_jobs")
            .select("id, status, current_step, retry_count, error_message, created_at, completed_at")
            .eq("workspace_id", workspace_id)
            .order("created_at", desc=True)
            .limit(2000)
            .execute()
        )
        jobs = jobs_res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"queue fetch failed: {e}")

    now = datetime.now(timezone.utc)
    last_24h = (now - timedelta(hours=24)).isoformat()

    # Counts per status
    status_counts: dict[str, int] = {}
    failed_step_counts: dict[str, int] = {}
    retry_dist: dict[int, int] = {}
    recent_24h_count = 0
    total_completed = 0
    total_failed = 0

    for job in jobs:
        status = job.get("status") or "(unknown)"
        status_counts[status] = status_counts.get(status, 0) + 1
        if job.get("created_at", "") > last_24h:
            recent_24h_count += 1
        if status == "completed":
            total_completed += 1
        elif status == "failed":
            total_failed += 1
            step = job.get("current_step") or "(unknown)"
            failed_step_counts[str(step)] = failed_step_counts.get(str(step), 0) + 1
        rc = int(job.get("retry_count") or 0)
        retry_dist[rc] = retry_dist.get(rc, 0) + 1

    # Top failing steps
    top_failures = sorted(
        [{"step": k, "fail_count": v} for k, v in failed_step_counts.items()],
        key=lambda x: -x["fail_count"],
    )[:5]

    # Health verdict
    pending = status_counts.get("pending", 0)
    running = status_counts.get("running", 0)
    completion_rate = round(total_completed / max(len(jobs), 1) * 100, 1)
    fail_rate = round(total_failed / max(len(jobs), 1) * 100, 1)

    hints = []
    if pending > 100 and recent_24h_count > 0:
        # Extrapolate: at current pace, hoeveel uur duurt het?
        per_hour = recent_24h_count / 24
        if per_hour > 0:
            hours_to_drain = round(pending / per_hour, 1)
            hints.append(f"Queue drain estimate: {hours_to_drain}u bij huidige snelheid ({per_hour:.1f}/u).")
    if pending > 0 and running == 0:
        hints.append("Pending jobs maar geen running — worker mogelijk offline. Check 'caffeinate' proces.")
    if fail_rate > 20:
        hints.append(f"Fail-rate {fail_rate}% — bekijk top failing steps voor root-cause.")

    return {
        "total_jobs": len(jobs),
        "recent_24h": recent_24h_count,
        "completion_rate_pct": completion_rate,
        "fail_rate_pct": fail_rate,
        "status_counts": status_counts,
        "retry_distribution": dict(sorted(retry_dist.items())),
        "top_failing_steps": top_failures,
        "diagnostic_hints": hints,
    }


@app.get("/analytics/scraping-live")
async def analytics_scraping_live(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Live view of all scraping jobs + recent companies_raw rows.

    Built for frontend polling at ~3-5s interval. Returns a compact JSON
    snapshot: per-status counters, currently-running jobs, recent results,
    per (sector, city) aggregates van vandaag.
    """
    from datetime import datetime, timezone, timedelta

    since = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    def _safe_count(status: str) -> int:
        try:
            r = (
                db.table("scraping_jobs")
                .select("id", count="exact")
                .eq("workspace_id", workspace_id)
                .eq("status", status)
                .execute()
            )
            return r.count or 0
        except Exception:
            return 0

    pending = _safe_count("pending")
    running_res = (
        db.table("scraping_jobs")
        .select("id, sector, city, search_query, source, created_at")
        .eq("workspace_id", workspace_id)
        .eq("status", "running")
        .order("created_at", desc=False)
        .limit(30)
        .execute()
    )
    running_rows = running_res.data or []

    completed_res = (
        db.table("scraping_jobs")
        .select("id, sector, city, search_query, total_found, total_new, created_at")
        .eq("workspace_id", workspace_id)
        .eq("status", "completed")
        .gte("created_at", since)
        .order("created_at", desc=True)
        .limit(20)
        .execute()
    )
    completed_rows = completed_res.data or []

    failed_res = (
        db.table("scraping_jobs")
        .select("id, sector, city, search_query, error_message, created_at")
        .eq("workspace_id", workspace_id)
        .eq("status", "failed")
        .gte("created_at", since)
        .order("created_at", desc=True)
        .limit(10)
        .execute()
    )
    failed_rows = failed_res.data or []

    # Recent companies_raw (newest 30)
    try:
        cr_res = (
            db.table("companies_raw")
            .select("id, company_name, city, sector, domain, google_rating, google_review_count, created_at")
            .eq("workspace_id", workspace_id)
            .order("created_at", desc=True)
            .limit(30)
            .execute()
        )
        recent_companies = cr_res.data or []
    except Exception:
        recent_companies = []

    # Per (sector, city) aggregate of companies_raw added in last 24h
    try:
        cr_recent_res = (
            db.table("companies_raw")
            .select("sector, city")
            .eq("workspace_id", workspace_id)
            .gte("created_at", since)
            .execute()
        )
        cr_recent = cr_recent_res.data or []
    except Exception:
        cr_recent = []

    agg: dict[tuple, int] = {}
    for r in cr_recent:
        key = (r.get("sector") or "?", r.get("city") or "?")
        agg[key] = agg.get(key, 0) + 1
    agg_list = [
        {"sector": s, "city": c, "count": n}
        for (s, c), n in sorted(agg.items(), key=lambda kv: -kv[1])
    ]

    return {
        "counters": {
            "pending": pending,
            "running": len(running_rows),
            "completed_24h": len(completed_rows),
            "failed_24h": len(failed_rows),
            "total_companies_24h": len(cr_recent),
        },
        "running_jobs": running_rows,
        "completed_jobs": completed_rows,
        "failed_jobs": failed_rows,
        "recent_companies": recent_companies,
        "by_sector_city": agg_list[:40],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# SECTORS
# =============================================================================

@app.get("/sectors")
async def list_sectors() -> dict:
    from config.sectors import list_sectors as _list_sectors
    return {"sectors": _list_sectors()}


@app.get("/config/sendability")
async def get_sendability_config() -> dict:
    """Sendability-statusdefinitie voor de UI — één bron van waarheid (I5).

    De frontend gebruikt dit voor risico-badges i.p.v. een eigen kopie van
    de statuslijsten (Sprint 1-audit §8: definitie-drift).
    """
    from utils.email_sendability import sendability_config
    return sendability_config()


# =============================================================================
# WEBHOOKS — Warmr
# =============================================================================

def _mark_retarget_replied(db: Client, workspace_id: str, heatr_lead_id: str) -> None:
    """Zet een lopende check-up retarget-flow op 'replied' bij een reply.

    Een reply na een rapport-send of retarget betekent dat de flow z'n werk deed:
    stop 'm. Fail-soft — de lead-level crm_stage is al gezet (eerste linie).
    """
    try:
        db.table("call_records").update({"retarget_status": "replied", "updated_at": _now_iso()}) \
            .eq("workspace_id", workspace_id).eq("lead_id", heatr_lead_id) \
            .in_("retarget_status", ["scheduled", "exhausted"]).execute()
    except Exception as e:
        logger.warning("_mark_retarget_replied faalde (lead=%s): %s", heatr_lead_id, e)


def _register_suppression_from_webhook(
    db: Client,
    heatr_lead_id: str,
    workspace_id: str,
    *,
    suppression_type: str,
    event_type: str,
    fallback_email: str | None = None,
) -> None:
    """Registreer een webhook-event (bounce/unsubscribe) in de platformbrede
    suppressielijst (fase 2 PR 7). Fail-soft met luide log: de lead-level
    status is al gezet (eerste linie); een gemiste suppressie-write mag de
    webhook-verwerking niet laten crashen, maar moet wél zichtbaar zijn.
    """
    from utils.suppression import add_suppression
    email = fallback_email
    try:
        res = (db.table("leads").select("email")
               .eq("id", heatr_lead_id).eq("workspace_id", workspace_id)
               .maybe_single().execute())
        email = (res.data or {}).get("email") or fallback_email
    except Exception as e:
        logger.warning("webhooks/warmr: email-lookup voor suppressie faalde (lead=%s): %s",
                       heatr_lead_id, e)
    outcome = add_suppression(
        db, email=email, suppression_type=suppression_type,
        source="warmr_webhook", source_workspace_id=workspace_id,
        lead_id=heatr_lead_id, reason=event_type,
    )
    if not outcome.get("ok"):
        logger.error(
            "webhooks/warmr: SUPPRESSIE NIET GEREGISTREERD (lead=%s type=%s): %s — "
            "alleen de per-lead status blokkeert nu; cross-workspace dekking mist.",
            heatr_lead_id, suppression_type, outcome.get("error"),
        )


@app.post("/webhooks/warmr")
async def warmr_webhook(
    request: Request,
    db: Client = Depends(get_supabase),
) -> dict:
    import hashlib, hmac
    secret = os.getenv("WARMR_WEBHOOK_SECRET", "")
    sig = request.headers.get("X-Warmr-Signature", "")
    body_bytes = await request.body()

    # FAIL-CLOSED (recovery-fix): dit endpoint muteert lead-status en stopt
    # sequences. Voorheen werd de HMAC-check overgeslagen als het secret leeg
    # was → iedereen kon events posten. Zonder geconfigureerd secret weigeren
    # we nu (luid), en een ongeldige handtekening geeft 401.
    if not secret:
        logger.error("webhooks/warmr geweigerd: WARMR_WEBHOOK_SECRET niet geconfigureerd (fail-closed).")
        raise HTTPException(status_code=503, detail="Webhook secret not configured")
    expected = hmac.new(secret.encode(), body_bytes, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    payload = await request.json()
    event_type = payload.get("event")
    heatr_lead_id = payload.get("custom_fields", {}).get("heatr_lead_id")
    workspace_id = payload.get("custom_fields", {}).get("workspace_id", DEFAULT_WORKSPACE)
    campaign_ref = payload.get("campaign_id") or payload.get("custom_fields", {}).get("campaign_id")

    # Fase 3 PR 10 — eventledger vóór élk side-effect (audit v2 scenario 3):
    # een geredeliverd event (Warmr = at-least-once) botst op de UNIQUE en
    # stopt hier, vóór dubbele inbox-rijen/crm_tasks/statuswissels.
    from utils.webhook_ledger import (
        LEAD_BOUND_EVENTS, finalize_event, make_event_id, record_event,
    )
    event_id = make_event_id(payload)
    ledger_state = record_event(
        db, event_id=event_id, workspace_id=workspace_id,
        event_type=event_type, payload=payload,
        lead_id=heatr_lead_id, campaign_id=campaign_ref,
    )
    if ledger_state == "duplicate":
        return {"ok": True, "duplicate": True, "event_id": event_id}

    # Lead-resolutie (audit v2 scenario 14): lead-gebonden events zonder
    # bekende lead gaan naar dead_letter i.p.v. stil {"ok": true}. Fallback-
    # correlatie op Warmr's eigen lead-id vóór we opgeven.
    if event_type in LEAD_BOUND_EVENTS:
        lead_known = False
        if heatr_lead_id:
            try:
                _lr = (db.table("leads").select("id").eq("id", heatr_lead_id)
                       .eq("workspace_id", workspace_id).maybe_single().execute())
                lead_known = bool(_lr.data)
            except Exception as e:
                logger.warning("webhooks/warmr: lead-lookup faalde (%s): %s", heatr_lead_id, e)
        if not lead_known:
            warmr_ref = payload.get("lead_id") or payload.get("warmr_lead_id")
            if warmr_ref:
                try:
                    _wr = (db.table("leads").select("id").eq("warmr_lead_id", str(warmr_ref))
                           .eq("workspace_id", workspace_id).maybe_single().execute())
                    if _wr.data:
                        heatr_lead_id = _wr.data["id"]
                        lead_known = True
                        logger.info("webhooks/warmr: lead gecorreleerd via warmr_lead_id=%s → %s",
                                    warmr_ref, heatr_lead_id)
                except Exception as e:
                    logger.warning("webhooks/warmr: warmr-correlatie faalde (%s): %s", warmr_ref, e)
        if not lead_known:
            detail = (f"onbekende lead (heatr_lead_id={heatr_lead_id!r}, "
                      f"warmr_ref={payload.get('lead_id') or payload.get('warmr_lead_id')!r})")
            finalize_event(db, event_id, "dead_letter", error=detail)
            logger.error("webhooks/warmr: DEAD-LETTER %s event=%s — %s",
                         event_type, event_id, detail)
            return {"ok": False, "reason": "unknown_lead", "dead_letter": True,
                    "event_id": event_id}

    audit_logged = True
    if heatr_lead_id:
        # Log all events to reply_inbox for audit.
        # RECOVERY-FIX (Patch 7): kolommen conform het echte schema
        # (heatr_reply_inbox, migratie 004/011): `body` i.p.v. `body_text`,
        # GEEN `event_type`/`from_name` (bestaan niet) → voorheen faalde de
        # insert stil met PGRST204 en ging de hele inbound-reply-audittrail
        # verloren, terwijl de webhook tóch {"ok": true} teruggaf.
        # Deterministische classificatie (2026-07-14): voor events waarvan de
        # aard al vaststaat hoeft geen Claude-classifier te draaien — de
        # Afgemeld-tab in de inbox werkt dan direct, en bounce-audit-rijen
        # vervuilen de unclassified-wachtrij niet.
        _event_classification: dict[str, tuple[str, str]] = {
            "unsubscribed": ("unsubscribe_request", "Afmelding via Warmr-event (deterministisch)"),
            "lead.unsubscribed": ("unsubscribe_request", "Afmelding via Warmr-event (deterministisch)"),
            "bounced": ("other", "Bounce-event (geen reply)"),
            "lead.bounced": ("other", "Bounce-event (geen reply)"),
        }
        _cls, _cls_summary = _event_classification.get(event_type, (None, None))
        try:
            db.table("reply_inbox").insert({
                "workspace_id": workspace_id,
                "lead_id": heatr_lead_id,
                "from_email": payload.get("from_email"),
                "subject": payload.get("subject") or event_type,
                "body": payload.get("body_text") or payload.get("body"),
                "body_html": payload.get("body_html"),
                "received_at": _now_iso(),
                "classification": _cls,
                "classification_summary": _cls_summary,
            }).execute()
        except Exception as exc:
            audit_logged = False
            logger.error("webhooks/warmr: reply_inbox-insert MISLUKT voor lead %s: %s", heatr_lead_id, exc)

        # v1.0 spec: ELKE reply / bounce / unsubscribe stopt de hele sequence direct.
        # Re-entry op recontact-cooldown via lead.next_contact_after.
        from campaigns.sequence_engine import stop_all_sequences_for_lead

        # Route events to appropriate handlers
        if event_type in ("interested", "lead.interested"):
            db.table("leads").update({"crm_stage": "gereageerd"}).eq(
                "id", heatr_lead_id).eq("workspace_id", workspace_id).execute()
            _mark_retarget_replied(db, workspace_id, heatr_lead_id)
            stopped = await stop_all_sequences_for_lead(heatr_lead_id, workspace_id, db)
            _insert_timeline_event(
                db, workspace_id, heatr_lead_id, "reply_received",
                f"Reply ontvangen: geïnteresseerd — {stopped} sequence(s) gestopt",
            )

        elif event_type in ("replied", "lead.replied"):
            # 'gereageerd' i.p.v. het oude 'beantwoord' (2026-07-14): de
            # CRM-frontend (STAGES-enum) kent 'beantwoord' niet, waardoor
            # beantwoorders in een onzichtbare kolom belandden.
            db.table("leads").update({"crm_stage": "gereageerd"}).eq(
                "id", heatr_lead_id).eq("workspace_id", workspace_id).execute()
            _mark_retarget_replied(db, workspace_id, heatr_lead_id)
            stopped = await stop_all_sequences_for_lead(heatr_lead_id, workspace_id, db)
            _insert_timeline_event(
                db, workspace_id, heatr_lead_id, "reply_received",
                f"Reply ontvangen — {stopped} sequence(s) gestopt",
            )

        elif event_type in ("bounced", "lead.bounced"):
            # Fase 2-hotfix (audit v2 P0-2): óók leads.status zetten — de
            # centrale compliance_check leest ALLEEN status, dus alleen
            # email_status schrijven liet een gebouncede lead launchbaar
            # (/campaigns/launch heeft geen email_status-gate). Workspace-
            # scoping toegevoegd op deze mutatie (audit v2 P1-1).
            db.table("leads").update({
                "email_status": "bounced",
                "status": "bounced",
            }).eq("id", heatr_lead_id).eq("workspace_id", workspace_id).execute()
            _register_suppression_from_webhook(
                db, heatr_lead_id, workspace_id,
                suppression_type="hard_bounce", event_type=event_type,
                fallback_email=payload.get("from_email") or payload.get("email"),
            )
            stopped = await stop_all_sequences_for_lead(heatr_lead_id, workspace_id, db)
            _insert_timeline_event(
                db, workspace_id, heatr_lead_id, "bounced",
                f"Email gebounced — {stopped} sequence(s) gestopt",
            )

        elif event_type in ("unsubscribed", "lead.unsubscribed"):
            # Fase 2-hotfix: status='unsubscribed' is de kolom die
            # compliance_check blokkeert — email_status/crm_stage alleen
            # was het suppression-lek uit audit v2 P0-2.
            # crm_stage 'verloren' i.p.v. het oude 'afgesloten' (2026-07-14):
            # 'afgesloten' bestaat niet in de CRM-frontend-enum — afmelders
            # werden onzichtbaar. status='unsubscribed' + suppressie blijven
            # de autoritaire blokkade; crm_stage is alleen board-plaatsing.
            db.table("leads").update({
                "email_status": "unsubscribed",
                "status": "unsubscribed",
                "crm_stage": "verloren",
            }).eq("id", heatr_lead_id).eq("workspace_id", workspace_id).execute()
            _register_suppression_from_webhook(
                db, heatr_lead_id, workspace_id,
                suppression_type="unsubscribe", event_type=event_type,
                fallback_email=payload.get("from_email") or payload.get("email"),
            )
            stopped = await stop_all_sequences_for_lead(heatr_lead_id, workspace_id, db)
            _insert_timeline_event(
                db, workspace_id, heatr_lead_id, "unsubscribed",
                f"Lead heeft zich uitgeschreven — {stopped} sequence(s) gestopt",
            )

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
                # Fase 3 PR 9 (ADR-001): enrollment-closure via één helper met
                # (a) terminal-guard — een laat campaign.completed overschrijft
                # nooit een eerdere replied/unsubscribed/bounced (scenario 4);
                # (b) campaign_id-scoping als de payload die levert — een
                # no_response van campagne X vervuilt campagne Y niet meer;
                # (c) is_active=False + sent_at-backfill → F7-fix: de rij
                # verlaat de actieve set en wordt het 90d-cooldown-anker.
                from campaigns.enrollment import close_campaign_enrollments
                closed = close_campaign_enrollments(
                    db, workspace_id=workspace_id, lead_id=heatr_lead_id,
                    mapped_status=mapped_status,
                    campaign_id=(payload.get("campaign_id")
                                 or payload.get("custom_fields", {}).get("campaign_id")),
                    completed=(event_type == "campaign.completed"),
                )
                logger.info("webhooks/warmr: %d enrollment(s) afgesloten (lead=%s → %s)",
                            closed, heatr_lead_id, mapped_status)
        except Exception as exc:
            audit_logged = False
            logger.error("webhooks/warmr: lead_campaign_history-update MISLUKT voor lead %s: %s", heatr_lead_id, exc)

    # EERLIJKE response (recovery-fix): geen {"ok": true} als de audit-inserts
    # faalden. Zo blijft een stille PGRST204 niet langer als succes gerapporteerd.
    finalize_event(
        db, event_id,
        "processed" if audit_logged else "error",
        error=None if audit_logged else "één of meer audit-writes faalden (zie logs)",
    )
    return {"ok": audit_logged, "audit_logged": audit_logged, "event_id": event_id}


# =============================================================================
# ZOOM WEBHOOK (gebouwd, UIT tot ZOOM_WEBHOOK_SECRET gezet is)
# =============================================================================

@app.post("/webhooks/zoom")
async def zoom_webhook(request: Request, background: BackgroundTasks) -> dict:
    """Ontvang Zoom recording/transcript-events -> gesprekrecord.

    UIT tot ZOOM_WEBHOOK_SECRET gezet is (dan 404). Geen X-API-Key/JWT: Zoom
    authenticeert met HMAC (x-zm-signature), fail-closed geverifieerd. De URL-
    validatie-handshake wordt vóór de HMAC-check afgehandeld. Zware verwerking
    (VTT-download, matching, insert) draait als background-task zodat we binnen
    Zoom's 3s-window 200 teruggeven.
    """
    from calls.zoom_webhook import (
        zoom_enabled, verify_signature, url_validation_response, process_recording,
    )
    if not zoom_enabled():
        raise HTTPException(status_code=404, detail="Zoom-webhook staat uit (ZOOM_WEBHOOK_SECRET niet gezet).")

    raw = await request.body()
    try:
        payload = json.loads(raw or b"{}")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="ongeldige JSON")

    # URL-validatie-handshake (Zoom stuurt dit bij het instellen van de endpoint).
    if payload.get("event") == "endpoint.url_validation":
        plain = ((payload.get("payload") or {}).get("plainToken")) or ""
        return url_validation_response(plain)

    # HMAC — fail-closed.
    ts = request.headers.get("x-zm-request-timestamp", "")
    sig = request.headers.get("x-zm-signature", "")
    if not verify_signature(ts, raw, sig):
        raise HTTPException(status_code=401, detail="ongeldige Zoom-signature")

    event = payload.get("event") or ""
    if event in ("recording.transcript_completed", "recording.completed"):
        workspace_id = os.getenv("DEFAULT_WORKSPACE_ID", "aerys")
        background.add_task(process_recording, payload.get("payload") or {}, workspace_id)
        return {"ok": True, "queued": True, "event": event}
    return {"ok": True, "ignored": event}


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

    q = db.table("crm_tasks").select("*").eq("workspace_id", workspace_id)

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

    q = q.order("due_date", desc=False)  # ASC → NULLs sorteren in postgres sowieso als laatste
    res = q.execute()
    tasks = res.data or []
    # Geen FK crm_tasks→leads in de schema-cache → lead-context via batch-query erbij hangen.
    lead_ids = list({t.get("lead_id") for t in tasks if t.get("lead_id")})
    if lead_ids:
        lr = (
            db.table("leads")
            .select("id, company_name, city, sector, crm_stage")
            .eq("workspace_id", workspace_id)
            .in_("id", lead_ids)
            .execute()
        )
        leads_by_id = {l["id"]: l for l in (lr.data or [])}
        for t in tasks:
            t["leads"] = leads_by_id.get(t.get("lead_id"))
    return {"tasks": tasks}


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
    # Geen FK heatr_lead_timeline→leads in de schema-cache → geen PostgREST-embed.
    # Twee-query pattern: timeline-rijen, dan de lead-namen batched per lead_id.
    res = (
        db.table("lead_timeline")
        .select("*")
        .eq("workspace_id", workspace_id)
        .order("created_at", desc=True)
        .limit(20)
        .execute()
    )
    rows = res.data or []
    lead_ids = list({r.get("lead_id") for r in rows if r.get("lead_id")})
    leads_by_id: dict[str, dict] = {}
    if lead_ids:
        lr = (
            db.table("leads")
            .select("id, company_name, city")
            .eq("workspace_id", workspace_id)
            .in_("id", lead_ids)
            .execute()
        )
        leads_by_id = {l["id"]: l for l in (lr.data or [])}
    events = []
    for row in rows:
        lead = leads_by_id.get(row.get("lead_id"), {})
        events.append({**row, "company_name": lead.get("company_name"), "lead_city": lead.get("city")})
    return {"events": events}


@app.get("/timeline/{lead_id}")
async def get_timeline(
    lead_id: str,
    limit: int = 100,
    compact: bool = False,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Lead timeline events.

    Args:
        limit: Max events (default 100, kanban-flip gebruikt 5)
        compact: Als True, filtert op key event-types (email_sent,
                reply_received, sequence_completed, manual_status_override,
                bounced, unsubscribed, stage_changed) en strips metadata.
                Voor kanban-card-flip waar je beknopt overzicht wilt.
    """
    q = (
        db.table("lead_timeline")
        .select("id, event_type, title, created_at, metadata, created_by")
        .eq("lead_id", lead_id)
        .eq("workspace_id", workspace_id)
        .order("created_at", desc=True)
    )
    if compact:
        # Alleen events die echte status-veranderingen markeren
        key_types = [
            "email_sent", "reply_received", "sequence_completed",
            "manual_status_override", "bounced", "unsubscribed",
            "stage_changed", "interested", "review_email_sent",
        ]
        q = q.in_("event_type", key_types)
    res = q.limit(min(limit, 500)).execute()
    events = res.data or []
    if compact:
        # Strip metadata om payload klein te houden voor kanban-flip
        events = [
            {"id": e["id"], "event_type": e["event_type"], "title": e["title"],
             "created_at": e["created_at"]}
            for e in events
        ]
    return {"events": events, "count": len(events)}


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

    # Mark lead as gewonnen — workspace-scoped (fase 4 PR 13, audit v2 P1-1:
    # dit was een id-only mutatie waarmee een caller met andermans lead-UUID
    # die lead op 'gewonnen' kon zetten).
    db.table("leads").update({"crm_stage": "gewonnen"}).eq(
        "id", body.lead_id).eq("workspace_id", workspace_id).execute()
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
        "auth": _auth_mode_summary(),
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
    if not result.get("ok"):
        # Fase 2-fix: een onvolledige erasure mag NOOIT als succes terugkomen
        # (Art. 17). De caller ziet exact welke stap faalde en kan retryen —
        # forget_lead is idempotent.
        raise HTTPException(status_code=500, detail={
            "error": "forget_incomplete",
            **result,
            "hint": "Retry is veilig (idempotent); los eerst de gerapporteerde stap-fout op.",
        })
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
# COMPLIANCE-VLAGGEN (drip-blokkades zichtbaar/afhandelbaar) — migratie 044
# =============================================================================

@app.get("/compliance/flags")
async def list_compliance_flags(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Open (niet-acknowledged) compliance-vlaggen — de fail-closed drip-blokkades.
    Eén open vlag legt de hele drip stil (sequence_engine.assert_no_open_flags)."""
    from utils.compliance_flags import open_flags
    return {"flags": open_flags(db, workspace_id)}


class AcknowledgeFlags(BaseModel):
    flag_ids: list[str]


@app.post("/compliance/flags/acknowledge")
async def acknowledge_compliance_flags(
    body: AcknowledgeFlags,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Markeer vlaggen als afgehandeld (deblokkeert de drip). Review-actie via de UI;
    de echte send blijft achter activate + de kill-switch. Workspace-veilig: alleen
    eigen open vlaggen worden afgehandeld (id's uit andere workspaces zijn no-op)."""
    from utils.compliance_flags import open_flags, acknowledge_flags
    own_ids = {f["id"] for f in open_flags(db, workspace_id)}
    ids = [fid for fid in body.flag_ids if fid in own_ids]
    return {"acknowledged": acknowledge_flags(db, ids, by="ui")}


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


@app.post("/queue/reap-stuck")
async def queue_reap_stuck(
    stuck_minutes: int = 30,
    _ws: str = Depends(require_service_key),
    db: Client = Depends(get_supabase),
) -> dict:
    """Recovery Patch 3: requeue/dead-letter jobs die te lang op 'running' staan.

    Systeembreed onderhoud (geen workspace-scope) — vandaar require_service_key.
    Bedoeld voor een n8n-cron (bv. elke 10 min). Elke reap met stuck>0 is óók
    een worker-liveness-signaal (een gezonde worker laat niets lang op running).
    """
    from utils.queue_reaper import requeue_stuck_jobs
    summary = await requeue_stuck_jobs(db, stuck_minutes=stuck_minutes)
    total_stuck = sum(v.get("stuck", 0) for v in summary.values())
    if total_stuck:
        try:
            from utils.alert_manager import send_alert
            await send_alert(
                "stuck_jobs_reaped",
                f"{total_stuck} stuck job(s) hersteld: {summary}",
                "warning", DEFAULT_WORKSPACE, db,
            )
        except Exception as exc:
            logger.warning("reaper: alert kon niet verstuurd worden: %s", exc)
    return {"reaped": summary, "total_stuck": total_stuck}


# =============================================================================
# DISCOVERY SCHEDULES (automatic recurring scrapes)
# =============================================================================

class ScheduleCreate(BaseModel):
    sector: str
    city: str
    frequency_days: int = 14
    country: str = "NL"
    target_new_leads: int = 20
    max_results: int = 40


@app.get("/discovery-schedules")
async def list_discovery_schedules(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """List all configured recurring scrape schedules."""
    from scrapers.discovery_scheduler import list_schedules
    schedules = await list_schedules(workspace_id, db)
    return {"schedules": schedules}


@app.post("/discovery-schedules")
async def create_discovery_schedule(
    body: ScheduleCreate,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Create a new recurring scrape schedule."""
    from scrapers.discovery_scheduler import create_schedule
    sid = await create_schedule(
        workspace_id=workspace_id,
        sector=body.sector,
        city=body.city,
        frequency_days=body.frequency_days,
        country=body.country,
        target_new_leads=body.target_new_leads,
        max_results=body.max_results,
        supabase_client=db,
    )
    if not sid:
        raise HTTPException(status_code=500, detail="create_schedule_failed")
    return {"schedule_id": sid}


@app.post("/discovery-schedules/{schedule_id}/pause")
async def pause_discovery_schedule(
    schedule_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    from scrapers.discovery_scheduler import pause_schedule
    ok = await pause_schedule(schedule_id, db)
    return {"ok": ok}


@app.delete("/discovery-schedules/{schedule_id}")
async def delete_discovery_schedule(
    schedule_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    from scrapers.discovery_scheduler import delete_schedule
    ok = await delete_schedule(schedule_id, db)
    return {"ok": ok}


@app.post("/discovery-schedules/{schedule_id}/resume")
async def resume_discovery_schedule(
    schedule_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Heractiveer een gepauzeerde schedule (spiegel van /pause)."""
    from scrapers.discovery_scheduler import resume_schedule
    ok = await resume_schedule(schedule_id, db)
    return {"ok": ok}


@app.post("/discovery-schedules/run-due")
async def run_due_discovery_schedules(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Run all schedules that are due. Called by daily cron."""
    from scrapers.discovery_scheduler import run_due_schedules
    result = await run_due_schedules(workspace_id, db)
    return result


# =============================================================================
# RECONTACT SIGNALS (trigger-based recontact)
# =============================================================================

@app.get("/leads/{lead_id}/recontact-signals")
async def get_lead_recontact_signals(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Check if there are new change signals justifying recontact."""
    from scoring.recontact_signals import detect_recontact_signals
    return await detect_recontact_signals(lead_id, workspace_id, db)


@app.post("/leads/{lead_id}/outreach-snapshot")
async def save_lead_outreach_snapshot(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Save baseline for future change-signal detection. Call after campaign completes."""
    from scoring.recontact_signals import save_outreach_snapshot
    await save_outreach_snapshot(lead_id, db)
    return {"ok": True}


# =============================================================================
# REPLY CLASSIFIER
# =============================================================================

class ReplyClassifyRequest(BaseModel):
    reply_id: str
    reply_text: str
    reply_from: str
    lead_id: str


@app.post("/replies/classify")
async def classify_single_reply(
    body: ReplyClassifyRequest,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Classify an incoming reply and apply automatic actions.
    Called by Warmr webhook or n8n when a reply comes in.
    """
    from integrations.reply_classifier import process_reply
    import anthropic
    import os as _os
    ac = anthropic.Anthropic(api_key=_os.environ["ANTHROPIC_API_KEY"])
    result = await process_reply(
        reply_id=body.reply_id,
        reply_text=body.reply_text,
        reply_from=body.reply_from,
        lead_id=body.lead_id,
        workspace_id=workspace_id,
        supabase_client=db,
        anthropic_client=ac,
    )
    return result


class LeadImportRequest(BaseModel):
    rows: list[dict]
    dry_run: bool = False
    source: str = "csv"
    auto_enrich: bool = True   # geïmporteerde leads gaan direct de enrichment-queue in
    import_run_id: str | None = None  # client-UUID voor idempotency (24u TTL)
    merge_strategy: str = "skip"  # skip | fill_blanks | overwrite — bij duplicates


@app.post("/leads/import")
async def import_leads_endpoint(
    body: LeadImportRequest,
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Bulk lead-import met automatische dedup + auto-enrich.

    Dedup-volgorde: email → domain → kvk → fuzzy company+city.
    Auto-enrich (default true): nieuwe leads gaan direct de enrichment-queue
    in zodat dezelfde verrijking als gescrapte leads gebeurt
    (KvK, website intel, owner-extract, archetype, etc.).
    Cap: 500 rows per call.
    """
    from utils.lead_import import import_leads

    principal = identify_principal(request)
    result = await import_leads(
        rows=body.rows,
        workspace_id=workspace_id,
        supabase_client=db,
        imported_by=principal.get("created_by", "unknown"),
        source=body.source,
        dry_run=body.dry_run,
        auto_enrich=body.auto_enrich,
        import_run_id=body.import_run_id,
        merge_strategy=body.merge_strategy,
    )
    return result


@app.get("/crm/activity-board")
async def leads_activity_board(
    response: Response,
    limit: int = 500,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Batch CRM-board view. Returnt alle workspace-leads met derived status,
    last_outbound_at, last_inbound_at — geclusterd per status voor kanban.

    Cache-Control: stale-while-revalidate. Browser/CDN serveert ~10s gestale
    response terwijl er nieuwe data binnen komt — voorkomt redundante fetches
    bij multi-tab gebruikers, met minimale UX-impact.

    Doet de derive_status logic INLINE met SQL ipv per-lead aanroepen
    (anders 200 sequentie-fetches). Snel pad: één join-query.
    """
    from utils.lead_activity import derive_status, parse_iso as _parse_iso

    # Fetch leads (cap)
    try:
        leads_res = (
            db.table("leads")
            .select(
                "id, company_name, domain, sector, archetype, score, city, "
                "email_status, manual_status_override, manual_status_override_reason, "
                "manual_status_override_at, recontact_after, is_test_lead, created_at"
            )
            .eq("workspace_id", workspace_id)
            .order("created_at", desc=True)
            .limit(min(limit, 500))
            .execute()
        )
        leads = leads_res.data or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"leads fetch failed: {e}")
    if not leads:
        return {"leads": [], "buckets": {}, "total": 0}

    lead_ids = [l["id"] for l in leads]

    # Bulk-fetch sequence-history voor al deze leads in 1 call
    seq_by_lead: dict[str, list[dict]] = {}
    try:
        sh_res = (
            db.table("lead_campaign_history")
            .select("lead_id, status, is_active, step_index, sent_at, created_at, next_send_at")
            .in_("lead_id", lead_ids)
            .eq("workspace_id", workspace_id)
            .order("created_at", desc=True)
            .execute()
        )
        for row in (sh_res.data or []):
            seq_by_lead.setdefault(row["lead_id"], []).append(row)
    except Exception as e:
        logger.debug("activity-board sequence fetch failed: %s", e)

    # Bulk-fetch laatste reply per lead in 1 call
    reply_by_lead: dict[str, dict] = {}
    try:
        rep_res = (
            db.table("reply_inbox")
            .select("lead_id, received_at, classification, classifier_summary, body_preview")
            .in_("lead_id", lead_ids)
            .eq("workspace_id", workspace_id)
            .order("received_at", desc=True)
            .execute()
        )
        # Eerste-zien wint (al gesorteerd desc op received_at)
        for row in (rep_res.data or []):
            lid = row["lead_id"]
            if lid not in reply_by_lead:
                reply_by_lead[lid] = row
    except Exception as e:
        logger.debug("activity-board replies fetch failed: %s", e)

    # Per lead: derive + assemble
    buckets: dict[str, list[dict]] = {}
    enriched: list[dict] = []
    for lead in leads:
        seq_history = seq_by_lead.get(lead["id"], [])
        last_reply = reply_by_lead.get(lead["id"])
        last_inbound_at = _parse_iso(last_reply.get("received_at")) if last_reply else None
        last_inbound_class = (
            {"category": last_reply.get("classification")} if last_reply else None
        )

        last_outbound_at = None
        for h in seq_history:
            sa = _parse_iso(h.get("sent_at"))
            if sa and (last_outbound_at is None or sa > last_outbound_at):
                last_outbound_at = sa

        status, status_reason, status_changed_at = derive_status(
            lead, last_inbound_class, last_inbound_at, seq_history,
        )

        item = {
            "lead_id": lead["id"],
            "company_name": lead.get("company_name"),
            "domain": lead.get("domain"),
            "city": lead.get("city"),
            "sector": lead.get("sector"),
            "archetype": lead.get("archetype"),
            "score": lead.get("score"),
            "status": status,
            "status_reason": status_reason,
            "status_changed_at": status_changed_at.isoformat() if status_changed_at else None,
            "is_manual_override": bool(lead.get("manual_status_override")),
            "is_test_lead": bool(lead.get("is_test_lead")),
            "last_outbound_at": last_outbound_at.isoformat() if last_outbound_at else None,
            "last_inbound_at": last_inbound_at.isoformat() if last_inbound_at else None,
            "last_inbound_category": (last_inbound_class or {}).get("category"),
        }
        enriched.append(item)
        buckets.setdefault(status, []).append(item)

    response.headers["Cache-Control"] = "private, max-age=10, stale-while-revalidate=30"
    return {"leads": enriched, "buckets": buckets, "total": len(enriched)}


@app.get("/leads/{lead_id}/activity")
async def lead_activity_endpoint(
    lead_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Activity-rollup + derived CRM status voor één lead."""
    from utils.lead_activity import get_lead_activity

    result = await get_lead_activity(lead_id, workspace_id, db)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


class BulkStatusRequest(BaseModel):
    lead_ids: list[str]
    status: str   # afgemeld | geen_interesse | recontact_later | actief_gesprek | verkeerde_contact | "" (clear)
    reason: str | None = None
    recontact_after: str | None = None  # ISO date — alleen voor recontact_later


@app.post("/leads/bulk-status")
async def bulk_status_endpoint(
    body: BulkStatusRequest,
    request: Request,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Bulk handmatige status-override. Lege string = clear override."""
    from utils.lead_activity import CRM_STATUSES

    new_status = body.status.strip()
    if new_status and new_status not in CRM_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"status moet een van {sorted(CRM_STATUSES)} of '' (clear)",
        )

    principal = identify_principal(request)
    now = datetime.now(timezone.utc).isoformat()

    patch: dict[str, Any] = {
        "manual_status_override": new_status or None,
        "manual_status_override_reason": body.reason if new_status else None,
        "manual_status_override_at": now if new_status else None,
        "manual_status_override_by": principal.get("created_by") if new_status else None,
    }
    if new_status == "recontact_later":
        # Zonder expliciete datum: default 90 dagen. Anders zet de recontact-logica
        # (die op recontact_after <= now filtert) nooit aan bij drag/bulk-verplaatsing.
        patch["recontact_after"] = body.recontact_after or (
            datetime.now(timezone.utc) + timedelta(days=90)
        ).isoformat()

    try:
        res = (
            db.table("leads")
            .update(patch)
            .in_("id", body.lead_ids)
            .eq("workspace_id", workspace_id)
            .execute()
        )
        updated = len(res.data or [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"bulk update failed: {e}")

    # I4: elke operator-actie als timeline-event — bulk-status schreef
    # voorheen alleen attributie-kolommen op de lead-row, geen event.
    for row in (res.data or []):
        try:
            _insert_timeline_event(
                db, workspace_id, row["id"], "manual_status_override",
                f"Status-override: {new_status or '(cleared)'}"
                + (f" — {body.reason}" if body.reason else ""),
                metadata={"by": principal.get("created_by"), "via": "bulk-status"},
            )
        except Exception as e:
            logger.warning("bulk-status: timeline-event mislukt voor %s: %s", row.get("id"), e)

    return {"updated": updated, "status": new_status or "(cleared)", "reason": body.reason}


@app.get("/system/sidebar-counts")
async def sidebar_counts(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Compacte counts voor sidebar-badges (CRM activity, Inbox).

    - recontact_due: leads met recontact_after datum vandaag of in verleden,
      OF status 'klaar_voor_recontact' (cooldown afgelopen).
    - unread_replies: replies sinds last_seen_inbox marker.
    - worker_healthy: laatste enrichment_jobs.completed_at < 5 min geleden.

    Polling-vriendelijk: 1 lichte query per metric. Faalt-tolerant — None bij errors.
    """
    from datetime import datetime, timezone, timedelta

    counts: dict[str, Any] = {}
    now = datetime.now(timezone.utc)

    # Recontact-due (klaar voor recontact OF expliciete recontact_after datum bereikt)
    try:
        # Leads waarbij recontact_after datum is verstreken
        due_explicit = (
            db.table("leads")
            .select("id", count="exact")
            .eq("workspace_id", workspace_id)
            .lte("recontact_after", now.isoformat())
            .execute()
        )
        counts["recontact_due"] = due_explicit.count or 0
    except Exception:
        counts["recontact_due"] = None

    # Unread replies — alle van afgelopen 7 dagen (simpler dan per-user last_seen tracking)
    try:
        recent_cutoff = (now - timedelta(days=7)).isoformat()
        unread = (
            db.table("reply_inbox")
            .select("id", count="exact")
            .eq("workspace_id", workspace_id)
            .gte("received_at", recent_cutoff)
            .execute()
        )
        counts["unread_replies"] = unread.count or 0
    except Exception:
        counts["unread_replies"] = None

    # Worker health — recent completed enrichment_jobs
    try:
        threshold = (now - timedelta(minutes=5)).isoformat()
        recent_done = (
            db.table("enrichment_jobs")
            .select("id", count="exact")
            .eq("workspace_id", workspace_id)
            .gte("completed_at", threshold)
            .execute()
        )
        # Of er pending jobs zijn (worker zou die moeten oppakken)
        pending = (
            db.table("enrichment_jobs")
            .select("id", count="exact")
            .eq("workspace_id", workspace_id)
            .eq("status", "pending")
            .execute()
        )
        recent_count = recent_done.count or 0
        pending_count = pending.count or 0
        # Healthy: recent activiteit, OF geen pending werk om op te pakken
        counts["worker_healthy"] = recent_count > 0 or pending_count == 0
        counts["worker_pending_jobs"] = pending_count
        counts["worker_recent_completed"] = recent_count
    except Exception:
        counts["worker_healthy"] = None

    return counts


@app.post("/replies/{reply_id}/draft")
async def draft_reply_endpoint(
    reply_id: str,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Generate een Claude-suggestie voor antwoord op een binnengekomen reply.

    Verstuurt NIETS — alleen draft tekst voor Sami om te lezen + zelf versturen.
    Gecached: re-clicks zijn gratis tenzij classification verandert.
    """
    from campaigns.reply_drafter import draft_reply
    import anthropic
    import os as _os

    # Fetch reply
    try:
        reply_res = (
            db.table("reply_inbox")
            .select("*")
            .eq("id", reply_id)
            .eq("workspace_id", workspace_id)
            .maybe_single()
            .execute()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reply fetch failed: {e}")
    if not reply_res or not reply_res.data:
        raise HTTPException(status_code=404, detail="Reply not found")
    reply_row = reply_res.data

    # Fetch lead (incl. archetype zodat draft tone matcht)
    lead = None
    if reply_row.get("lead_id"):
        try:
            lead_res = (
                db.table("leads")
                .select("id, company_name, contact_first_name, sector, domain, archetype")
                .eq("id", reply_row["lead_id"])
                .maybe_single()
                .execute()
            )
            if lead_res and lead_res.data:
                lead = lead_res.data
        except Exception:
            pass

    # Thread-context: fetch onze laatste verstuurde mail uit deze sequence
    # zodat Claude weet WAAROP de prospect reageert.
    original_emails: list[dict] = []
    if reply_row.get("lead_id"):
        try:
            hist_res = (
                db.table("lead_campaign_history")
                .select("sequence_steps, step_index, sent_at")
                .eq("lead_id", reply_row["lead_id"])
                .eq("workspace_id", workspace_id)
                .order("sent_at", desc=True)
                .limit(1)
                .execute()
            )
            if hist_res.data:
                row = hist_res.data[0]
                steps = row.get("sequence_steps") or []
                idx = max((row.get("step_index") or 1) - 1, 0)
                if 0 <= idx < len(steps):
                    last_sent = steps[idx]
                    original_emails.append({
                        "subject": last_sent.get("subject", ""),
                        "body": last_sent.get("body", ""),
                    })
        except Exception:
            pass

    # Use existing classification if present, else classify on-the-fly.
    # Kolom-fix (2026-07-14): de echte kolommen heten 'classification' en
    # 'classification_summary' (migratie 004) — 'category'/'classifier_summary'
    # bestonden nooit, dus de summary was altijd leeg.
    classification = {
        "category": reply_row.get("classification") or "other",
        "summary": reply_row.get("classification_summary") or "",
    }

    ac = anthropic.AsyncAnthropic(api_key=_os.environ["ANTHROPIC_API_KEY"])
    result = await draft_reply(
        reply_inbox_row=reply_row,
        lead=lead,
        classification=classification,
        workspace_id=workspace_id,
        supabase_client=db,
        anthropic_client=ac,
        original_emails=original_emails or None,
    )
    return result


@app.post("/replies/process-unclassified")
async def process_unclassified_replies(
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """
    Process all reply_inbox rows that don't have a classification yet.
    Called by n8n every 15 min.
    """
    from integrations.reply_classifier import process_reply
    import anthropic
    import os as _os

    try:
        unclassified = db.table("reply_inbox").select("*").eq(
            "workspace_id", workspace_id,
        ).is_("classification", "null").limit(25).execute()
    except Exception as e:
        # Laptop offline op het cron-moment (Errno 51) gaf een kale 500-traceback.
        # 503 met reden → leesbaar in het (nu timestamped) cron-log; cron retryt vanzelf.
        raise HTTPException(status_code=503, detail=f"reply_inbox onbereikbaar (offline?): {type(e).__name__}")

    rows = unclassified.data or []
    if not rows:
        return {"processed": 0}

    ac = anthropic.Anthropic(api_key=_os.environ["ANTHROPIC_API_KEY"])
    processed = 0
    for row in rows:
        try:
            await process_reply(
                reply_id=row["id"],
                reply_text=row.get("body", ""),
                reply_from=row.get("from_email", ""),
                lead_id=row.get("lead_id", ""),
                workspace_id=workspace_id,
                supabase_client=db,
                anthropic_client=ac,
            )
            processed += 1
        except Exception as e:
            logger.warning("classify reply %s failed: %s", row.get("id"), e)

    return {"processed": processed, "total": len(rows)}


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
            # Operator-egress via de dispatcher (I3/I7). Natuurlijke
            # dag-key: één briefing per workspace per dag — een dubbele
            # trigger (n8n-retry, handmatige re-run) mailt niet dubbel.
            from utils.outbound_dispatcher import dispatch_outbound

            async def _send_briefing():
                async with httpx.AsyncClient(timeout=10.0) as client:
                    r = await client.post(
                        "https://api.resend.com/emails",
                        json=payload,
                        headers={"Authorization": f"Bearer {resend_key}"},
                    )
                    return {"status_code": r.status_code, "ok": r.status_code < 400}

            disp = await dispatch_outbound(
                kind="operator_email",
                idempotency_key=f"briefing:{workspace_id}:{briefing['date']}",
                actor="scheduler:briefing",
                send=_send_briefing,
                supabase_client=db,
                workspace_id=workspace_id,
                metadata={"endpoint": "/briefing/generate", "to": operator_email},
            )
            if disp.skipped_duplicate:
                briefing["email_sent"] = False
                briefing["skipped_duplicate"] = True
            else:
                briefing["email_sent"] = bool((disp.result or {}).get("ok"))
        except Exception as e:
            logger.warning("Briefing email failed: %s", e)
            briefing["email_sent"] = False
    else:
        briefing["email_sent"] = False

    return briefing


# =============================================================================
# SCORING — Feedback loop (Warmr replies → insights)
# =============================================================================

@app.post("/scoring/process-feedback")
async def scoring_process_feedback(
    days: int = 30,
    workspace_id: str = Depends(require_service_key),
    db: Client = Depends(get_supabase),
) -> dict:
    """Run feedback_processor over de afgelopen N dagen + persist de run.

    Service-only — bedoeld voor n8n / scheduled invocation. Adjustments zijn
    suggesties, géén auto-apply: de operator moet handmatig scoring weights
    aanpassen via config/scoring_weights.py op basis van de output.
    """
    from scoring.feedback_processor import process_feedback

    result = await process_feedback(workspace_id, db, days=days)

    # Persist run voor history
    try:
        db.table("feedback_runs").insert({
            "workspace_id": workspace_id,
            "period_days": days,
            "leads_analyzed": result.get("leads_analyzed", 0),
            "replied": result.get("replied", 0),
            "bounced": result.get("bounced", 0),
            "reply_rate": result.get("reply_rate", 0),
            "bounce_rate": result.get("bounce_rate", 0),
            "insights": result.get("insights", []),
            "adjustments": result.get("adjustments", []),
        }).execute()
    except Exception as e:
        logger.warning("feedback_runs persist failed (table missing?): %s", e)

    return result


@app.get("/scoring/feedback-history")
async def scoring_feedback_history(
    limit: int = 20,
    workspace_id: str = Depends(get_workspace),
    db: Client = Depends(get_supabase),
) -> dict:
    """Return de laatste N feedback-runs voor deze workspace."""
    try:
        res = (
            db.table("feedback_runs")
            .select("*")
            .eq("workspace_id", workspace_id)
            .order("created_at", desc=True)
            .limit(min(limit, 100))
            .execute()
        )
        return {"runs": res.data or []}
    except Exception as e:
        logger.warning("feedback_history fetch failed: %s", e)
        return {"runs": [], "error": str(e)}
