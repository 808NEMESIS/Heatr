"""
audit/scorer.py — orkestratie van de prospect-facing audit.

Leest bestaande data (website_intelligence, leads, netwerk-log, enrichment_data)
+ één lichte homepage-fetch voor DOM-structuur-checks, draait de checklist, en
normaliseert over de BEHAALDE noemer (not_measurable-checks tellen niet mee).
Knock-outs cappen de eindscore; lege-site is een aparte finding, geen lage score.

Schrijft website_intelligence NIET — deelt data, geen logica (harde eis). Het
report gaat append-only naar heatr_audit_reports met een oplopende version.
"""
from __future__ import annotations

import logging
import re
from typing import Any

from config import audit_weights as W
from audit import checks as C

logger = logging.getLogger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")


async def _fetch_html(domain: str) -> tuple[str, dict]:
    """Lichte homepage-fetch (https, http-fallback): (html, response_headers).

    De headers uit deze fetch maken de security_headers-check meetbaar zónder
    re-enrichment (die headers komen normaal uit scrape_website, dat bestaande
    leads nog niet hadden).
    """
    import httpx
    for scheme in ("https", "http"):
        try:
            async with httpx.AsyncClient(timeout=12.0, follow_redirects=True,
                                         headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr-Audit/1.0)"}) as cl:
                r = await cl.get(f"{scheme}://{domain}")
                if r.status_code == 200 and r.text:
                    return r.text, dict(r.headers)
        except Exception:
            continue
    return "", {}


def _dom_text_len(html: str, page_text: str) -> int:
    if html:
        return len(" ".join(_TAG_RE.sub(" ", html).split()))
    return len(page_text or "")


async def _load_context(lead: dict, sb: Any, *, tier: int, places: dict | None) -> C.ScoreContext:
    ws, lead_id, domain = lead["workspace_id"], lead["id"], lead.get("domain") or ""
    wi = (sb.table("website_intelligence").select("*").eq("lead_id", lead_id)
          .eq("workspace_id", ws).limit(1).execute()).data or []
    wi = wi[0] if wi else {}

    net = (sb.table("website_network_log").select("requests, summary, captured_at")
           .eq("lead_id", lead_id).eq("workspace_id", ws)
           .order("captured_at", desc=True).limit(1).execute()).data or []
    net_requests = (net[0].get("requests") if net else None) or []

    # enrichment_data: contact_crawl_v2 (page_text/schema_org) + website (headers)
    ed = (sb.table("enrichment_data").select("source, raw_result")
          .eq("lead_id", lead_id).eq("workspace_id", ws).execute()).data or []
    page_text, schema_org, response_headers = "", {}, {}
    for row in ed:
        rr = row.get("raw_result") or {}
        if row.get("source") == "contact_crawl_v2":
            page_text = page_text or rr.get("page_text") or ""
            schema_org = schema_org or rr.get("schema_org") or {}
        if row.get("source") == "website":
            response_headers = response_headers or rr.get("response_headers") or {}

    html, fetched_headers = await _fetch_html(domain)
    return C.ScoreContext(lead=lead, wi=wi, network_requests=net_requests,
                          page_text=page_text, schema_org=schema_org,
                          response_headers=fetched_headers or response_headers, html=html,
                          sector=lead.get("sector") or "", places=places)


def _knockouts(ctx: C.ScoreContext, normalized: int) -> tuple[int, str | None]:
    """Cap de eindscore op basis van de twee knock-out-condities."""
    reason = None
    cap = 100
    has_appt = bool(ctx.lead.get("has_online_booking") or ctx.conv.get("has_online_booking")
                    or ctx.conv.get("has_contact_form") or ctx.lead.get("has_whatsapp") or ctx.conv.get("has_whatsapp"))
    has_phone = bool(ctx.conv.get("has_phone_clickable") or ctx.lead.get("phone")
                     or C._txt_has(ctx.page_text, r"\b0\d[\s\-]?\d{7,8}\b", r"\+31"))
    if not has_appt:
        cap = min(cap, W.KNOCKOUTS["no_appointment"]["cap"]); reason = W.KNOCKOUTS["no_appointment"]["reason"]
    if not has_phone:
        c2 = W.KNOCKOUTS["no_phone"]["cap"]
        if c2 < cap:
            cap = c2; reason = W.KNOCKOUTS["no_phone"]["reason"]
    return (min(normalized, cap), reason if cap < 100 else None)


async def score_lead(lead: dict, sb: Any, *, tier: int = 1, places: dict | None = None) -> dict:
    """Bereken de audit voor één lead. Geeft het report-dict terug (schrijft niet)."""
    sector = lead.get("sector") or ""
    ctx = await _load_context(lead, sb, tier=tier, places=places)

    findings: list[dict] = []
    for chk in W.checks_for_sector(sector, include_tier2=(tier == 2)):
        fn = C.CHECK_FUNCS.get(chk["check_id"])
        if not fn:
            continue
        try:
            findings.append(fn(ctx))
        except Exception as e:  # noqa: BLE001 - één check mag de audit niet breken
            logger.warning("audit-check %s faalde: %s", chk["check_id"], e)

    # Aggregatie per categorie over de MEETBARE checks (not_measurable telt niet).
    cats: dict[str, dict] = {}
    for f in findings:
        if f["status"] == "not_measurable":
            continue
        c = cats.setdefault(f["categorie"], {"behaald": 0, "max": 0})
        c["behaald"] += f["punten_behaald"]
        c["max"] += f["punten_max"]
    for cat, c in cats.items():
        c["label"] = W.CATEGORY_LABELS.get(cat, cat)

    achieved = sum(c["behaald"] for c in cats.values())
    denom = sum(c["max"] for c in cats.values())
    normalized = round(achieved / denom * 100) if denom else 0
    scored_layers = sorted(cats.keys())

    capped, capped_by = _knockouts(ctx, normalized)

    # Lege-site: aparte finding, geen lage score (verlaten placeholder).
    dom_len = _dom_text_len(ctx.html, ctx.page_text)
    if dom_len < W.EMPTY_SITE_DOM_CHARS:
        findings.append(C._f(
            "empty_site", "technical", "fail", 0, 0,
            bewijs=f"DOM-tekst {dom_len} tekens",
            mail_zin="De site lijkt een onafgemaakte placeholder: er staat vrijwel geen inhoud op.",
            mail_safe=True, severity="high",
            uitleg="Onder de lege-site-drempel; dit is geen slechte site maar een verlaten pagina."))

    return {
        "lead_id": lead["id"], "workspace_id": lead["workspace_id"],
        "tier": tier,
        "score_total": achieved, "score_denominator": denom,
        "score_normalized": capped, "score_capped_by": capped_by,
        "scored_layers": scored_layers,
        "categories": cats,
        "findings": findings,
        "not_measurable": [f["check_id"] for f in findings if f["status"] == "not_measurable"],
        "screenshot_desktop_url": ctx.wi.get("screenshot_desktop_url"),
        "screenshot_mobile_url": ctx.wi.get("screenshot_mobile_url"),
        "content_hash": ctx.wi.get("dom_text_hash"),
        "is_empty_site": dom_len < W.EMPTY_SITE_DOM_CHARS,
    }


async def persist_audit_report(report: dict, sb: Any, *, benchmark: dict | None = None) -> dict | None:
    """Schrijf het report append-only met een oplopende version per lead.

    LET OP benchmark (cohort-beslissing 2026-07): de data-gaten zijn COHORT-GEBONDEN
    — oude leads (vóór 033) worden op minder checks gemeten dan nieuwe (has_cookie_
    banner, response_headers, schema_org ontbreken). Voor de PER-LEAD-score prima
    (normalisatie over de behaalde noemer). Voor de BENCHMARK niet: percentielen zijn
    VOORLOPIG tot een re-enrichment-sweep het hele cohort op gelijke meetbaarheid
    heeft gebracht. benchmark.py moet dit als 'provisional: true' markeren tot dan.
    """
    ws, lead_id = report["workspace_id"], report["lead_id"]
    existing = (sb.table("audit_reports").select("version")
                .eq("lead_id", lead_id).eq("workspace_id", ws)
                .order("version", desc=True).limit(1).execute()).data or []
    version = (existing[0]["version"] + 1) if existing else 1
    row = {
        "workspace_id": ws, "lead_id": lead_id, "version": version, "tier": report["tier"],
        "score_total": report["score_total"], "score_normalized": report["score_normalized"],
        "score_capped_by": report["score_capped_by"], "scored_layers": report["scored_layers"],
        "categories": report["categories"], "findings": report["findings"],
        "benchmark": benchmark,
        "screenshot_desktop_url": report.get("screenshot_desktop_url"),
        "screenshot_mobile_url": report.get("screenshot_mobile_url"),
        "content_hash": report.get("content_hash"),
    }
    try:
        res = sb.table("audit_reports").insert(row).execute()
        return res.data[0] if res.data else None
    except Exception as e:
        logger.error("persist_audit_report faalde (lead=%s v%s): %s", lead_id, version, e)
        return None
