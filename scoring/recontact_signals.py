"""
scoring/recontact_signals.py — Detect change signals that justify recontact.

De oude "90 dagen cooldown" is simpel maar dom: je benadert iemand weer
alleen omdat er tijd is verstreken, niet omdat er iets is veranderd.

Deze module detecteert nieuwe signalen waardoor recontact gerechtvaardigd is:
  - Website inhoud is significant veranderd sinds vorige outreach
  - Google rating is meetbaar gedaald
  - Nieuwe klachten in reviews
  - Nieuwe vacatures (groei-signaal)
  - Nieuwe eigenaar / KvK bestuurder gewijzigd
  - Concurrent in zelfde stad heeft iets geüpgraded

Zonder signaal → geen recontact, ook niet na 90 dagen.
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)


SIGNAL_WEIGHTS = {
    "website_changed":       0.40,  # Sterkste signaal — ze hebben blijkbaar iets gedaan
    "rating_dropped":        0.30,  # Klanten zijn ontevreden — verkoopmoment
    "new_complaint_reviews": 0.25,  # Nieuwe pijnpunten om naar te verwijzen
    "new_job_posting":       0.15,  # Groei-signaal
    "kvk_owner_changed":     0.50,  # Nieuwe beslisser — compleet andere ballgame
    "competitor_upgraded":   0.20,  # Urgentie (zij kunnen achterop raken)
}


async def detect_recontact_signals(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict[str, Any]:
    """
    Check alle recontact-triggers voor 1 lead.

    Returns:
        {
            "has_signal": bool,
            "signals": [{"type": "website_changed", "detail": "...", "weight": 0.4}],
            "score": float (0.0-1.0, higher = sterker signaal),
            "suggested_opener_angle": str,
        }
    """
    result: dict[str, Any] = {
        "has_signal": False,
        "signals": [],
        "score": 0.0,
        "suggested_opener_angle": "",
    }

    # Load lead
    lead_res = supabase_client.table("leads").select("*").eq(
        "id", lead_id,
    ).maybe_single().execute()
    if not lead_res.data:
        return result
    lead = lead_res.data

    # Previous outreach snapshot (from last completed campaign_history)
    snapshot = await _load_previous_snapshot(lead_id, supabase_client)

    # ── Signal 1: Website content changed ────────────────────────────
    if lead.get("domain"):
        new_hash = await _compute_website_hash(lead["domain"])
        old_hash = snapshot.get("website_hash") if snapshot else None
        if new_hash and old_hash and new_hash != old_hash:
            result["signals"].append({
                "type": "website_changed",
                "detail": "Website-inhoud is significant gewijzigd",
                "weight": SIGNAL_WEIGHTS["website_changed"],
            })

    # ── Signal 2: Rating dropped ─────────────────────────────────────
    current_rating = lead.get("google_rating") or 0
    prev_rating = snapshot.get("google_rating") if snapshot else None
    if prev_rating and current_rating and (prev_rating - current_rating >= 0.2):
        result["signals"].append({
            "type": "rating_dropped",
            "detail": f"Google rating gedaald van {prev_rating} naar {current_rating}",
            "weight": SIGNAL_WEIGHTS["rating_dropped"],
        })

    # ── Signal 3: More reviews with complaints ──────────────────────
    review_analysis = lead.get("review_analysis") or {}
    complaints_now = len(review_analysis.get("complaints") or [])
    complaints_before = snapshot.get("complaint_count", 0) if snapshot else 0
    if complaints_now > complaints_before and complaints_now >= 2:
        result["signals"].append({
            "type": "new_complaint_reviews",
            "detail": f"{complaints_now - complaints_before} nieuwe klachten sinds vorige outreach",
            "weight": SIGNAL_WEIGHTS["new_complaint_reviews"],
        })

    # ── Signal 4: New job posting / growth ──────────────────────────
    # Cheap check: kijk naar /vacatures, /werken-bij, /jobs op de site
    if lead.get("domain"):
        has_jobs = await _check_jobs_page(lead["domain"])
        had_jobs_before = snapshot.get("has_jobs_page", False) if snapshot else False
        if has_jobs and not had_jobs_before:
            result["signals"].append({
                "type": "new_job_posting",
                "detail": "Nieuwe vacaturepagina gedetecteerd — groei signaal",
                "weight": SIGNAL_WEIGHTS["new_job_posting"],
            })

    # ── Signal 5: KvK bestuurder changed ────────────────────────────
    current_kvk = lead.get("kvk_bestuurder_name") or ""
    prev_kvk = snapshot.get("kvk_bestuurder_name") if snapshot else None
    if prev_kvk and current_kvk and current_kvk.lower() != prev_kvk.lower():
        result["signals"].append({
            "type": "kvk_owner_changed",
            "detail": f"Nieuwe bestuurder: {current_kvk} (was: {prev_kvk})",
            "weight": SIGNAL_WEIGHTS["kvk_owner_changed"],
        })

    # ── Signal 6: Competitor upgrade ────────────────────────────────
    try:
        wi_res = supabase_client.table("website_intelligence").select(
            "competitor_data, score_vs_market"
        ).eq("lead_id", lead_id).maybe_single().execute()
        if wi_res.data:
            current_delta = wi_res.data.get("score_vs_market")
            prev_delta = snapshot.get("score_vs_market") if snapshot else None
            if prev_delta is not None and current_delta is not None:
                # If lead is now more behind market than before, competitors improved
                if current_delta < prev_delta - 5:
                    result["signals"].append({
                        "type": "competitor_upgraded",
                        "detail": f"Concurrenten zijn vooruit gegaan (delta van {prev_delta:+d} naar {current_delta:+d})",
                        "weight": SIGNAL_WEIGHTS["competitor_upgraded"],
                    })
    except Exception:
        pass

    # ── Aggregate score ──────────────────────────────────────────────
    if result["signals"]:
        result["has_signal"] = True
        # Score is sum of weights, capped at 1.0
        result["score"] = min(sum(s["weight"] for s in result["signals"]), 1.0)

        # Suggested angle = strongest signal
        strongest = max(result["signals"], key=lambda s: s["weight"])
        angles = {
            "website_changed":       "Ik zag dat jullie de website hebben aangepast — hoe is dat bevallen?",
            "rating_dropped":        "Een aantal recente reviews viel me op. Is er iets specifieks veranderd?",
            "new_complaint_reviews": "Recent zag ik klachten in reviews over [X]. Herkenbaar?",
            "new_job_posting":       "Ik zag dat jullie aan het uitbreiden zijn — dat doet me denken aan iets dat wij vaak oplossen bij groei.",
            "kvk_owner_changed":     "Ik zag dat er een nieuwe eigenaar/bestuurder is gestart.",
            "competitor_upgraded":   "Concurrent X heeft onlangs hun site verbeterd. Willen jullie daar tegenwicht aan bieden?",
        }
        result["suggested_opener_angle"] = angles.get(strongest["type"], "")

    logger.info(
        "detect_recontact_signals: lead=%s has_signal=%s score=%.2f signals=%d",
        lead_id, result["has_signal"], result["score"], len(result["signals"]),
    )

    return result


async def get_recontact_ready(
    workspace_id: str,
    supabase_client: Any,
    limit: int = 50,
) -> list[dict]:
    """
    Retourneert leads die klaar zijn voor recontact op basis van signals,
    niet puur op time-based cooldown.

    Filters:
      - status = 'no_response'
      - next_contact_after <= now (90-dagen window voorbij)
      - gdpr_safe = true
      - contact_attempt_count < 3
      - has_signal = true (minstens 1 recontact-trigger)
    """
    now = datetime.now(timezone.utc).isoformat()

    res = (
        supabase_client.table("leads")
        .select("*")
        .eq("workspace_id", workspace_id)
        .eq("status", "no_response")
        .eq("gdpr_safe", True)
        .lte("next_contact_after", now)
        .lt("contact_attempt_count", 3)
        .order("score", desc=True)
        .limit(limit * 2)  # We filteren straks op signals, dus meer ophalen
        .execute()
    )
    candidates = res.data or []

    # Check signals voor elke candidate
    recontact_ready: list[dict] = []
    for lead in candidates:
        signal_result = await detect_recontact_signals(
            lead["id"], workspace_id, supabase_client,
        )
        if signal_result["has_signal"]:
            lead["_recontact_signals"] = signal_result
            recontact_ready.append(lead)
        if len(recontact_ready) >= limit:
            break

    # Sort by signal score, not just lead score
    recontact_ready.sort(
        key=lambda l: l["_recontact_signals"]["score"],
        reverse=True,
    )

    return recontact_ready


async def save_outreach_snapshot(
    lead_id: str,
    supabase_client: Any,
) -> None:
    """
    Save huidige staat van lead als baseline voor future signal detection.

    Roep aan NA een campagne-completion zodat signals kunnen worden gedetecteerd
    bij volgende check.
    """
    lead_res = supabase_client.table("leads").select("*").eq(
        "id", lead_id,
    ).maybe_single().execute()
    if not lead_res.data:
        return

    lead = lead_res.data
    snapshot: dict[str, Any] = {
        "lead_id": lead_id,
        "workspace_id": lead.get("workspace_id"),
        "snapshot_at": datetime.now(timezone.utc).isoformat(),
        "google_rating": lead.get("google_rating"),
        "google_review_count": lead.get("google_review_count"),
        "score_vs_market": None,
        "kvk_bestuurder_name": lead.get("kvk_bestuurder_name"),
        "complaint_count": len((lead.get("review_analysis") or {}).get("complaints") or []),
    }

    # Website hash
    if lead.get("domain"):
        snapshot["website_hash"] = await _compute_website_hash(lead["domain"])
        snapshot["has_jobs_page"] = await _check_jobs_page(lead["domain"])

    # score_vs_market from website_intelligence
    try:
        wi = supabase_client.table("website_intelligence").select(
            "score_vs_market"
        ).eq("lead_id", lead_id).maybe_single().execute()
        if wi.data:
            snapshot["score_vs_market"] = wi.data.get("score_vs_market")
    except Exception:
        pass

    try:
        supabase_client.table("lead_outreach_snapshots").insert(snapshot).execute()
    except Exception as e:
        logger.debug("save_outreach_snapshot: %s", e)


async def _load_previous_snapshot(lead_id: str, supabase_client: Any) -> dict | None:
    """Load most recent snapshot for a lead."""
    try:
        res = (
            supabase_client.table("lead_outreach_snapshots")
            .select("*")
            .eq("lead_id", lead_id)
            .order("snapshot_at", desc=True)
            .limit(1)
            .maybe_single()
            .execute()
        )
        return res.data
    except Exception:
        return None


async def _compute_website_hash(domain: str) -> str:
    """Compute content hash of homepage — detect changes over time."""
    try:
        async with httpx.AsyncClient(
            timeout=10, follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"},
        ) as client:
            r = await client.get(f"https://{domain}")
            if r.status_code == 200:
                # Strip whitespace + scripts for stable hash
                import re
                text = re.sub(r"<script[^>]*>.*?</script>", "", r.text, flags=re.DOTALL)
                text = re.sub(r"\s+", " ", text).strip()
                return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:16]
    except Exception:
        pass
    return ""


async def _check_jobs_page(domain: str) -> bool:
    """Quick check for /vacatures, /werken-bij, /jobs."""
    paths = ["/vacatures", "/werken-bij", "/werkenbij", "/jobs", "/careers", "/wij-zoeken"]
    try:
        async with httpx.AsyncClient(timeout=6, follow_redirects=True) as client:
            for path in paths:
                try:
                    r = await client.head(f"https://{domain}{path}")
                    if r.status_code == 200:
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False
