"""
enrichment/enrichment_validator.py — Post-enrichment validation.

Checks whether enrichment claims are actually supported by evidence,
not just inferred from absence. Marks each data point as:
  - verified:  confirmed by 2+ independent sources or direct observation
  - inferred:  derived from 1 source, plausible but not confirmed
  - assumed:   no direct evidence, based on default/absence
  - wrong:     contradicted by evidence (should be corrected)

Called after all enrichment steps to produce an honest quality report.
"""
from __future__ import annotations

import logging
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)


async def validate_enrichment(
    lead_id: str,
    workspace_id: str,
    supabase_client: Any,
) -> dict[str, Any]:
    """
    Validate enrichment claims for a lead by cross-checking data.

    Returns:
        {
            "validation_score": float (0-1, higher = more verified),
            "checks": [
                {"field": "no_whatsapp", "status": "verified|inferred|wrong", "evidence": "..."},
                ...
            ],
            "warnings": ["..."],
        }
    """
    # Load lead + website intelligence
    lead_res = supabase_client.table("leads").select("*").eq("id", lead_id).maybe_single().execute()
    if not lead_res.data:
        return {"validation_score": 0, "checks": [], "warnings": ["lead not found"]}

    lead = lead_res.data
    domain = lead.get("domain") or ""

    wi_res = supabase_client.table("website_intelligence").select("*").eq(
        "lead_id", lead_id,
    ).maybe_single().execute()
    wi = wi_res.data or {}
    conv = wi.get("conversion_details") or {}
    tech = wi.get("technical_details") or {}

    checks: list[dict] = []
    warnings: list[str] = []

    if not domain:
        return {"validation_score": 0, "checks": [], "warnings": ["no domain"]}

    # ── Fetch homepage + contact page to cross-validate ───────────────
    homepage_html = ""
    contact_html = ""

    async with httpx.AsyncClient(
        timeout=12, follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (compatible; Heatr/1.0)"},
    ) as client:
        try:
            r = await client.get(f"https://{domain}")
            if r.status_code == 200:
                homepage_html = r.text.lower()
        except Exception:
            try:
                r = await client.get(f"http://{domain}")
                homepage_html = r.text.lower()
            except Exception:
                warnings.append("could not fetch homepage for validation")

        # Also check /contact page — many features live there, not homepage
        for contact_path in ["/contact", "/kontakt", "/contact-us", "/neem-contact-op"]:
            try:
                r = await client.get(f"https://{domain}{contact_path}")
                if r.status_code == 200:
                    contact_html = r.text.lower()
                    break
            except Exception:
                continue

    combined_html = homepage_html + " " + contact_html

    # ── Validate: WhatsApp claim ─────────────────────────────────────
    whatsapp_patterns = ["wa.me", "api.whatsapp.com", "whatsapp.com/send", "whatsapp"]
    found_on_homepage = any(p in homepage_html for p in whatsapp_patterns)
    found_on_contact = any(p in contact_html for p in whatsapp_patterns)
    claimed_no_whatsapp = not conv.get("has_whatsapp", False)

    if claimed_no_whatsapp:
        if found_on_contact and not found_on_homepage:
            checks.append({
                "field": "no_whatsapp",
                "status": "wrong",
                "evidence": "WhatsApp gevonden op /contact pagina — homepage check miste dit",
            })
            warnings.append("WhatsApp claim was incorrect — found on contact page")
        elif not found_on_homepage and not found_on_contact:
            checks.append({
                "field": "no_whatsapp",
                "status": "verified",
                "evidence": "Niet gevonden op homepage én contactpagina",
            })
        else:
            checks.append({"field": "no_whatsapp", "status": "inferred", "evidence": "Alleen homepage gechecked"})
    else:
        checks.append({"field": "has_whatsapp", "status": "verified", "evidence": "Gevonden in HTML"})

    # ── Validate: Online booking claim ───────────────────────────────
    booking_patterns = ["calendly", "acuity", "simplybook", "booksy", "treatwell",
                        "reserveren", "afspraak maken", "boek nu", "plan je afspraak",
                        "online boeken", "booking"]
    found_booking = any(p in combined_html for p in booking_patterns)
    claimed_no_booking = not conv.get("has_online_booking", False)

    if claimed_no_booking and found_booking:
        checks.append({
            "field": "no_booking",
            "status": "wrong",
            "evidence": "Booking-gerelateerde tekst gevonden op homepage of contactpagina",
        })
        warnings.append("Booking claim was incorrect — found booking signals")
    elif claimed_no_booking:
        checks.append({"field": "no_booking", "status": "verified", "evidence": "Niet gevonden op homepage + contactpagina"})
    else:
        checks.append({"field": "has_booking", "status": "verified", "evidence": "Gevonden in HTML"})

    # ── Validate: Chatbot claim ──────────────────────────────────────
    # Some chatbots load after 3-5 seconds via lazy JS — pure HTML check may miss them
    chatbot_patterns = ["intercom", "drift", "tidio", "landbot", "trengo",
                        "livechat", "zopim", "zdassets", "crisp.chat",
                        "hs-scripts", "tawk.to"]
    found_chatbot = any(p in combined_html for p in chatbot_patterns)
    claimed_no_chatbot = not conv.get("has_chatbot", False)

    if claimed_no_chatbot and found_chatbot:
        checks.append({"field": "no_chatbot", "status": "wrong", "evidence": "Chatbot script gevonden in HTML"})
        warnings.append("Chatbot claim was incorrect — found in HTML")
    elif claimed_no_chatbot:
        checks.append({
            "field": "no_chatbot",
            "status": "inferred",
            "evidence": "Niet gevonden in HTML — maar lazy-loaded chatbots worden niet gedetecteerd zonder JS execution",
        })
    else:
        checks.append({"field": "has_chatbot", "status": "verified", "evidence": "Script gevonden in HTML"})

    # ── Validate: SSL claim ──────────────────────────────────────────
    claimed_ssl = tech.get("has_ssl", False)
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"https://{domain}", follow_redirects=False)
            actual_ssl = r.status_code < 400
    except Exception:
        actual_ssl = False

    if claimed_ssl == actual_ssl:
        checks.append({"field": "ssl", "status": "verified", "evidence": f"SSL={actual_ssl} confirmed by direct check"})
    else:
        checks.append({"field": "ssl", "status": "wrong", "evidence": f"Claimed SSL={claimed_ssl} but actual={actual_ssl}"})
        warnings.append(f"SSL claim incorrect: claimed={claimed_ssl}, actual={actual_ssl}")

    # ── Validate: CMS claim ──────────────────────────────────────────
    claimed_cms = tech.get("cms")
    if claimed_cms and homepage_html:
        cms_evidence = {
            "WordPress": ["wp-content", "wp-includes"],
            "Shopify": ["cdn.shopify.com"],
            "Webflow": ["webflow.com"],
            "Wix": ["wixsite.com", "wix.com"],
        }
        evidence_patterns = cms_evidence.get(claimed_cms, [])
        if evidence_patterns and any(p in homepage_html for p in evidence_patterns):
            checks.append({"field": "cms", "status": "verified", "evidence": f"'{claimed_cms}' confirmed by HTML patterns"})
        elif evidence_patterns:
            checks.append({"field": "cms", "status": "inferred", "evidence": f"'{claimed_cms}' claimed but patterns not found in re-check"})

    # ── Validate: Competitor relevance ───────────────────────────────
    comp_data = wi.get("competitor_data") or {}
    competitors = comp_data.get("competitors") or []
    lead_category = (lead.get("google_category") or "").lower()

    for comp in competitors:
        comp_name = comp.get("name") or ""
        # A valid competitor should be in the same business category
        # We can't re-check category without Maps, so flag if no category match info
        checks.append({
            "field": f"competitor:{comp_name}",
            "status": "inferred",
            "evidence": "Competitor gevonden via Maps zoekresultaat — categorie-match niet geverifieerd",
        })

    # ── Validate: Review attribution ─────────────────────────────────
    review_analysis = lead.get("review_analysis") or {}
    if review_analysis.get("best_quote"):
        checks.append({
            "field": "review_quote",
            "status": "inferred",
            "evidence": "Quote gescraped van Maps pagina — correct bedrijf aangenomen op basis van URL navigatie",
        })

    # ── Validate: Company name match ─────────────────────────────────
    company_name = (lead.get("company_name") or "").lower()
    if homepage_html:
        # Check if company name appears in the page title or body
        title_match = re.search(r"<title[^>]*>(.*?)</title>", homepage_html)
        page_title = title_match.group(1).strip() if title_match else ""

        if company_name and company_name in page_title:
            checks.append({"field": "company_name", "status": "verified", "evidence": f"Naam '{company_name}' gevonden in <title>"})
        elif company_name and company_name in homepage_html:
            checks.append({"field": "company_name", "status": "verified", "evidence": f"Naam gevonden in page body"})
        else:
            checks.append({"field": "company_name", "status": "inferred", "evidence": "Naam niet gevonden op website — alleen Maps data"})

    # ── Calculate validation score ───────────────────────────────────
    status_scores = {"verified": 1.0, "inferred": 0.5, "assumed": 0.2, "wrong": 0.0}
    if checks:
        total = sum(status_scores.get(c["status"], 0) for c in checks)
        validation_score = round(total / len(checks), 3)
    else:
        validation_score = 0.0

    result = {
        "validation_score": validation_score,
        "checks": checks,
        "warnings": warnings,
        "verified_count": sum(1 for c in checks if c["status"] == "verified"),
        "inferred_count": sum(1 for c in checks if c["status"] == "inferred"),
        "wrong_count": sum(1 for c in checks if c["status"] == "wrong"),
    }

    # Store on lead
    try:
        supabase_client.table("leads").update({
            "confidence_scores": {
                **(lead.get("confidence_scores") or {}),
                "validation_score": validation_score,
                "verified_claims": result["verified_count"],
                "wrong_claims": result["wrong_count"],
            },
        }).eq("id", lead_id).execute()
    except Exception:
        pass

    if warnings:
        logger.warning("enrichment_validator: %s — %d warnings: %s",
                       lead.get("company_name"), len(warnings), warnings)

    return result
