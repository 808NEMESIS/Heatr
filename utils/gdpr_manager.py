"""
utils/gdpr_manager.py — GDPR compliance utilities.

Implements:
  - forget_lead()     : right to erasure (Art. 17 GDPR)
  - export_lead_data(): right of access / data portability (Art. 15/20 GDPR)
  - generate_register(): Article 30 processing register

All operations are logged to gdpr_log table.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Fields replaced with anonymized placeholder on forget
_REDACTED = "VERWIJDERD"
# Legacy gedeelde placeholder (pre fase-2). Bewaard voor herkenning van oude
# rijen; NIET meer gebruikt voor nieuwe forgets — met de unique index op
# (workspace_id, lower(email)) uit migratie 021 botste de TWEEDE forget in
# een workspace op de constraint, werd de fout geslikt en bleef de echte
# PII staan terwijl de functie succes rapporteerde (audit v2, Art.17-fout).
_LEGACY_REDACTED_EMAIL = "verwijderd@anoniem.nl"


def _redacted_email(lead_id: str) -> str:
    """Per-lead-unieke, niet-herleidbare placeholder.

    - uniek per lead (volledige UUID) → geen unique-index-collision;
    - afgeleid van lead_id, niet van het originele adres → niet herleidbaar;
    - @anoniem.invalid (RFC 2606-gereserveerd TLD) → nooit afleverbaar;
    - stabiel per lead → een forget-retry schrijft dezelfde waarde
      (idempotent, geen nieuwe collision).
    """
    return f"verwijderd+{lead_id}@anoniem.invalid"


# ─────────────────────────────────────────────────────────────────────────────
# GDPR-GEGEVENSKAART (2026-07-19) — de ENE bron van waarheid voor erase + export.
#
# Elke tabel uit config.database._HEATR_TABLES moet hier geclassificeerd zijn —
# tests/test_gdpr_coverage.py faalt zodra een nieuwe tabel aan de allowlist
# wordt toegevoegd zonder classificatie. Zo kan de verwijder-dekking nooit meer
# stil achterlopen op het schema (de fout die deze kaart repareert: forget_lead
# was gebouwd voor het schema van vóór migratie 029-035; transcripten,
# lead_contacts, snapshots, network-log e.d. bleven staan).
#
# action:
#   delete       — harde delete van de rijen van de lead (via `by`)
#   redact       — kolommen leegmaken/anonimiseren, rij blijft (tel-/ledger-integriteit)
#   none         — geen persoonsgegevens (reden verplicht)
#   special      — maatwerk in forget_lead (leads zelf, storage, domein-caches)
# by: lead_id | domain | company_raw_id
# export: True = kategorie in het inzage-export-overzicht (spiegel van erase)
# ─────────────────────────────────────────────────────────────────────────────
GDPR_DATA_MAP: dict[str, dict] = {
    # kern
    "leads":                 {"action": "special", "export": True,
                              "reason": "redactie van alle contact-/persoonsvelden (zie _LEADS_REDACTION)"},
    "website_intelligence":  {"action": "redact", "by": "lead_id", "export": True,
                              "redact_columns": {"team_contacts": None, "personalization": None},
                              # claude_vision_analysis-kolom bestaat NIET in prod (drift);
                              # vision-tekst leeft in vision_cache (per domein gewist)
                              "reason": "namen/bio's + vrije AI-tekst; per-rij data-update, geen structuurwijziging (backfill-veilig)"},
    "enrichment_data":       {"action": "delete", "by": "lead_id", "export": True,
                              "reason": "page_text, e-mails, schema_org"},
    "lead_contacts":         {"action": "delete", "by": "lead_id", "export": True,
                              "reason": "full_name/title/bio — puur persoonsdata"},
    "companies_raw":         {"action": "special", "export": True,
                              "reason": "redactie phone + raw_data via leads.company_raw_id; bedrijfsnaam blijft"},
    # gespreks-/outreach-artefacten
    "call_records":          {"action": "delete", "by": "lead_id", "export": True,
                              "reason": "volledige transcripten + deelnemer-e-mails; niet zinvol te redigeren"},
    "lead_campaign_history": {"action": "delete", "by": "lead_id", "export": True, "reason": "outreach-historie"},
    "reply_inbox":           {"action": "delete", "by": "lead_id", "export": True, "reason": "reply-teksten"},
    "lead_outreach_snapshots": {"action": "redact", "by": "lead_id", "export": True,
                              "redact_columns": {"kvk_bestuurder_name": None},
                              "reason": "bestuurdersnaam eruit; snapshot-metriek blijft (tel-integriteit)"},
    "email_verifications":   {"action": "redact", "by": "lead_id", "export": True,
                              "redact_columns": {"email": "__REDACTED_EMAIL__"},
                              "reason": "ONTWERPCONFLICT append-only vs verwijderrecht: verwijderrecht wint; adres -> redactie-token, verificatie-uitkomst + timestamp blijven (audittrail-bedoeling intact)"},
    "website_network_log":   {"action": "delete", "by": "lead_id", "export": True,
                              "reason": "domein-URL's van (vaak) eenmanszaken = herleidbaar; bij twijfel = persoonsgegevens"},
    "audit_reports":         {"action": "delete", "by": "lead_id", "export": True,
                              "reason": "findings-bewijs bevat sitetekst-snippets (kunnen namen bevatten, bv. BIG-context)"},
    "webhook_events":        {"action": "redact", "by": "lead_id", "export": True,
                              "redact_columns": {"payload": None},
                              "reason": "payload bevat from_email/reply-fragmenten; event-rij blijft (ledger)"},
    "outbound_log":          {"action": "redact", "by": "lead_id", "export": True,
                              "redact_columns": {"result": None, "metadata": None},
                              "reason": "bevroren mailcontent bevat naam; ledger-rij (idempotency/status) blijft — zelfde afweging als email_verifications"},
    "crm_tasks":             {"action": "redact", "by": "lead_id", "export": True,
                              "redact_columns": {"title": "[verwijderd]"},
                              "reason": "taak-titel kan naam bevatten; status wordt apart op cancelled gezet"},
    "crm_deals":             {"action": "delete", "by": "lead_id", "export": True,
                              "reason": "deal-notities over de persoon"},
    "lead_timeline":         {"action": "special", "export": True,
                              "reason": "title -> generiek label, body/metadata leeg (rijen blijven: activiteit-integriteit)"},
    # domein-gekoppelde caches (herleidbaar bij eenmanszaak)
    "vision_cache":          {"action": "delete", "by": "domain", "export": False,
                              "reason": "vision-tekst over de site; domein-gekoppeld"},
    "domain_age_cache":      {"action": "delete", "by": "domain", "export": False, "reason": "domein-gekoppeld"},
    "meta_ads_cache":        {"action": "delete", "by": "domain", "export": False, "reason": "domein-gekoppeld"},
    "domain_cache":          {"action": "delete", "by": "domain", "export": False,
                              "reason": "catch-all/MX-cache per domein; domein-gekoppeld (herleidbaar bij eenmanszaak)"},
    # bewust behouden
    "suppressions":          {"action": "none", "export": False,
                              "reason": "BEWUST BEHOUDEN mét origineel adres: nodig om de erasure-belofte af te dwingen (nooit her-aanschrijven, ook na her-scrape). Hashing = latere verbetering"},
    "gdpr_log":              {"action": "none", "export": False,
                              "reason": "wettelijk bewijs dát en wanneer aan een verzoek is voldaan (Art. 5(2) accountability)"},
    # geen persoonsgegevens (hooguit lead_id-verwijzing naar een geredigeerde rij)
    "workspaces":            {"action": "none", "export": False, "reason": "config"},
    "sector_configs":        {"action": "none", "export": False, "reason": "config"},
    "scraping_jobs":         {"action": "none", "export": False, "reason": "job-administratie (sector/stad)"},
    "enrichment_jobs":       {"action": "none", "export": False, "reason": "job-administratie"},
    "blocked_sends":         {"action": "none", "export": False, "reason": "besluit-log, id-only"},
    "system_alerts":         {"action": "none", "export": False, "reason": "ops"},
    "daily_metrics":         {"action": "none", "export": False, "reason": "aggregaten"},
    "startup_log":           {"action": "none", "export": False, "reason": "ops"},
    "claude_cache":          {"action": "none", "export": False, "reason": "prompt-hash-cache; geen lead-koppeling"},
    "api_cost_log":          {"action": "none", "export": False, "reason": "kosten-telemetrie; lead_id verwijst na erase naar geredigeerde rij"},
    "competitor_cache":      {"action": "none", "export": False, "reason": "tabel bestaat niet in prod (bekende drift)"},
    "lead_discovery_schedules": {"action": "none", "export": False, "reason": "alleen sector/stad/frequentie"},
    "rate_limit_state":      {"action": "none", "export": False, "reason": "ops"},
    "system_state":          {"action": "none", "export": False, "reason": "ops"},
    "feedback_runs":         {"action": "none", "export": False, "reason": "aggregaten"},
    "campaigns":             {"action": "none", "export": False, "reason": "campagne-audit, id-only"},
    "import_runs":           {"action": "none", "export": False, "reason": "import-idempotency, id-only"},
    # bestaan (nog) niet in prod — graceful skip zolang afwezig
    "teardown_pages":        {"action": "delete", "by": "lead_id", "export": True,
                              "reason": "per-lead analyse-pagina (tabel nog niet in prod; skip-if-missing)"},
    "page_views":            {"action": "none", "export": False,
                              "reason": "ua-hash/referer zonder direct persoonsgegeven (tabel nog niet in prod)"},
}

# Leads-kolommen die bij erase geredigeerd worden. Uitbreiding 2026-07-19:
# alles rechts van 'company_summary' ontbrak in de oude flow.
_LEADS_REDACTION: dict[str, object] = {
    # oude lijst
    "contact_first_name": None, "contact_last_name": None, "phone": None,
    "linkedin_url": None, "personalized_opener": None, "company_summary": None,
    # 2026-07-19 — de gemiste velden (audit_compliance_verwerker.md)
    "contact_name": None, "contact_tussenvoegsel": None, "contact_title": None,
    "contact_linkedin_url": None, "contact_why_chosen": None, "contact_source": None,
    "review_best_quote": None, "review_pain_points": [],
    # arrays zijn NOT NULL in prod -> leegmaken i.p.v. NULL (E2E-vondst 2026-07-19)
    "personalization_hooks": [], "personalization_observations": [],
    "company_positioning": None, "kvk_bestuurder_name": None,
    "checkup_data": None, "crm_notes": None, "last_call_outcome": None,
}

# Storage-paden die bij erase verwijderd worden (per domein / per call-id).
_STORAGE_PATHS_DOMAIN = ["{domain}.png", "captures/{domain}/desktop.webp", "captures/{domain}/mobile.webp"]
_STORAGE_PATH_CHECKUP = "checkup-reports/{call_id}.pdf"


async def forget_lead(
    lead_id: str,
    workspace_id: str,
    supabase_client,
    performed_by: str = "user",
) -> dict[str, Any]:
    """
    Permanently remove all personal data for a lead.
    Retains anonymised statistical record for analytics.

    Steps (2026-07-19: kaart-gedreven — GDPR_DATA_MAP is de bron van waarheid):
    1. Capture email for GDPR audit trail before deletion
    2. Anonymise leads table (_LEADS_REDACTION: álle contact-/persoonsvelden,
       per-lead-unieke placeholder) — KRITIEK: faalt dit of matcht het 0 rijen,
       dan is ok=False (fail-loud)
    2b. Platformbrede suppressie van het originele adres
    3. Referenties vangen (domain, company_raw_id, call-ids) vóór de deletes
    4. Storage: {domain}.png + captures/{domain}/*.webp + checkup-reports/{call}.pdf
    5. lead_timeline: title/body strippen + metadata leegmaken
    6. crm_tasks annuleren
    7. companies_raw redigeren (phone, raw_data)
    8. GDPR_DATA_MAP-executor: alle delete/redact-tabellen (call_records,
       lead_contacts, snapshots, email_verifications, network_log, audit_reports,
       webhook_events, outbound_log, crm_deals, WI-kolommen, domein-caches, ...)
       — ontbrekende tabel = skip, fout op bestaande tabel = error (ok=False)
    9. Log to gdpr_log (audittrail; vereist migratie 036)

    Elke stap-fout belandt in `errors`; ok=True ALLEEN als alle stappen
    slaagden. Een retry is idempotent (placeholder is stabiel per lead).

    Returns:
        { "ok": bool, "deleted_records": int, "anonymized_records": int,
          "errors": list[str] }
    """
    deleted = 0
    anonymized = 0
    errors: list[str] = []

    # Step 1: get email before wiping
    lead_email: str | None = None
    try:
        res = supabase_client.table("leads").select("email, domain").eq("id", lead_id).maybe_single().execute()
        if res.data:
            lead_email = res.data.get("email")
    except Exception as e:
        logger.warning("forget_lead: could not read lead email: %s", e)

    # Step 2: anonymise leads row — de kritieke stap. Per-lead-unieke
    # placeholder (zie _redacted_email) i.p.v. één gedeeld adres, zodat de
    # unique index (workspace_id, lower(email)) nooit de anonimisering van
    # de tweede vergeten lead blokkeert.
    try:
        res = supabase_client.table("leads").update({
            **_LEADS_REDACTION,
            "email": _redacted_email(lead_id),
            "contact_first_name": _REDACTED,
            "status": "forgotten",
        }).eq("id", lead_id).eq("workspace_id", workspace_id).execute()
        if res.data:
            anonymized += 1
        else:
            errors.append("leads_anonymize: 0 rijen gematcht (verkeerde lead_id/workspace?)")
            logger.error("forget_lead: leads anonymize matchte 0 rijen (lead=%s ws=%s)",
                         lead_id, workspace_id)
    except Exception as e:
        errors.append(f"leads_anonymize: {e}")
        logger.error("forget_lead: leads anonymize failed: %s", e)

    # Step 2b: platformbrede suppressie van het ORIGINELE adres (fase 2 PR 7).
    # Onderdeel van de erasure-belofte: her-scrapen van hetzelfde bedrijf mag
    # dit adres nooit opnieuw aanschrijfbaar maken — in geen enkele workspace.
    # Idempotent (unique-conflict = al gesuppressed = ok).
    if lead_email:
        from utils.suppression import add_suppression
        sup = add_suppression(
            supabase_client, email=lead_email, suppression_type="forgotten",
            source="gdpr_forget", source_workspace_id=workspace_id,
            lead_id=lead_id, reason=f"Art.17 erasure door {performed_by}",
        )
        if not sup.get("ok"):
            errors.append(f"suppression_register: {sup.get('error')}")
    else:
        logger.warning("forget_lead: geen origineel e-mailadres beschikbaar — "
                       "suppressie-registratie overgeslagen (lead=%s)", lead_id)

    # Step 3: referenties vangen VÓÓR de deletes (verdwijnen anders):
    # domein (storage + domein-caches), company_raw_id (companies_raw-redactie),
    # call-ids (check-up-PDF's in Storage).
    lead_domain: str | None = None
    company_raw_id: str | None = None
    call_ids: list[str] = []
    try:
        lr = supabase_client.table("leads").select("domain").eq("id", lead_id).maybe_single().execute()
        if lr.data:
            lead_domain = (lr.data.get("domain") or "").strip() or None
    except Exception as e:
        logger.warning("forget_lead: kon domain niet lezen: %s", e)
    # company_raw_id apart (kolom bestaat niet in elke prod-DB — basis-schema-
    # drift, E2E-vondst 2026-07-19); fallback in stap 7 gaat via domain.
    try:
        lr2 = supabase_client.table("leads").select("company_raw_id").eq("id", lead_id).maybe_single().execute()
        if lr2.data:
            company_raw_id = lr2.data.get("company_raw_id")
    except Exception:
        pass
    try:
        cr = supabase_client.table("call_records").select("id").eq("lead_id", lead_id).execute()
        call_ids = [r["id"] for r in (cr.data or []) if r.get("id")]
    except Exception:
        pass  # tabel kan ontbreken; map-executor logt dat zelf

    # Step 4: Storage — ALLE bekende paden (oud png + nieuwe captures + check-up-PDF's).
    # De oude flow wiste alleen {domain}.png; de captures-WebP's en de check-up-
    # PDF's bleven staan (audit_compliance_verwerker.md) — screenshots kunnen
    # gezichten/teamfoto's bevatten.
    try:
        bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "screenshots")
        paths = []
        if lead_domain:
            paths += [p.format(domain=lead_domain) for p in _STORAGE_PATHS_DOMAIN]
        paths += [_STORAGE_PATH_CHECKUP.format(call_id=c) for c in call_ids]
        if paths:
            supabase_client.storage.from_(bucket).remove(paths)
            deleted += len(paths)
    except Exception as e:
        logger.debug("forget_lead: storage delete deels/geheel gefaald (bestanden kunnen ontbreken): %s", e)

    # Step 5: timeline — GENERIEK label i.p.v. heuristische PII-strip
    # (2026-07-19: _strip_pii is patroon-gebaseerd en liet vrije-tekst-PII in
    # titels staan; bij erase geldt "bij twijfel = persoonsgegevens" -> alles
    # plat. event_type blijft voor activiteit-statistiek; metadata leeg).
    try:
        res = supabase_client.table("lead_timeline").update(
            {"title": "[geanonimiseerd]", "body": None, "metadata": None}
        ).eq("lead_id", lead_id).execute()
        anonymized += len(res.data or [])
    except Exception as e:
        errors.append(f"timeline_anonymize: {e}")
        logger.warning("forget_lead: timeline anonymize failed: %s", e)

    # Step 6: crm_tasks annuleren (redactie van de titel doet de map-executor).
    # Graceful skip als de tabel ontbreekt (basis-schema-drift: heatr_crm_tasks
    # bestaat niet in elke prod-DB) — zelfde regel als de map-executor.
    try:
        supabase_client.table("crm_tasks").update({"status": "cancelled"}).eq(
            "lead_id", lead_id).eq("status", "open").execute()
    except Exception as e:
        msg = str(e)
        if "PGRST205" in msg or "Could not find the table" in msg:
            logger.info("forget_lead: crm_tasks ontbreekt in prod — overgeslagen")
        else:
            errors.append(f"task_cancellation: {msg[:120]}")
            logger.warning("forget_lead: task cancellation failed: %s", e)

    # Step 7: companies_raw-redactie (scrape-payload kan contactdata bevatten;
    # bedrijfsnaam blijft voor dedup/statistiek). Via company_raw_id als die
    # kolom/waarde bestaat, anders via (workspace_id, domain) — de kolom
    # ontbreekt in prod (basis-schema-drift).
    try:
        q = supabase_client.table("companies_raw").update({"phone": None, "raw_data": {}})
        if company_raw_id:
            q = q.eq("id", company_raw_id)
        elif lead_domain:
            q = q.eq("workspace_id", workspace_id).eq("domain", lead_domain)
        else:
            q = None
        if q is not None:
            res = q.execute()
            anonymized += len(res.data or [])
    except Exception as e:
        errors.append(f"companies_raw_redact: {e}")

    # Step 8: kaart-gedreven deletes/redacties — GDPR_DATA_MAP is de bron van
    # waarheid; alles met action delete/redact draait hier generiek. Ontbrekende
    # tabellen (bv. teardown_pages vóór migratie 029) worden als skip gelogd,
    # niet als fout — maar een fout op een BESTAANDE tabel telt als error
    # (fail-loud: ok=False), want een halve verwijdering mag nooit stil slagen.
    for table, spec in GDPR_DATA_MAP.items():
        action = spec.get("action")
        if action not in ("delete", "redact"):
            continue
        by = spec.get("by", "lead_id")
        if by == "domain":
            if not lead_domain:
                continue
            key_col, key_val = "domain", lead_domain
        else:
            key_col, key_val = "lead_id", lead_id
        try:
            if action == "delete":
                res = supabase_client.table(table).delete().eq(key_col, key_val).execute()
                deleted += len(res.data or [])
            else:
                cols = dict(spec.get("redact_columns") or {})
                for c, v in cols.items():
                    if v == "__REDACTED_EMAIL__":
                        cols[c] = _redacted_email(lead_id)
                res = supabase_client.table(table).update(cols).eq(key_col, key_val).execute()
                anonymized += len(res.data or [])
        except Exception as e:
            msg = str(e)
            if "PGRST205" in msg or "does not exist" in msg or "Could not find the table" in msg:
                logger.info("forget_lead: tabel %s ontbreekt in prod — overgeslagen", table)
            else:
                errors.append(f"{table}_{action}: {msg[:120]}")
                logger.warning("forget_lead: %s op %s faalde: %s", action, table, e)

    # Step 9: log to gdpr_log
    try:
        supabase_client.table("gdpr_log").insert({
            "workspace_id": workspace_id,
            "action": "forget",
            "lead_id": lead_id,
            "lead_email": lead_email or "unknown",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "performed_by": performed_by,
        }).execute()
    except Exception as e:
        errors.append(f"gdpr_log_write: {e}")
        logger.error("forget_lead: gdpr_log write failed: %s", e)

    ok = not errors
    if ok:
        logger.info("forget_lead: lead %s anonymized. deleted=%d anonymized=%d",
                    lead_id, deleted, anonymized)
    else:
        logger.error("forget_lead: lead %s ONVOLLEDIG vergeten — %d fout(en): %s",
                     lead_id, len(errors), "; ".join(errors)[:500])
    return {"ok": ok, "deleted_records": deleted,
            "anonymized_records": anonymized, "errors": errors}


async def export_lead_data(lead_id: str, supabase_client) -> dict[str, Any]:
    """
    Export all stored data for a lead as a structured dict.
    Used for GDPR right of access (Art. 15) and data portability (Art. 20).

    2026-07-19: kaart-gedreven — leest exact dezelfde tabellen als de erase
    (GDPR_DATA_MAP, export=True), zodat inzage en verwijdering nooit meer uit
    elkaar kunnen groeien. Als je iets kunt wissen maar niet kunt tonen, is
    het inzagerecht incompleet.
    """
    data: dict[str, Any] = {}

    # Lead core data (special in de map)
    try:
        res = supabase_client.table("leads").select("*").eq("id", lead_id).maybe_single().execute()
        data["lead"] = res.data or {}
    except Exception as e:
        data["lead"] = {"error": str(e)}

    # companies_raw via company_raw_id (special)
    try:
        crid = (data.get("lead") or {}).get("company_raw_id")
        if crid:
            res = supabase_client.table("companies_raw").select("*").eq("id", crid).maybe_single().execute()
            data["companies_raw"] = res.data or {}
    except Exception:
        data["companies_raw"] = {}

    # Alle map-tabellen met export=True (behalve de specials hierboven)
    for table, spec in GDPR_DATA_MAP.items():
        if not spec.get("export") or spec.get("action") == "special" and table in ("leads", "companies_raw"):
            continue
        if table in ("leads", "companies_raw"):
            continue
        try:
            res = supabase_client.table(table).select("*").eq("lead_id", lead_id).execute()
            data[table] = res.data or []
        except Exception as e:
            msg = str(e)
            if "PGRST205" in msg or "Could not find the table" in msg:
                data[table] = []  # tabel bestaat (nog) niet in prod
            else:
                data[table] = [{"error": msg[:120]}]

    data["exported_at"] = datetime.now(timezone.utc).isoformat()
    # 2026-07-19 (Sami-keuze): de bewaartermijn-belofte ("2 jaar, daarna
    # automatisch geanonimiseerd") is VERWIJDERD uit deze tekst — er bestaat
    # geen purge-job die 'm waarmaakt. Terugzetten mag pas mét zo'n job.
    data["gdpr_note"] = (
        "Dit overzicht bevat alle persoonsgegevens die Heatr heeft opgeslagen voor deze lead. "
        "Rechtsgrond: gerechtvaardigd belang (B2B commercieel contact). "
        "Verwijdering op verzoek via de beheerder (Art. 17). "
        "Externe verwerkers: Supabase (opslag), Anthropic (AI-analyse), "
        "Bouncer (e-mailverificatie), Warmr (e-mailverzending)."
    )

    return data


def generate_processing_register() -> dict[str, Any]:
    """
    Generate Article 30 GDPR processing register.
    Describes what personal data Heatr processes, why, and for how long.
    """
    return {
        "organisation": "Aerys",
        "tool": "Heatr",
        "dpo_contact": os.getenv("OPERATOR_EMAIL", "privacy@aerys.nl"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "processing_activities": [
            {
                "name": "B2B Lead Discovery",
                "purpose": "Identificeren van potentiële zakelijke klanten voor Aerys diensten",
                "legal_basis": "Gerechtvaardigd belang (B2B commercieel contact, Art. 6(1)(f) AVG)",
                "data_categories": [
                    "Bedrijfsnaam en vestigingsadres",
                    "Zakelijk emailadres (role-based: info@, contact@)",
                    "Telefoonnummer (zakelijk)",
                    "KvK-nummer en SBI-code",
                    "Contactpersoon naam (alleen zakelijke context)",
                    "Website URL en publiek beschikbare website-inhoud",
                    "Google Maps rating en reviewcount (publiek beschikbaar)",
                ],
                "data_subjects": "Eigenaren/contactpersonen van MKB-bedrijven in BENELUX",
                "recipients": [
                    {"name": "Supabase", "role": "Processor", "country": "EU", "purpose": "Database opslag"},
                    {"name": "Anthropic", "role": "Processor", "country": "US (SCCs)", "purpose": "AI-analyse van website en bedrijfsprofiel"},
                    {"name": "Bouncer (usebouncer.com)", "role": "Processor", "country": "EU", "purpose": "E-mailverificatie"},
                    {"name": "Warmr", "role": "Processor", "country": "EU", "purpose": "Email verzending"},
                ],
                # 2026-07-19 (Sami-keuze): automatische-anonimisering-belofte verwijderd —
                # er is geen purge-job. Terugzetten mag pas mét zo'n job.
                "retention": "Handmatig beheerd; verwijdering op verzoek (Art. 17). Geen geautomatiseerde verwijdering geconfigureerd.",
                "security_measures": [
                    "Row Level Security in Supabase per workspace",
                    "Encrypted at rest (Supabase AES-256)",
                    "GDPR-veiligheidscheck voor elke outreach",
                    "Alleen zakelijke role emails verwerkt (geen persoonlijke adressen)",
                    "Unsubscribe verwerkt binnen 60 seconden",
                ],
            },
            {
                "name": "Outreach Campagnes",
                "purpose": "Sturen van gepersonaliseerde zakelijke emails namens Aerys",
                "legal_basis": "Gerechtvaardigd belang (B2B cold outreach, Art. 6(1)(f) AVG)",
                "data_categories": ["Zakelijk emailadres", "Voornaam (indien zakelijk beschikbaar)", "Bedrijfsnaam", "Stad"],
                "recipients": [{"name": "Warmr", "role": "Processor", "country": "EU", "purpose": "Email verzending en inbox warming"}],
                "retention": "Handmatig beheerd; verwijdering op verzoek (Art. 17). Geen geautomatiseerde verwijdering geconfigureerd.",
                "security_measures": ["Unsubscribe verwerkt binnen 60 seconden", "Bounce handling binnen 5 minuten"],
            },
        ],
        "rights_info": {
            "access": "POST /gdpr/export/{lead_id}",
            "erasure": "POST /gdpr/forget/{lead_id}",
            "contact": os.getenv("OPERATOR_EMAIL", "privacy@aerys.nl"),
            "supervisory_authority": "Autoriteit Persoonsgegevens (autoriteitpersoonsgegevens.nl)",
        },
    }


def _strip_pii(text: str) -> str:
    """Remove emails and common name patterns from text for timeline anonymization."""
    # Strip email addresses
    text = re.sub(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", "[email]", text)
    # Strip phone numbers
    text = re.sub(r"\+?[\d\s\-\(\)]{9,15}", "[telefoon]", text)
    return text
