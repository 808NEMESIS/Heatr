"""
utils/lead_import.py — CSV/JSON lead-import met dedup.

Dedup-pipeline (eerste hit wint, never queries silent — log de match):
  1. email exact match (lowercased)
  2. domain match (gen normaliseerd: lowercase, www-stripped)
  3. kvk_number exact match
  4. (company_name + city) fuzzy match — Levenshtein ratio ≥ 0.92

Returns: {imported: list, duplicates: list, errors: list}.

Niet vooraf-validerend op complete velden: minimum is dat er ÉÉN matchbaar
veld is (email, domain, of kvk_number). Anders kan dedup niet werken en
wordt de row als 'error' gemarkeerd.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any

logger = logging.getLogger(__name__)


_ALLOWED_FIELDS = {
    "company_name", "domain", "email", "city", "phone",
    "sector", "contact_first_name", "contact_last_name",
    "kvk_number", "google_maps_url", "google_rating",
    "google_review_count", "google_category",
}


def _normalize_domain(d: str | None) -> str | None:
    if not d:
        return None
    d = d.strip().lower()
    d = re.sub(r"^https?://", "", d)
    d = re.sub(r"^www\.", "", d)
    d = d.rstrip("/")
    return d or None


def _normalize_email(e: str | None) -> str | None:
    if not e:
        return None
    e = e.strip().lower()
    if "@" not in e:
        return None
    return e


def _normalize_company_city(name: str | None, city: str | None) -> str | None:
    if not name:
        return None
    n = name.strip().lower()
    n = re.sub(r"\s+(b\.?v\.?|bv|n\.?v\.?|nv|vof|holding)$", "", n)
    n = re.sub(r"[^\w\s]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    if city:
        return f"{n}|{city.strip().lower()}"
    return n


def _fuzzy_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _row_dedup_match(
    row: dict[str, Any], existing_leads: list[dict],
) -> tuple[dict | None, str | None]:
    """Return (matched_lead, match_type) of (None, None) als geen dubbel.

    match_type: 'email' | 'domain' | 'kvk' | 'fuzzy_company_city'
    """
    norm_email = _normalize_email(row.get("email"))
    norm_domain = _normalize_domain(row.get("domain"))
    norm_kvk = (row.get("kvk_number") or "").strip()
    norm_company_city = _normalize_company_city(row.get("company_name"), row.get("city"))

    for lead in existing_leads:
        if norm_email and _normalize_email(lead.get("email")) == norm_email:
            return lead, "email"
    for lead in existing_leads:
        if norm_domain and _normalize_domain(lead.get("domain")) == norm_domain:
            return lead, "domain"
    for lead in existing_leads:
        if norm_kvk and norm_kvk and (lead.get("kvk_number") or "").strip() == norm_kvk:
            return lead, "kvk"
    if norm_company_city:
        for lead in existing_leads:
            other = _normalize_company_city(lead.get("company_name"), lead.get("city"))
            if not other:
                continue
            if _fuzzy_ratio(norm_company_city, other) >= 0.92:
                return lead, "fuzzy_company_city"
    return None, None


def _validate_row(row: dict[str, Any]) -> str | None:
    """Return error message of None als ok."""
    has_match_field = any([
        _normalize_email(row.get("email")),
        _normalize_domain(row.get("domain")),
        (row.get("kvk_number") or "").strip(),
        row.get("company_name"),
    ])
    if not has_match_field:
        return "row mist alle dedup-velden (email, domain, kvk, company_name)"
    return None


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    """Filter to allowed fields + normalize known ones."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if k not in _ALLOWED_FIELDS:
            continue
        if v is None or (isinstance(v, str) and not v.strip()):
            continue
        if k == "domain":
            out[k] = _normalize_domain(v)
        elif k == "email":
            out[k] = _normalize_email(v)
        elif k in ("google_rating",):
            try:
                out[k] = float(v)
            except (TypeError, ValueError):
                continue
        elif k in ("google_review_count",):
            try:
                out[k] = int(v)
            except (TypeError, ValueError):
                continue
        else:
            out[k] = v.strip() if isinstance(v, str) else v
    return out


# Schatting van Claude-cost per volledige enrichment van 1 lead.
# Som van Haiku-calls: company_enrichment, owner_extract, treatment_focus,
# archetype + Sonnet-vision (1 screenshot). Komt uit observatie van eerdere runs.
ESTIMATED_ENRICHMENT_COST_EUR = 0.005

# Boven deze drempel beschouwen we auto-enrich queue-faal als systeemfout
# (return als top-level fatal ipv silent log). Voorkomt "100 imported, 0 enriched"
# zonder duidelijk signaal.
QUEUE_FAILURE_THRESHOLD = 0.10  # 10%

# Merge-strategieën bij duplicate-detectie
MERGE_SKIP        = "skip"          # Default: nieuwe data wordt genegeerd
MERGE_FILL_BLANKS = "fill_blanks"   # Vul lege velden op bestaande lead bij
MERGE_OVERWRITE   = "overwrite"     # Schrijft alle non-null nieuwe velden over (gevaarlijk)
ALLOWED_MERGE_STRATEGIES = {MERGE_SKIP, MERGE_FILL_BLANKS, MERGE_OVERWRITE}

# Velden die NOOIT overschreven mogen worden, ook niet via overwrite-strategy.
# Reden: deze worden door enrichment-pipeline gegenereerd op basis van actuele
# website/data. Een import-CSV met lege score-kolom zou anders alle bestaande
# scoring-data wegvagen.
_PROTECTED_ON_OVERWRITE = {
    "id", "workspace_id",                      # immutable
    "score", "fit_score", "data_quality_score",  # scoring output
    "personalization_potential", "reachability_score",
    "archetype", "archetype_reason", "archetype_confidence",
    "website_score", "visual_score",
    "personalized_opener", "company_summary",  # Claude-gegenereerd
    "manual_status_override", "manual_status_override_reason",
    "imported_at", "imported_by",              # audit trail
    "created_at",
}


def _is_blank(value: Any) -> bool:
    """True als value None, lege string, of alleen whitespace.

    Nodig omdat Postgres `""` en `" "` retourneert ipv NULL voor lege velden,
    en mijn fill_blanks-check wil die als "leeg" beschouwen.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def estimate_import_cost(row_count: int) -> dict[str, Any]:
    """Voorspelt enrichment-cost zodat UI dat vóór commit kan tonen."""
    return {
        "row_count": row_count,
        "cost_per_lead_eur": ESTIMATED_ENRICHMENT_COST_EUR,
        "estimated_total_eur": round(row_count * ESTIMATED_ENRICHMENT_COST_EUR, 4),
        "note": "Schatting o.b.v. gemiddelde Haiku+Sonnet calls in volledige pipeline",
    }


async def import_leads(
    rows: list[dict[str, Any]],
    workspace_id: str,
    supabase_client: Any,
    imported_by: str = "unknown",
    source: str = "csv",
    dry_run: bool = False,
    auto_enrich: bool = True,
    import_run_id: str | None = None,
    merge_strategy: str = MERGE_SKIP,
) -> dict[str, Any]:
    """Import leads met dedup + optionele auto-enqueue voor enrichment.

    Args:
        rows: list of dicts uit CSV/JSON
        workspace_id: target workspace
        imported_by: principal voor audit (e.g. 'user:foo@bar.com')
        source: 'csv' | 'manual' | 'json_api'
        dry_run: als True, doet alleen detectie en returnt zonder writes
        auto_enrich: als True (default), elke nieuwe lead wordt direct in de
                    enrichment-queue gezet (zelfde stappen als gescrapte leads).
        import_run_id: client-side gegenereerde UUID. Tweede call met zelfde
                    id binnen 24u → returnt cached resultaat. Voorkomt dubbel-
                    klikken-corruption. Optioneel — als None, geen idempotency.
        merge_strategy: bij duplicate detection — 'skip' (negeer nieuwe data,
                    standaard veilig), 'fill_blanks' (vul ontbrekende velden
                    op bestaande lead bij — meest waardevol bij CSV met aanvullende
                    info), 'overwrite' (schrijf alle nieuwe non-null velden — gevaarlijk).
    """
    # ============================================================
    # Idempotency check — heeft client al een succesvolle run met deze id?
    # Returnt slim-cached resultaat (summary + counts, geen detail-arrays).
    # ============================================================
    if import_run_id and not dry_run:
        try:
            existing = (
                supabase_client.table("import_runs")
                .select("result, completed_at, imported_by")
                .eq("id", import_run_id)
                .eq("workspace_id", workspace_id)
                .maybe_single()
                .execute()
            )
            if existing and existing.data and existing.data.get("result") and existing.data.get("completed_at"):
                # Cross-user safety: alleen replay als zelfde principal de run startte
                cached_by = existing.data.get("imported_by")
                if cached_by and cached_by != imported_by:
                    logger.warning(
                        "import_run_id %s claimed by %s but cached by %s — refusing replay",
                        import_run_id, imported_by, cached_by,
                    )
                else:
                    cached = existing.data["result"]
                    # Cached result is "slim" — wrap in een full-shape response
                    return {
                        "imported": [],         # detail uit cache niet bewaard, zie imported_lead_ids
                        "duplicates": [],
                        "errors": cached.get("first_errors_sample", []),
                        "fatal": cached.get("fatal"),
                        "summary": cached.get("summary", {}),
                        "cost_estimate": cached.get("cost_estimate"),
                        "queue_failure_samples": cached.get("first_queue_failures_sample", []),
                        "idempotent_replay": True,
                        "cached_imported_lead_ids": cached.get("imported_lead_ids", []),
                        "cached_duplicate_breakdown": cached.get("duplicate_count_by_match_type", {}),
                        "cached_merge_breakdown": cached.get("duplicate_count_by_merge_action", {}),
                    }
        except Exception as e:
            logger.debug("import_runs idempotency lookup failed (table missing?): %s", e)

    # Validate merge_strategy
    if merge_strategy not in ALLOWED_MERGE_STRATEGIES:
        return {
            "imported": [], "duplicates": [], "errors": [],
            "fatal": f"merge_strategy moet zijn: {sorted(ALLOWED_MERGE_STRATEGIES)}",
        }
    if len(rows) > 500:
        return {
            "imported": [], "duplicates": [], "errors": [],
            "fatal": "te veel rows in één call (max 500). Splits in batches.",
        }

    # Pre-fetch ALL leads voor dedup. Bij 1000-leads workspace is dit ~50KB,
    # 1 query — sneller dan N losse lookups per row.
    try:
        existing = (
            supabase_client.table("leads")
            .select("id, company_name, domain, email, city, kvk_number")
            .eq("workspace_id", workspace_id)
            .execute()
        )
        existing_leads = existing.data or []
    except Exception as e:
        return {
            "imported": [], "duplicates": [], "errors": [],
            "fatal": f"kon bestaande leads niet ophalen: {e}",
        }

    imported: list[dict] = []
    duplicates: list[dict] = []
    errors: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for idx, raw_row in enumerate(rows):
        # Validate
        err = _validate_row(raw_row)
        if err:
            errors.append({"row_index": idx, "row": raw_row, "error": err})
            continue

        clean = _clean_row(raw_row)

        # Dedup
        matched, match_type = _row_dedup_match(clean, existing_leads)
        if matched:
            dup_entry = {
                "row_index": idx,
                "row": clean,
                "matched_lead_id": matched.get("id"),
                "matched_company": matched.get("company_name"),
                "match_type": match_type,
                "merge_action": "skipped",
            }

            # Merge-strategieën: alleen toepassen als er DAADWERKELIJK iets te vullen is
            # en niet in dry_run modus.
            if merge_strategy in (MERGE_FILL_BLANKS, MERGE_OVERWRITE) and not dry_run:
                merge_patch: dict[str, Any] = {}
                for k, v in clean.items():
                    # Protect scoring-output + audit-velden tegen overschrijven
                    if k in _PROTECTED_ON_OVERWRITE:
                        continue
                    if _is_blank(v):
                        continue
                    existing_val = matched.get(k)
                    if merge_strategy == MERGE_FILL_BLANKS:
                        # Vul alleen velden die op bestaande lead leeg zijn (whitespace = leeg)
                        if _is_blank(existing_val):
                            merge_patch[k] = v
                    else:  # OVERWRITE — alleen als waarde echt verschilt
                        if v != existing_val:
                            merge_patch[k] = v
                if merge_patch:
                    try:
                        supabase_client.table("leads").update(merge_patch).eq("id", matched["id"]).execute()
                        dup_entry["merge_action"] = merge_strategy
                        dup_entry["merged_fields"] = list(merge_patch.keys())
                    except Exception as e:
                        dup_entry["merge_action"] = "merge_failed"
                        dup_entry["merge_error"] = str(e)
            duplicates.append(dup_entry)
            continue

        # New lead — bouw insert payload
        insert_payload = {
            **clean,
            "workspace_id": workspace_id,
            "status": "discovered",
            "source": source,
            "imported_at": now,
            "imported_by": imported_by,
            "imported_source": source,
        }

        if dry_run:
            imported.append({"row_index": idx, "row": insert_payload, "would_insert": True})
            continue

        try:
            res = supabase_client.table("leads").insert(insert_payload).execute()
            if res.data:
                new_lead = res.data[0]
                imported.append({
                    "row_index": idx,
                    "lead_id": new_lead.get("id"),
                    "row": clean,
                    "enrichment_queued": False,  # gezet hieronder
                })
                # Opname in existing_leads zodat duplicaten BINNEN dezelfde import ook gevangen worden
                existing_leads.append({
                    "id": new_lead.get("id"),
                    "company_name": clean.get("company_name"),
                    "domain": clean.get("domain"),
                    "email": clean.get("email"),
                    "city": clean.get("city"),
                    "kvk_number": clean.get("kvk_number"),
                })
        except Exception as e:
            errors.append({"row_index": idx, "row": clean, "error": f"insert failed: {e}"})

    # Auto-enqueue voor enrichment — zet elke geïmporteerde lead op de queue
    # zodat de worker dezelfde stappen draait als bij gescrapte leads.
    # Error-aggregation: als ≥10% van queue-calls faalt → top-level fatal.
    enriched_count = 0
    queue_failures: list[dict[str, str]] = []
    if auto_enrich and imported and not dry_run:
        from job_queue.enrichment_queue import queue_lead_for_enrichment
        for entry in imported:
            lead_id = entry.get("lead_id")
            if not lead_id:
                continue
            try:
                job_id = await queue_lead_for_enrichment(
                    lead_id=lead_id,
                    workspace_id=workspace_id,
                    priority=5,  # normale priority — gescrapte leads krijgen deze ook
                    supabase_client=supabase_client,
                )
                if job_id:
                    entry["enrichment_queued"] = True
                    entry["enrichment_job_id"] = job_id
                    enriched_count += 1
                else:
                    queue_failures.append({"lead_id": lead_id, "error": "queue_lead_for_enrichment returned None"})
            except Exception as e:
                queue_failures.append({"lead_id": lead_id, "error": str(e)[:200]})
                logger.warning("auto-enqueue failed for lead %s: %s", lead_id, e)

    # Detect systemic queue failure
    fatal_msg: str | None = None
    if auto_enrich and imported and not dry_run:
        failure_rate = len(queue_failures) / max(len(imported), 1)
        if failure_rate >= QUEUE_FAILURE_THRESHOLD:
            fatal_msg = (
                f"Auto-enrich faalde voor {len(queue_failures)}/{len(imported)} "
                f"leads ({int(failure_rate*100)}%). Mogelijk ontbreekt heatr_enrichment_jobs "
                f"of is de worker offline. Eerste fout: {queue_failures[0].get('error', 'onbekend')}"
            )

    cost_estimate = estimate_import_cost(len(imported))

    result = {
        "imported": imported,
        "duplicates": duplicates,
        "errors": errors,
        "fatal": fatal_msg,
        "summary": {
            "total_rows": len(rows),
            "imported_count": len(imported),
            "duplicate_count": len(duplicates),
            "error_count": len(errors),
            "enrichment_queued_count": enriched_count,
            "enrichment_queue_failures": len(queue_failures),
            "auto_enrich": auto_enrich,
            "merge_strategy": merge_strategy,
        },
        "cost_estimate": cost_estimate,
        "queue_failure_samples": queue_failures[:5],   # Eerste 5 voor debugging
        "idempotent_replay": False,
    }

    # Idempotency persist — slim opslag: alleen summary + counts, geen detail-arrays.
    # Anders groeit de tabel ~150KB per import en raakt Supabase ruimte snel vol.
    # Replay-gebruik heeft genoeg aan summary + lead_id-list om te bevestigen "ja
    # dit is al gebeurd"; de UI hoeft niet de hele detail-trail opnieuw te tonen.
    if import_run_id and not dry_run:
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            slim_result = {
                "summary": result["summary"],
                "fatal": result["fatal"],
                "imported_lead_ids": [e.get("lead_id") for e in result["imported"] if e.get("lead_id")],
                "duplicate_count_by_match_type": _count_by_key(result["duplicates"], "match_type"),
                "duplicate_count_by_merge_action": _count_by_key(result["duplicates"], "merge_action"),
                "error_count_by_type": _count_by_key(result["errors"], "error", truncate=60),
                "cost_estimate": result.get("cost_estimate"),
                # Sample voor debugging — eerste 5 errors only
                "first_errors_sample": result["errors"][:5],
                "first_queue_failures_sample": result.get("queue_failure_samples", [])[:5],
            }
            supabase_client.table("import_runs").upsert({
                "id": import_run_id,
                "workspace_id": workspace_id,
                "started_at": now_iso,
                "completed_at": now_iso,
                "result": slim_result,
                "row_count": len(rows),
                "imported_by": imported_by,
            }, on_conflict="id").execute()
        except Exception as e:
            logger.debug("import_runs persist failed: %s", e)

    return result


def _count_by_key(items: list[dict], key: str, truncate: int | None = None) -> dict[str, int]:
    """Tel hoeveel items per uniek waarde voor `key`. Voor cache-summarization."""
    counts: dict[str, int] = {}
    for item in items:
        v = item.get(key) or "(none)"
        v_str = str(v)[:truncate] if truncate else str(v)
        counts[v_str] = counts.get(v_str, 0) + 1
    return counts
