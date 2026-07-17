"""
config/database.py — Database table name configuration for Heatr.

All Heatr table references go through this module. When Heatr shares a
database with Warmr, tables are prefixed with 'heatr_' to avoid conflicts.

When Heatr gets its own Supabase project, set HEATR_TABLE_PREFIX="" in .env
and all table names resolve to their unprefixed form.

Two usage patterns:

    # Pattern 1: Table name lookup
    from config.database import T
    db.table(T.leads).select("*")...     # → "heatr_leads" or "leads"

    # Pattern 2: Wrapped Supabase client (auto-prefixes all .table() calls)
    from config.database import get_heatr_supabase
    db = get_heatr_supabase()
    db.table("leads").select("*")...     # → automatically queries "heatr_leads"
"""
from __future__ import annotations

import os
from typing import Any

TABLE_PREFIX = os.getenv("HEATR_TABLE_PREFIX", "heatr_")

# Tables that belong to Heatr (will be prefixed)
_HEATR_TABLES = {
    "workspaces", "sector_configs", "companies_raw", "leads", "lead_contacts",
    "website_intelligence", "enrichment_data", "scraping_jobs", "enrichment_jobs",
    "lead_campaign_history", "lead_timeline", "crm_tasks", "crm_deals",
    "reply_inbox", "blocked_sends", "system_alerts", "gdpr_log", "daily_metrics",
    "startup_log", "claude_cache", "api_cost_log", "competitor_cache",
    "lead_discovery_schedules", "lead_outreach_snapshots", "rate_limit_state",
    # Added by APPLY_ME_IN_SUPABASE.sql migration
    "domain_age_cache", "meta_ads_cache", "vision_cache", "system_state",
    # Feedback loop (migration 012)
    "feedback_runs",
    # Campaign audit-trail (migration 013)
    "campaigns",
    # Import idempotency (migration 016)
    "import_runs",
    # Outbound side-effect-ledger + idempotency (migration 020 + 022).
    # KRITIEK: ontbrak hier tot 2026-07-10, waardoor de dispatcher naar de
    # ongeprefixte (niet-bestaande) tabel schreef en idempotency fail-open
    # was op elk send-pad (audit v2, P0-1).
    "outbound_log",
    # Platformbrede suppressielijst (migration 024, fase 2 PR 7)
    "suppressions",
    # Inbound webhook-eventledger (migration 026, fase 3 PR 10)
    "webhook_events",
    # Teardown-pagina's + intent-view-log (migration 029, fase A PR A1)
    "teardown_pages",
    "page_views",
    # E-mailverificatie-diagnostiek (migration 030) + catch-all-cache (031).
    # 'domain_cache' ontbrak → catch-all-write faalde stil (PGRST205).
    "email_verifications",
    "domain_cache",
    # Check-up follow-up (migration 032) — anders queryt de wrapper de
    # ongeprefixte (niet-bestaande) call_records → PGRST205.
    "call_records",
    # Crawl-instrumentatie (migration 033) — append-only netwerk-log voor de
    # pre/post-consent tracking-check.
    "website_network_log",
    # Prospect-facing website-audit (migration 035) — append-only, versioned.
    "audit_reports",
}


class _Tables:
    """Table name resolver. Access as attributes — returns prefixed name."""

    def __getattr__(self, name: str) -> str:
        if name.startswith("_"):
            raise AttributeError(name)
        return f"{TABLE_PREFIX}{name}"

    def __repr__(self) -> str:
        return f"Tables(prefix='{TABLE_PREFIX}')"


# Singleton — import this everywhere
T = _Tables()


def prefixed(table_name: str) -> str:
    """Return the prefixed table name if it's a Heatr table."""
    if table_name in _HEATR_TABLES:
        return f"{TABLE_PREFIX}{table_name}"
    return table_name


class HeatrSupabaseWrapper:
    """Wrapper around Supabase client that auto-prefixes Heatr table names.

    This means existing code using db.table("leads") will automatically
    query "heatr_leads" without any code changes needed.
    """

    def __init__(self, client: Any):
        self._client = client

    def table(self, name: str) -> Any:
        return self._client.table(prefixed(name))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)


_heatr_client: Any = None


def get_heatr_supabase() -> HeatrSupabaseWrapper:
    """Return a wrapped Supabase client that auto-prefixes Heatr tables.

    Uses SUPABASE_URL and SUPABASE_KEY from environment.
    """
    global _heatr_client
    if _heatr_client is None:
        from supabase import create_client
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_KEY"]
        raw_client = create_client(url, key)
        _heatr_client = HeatrSupabaseWrapper(raw_client)
    return _heatr_client
