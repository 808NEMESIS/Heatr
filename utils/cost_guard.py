"""
utils/cost_guard.py — Hard cost-ceilings voor enrichment pipeline.

Twee guards:
  1. Daily budget kill-switch  — ENRICHMENT_DAILY_BUDGET_EUR (default €0.50)
  2. Per-lead cost ceiling     — MAX_COST_PER_LEAD_EUR (default €0.05)

Beide zijn HARDE stops: bij overschrijding wordt de Claude/externe call geweigerd.
De pipeline crasht niet — de lead krijgt een `enrichment_blocked_reason` en gaat
door naar niet-Claude steps (HTML parsing, database updates, etc.).

Design:
  - Daily budget: 1 Supabase query per call-site (geen per-request cache om
    multi-worker safety te houden). Query draait een sum over heatr_api_cost_log
    van vandaag.
  - Per-lead ceiling: accumulator in-memory per enrichment run, doorgegeven
    via `LeadCostAccumulator` context.
  - Beide guards schrijven een `cost_guard_events` log-rij bij een block
    voor analytics (nooit een hit silenter laten verdwijnen).

Env vars:
  ENRICHMENT_DAILY_BUDGET_EUR   default "0.50"
  MAX_COST_PER_LEAD_EUR         default "0.05"
  COST_GUARD_DISABLED           default "false"  (only for integration tests)
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _daily_budget_eur() -> float:
    try:
        return float(os.environ.get("ENRICHMENT_DAILY_BUDGET_EUR", "0.50"))
    except ValueError:
        return 0.50


def _monthly_budget_eur() -> float:
    """Total maandelijkse cap voor enrichment + Claude calls.
    Default €20 — kan omhoog via env MONTHLY_BUDGET_EUR.
    """
    try:
        return float(os.environ.get("MONTHLY_BUDGET_EUR", "20.00"))
    except ValueError:
        return 20.00


def _per_lead_ceiling_eur() -> float:
    try:
        return float(os.environ.get("MAX_COST_PER_LEAD_EUR", "0.05"))
    except ValueError:
        return 0.05


def _disabled() -> bool:
    return os.environ.get("COST_GUARD_DISABLED", "false").lower() == "true"


@dataclass
class LeadCostAccumulator:
    """Track cumulative cost for one enrichment run on a single lead.

    Workers instantiate one per lead. Every enrichment step that spends money
    calls `.charge(cost_eur, context)` after the call — or `.check_allowed()`
    before it. Exceeding the per-lead ceiling sets `.blocked = True` and
    subsequent `.check_allowed()` calls return False.
    """

    lead_id: str
    workspace_id: str
    ceiling_eur: float = field(default_factory=_per_lead_ceiling_eur)
    spent_eur: float = 0.0
    blocked: bool = False
    blocked_reason: str | None = None

    def check_allowed(self, estimated_cost_eur: float = 0.0) -> tuple[bool, str]:
        """Return (allowed, reason). Use BEFORE a Claude/external call."""
        if _disabled():
            return True, ""
        if self.blocked:
            return False, self.blocked_reason or "per_lead_ceiling_exceeded"
        if self.spent_eur + estimated_cost_eur > self.ceiling_eur:
            self.blocked = True
            self.blocked_reason = (
                f"per_lead_ceiling_exceeded: spent={self.spent_eur:.6f} "
                f"+estimated={estimated_cost_eur:.6f} > cap={self.ceiling_eur:.6f}"
            )
            return False, self.blocked_reason
        return True, ""

    def charge(self, cost_eur: float, context: str) -> None:
        """Record actual cost AFTER a successful call."""
        self.spent_eur += float(cost_eur or 0)
        if self.spent_eur > self.ceiling_eur and not self.blocked:
            self.blocked = True
            self.blocked_reason = (
                f"per_lead_ceiling_exceeded_after_charge: "
                f"spent={self.spent_eur:.6f} > cap={self.ceiling_eur:.6f} "
                f"(last context={context})"
            )
            logger.warning(
                "cost_guard: lead %s exceeded per-lead cap after %s — blocked",
                self.lead_id, context,
            )



def _cost_sum(supabase_client: Any, workspace_id: str | None, since_iso: str) -> float:
    """Kosten-som via de heatr_cost_sum-RPC (migratie 027) — één geïndexeerd
    aggregaat i.p.v. de oude full-scan-plus-Python-som die bij elke AI-call
    alle maandrijen ophaalde (audit v2 P1-2, O(n²) over een batch).

    workspace_id=None → platformbrede som (globaal budget, PR 16).
    Raises bij DB/RPC-fout — alle callers zijn fail-closed.
    """
    res = supabase_client.rpc(
        "heatr_cost_sum", {"p_workspace": workspace_id, "p_since": since_iso}
    ).execute()
    data = res.data
    if isinstance(data, list):
        data = data[0] if data else 0
    return float(data or 0)


def _platform_budget_eur(period: str) -> float:
    """PLATFORM_DAILY_BUDGET_EUR / PLATFORM_MONTHLY_BUDGET_EUR — 0/unset =
    uitgeschakeld (bewust: single-tenant heeft het workspace-budget al; zet
    dit AAN vóór er een tweede tenant komt — audit v2 P2-5)."""
    var = "PLATFORM_DAILY_BUDGET_EUR" if period == "day" else "PLATFORM_MONTHLY_BUDGET_EUR"
    try:
        return float(os.getenv(var, "0") or 0)
    except ValueError:
        return 0.0


async def check_platform_budget(supabase_client: Any) -> tuple[bool, str]:
    """Globaal plafond over ALLE workspaces (fase 4 PR 16).

    1.000 tenants x EUR 20 workspace-cap = EUR 20k/maand zonder deze rem.
    Waarschuwt op 70/85/95%, harde stop op 100%. FAIL-CLOSED bij leesfout.
    """
    now = datetime.now(timezone.utc)
    checks = (
        ("dag", _platform_budget_eur("day"),
         now.date().isoformat() + "T00:00:00+00:00"),
        ("maand", _platform_budget_eur("month"),
         now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()),
    )
    for label, cap, since in checks:
        if cap <= 0:
            continue  # niet geconfigureerd = uitgeschakeld
        try:
            spent = _cost_sum(supabase_client, None, since)
        except Exception as e:
            logger.error("cost_guard: platform-%sbudget-check faalde — FAIL-CLOSED: %s", label, e)
            return False, f"platform_budget_check_failed_{label} (fail-closed)"
        pct = spent / cap * 100.0
        if spent >= cap:
            logger.error("cost_guard: PLATFORM-%sBUDGET OVERSCHREDEN — spent=%.2f cap=%.2f",
                         label.upper(), spent, cap)
            return False, f"platform_{label}budget_exceeded: spent={spent:.2f} cap={cap:.2f}"
        for tier in (95, 85, 70):
            if pct >= tier:
                logger.warning("cost_guard: platform-%sbudget op %.0f%% (spent=%.2f cap=%.2f)",
                               label, pct, spent, cap)
                break
    return True, ""


async def check_monthly_budget(
    workspace_id: str,
    supabase_client: Any,
) -> dict:
    """Return dict met monthly spend + budget + 50%-approval-status.

    Returns:
      {
        "month_eur": float,
        "monthly_budget_eur": float,
        "pct_used": float,
        "tier_50_hit": bool,         # > 50% van monthly budget uitgegeven
        "tier_100_hit": bool,        # monthly budget op/over
        "approved_over_50": bool,    # gebruiker klikte "continue"
        "allowed": bool,             # mag enrichment draaien?
        "block_reason": str | None,
      }
    """
    base_cap = _monthly_budget_eur()

    # Read manual cap-override (user klikte "verhoog met €X")
    cap_override = 0.0
    override_info: dict = {}
    if not _disabled():
        try:
            o = (
                supabase_client.table("system_state")
                .select("value, expires_at")
                .eq("key", f"enrichment_monthly_override:{workspace_id}")
                .maybe_single()
                .execute()
            )
            if o.data:
                exp = o.data.get("expires_at")
                if exp:
                    try:
                        exp_dt = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
                        if exp_dt > datetime.now(timezone.utc):
                            v = o.data.get("value") or {}
                            cap_override = float(v.get("extra_eur") or 0)
                            override_info = v
                    except ValueError:
                        pass
        except Exception:
            pass

    cap = base_cap + cap_override
    if _disabled():
        return {
            "month_eur": 0.0, "monthly_budget_eur": cap, "monthly_base_eur": base_cap,
            "monthly_override_eur": cap_override, "override_info": override_info,
            "pct_used": 0.0, "tier_50_hit": False, "tier_100_hit": False,
            "approved_over_50": True, "allowed": True, "block_reason": None,
        }

    # Start of current month UTC
    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()

    try:
        # Fase 4 PR 15: geïndexeerd SQL-aggregaat i.p.v. full-scan (P1-2).
        spent = _cost_sum(supabase_client, workspace_id, month_start)
    except Exception as e:
        # FAIL-CLOSED (recovery-fix): blokkeer bij een leesfout i.p.v. blind
        # door te spenden. Herkenbaar aan block_reason="cost_guard_db_error".
        logger.error("cost_guard: monthly check faalde — FAIL-CLOSED (blokkeer): %s", e)
        return {
            "month_eur": cap, "monthly_budget_eur": cap, "monthly_base_eur": base_cap,
            "monthly_override_eur": cap_override, "override_info": override_info,
            "pct_used": 100.0, "tier_50_hit": True, "tier_100_hit": True,
            "approved_over_50": False, "allowed": False, "block_reason": "cost_guard_db_error",
        }

    pct = (spent / cap * 100.0) if cap > 0 else 0
    tier_50 = spent >= (cap * 0.5)
    tier_100 = spent >= cap

    # Check approval flag
    approved = False
    try:
        flag = (
            supabase_client.table("system_state")
            .select("value, expires_at")
            .eq("key", f"enrichment_approved_over_50:{workspace_id}")
            .maybe_single()
            .execute()
        )
        if flag.data:
            exp = flag.data.get("expires_at")
            if exp:
                try:
                    exp_dt = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
                    if exp_dt > now:
                        approved = True
                except ValueError:
                    pass
    except Exception:
        pass

    # Block logic
    allowed = True
    block_reason = None
    if tier_100:
        allowed = False
        block_reason = f"monthly_budget_100pct_hit: {spent:.2f}/{cap:.2f} EUR"
    elif tier_50 and not approved:
        allowed = False
        block_reason = f"monthly_budget_50pct_needs_approval: {spent:.2f}/{cap:.2f} EUR"

    return {
        "month_eur": round(spent, 4),
        "monthly_budget_eur": cap,
        "monthly_base_eur": base_cap,
        "monthly_override_eur": cap_override,
        "override_info": override_info,
        "pct_used": round(pct, 1),
        "tier_50_hit": tier_50,
        "tier_100_hit": tier_100,
        "approved_over_50": approved,
        "allowed": allowed,
        "block_reason": block_reason,
    }


async def check_daily_budget(
    workspace_id: str,
    supabase_client: Any,
) -> tuple[bool, float, float]:
    """Return (allowed, spent_today_eur, cap_eur).

    Query heatr_api_cost_log for today's sum for this workspace. If below cap:
    allowed. Cache-less — each call is a fresh DB read so multi-worker reality
    stays consistent.
    """
    cap = _daily_budget_eur()
    if _disabled():
        return True, 0.0, cap

    try:
        today = datetime.now(timezone.utc).date().isoformat()
        # Fase 4 PR 15: geïndexeerd SQL-aggregaat i.p.v. full-scan (P1-2).
        spent = _cost_sum(supabase_client, workspace_id, f"{today}T00:00:00+00:00")
    except Exception as e:
        # FAIL-CLOSED (recovery-fix): kunnen we de cost-log niet lezen, dan
        # BLOKKEREN we i.p.v. blind door te spenden — een DB-hik mag het
        # kostenplafond niet uitschakelen. Herkenbaar aan spent==cap.
        logger.error("cost_guard: daily budget check faalde — FAIL-CLOSED (blokkeer): %s", e)
        return False, cap, cap

    allowed = spent < cap
    if not allowed:
        logger.warning(
            "cost_guard: daily budget exceeded for workspace %s — spent=%.4f cap=%.4f",
            workspace_id, spent, cap,
        )
    return allowed, spent, cap


async def record_block(
    workspace_id: str,
    lead_id: str | None,
    context: str,
    reason: str,
    supabase_client: Any,
) -> None:
    """Persist a block event in heatr_api_cost_log with cost_eur=0 for audit.

    We reuse api_cost_log instead of a separate table — the `context` field
    prefixes with 'BLOCKED:' so queries can distinguish.
    """
    try:
        supabase_client.table("api_cost_log").insert({
            "workspace_id": workspace_id,
            "model": "cost_guard",
            "prompt_tokens": 0,
            "response_tokens": 0,
            "cost_eur": 0.0,
            "context": f"BLOCKED:{context}:{reason[:120]}",
            "lead_id": lead_id,
        }).execute()
    except Exception as e:
        logger.debug("cost_guard: record_block insert failed: %s", e)


async def guarded_call(
    workspace_id: str,
    lead_id: str | None,
    context: str,
    estimated_cost_eur: float,
    supabase_client: Any,
    accumulator: LeadCostAccumulator | None = None,
) -> tuple[bool, str]:
    """One-shot gate: check both daily budget AND per-lead ceiling.

    Returns (allowed, reason). On block: records the event in api_cost_log.
    Caller must itself call log_api_cost() after a successful Claude/API call
    to register actual cost for today's bucket.
    """
    if _disabled():
        return True, ""

    # 1. Per-lead ceiling (in-memory, fastest)
    if accumulator is not None:
        ok, reason = accumulator.check_allowed(estimated_cost_eur)
        if not ok:
            await record_block(workspace_id, lead_id, context, reason, supabase_client)
            return False, reason

    # 2. Platformbreed plafond (fase 4 PR 16) — over alle workspaces heen;
    #    uitgeschakeld zolang PLATFORM_*_BUDGET_EUR niet gezet is.
    ok, reason = await check_platform_budget(supabase_client)
    if not ok:
        await record_block(workspace_id, lead_id, context, reason, supabase_client)
        return False, reason

    # 3. Daily budget (DB read)
    ok, spent, cap = await check_daily_budget(workspace_id, supabase_client)
    if not ok:
        reason = f"daily_budget_exceeded: spent={spent:.4f} cap={cap:.4f}"
        await record_block(workspace_id, lead_id, context, reason, supabase_client)
        return False, reason

    # 4. Monthly budget — vereist user approval bij 50%, hard stop bij 100%
    m = await check_monthly_budget(workspace_id, supabase_client)
    if not m["allowed"]:
        await record_block(workspace_id, lead_id, context, m["block_reason"] or "monthly_blocked", supabase_client)
        return False, m["block_reason"] or "monthly_budget_block"

    return True, ""
