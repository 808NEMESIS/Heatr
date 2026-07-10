"""
utils/sending_guard.py — Central sending safeguard for all Warmr email sends.

Called BEFORE every send. Returns (can_send, reason) — never raises.
Logs every blocked send to blocked_sends table.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class SendingBlockedError(Exception):
    """Raised when a batch send should be aborted entirely."""


class SendingGuard:
    """
    Central gatekeeper for all email sends via Warmr.
    All limits are configurable via environment variables.
    """

    DAILY_MAX_PER_INBOX: int     = int(os.getenv("DAILY_MAX_PER_INBOX", "50"))
    DAILY_MAX_PER_DOMAIN: int    = int(os.getenv("DAILY_MAX_PER_DOMAIN", "100"))
    DAILY_MAX_PER_WORKSPACE: int = int(os.getenv("DAILY_MAX_PER_WORKSPACE", "500"))
    MIN_INBOX_REPUTATION: float  = float(os.getenv("MIN_INBOX_REPUTATION", "70"))
    MAX_BOUNCE_RATE: float       = float(os.getenv("MAX_BOUNCE_RATE", "0.03"))
    MAX_SPAM_RATE: float         = float(os.getenv("MAX_SPAM_RATE", "0.01"))

    async def check_can_send(
        self,
        lead_id: str,
        inbox_id: str,
        workspace_id: str,
        supabase_client,
    ) -> tuple[bool, str]:
        """
        Run all send checks in priority order.

        Returns:
            (True, "") if send is allowed.
            (False, reason) if blocked — never raises.
        """
        try:
            return await self._run_checks(lead_id, inbox_id, workspace_id, supabase_client)
        except Exception as e:
            logger.error("SendingGuard unexpected error for lead %s: %s", lead_id, e)
            return False, f"Interne fout in SendingGuard: {e}"

    async def _run_checks(
        self,
        lead_id: str,
        inbox_id: str,
        workspace_id: str,
        db,
    ) -> tuple[bool, str]:

        # 1-3. Compliance (GDPR + status) — via de CENTRALE gate zodat het
        # dispatch-pad (mail 2/3) exact hetzelfde oordeel velt als launch,
        # preview, send-to-warmr en review-email. Sprint 1-audit vond dat de
        # eigen checks hier "disqualified" misten: een lead die ná launch
        # gediskwalificeerd werd, kreeg vervolg-mails gewoon door.
        lead = await self._get_lead(lead_id, workspace_id, db)
        if not lead:
            return await self._block(lead_id, inbox_id, workspace_id, "lead_not_found", "Lead niet gevonden", db)

        from utils.enrichment_check import compliance_check
        compliant, compliance_reason = compliance_check(lead)
        if not compliant:
            return await self._block(lead_id, inbox_id, workspace_id, "compliance",
                                     compliance_reason or "compliance-block", db)

        # 4. Next contact cooldown
        next_contact = lead.get("next_contact_after")
        if next_contact:
            nc_dt = datetime.fromisoformat(next_contact.replace("Z", "+00:00"))
            if nc_dt > datetime.now(timezone.utc):
                return await self._block(lead_id, inbox_id, workspace_id, "cooldown",
                                         f"Lead in cooldown tot {nc_dt.date()}", db)

        # 5. Already in active campaign
        campaign_res = db.table("lead_campaign_history").select("id").eq("lead_id", lead_id).eq("is_active", True).limit(1).execute()
        if campaign_res.data:
            return await self._block(lead_id, inbox_id, workspace_id, "already_in_campaign",
                                     "Lead is al in een actieve campagne", db)

        # 5b. 90-dagen-cooldown na een afgeronde/gestopte sequence. Recovery-fix:
        # dit ontbrak in het live pad — een lead die een sequence afmaakte kon
        # direct opnieuw worden aangeschreven. `next_contact_after` (stap 4) dekt
        # alleen reply/recontact-events, niet een natuurlijke completion.
        from utils.deduplicator import campaign_cooldown_block
        cooldown = await campaign_cooldown_block(lead_id, db)
        if cooldown:
            return await self._block(lead_id, inbox_id, workspace_id, "cooldown_90d",
                                     f"90-dagen-cooldown actief ({cooldown})", db)

        # 6. Inbox reputation
        inbox = await self._get_inbox(inbox_id, db)
        if inbox:
            rep = (inbox.get("reputation_score") or 0)
            # Warmr reputation is 0.0–1.0 — convert to 0–100 if needed
            if rep <= 1.0:
                rep = rep * 100
            if rep < self.MIN_INBOX_REPUTATION:
                return await self._block(lead_id, inbox_id, workspace_id, "low_reputation",
                                         f"Inbox reputatie te laag: {rep:.0f} (min {self.MIN_INBOX_REPUTATION})", db)

        # 7. Inbox daily limit — heatr-owned sends (Model B) hebben sent_at per
        # stap; warmr-tracking-rijen tellen mee via enrolled_at (de push).
        # Workspace-scoping toegevoegd (fase 3 PR 11, audit v2 P1-1).
        today = datetime.now(timezone.utc).date().isoformat()
        inbox_sent_res = db.table("lead_campaign_history").select("id", count="exact").eq(
            "workspace_id", workspace_id).eq(
            "inbox_id", inbox_id).gte("sent_at", today).execute()
        inbox_today = inbox_sent_res.count or 0
        if inbox_today >= self.DAILY_MAX_PER_INBOX:
            return await self._block(lead_id, inbox_id, workspace_id, "inbox_daily_limit",
                                     f"Inbox dagelijkse limiet bereikt: {inbox_today}/{self.DAILY_MAX_PER_INBOX}", db)

        # 8. Sending-domain daily limit — VERWIJDERD als query (fase 3 PR 11).
        # De oude check filterde op lead_campaign_history.sending_domain: die
        # kolom bestaat alleen in het dode supabase_schema.sql, niet in de
        # runtime-tabel (migratie 009/025) → de query faalde ALTIJD en de cap
        # was cosmetisch (audit v2 P0-3). Een eerlijke domein-cap vereist
        # Warmr-capacity-events (actieplan, Warmr-contract) — geen nep-check
        # die groen lijkt maar niets meet. DAILY_MAX_PER_DOMAIN blijft
        # gereserveerd voor die aansluiting.

        # 9. Workspace daily limit — pushes van vandaag: warmr-enrollments
        # (enrolled_at, fase 3 PR 9) + heatr-owned stap-sends (sent_at).
        ws_sent_res = db.table("lead_campaign_history").select("id", count="exact").eq(
            "workspace_id", workspace_id).or_(
            f"enrolled_at.gte.{today},sent_at.gte.{today}").execute()
        ws_today = ws_sent_res.count or 0
        if ws_today >= self.DAILY_MAX_PER_WORKSPACE:
            return await self._block(lead_id, inbox_id, workspace_id, "workspace_daily_limit",
                                     f"Workspace dagelijkse limiet bereikt: {ws_today}/{self.DAILY_MAX_PER_WORKSPACE}", db)

        # 10. Bounce rate circuit breaker
        bounce_check = await self._check_bounce_rate(workspace_id, today, db)
        if bounce_check:
            # Critical — also fire alert
            from utils.alert_manager import send_alert
            try:
                await send_alert(
                    "high_bounce_rate", bounce_check, "critical", workspace_id, db
                )
            except Exception:
                pass
            return await self._block(lead_id, inbox_id, workspace_id, "bounce_rate_exceeded", bounce_check, db)

        return True, ""

    async def _get_lead(self, lead_id: str, workspace_id: str, db) -> dict | None:
        try:
            res = db.table("leads").select(
                "id, gdpr_safe, status, next_contact_after, crm_stage"
            ).eq("id", lead_id).eq("workspace_id", workspace_id).maybe_single().execute()
            return res.data
        except Exception:
            return None

    async def _get_inbox(self, inbox_id: str, db) -> dict | None:
        """Try to get inbox info from system_state cache (set by enrichment_queue)."""
        try:
            import json
            res = db.table("system_state").select("value").eq("key", "warmr_inboxes_cache").maybe_single().execute()
            if res.data:
                inboxes = json.loads(res.data["value"])
                for i in inboxes:
                    if i.get("id") == inbox_id:
                        return i
        except Exception:
            pass
        return None

    async def _check_bounce_rate(self, workspace_id: str, today: str, db) -> str | None:
        """Returns error message if bounce rate is too high, else None.

        HERBEDRAAD (fase 3 PR 11, audit v2 P0-3): de oude check telde
        reply_inbox.event_type='bounced' — die kolom bestaat niet in de
        runtime-tabel (migratie 004), dus de query faalde altijd, de except
        slikte hem en de breaker vuurde NOOIT. Nu tellen we hard-bounce-
        suppressies (fase 2 PR 7 — de webhook schrijft die per bounce) tegen
        de pushes van vandaag.

        BEWUST GEEN try/except meer: een leesfout hoort te propageren naar
        check_can_send, dat top-level (False, reason) retourneert —
        guard-data-onbeschikbaar = geen prospect-send (Besluit 3), niet
        stilletjes doorlaten zoals voorheen.
        """
        sent_res = db.table("lead_campaign_history").select("id", count="exact").eq(
            "workspace_id", workspace_id).or_(
            f"enrolled_at.gte.{today},sent_at.gte.{today}").execute()
        total_sent = sent_res.count or 0
        if total_sent < 10:
            return None  # Not enough data

        bounced_res = db.table("suppressions").select("id", count="exact").eq(
            "suppression_type", "hard_bounce").eq(
            "source_workspace_id", workspace_id).gte("created_at", today).execute()
        bounced = bounced_res.count or 0
        rate = bounced / total_sent

        if rate > self.MAX_BOUNCE_RATE:
            return f"Bounce rate {rate:.1%} overschrijdt maximum {self.MAX_BOUNCE_RATE:.1%} — alle sends gestopt"
        return None

    async def _block(
        self,
        lead_id: str,
        inbox_id: str,
        workspace_id: str,
        reason_code: str,
        reason: str,
        db,
    ) -> tuple[bool, str]:
        """Log the blocked send and return (False, reason)."""
        logger.info("Send blocked [%s] lead=%s inbox=%s: %s", reason_code, lead_id, inbox_id, reason)
        try:
            db.table("blocked_sends").insert({
                "workspace_id": workspace_id,
                "lead_id": lead_id,
                "inbox_id": inbox_id,
                "reason": f"{reason_code}: {reason}",
            }).execute()
        except Exception as e:
            logger.debug("Could not log blocked send: %s", e)
        return False, reason
