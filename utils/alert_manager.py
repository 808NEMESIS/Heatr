"""
utils/alert_manager.py — Heatr alert system.

Stores alerts in system_alerts table.
On severity='critical': sends email via Resend to OPERATOR_EMAIL.
On severity='warning': creates notification in Heatr UI (via system_alerts).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

ALERT_TYPES = {
    "high_bounce_rate",
    "spam_complaint",
    "domain_blacklisted",
    "low_inbox_reputation",
    "workspace_daily_limit",
    "captcha_blocked",
    "enrichment_queue_overload",
    "claude_api_error_rate",
    "sequence_engine_error",
    "no_recent_discoveries",
    "low_open_rate",
    "low_reply_rate",
    "high_unsubscribe_rate",
    "low_email_coverage",
}


async def send_alert(
    alert_type: str,
    message: str,
    severity: str,  # info | warning | critical
    workspace_id: str,
    supabase_client,
) -> None:
    """
    Store alert in system_alerts. Fire email for critical severity.

    Args:
        alert_type: One of ALERT_TYPES (arbitrary strings also accepted).
        message: Human-readable description.
        severity: 'info' | 'warning' | 'critical'
        workspace_id: Workspace scope.
        supabase_client: Supabase client.
    """
    logger.warning("[ALERT %s] %s: %s", severity.upper(), alert_type, message)

    # Store in database
    try:
        supabase_client.table("system_alerts").insert({
            "workspace_id": workspace_id,
            "alert_type": alert_type,
            "message": message,
            "severity": severity,
            "is_read": False,
        }).execute()
    except Exception as e:
        logger.error("Failed to store alert in system_alerts: %s", e)

    # Email for critical alerts
    if severity == "critical":
        await _send_critical_email(alert_type, message, workspace_id)


async def _send_critical_email(alert_type: str, message: str, workspace_id: str) -> None:
    """Send email notification via Resend for critical alerts."""
    operator_email = os.getenv("OPERATOR_EMAIL")
    resend_key = os.getenv("RESEND_API_KEY")

    if not operator_email or not resend_key:
        logger.warning(
            "Critical alert not emailed — OPERATOR_EMAIL or RESEND_API_KEY not set. Alert: %s",
            alert_type,
        )
        return

    try:
        import httpx
        payload = {
            "from": "alerts@heatr.aerys.nl",
            "to": [operator_email],
            "subject": f"[HEATR CRITICAL] {alert_type.replace('_', ' ').title()}",
            "html": f"""
                <h2>🚨 Heatr Critical Alert</h2>
                <p><strong>Type:</strong> {alert_type}</p>
                <p><strong>Workspace:</strong> {workspace_id}</p>
                <p><strong>Tijdstip:</strong> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>
                <hr />
                <p>{message}</p>
                <p><a href="{os.getenv('HEATR_BASE_URL', 'http://localhost:8000')}/analytics.html">
                    Open Heatr Analytics →
                </a></p>
            """,
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(
                "https://api.resend.com/emails",
                json=payload,
                headers={"Authorization": f"Bearer {resend_key}"},
            )
            if r.status_code >= 400:
                logger.error("Resend email failed: %s %s", r.status_code, r.text[:200])
            else:
                logger.info("Critical alert email sent to %s", operator_email)
    except Exception as e:
        logger.error("Critical alert email failed: %s", e)


async def check_metric_alerts(
    metrics: dict,
    workspace_id: str,
    supabase_client,
) -> None:
    """
    Check daily metrics against thresholds and fire alerts if needed.
    Called at end of collect_daily_metrics().
    """
    # Open rate < 20%
    open_rate = metrics.get("open_rate") or 0
    if 0 < open_rate < 0.20:
        await send_alert(
            "low_open_rate",
            f"Open rate vandaag: {open_rate:.1%} (drempel: 20%). Check onderwerpregel en afzender reputatie.",
            "warning", workspace_id, supabase_client,
        )

    # Unsubscribe rate > 2%
    unsub_rate = metrics.get("unsubscribe_rate") or 0
    if unsub_rate > 0.02:
        severity = "critical" if unsub_rate > 0.05 else "warning"
        await send_alert(
            "high_unsubscribe_rate",
            f"Unsubscribe rate vandaag: {unsub_rate:.1%} (drempel: 2%). Controleer messaging en targeting.",
            severity, workspace_id, supabase_client,
        )

    # Bounce rate > 2%
    bounce_rate = metrics.get("bounce_rate") or 0
    if bounce_rate > 0.03:
        await send_alert(
            "high_bounce_rate",
            f"Bounce rate vandaag: {bounce_rate:.1%}. Alle sends worden geblokkeerd tot < 3%.",
            "critical", workspace_id, supabase_client,
        )
    elif bounce_rate > 0.02:
        await send_alert(
            "high_bounce_rate",
            f"Bounce rate vandaag: {bounce_rate:.1%} — nadert kritieke drempel van 3%.",
            "warning", workspace_id, supabase_client,
        )

    # Email coverage < 60%
    coverage = metrics.get("email_coverage_rate") or 0
    if 0 < coverage < 0.60:
        await send_alert(
            "low_email_coverage",
            f"Email coverage vandaag: {coverage:.1%}. Minder dan 60% leads bereikbaar.",
            "warning", workspace_id, supabase_client,
        )
