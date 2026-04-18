"""
utils/startup_validator.py — Heatr startup validation.

Aangeroepen via FastAPI lifespan event bij elke uvicorn start.
Gooit StartupError als kritieke configuratie ontbreekt.
Logt warnings voor optionele maar aanbevolen configuratie.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class StartupError(RuntimeError):
    """Raised when a hard-fail check does not pass."""


@dataclass
class StartupCheck:
    name: str
    passed: bool
    warning: bool = False   # True = warn-only, False = hard fail
    detail: str = ""


@dataclass
class StartupResult:
    checks: list[StartupCheck] = field(default_factory=list)

    @property
    def passed(self) -> list[StartupCheck]:
        return [c for c in self.checks if c.passed]

    @property
    def warnings(self) -> list[StartupCheck]:
        return [c for c in self.checks if not c.passed and c.warning]

    @property
    def failures(self) -> list[StartupCheck]:
        return [c for c in self.checks if not c.passed and not c.warning]

    def add(self, check: StartupCheck) -> None:
        self.checks.append(check)
        if not check.passed:
            if check.warning:
                logger.warning("[STARTUP WARNING] %s — %s", check.name, check.detail)
            else:
                logger.error("[STARTUP FAIL] %s — %s", check.name, check.detail)
        else:
            logger.info("[STARTUP OK] %s", check.name)


async def validate_startup(supabase_client=None) -> StartupResult:
    """
    Validates all critical configuration at startup.
    Raises StartupError if any hard-fail check fails.
    Returns StartupResult for logging.

    Args:
        supabase_client: Optional pre-built Supabase client. If None, builds one from env.
    """
    result = StartupResult()

    # -------------------------------------------------------------------------
    # 1. Required env vars (hard fail)
    # -------------------------------------------------------------------------
    _check_required_env(result, "SUPABASE_URL", "Supabase verbinding vereist")
    _check_required_env(result, "SUPABASE_KEY", "Supabase service role key vereist")
    _check_required_env(result, "ANTHROPIC_API_KEY", "Claude API key vereist voor AI enrichment")
    _check_required_env(result, "WARMR_API_URL", "Warmr URL vereist voor email sending")
    _check_required_env(result, "WARMR_API_KEY", "Warmr API key vereist voor email sending")
    _check_required_env(result, "DEFAULT_WORKSPACE_ID", "DEFAULT_WORKSPACE_ID vereist")

    heatr_key = os.getenv("HEATR_API_KEY", "")
    if len(heatr_key) >= 32:
        result.add(StartupCheck("HEATR_API_KEY lengte", True))
    else:
        result.add(StartupCheck(
            "HEATR_API_KEY lengte", False,
            detail="HEATR_API_KEY ontbreekt of is korter dan 32 tekens — stel in als veilige random string",
        ))

    # Bail early if core env vars missing — no point continuing
    if result.failures:
        _log_to_supabase_sync(result, supabase_client)
        raise StartupError(
            "Startup geblokkeerd door ontbrekende environment variables: " +
            ", ".join(c.name for c in result.failures)
        )

    # -------------------------------------------------------------------------
    # 2. Supabase bereikbaar + workspace bestaat
    # -------------------------------------------------------------------------
    from supabase import create_client
    try:
        db = supabase_client or create_client(
            os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"]
        )
        ws_res = db.table("workspaces").select("id").eq("id", os.environ["DEFAULT_WORKSPACE_ID"]).execute()
        if ws_res.data:
            result.add(StartupCheck("Supabase + workspace", True))
        else:
            result.add(StartupCheck(
                "Supabase + workspace", False,
                detail=f"Workspace '{os.environ['DEFAULT_WORKSPACE_ID']}' niet gevonden in workspaces tabel",
            ))
    except Exception as e:
        result.add(StartupCheck("Supabase + workspace", False, detail=f"Supabase onbereikbaar: {e}"))

    # -------------------------------------------------------------------------
    # 3. Anthropic API bereikbaar
    # -------------------------------------------------------------------------
    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        # Minimal test call — cheapest possible
        import asyncio
        msg = asyncio.get_event_loop().run_until_complete(
            client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=1,
                messages=[{"role": "user", "content": "ping"}],
            )
        ) if not _is_async_context() else await client.messages.create(  # type: ignore[misc]
            model="claude-haiku-4-5-20251001",
            max_tokens=1,
            messages=[{"role": "user", "content": "ping"}],
        )
        result.add(StartupCheck("Anthropic API", True))
    except Exception as e:
        result.add(StartupCheck("Anthropic API", False, detail=f"Claude API onbereikbaar: {e}"))

    # -------------------------------------------------------------------------
    # 4. Warmr API bereikbaar
    # -------------------------------------------------------------------------
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10.0) as hx:
            r = await hx.get(
                f"{os.environ['WARMR_API_URL'].rstrip('/')}/inboxes",
                headers={"Authorization": f"Bearer {os.environ['WARMR_API_KEY']}"},
            )
            if r.status_code < 500:
                result.add(StartupCheck("Warmr API bereikbaar", True))
            else:
                result.add(StartupCheck("Warmr API bereikbaar", False, detail=f"Warmr antwoordde {r.status_code}"))
    except Exception as e:
        result.add(StartupCheck("Warmr API bereikbaar", False, detail=f"Warmr onbereikbaar: {e}"))

    # -------------------------------------------------------------------------
    # 5. Supabase Storage bucket 'screenshots'
    # -------------------------------------------------------------------------
    try:
        db = supabase_client or create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
        buckets = db.storage.list_buckets()
        names = [b.name for b in (buckets or [])]
        bucket_name = os.getenv("SUPABASE_STORAGE_BUCKET", "screenshots")
        if bucket_name in names:
            result.add(StartupCheck(f"Storage bucket '{bucket_name}'", True))
        else:
            result.add(StartupCheck(
                f"Storage bucket '{bucket_name}'", False,
                detail=f"Bucket '{bucket_name}' niet gevonden. Beschikbaar: {names}",
            ))
    except Exception as e:
        result.add(StartupCheck("Storage bucket", False, detail=str(e)))

    # -------------------------------------------------------------------------
    # 6. Playwright Chromium geïnstalleerd
    # -------------------------------------------------------------------------
    try:
        result_proc = subprocess.run(
            [sys.executable, "-m", "playwright", "install", "--dry-run", "chromium"],
            capture_output=True, text=True, timeout=15,
        )
        # If chromium is installed, dry-run exits 0
        if result_proc.returncode == 0 or "chromium" in (result_proc.stdout + result_proc.stderr).lower():
            result.add(StartupCheck("Playwright Chromium", True))
        else:
            result.add(StartupCheck(
                "Playwright Chromium", False,
                detail="Chromium niet geïnstalleerd. Run: playwright install chromium",
            ))
    except Exception as e:
        result.add(StartupCheck("Playwright Chromium", False, detail=str(e)))

    # -------------------------------------------------------------------------
    # 7. Warn-only checks
    # -------------------------------------------------------------------------
    _check_optional_env(result, "KVK_API_KEY", "KvK enrichment uitgeschakeld (stap 4 waterval overgeslagen)")
    _check_optional_env(result, "PAGESPEED_API_KEY", "Pagespeed scores worden overgeslagen (technische laag onvolledig)")

    if os.getenv("PROXY_ENABLED", "false").lower() == "true" and not os.getenv("PROXY_URL"):
        result.add(StartupCheck(
            "Proxy configuratie", False, warning=True,
            detail="PROXY_ENABLED=true maar PROXY_URL ontbreekt — scraping zonder proxy",
        ))
    else:
        result.add(StartupCheck("Proxy configuratie", True))

    if not os.getenv("WARMR_WEBHOOK_SECRET"):
        result.add(StartupCheck(
            "Warmr webhook secret", False, warning=True,
            detail="WARMR_WEBHOOK_SECRET ontbreekt — webhook signature verificatie uitgeschakeld (onveilig voor productie)",
        ))
    else:
        result.add(StartupCheck("Warmr webhook secret", True))

    # Check ready inboxes count
    try:
        import httpx
        async with httpx.AsyncClient(timeout=8.0) as hx:
            r = await hx.get(
                f"{os.environ['WARMR_API_URL'].rstrip('/')}/inboxes?status=ready",
                headers={"Authorization": f"Bearer {os.environ['WARMR_API_KEY']}"},
            )
            data = r.json()
            inboxes = data.get("inboxes", data) if isinstance(data, dict) else data
            ready = [i for i in (inboxes or []) if i.get("status") == "ready" or i.get("daily_remaining", 0) > 0]
            if len(ready) >= 2:
                result.add(StartupCheck("Warmr ready inboxes", True, detail=f"{len(ready)} beschikbaar"))
            else:
                result.add(StartupCheck(
                    "Warmr ready inboxes", False, warning=True,
                    detail=f"Slechts {len(ready)} inbox(es) met status=ready. Sending capacity laag.",
                ))
    except Exception:
        result.add(StartupCheck("Warmr ready inboxes", False, warning=True, detail="Kon inbox count niet verifiëren"))

    # -------------------------------------------------------------------------
    # 8. Log resultaat naar Supabase startup_log
    # -------------------------------------------------------------------------
    await _log_to_supabase(result, supabase_client)

    # Hard fail if critical checks failed
    if result.failures:
        raise StartupError(
            "Startup geblokkeerd — " + "; ".join(f"{c.name}: {c.detail}" for c in result.failures)
        )

    logger.info(
        "Startup OK — %d checks passed, %d warnings",
        len(result.passed), len(result.warnings),
    )
    return result


def _check_required_env(result: StartupResult, key: str, detail: str) -> None:
    val = os.getenv(key, "")
    result.add(StartupCheck(
        f"Env: {key}", bool(val),
        detail=detail if not val else "",
    ))


def _check_optional_env(result: StartupResult, key: str, warn_msg: str) -> None:
    val = os.getenv(key, "")
    result.add(StartupCheck(
        f"Env: {key}", bool(val),
        warning=True,
        detail=warn_msg if not val else "",
    ))


def _is_async_context() -> bool:
    """True if we're inside a running event loop."""
    import asyncio
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


async def _log_to_supabase(result: StartupResult, db=None) -> None:
    """Write startup check summary to startup_log table."""
    if not db:
        try:
            from supabase import create_client
            db = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
        except Exception:
            return
    try:
        db.table("startup_log").insert({
            "started_at": datetime.now(timezone.utc).isoformat(),
            "checks_passed": len(result.passed),
            "checks_warned": len(result.warnings),
            "checks_failed": len(result.failures),
            "details": {
                "passed": [c.name for c in result.passed],
                "warnings": [{"name": c.name, "detail": c.detail} for c in result.warnings],
                "failures": [{"name": c.name, "detail": c.detail} for c in result.failures],
            },
        }).execute()
    except Exception as e:
        logger.warning("Could not write to startup_log: %s", e)


def _log_to_supabase_sync(result: StartupResult, db=None) -> None:
    """Synchronous fallback for early-fail logging."""
    import asyncio
    try:
        asyncio.get_event_loop().run_until_complete(_log_to_supabase(result, db))
    except Exception:
        pass
