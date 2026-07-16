"""
scripts/send_checkup_report.py — verstuur een vrijgegeven check-up rapport.

De send-endpoint is service-only (require_service_key): een browser-JWT wordt
geweigerd, want een send hoort nooit per ongeluk via de UI te lopen. Dit script
is het operator-pad (of n8n): het POST't naar de draaiende API met X-API-Key.

    python3 scripts/send_checkup_report.py <call_id> --dry-run   # render+URL, geen send
    python3 scripts/send_checkup_report.py <call_id>             # echte send

Twee sloten staan los van dit script: CHECKUP_REPORT_ENABLED (feature) en
ENABLE_PROSPECT_SENDS (dispatcher). Staat één van beide uit -> de API weigert
netjes met een 403; dit script verstuurt zelf niets.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

import httpx


def main() -> int:
    ap = argparse.ArgumentParser(description="Verstuur een vrijgegeven check-up rapport via de API.")
    ap.add_argument("call_id", help="id van het call-record (report_status moet 'approved' zijn)")
    ap.add_argument("--dry-run", action="store_true", help="render PDF + signed URL, geen Warmr-push")
    ap.add_argument("--api-url", default=os.getenv("HEATR_API_URL", "http://127.0.0.1:8001"))
    args = ap.parse_args()

    api_key = os.getenv("HEATR_API_KEY")
    if not api_key:
        print("FOUT: HEATR_API_KEY ontbreekt in .env", file=sys.stderr)
        return 2

    url = f"{args.api_url.rstrip('/')}/calls/{args.call_id}/send-report"
    try:
        resp = httpx.post(
            url,
            headers={"X-API-Key": api_key},
            json={"dry_run": args.dry_run},
            timeout=60.0,
        )
    except httpx.HTTPError as e:
        print(f"FOUT: kon de API niet bereiken ({url}): {e}", file=sys.stderr)
        return 1

    body = resp.text
    try:
        body = resp.json()
    except Exception:
        pass

    if resp.status_code >= 400:
        print(f"GEWEIGERD ({resp.status_code}): {body}", file=sys.stderr)
        return 1

    print(f"OK ({resp.status_code}): {body}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
