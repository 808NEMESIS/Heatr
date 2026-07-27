"""scripts/send_receptie_testmail.py — end-to-end receptie-verzendtest naar een
EIGEN inbox, met harde guards zodat een echte kliniek FYSIEK niet geraakt kan
worden tijdens de run (Sami 2026-07-27).

Vier onafhankelijke muren, elke send moet ze allemaal passeren:
  1. is_test_lead + TEST_MODE batch-guard (utils/testmail_guard) — één niet-test
     record → hele batch geabort; is_test_lead-target → reroute naar TEST_RECIPIENT.
  2. HEATR_SEND_ALLOWLIST — alleen adressen op die lijst mogen mail ontvangen.
  3. kill-switch (ENABLE_PROSPECT_SENDS) — blijft leidend; expliciet aanzetten in
     déze run (ephemeer), niet in .env.
  4. compliance/suppressie in de dispatcher (laatste vangnet).
Plus: het testrecord draait op een NIET-bestaand domein (test-aerys.local), dus
zelfs een gefaalde reroute kan geen echte kliniek raken — het bounced.

DRY-RUN (default): rendert + toont de guard-beslissingen, pusht NIETS naar Warmr,
schrijft NIETS naar prod. --confirm-test schakelt de echte push in, mits alle envs
gezet zijn en de render-gate groen is (privacy/afmeld-tokens aanwezig).

  python3 scripts/send_receptie_testmail.py                       # dry-run
  TEST_MODE=1 TEST_RECIPIENT=... HEATR_SEND_ALLOWLIST=... \\
    ENABLE_PROSPECT_SENDS=true RECEPTIE_PRIVACY_NOTICE=... \\
    RECEPTIE_UNSUBSCRIBE_TEMPLATE=... WARMR_TEST_CAMPAIGN_ID=... \\
    python3 scripts/send_receptie_testmail.py --confirm-test      # echte push
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WORKSPACE = "aerys"
TEST_DOMAIN = "test-aerys.local"          # bestaat niet → bounce, nooit een echte kliniek


def _test_lead(hook: str) -> dict:
    """Een volledig in-memory testrecord (geen prod-write). is_test_lead=True,
    domein bestaat niet, e-mail op het test-domein (wordt gererouteerd)."""
    return {
        "id": "test-receptie-0001",
        "workspace_id": WORKSPACE,
        "is_test_lead": True,
        "company_name": "Testkliniek Amsterdam",
        "contact_first_name": "Sami",
        "first_name": "Sami",
        "email": f"receptie@{TEST_DOMAIN}",
        "email_status": "valid",
        "gdpr_safe": True,
        "domain": TEST_DOMAIN,
        "city": "Amsterdam",
        "google_review_count": 47,
        "google_rating": 4.6,
        "receptie_hook_code": hook,
    }


def _render(lead: dict, hook: str, *, privacy: str, unsubscribe: str) -> dict:
    from config.receptie_sequence import render_receptie_mail
    return render_receptie_mail(1, lead, hook_code=hook, hook_variant=None,
                                privacy_notice=privacy, unsubscribe=unsubscribe,
                                seed=lead.get("id"))


def _print_render(title: str, r: dict) -> None:
    print(f"\n── {title} ──")
    print(f"  sendable={r.get('sendable')}  block_reason={r.get('block_reason')}")
    print(f"  subject: {r.get('subject')}")
    body = r.get("body") or ""
    for ln in body.splitlines():
        print(f"  | {ln}")


def _guard_demo() -> None:
    """Toon de guard-muren zonder iets te versturen (pure functie, eigen env-snapshot)."""
    from utils.testmail_guard import enforce_and_reroute, TestSendBlocked
    snap = {k: os.environ.get(k) for k in ("TEST_MODE", "TEST_RECIPIENT")}
    try:
        os.environ["TEST_MODE"] = "1"
        os.environ["TEST_RECIPIENT"] = os.environ.get("TEST_RECIPIENT") or "demo@voorbeeld.nl"
        print("\n── GUARD-DEMO (geen send) ──")
        t = [{"id": "test-1", "email": f"x@{TEST_DOMAIN}", "is_test_lead": True}]
        orig = enforce_and_reroute(t, confirm_test=True)
        print(f"  reroute: {orig[0]} → {t[0]['email']}  (is_test_lead → TEST_RECIPIENT)")
        mixed = [{"id": "test-1", "email": f"x@{TEST_DOMAIN}", "is_test_lead": True},
                 {"id": "echt", "email": "prospect@kliniek.nl", "is_test_lead": False}]
        try:
            enforce_and_reroute(mixed, confirm_test=True)
            print("  ✗ FOUT: gemengde batch niet geabort!")
        except TestSendBlocked as e:
            print(f"  batch-guard: gemengde batch geabort → {str(e)[:70]}")
            print(f"    (echt record onaangeraakt: {mixed[1]['email']})")
        try:
            enforce_and_reroute(t, confirm_test=False)
            print("  ✗ FOUT: send zonder --confirm-test niet geblokkeerd!")
        except TestSendBlocked:
            print("  confirm-gate: send zonder --confirm-test geweigerd")
    finally:
        for k, v in snap.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _env_checklist() -> list[tuple[str, bool, str]]:
    from utils.testmail_guard import test_mode_active
    from config.receptie_sequence import receptie_unsubscribe_via_warmr
    def _set(k):
        return bool((os.getenv(k) or "").strip())
    return [
        ("TEST_MODE=1", test_mode_active(), "guard actief"),
        ("TEST_RECIPIENT", _set("TEST_RECIPIENT"), "reroute-doeladres"),
        ("HEATR_SEND_ALLOWLIST", _set("HEATR_SEND_ALLOWLIST"), "harde ontvanger-muur"),
        ("ENABLE_PROSPECT_SENDS=true",
         (os.getenv("ENABLE_PROSPECT_SENDS") or "").strip().lower() == "true",
         "kill-switch (ephemeer aan voor de run)"),
        ("RECEPTIE_PRIVACY_NOTICE", _set("RECEPTIE_PRIVACY_NOTICE"), "AVG art.14 — Heatr levert (staat)"),
        ("RECEPTIE_UNSUBSCRIBE_VIA_WARMR=true", receptie_unsubscribe_via_warmr(),
         "Warmr bezit de afmeldlink; POST-send geverifieerd op unsubscribe_tokens"),
        ("WARMR_TEST_CAMPAIGN_ID", _set("WARMR_TEST_CAMPAIGN_ID"),
         "verse campagne: sequence-body {{custom:custom_body}} + subject {{custom:custom_subject}} + ready inbox"),
    ]


async def _real_send(lead: dict, rendered: dict, campaign_id: str) -> dict:
    from integrations.warmr_client import WarmrClient
    from utils.outbound_dispatcher import dispatch_outbound
    wc = WarmrClient()
    disp = await dispatch_outbound(
        kind="warmr_push",
        idempotency_key=f"receptie-testmail:{lead['id']}:{os.getenv('TEST_RECIPIENT')}",
        actor="script:receptie-testmail",
        lead=lead,
        send=lambda: wc.push_lead(
            lead, campaign_id=campaign_id,
            custom_subject=rendered["subject"], custom_body=rendered["body"],
            content_type="text/plain"),
        supabase_client=None,  # dispatcher heeft de ledger nodig; zie waarschuwing
        workspace_id=WORKSPACE,
        confirm_test=True,
        metadata={"receptie_testmail": True, "to": os.getenv("TEST_RECIPIENT")},
    )
    return {"executed": disp.executed, "skipped": disp.skipped_duplicate}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--confirm-test", action="store_true",
                    help="schakel de ECHTE push in (vereist alle envs + groene render-gate)")
    ap.add_argument("--hook", default="Q7", help="receptie-haakcode (Q4/Q7/Q2/P1)")
    args = ap.parse_args()

    lead = _test_lead(args.hook)
    from config.receptie_sequence import receptie_compliance_tokens
    privacy, unsub = receptie_compliance_tokens(lead)

    print(f"Receptie-verzendtest — hook={args.hook} — testrecord op {TEST_DOMAIN} "
          f"(bestaat niet). DRY-RUN tenzij --confirm-test + envs.")

    # 1. Render met de ECHTE env-tokens (toont de huidige gate-stand).
    r_real = _render(lead, args.hook, privacy=privacy, unsubscribe=unsub)
    _print_render("RENDER met echte env-tokens", r_real)

    # 2. Render met duidelijk-neppe placeholder-tokens (alleen om de compositie te
    #    tonen — NIET verzendbaar: de URL is fake).
    demo_priv = "[PLACEHOLDER privacy — jij levert de echte AVG art.14-zin]"
    demo_unsub = "Afmelden: https://voorbeeld.invalid/afmelden/{id}".replace("{id}", lead["id"])
    r_demo = _render(lead, args.hook, privacy=demo_priv, unsubscribe=demo_unsub)
    _print_render("RENDER met PLACEHOLDER-tokens (alleen ter illustratie)", r_demo)

    _guard_demo()

    print("\n── ENV-CHECKLIST voor de echte push ──")
    checklist = _env_checklist()
    for name, ok, note in checklist:
        print(f"  [{'x' if ok else ' '}] {name:32} — {note}")
    missing = [n for n, ok, _ in checklist if not ok]

    if not args.confirm_test:
        print("\nDRY-RUN klaar. Geen push, geen prod-write. Draai met --confirm-test "
              "(+ envs) voor de echte send.")
        return 0

    # ── Echte push-pad ──
    if not r_real.get("sendable"):
        print(f"\n✗ GEBLOKKEERD door de render-gate: {r_real.get('block_reason')}. "
              "Zet RECEPTIE_PRIVACY_NOTICE + RECEPTIE_UNSUBSCRIBE_TEMPLATE en probeer opnieuw.")
        return 1
    if missing:
        print(f"\n✗ Ontbrekende envs: {', '.join(missing)}. Push niet uitgevoerd.")
        return 1
    campaign_id = (os.getenv("WARMR_TEST_CAMPAIGN_ID") or "").strip()
    print(f"\n→ Echte push naar Warmr (reroute → {os.getenv('TEST_RECIPIENT')}, "
          f"campagne {campaign_id[:8]}…). Guards actief.")
    try:
        out = asyncio.run(_real_send(lead, r_real, campaign_id))
        print(f"✓ Dispatcher: executed={out['executed']} skipped_duplicate={out['skipped']}")
        return 0
    except Exception as e:
        print(f"✗ Push faalde/geblokkeerd: {type(e).__name__}: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
