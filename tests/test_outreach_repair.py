"""
tests/test_outreach_repair.py — de outreach-reparatie van 2026-07-18.

Vier fixes, allemaal zonder één send te doen (pure functies + render):
  1. catchall_risky (wat Bouncer werkelijk schrijft) volgt het fail-closed
     risky-regime i.p.v. door de role-email-tak te lekken.
  2. personalized_opener is HARD (elders getest: test_enrichment_check).
  3. Loom/VIDEO-blokken worden conditioneel weggelaten als de link leeg is.
  4. pick_brug routeert op de genormaliseerde website_score (dode
     visual_score-tak weg; drempel-herijking geblokkeerd op de eindstaat).
"""
from campaigns.sequence_engine import inject_variables, render_step
from config.sequence_templates import pick_brug
from utils.email_sendability import is_sendable


# ── 1. catchall_risky in het risky-regime ───────────────────────────────────
def test_catchall_risky_follows_risky_regime():
    # zonder verification_method: fail-closed weigeren, ook met allow_risky
    ok, reason = is_sendable("a@kliniek.nl", "catchall_risky", allow_risky=True)
    assert not ok and "risky_unverified" in reason
    # mét bouncer_api-methode + allow_risky: sendable
    ok, _ = is_sendable("a@kliniek.nl", "catchall_risky", allow_risky=True,
                        verification_method="bouncer_api")
    assert ok
    # strikte eerste-campagne-keuze: allow_risky=False weigert óók mét methode
    ok, _ = is_sendable("a@kliniek.nl", "catchall_risky", allow_risky=False,
                        verification_method="bouncer_api")
    assert not ok


def test_catchall_risky_role_email_no_longer_leaks():
    """Vóór de fix passeerde catchall_risky+info@ via de role-email-tak zonder
    methode-eis. Nu valt de status in het risky-regime en geldt de eis wél."""
    ok, reason = is_sendable("info@kliniek.nl", "catchall_risky", allow_risky=True)
    assert not ok and "risky_unverified" in reason


def test_valid_only_gate_args_give_exactly_valid():
    """De gate-args van de eerste campagne (allow_risky=False,
    allow_role_emails=False): alleen 'valid'/'verified' passeert."""
    args = {"allow_risky": False, "allow_role_emails": False}
    assert is_sendable("a@x.nl", "valid", **args)[0]
    assert is_sendable("info@x.nl", "valid", **args)[0]   # role + valid = ok
    for status in ("catchall_risky", "risky", "not_checked", "not_found", "invalid", None):
        assert not is_sendable("info@x.nl", status, **args,
                               verification_method="bouncer_api")[0], status


# ── 3. Loom/VIDEO conditioneel ──────────────────────────────────────────────
def test_loom_block_dropped_when_empty():
    body = "Intro.\n\n{{LOOM_LINK}}\n\nGroet,\nSami"
    out = inject_variables(body, {"company_name": "X"})
    assert "{{LOOM_LINK}}" not in out
    assert "\n\n\n" not in out          # geen kale dubbele witregel
    assert "Intro.\n\nGroet," in out


def test_loom_block_rendered_when_present():
    body = "Intro.\n\n{{LOOM_LINK}}\n\nGroet,"
    out = inject_variables(body, {"company_name": "X", "loom_link": "https://loom.com/s/abc"})
    assert "https://loom.com/s/abc" in out


def test_render_step_has_no_unresolved_loom():
    step = {"subject": "{{company}}", "body": "Kijk:\n\n{{LOOM_LINK}}\n\nTot zo."}
    out = render_step(step, {"company_name": "Kliniek X"}, seed="l:0")
    assert "{{" not in out["body"]


# ── 4. pick_brug op genormaliseerde website_score ───────────────────────────
def test_pick_brug_ignores_visual_score():
    """De dode tak is weg: visual_score in de input verandert niets meer."""
    base = {"website_score": 80, "website_age_years": 0}
    with_visual = {**base, "visual_score": 5}
    assert pick_brug(base) == pick_brug(with_visual) == "ai_audit"


def test_pick_brug_missing_score_is_conservative():
    """Geen website_score → geen website-punten → default ai_audit."""
    assert pick_brug({}) == "ai_audit"
