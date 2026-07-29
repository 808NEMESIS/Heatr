"""tests/test_unsubscribe_sweep_scheduling.py — schedulbaarheid van de afmeld-sweep.

Drift-audit 2026-07-28 (gat ④): de post-send-sweep die de compliance-vlag zet, draaide
alleen handmatig (--campaign-id required) — geen cron/launchd verwees ernaar, dus de vlag
werd nooit automatisch gezet. Fix: de geplande launchd-job draait zonder --campaign-id en
resolvet de campagne(s) uit RECEPTIE_SWEEP_CAMPAIGN_IDS. Deze suite pint die resolutie.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.verify_unsubscribe_compliance import _resolve_campaign_ids


def test_cli_id_takes_precedence_over_env():
    assert _resolve_campaign_ids("cli-uuid", "env-a,env-b") == ["cli-uuid"]


def test_env_comma_separated_multiple():
    assert _resolve_campaign_ids(None, "a, b ,c") == ["a", "b", "c"]


def test_env_single():
    assert _resolve_campaign_ids(None, "solo-uuid") == ["solo-uuid"]


def test_empty_both_is_empty_list():
    # Lege lijst → de caller faalt hard (SystemExit 2), zet NIETS stil.
    assert _resolve_campaign_ids(None, None) == []
    assert _resolve_campaign_ids("", "") == []
    assert _resolve_campaign_ids("   ", "  ,  ") == []


def test_env_ignores_blank_entries():
    assert _resolve_campaign_ids(None, "a,,  ,b,") == ["a", "b"]


def test_plist_exists_and_references_the_script():
    plist = (Path(__file__).resolve().parent.parent
             / "deployment" / "launchd" / "nl.aerys.heatr.unsubscribe-compliance.plist")
    assert plist.exists(), "launchd-plist voor de afmeld-sweep ontbreekt"
    text = plist.read_text()
    assert "verify_unsubscribe_compliance.py" in text
    assert "--apply" in text                                   # geplande job zet de vlag echt
    assert "RECEPTIE_SWEEP_CAMPAIGN_IDS" in text               # env-configureerbaar, geen hardcoded UUID
    assert "StartInterval" in text                             # draait periodiek
