"""
tests/test_website_intel_recovery.py — Recovery Patch 4.

Bewijst dat de website-intelligence-lagen niet meer stil dood/vervuild zijn:
  - de dedicated-queue Anthropic-client is async (sector-laag gaf 0 door een
    sync-client op `await`);
  - de conversie-form-telling gebeurt PER <form>, niet pagina-breed (zoekbalk/
    nieuwsbrief blaasden de telling op → contactform onterecht gestraft).
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_dedicated_queue_anthropic_client_is_async(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-xxxxxxxx")
    from job_queue.website_analysis_queue import _get_anthropic_client
    client = _get_anthropic_client()
    # Sync anthropic.Anthropic gaf 'await client.messages.create' → TypeError →
    # sector_score=0. Moet AsyncAnthropic zijn.
    assert type(client).__name__ == "AsyncAnthropic"


# --- Conversie: form-telling per <form> --------------------------------------

from website_intelligence.conversion_checker import check_conversion

_SEARCH_AND_NEWSLETTER = (
    '<input type="search" name="q">'                       # zoekbalk (pagina-breed)
    '<form action="/newsletter"><input type="email" name="nl"></form>'  # nieuwsbrief
)


def test_form_count_counts_contact_form_not_whole_page():
    html = (
        _SEARCH_AND_NEWSLETTER +
        '<form action="/contact">'
        '  <input name="naam"><input type="email" name="email">'
        '  <textarea name="bericht"></textarea>'
        '</form>'
    )
    res = _run(check_conversion("x.nl", html, "cosmetische_behandelaars"))
    # Contactform = 3 velden (naam, email, textarea) — NIET pagina-breed (5).
    assert res["form_field_count"] == 3
    assert res["has_contact_form"] is True


def test_form_count_penalizes_genuinely_large_form():
    big = "".join(f'<input name="f{i}">' for i in range(7))
    html = f'<form action="/contact"><input type="email">{big}<textarea></textarea></form>'
    res = _run(check_conversion("x.nl", html, "cosmetische_behandelaars"))
    assert res["form_field_count"] >= 6  # >5 → juist gestraft


def test_no_form_is_flagged_absent():
    res = _run(check_conversion("x.nl", "<div>geen formulier</div>", "cosmetische_behandelaars"))
    assert res["has_contact_form"] is False
    assert res["form_field_count"] == 0
