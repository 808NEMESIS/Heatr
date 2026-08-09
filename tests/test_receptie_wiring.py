"""
tests/test_receptie_wiring.py — render_faseA_marker receptie-brug (Fase 3-wiring).

Bewijst dat _render_receptie_marker de volledige gate-stack toepast: compliance-
tokens (privacyzin/afmeld), AVG-02-rechtsvorm en org-suppressie — fail-closed —
en dat mail 2 zonder tweede haak wordt overgeslagen.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from campaigns.sequence_engine import _render_receptie_marker


def _run(coro):
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _FakeSb:
    def __init__(self, wi=None, supp_rows=None, flags=None):
        self.wi, self.supp_rows = wi or {}, supp_rows or []
        self.flags, self._t = flags or [], None

    def table(self, name): self._t = name; return self
    def select(self, *_): return self
    def eq(self, *_): return self
    def or_(self, *_): return self
    def is_(self, *_): return self
    def limit(self, *_): return self

    def execute(self):
        if self._t == "website_intelligence":
            data = [self.wi] if self.wi else []
        elif self._t == "suppressions":
            data = self.supp_rows
        elif self._t == "compliance_flags":
            data = self.flags
        else:
            data = []
        return type("R", (), {"data": data})()


WI = {"receptie_hook_code": "Q4", "receptie_second_hook": None}
LEAD = {"id": "l1", "company_name": "Kliniek Vrijdag", "contact_first_name": "Sanne",
        "email": "info@kliniekvrijdag.nl", "city": "Utrecht",
        "google_review_count": 40, "google_rating": 4.7,
        "kvk_legal_form": "Besloten Vennootschap"}
MARK = {"faseA_brug": "receptie", "faseA_step": 0, "delay_days": 0}


def test_blocks_without_compliance_tokens(monkeypatch):
    monkeypatch.delenv("RECEPTIE_PRIVACY_NOTICE", raising=False)
    monkeypatch.delenv("RECEPTIE_UNSUBSCRIBE_TEMPLATE", raising=False)
    out = _run(_render_receptie_marker(MARK, LEAD, _FakeSb(wi=WI)))
    assert out["sendable"] is False and out["block_reason"] == "privacy_notice_missing"


def _set_tokens(monkeypatch):
    monkeypatch.setenv("RECEPTIE_PRIVACY_NOTICE", "Herkomst: aeryssolution.nl/privacy")
    monkeypatch.setenv("RECEPTIE_UNSUBSCRIBE_TEMPLATE", "Afmelden: aeryssolution.nl/uit?e={email}")


# ── drip-pre-gate: een open compliance-vlag stopt de volgende receptie-send ──────
def test_drip_blocks_on_open_unsubscribe_flag(monkeypatch):
    # Warmr-bezit-modus + een open missing_unsubscribe-vlag → send geblokkeerd.
    monkeypatch.setenv("RECEPTIE_PRIVACY_NOTICE", "Herkomst: aeryssolution.nl/privacy")
    monkeypatch.setenv("RECEPTIE_UNSUBSCRIBE_VIA_WARMR", "true")
    flag = {"id": "f1", "flag_type": "missing_unsubscribe", "lead_id": "lx",
            "acknowledged_at": None}
    out = _run(_render_receptie_marker(MARK, LEAD, _FakeSb(wi=WI, flags=[flag])))
    assert out["sendable"] is False and out["block_reason"].startswith("compliance_hold")


def test_drip_proceeds_without_open_flag(monkeypatch):
    monkeypatch.setenv("RECEPTIE_PRIVACY_NOTICE", "Herkomst: aeryssolution.nl/privacy")
    monkeypatch.setenv("RECEPTIE_UNSUBSCRIBE_VIA_WARMR", "true")
    out = _run(_render_receptie_marker(MARK, LEAD, _FakeSb(wi=WI, flags=[])))
    assert out["sendable"] is True and out["block_reason"] == "ok"


def test_blocks_when_rechtsvorm_onbepaald(monkeypatch):
    _set_tokens(monkeypatch)
    lead = {**LEAD, "kvk_legal_form": None}      # KvK uit → onbepaald, geen site-adres
    out = _run(_render_receptie_marker(MARK, lead, _FakeSb(wi=WI)))
    assert out["sendable"] is False and out["block_reason"] == "rechtsvorm_onbepaald_geen_gepubliceerd_adres"


def test_value_first_used_when_review_themes_present(monkeypatch):
    # WI met echte review-thema's → value-first mail-1 (met Founding Five + Loom).
    _set_tokens(monkeypatch)
    wi = {"receptie_hook_code": "Q4", "receptie_second_hook": None, "total_score": 40,
          "personalization": {"review_themes":
                              ["de tijd en aandacht van het team", "het eerlijke advies"]}}
    out = _run(_render_receptie_marker(MARK, LEAD, _FakeSb(wi=wi)))
    assert out["sendable"] is True
    assert "iets wat me opviel" in out["subject"]
    assert "de tijd en aandacht van het team" in out["body"]
    assert "Loom" in out["body"] and "gratis een concept" in out["body"]


def test_falls_back_to_deterministic_without_review_themes(monkeypatch):
    # Geen thema's → deterministische mail-1 (geen Loom/Founding Five) — fail-closed.
    _set_tokens(monkeypatch)
    wi = {"receptie_hook_code": "Q4", "receptie_second_hook": None, "total_score": 40}
    out = _run(_render_receptie_marker(MARK, LEAD, _FakeSb(wi=wi)))
    assert out["sendable"] is True and "Loom" not in out["body"]


def test_value_first_off_via_env_falls_back(monkeypatch):
    _set_tokens(monkeypatch)
    monkeypatch.setenv("RECEPTIE_VALUE_FIRST", "false")
    wi = {"receptie_hook_code": "Q4", "total_score": 40,
          "personalization": {"review_themes": ["a-thema langer", "b-thema langer"]}}
    out = _run(_render_receptie_marker(MARK, LEAD, _FakeSb(wi=wi)))
    assert "Loom" not in out["body"]                    # value-first uit → deterministisch


def test_sendable_when_onbepaald_but_email_from_own_site(monkeypatch):
    # Art. 11.7 lid 3: onbepaalde rechtsvorm maar adres van hun eigen website →
    # gate laat door (deblokkeert de grote onbepaald-bucket).
    _set_tokens(monkeypatch)
    lead = {**LEAD, "kvk_legal_form": None, "email_discovery_source": "website"}
    out = _run(_render_receptie_marker(MARK, lead, _FakeSb(wi=WI)))
    assert out["sendable"] is True and out["block_reason"] == "ok"


def test_sendable_when_rechtspersoon_and_not_suppressed(monkeypatch):
    _set_tokens(monkeypatch)
    out = _run(_render_receptie_marker(MARK, LEAD, _FakeSb(wi=WI)))
    assert out["sendable"] is True and out["block_reason"] == "ok"
    assert out["body"].startswith("Hoi Sanne,") and "{{" not in out["body"]
    assert "aeryssolution.nl/uit?e=info@kliniekvrijdag.nl" in out["body"]


def test_blocks_when_suppressed(monkeypatch):
    _set_tokens(monkeypatch)
    sb = _FakeSb(wi=WI, supp_rows=[{"suppression_type": "forgotten"}])
    out = _run(_render_receptie_marker(MARK, LEAD, sb))
    assert out["sendable"] is False and out["block_reason"] == "suppressed:forgotten"


def test_suppression_check_fail_closed(monkeypatch):
    _set_tokens(monkeypatch)

    class _Boom(_FakeSb):
        def execute(self):
            if self._t == "suppressions":
                raise RuntimeError("db down")
            return super().execute()
    out = _run(_render_receptie_marker(MARK, LEAD, _Boom(wi=WI)))
    assert out["sendable"] is False and out["block_reason"].startswith("suppression_check_failed")


def test_mail2_skipped_without_second_hook(monkeypatch):
    _set_tokens(monkeypatch)
    mark2 = {"faseA_brug": "receptie", "faseA_step": 1, "delay_days": 3}
    out = _run(_render_receptie_marker(mark2, LEAD, _FakeSb(wi=WI)))
    assert out["skipped"] is True and out["sendable"] is False
