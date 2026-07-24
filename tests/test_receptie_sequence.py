"""
tests/test_receptie_sequence.py — receptie-mailsequentie mail 1/2/3 (Fase 3b).

Kernpunten: de twee compliance-gates ({{privacy_notice}}, {{unsubscribe}}) weigeren
hard zolang ze leeg zijn; mail 2 wordt overgeslagen zonder tweede thema-haak; de
hele mail blijft F4-proof en dash-vrij; mail 3 is haak-specifiek.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.receptie_sequence import receptie_mail_sendable, render_receptie_mail

LEAD = {"id": 1, "company_name": "Kliniek Vrijdag", "city": "Utrecht",
        "contact_first_name": "Sanne", "google_review_count": 48, "google_rating": 4.8}
PRIV = "Meer over hoe wij aan deze gegevens komen: aeryssolution.nl/privacy"
UNSUB = "Geen mail meer? Afmelden: aeryssolution.nl/afmelden?x=abc"


def _m(step, **kw):
    base = dict(hook_code="Q4", privacy_notice=PRIV, unsubscribe=UNSUB)
    base.update(kw)
    return render_receptie_mail(step, LEAD, **base)


# ── de harde compliance-gates ────────────────────────────────────────────────
def test_gate_blocks_without_privacy_notice():
    r = _m(1, privacy_notice="")
    assert r["sendable"] is False and r["block_reason"] == "privacy_notice_missing"


def test_gate_blocks_without_unsubscribe():
    r = _m(1, unsubscribe="")
    assert r["sendable"] is False and r["block_reason"] == "unsubscribe_missing"


def test_sendable_with_both_tokens_and_first_name():
    r = _m(1)
    assert r["sendable"] is True and r["block_reason"] == "ok"
    assert "{{" not in r["body"] and "—" not in r["body"]
    assert PRIV in r["body"] and UNSUB in r["body"]


def test_no_first_name_falls_back_to_hallo_not_bare():
    # geen voornaam → "Hallo," (grammaticaal correct), nooit "Hoi ," — en sendable.
    lead = {**LEAD, "contact_first_name": None}
    r = render_receptie_mail(1, lead, hook_code="Q4", privacy_notice=PRIV, unsubscribe=UNSUB)
    assert r["body"].startswith("Hallo,")
    assert "Hoi ," not in r["body"] and r["sendable"] is True


def test_junk_first_name_falls_back_to_hallo():
    # 'Afspraak'/'Glowclinicnl' zijn junk → behandeld als geen naam → 'Hallo,'.
    for junk in ("Afspraak", "Glowclinicnl", "A."):
        lead = {**LEAD, "contact_first_name": junk}
        r = render_receptie_mail(1, lead, hook_code="Q4", privacy_notice=PRIV, unsubscribe=UNSUB)
        assert r["body"].startswith("Hallo,"), f"{junk} niet afgevangen"
        assert junk not in r["body"]


def test_real_first_name_used_in_greeting():
    r = render_receptie_mail(1, LEAD, hook_code="Q4", privacy_notice=PRIV, unsubscribe=UNSUB)
    assert r["body"].startswith("Hoi Sanne,")


def test_mail2_overgang_causal_only_for_reinforcing_pair():
    # Q4+Q7 = versterkend → causale overgang. Q7+P1 = los → neutraal, geen causale claim.
    q4q7 = render_receptie_mail(2, LEAD, hook_code="Q4", second_hook="Q7",
                                privacy_notice=PRIV, unsubscribe=UNSUB)["body"]
    q7p1 = render_receptie_mail(2, LEAD, hook_code="Q7", second_hook="P1",
                                privacy_notice=PRIV, unsubscribe=UNSUB)["body"]
    assert "dit maakt het eerste lastiger" in q4q7
    assert "dit maakt het eerste lastiger" not in q7p1
    assert "op een heel ander punt" in q7p1


def test_gate_catches_unresolved_token():
    assert receptie_mail_sendable("Hoi Sanne,\n\n{{haakje}}", privacy_notice=PRIV,
                                  unsubscribe=UNSUB) == (False, "unresolved_token")


def test_gate_catches_em_dash():
    assert receptie_mail_sendable("Hoi Sanne, iets — anders.", privacy_notice=PRIV,
                                  unsubscribe=UNSUB)[1] == "em_dash"


def test_gate_catches_multiple_questions():
    ok, reason = receptie_mail_sendable("Hoi? echt? ja", privacy_notice=PRIV, unsubscribe=UNSUB)
    assert not ok and reason == "multiple_questions"


# ── mail 2: alleen bij tweede thema-haak ─────────────────────────────────────
def test_mail2_skipped_without_second_hook():
    r = _m(2, second_hook=None)
    assert r["skipped"] is True and r["block_reason"] == "no_second_hook"


def test_mail2_rendered_with_second_hook():
    r = _m(2, hook_code="Q4", second_hook="Q7", second_variant="A")
    assert r["skipped"] is False and r["sendable"] is True
    # de tweede haak (Q7-meting, variant A) staat in de body
    assert "meting" in r["body"].lower()


# ── mail 3: haak-specifiek ───────────────────────────────────────────────────
def test_mail3_lek_is_hook_specific():
    q4 = _m(3, hook_code="Q4")["body"]
    q7 = _m(3, hook_code="Q7")["body"]
    assert "niemand kan vastleggen" in q4
    assert "in je cijfers opduikt" in q7
    assert q4 != q7


def test_mail3_pitch_has_no_question():
    # de PITCH van mail 3 heeft geen vraag (open einde); de footer mag er wel een
    # hebben (afmeldregel). Sendable = True bewijst dat de gate 'm doorlaat.
    r = _m(3)
    pitch = r["body"].replace(PRIV, "").replace(UNSUB, "")
    assert pitch.count("?") == 0 and r["sendable"] is True
    assert "laat het weten" in r["body"]


def test_all_mails_f4_proof():
    for step in (1, 3):
        r = _m(step)
        assert "gisteravond" not in r["body"] and "op mijn telefoon" not in r["body"]
        assert r["block_reason"] != "f4_time_claim"
