"""utils/testmail_guard.py — harde end-to-end-verzendtest-guard (Sami 2026-07-27).

Doel: een verzendtest naar een EIGEN inbox kunnen draaien terwijl het FYSIEK
onmogelijk is dat een echte prospect geraakt wordt tijdens die run. Guard, geen
belofte.

INVARIANT (veiligheidskern): dit alles is MONOTOON RESTRICTIEF. Het kan een send
alleen BLOKKEREN of de ontvanger HERSCHRIJVEN naar het testadres — het maakt nooit
een send mogelijk die de kill-switch of de allowlist niet al toestonden. En het is
VOLLEDIG NO-OP als TEST_MODE niet aanstaat: nul gedragsverandering in productie,
de bestaande suite en het 017-test-lead-pad blijven ongemoeid.

Drie regels, alle gekeyd op TEST_MODE (env, "1"/"true"):
  1. batch-guard — in TEST_MODE moet ELK target is_test_lead=True zijn. Eén echt
     record in de batch → abort de HELE batch. Zo kunnen test en echt nooit in
     dezelfde run zitten.
  2. reroute — een is_test_lead-target krijgt to: = TEST_RECIPIENT (in-place; de
     send-callable sluit over hetzelfde lead-dict, dus de echte ontvanger wijzigt).
  3. confirm — een TEST_MODE-send vereist expliciete confirm_test=True (de
     --confirm-test-vlag). Zonder → block.

De kill-switch blijft leidend en wordt NIET omzeild: om in TEST_MODE daadwerkelijk
te versturen moet de operator ENABLE_PROSPECT_SENDS in díe run expliciet aanzetten
(ephemeer, niet in .env). Deze guard voegt daar alleen restricties aan toe. De
bestaande HEATR_SEND_ALLOWLIST is de laatste muur: zet die op exact TEST_RECIPIENT,
dan blokkeert zelfs een falende reroute elk niet-test-adres.
"""
from __future__ import annotations

import os


class TestSendBlocked(Exception):
    """Test-send geweigerd door de guard (batch-guard / ontbrekende confirm of env)."""
    __test__ = False   # pytest: geen test-class (naam begint toevallig met 'Test')


def test_mode_active() -> bool:
    """True als TEST_MODE expliciet aanstaat. Alles hieronder is no-op als dit False is."""
    return (os.getenv("TEST_MODE") or "").strip().lower() in ("1", "true", "yes", "on")


def test_recipient() -> str:
    """Het (enige) toegestane test-ontvangeradres uit env, of ''."""
    return (os.getenv("TEST_RECIPIENT") or "").strip()


def enforce_and_reroute(targets: list[dict], *, confirm_test: bool) -> list[str]:
    """Pas de test-send-guard toe op een batch dispatch-targets (lead-dicts).

    NO-OP als TEST_MODE uit staat (returnt []). Anders, in volgorde:
      - eist confirm_test=True (anders TestSendBlocked);
      - eist een niet-lege TEST_RECIPIENT (anders TestSendBlocked);
      - eist dat ELK target is_test_lead=True is — één echt record → TestSendBlocked
        op de HELE batch (test en echt nooit samen);
      - herschrijft daarna elk target['email'] IN-PLACE naar TEST_RECIPIENT.

    Returns de lijst originele e-mailadressen (voor logging/rapport). Muteert de
    dicts in-place zodat de send-callable die erover sluit de reroute meekrijgt.
    """
    if not test_mode_active():
        return []
    if not confirm_test:
        raise TestSendBlocked(
            "TEST_MODE=1 maar geen --confirm-test — test-send geweigerd (expliciete "
            "bevestiging vereist).")
    rcpt = test_recipient()
    if not rcpt:
        raise TestSendBlocked(
            "TEST_MODE=1 maar TEST_RECIPIENT is leeg — test-send geweigerd (geen "
            "doeladres om naar te herschrijven).")
    # Batch-guard VÓÓR enige reroute: één niet-test record aborteert alles.
    for t in targets:
        if not bool(t.get("is_test_lead")):
            raise TestSendBlocked(
                f"TEST_MODE=1 maar target {t.get('id')!r} is geen is_test_lead — "
                "hele batch geabort. Test en echt mogen nooit in dezelfde run zitten.")
    originals: list[str] = []
    for t in targets:
        originals.append((t.get("email") or "").strip())
        t["email"] = rcpt
    return originals
