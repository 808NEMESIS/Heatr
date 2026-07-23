"""tests/test_lead_data_quality.py — datakwaliteit-gate (Sami punt 3, 2026-07-22).

Fixtures = de echte testrun-gevallen die als datafout doorglipten."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.lead_data_quality import check_contact_data_quality as chk


def test_de_huidkliniek_webbureau_email_flagged():
    # e-mail hoort bij het webbureau interip.nl, niet bij de kliniek
    r = chk("sales@interip.nl", "dehuidkliniekrotterdam.nl")
    assert not r["ok"] and r["block_name_enrichment"]
    assert any("bureau" in f for f in r["flags"])


def test_matching_domain_ok():
    r = chk("info@kveg.nl", "https://www.kveg.nl/")
    assert r["ok"] and not r["block_name_enrichment"] and not r["flags"]


def test_subdomain_email_same_company_ok():
    r = chk("afspraak@mail.kveg.nl", "kveg.nl")
    assert r["ok"] and not r["block_name_enrichment"]


def test_freemail_is_flag_not_block():
    # eenmanszaak-signaal, maar naam zoeken blijft zinvol
    r = chk("petranederhoed@gmail.com", "naturalbeautysalon.nl")
    assert r["ok"] and not r["block_name_enrichment"]
    assert any("freemail" in f for f in r["flags"])


def test_generic_inbox_foreign_domain_flagged():
    r = chk("info@marketingbureau.nl", "mijnkliniek.nl")
    assert not r["ok"] and any("bureau" in f or "generieke" in f for f in r["flags"])


def test_no_email_blocks():
    r = chk(None, "kliniek.nl")
    assert not r["ok"] and r["block_name_enrichment"]
