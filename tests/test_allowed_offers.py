"""
tests/test_allowed_offers.py — sector-gebonden aanbod-logica (2026-07-20).

Cosmetiek = volledige funnel (website + automatisering); alt-zorg/chiro = alleen
website, NOOIT automatisering/chatbot/ai_audit (compliance: pijnklachten + medische
info). Onbekende sector → website-only (veilige default).
"""
from config.sectors import get_allowed_offers, SECTORS
from website_intelligence.opportunity_classifier import classify_opportunities, _OFFER_CATEGORY
from campaigns.review_email_generator import _derive_top_issue


# ── config ──────────────────────────────────────────────────────────────────
def test_allowed_offers_per_sector():
    assert "automatisering" in get_allowed_offers("cosmetische_behandelaars")
    assert "automatisering" not in get_allowed_offers("alternatieve_geneeskunde")
    assert "automatisering" not in get_allowed_offers("chiropractoren")
    for sec in ("alternatieve_geneeskunde", "chiropractoren"):
        assert set(get_allowed_offers(sec)) == {"website_rebuild", "conversie_optimalisatie"}


def test_unknown_sector_defaults_website_only():
    assert "automatisering" not in get_allowed_offers("makelaars")
    assert "automatisering" not in get_allowed_offers(None)


def test_every_active_sector_has_allowed_offers():
    for sec, cfg in SECTORS.items():
        assert cfg.get("allowed_offers"), f"{sec} mist allowed_offers"


# ── classifier: sector-poort ────────────────────────────────────────────────
_LOW = dict(total_score=25, technical_result={"cms": "Wix"},
            conversion_result={"conversion_score": 5, "has_whatsapp": False,
                               "has_online_booking": False, "has_chatbot": False},
            sector_result={})


def test_cosmetic_gets_automatisering_offers():
    out = classify_opportunities(**_LOW, sector="cosmetische_behandelaars")
    assert "chatbot" in out["opportunity_types"]
    assert "ai_audit" in out["opportunity_types"]


def test_altmed_never_gets_automatisering():
    out = classify_opportunities(**_LOW, sector="alternatieve_geneeskunde")
    assert "chatbot" not in out["opportunity_types"]
    assert "ai_audit" not in out["opportunity_types"]
    # wel de website-angles
    assert set(out["opportunity_types"]) <= {"website_rebuild", "conversie_optimalisatie"}


def test_chiro_never_gets_automatisering():
    out = classify_opportunities(**_LOW, sector="chiropractoren")
    assert all(_OFFER_CATEGORY.get(t) != "automatisering" for t in out["opportunity_types"])


def test_strong_site_altmed_falls_back_to_website_angle_not_empty():
    """Sterke site (geen website-pijn): chatbot/ai_audit eruit gefilterd → mag
    niet leeg blijven, val terug op de sterkste toegestane website-angle."""
    strong = dict(total_score=80,
                  technical_result={"cms": "modern"},
                  conversion_result={"conversion_score": 25, "has_whatsapp": True,
                                     "has_online_booking": True, "has_chatbot": True},
                  sector_result={})
    out = classify_opportunities(**strong, sector="alternatieve_geneeskunde")
    assert out["opportunity_types"] == ["conversie_optimalisatie"]
    # cosmetiek in dezelfde staat houdt wél ai_audit
    out_c = classify_opportunities(**strong, sector="cosmetische_behandelaars")
    assert "ai_audit" in out_c["opportunity_types"]


def test_unknown_sector_filtered_website_only():
    out = classify_opportunities(**_LOW, sector=None)
    assert all(_OFFER_CATEGORY.get(t) != "automatisering" for t in out["opportunity_types"])


# ── review-email: geen chat/AI-angle voor alt-zorg ──────────────────────────
_WI_NO_CHAT = {"technical_score": 15, "conversion_score": 5,
               "conversion_details": {"has_online_booking": True, "has_whatsapp": True,
                                      "has_chatbot": False}}


def test_review_email_chat_angle_only_when_allowed():
    assert _derive_top_issue(_WI_NO_CHAT, allow_automatisering=True) == "geen chat voor snelle vragen"
    # alt-zorg: chat-angle onderdrukt → valt door naar generieke conversie
    assert _derive_top_issue(_WI_NO_CHAT, allow_automatisering=False) == "ontbrekende conversie-elementen"
