"""
tests/test_icp_scoring.py — pure scoring-functies (2026-07-14).

Dekt de scoring-fix: `compute_icp_match` normaliseert nu over ALLEEN de
evalueerbare componenten (SBI/size conditioneel), gebruikt keyword-saturatie op
een absoluut aantal matches, en leest de echte kolom `kvk_sbi_code`. Voorheen
was `max_score` altijd 1.0 → delen was een no-op en icp_match capte rond 0.45.

Pure functies → geen DB-mocks nodig. Run: pytest tests/test_icp_scoring.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scoring.icp_matcher import compute_icp_match, _parse_employee_count
from scoring.lead_scoring import compute_lead_score


# Minimale sector-config in dezelfde vorm als config/sectors.py get_sector():
# top-level lead_keywords + subcategories{icp_signals, disqualifiers} + sbi_codes.
def _sector(*, keywords=None, sbi_codes=None, disqualifiers=None,
            sub_disqualifiers=None, n_keywords=None):
    kws = keywords or ["botox", "filler", "huidverbetering", "laser", "peeling",
                       "injectable", "rimpel", "acne"]
    if n_keywords is not None:  # pad tot n_keywords met unieke fillers
        kws = list(kws)
        while len(kws) < n_keywords:
            kws.append(f"kw{len(kws)}")
    return {
        "lead_keywords": kws,
        # sub_disqualifiers = DISAMBIGUATIE (mag NIET sector-globaal disqualificeren);
        # disqualifiers = sector-brede uitsluiting (WEL). Zie icp_matcher-fix 2026-07-22.
        "subcategories": {"main": {"icp_signals": [], "disqualifiers": sub_disqualifiers or []}},
        "sbi_codes": sbi_codes or [],
        "disqualifiers": disqualifiers or [],
    }


def _lead(**over):
    base = {
        "company_name": "Kliniek Zonneveld",
        "company_summary": "",
        "google_rating": 4.6,
        "domain": "kliniekzonneveld.nl",
        "email_status": "valid",
    }
    base.update(over)
    return base


# ==============================================================================
# compute_icp_match — normalisatie + conditionele denominator
# ==============================================================================

class TestIcpNormalisation:
    def test_strong_lead_without_kvk_scores_high(self):
        """5+ keyword-matches, rating≥4.5, site, mail — zónder KvK/size-data.
        Denominator = 0.35+0.15+0.10+0.05 = 0.65 (geen SBI/size); numerator vol
        → icp_match ~1.0. Vroeger capte dit op ~0.45."""
        lead = _lead(company_summary="botox filler laser peeling injectable rimpel")
        r = compute_icp_match(lead, _sector())
        assert r["icp_match"] >= 0.95
        assert r["evaluable_max"] == 0.65  # SBI en size NIET meegeteld
        assert "sbi_match" not in r["signals"] and "size_fit" not in r["signals"]

    def test_median_lead_reasonable(self):
        """3 matches (0.21 van 0.35), rating 4.0 (0.10 van 0.15), site, mail.
        score=0.21+0.10+0.10+0.05=0.46 / 0.65 ≈ 0.71 — echte spreiding."""
        lead = _lead(google_rating=4.0, company_summary="botox filler laser")
        r = compute_icp_match(lead, _sector())
        assert 0.65 <= r["icp_match"] <= 0.75

    def test_weak_lead_scores_low(self):
        """Geen keyword-matches, geen rating, wel site+mail → laag maar niet 0."""
        lead = _lead(google_rating=0, company_summary="", company_name="X BV")
        r = compute_icp_match(lead, _sector())
        assert r["icp_match"] < 0.30


class TestKeywordSaturation:
    def test_saturation_absolute_not_listlength_dependent(self):
        """5 matches = volle keyword-bijdrage, onafhankelijk van lijstlengte:
        zelfde uitkomst bij een 13- en een 137-keywords-sector."""
        summary = "botox filler huidverbetering laser peeling"  # 5 matches
        small = compute_icp_match(_lead(company_summary=summary), _sector(n_keywords=13))
        large = compute_icp_match(_lead(company_summary=summary), _sector(n_keywords=137))
        assert small["icp_match"] == large["icp_match"]

    def test_two_matches_partial(self):
        """2 van 5 = 0.14 van de 0.35-component."""
        lead = _lead(company_summary="botox filler")  # 2 matches
        r = compute_icp_match(lead, _sector())
        # keyword-bijdrage 2/5*0.35=0.14; +rating0.15+web0.10+mail0.05=0.44 /0.65
        assert 0.65 <= r["icp_match"] <= 0.70
        assert "keywords:2" in r["signals"]


class TestSbiConditional:
    def test_matching_sbi_counts_in_num_and_denom(self):
        lead = _lead(kvk_sbi_code="86919", company_summary="botox filler laser peeling injectable")
        r = compute_icp_match(lead, _sector(sbi_codes=["86919", "96022"]))
        assert "sbi_match" in r["signals"]
        assert r["evaluable_max"] == 0.85  # 0.65 + 0.20 SBI

    def test_nonmatching_sbi_in_denom_not_num(self):
        """Lead heeft SBI-data maar verkeerde code → wél in noemer (penalty),
        geen punten."""
        lead = _lead(kvk_sbi_code="99999", company_summary="botox filler laser peeling injectable")
        r = compute_icp_match(lead, _sector(sbi_codes=["86919"]))
        assert "sbi_match" not in r["signals"]
        assert r["evaluable_max"] == 0.85  # SBI telt in de noemer
        assert r["icp_match"] < 1.0        # gedrukt door de gemiste 0.20

    def test_no_sbi_data_excludes_component(self):
        lead = _lead(company_summary="botox filler laser peeling injectable")
        r = compute_icp_match(lead, _sector(sbi_codes=["86919"]))
        assert r["evaluable_max"] == 0.65  # geen SBI-data → component weg

    def test_reads_kvk_sbi_code_column(self):
        """De echte kolom is kvk_sbi_code, niet het phantom sbi_code."""
        lead = _lead(kvk_sbi_code="86919", company_summary="botox")
        r = compute_icp_match(lead, _sector(sbi_codes=["86919"]))
        assert "sbi_match" in r["signals"]


class TestSizeConditional:
    def test_parse_employee_count_variants(self):
        assert _parse_employee_count({"kvk_employee_count_range": "2-10"}) == 2
        assert _parse_employee_count({"company_size_estimate": "1-5 medewerkers"}) == 1
        assert _parse_employee_count({"kvk_employee_count_range": "50-100"}) == 50
        assert _parse_employee_count({"company_size_estimate": "onbekend"}) is None
        assert _parse_employee_count({}) is None

    def test_size_in_range_scores(self):
        lead = _lead(kvk_employee_count_range="2-10", company_summary="botox filler laser peeling injectable")
        r = compute_icp_match(lead, _sector())
        assert "size_fit" in r["signals"]
        assert r["evaluable_max"] == 0.80  # 0.65 + 0.15 size

    def test_size_out_of_range_in_denom_only(self):
        lead = _lead(company_size_estimate="50-100", company_summary="botox filler laser peeling injectable")
        r = compute_icp_match(lead, _sector())
        assert "size_fit" not in r["signals"]
        assert r["evaluable_max"] == 0.80  # telt in noemer, geen punten

    def test_no_size_data_excludes_component(self):
        lead = _lead(company_summary="botox filler laser peeling injectable")
        r = compute_icp_match(lead, _sector())
        assert r["evaluable_max"] == 0.65


class TestDisqualifiersAndEdges:
    def test_disqualifier_zeroes_out(self):
        lead = _lead(company_summary="wij zijn een dierenarts praktijk")
        r = compute_icp_match(lead, _sector(disqualifiers=["dierenarts"]))
        assert r["icp_match"] == 0.0
        assert r["signals"] == ["disqualified"]

    def test_subcategory_disqualifier_does_not_zero_out(self):
        # Regressie 2026-07-22: subcategory-disqualifiers zijn DISAMBIGUATIE, geen
        # sector-uitsluiting. "medisch" in de schoonheidssalons-subcat disqualificeerde
        # 51% van de cosmetische ICP (medisch-esthetische klinieken). Een subcategory-
        # disqualifier mag de score NIET nullen.
        lead = _lead(company_summary="medisch esthetisch centrum met botox en laser")
        r = compute_icp_match(lead, _sector(sub_disqualifiers=["medisch", "arts"]))
        assert r["icp_match"] > 0.0
        assert r["signals"] != ["disqualified"]
        # sector-brede disqualifier vuurt WÉL, ook al matcht ook een keyword
        r2 = compute_icp_match(lead, _sector(disqualifiers=["ziekenhuis"],
                                             sub_disqualifiers=["medisch"]))
        assert r2["icp_match"] > 0.0  # "ziekenhuis" staat niet in de tekst

    def test_no_keywords_configured_still_normalises(self):
        lead = _lead(company_summary="botox")
        r = compute_icp_match(lead, _sector(keywords=[]))
        # keyword-max blijft in de noemer (0.35) ook zonder keywords
        assert r["evaluable_max"] == 0.65
        assert r["icp_match"] >= 0.0


# ==============================================================================
# compute_lead_score — dimensie-caps + review-bonus (formules ongewijzigd)
# ==============================================================================

class TestLeadScore:
    def test_fit_from_icp_plus_review_bonus(self):
        r = compute_lead_score({"google_review_count": 120}, icp_match=1.0)
        assert r["fit_score"] == 40  # int(1.0*40)=40, +4 capped op 40

    def test_review_bonus_tiers(self):
        assert compute_lead_score({"google_review_count": 60}, 0.5)["fit_score"] == 23  # 20+3
        assert compute_lead_score({"google_review_count": 25}, 0.5)["fit_score"] == 22  # 20+2
        assert compute_lead_score({"google_review_count": 8}, 0.5)["fit_score"] == 21   # 20+1
        assert compute_lead_score({"google_review_count": 0}, 0.5)["fit_score"] == 20   # geen bonus

    def test_dimension_caps_and_total(self):
        lead = {
            "google_review_count": 200,
            "email_status": "valid",
            "contact_first_name": "Anna", "contact_source": "website_team_page",
            "phone": "0612345678", "gdpr_safe": True, "contact_linkedin_url": "x",
            "email": "info@kliniek.nl", "domain": "kliniek.nl",
            "personalization_hooks": ["a", "b", "c", "d"],
            "personalization_observations": ["x", "y", "z", "w", "v"],
            "company_positioning": "premium", "personalized_opener": "Hoi",
        }
        r = compute_lead_score(lead, icp_match=1.0)
        assert r["fit_score"] == 40
        assert r["reachability_score"] == 25   # capped
        assert r["personalization_potential"] == 15  # capped
        assert r["score"] <= 100

    def test_zero_icp_low_score(self):
        r = compute_lead_score({}, icp_match=0.0)
        assert r["fit_score"] == 0
        assert r["score"] < 65  # blijft onder een realistische drempel
