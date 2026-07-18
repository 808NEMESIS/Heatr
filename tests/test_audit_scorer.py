"""
tests/test_audit_scorer.py — unit-tests voor de prospect-facing audit-scorer.

Dekt de pure logica (geen DB/HTTP/Claude/Places nodig): tracking-detectie,
NL-trust, gewichten, check-framework + aggregatie, knock-outs, lege-site,
Places-parsing/framing, en de benchmark-aggregatie.
"""
import asyncio
import os

import config.audit_weights as W


def _run(coro):
    """Draai een coroutine zonder de gedeelde event-loop te sluiten.

    asyncio.run() sluit de loop -> volgende tests die
    asyncio.get_event_loop().run_until_complete() gebruiken falen ('no current
    event loop'). Deze helper hergebruikt/herstelt de loop en sluit 'm niet.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop.run_until_complete(coro)
from audit import checks as C
from audit import tracking, nl_trust, places, benchmark
from audit.scorer import _knockouts, _dom_text_len


# ── tracking ────────────────────────────────────────────────────────────────
def test_tracking_flags_pre_consent_trackers_only():
    reqs = [
        {"url": "https://kliniek.nl/", "phase": "pre_freeze"},
        {"url": "https://www.google-analytics.com/g/collect", "phase": "pre_freeze"},
        {"url": "https://connect.facebook.net/fbevents.js", "phase": "pre_freeze"},
        {"url": "https://fonts.googleapis.com/css2", "phase": "pre_freeze"},
        {"url": "https://maps.googleapis.com/maps/api/js", "phase": "pre_freeze"},
        {"url": "https://www.google.com/recaptcha/api.js", "phase": "pre_freeze"},
        {"url": "https://static.hotjar.com/c/hotjar.js", "phase": "post_freeze"},
    ]
    r = tracking.detect_pre_consent_tracking(reqs)
    assert r["has_pre_consent_tracking"] is True
    assert sorted(t["name"] for t in r["trackers"]) == ["Facebook Pixel", "Google Analytics"]
    assert [t["name"] for t in r["post_consent_only"]] == ["Hotjar"]


def test_tracking_empty_is_clean():
    r = tracking.detect_pre_consent_tracking([])
    assert r["has_pre_consent_tracking"] is False and r["trackers"] == []


# ── nl_trust ────────────────────────────────────────────────────────────────
def test_big_number_requires_context():
    assert [b["number"] for b in nl_trust.find_big_numbers("BIG-nummer 12345678901")] == ["12345678901"]
    assert nl_trust.find_big_numbers("KvK-nummer 12345678901") == []


def test_keurmerken_per_sector():
    assert [k["keurmerk"] for k in nl_trust.find_keurmerken("ZKN en NVCG", "cosmetische_behandelaars")] == ["ZKN", "NVCG"]
    assert [k["keurmerk"] for k in nl_trust.find_keurmerken("SCN en NCA", "chiropractoren")] == ["SCN", "NCA"]


def test_wkkgz_and_geschil_and_opleiding():
    assert nl_trust.has_wkkgz("volgens de Wkkgz")
    assert nl_trust.has_geschilleninstantie("aangesloten bij een geschilleninstantie")
    assert nl_trust.has_erkende_opleiding_chiro("Anglo-European College of Chiropractic")


# ── gewichten ───────────────────────────────────────────────────────────────
def test_category_maxes_and_niche_scale():
    assert W.total_max("cosmetische_behandelaars") == 108
    assert W.total_max("chiropractoren") == 107
    cos = W.category_max("cosmetische_behandelaars")
    assert cos["lead_conversion"] == 32 and cos["social_proof"] == 18 and cos["visual"] == 8


def test_all_check_ids_have_a_function():
    cfg = {c["check_id"] for c in W.CHECKS}
    assert cfg == set(C.CHECK_FUNCS)


# ── check-framework + aggregatie ────────────────────────────────────────────
def _agg(ctx, sector):
    findings = [C.CHECK_FUNCS[c["check_id"]](ctx) for c in W.checks_for_sector(sector)]
    cats: dict = {}
    for f in findings:
        if f["status"] == "not_measurable":
            continue
        d = cats.setdefault(f["categorie"], {"b": 0, "m": 0})
        d["b"] += f["punten_behaald"]; d["m"] += f["punten_max"]
    ach = sum(d["b"] for d in cats.values()); den = sum(d["m"] for d in cats.values())
    return findings, cats, ach, den, (round(ach / den * 100) if den else 0)


def _good_ctx():
    return C.ScoreContext(
        lead={"has_online_booking": True, "has_whatsapp": True, "phone": "0201234567",
              "google_rating": 4.7, "google_review_count": 87, "google_maps_url": "g",
              "has_cookie_banner": True, "sector": "cosmetische_behandelaars"},
        wi={"conversion_details": {"has_phone_clickable": True, "has_cta_above_fold": True,
                                   "has_contact_form": True, "form_field_count": 4, "has_chatbot": True},
            "technical_details": {"has_ssl": True, "has_sitemap": True, "pagespeed_mobile": 62},
            "visual_score": 18, "team_contacts": [{"full_name": "Dr. Jansen"}]},
        network_requests=[{"url": "https://google-analytics.com/g", "phase": "pre_freeze"}],
        page_text="Maak een consult. BIG-nummer 12345678901. ZKN. Wkkgz klachtenregeling. "
                  "Privacyverklaring. Openingstijden ma-vr. Adres 1012 AB Amsterdam. Weiger cookies.",
        schema_org={"@type": "MedicalClinic", "openingHours": "Mo-Fr"},
        response_headers={"strict-transport-security": "x"},
        html="<title>Kliniek</title><h1>Welkom</h1><iframe src='https://www.google.com/maps/embed'></iframe>"
             "<a href='/behandeling/botox'>b</a><a href='/behandeling/filler'>f</a>"
             "<a href='/behandeling/laser'>l</a><a href='/contact'>c</a> weiger cookies privacyverklaring",
        sector="cosmetische_behandelaars")


def test_good_site_scores_well_and_privacy_reads_page_text():
    # regressie op de html-or-page_text short-circuit bug: privacy zit in page_text
    ctx = _good_ctx()
    _, cats, ach, den, norm = _agg(ctx, "cosmetische_behandelaars")
    assert norm >= 75
    assert cats["privacy"]["b"] >= 6  # privacyverklaring + cookiebanner gevonden via text_all


def test_tracking_finding_is_not_mail_safe():
    ctx = _good_ctx()
    findings, *_ = _agg(ctx, "cosmetische_behandelaars")
    trk = next(f for f in findings if f["check_id"] == "geen_tracking_pre_consent")
    assert trk["status"] == "fail" and trk["mail_safe"] is False


def test_not_measurable_excluded_from_denominator():
    ctx = _good_ctx()
    findings, cats, *_ = _agg(ctx, "cosmetische_behandelaars")
    nm = [f["check_id"] for f in findings if f["status"] == "not_measurable"]
    # echte_praktijkfotos + lcp zijn structureel not_measurable
    assert "echte_praktijkfotos" in nm and "lcp_onder_2_5" in nm


def test_knockouts():
    # geen afspraak + geen telefoon -> laagste cap (70)
    ctx = C.ScoreContext(lead={}, wi={}, network_requests=[], page_text="", schema_org={},
                         response_headers={}, html="", sector="cosmetische_behandelaars")
    capped, reason = _knockouts(ctx, 90)
    assert capped == 70 and reason == "geen enkele afspraakmogelijkheid"
    # wel telefoon, geen afspraak -> 70
    ctx2 = C.ScoreContext(lead={"phone": "0201234567"}, wi={}, network_requests=[], page_text="",
                          schema_org={}, response_headers={}, html="", sector="cosmetische_behandelaars")
    assert _knockouts(ctx2, 90)[0] == 70
    # afspraak + telefoon -> geen cap
    ctx3 = C.ScoreContext(lead={"phone": "0201234567", "has_online_booking": True}, wi={},
                          network_requests=[], page_text="", schema_org={}, response_headers={},
                          html="", sector="cosmetische_behandelaars")
    assert _knockouts(ctx3, 90) == (90, None)


def test_empty_site_detection():
    assert _dom_text_len("<html><body>IN THE FACE OF PERFECTION</body></html>", "") < W.EMPTY_SITE_DOM_CHARS
    assert _dom_text_len("<p>" + "inhoud " * 100 + "</p>", "") >= W.EMPTY_SITE_DOM_CHARS


# ── places ──────────────────────────────────────────────────────────────────
def test_parse_place_details():
    assert places.parse_place_details({"result": {"rating": 4.7, "user_ratings_total": 87, "name": "X"}}) == \
        {"rating": 4.7, "review_count": 87, "name": "X"}
    assert places.parse_place_details({}) == {}


def test_review_finding_branches():
    strong_hidden = places.build_review_finding({"rating": 4.7, "review_count": 87}, on_site_shown=False)
    assert strong_hidden["status"] == "fail" and strong_hidden["mail_safe"] and "87" in strong_hidden["mail_zin"]
    assert places.build_review_finding({"rating": 4.7, "review_count": 87}, True)["status"] == "pass"
    assert places.build_review_finding({"rating": 3.9, "review_count": 4}, False)["status"] == "fail"
    assert places.build_review_finding({}, False)["status"] == "not_measurable"


def test_get_place_reviews_fake_fetcher_and_no_key():
    async def fake(url, sb):
        if "findplace" in url:
            return {"candidates": [{"place_id": "PID"}]}
        return {"result": {"rating": 4.6, "user_ratings_total": 54, "name": "X"}}
    r = _run(places.get_place_reviews({"company_name": "A", "city": "B"}, None, api_key="k", fetcher=fake))
    assert r["rating"] == 4.6 and r["review_count"] == 54 and r["place_id"] == "PID"
    os.environ.pop("GOOGLE_PLACES_API_KEY", None)
    assert _run(places.get_place_reviews({"company_name": "A", "city": "B"}, None)) == {"error": "no_places_key"}


# ── tier 2: reviews via Places ──────────────────────────────────────────────
def test_tier2_check_excluded_from_tier1_sums():
    """tier2_only-checks tellen niet mee in de tier-1-schaal (108/107 intact)."""
    t1 = {c["check_id"] for c in W.checks_for_sector("cosmetische_behandelaars")}
    t2 = {c["check_id"] for c in W.checks_for_sector("cosmetische_behandelaars", include_tier2=True)}
    assert "reviews_via_places" not in t1
    assert "reviews_via_places" in t2
    assert W.total_max("cosmetische_behandelaars") == 108  # ongewijzigd


def test_reviews_via_places_not_measurable_without_places():
    ctx = _good_ctx()  # geen ctx.places
    f = C.c_reviews_via_places(ctx)
    assert f["status"] == "not_measurable"


def test_reviews_via_places_strong_hidden_uses_places_data():
    ctx = _good_ctx()
    ctx.places = {"rating": 4.7, "review_count": 87}
    ctx.text_all = "hier staat niets relevants"   # site toont geen reviews
    f = C.c_reviews_via_places(ctx)
    assert f["status"] == "fail" and "87" in f["mail_zin"] and f["mail_safe"]


def test_google_rating_min_prefers_places():
    ctx = _good_ctx()   # lead: 4.7 / 87
    ctx.places = {"rating": 3.0, "review_count": 5}   # Places is verser -> wint
    f = C.c_google_rating_min(ctx)
    assert f["status"] == "fail" and "3.0" in str(f["bewijs"])


# ── benchmark ───────────────────────────────────────────────────────────────
def test_benchmark_needs_min_n_and_is_provisional():
    small = benchmark._benchmark_from_scores(60, [50, 55, 58], city="X", sector="chiropractoren")
    assert small["percentile"] is None and small["provisional"] is True and "minder dan 10" in small["note"]


def test_benchmark_rank_and_percentile():
    cohort = list(range(40, 63))  # 23 anderen
    bm = benchmark._benchmark_from_scores(68, cohort, city="Groningen", sector="cosmetische_behandelaars")
    assert bm["n"] == 24 and bm["provisional"] is True and bm["percentile"] is not None
    assert bm["above"] == 0 and "van de 24 cosmetische klinieken in Groningen" in bm["rank_sentence"]
