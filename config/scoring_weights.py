"""
config/scoring_weights.py — Lead scoring and website scoring weight definitions.

Two separate dicts:
  LEAD_SCORING_WEIGHTS  — factors that determine lead quality (email, signals, fit)
  WEBSITE_SCORING_WEIGHTS — factors for the 5-layer website analysis (max 100 pts)

Important design decision: a bad website score does NOT reduce the lead score.
A bad website = opportunity for Aerys → it should INCREASE commercial value.
Website score feeds into opportunity classification, not lead qualification.
"""

from __future__ import annotations


# =============================================================================
# Lead Scoring Weights
# Max theoretical score: sum of all positive weights = 100
# Negative weights (penalties) can push the score below the sum of positives.
#
# A lead needs score >= MIN_SCORE_FOR_WARMR (default 65) to push to Warmr.
# =============================================================================

LEAD_SCORING_WEIGHTS: dict[str, int] = {

    # --- Email quality (33 pts total, the most important signal) -------------

    "has_valid_email": 25,
    # A verified, deliverable email address. Without this the lead cannot be
    # emailed at all — the single biggest scoring factor.

    "email_type_role": 8,
    # Role email (info@, contact@, praktijk@) vs personal. Role emails are
    # GDPR-safe and typically reach the right person in small practices.

    "email_discovery_website": 5,
    # Email was found directly on the company website (step 1 of waterfall).
    # Indicates the company actively publishes contact info = warmer lead.

    # --- Company signals (32 pts) --------------------------------------------

    "website_quality": 8,
    # Rough website quality signal (not the full website_intelligence score).
    # Checks: domain resolves, not a Facebook-only presence, not parked page.
    # Note: intentionally low — a bad website is an OPPORTUNITY not a penalty.

    "has_kvk_data": 7,
    # KvK registration confirmed — verifies the business is real and active in NL.

    "company_size_fit": 8,
    # Company size in the target range (1-15 employees for both sectors).
    # Larger companies have procurement processes; smaller are easier to reach.

    "google_rating_above_4": 8,
    # Google rating >= 4.0 signals an active, motivated business owner.
    # Businesses with high ratings care about reputation → responsive to outreach.

    "google_review_count": 5,
    # Number of Google reviews (awarded if > 10 reviews).
    # More reviews = established business, owner likely more reachable.

    # --- Contact quality (9 pts) ---------------------------------------------

    "has_contact_name": 5,
    # A named contact was found (from website, KvK, or Google).
    # Enables personalised openers in Warmr sequences.

    # --- Tech signals (11 pts) -----------------------------------------------

    "cms_detected": 4,
    # A CMS was detected on the website (WordPress, Squarespace, Webflow, etc.).
    # CMS = they invested in a website = they care about digital presence.

    "has_instagram": 4,
    # Instagram account linked or detectable (especially relevant for cosmetische sector).
    # Active social = digitally engaged owner.

    "has_online_booking": 5,
    # Online booking system detected (Acuity, Calendly, custom, etc.).
    # Strong signal of a growth-oriented business — they convert digitally.

    # --- Trust / tracking signals (6 pts) ------------------------------------

    "tracking_tools_detected": 3,
    # Google Analytics, Facebook Pixel, or similar detected.
    # Indicates they are actively marketing = knows digital ROI matters.

    "gdpr_safe": 3,
    # Email passes GDPR check (role email, no personal domain, no firstname.lastname pattern).
    # All Warmr-eligible leads must be gdpr_safe=True regardless of this score.

    # --- Penalties -----------------------------------------------------------

    "catchall_penalty": -10,
    # Email server is a catch-all (accepts any address = unverifiable).
    # Catch-all leads have poor deliverability and skew email stats.
}


# =============================================================================
# Website Scoring Weights
# 5-layer analysis, maximum 100 points total.
# Scores feed into opportunity_classifier.py to determine which Aerys service
# to pitch: website rebuild, conversion optimisation, chatbot, or AI audit.
# =============================================================================

WEBSITE_SCORING_WEIGHTS: dict[str, dict] = {

    # -------------------------------------------------------------------------
    # Layer 1 — Technical (max 25 pts)
    # Checked via Google PageSpeed API + httpx headers + dnspython.
    # -------------------------------------------------------------------------
    "technical": {
        "max_points": 25,

        "has_ssl": 3,
        # HTTPS active. No SSL in 2024 = immediate trust problem.

        "is_mobile_friendly": 4,
        # Google PageSpeed mobile-friendly test passed.
        # Most Dutch SMB traffic is mobile — biggest single conversion killer if absent.

        "pagespeed_mobile_above_50": 5,
        # PageSpeed mobile score > 50. Dutch mobile speeds are high; slow sites lose conversions.

        "pagespeed_desktop_above_70": 3,
        # PageSpeed desktop score > 70. Desktop still dominant for B2B research.

        "cms_modern": 4,
        # CMS is modern (Webflow, Squarespace, WordPress 6+, Framer) vs legacy (Joomla 1.x, Dreamweaver).
        # Old CMS = likely unmaintained, security risk, poor SEO.

        "server_nl_or_be": 2,
        # Server physically located in NL or BE. Affects local SEO and latency.

        "has_sitemap": 1,
        # sitemap.xml accessible. Basic SEO hygiene — low weight but easy win.

        "has_schema_markup": 3,
        # JSON-LD or microdata schema detected. Strong SEO signal for local businesses.
    },

    # -------------------------------------------------------------------------
    # Layer 2 — Visual via Claude Sonnet Vision (max 25 pts)
    # Playwright takes full-page screenshot → uploaded to Supabase Storage →
    # Claude Sonnet Vision analyses it with the sector-specific prompt.
    # -------------------------------------------------------------------------
    "visual": {
        "max_points": 25,

        "overall_impression": 10,
        # Claude Vision score 0-10 for general modern & professional look.
        # Highest weight because first impressions determine if visitors stay.

        "professional_photography": 5,
        # Real photos vs stock photos. In zorg/kliniek sectors authenticity = trust.

        "typography": 5,
        # Readable, modern, consistent typographic hierarchy.

        "color_coherence": 5,
        # Colour palette fits the sector and is applied consistently.
    },

    # -------------------------------------------------------------------------
    # Layer 3 — Conversion (max 30 pts)
    # Playwright + regex detection. Highest max because conversion gaps are
    # the most actionable and profitable improvements Aerys can pitch.
    # -------------------------------------------------------------------------
    "conversion": {
        "max_points": 30,

        "primary_cta_above_fold": 5,
        # A clear primary CTA is visible without scrolling.
        # Above-the-fold CTAs are the single biggest conversion lever.

        "cta_text_strength": 5,
        # Claude Haiku rates the CTA copy quality (1-5 scale mapped to 0-5 pts).
        # Weak CTAs ('click here', 'meer info') vs strong ('Plan gratis gesprek').

        "phone_clickable": 3,
        # Phone number is a tel: link (clickable on mobile).
        # Clinics get significant bookings via phone — dead number = lost revenue.

        "has_whatsapp": 4,
        # WhatsApp contact button/link detected.
        # WhatsApp is the dominant async contact channel in NL/BE SMB.

        "has_online_booking": 6,
        # Online booking widget detected. Highest conversion weight —
        # practices with online booking convert 3-4x more than phone-only.

        "has_chatbot_or_live_chat": 4,
        # Chatbot or live chat platform detected.
        # Absence triggers chatbot_opportunity = True in classifier.

        "contact_form_max_5_fields": 3,
        # Contact form has ≤ 5 fields (fewer fields = more conversions).
        # Forms with > 5 fields lose 40%+ of potential submissions.
    },

    # -------------------------------------------------------------------------
    # Layer 4 — Sector Specific (max 15 pts)
    # Criteria defined per sector in config/sectors.py under
    # sector_website_expectations → must_have (each with their own points).
    # -------------------------------------------------------------------------
    "sector_specific": {
        "max_points": 15,
        # Points allocated per must_have item in sector config.
        # Bonus points from should_have items do not count towards max.
    },

    # -------------------------------------------------------------------------
    # Layer 5 — Competitor Comparison (bonus, no hard max)
    # Top-3 local competitors scraped from Google Maps (same city + sector).
    # Used for score_vs_market calculation, not added to total_score.
    # score_vs_market < -10 = strong pitch: "your competitors outperform you by X pts"
    # -------------------------------------------------------------------------
    "competitor": {
        "max_points": 0,        # Does not add to total_score
        "competitor_count": 3,  # Number of competitors to analyse
        # score_vs_market = total_score - market_average_score
        # Negative = below market → stronger rebuild pitch for Aerys
    },
}


def calculate_max_score(weights_dict: dict) -> int:
    """Calculate the theoretical maximum score for a given scoring weights dict.

    For LEAD_SCORING_WEIGHTS: sums all positive integer values.
    For WEBSITE_SCORING_WEIGHTS: sums the max_points per layer.

    Args:
        weights_dict: Either LEAD_SCORING_WEIGHTS or WEBSITE_SCORING_WEIGHTS.

    Returns:
        Maximum achievable score as an integer.
    """
    total = 0

    for key, value in weights_dict.items():
        if isinstance(value, int) and value > 0:
            # Lead scoring dict: flat int values
            total += value
        elif isinstance(value, dict):
            # Website scoring dict: nested dicts with max_points key
            max_pts = value.get("max_points", 0)
            if isinstance(max_pts, int) and max_pts > 0:
                total += max_pts

    return total
