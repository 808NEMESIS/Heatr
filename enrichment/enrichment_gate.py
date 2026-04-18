"""
enrichment/enrichment_gate.py — Decide which enrichment stages to run per lead.

Cheap stages (no Claude, no API cost) run for all leads.
Expensive stages (Claude calls, Playwright scraping) only run if the lead
passes quality thresholds.

This is the single biggest cost saver — ~40% of leads don't need Claude.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# Thresholds (tune based on real data over time)
MIN_WEBSITE_SCORE_FOR_CLAUDE = 25   # Below this: don't spend Claude credits
MIN_TECHNICAL_SCORE_FOR_REVIEWS = 10  # Broken websites rarely have useful reviews
MIN_CONVERSION_SCORE_FOR_OPENERS = 3  # Websites with zero conversion signals
                                      # have no hooks to reference


class EnrichmentDecision:
    """What to run for a specific lead based on cheap-tier scores."""

    def __init__(
        self,
        run_reviews: bool,
        run_personalization: bool,
        run_openers: bool,
        run_contact_discovery: bool,
        reason: str,
    ):
        self.run_reviews = run_reviews
        self.run_personalization = run_personalization
        self.run_openers = run_openers
        self.run_contact_discovery = run_contact_discovery
        self.reason = reason

    def skips_claude(self) -> bool:
        return not any([
            self.run_reviews,
            self.run_personalization,
            self.run_openers,
            self.run_contact_discovery,
        ])

    def __repr__(self) -> str:
        flags = []
        if self.run_reviews: flags.append("reviews")
        if self.run_personalization: flags.append("personalization")
        if self.run_openers: flags.append("openers")
        if self.run_contact_discovery: flags.append("contacts")
        return f"EnrichmentDecision({', '.join(flags) or 'SKIP_ALL'}: {self.reason})"


def decide_enrichment(
    lead: dict[str, Any],
    technical_score: int,
    conversion_score: int,
    sector_score: int,
) -> EnrichmentDecision:
    """
    Decide which expensive enrichment stages to run for this lead.

    Args:
        lead: Lead dict from DB
        technical_score: 0-25 from technical_checker
        conversion_score: 0-30 from conversion_checker
        sector_score: 0-15+ from sector_checker

    Returns:
        EnrichmentDecision with per-stage booleans
    """
    total = technical_score + conversion_score + sector_score
    google_reviews = lead.get("google_review_count") or 0
    rating = lead.get("google_rating") or 0

    # ── Gate 1: Broken websites ──────────────────────────────────────
    # If technical is 0-5, the website barely works. No point analyzing.
    if technical_score < 5:
        return EnrichmentDecision(
            run_reviews=False,
            run_personalization=False,
            run_openers=False,
            run_contact_discovery=False,
            reason=f"broken_website (tech={technical_score}/25)",
        )

    # ── Gate 2: Very low total score ─────────────────────────────────
    # Lead will never reach push threshold anyway. Skip Claude.
    if total < MIN_WEBSITE_SCORE_FOR_CLAUDE:
        # Still try contact discovery (cheap website scrape, no Claude)
        return EnrichmentDecision(
            run_reviews=False,
            run_personalization=False,
            run_openers=False,
            run_contact_discovery=True,
            reason=f"low_total_score ({total}/70)",
        )

    # ── Gate 3: Very few reviews ─────────────────────────────────────
    # Under 3 reviews = not enough signal for Claude review analysis
    run_reviews = google_reviews >= 3 and technical_score >= MIN_TECHNICAL_SCORE_FOR_REVIEWS

    # ── Gate 4: Zero conversion signals ──────────────────────────────
    # If the site has NOTHING (no CTA, phone, form, booking, chat)
    # there's nothing specific to reference in an opener
    run_openers = conversion_score >= MIN_CONVERSION_SCORE_FOR_OPENERS

    # ── Gate 5: Passed all gates — full enrichment ───────────────────
    return EnrichmentDecision(
        run_reviews=run_reviews,
        run_personalization=True,
        run_openers=run_openers,
        run_contact_discovery=True,
        reason=f"qualified (total={total}/70, reviews={google_reviews})",
    )
