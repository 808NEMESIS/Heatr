"""
config/scoring_thresholds.py — website-score-drempels, ÉÉN bron van waarheid.

Percentiel-verankerd (herijking 2026-07-21, keuze B op de post-visual-dekking-
verdeling, ICP-cohort n=838): p25/p50/p75. Vervangt de uiteenlopende oude sets
(classifier 30/40/50, frontend 30/45/70, nergens-afgedwongen MIN_WEBSITE=50).

De frontend (frontend-next/src/lib/format.ts + WebsiteKansen.tsx) hardcodeert
DEZELFDE getallen met een verwijzing hierheen — houd ze gelijk.

Vier banden:
  score < URGENT (41)        → urgent   (onderste ~25%)
  URGENT..HIGH (41-49)       → hoog
  HIGH..OPPORTUNITY (49-56)  → medium
  score >= OPPORTUNITY (56)  → laag     (top ~25%, geen website-kans)
"""
WEBSITE_SCORE_URGENT = 41       # < p25
WEBSITE_SCORE_HIGH = 49         # < p50  (ook: website_rebuild-kans-grens)
WEBSITE_SCORE_OPPORTUNITY = 56  # < p75  (>= = top-kwart, MIN_WEBSITE_SCORE_FOR_OPPORTUNITY)
