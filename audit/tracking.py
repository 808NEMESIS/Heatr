"""
audit/tracking.py — pre-consent tracking-detectie over de netwerk-log.

De netwerk-log (heatr_website_network_log) heeft de fasen al: pre_freeze (vóór
een consent-mogelijkheid) en post_freeze. Maar `is_third_party` is te breed voor
een AVG-claim — CDN's, fonts en maps-tiles zijn óók third-party maar geen
tracking. Daarom een expliciete blocklist (echte trackers) + allowlist (neutrale
resources), en ALLEEN `pre_freeze`-matches gelden als harde bevinding.

Conservatief: false positives zijn hier duurder dan false negatives (je verwijt
een ondernemer een wetsovertreding). Alleen bekende tracker-patronen matchen; de
gematchte request wordt als bewijs vastgelegd.
"""
from __future__ import annotations

from urllib.parse import urlparse

# Blocklist — URL-substring -> herkenbare tracker-naam. Substring dekt zowel
# host-patronen (facebook.net) als pad-patronen (linkedin.com/px).
_TRACKERS: dict[str, str] = {
    "connect.facebook.net": "Facebook Pixel",
    "facebook.net": "Facebook Pixel",
    "google-analytics.com": "Google Analytics",
    "googletagmanager.com": "Google Tag Manager",
    "analytics.google.com": "Google Analytics",
    "doubleclick.net": "Google Ads (DoubleClick)",
    "googleadservices.com": "Google Ads",
    "hotjar.com": "Hotjar",
    "static.hotjar.com": "Hotjar",
    "clarity.ms": "Microsoft Clarity",
    "px.ads.linkedin.com": "LinkedIn Insight",
    "snap.licdn.com": "LinkedIn Insight",
    "linkedin.com/px": "LinkedIn Insight",
    "analytics.tiktok.com": "TikTok Pixel",
    "tiktok.com/i18n/pixel": "TikTok Pixel",
}

# Allowlist — neutrale third-party resources; nooit als tracking flaggen.
# (Puur ter documentatie/veiligheid; de blocklist is leidend — we flaggen alleen
# wat expliciet een tracker is, niet "alles wat niet neutraal is".)
_NEUTRAL: tuple[str, ...] = (
    "fonts.googleapis.com", "fonts.gstatic.com",
    "maps.googleapis.com", "maps.gstatic.com", "maps.google.com",
    "google.com/recaptcha", "gstatic.com/recaptcha", "recaptcha.net",
    "cdnjs.cloudflare.com", "cdn.jsdelivr.net", "unpkg.com", "ajax.googleapis.com",
)


def _match_tracker(url: str) -> str | None:
    """Naam van de tracker als de URL er een is, anders None. Neutrale eerst uit."""
    u = (url or "").lower()
    if not u:
        return None
    if any(n in u for n in _NEUTRAL):
        return None
    for pat, name in _TRACKERS.items():
        if pat in u:
            return name
    return None


def detect_pre_consent_tracking(requests: list[dict]) -> dict:
    """Vind trackers die vuurden vóór het vries-moment (pre_freeze).

    Args:
        requests: de `requests`-lijst uit de laatste heatr_website_network_log-rij
            (elk met url, phase, is_third_party, timing_ms, ...).

    Returns:
        {
          "has_pre_consent_tracking": bool,
          "trackers": [{"name","url","phase","timing_ms"}],   # pre_freeze-hits (bewijs)
          "post_consent_only": [...],   # trackers die pas post_freeze vuurden (context, geen claim)
        }
    """
    pre, post = [], []
    for r in requests or []:
        name = _match_tracker(r.get("url", ""))
        if not name:
            continue
        hit = {"name": name, "url": r.get("url"), "phase": r.get("phase"),
               "timing_ms": r.get("timing_ms")}
        if r.get("phase") == "pre_freeze":
            pre.append(hit)
        else:
            post.append(hit)
    return {
        "has_pre_consent_tracking": bool(pre),
        "trackers": pre,
        "post_consent_only": post,
    }
