"""
website_intelligence/hook_detector.py — Guardrail-1-proof haakje-detectie.

Fase 2a van de haakje-machine (zie docs/plan_haakje_machine_fase2.md en
docs/haakje_mapping_mail1.md). Detecteert LIVE, op een echte mobiele render,
welk ladder-signaal vuurt voor de mail-1-haakje:

  1. Geen online boekoptie (alleen telefoon)  — hardste geldkoppeling
  2. Boek-CTA onder de vouw op mobiel          — onweerlegbaar, direct pijnlijk
  3. Trage laadtijd (TTI) op mobiel            — rough proxy (zonder 4G-throttle)

Aanleiding: de opgeslagen `website_intelligence`-signalen bleken niet te
vertrouwen (live-verificatie 2026-07-22 — Tajmeel=Salonkee, My Unique=Treatwell
HADDEN boeking, maar het "alleen telefoon"-haakje vuurde toch). Conclusie: geen
haakje mag de deur uit zonder live, Guardrail-1-proof detectie.

Guardrail 1 (spec §Guardrails): vóór een signaal mag vuren wordt de pagina
mobiel geladen, de cookie-/consent-banner weggeklikt, volledig gescrold
(lazy-load), en op `networkidle` gewacht — pas dán wordt gemeten. Bij twijfel
of een mislukte/partiële fetch: NIET vuren (fired_signal=None). Liever een
haakje minder dan één keer aantoonbaar mis.

Puur fail-soft: geen enkele fout mag de batch stoppen; bij een probleem komt er
een resultaat met `error` en `fired_signal=None` terug.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

from playwright.async_api import async_playwright

from utils.playwright_helpers import mobile_context, new_browser_context
from website_intelligence.booking_detector import BOOKING_PLATFORMS, detect_booking

logger = logging.getLogger(__name__)

# iPhone 13 logische viewport-hoogte — de "vouw" op mobiel.
FOLD_PX = 844
# Signaal 2 ("moest een paar schermen scrollen") mag pas vuren als de boek-CTA
# ECHT diep zit: minstens 2 schermen (2x de vouw) naar beneden. Vlak onder de
# vouw (bv. 900px) maakt de haakje-claim onwaar → dan niet vuren (Guardrail 1).
SIGNAL2_MIN_Y = FOLD_PX * 2

# --- Boekplatformen die de canonieke lijst (booking_detector) nog mist. -------
# Salonkee (Tajmeel) en Treatwell (My Unique) waren de bewezen false-negatives.
# Treatwell zit al in de canonieke lijst; Salonkee + de grote beauty/kliniek-
# platformen hieronder ontbraken en zorgden voor foute "geen boeking"-signalen.
_EXTRA_BOOKING_PLATFORMS = [
    "salonkee.",            # Tajmeel — de bewezen misser
    "fresha.com",
    "planity.com", "planity.io",
    "salonlite.",
    "shore.com",
    "skedify.",
    "appointmed.",
    "supersaas.com",
    "timerbee.",
    "aesthetico.",
    "salonized.",           # variant zonder .com
    "onlineafspraken.",
    "afspraken24.",
    "twtimer.",
    "salonsuccess.",
    "mynpriva.",
    "eersteafspraak.",
    "aptus.",
    "walnut.nl",
    "claudia.nl",           # NL kliniek-boekingssoftware
]
# Samengevoegde, ontdubbelde platformlijst voor de haakje-detectie.
ALL_BOOKING_PLATFORMS = list(dict.fromkeys(BOOKING_PLATFORMS + _EXTRA_BOOKING_PLATFORMS))

# Cookie-/consent-banner knop-teksten (NL + EN), voor Guardrail 1.
_COOKIE_TEXT = [
    "alles accepteren", "accepteer alles", "accepteer", "accepteren",
    "ik ga akkoord", "akkoord", "toestaan", "sta toe", "ok, prima",
    "begrepen", "sluiten", "accept all", "accept cookies", "accept", "agree",
    "allow all", "got it", "i agree",
]

# Afspraak-INTENTIE-teksten voor de element-/positiescan. Zelfde canonieke
# definitie als booking_detector (besluit 2026-07-22): widget óf aanvraag-/
# terugbelformulier telt als boekingang; een GENERIEK "contact" telt NIET —
# anders doodt elke contactpagina-link in de nav het signaal (Sami punt 2:
# geen false negatives inruilen voor de false positives).
_BOOK_TEXT = [
    "afspraak maken", "maak een afspraak", "maak afspraak", "online afspraak",
    "afspraak inplannen", "plan een afspraak", "plan je afspraak", "boek nu",
    "boek een", "boek afspraak", "boek je", "direct boeken", "online boeken",
    "afspraak online", "reserveer", "reserveren", "consult aanvragen",
    "consult plannen", "afspraak aanvragen", "bel mij terug", "bel me terug",
    "terugbelverzoek", "book now", "book appointment", "make appointment",
    "schedule", "request a callback",
]
# href-fragmenten die op een boek-bestemming wijzen ("/contact" matcht bewust niet).
_BOOK_HREF_HINTS = ("afspra", "/boek", "booking", "appoint", "reserv", "consult", "terugbel")

# Markers van challenge-/blokkadepagina's. Zo'n render is geen echte pagina →
# machinaal niet verifieerbaar → nooit een signaal; de lead mag alleen na
# handmatige check verder (Sami-regel 2026-07-22). ALLEEN specifieke challenge-
# frasen: bare "cloudflare"/"captcha" matchte ook legitieme CDN-/reCAPTCHA-
# script-tags op gewone pagina's (vals "niet verifieerbaar" op amstelzijde/
# monalisa in de rerun).
_CHALLENGE_MARKERS = (
    "just a moment", "attention required", "cf-challenge", "cf-mitigated",
    "checking your browser", "enable javascript and cookies", "even geduld",
    "verifying you are human", "access denied", "you have been blocked",
    "een ogenblik geduld", "controleren of de verbinding", "ddos-guard",
)

# Geparkeerde/te-koop-domeinen: geen echte site → lead is dood, geen haakje.
_PARKING_MARKERS = (
    "domein te koop", "domain is for sale", "domain for sale", "buy this domain",
    "this domain is parked", "dit domein is te koop", "parkingcrew", "sedoparking",
    "domein geparkeerd", "te koop aangeboden", "godaddy.com/domainfind",
)

# Vision-elementen die GEEN design-signaal zijn maar een overlay/foutpagina —
# een haakje daarover is onzin (of erger: onthult dat de site dood/geblokkeerd
# is). Vision mag hier nooit op vuren (Guardrail, testrun 2026-07-23).
_BAD_VISION_RE = re.compile(
    r"cookie|banner|cloudflare|captcha|beveilig|security|verificat|"
    r"te koop|for sale|geparkeerd|parked|just a moment|even geduld|"
    r"404|foutmelding|error|onderhoud|maintenance|leeg scherm|blank|"
    # third-party overlays/widgets — geen eigen design, geen eerlijk haakje
    r"accessibility|toegankelijk|chatknop|chatbot-knop|widget-knop|"
    r"whatsapp-knop|zweven",
    re.IGNORECASE,
)


def _combine_booking(rendered_html: str, *, fetch_ok: bool) -> dict[str, Any]:
    """Booking-detectie op de GERENDERDE mobiele HTML.

    Draait de canonieke `detect_booking` (met z'n G1-confidence-model: nooit
    'no_booking' bij een mislukte/dunne fetch) én een aanvullende scan op de
    verbrede platformlijst (Salonkee/Fresha/...). Een positieve platform-hit
    uit de verbrede lijst overschrijft een 'no_booking' van de canonieke
    detector — precies de false-negative die we dichttimmeren.
    """
    base = detect_booking(rendered_html, fetch_ok=fetch_ok)
    if base.get("value") == "has_booking":
        return base

    low = re.sub(r"\s+", " ", (rendered_html or "")).lower()
    for plat in _EXTRA_BOOKING_PLATFORMS:
        if plat in low:
            return {
                "value": "has_booking",
                "confidence": "high",
                "evidence": [f"platform_extra:{plat}"],
                "platform": plat.rstrip("."),
            }
    return base


async def _dismiss_cookie_banner(page) -> bool:
    """Klik een cookie-/consent-banner weg (best-effort). Returns True bij klik."""
    for label in _COOKIE_TEXT:
        try:
            btn = page.get_by_role("button", name=re.compile(re.escape(label), re.IGNORECASE)).first
            if await btn.count() and await btn.is_visible():
                await btn.click(timeout=1500)
                await page.wait_for_timeout(350)
                return True
        except Exception:
            continue
    # Fallback: veelvoorkomende consent-knop via tekst-locator.
    for label in ("Alles accepteren", "Accepteren", "Accept all", "Accepteer"):
        try:
            el = page.get_by_text(label, exact=False).first
            if await el.count() and await el.is_visible():
                await el.click(timeout=1500)
                await page.wait_for_timeout(350)
                return True
        except Exception:
            continue
    return False


async def _scroll_full(page) -> None:
    """Scroll de pagina volledig door (lazy-load) en terug naar boven."""
    try:
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(700)
        await page.evaluate("() => window.scrollTo(0, 0)")
        await page.wait_for_timeout(200)
    except Exception:
        pass


_ENTRY_SCAN_JS = """
() => {
  const norm = s => (s || '').toLowerCase().replace(/[-_]+/g, ' ')
                              .replace(/\\s+/g, ' ').trim();
  const els = Array.from(document.querySelectorAll('a, button')).slice(0, 600);
  return els.map(el => {
    const r = el.getBoundingClientRect();
    let visible = !!(r.width || r.height);
    try {
      const st = window.getComputedStyle(el);
      if (st.visibility === 'hidden' || st.display === 'none') visible = false;
    } catch (e) {}
    let inNav = false, p = el;
    while (p && p !== document.body) {
      const tag = (p.tagName || '').toLowerCase();
      let cls = '';
      try { cls = norm(typeof p.className === 'string' ? p.className : (p.className.baseVal || '')); } catch (e) {}
      const role = (p.getAttribute && p.getAttribute('role')) || '';
      if (tag === 'header' || tag === 'nav' || role === 'navigation' ||
          /(^| )(nav|navbar|menu|hamburger|header|topbar|offcanvas)( |$)/.test(cls)) {
        inNav = true; break;
      }
      p = p.parentElement;
    }
    return {
      text: norm(el.innerText || '').slice(0, 80),
      href: norm(el.getAttribute('href') || '').slice(0, 120),
      aria: norm(el.getAttribute('aria-label') || '').slice(0, 80),
      y: (r.width || r.height) ? Math.round((r.top + window.scrollY) * 10) / 10 : null,
      visible: visible,
      in_nav: inNav,
    };
  });
}
"""


def _entry_is_booking(e: dict) -> bool:
    """Heeft dit a/button-element afspraak-intentie? (canonieke definitie —
    generieke 'contact' telt bewust NIET, alleen boek-/afspraak-/consult-taal)."""
    blob_txt = f"{e.get('text') or ''} {e.get('aria') or ''}"
    href = e.get("href") or ""
    return (any(k in blob_txt for k in _BOOK_TEXT)
            or any(h in href for h in _BOOK_HREF_HINTS)
            or any(p in href for p in ALL_BOOKING_PLATFORMS))


# --- Signaal 1b: consult-only (request_form, geen self-booking) --------------
# Fragmenten die POSITIEF bewijzen dat je niet zelf kunt boeken, alleen
# aanvragen/terugbellen (Sami 2026-07-23). Aanwezig → geen zelf-boek-agenda.
_REQUEST_ONLY_RE = re.compile(
    r"bel (?:mij|me) terug|terugbel|wij bellen (?:u|je) terug|"
    r"(?:consult|afspraak|behandeling) aanvragen|vraag (?:een )?(?:consult|afspraak) aan|"
    r"request a callback|call (?:me|you) back",
    re.IGNORECASE,
)
# Interne form-paden (aanvraag/consult/terugbel/contact-formulier).
_FORM_PATH_RE = re.compile(
    r"/(?:consult|consultatie|afspraak-aanvra|aanvraag|terugbel|contact)", re.IGNORECASE)

# Weerlegbaarheids-check voor signaal 1b (Sami 2026-07-23): waar het consult
# EXPLICIET onderdeel van de propositie is (uitgebreid intakegesprek, behandelplan
# op maat, "na het consult ..."), is "je kunt geen moment kiezen" geen gemis maar
# beleid — de ontvanger weerlegt de claim in één zin. Dan vuurt 1b NIET.
# Amstelzijde-fixture: "Na het consult stellen we een persoonlijk behandelplan op."
_CONSULT_PROPOSITION_RE = re.compile(
    r"na (?:het|dit|uw|jouw|een) (?:uitgebreid[e]? |grondig[e]? |persoonlijk[e]? )?"
    r"(?:consult|intake|kennismaking|adviesgesprek)"
    r"|persoonlijk[e]? behandelplan|behandelplan op maat|behandelplan voor (?:je|jou|u)"
    r"|(?:uitgebreid|grondig)[e]? (?:intake|consult|intakegesprek|adviesgesprek|kennismaking)"
    r"|intakegesprek|adviesgesprek"
    r"|(?:begint|start(?:en)?|starten we) met een (?:uitgebreid |grondig |persoonlijk )?"
    r"(?:consult|intake)",
    re.IGNORECASE,
)


def _registrable_host(host: str | None) -> str:
    if not host:
        return ""
    h = str(host).lower().strip()
    h = re.sub(r"^https?://", "", h).split("/")[0].split(":")[0]
    if h.startswith("www."):
        h = h[4:]
    parts = h.split(".")
    return ".".join(parts[-2:]) if len(parts) > 2 else h


def _href_host(href: str | None) -> str:
    m = re.match(r"^https?://([^/]+)", str(href or ""), re.IGNORECASE)
    return m.group(1) if m else ""


def classify_booking_mechanism(booking: dict | None, entries: list[dict],
                               site_domain: str | None,
                               request_only_present: bool) -> str | None:
    """Bepaal HOE er geboekt kan worden: self_booking | request_form | ambiguous.

    - self_booking : zelf-boek-agenda (platform/iframe, of een EXTERNE boek-link).
    - request_form : uitsluitend aanvraag-/terugbelpad (geen slot-keuze) — bewezen
      via een request-only-frase óf een interne form-path-link.
    - ambiguous    : boeking aanwezig maar mechanisme niet vast te stellen
      (bv. een 'boek nu'-knop met href='#' die een onbekende widget kan openen).
      Signaal 1b vuurt dan NIET — bij twijfel geen onware claim.
    None → geen boeking.
    """
    if (booking or {}).get("value") != "has_booking":
        return None
    if booking.get("kind") == "self_booking":
        return "self_booking"
    site_reg = _registrable_host(site_domain)
    booking_entries = [e for e in entries if _entry_is_booking(e)]
    for e in booking_entries:
        host = _href_host(e.get("href"))
        if host and site_reg and _registrable_host(host) != site_reg:
            return "self_booking"  # externe boek-ingang = zelf-boek-systeem
    if request_only_present:
        return "request_form"
    for e in booking_entries:
        if _FORM_PATH_RE.search(e.get("href") or ""):
            return "request_form"
    return "ambiguous"


def evaluate_tap_targets(tap: dict | None) -> dict[str, Any]:
    """Beoordeel de tap-target-meting (signaal 4). Conservatief: vuurt alleen bij
    ECHTE overlap of veel te kleine knoppen dicht op elkaar — social-icoontjes
    (los, klein) mogen geen false positive geven."""
    tap = tap or {}
    count = tap.get("count") or 0
    small = tap.get("small") or 0
    overlaps = tap.get("overlaps") or 0
    # Twee onafhankelijke bewijzen, allebei streng: ECHTE overlap (≥4 paren) óf een
    # pagina die dicht vol staat met te kleine targets (≥10 én >helft, en genoeg
    # elementen om van "vol" te spreken). Een sparse iconen-menu (9/9 klein, 1
    # overlap) vuurt bewust niet (CadanCe, testrun 2026-07-23).
    fires = overlaps >= 4 or (small >= 10 and count >= 12 and small / count > 0.5)
    return {"fires": bool(fires), "count": count, "small": small, "overlaps": overlaps}


_TAP_SCAN_JS = """
() => {
  const limit = 844 * 2;
  const els = Array.from(document.querySelectorAll('a, button')).filter(el => {
    const r = el.getBoundingClientRect();
    let vis = r.width > 0 && r.height > 0;
    try { const st = getComputedStyle(el); if (st.visibility==='hidden'||st.display==='none') vis=false; } catch(e){}
    return vis && (r.top + window.scrollY) < limit;
  }).map(el => { const r = el.getBoundingClientRect();
    return {x: r.left, y: r.top + window.scrollY, w: r.width, h: r.height}; });
  let small = 0, overlaps = 0;
  for (const e of els) if (Math.min(e.w, e.h) < 40) small++;
  for (let i = 0; i < els.length; i++) for (let j = i + 1; j < els.length; j++) {
    const a = els[i], b = els[j];
    const ox = Math.min(a.x+a.w, b.x+b.w) - Math.max(a.x, b.x);
    const oy = Math.min(a.y+a.h, b.y+b.h) - Math.max(a.y, b.y);
    if (ox > 1 && oy > 1) overlaps++;
  }
  return {count: els.length, small: small, overlaps: overlaps};
}
"""

_FOOTER_SCAN_JS = """
() => {
  const foot = document.querySelector('footer') || document.body;
  const txt = (foot.innerText || '').slice(-2500);
  const ys = (txt.match(/20\\d{2}/g) || []).map(Number).filter(y => y >= 2005 && y <= 2100);
  return {max_year: ys.length ? Math.max(...ys) : null};
}
"""


def evaluate_footer_year(max_year: int | None, current_year: int) -> dict[str, Any]:
    """Signaal 5: footer-jaartal ≥3 jaar terug. Guardrail: alleen de footer, en
    het MEEST RECENTE jaartal telt (een oud jaartal in body-tekst mag niet
    vuren)."""
    if not max_year:
        return {"fires": False, "max_year": None}
    return {"fires": (current_year - int(max_year)) >= 3, "max_year": int(max_year)}


# ── Q-ladder (receptie): Q7 meting/tracking + P1 prijsanker ──────────────────
# Nieuwe meet-primitieven voor de receptie-haakje-ladder (Sami 2026-07-24). Zelfde
# stijl als de signaal-1-6-evaluatoren: PURE functies op AL-gescande data, met een
# expliciete DRIE-standen-uitslag in ladder-termen:
#   "hit"       — het criterium vuurt (Q7: de site meet niets / P1: geen prijsanker)
#   "geen"      — het criterium vuurt aantoonbaar NIET (er is meting / er is prijs)
#   "onbepaald" — niet betrouwbaar vast te stellen (consent-gated/JS-geladen
#                 tracking, prijs achter een interactie, mislukte render)
# "onbepaald" is GEEN negatief resultaat: een onmeetbaar criterium mag nooit als
# hit én nooit als geen-hit tellen (de C9-les: vier "hits", nul bevestigd, omdat
# de detector iets MOEST zeggen). Fail-closed: bij twijfel "onbepaald", nooit "hit".
TRISTATE = ("hit", "geen", "onbepaald")

# Analytics / tag-managers: laadt één hiervan, dan MEET de site wel → geen Q7-hit.
_GA_RE = re.compile(
    r"googletagmanager\.com/(?:gtm|gtag)|google-analytics\.com|/gtag/js|/g/collect|"
    r"analytics\.google\.com|\bgtag\s*\(|\bdataLayer\b|plausible\.io|cdn\.matomo|"
    r"matomo\.(?:php|js)|piwik|stats\.g\.doubleclick|clarity\.ms|(?:static\.)?hotjar|"
    r"mc\.yandex|segment\.(?:com|io)/analytics|cdn\.segment|"
    # verificatie-batch 2026-07-25: WordPress/Jetpack-stats + andere privacy-
    # analytics die _GA_RE miste → vals Q7-hit op WP-SMB-sites. Streng-boven-mis:
    # méér herkennen = strengere Q7-hit.
    r"stats\.wp\.com|pixel\.wp\.com|usefathom|simpleanalytics(?:cdn)?\.com|"
    r"/umami\.js|umami\.is|posthog|mixpanel|heapanalytics|mouseflow|luckyorange|crazyegg",
    re.IGNORECASE,
)
# Advertentie-/remarketing-pixels tellen óók als 'meten' (conversie/retargeting).
_PIXEL_RE = re.compile(
    r"connect\.facebook\.net/[^\"']*fbevents|facebook\.com/tr\b|\bfbq\s*\(|"
    r"snap\.licdn\.com|linkedin\.com/(?:px|li\.lms)|px\.ads\.linkedin|"
    r"analytics\.tiktok|googleadservices\.com|googlesyndication|bat\.bing\.com",
    re.IGNORECASE,
)
# Consent-managers: staat er één en klikten we niet aantoonbaar 'accepteren', dan
# kan tracking pas ná consent laden → afwezigheid is dan niet hard (F2) → onbepaald.
_CMP_MARKERS = (
    "cookiebot", "cookieyes", "cookiefirst", "cookie-script", "complianz",
    "onetrust", "cookiepro", "usercentrics", "borlabs", "iubenda", "termly",
    "quantcast-choice", "didomi", "klaro", "osano", "cookieconsent",
)


def evaluate_tracking(request_urls: list[str] | None, html: str | None, *,
                      consent_accepted: bool, fetch_ok: bool) -> dict[str, Any]:
    """Q7 — meet de site het bezoekersgedrag? Pure tri-state (zie TRISTATE).

    - "geen"      : GA/GTM/analytics/pixel geladen → er wórdt gemeten (geen hit).
    - "onbepaald" : render mislukt, óf een consent-manager staat er en we hebben
                    niet aantoonbaar geaccepteerd (tracking kan post-consent laden).
    - "hit"       : géén analytics én géén pixel client-side gezien → de site meet
                    niets. LIMIET: puur server-side/proxied tracking zonder enige
                    client-trace is per definitie onzichtbaar; de copy claimt dan
                    ook enkel wat client-side observeerbaar is.
    """
    if not fetch_ok:
        return {"state": "onbepaald", "evidence": ["fetch_failed"], "found": []}
    reqs = " ".join(request_urls or [])
    blob = reqs + " " + (html or "")
    found: list[str] = []
    if _GA_RE.search(blob):
        found.append("analytics")
    if _PIXEL_RE.search(blob):
        found.append("pixel")
    if found:
        return {"state": "geen", "evidence": [f"meet_via_{'+'.join(found)}"], "found": found}
    low = ((html or "") + " " + reqs).lower()
    cmp_hit = next((c for c in _CMP_MARKERS if c in low), None)
    if cmp_hit and not consent_accepted:
        return {"state": "onbepaald",
                "evidence": [f"consent_manager={cmp_hit}_niet_geaccepteerd"], "found": []}
    return {"state": "hit", "evidence": ["geen_analytics_geen_pixel_client_side"], "found": []}


# De prijs kan op een APARTE pagina staan (tarieven/diensten/behandeling), niet op
# de home — en het label kan Engels zijn ("Prices"/"Pricing"). De scan zoekt daarom
# over ALLE links (niet enkel header/nav; steekproef 2026-07-24: thebodyshaper had
# de tarieven-link buiten de nav) en levert een `service_link` op die de writer/
# meting rendert om óók dáár op een €-bedrag te controleren.
_PRICE_SCAN_JS = r"""
() => {
  const low = s => (s || '').toLowerCase();
  const bodyTxt = low(document.body ? document.body.innerText : '');
  const euro = /€\s?\d{2,}|\bvanaf\s?€|\d{2,}\s*euro\b|prijs vanaf/.test(bodyTxt);
  const links = [...document.querySelectorAll('a')];
  const hrefRe = /tarie|prijz|prijslijst|\/kosten|\/prices?\b|\/pricing|price-list/;
  const txtRe  = /tarief|tarieven|prijzen|prijslijst|\bprices?\b|pricing/;
  let price_page_link = false, service_link = null;
  for (const a of links) {
    const h = low(a.getAttribute('href') || ''), t = low(a.innerText || '');
    if (hrefRe.test(h) || txtRe.test(t)) price_page_link = true;
    if (!service_link && /\/(diensten|behandel|behandeling|services?|treatments?)/.test(h)
        && a.href && a.href.startsWith('http')) service_link = a.href;
  }
  const clickers = [...document.querySelectorAll('a,button,summary,[role=button]')];
  const teaser = clickers.some(el => /prijs|tarief|tarieven|kosten|prijslijst|price|pricing/.test(low(el.innerText || '')));
  return {euro_amount: euro, price_page_link: price_page_link,
          service_link: service_link, price_teaser_no_amount: teaser && !euro};
}
"""

# Raw-HTML-vangnet voor de prijzen-/tarieven-link: sommige sites strippen de link
# op mobiel uit de gerenderde DOM (JS-reflow), terwijl de GESERVEERDE HTML 'm wél
# heeft (steekproef thebodyshaper: /tarieven/ zat in de bron, niet in de render).
# Een prijs-pagina in de bron = de claim "geen prijsindicatie" is één-klik-
# weerlegbaar → de scanner OR't deze over de JS-uitslag heen (belt-and-braces,
# zelfde patroon als de booking-nav-sweep).
_PRICE_HREF_RE = re.compile(
    r"href=[\"'][^\"']*(?:tarieven|prijzen|prijslijst|/prijs|/kosten|/prices?\b|/pricing|price-list)[^\"']*",
    re.IGNORECASE,
)


def price_link_in_html(html: str | None) -> bool:
    """True als de (geserveerde) HTML naar een tarieven-/prijzen-/prices-pagina
    linkt. Vangnet voor links die JS uit de mobiele render weglaat."""
    return bool(_PRICE_HREF_RE.search(html or ""))


def booking_platform_in_html(html: str | None) -> bool:
    """True als de HTML naar een bekend boekplatform verwijst (self-booking).
    Vangnet voor platform-links die JS uit de mobiele render strippt (analoog aan
    price_link_in_html) — Q4-steekproef 2026-07-24: doctorsatsoap (clinicminds) en
    dcklinieken (zorgdomein) hadden het platform in de bron, niet in de render →
    vals Q4/Q2-hit. Fail-closed: een platform in de bron laat Q4/Q2 vervallen."""
    low = (html or "").lower()
    return any(p.lower() in low for p in ALL_BOOKING_PLATFORMS)


def evaluate_price_anchor(price: dict | None, *, fetch_ok: bool) -> dict[str, Any]:
    """P1 — geeft de site een prijsindicatie? Pure tri-state (zie TRISTATE).

    Verwacht gecombineerde signalen over de home ÉN (indien aanwezig) de
    diensten-/behandelpagina:
    - "geen"      : een €-bedrag ergens gezien, óf een tarieven-/prijzen-/prices-
                    pagina gelinkt (waar dan ook) → er IS een prijsanker.
    - "onbepaald" : er is een diensten-/behandelpagina die we NIET op prijs konden
                    controleren (`service_unchecked`), óf een prijs-/tarief-element
                    zonder zichtbaar bedrag → prijs mogelijk elders/achter een klik
                    → nooit als hit (fail-closed, streng-boven-mis; steekproef-les).
    - "hit"       : geen bedrag, geen prijzen-/tarieven-link, en geen ongecontro-
                    leerde prijs-pagina → geen prijsanker.
    """
    if not fetch_ok:
        return {"state": "onbepaald", "evidence": ["fetch_failed"]}
    p = price or {}
    ev: list[str] = []
    if p.get("euro_amount"):
        ev.append("euro_bedrag_op_pagina")
    if p.get("price_page_link"):
        ev.append("tarieven_of_prijzen_pagina_gelinkt")
    if ev:
        return {"state": "geen", "evidence": ev}
    if p.get("service_unchecked"):
        return {"state": "onbepaald", "evidence": ["diensten_behandelpagina_niet_op_prijs_gecontroleerd"]}
    if p.get("price_teaser_no_amount"):
        return {"state": "onbepaald", "evidence": ["prijs_element_zonder_bedrag_mogelijk_achter_interactie"]}
    return {"state": "hit", "evidence": ["geen_bedrag_geen_prijzenpagina"]}


def combine_stable(state_a: str | None, state_b: str | None) -> str:
    """2-run-stabiliteit: alleen een IDENTIEKE 'hit'/'geen' over BEIDE runs telt;
    elke afwijking tussen de runs of een 'onbepaald' → 'onbepaald' (fail-closed).
    Dit is de "hits identiek in beide runs"-eis uit de klaar-definitie."""
    if state_a == state_b and state_a in ("hit", "geen"):
        return state_a
    return "onbepaald"


# Async-/24-7-kanalen voor Q4 ("geen dekking buiten kantooruren"). Herkende
# chat-platformen, WhatsApp-deeplinks/-widgets en Messenger. Elk gevonden kanaal
# laat Q4 vervallen (dan IS er dekking) → Q4-hit vraagt de afwezigheid van álle.
_CHAT_PLATFORM_RE = re.compile(
    r"intercom|drift\.com|js\.driftt|tidio|landbot|trengo|livechat(?:inc)?|zopim|"
    r"zdassets|zendesk[^\"']*(?:chat|widget|messaging)|crisp\.chat|tawk\.to|smartsupp|"
    r"userlike|olark|chatwoot|gorgias(?:chat)?|formilla|chaport|reamaze|whatshelp|"
    r"hubspot[^\"']*(?:messages|conversations)|leadinfo|watermelon|obi4wan",
    re.IGNORECASE,
)
# WhatsApp: deeplinks (tel-gebaseerd), click-to-chat en widget-scripts (F7-harding).
_WA_RE = re.compile(
    r"wa\.me/|api\.whatsapp\.com|chat\.whatsapp\.com|whatsapp://|\.whatsapp\.com/send|"
    r"href=[\"'][^\"']*whatsapp|whatsapp[-_]?(?:widget|chat|button|me)|data-whatsapp",
    re.IGNORECASE,
)
# Facebook Messenger: m.me-deeplink of de customer-chat-plugin.
_MESSENGER_RE = re.compile(
    r"m\.me/|facebook\.com/messages|fb-customerchat|customerchat|"
    r"facebook[^\"']*customer.?chat|messenger[^\"']*plugin",
    re.IGNORECASE,
)
# Chat-marker ZONDER herkend platform: knop/tekst die op live-chat wijst maar
# waarvan we het platform niet zien → de widget laadt mogelijk pas na interactie
# → niet hard vast te stellen → onbepaald (Sami-regel), nooit een Q4-hit.
_AMBIG_CHAT_RE = re.compile(
    r"live[- ]?chat|chat met ons|chat nu|start(?:en)? (?:een )?chat|open(?: de)? chat|"
    r"chat openen|stel je vraag via (?:de )?chat|chatten met",
    re.IGNORECASE,
)


def evaluate_coverage(*, chat_platform: bool, wa: bool, messenger: bool,
                      self_booking: bool, ambiguous_chat: bool, fetch_ok: bool) -> dict[str, Any]:
    """Q4 — kan een bezoeker buiten kantooruren iets vastleggen of contact leggen?
    Pure tri-state (zie TRISTATE). Async-/24-7-kanaal = chat-widget, WhatsApp,
    Messenger of zelf-boeken.

    - "geen"      : minstens één async-kanaal gevonden → er IS dekking (geen hit).
    - "onbepaald" : render mislukt, óf een chat-marker zonder herkend platform
                    (laadt mogelijk pas na interactie) → niet hard → nooit hit.
    - "hit"       : geen chat/WhatsApp/Messenger/zelf-boeken → buiten kantooruren
                    kan een bezoeker niets vastleggen (de 22:47-lek).
    """
    if not fetch_ok:
        return {"state": "onbepaald", "evidence": ["fetch_failed"]}
    have: list[str] = []
    if self_booking:
        have.append("self_booking")
    if chat_platform:
        have.append("chat")
    if wa:
        have.append("whatsapp")
    if messenger:
        have.append("messenger")
    if have:
        return {"state": "geen", "evidence": ["kanaal_" + "+".join(have)]}
    if ambiguous_chat:
        return {"state": "onbepaald",
                "evidence": ["chat_marker_zonder_herkend_platform_mogelijk_na_interactie"]}
    return {"state": "hit", "evidence": ["geen_chat_wa_messenger_zelfboeken"]}


# Receptie-ladder-volgorde (Sami 2026-07-24): eerste die vuurt wint de mail-1-haak.
_RECEPTIE_LADDER = ("Q4", "Q7", "Q2", "P1")
# Thematische groepen — mail 2 wil een haak uit een ÁNDER thema (niet Q4 én Q2,
# want dat is twee keer 'boeken'); zie decide_receptie_hook / mail-2-selectie.
_HOOK_THEME = {"Q4": "boeken", "Q2": "boeken", "Q7": "meten", "P1": "prijs"}


def decide_receptie_hook(q4: str, q7: str, q2: str, p1: str, *,
                         form_present: bool) -> dict[str, Any]:
    """Wijs de mail-1-haak toe uit de vier tri-state-uitslagen via de receptie-
    ladder Q4→Q7→Q2→P1. Alleen 'hit' telt als gevuurd ('geen'/'onbepaald' niet).

    Q4 als mail-1-haak vereist een formulier (de copy claimt 'alleen een formulier
    achterlaten'); zonder formulier → `q4_gated`, Q4 valt door naar de volgende
    rung (invariant-2-les). Q4 ⊆ Q2 geldt, dus als Q4 vuurt vuurt Q2 ook.

    Returns:
      hook_code    — mail-1-winnaar (of None → lead niet mailbaar)
      hook_ladder  — ALLE gevuurde haken, in ladder-volgorde (voor mail-2-keuze)
      second_hook  — eerstvolgende gevuurde haak uit een ÁNDER thema dan hook_code
                     (of None) — dit is de enige geldige mail-2-haak
      q4_gated     — Q4 vuurde maar viel door bij gebrek aan formulier
    """
    fired: list[str] = []
    q4_gated = False
    if q4 == "hit":
        if form_present:
            fired.append("Q4")
        else:
            q4_gated = True
    if q7 == "hit":
        fired.append("Q7")
    if q2 == "hit":
        fired.append("Q2")
    if p1 == "hit":
        fired.append("P1")
    fired.sort(key=lambda k: _RECEPTIE_LADDER.index(k))
    hook_code = fired[0] if fired else None
    second_hook = None
    if hook_code is not None:
        theme = _HOOK_THEME[hook_code]
        second_hook = next((k for k in fired[1:] if _HOOK_THEME[k] != theme), None)
    return {"hook_code": hook_code, "hook_ladder": fired,
            "second_hook": second_hook, "q4_gated": q4_gated}


def q4_form_available(dom_form: bool, mechanism: str | None) -> bool:
    """Voor de Q4-form-gate: er is een formulier-pad als er een inline <form> is
    OF het mechanisme een aanvraag-/terugbelformulier is (request_form). Zonder
    één van beide is Q4's copy-claim 'alleen een formulier achterlaten' onwaar en
    moet Q4 gaten. skin.nl-regressie 2026-07-24: mechanism=request_form zonder
    inline <form> is wél een formulier → Q4 mag niet naar Q2 weggedrukt worden."""
    return bool(dom_form) or mechanism == "request_form"


def evaluate_selfbooking(mechanism: str | None, *, form_present: bool,
                         fetch_ok: bool) -> dict[str, Any]:
    """Q2 — 'alleen een aanvraag, geen moment zelf vastleggen'. Canoniek op het
    booking_detector-mechanisme (audit F3: niet op een losse platform-namenlijst).

    - "geen"      : self_booking → de bezoeker kan wél zelf een moment kiezen.
    - "hit"       : request_form (aanvraag-/terugbelformulier), óf geen boeking maar
                    wél een formulier → 'alleen een aanvraag' klopt.
    - "onbepaald" : ambigu mechanisme, óf geen boeking én geen formulier (dan houdt
                    de 'alleen een aanvraag'-claim geen stand) → nooit hit.
    """
    if not fetch_ok:
        return {"state": "onbepaald", "evidence": ["fetch_failed"]}
    if mechanism == "self_booking":
        return {"state": "geen", "evidence": ["self_booking"]}
    if mechanism == "request_form":
        return {"state": "hit", "evidence": ["alleen_aanvraagformulier"]}
    if mechanism == "ambiguous":
        return {"state": "onbepaald", "evidence": ["booking_mechanisme_ambigu"]}
    # geen herkend boekmechanisme: 'alleen aanvraag' vraagt minstens een formulier.
    if form_present:
        return {"state": "hit", "evidence": ["formulier_geen_zelfboeken"]}
    return {"state": "onbepaald", "evidence": ["geen_boeking_geen_formulier"]}


# Formulier op de pagina (voor de Q4/Q2-copy die 'een formulier' claimt).
_FORM_PRESENT_JS = """
() => {
  for (const f of document.querySelectorAll('form')) {
    const n = [...f.querySelectorAll('input,textarea,select')].filter(i => i.type !== 'hidden').length;
    if (n >= 1) return true;
  }
  // losse velden buiten een <form> (sommige site-builders)
  return document.querySelectorAll('input[type=email], textarea').length >= 1;
}
"""


def evaluate_booking_entries(raw_entries: list[dict], *, fold: int = FOLD_PX,
                             min_y_sig2: int | None = None) -> dict[str, Any]:
    """PURE evaluatie van de gescande a/button-entries → boekingang-oordeel.

    De vraag van de weerleg-pas (Sami, 2026-07-22): "staat er ergens boven de
    vouw, header/nav inbegrepen, een manier om te boeken?" — en dan telt óók een
    verborgen menu-item (hamburger): de bezoeker tikt het menu open en boekt
    zonder te scrollen. Live bewezen op Fairday (header-'boek nu') en CadanCe
    ('afspraak maken' in het hoofdmenu): de widget-diepte in de body zei
    "moeilijk te vinden", de nav bewees het tegendeel.

    Returns:
      found          — er bestaat minstens één boek-achtig element
      nav_booking    — boekingang in header/nav/menu (zichtbaar of niet)
      hidden_booking — boek-element zonder meetbare positie buiten de nav
                       (overlay/collapsed menu → bereikbaar zonder scrollen)
      above_fold     — zichtbaar boek-element boven de vouw
      min_visible_y  — hoogste zichtbare boekingang (px)
      sig2_allowed   — signaal 2 mag alleen vuren als dit True is
      sample         — bewijs-samples (max 4) voor de uitlezing
    """
    if min_y_sig2 is None:
        min_y_sig2 = SIGNAL2_MIN_Y

    hits = [e for e in raw_entries if _entry_is_booking(e)]
    nav_booking = any(e.get("in_nav") for e in hits)
    hidden_booking = any(not e.get("in_nav") and (e.get("y") is None or not e.get("visible"))
                         for e in hits)
    vis_ys = [e["y"] for e in hits if e.get("visible") and e.get("y") is not None]
    min_visible_y = min(vis_ys) if vis_ys else None
    above_fold = (min_visible_y is not None and min_visible_y < fold) or nav_booking
    sig2_allowed = (bool(hits) and not nav_booking and not hidden_booking
                    and min_visible_y is not None and min_visible_y >= min_y_sig2)
    sample = [{"text": (e.get("text") or e.get("href") or "")[:60], "y": e.get("y"),
               "visible": e.get("visible"), "in_nav": e.get("in_nav")} for e in hits[:4]]
    return {"found": bool(hits), "nav_booking": nav_booking,
            "hidden_booking": hidden_booking, "above_fold": above_fold,
            "min_visible_y": min_visible_y, "y": min_visible_y,
            "sig2_allowed": sig2_allowed, "sample": sample}


async def _scan_booking_entries(page) -> dict[str, Any]:
    """Scan ALLE a/button-elementen (zichtbaar én verborgen, incl. header/nav)
    en beoordeel de boekingangen. Belt-and-braces: naast de JS-pass ook een
    locator-sweep door header/nav (die pierct open shadow DOM)."""
    empty = {"found": False, "nav_booking": False, "hidden_booking": False,
             "above_fold": None, "min_visible_y": None, "y": None,
             "sig2_allowed": False, "sample": [], "entries": []}
    try:
        raw = await page.evaluate(_ENTRY_SCAN_JS)
    except Exception:
        return empty
    result = evaluate_booking_entries(raw or [])
    result["entries"] = raw or []  # ruwe entries voor de mechanisme-classificatie
    if not result["nav_booking"]:
        # shadow-DOM-vangnet: locators piercen open shadow roots, de JS-pass niet.
        try:
            nav_els = page.locator("header, nav, [role='navigation']").locator("a, button")
            n = min(await nav_els.count(), 120)
            for i in range(n):
                el = nav_els.nth(i)
                try:
                    txt = ((await el.inner_text(timeout=150)) or "").lower().replace("-", " ")
                    href = ((await el.get_attribute("href")) or "").lower()
                except Exception:
                    continue
                if (any(k in txt for k in _BOOK_TEXT)
                        or any(h in href for h in _BOOK_HREF_HINTS)
                        or any(p in href for p in ALL_BOOKING_PLATFORMS)):
                    result["nav_booking"] = True
                    result["above_fold"] = True
                    result["sig2_allowed"] = False
                    result["sample"].append({"text": txt[:60], "y": None,
                                             "visible": None, "in_nav": True})
                    break
        except Exception:
            pass
    return result


async def _measure_timing(page, wall_load_ms: int | None) -> dict[str, Any]:
    """Rough TTI-proxy uit de Navigation Timing API (geen 4G-throttle — package B).

    domInteractive t.o.v. navigationStart benadert 'time-to-interactive'. Zonder
    throttle is dit een ONDERGRENS van de echte mobiele TTI; daarom rough en niet
    als enige bewijs voor een send te gebruiken.
    """
    dom_interactive_ms: int | None = None
    fcp_ms: int | None = None
    try:
        val = await page.evaluate(
            """() => {
                const out = {di: null, fcp: null};
                const n = performance.getEntriesByType('navigation')[0];
                if (n && n.domInteractive) out.di = Math.round(n.domInteractive);
                else { const t = performance.timing;
                  if (t && t.domInteractive && t.navigationStart) out.di = t.domInteractive - t.navigationStart; }
                const fcp = performance.getEntriesByName('first-contentful-paint')[0];
                if (fcp) out.fcp = Math.round(fcp.startTime);
                return out;
            }"""
        )
        if isinstance(val, dict):
            if isinstance(val.get("di"), (int, float)) and val["di"] >= 0:
                dom_interactive_ms = int(val["di"])
            if isinstance(val.get("fcp"), (int, float)) and val["fcp"] >= 0:
                fcp_ms = int(val["fcp"])
    except Exception:
        pass
    return {"load_ms": wall_load_ms, "dom_interactive_ms": dom_interactive_ms,
            "fcp_ms": fcp_ms}


# Trage-render-drempel (signaal 3). Ongethrotteld → conservatieve ondergrens:
# een site die er ONgethrottled al >4s over doet, is op 4G aantoonbaar traag.
SLOW_MS = 4000


def _decide_signal(
    *,
    fetch_ok: bool,
    booking: dict[str, Any],
    tel_present: bool,
    cta: dict[str, Any],
    mechanism: str | None = None,
    consult_proposition: bool = False,
    dom_interactive_ms: int | None = None,
    fcp_ms: int | None = None,
    tap: dict | None = None,
    footer: dict | None = None,
    vision: dict | None = None,
) -> dict[str, Any]:
    """Prioriteitsladder (eerste dat vuurt wint), Guardrail 1 hard ingebouwd.

    Volgorde (docs/haakje_mapping_mail1.md): 1 geen online boeking → 1b consult-
    only → 2 boeking diep → 3 traag → 4 tap-targets → 5 verouderde footer →
    6 vision. `needs_review` = mens-in-de-lus vóór send (signaal 1b + 6)."""
    evidence: list[str] = []
    if not fetch_ok:
        return {"fired_signal": None, "signal_name": None, "confidence": "low",
                "needs_review": False, "evidence": ["fetch_failed_or_partial"]}

    bval = booking.get("value")
    bconf = booking.get("confidence")
    real_online_booking = (bval == "has_booking" and bconf == "high")

    # Signaal 1 — geen online boeking, wél een telefoonnummer. UITSLUITEND bij
    # een geverifieerde HIGH-confidence 'no_booking' (testrun 2026-07-22: de
    # low-conf-route gaf 4/4 false-positives).
    if bval == "no_booking" and bconf == "high" and tel_present:
        return {"fired_signal": 1, "signal_name": "no_online_booking",
                "confidence": "high", "needs_review": False,
                "evidence": ["no_online_booking", "tel_present"] + (booking.get("evidence") or [])}

    # Signaal 1b — er is WEL een online ingang, maar alléén een aanvraag-/terugbel-
    # formulier: de bezoeker kan geen moment kiezen. Alleen bij een BEWEZEN
    # request_form-mechanisme (geen self-booking); bij 'ambiguous' niet vuren.
    # Weerlegbaarheids-check: als het consult expliciet propositie is (behandelplan
    # op maat, "na het consult ..."), dan is dit geen gemis maar beleid → NIET
    # vuren (Sami 2026-07-23; Amstelzijde-fixture). Gaat anders door mens-review.
    if mechanism == "request_form" and consult_proposition:
        evidence.append("1b_suppressed_consult_is_proposition")
    elif mechanism == "request_form":
        return {"fired_signal": "1b", "signal_name": "consult_only",
                "confidence": "high" if bconf == "high" else "low", "needs_review": True,
                "evidence": ["request_form_only", f"booking_kind={booking.get('kind')}"] + (booking.get("evidence") or [])}

    # Signaal 2 — echte zelf-boek-agenda, maar niet bereikbaar zonder scrollen
    # (nav/header/menu schoon én ≥2 schermen diep). Alleen bij self_booking.
    if real_online_booking and mechanism == "self_booking" and cta.get("sig2_allowed"):
        return {"fired_signal": 2, "signal_name": "cta_below_fold",
                "confidence": "high", "needs_review": False,
                "evidence": ["booking_cta_below_fold", f"cta_y={cta.get('y')}>={SIGNAL2_MIN_Y}",
                             "nav_check=schoon"] + (booking.get("evidence") or [])}
    if real_online_booking and cta.get("nav_booking"):
        evidence.append("nav_booking_present")
    if mechanism == "ambiguous":
        evidence.append("booking_mechanism_ambiguous")

    # Signaal 3 — trage eerste render (FCP, of domInteractive als fallback) >4s.
    slow_ms = fcp_ms if fcp_ms is not None else dom_interactive_ms
    if slow_ms is not None and slow_ms > SLOW_MS:
        metric = "fcp_ms" if fcp_ms is not None else "dom_interactive_ms"
        return {"fired_signal": 3, "signal_name": "slow_tti", "confidence": "low",
                "needs_review": False, "evidence": [f"{metric}={slow_ms}>{SLOW_MS}_ongethrottled"]}

    # Signaal 4 — tap-targets te klein/overlappend op mobiel.
    tap_eval = evaluate_tap_targets(tap)
    if tap_eval["fires"]:
        return {"fired_signal": 4, "signal_name": "tap_targets", "confidence": "low",
                "needs_review": False,
                "evidence": [f"overlaps={tap_eval['overlaps']}", f"small={tap_eval['small']}/{tap_eval['count']}"]}

    # Signaal 5 — verouderde footer (jaartal ≥3 jaar terug).
    if footer and footer.get("fires"):
        return {"fired_signal": 5, "signal_name": "stale_footer", "confidence": "high",
                "needs_review": False, "evidence": [f"footer_jaar={footer.get('max_year')}"],
                "jaar": footer.get("max_year")}

    # Signaal 6 — vision-vangnet (Sonnet), alleen als 1-5 niks gaven en de
    # score ≥60 (Guardrail 2). Mens-review vóór send (Guardrail 3).
    if vision and vision.get("fires"):
        return {"fired_signal": 6, "signal_name": "vision_element", "confidence": "high",
                "needs_review": True, "evidence": [f"vision_score={vision.get('score')}"],
                "vision_element": vision.get("element")}

    return {"fired_signal": None, "signal_name": None, "confidence": "low",
            "needs_review": False, "evidence": evidence + ["site_ok_no_signal"]}


_VISION_SCORE_MIN = 60  # Guardrail 2: onder 60 telt vision als GEEN.


async def _run_vision(page, anthropic_client) -> dict[str, Any]:
    """Signaal 6: één benoembaar gedateerd element via Claude Sonnet Vision.

    Vangnet, alleen als 1-5 niks gaven. Guardrail 2: score <60 → fires=False.
    Guardrail 3 (mens-review vóór send) zit in _decide_signal (needs_review).
    Nooit-raise; bij elke fout → fires=False.
    """
    from config.hook_templates import VISION_PROMPT
    out = {"fires": False, "element": None, "score": None, "error": None,
           "rejected": None, "challenge_or_dead": False}
    try:
        # Cookie-banner nog eens wegklikken + naar boven: anders fotografeert
        # vision de overlay i.p.v. de site (testrun 2026-07-23).
        await _dismiss_cookie_banner(page)
        try:
            await page.evaluate("() => window.scrollTo(0, 0)")
        except Exception:
            pass
        png = await page.screenshot(type="jpeg", quality=70, full_page=False)
        import base64
        b64 = base64.standard_b64encode(png).decode("ascii")
        msg = await anthropic_client.messages.create(
            model="claude-sonnet-4-6", max_tokens=120, temperature=0,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}},
                {"type": "text", "text": VISION_PROMPT},
            ]}],
        )
        text = "".join(getattr(b, "text", "") for b in msg.content)
        el_m = re.search(r"ELEMENT:\s*(.+)", text, re.IGNORECASE)
        sc_m = re.search(r"SCORE:\s*(\d{1,3})", text, re.IGNORECASE)
        element = (el_m.group(1).strip() if el_m else "").strip()
        score = int(sc_m.group(1)) if sc_m else None
        out["element"] = element
        out["score"] = score
        if element and _BAD_VISION_RE.search(element):
            # Vision beschrijft een overlay/foutpagina/cookie/challenge/parking —
            # geen design-signaal. Niet vuren; markeer of het op dood/geblokkeerd
            # wijst (dan is de lead sowieso onbruikbaar).
            out["rejected"] = element
            if re.search(r"cloudflare|captcha|beveilig|te koop|for sale|geparkeerd|parked|404|onderhoud|maintenance", element, re.IGNORECASE):
                out["challenge_or_dead"] = True
        elif element and element.upper() != "GEEN" and score is not None and score >= _VISION_SCORE_MIN:
            out["fires"] = True
    except Exception as e:
        out["error"] = str(e)[:160]
    return out


async def detect_hook_on_page(page, wall_load_ms: int | None = None, *,
                              domain: str = "", anthropic_client: Any | None = None,
                              current_year: int | None = None) -> dict[str, Any]:
    """Voer Guardrail-1-flow + de volle signaal-ladder uit op een AL genavigeerde
    mobiele page. Nooit-raise.

    `current_year` moet door de caller worden meegegeven (Playwright draait in de
    async-loop; we vermijden hier impliciete tijd). `anthropic_client` activeert
    signaal 6 (vision) als de rest niks gaf.
    """
    result: dict[str, Any] = {
        "fetch_ok": False, "verifiable": True, "fired_signal": None,
        "signal_name": None, "confidence": "low", "needs_review": False,
        "booking": None, "mechanism": None, "consult_proposition": False,
        "size": None, "tel_present": False,
        "cta": {"found": False, "nav_booking": False, "hidden_booking": False,
                "above_fold": None, "min_visible_y": None, "y": None,
                "sig2_allowed": False, "sample": []},
        "load_ms": wall_load_ms, "dom_interactive_ms": None, "fcp_ms": None,
        "tap": None, "footer": None, "vision": None,
        "evidence": [], "error": None,
    }
    try:
        await _dismiss_cookie_banner(page)
        await _scroll_full(page)
        try:
            await page.wait_for_load_state("networkidle", timeout=6000)
        except Exception:
            pass

        html = await page.content()
        fetch_ok = bool(html) and len(html) >= 500
        # Challenge-/blokkadepagina (Cloudflare e.d.) = geen echte render → de
        # site is machinaal NIET verifieerbaar en mag nooit als stuurbaar gelden:
        # verplicht handmatige telefoon-check (Sami-regel 2026-07-22).
        low_head = (html or "")[:6000].lower()
        if any(m in low_head for m in _CHALLENGE_MARKERS):
            result["fetch_ok"] = False
            result["verifiable"] = False
            result["evidence"] = ["challenge_page_not_machine_verifiable"]
            return result
        if any(m in (html or "").lower() for m in _PARKING_MARKERS):
            result["fetch_ok"] = False
            result["verifiable"] = False
            result["dead_site"] = True
            result["evidence"] = ["parked_or_for_sale_domain"]
            return result
        result["fetch_ok"] = fetch_ok

        booking = _combine_booking(html, fetch_ok=fetch_ok)
        result["booking"] = booking

        try:
            result["tel_present"] = await page.locator("a[href^='tel:']").count() > 0
        except Exception:
            result["tel_present"] = False

        # Boekingang-scan + mechanisme (self_booking / request_form / ambiguous).
        entries: list[dict] = []
        if booking.get("value") == "has_booking":
            result["cta"] = await _scan_booking_entries(page)
            entries = result["cta"].get("entries") or []
        page_text = re.sub(r"<[^>]+>", " ", html or "").lower()
        request_only = bool(_REQUEST_ONLY_RE.search(page_text))
        result["mechanism"] = classify_booking_mechanism(booking, entries, domain, request_only)
        result["consult_proposition"] = bool(_CONSULT_PROPOSITION_RE.search(page_text))
        from website_intelligence.practice_type import assess_practice_size
        result["size"] = assess_practice_size(page_text, current_year=current_year or 2026)

        timing = await _measure_timing(page, wall_load_ms)
        result["load_ms"] = timing["load_ms"]
        result["dom_interactive_ms"] = timing["dom_interactive_ms"]
        result["fcp_ms"] = timing["fcp_ms"]

        # Signaal 4 (tap-targets) + 5 (footer-jaar) — goedkope DOM-metingen.
        try:
            result["tap"] = await page.evaluate(_TAP_SCAN_JS)
        except Exception:
            result["tap"] = None
        try:
            foot_raw = await page.evaluate(_FOOTER_SCAN_JS)
            cy = current_year or 2026
            result["footer"] = evaluate_footer_year((foot_raw or {}).get("max_year"), cy)
        except Exception:
            result["footer"] = None

        decision = _decide_signal(
            fetch_ok=fetch_ok, booking=booking, tel_present=result["tel_present"],
            cta=result["cta"], mechanism=result["mechanism"],
            consult_proposition=result["consult_proposition"],
            dom_interactive_ms=result["dom_interactive_ms"], fcp_ms=result["fcp_ms"],
            tap=result["tap"], footer=result["footer"], vision=None,
        )

        # Signaal 6 (vision) — vangnet: alleen als 1-5 niks gaven én er een
        # anthropic-client is. Duur → pas hier, niet standaard.
        if decision.get("fired_signal") is None and anthropic_client is not None:
            vision = await _run_vision(page, anthropic_client)
            result["vision"] = vision
            if vision and vision.get("challenge_or_dead"):
                # Vision-backstop: HTML-markers misten de challenge/parking, de
                # screenshot niet. Lead is machinaal niet verifieerbaar.
                result["verifiable"] = False
                result["evidence"] = result.get("evidence", []) + ["vision_challenge_or_dead"]
            if vision and vision.get("fires"):
                decision = _decide_signal(
                    fetch_ok=fetch_ok, booking=booking, tel_present=result["tel_present"],
                    cta=result["cta"], mechanism=result["mechanism"],
                    consult_proposition=result["consult_proposition"],
                    dom_interactive_ms=result["dom_interactive_ms"], fcp_ms=result["fcp_ms"],
                    tap=result["tap"], footer=result["footer"], vision=vision,
                )
        result.update(decision)
    except Exception as e:
        result["error"] = str(e)[:200]
        result["fired_signal"] = None
        logger.warning("hook_detector: detectie faalde: %s", e)
    return result


async def _goto_with_fallback(page, domain: str, timeout_ms: int) -> int | None:
    """Navigeer https→http met fallback; return wall-clock laadtijd in ms of None."""
    clean = domain.strip()
    clean = re.sub(r"^https?://", "", clean, flags=re.IGNORECASE).strip("/")
    t0 = time.monotonic()
    for scheme in ("https://", "http://"):
        try:
            await page.goto(scheme + clean, wait_until="domcontentloaded", timeout=timeout_ms)
            return int((time.monotonic() - t0) * 1000)
        except Exception:
            continue
    return None


async def detect_hook(
    domain: str,
    playwright: Any | None = None,
    *,
    browser: Any | None = None,
    timeout_ms: int = 20000,
    anthropic_client: Any | None = None,
    current_year: int | None = None,
) -> dict[str, Any]:
    """Self-contained haakje-detectie voor één domein op een mobiele render.

    Beheert desgewenst een eigen Playwright/browser. Voor een batch geef je een
    gedeelde `playwright` + `browser` mee (scheelt opstartkosten per site); dan
    maakt deze functie alleen een verse mobiele context per site.

    Returns het `detect_hook_on_page`-resultaat, aangevuld met `domain`.
    Nooit-raise: bij navigatiefout → fetch_ok=False, fired_signal=None, error.
    """
    own_pw = playwright is None
    own_browser = browser is None
    pw_cm = None
    ctx = None
    result: dict[str, Any]
    try:
        if own_pw:
            pw_cm = async_playwright()
            playwright = await pw_cm.__aenter__()
        if own_browser:
            browser, _base_ctx = await new_browser_context(playwright)
            # We gebruiken alleen de browser; de meegeleverde desktop-context niet.
        ctx = await mobile_context(browser, playwright)
        page = await ctx.new_page()
        wall = await _goto_with_fallback(page, domain, timeout_ms)
        if wall is None:
            result = {
                "domain": domain, "fetch_ok": False, "verifiable": False,
                "fired_signal": None, "signal_name": None, "confidence": "low",
                "booking": None, "tel_present": False,
                "cta": {"found": False, "nav_booking": False, "hidden_booking": False,
                        "above_fold": None, "min_visible_y": None, "y": None,
                        "sig2_allowed": False, "sample": []},
                "load_ms": None, "dom_interactive_ms": None,
                "evidence": ["navigation_failed"], "error": "navigation_failed",
            }
        else:
            detected = await detect_hook_on_page(
                page, wall_load_ms=wall, domain=domain,
                anthropic_client=anthropic_client, current_year=current_year)
            detected["domain"] = domain
            result = detected
    except Exception as e:
        result = {
            "domain": domain, "fetch_ok": False, "verifiable": False,
            "fired_signal": None, "signal_name": None, "confidence": "low",
            "booking": None, "tel_present": False,
            "cta": {"found": False, "nav_booking": False, "hidden_booking": False,
                    "above_fold": None, "min_visible_y": None, "y": None,
                    "sig2_allowed": False, "sample": []},
            "load_ms": None, "dom_interactive_ms": None,
            "evidence": ["exception"], "error": str(e)[:200],
        }
        logger.warning("hook_detector: %s faalde: %s", domain, e)
    finally:
        try:
            if ctx is not None:
                await ctx.close()
        except Exception:
            pass
        try:
            if own_browser and browser is not None:
                await browser.close()
        except Exception:
            pass
        try:
            if own_pw and pw_cm is not None:
                await pw_cm.__aexit__(None, None, None)
        except Exception:
            pass
    return result


# ── Receptie-detectie: de volledige Q4/Q7/Q2/P1-ladder over 2 renders ─────────
# Dit is de kern van de writer (Fase 2c, 2026-07-24): één mobiele render levert
# alle vier de tri-states + form_present; detect_receptie draait 'm 2x en
# combineert fail-closed (combine_stable) per as, dan decide_receptie_hook.

async def _receptie_one_render(page, *, served_html: str, request_urls: list[str],
                               consent_accepted: bool, domain: str = "") -> dict[str, Any]:
    """Eén AL-genavigeerde render → per-run tri-states voor Q4/Q7/Q2/P1 +
    form_present. Nooit-raise (caller vangt af)."""
    html = await page.content()
    fetch_ok = bool(html) and len(html) >= 500
    low = (html or "").lower()
    if any(m in low[:6000] for m in _CHALLENGE_MARKERS) or any(m in low for m in _PARKING_MARKERS):
        fetch_ok = False
    reqs = request_urls or []
    blob = " ".join(reqs) + " " + (html or "")

    q7 = evaluate_tracking(reqs, html, consent_accepted=consent_accepted, fetch_ok=fetch_ok)

    booking = _combine_booking(html, fetch_ok=fetch_ok)
    entries: list[dict] = []
    if booking.get("value") == "has_booking":
        try:
            cta = await _scan_booking_entries(page)
            entries = cta.get("entries") or []
        except Exception:
            entries = []
    page_text = re.sub(r"<[^>]+>", " ", html or "").lower()
    request_only = bool(_REQUEST_ONLY_RE.search(page_text))
    mechanism = classify_booking_mechanism(booking, entries, domain, request_only)
    # served-HTML-vangnet: een boekplatform dat JS uit de mobiele render strippt
    # (clinicminds/zorgdomein-steekproef) → behandel als self_booking, zodat Q4/Q2
    # niet vals vuren op een refuteerbare "geen boeking".
    served_selfbook = booking_platform_in_html(served_html) or booking_platform_in_html(html)
    self_booking = (mechanism == "self_booking") or served_selfbook
    mechanism_eff = "self_booking" if served_selfbook else mechanism
    try:
        dom_form = bool(await page.evaluate(_FORM_PRESENT_JS))
    except Exception:
        dom_form = False
    # request_form telt mee als formulier-pad voor de Q4-gate (zie q4_form_available).
    form_present = q4_form_available(dom_form, mechanism_eff)
    q2 = evaluate_selfbooking(mechanism_eff, form_present=form_present, fetch_ok=fetch_ok)

    chat_platform = bool(_CHAT_PLATFORM_RE.search(blob))
    wa = bool(_WA_RE.search(blob))
    messenger = bool(_MESSENGER_RE.search(blob))
    ambiguous_chat = bool(_AMBIG_CHAT_RE.search(page_text)) and not chat_platform
    q4 = evaluate_coverage(chat_platform=chat_platform, wa=wa, messenger=messenger,
                           self_booking=self_booking,
                           ambiguous_chat=ambiguous_chat, fetch_ok=fetch_ok)

    try:
        price = await page.evaluate(_PRICE_SCAN_JS)
    except Exception:
        price = None
    p = dict(price or {})
    p["price_page_link"] = bool(p.get("price_page_link")
                                or price_link_in_html(served_html) or price_link_in_html(html))
    if fetch_ok and not (p.get("euro_amount") or p.get("price_page_link")) and p.get("service_link"):
        try:
            await page.goto(p["service_link"], wait_until="domcontentloaded", timeout=15000)
            await _dismiss_cookie_banner(page)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            sp = await page.evaluate(_PRICE_SCAN_JS) or {}
            p["euro_amount"] = p.get("euro_amount") or sp.get("euro_amount")
            p["price_page_link"] = p.get("price_page_link") or sp.get("price_page_link")
        except Exception:
            p["service_unchecked"] = True
    p1 = evaluate_price_anchor(p, fetch_ok=fetch_ok)

    return {"fetch_ok": fetch_ok, "q4": q4["state"], "q7": q7["state"],
            "q2": q2["state"], "p1": p1["state"], "form_present": form_present,
            "mechanism": mechanism,
            "evidence": {"q4": q4["evidence"], "q7": q7["evidence"],
                         "q2": q2["evidence"], "p1": p1["evidence"]}}


_RECEPTIE_FAIL = {"fetch_ok": False, "q4": "onbepaald", "q7": "onbepaald",
                  "q2": "onbepaald", "p1": "onbepaald", "form_present": False}


async def detect_receptie(domain: str, playwright: Any | None = None, *,
                          browser: Any | None = None, timeout_ms: int = 20000,
                          runs: int = 2) -> dict[str, Any]:
    """Volledige receptie-haakje-detectie voor één domein: `runs` verse mobiele
    renders, per as fail-closed 2-run-gecombineerd (combine_stable), dan de
    ladder-haak (decide_receptie_hook). Nooit-raise; bij falen hook_code=None.

    Voor een batch geef je een gedeelde `playwright`+`browser` mee. Dit is de
    functie die de writer/worker aanroept (audit: er moet iets zijn dat écht
    hook_* produceert in de productie-flow)."""
    own_pw = playwright is None
    own_browser = browser is None
    pw_cm = None
    per_run: list[dict[str, Any]] = []
    try:
        if own_pw:
            pw_cm = async_playwright()
            playwright = await pw_cm.__aenter__()
        if own_browser:
            browser, _ = await new_browser_context(playwright)
        for _ in range(max(1, runs)):
            ctx = await mobile_context(browser, playwright)
            page = await ctx.new_page()
            reqs: list[str] = []
            page.on("request", lambda r, _b=reqs: _b.append(r.url))
            try:
                clean = re.sub(r"^https?://", "", domain, flags=re.IGNORECASE).strip("/")
                resp = None
                for scheme in ("https://", "http://"):
                    try:
                        resp = await page.goto(scheme + clean, wait_until="domcontentloaded", timeout=timeout_ms)
                        if resp is not None:
                            break
                    except Exception:
                        continue
                if resp is None:
                    per_run.append(dict(_RECEPTIE_FAIL, error="navigation_failed"))
                    continue
                try:
                    served = await resp.text()
                except Exception:
                    served = ""
                consent = await _dismiss_cookie_banner(page)
                await _scroll_full(page)
                try:
                    await page.wait_for_load_state("networkidle", timeout=6000)
                except Exception:
                    pass
                per_run.append(await _receptie_one_render(
                    page, served_html=served, request_urls=reqs,
                    consent_accepted=bool(consent), domain=domain))
            except Exception as e:
                per_run.append(dict(_RECEPTIE_FAIL, error=str(e)[:120]))
            finally:
                try:
                    await ctx.close()
                except Exception:
                    pass
    finally:
        try:
            if own_browser and browser is not None:
                await browser.close()
        except Exception:
            pass
        try:
            if own_pw and pw_cm is not None:
                await pw_cm.__aexit__(None, None, None)
        except Exception:
            pass

    a, b = per_run[0], per_run[-1]
    axes = {ax: combine_stable(a.get(ax), b.get(ax)) for ax in ("q4", "q7", "q2", "p1")}
    form_present = bool(a.get("form_present") or b.get("form_present"))
    decision = decide_receptie_hook(axes["q4"], axes["q7"], axes["q2"], axes["p1"],
                                    form_present=form_present)
    return {"domain": domain, **axes, "form_present": form_present, **decision,
            "runs": per_run}
