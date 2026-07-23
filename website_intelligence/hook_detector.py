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
    "just a moment", "attention required", "cf-challenge",
    "checking your browser", "enable javascript and cookies",
    "verifying you are human", "access denied", "you have been blocked",
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

    def _is_booking(e: dict) -> bool:
        blob_txt = f"{e.get('text') or ''} {e.get('aria') or ''}"
        href = e.get("href") or ""
        return (any(k in blob_txt for k in _BOOK_TEXT)
                or any(h in href for h in _BOOK_HREF_HINTS)
                or any(p in href for p in ALL_BOOKING_PLATFORMS))

    hits = [e for e in raw_entries if _is_booking(e)]
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
             "sig2_allowed": False, "sample": []}
    try:
        raw = await page.evaluate(_ENTRY_SCAN_JS)
    except Exception:
        return empty
    result = evaluate_booking_entries(raw or [])
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
    try:
        val = await page.evaluate(
            """() => {
                const n = performance.getEntriesByType('navigation')[0];
                if (n && n.domInteractive) return Math.round(n.domInteractive);
                const t = performance.timing;
                if (t && t.domInteractive && t.navigationStart)
                    return t.domInteractive - t.navigationStart;
                return null;
            }"""
        )
        if isinstance(val, (int, float)) and val >= 0:
            dom_interactive_ms = int(val)
    except Exception:
        pass
    return {"load_ms": wall_load_ms, "dom_interactive_ms": dom_interactive_ms}


def _decide_signal(
    *,
    fetch_ok: bool,
    booking: dict[str, Any],
    tel_present: bool,
    cta: dict[str, Any],
    dom_interactive_ms: int | None,
) -> dict[str, Any]:
    """Ladder-beslissing (eerste dat vuurt), met Guardrail 1 hard ingebouwd."""
    evidence: list[str] = []
    # Guardrail 1: zonder geslaagde, volledige render vuurt NIETS.
    if not fetch_ok:
        return {"fired_signal": None, "signal_name": None, "confidence": "low",
                "evidence": ["fetch_failed_or_partial"]}

    bval = booking.get("value")
    bconf = booking.get("confidence")
    # 'Echte online boeking' = uitsluitend een HOGE-confidence hit (platform-
    # domein, boek-iframe of een echte boek-link). 'unknown' (mislukte/dunne
    # fetch) telt nergens mee: Guardrail 1.
    real_online_booking = (bval == "has_booking" and bconf == "high")

    # Signaal 1 — geen online boeking, wél een telefoonnummer. Vuurt UITSLUITEND
    # bij een geverifieerde HIGH-confidence 'no_booking'. De eerdere low-conf-
    # route (keyword-hit maar "geen echt boekkanaal") is verwijderd: de testrun
    # van 2026-07-22 bewees 4/4 false-positives op precies dat pad (Hairworld/
    # Amstelzijde/Aever/Monalisa hadden wél een consult-formulier of boekknop) —
    # dat waren aantoonbaar onware mails geworden. Bij twijfel: niet vuren.
    if bval == "no_booking" and bconf == "high" and tel_present:
        evidence = ["no_online_booking", "tel_present"]
        evidence += booking.get("evidence") or []
        return {"fired_signal": 1, "signal_name": "no_online_booking",
                "confidence": "high", "evidence": evidence}

    # Signaal 2 — er ís een echte online boeking, maar de bezoeker kan NIET
    # boeken zonder te scrollen. Harde regel (live bewezen op Fairday/CadanCe,
    # 2026-07-22): een boek-/afspraak-ingang in de header of hoofdnavigatie —
    # óók in een dichtgeklapt hamburger-menu — laat het signaal vervallen,
    # ongeacht hoe diep de body-widget zit. `sig2_allowed` (pure evaluatie in
    # evaluate_booking_entries) bundelt dat: geen nav-ingang, geen verborgen
    # boek-element, en de hoogste zichtbare ingang minstens 2 schermen diep.
    cta_y = cta.get("y")
    if real_online_booking and cta.get("sig2_allowed"):
        evidence = ["booking_cta_below_fold", f"cta_y={cta_y}>={SIGNAL2_MIN_Y}",
                    "nav_check=schoon (geen boekingang in header/nav/menu)"]
        evidence += booking.get("evidence") or []
        return {"fired_signal": 2, "signal_name": "cta_below_fold",
                "confidence": "high", "evidence": evidence}
    if real_online_booking and cta.get("found") and cta.get("nav_booking"):
        # expliciet bewijs voor de uitlezing waarom sig2 NIET vuurt
        evidence.append("nav_booking_present")

    # Signaal 3 — trage TTI (rough, zonder throttle → 'low').
    if dom_interactive_ms is not None and dom_interactive_ms > 4000:
        return {"fired_signal": 3, "signal_name": "slow_tti", "confidence": "low",
                "evidence": [f"dom_interactive_ms={dom_interactive_ms}>4000_rough"]}

    return {"fired_signal": None, "signal_name": None, "confidence": "low",
            "evidence": evidence + ["site_ok_no_signal_1_3"]}


async def detect_hook_on_page(page, wall_load_ms: int | None = None) -> dict[str, Any]:
    """Voer Guardrail-1-flow + detectie uit op een AL genavigeerde mobiele page.

    De caller heeft `page.goto(...)` al gedaan (zodat de wall-clock-laadtijd
    buiten gemeten kan worden). Deze functie: cookie weg → scroll → networkidle
    → boeking/tel/CTA-positie/TTI → laddersignaal. Nooit-raise.
    """
    result: dict[str, Any] = {
        "fetch_ok": False, "verifiable": True, "fired_signal": None,
        "signal_name": None, "confidence": "low", "booking": None,
        "tel_present": False,
        "cta": {"found": False, "nav_booking": False, "hidden_booking": False,
                "above_fold": None, "min_visible_y": None, "y": None,
                "sig2_allowed": False, "sample": []},
        "load_ms": wall_load_ms, "dom_interactive_ms": None,
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
        low_head = (html or "")[:4000].lower()
        if any(m in low_head for m in _CHALLENGE_MARKERS):
            result["fetch_ok"] = False
            result["verifiable"] = False
            result["evidence"] = ["challenge_page_not_machine_verifiable"]
            return result
        result["fetch_ok"] = fetch_ok

        booking = _combine_booking(html, fetch_ok=fetch_ok)
        result["booking"] = booking

        try:
            result["tel_present"] = await page.locator("a[href^='tel:']").count() > 0
        except Exception:
            result["tel_present"] = False

        # Boekingang-scan (alle a/button, óók verborgen + header/nav) — alleen
        # relevant als er überhaupt een boeking is.
        if booking.get("value") == "has_booking":
            result["cta"] = await _scan_booking_entries(page)

        timing = await _measure_timing(page, wall_load_ms)
        result["load_ms"] = timing["load_ms"]
        result["dom_interactive_ms"] = timing["dom_interactive_ms"]

        decision = _decide_signal(
            fetch_ok=fetch_ok, booking=booking, tel_present=result["tel_present"],
            cta=result["cta"], dom_interactive_ms=result["dom_interactive_ms"],
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
            detected = await detect_hook_on_page(page, wall_load_ms=wall)
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
