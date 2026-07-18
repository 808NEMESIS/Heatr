"""
audit/checks.py — de checklist-checks van de audit-scorer.

Elke check leest uit de ScoreContext (bestaande WI/leads-velden + page_text +
netwerk-log + een lichte homepage-fetch voor DOM-structuur) en geeft één finding:

    {check_id, categorie, status, punten_behaald, punten_max, bewijs, severity,
     mail_safe, mail_zin, uitleg}

status:
  pass            -> behaald == max
  warn            -> deels
  fail            -> 0, maar wél meetbaar (telt mee in de noemer)
  not_measurable  -> databron ontbreekt -> UITGESLOTEN uit teller én noemer
                     (geen valse fail; normalisatie over de behaalde noemer)

mail_safe=False voor alles wat naar wetsovertreding ruikt (tracking, privacy) —
juist en waardevol, maar nooit in een geautomatiseerde eerste mail.
"""
from __future__ import annotations

import re
from typing import Callable

from audit import nl_trust, tracking


class ScoreContext:
    """Bundelt alle databronnen voor de checks van één lead."""

    def __init__(self, *, lead: dict, wi: dict, network_requests: list[dict],
                 page_text: str, schema_org: dict, response_headers: dict,
                 html: str | None, sector: str, places: dict | None = None):
        self.lead = lead or {}
        self.wi = wi or {}
        self.conv = (wi or {}).get("conversion_details") or {}
        self.tech = (wi or {}).get("technical_details") or {}
        self.network_requests = network_requests or []
        self.page_text = page_text or ""
        self.schema_org = schema_org or {}
        self.response_headers = {k.lower(): v for k, v in (response_headers or {}).items()}
        self.html = html or ""
        # Gecombineerd tekstveld voor INHOUD-checks (kan in html óf page_text zitten).
        # Structuur-checks (H1/title/embed/links) blijven ctx.html gebruiken.
        self.text_all = (html or "") + " \n " + (page_text or "")
        self.sector = sector
        self.places = places


def _f(check_id, categorie, status, behaald, mx, *, bewijs=None, mail_zin="",
       mail_safe=True, severity="medium", uitleg="") -> dict:
    return {
        "check_id": check_id, "categorie": categorie, "status": status,
        "punten_behaald": behaald, "punten_max": mx, "bewijs": bewijs,
        "severity": severity, "mail_safe": mail_safe, "mail_zin": mail_zin,
        "uitleg": uitleg,
    }


def _bool(cid, cat, mx, ok, zin_pass, zin_fail, **kw) -> dict:
    return _f(cid, cat, "pass" if ok else "fail", mx if ok else 0, mx,
              mail_zin=zin_pass if ok else zin_fail, **kw)


def _txt_has(text: str, *patterns: str) -> str | None:
    for p in patterns:
        m = re.search(p, text, re.I)
        if m:
            s = text[max(0, m.start() - 20):m.start() + 40]
            return " ".join(s.split())
    return None


# ── Lead Conversion ─────────────────────────────────────────────────────────
def c_online_afspraak_widget(ctx):
    ok = bool(ctx.lead.get("has_online_booking") or ctx.conv.get("has_online_booking") or ctx.conv.get("booking_platform"))
    plat = ctx.conv.get("booking_platform")
    return _bool("online_afspraak_widget", "lead_conversion", 10, ok,
                 "Bezoekers kunnen direct online een afspraak maken.",
                 "Bezoekers buiten kantooruren kunnen op uw site niets doen behalve wegklikken.",
                 bewijs=plat or ("has_online_booking" if ok else None), severity="high")

def c_whatsapp_klikbaar(ctx):
    ok = bool(ctx.lead.get("has_whatsapp") or ctx.conv.get("has_whatsapp"))
    return _bool("whatsapp_klikbaar", "lead_conversion", 5, ok,
                 "Er is een klikbare WhatsApp-knop.",
                 "Er is geen WhatsApp, terwijl dat in NL het meestgebruikte contactkanaal is.")

def c_tel_above_fold(ctx):
    ok = bool(ctx.conv.get("has_phone_clickable") or ctx.lead.get("phone"))
    return _bool("tel_above_fold", "lead_conversion", 3, ok,
                 "Het telefoonnummer is klikbaar.",
                 "Het telefoonnummer is niet als klikbare tel-link opgenomen.")

def c_contactform_max5(ctx):
    has = ctx.conv.get("has_contact_form")
    n = ctx.conv.get("form_field_count")
    if has is None:
        return _f("contactform_max5", "lead_conversion", "not_measurable", 0, 4)
    ok = bool(has) and (n is None or n <= 5)
    return _bool("contactform_max5", "lead_conversion", 4, ok,
                 "Er is een kort contactformulier.",
                 "Het contactformulier ontbreekt of vraagt te veel velden.",
                 bewijs=f"velden={n}" if n is not None else None)

def c_cta_above_fold(ctx):
    ok = bool(ctx.conv.get("has_cta_above_fold") or ctx.conv.get("cta_above_fold"))
    return _bool("cta_above_fold", "lead_conversion", 3, ok,
                 "Er staat een duidelijke call-to-action boven de vouw.",
                 "Er is geen duidelijke actieknop zichtbaar zonder te scrollen.")

def c_consult_intake_aanbod(ctx):
    b = _txt_has(ctx.page_text, r"\bconsult\b", r"\bintake\b", r"kennismakingsgesprek", r"gratis\s+gesprek")
    return _bool("consult_intake_aanbod", "lead_conversion", 3, bool(b),
                 "Een consult/intake wordt expliciet als aanbod genoemd.",
                 "Er is geen expliciet consult- of intake-aanbod.", bewijs=b)

def c_vergoeding_aanvullend(ctx):
    b = _txt_has(ctx.page_text, r"aanvullende\s+verzekering", r"vergoed", r"vergoeding")
    return _bool("vergoeding_aanvullend", "lead_conversion", 2, bool(b),
                 "Vergoeding via de aanvullende verzekering wordt genoemd.",
                 "Er staat niets over vergoeding door de aanvullende verzekering.", bewijs=b)

def c_responstijd_belofte(ctx):
    b = _txt_has(ctx.page_text, r"binnen\s+\d+\s*(uur|werkdag|dag)", r"reageren\s+binnen", r"snel\s+terug")
    return _bool("responstijd_belofte", "lead_conversion", 2, bool(b),
                 "Er staat een belofte over de reactietijd.",
                 "Er is geen belofte over hoe snel iemand reageert.", bewijs=b)

def c_live_chat(ctx):
    ok = bool(ctx.conv.get("has_chatbot"))
    return _bool("live_chat", "lead_conversion", 2, ok,
                 "Er is een live chat of chatbot.",
                 "Er is geen live chat.")


# ── Social Proof ────────────────────────────────────────────────────────────
def c_reviews_zichtbaar(ctx):
    src = ctx.text_all
    b = _txt_has(src, r"google\s+reviews", r"beoordelingen", r"\breviews?\b", r"testimonial", r"waardering")
    return _bool("reviews_zichtbaar", "social_proof", 5, bool(b),
                 "Er staan reviews/beoordelingen op de site.",
                 "Er staan geen reviews op de site zelf.", bewijs=b)

def c_google_rating_min(ctx):
    # Tier 2: Places is de bron (verser dan de Maps-scrape); tier 1 valt terug
    # op de lead-velden uit de scrape.
    p = ctx.places or {}
    r = p.get("rating") if p.get("rating") is not None else ctx.lead.get("google_rating")
    n = p.get("review_count") if p.get("review_count") is not None else ctx.lead.get("google_review_count")
    if r is None or n is None:
        return _f("google_rating_min", "social_proof", "not_measurable", 0, 4)
    ok = r >= 4.3 and n >= 20
    return _bool("google_rating_min", "social_proof", 4, ok,
                 f"Sterke Google-reputatie: {r} met {n} reviews.",
                 f"Google-reputatie onder de norm ({r} / {n} reviews).",
                 bewijs=f"rating={r}, reviews={n}")

def c_reviews_via_places(ctx):
    """Tier 2: reviews via de Places API als bron. Zonder Places-data (tier 1,
    geen key, place niet gevonden) -> not_measurable, uit de noemer."""
    p = ctx.places or {}
    if p.get("rating") is None or p.get("review_count") is None:
        return _f("reviews_via_places", "social_proof", "not_measurable", 0, 5)
    from audit.places import build_review_finding
    on_site = bool(_txt_has(ctx.text_all, r"google\s+reviews", r"beoordelingen", r"\breviews?\b",
                            r"testimonial", r"waardering"))
    return build_review_finding(p, on_site_shown=on_site, sector=ctx.sector)


def c_voor_na_galerij(ctx):
    b = _txt_has(ctx.text_all, r"voor[\s/-]*na", r"before[\s/-]*after", r"resultaten")
    return _bool("voor_na_galerij", "social_proof", 4, bool(b),
                 "Er is een voor/na-galerij.",
                 "Er is geen voor/na-galerij die resultaten toont.", bewijs=b)

def c_patientverhalen(ctx):
    b = _txt_has(ctx.page_text, r"pati[eë]ntverhaal", r"pati[eë]ntervaring", r"ervaringen", r"casu[iï]stiek")
    return _bool("patientverhalen", "social_proof", 4, bool(b),
                 "Er staan patiëntverhalen of casuïstiek op de site.",
                 "Er staan geen patiëntverhalen op de site.", bewijs=b)

def c_behandelaars_naam_foto_kwal(ctx):
    team = ctx.wi.get("team_contacts") or []
    named = [t for t in team if isinstance(t, dict) and (t.get("full_name") or t.get("first_name"))]
    ok = len(named) >= 1
    return _bool("behandelaars_naam_foto_kwal", "social_proof", 3, ok,
                 "De behandelaars staan met naam op de site.",
                 "De behandelaars staan niet herkenbaar (naam/foto) op de site.",
                 bewijs=f"{len(named)} teamlid(en)" if named else None)

def c_echte_praktijkfotos(ctx):
    # BEWUST always not_measurable (beslissing 2026-07): NIET dichten via de
    # Vision-fotografie-dimensie. Die correleert bijna volledig met de andere drie
    # dimensies — hij meet "ziet er goed uit", niet "heeft echte praktijkfoto's".
    # Blijft not_measurable tot er een echte signaalbron is (bv. stock-image-
    # herkenning / reverse-image). Geen valse fail, telt niet in de noemer.
    return _f("echte_praktijkfotos", "social_proof", "not_measurable", 0, 2)


# ── Local Trust ─────────────────────────────────────────────────────────────
def c_maps_embed_contact(ctx):
    if not ctx.html:
        return _f("maps_embed_contact", "local_trust", "not_measurable", 0, 4)
    b = _txt_has(ctx.html, r"google\.com/maps/embed", r"maps\.google\.[a-z.]+/maps", r"<iframe[^>]+maps")
    return _bool("maps_embed_contact", "local_trust", 4, bool(b),
                 "Er staat een Google Maps-kaart op de site.",
                 "Er staat geen kaart die laat zien waar u zit.", bewijs="maps-embed" if b else None)

def c_adres_footer_volledig(ctx):
    b = _txt_has(ctx.page_text, r"\b\d{4}\s?[A-Z]{2}\b\s+[A-Za-z]")  # NL postcode + plaats
    return _bool("adres_footer_volledig", "local_trust", 4, bool(b),
                 "Het volledige adres staat op de site.",
                 "Er staat geen volledig adres (straat, postcode, plaats).", bewijs=b)

def c_gbp_link(ctx):
    ok = bool(ctx.lead.get("google_maps_url")) or bool(_txt_has(ctx.html, r"g\.page/", r"maps\.app\.goo\.gl"))
    return _bool("gbp_link", "local_trust", 2, ok,
                 "Er is een link naar het Google-bedrijfsprofiel.",
                 "Er is geen link naar uw Google-bedrijfsprofiel.")

def c_openingstijden_structured(ctx):
    so = ctx.schema_org or {}
    has_schema = bool(so.get("openingHours") or so.get("openingHoursSpecification"))
    b = "schema.openingHours" if has_schema else _txt_has(ctx.page_text, r"openingstijden", r"ma[\s\-].*?vr", r"maandag.*?vrijdag")
    return _bool("openingstijden_structured", "local_trust", 3, bool(b),
                 "De openingstijden staan duidelijk op de site.",
                 "De openingstijden staan niet (gestructureerd) op de site.", bewijs=b if isinstance(b, str) else None)


# ── Professional / Medical Trust ────────────────────────────────────────────
def c_big_nummer(ctx):
    bigs = nl_trust.find_big_numbers(ctx.page_text) or nl_trust.find_big_numbers(ctx.html)
    ok = bool(bigs)
    return _bool("big_nummer", "professional_trust", 4, ok,
                 "Er staat een BIG-nummer met vermelding op de site.",
                 "Er staat geen BIG-nummer op de site.",
                 bewijs=bigs[0]["bewijs"] if bigs else None, severity="high")

def c_keurmerk_cosmetiek(ctx):
    k = nl_trust.find_keurmerken(ctx.page_text, ctx.sector) or nl_trust.find_keurmerken(ctx.html, ctx.sector)
    return _bool("keurmerk_cosmetiek", "professional_trust", 3, bool(k),
                 f"Er staat een keurmerk op de site ({', '.join(x['keurmerk'] for x in k)}).",
                 "Er staat geen erkend keurmerk (ZKN/NVCG/NVEPC) op de site.",
                 bewijs=k[0]["bewijs"] if k else None)

def c_wkkgz_klachten(ctx):
    w = nl_trust.has_wkkgz(ctx.page_text) or nl_trust.has_wkkgz(ctx.html)
    return _bool("wkkgz_klachten", "professional_trust", 1, bool(w),
                 "Er is een klachtenregeling (Wkkgz) vermeld.",
                 "Er staat geen klachtenregeling (Wkkgz) op de site.",
                 bewijs=w["bewijs"] if w else None)

def c_voor_na_toestemming(ctx):
    b = _txt_has(ctx.page_text, r"toestemming", r"met\s+toestemming")
    return _bool("voor_na_toestemming", "professional_trust", 1, bool(b),
                 "Voor/na-beelden vermelden toestemming.",
                 "Bij voor/na-beelden ontbreekt een toestemmingsvermelding.", bewijs=b)

def c_scn_nca_registratie(ctx):
    k = nl_trust.find_keurmerken(ctx.page_text, "chiropractoren") or nl_trust.find_keurmerken(ctx.html, "chiropractoren")
    return _bool("scn_nca_registratie", "professional_trust", 4, bool(k),
                 f"SCN/NCA-registratie vermeld ({', '.join(x['keurmerk'] for x in k)}).",
                 "Er staat geen SCN/NCA-registratie op de site.",
                 bewijs=k[0]["bewijs"] if k else None, severity="high")

def c_erkende_opleiding_chiro(ctx):
    o = nl_trust.has_erkende_opleiding_chiro(ctx.page_text) or nl_trust.has_erkende_opleiding_chiro(ctx.html)
    return _bool("erkende_opleiding_chiro", "professional_trust", 3, bool(o),
                 "Een erkende chiropractie-opleiding wordt genoemd.",
                 "Er staat geen erkende opleiding (AECC/Barcelona/Odense) vermeld.",
                 bewijs=o["bewijs"] if o else None)

def c_geschilleninstantie(ctx):
    g = nl_trust.has_geschilleninstantie(ctx.page_text) or nl_trust.has_geschilleninstantie(ctx.html)
    return _bool("geschilleninstantie", "professional_trust", 1, bool(g),
                 "Aansluiting bij een geschilleninstantie is vermeld.",
                 "Er staat geen geschilleninstantie vermeld.", bewijs=g["bewijs"] if g else None)


# ── Privacy / AVG (mail_safe=False) ─────────────────────────────────────────
def c_privacyverklaring_bereikbaar(ctx):
    b = _txt_has(ctx.text_all, r"privacy(verklaring|beleid|statement)?", r"privacy")
    return _bool("privacyverklaring_bereikbaar", "privacy", 3, bool(b),
                 "Er is een bereikbare privacyverklaring.",
                 "Er is geen (vindbare) privacyverklaring.",
                 bewijs=b, mail_safe=False, severity="high")

def c_cookiebanner_weigeren(ctx):
    has_banner = ctx.lead.get("has_cookie_banner")
    if has_banner is None:
        return _f("cookiebanner_weigeren", "privacy", "not_measurable", 0, 3, mail_safe=False)
    reject = _txt_has(ctx.html, r"weiger", r"afwijzen", r"alleen\s+noodzakelijk", r"reject")
    ok = bool(has_banner) and bool(reject)
    return _bool("cookiebanner_weigeren", "privacy", 3, ok,
                 "De cookiebanner biedt een even prominente weiger-optie.",
                 "De cookiebanner heeft geen (gelijkwaardige) weiger-optie." if has_banner else "Er is geen cookiebanner terwijl er wel getrackt wordt.",
                 bewijs=reject, mail_safe=False, severity="high")

def c_geen_tracking_pre_consent(ctx):
    res = tracking.detect_pre_consent_tracking(ctx.network_requests)
    if not ctx.network_requests:
        return _f("geen_tracking_pre_consent", "privacy", "not_measurable", 0, 3, mail_safe=False)
    ok = not res["has_pre_consent_tracking"]
    names = ", ".join(sorted({t["name"] for t in res["trackers"]}))
    return _bool("geen_tracking_pre_consent", "privacy", 3, ok,
                 "Er wordt geen tracking geladen vóór toestemming.",
                 f"Er laden trackers vóór toestemming ({names}).",
                 bewijs=res["trackers"][:5] or None, mail_safe=False, severity="high")

def c_verwerkersregister_dpo(ctx):
    b = _txt_has(ctx.page_text, r"verwerkersregister", r"functionaris\s+gegevensbescherming", r"\bDPO\b")
    return _bool("verwerkersregister_dpo", "privacy", 1, bool(b),
                 "Er is een verwerkersregister/DPO-contact vermeld.",
                 "Er is geen verwerkersregister of DPO-contact vermeld.",
                 bewijs=b, mail_safe=False)


# ── SEO ─────────────────────────────────────────────────────────────────────
def c_unieke_title_meta(ctx):
    if not ctx.html:
        return _f("unieke_title_meta", "seo_visibility", "not_measurable", 0, 2)
    title = re.search(r"<title[^>]*>(.*?)</title>", ctx.html, re.I | re.S)
    meta = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{20,})', ctx.html, re.I)
    ok = bool(title and title.group(1).strip()) and bool(meta)
    return _bool("unieke_title_meta", "seo_visibility", 2, ok,
                 "De pagina heeft een title en meta-description.",
                 "Title of meta-description ontbreekt.",
                 bewijs=(title.group(1).strip()[:60] if title else None))

def c_een_h1_per_pagina(ctx):
    if not ctx.html:
        return _f("een_h1_per_pagina", "seo_visibility", "not_measurable", 0, 1)
    h1 = re.findall(r"<h1[\s>]", ctx.html, re.I)
    ok = len(h1) == 1
    return _bool("een_h1_per_pagina", "seo_visibility", 1, ok,
                 "De homepage heeft precies één H1.",
                 f"De homepage heeft {len(h1)} H1-koppen (hoort er één te zijn).",
                 bewijs=f"h1-count={len(h1)}")

def c_schema_medical_valide(ctx):
    so = ctx.schema_org or {}
    t = str(so.get("@type") or so.get("type") or "")
    ok = bool(re.search(r"Medical(Clinic|Business|Organization)|Dentist|Physician|Chiropractic", t, re.I))
    if not so:
        return _f("schema_medical_valide", "seo_visibility", "not_measurable", 0, 2)
    return _bool("schema_medical_valide", "seo_visibility", 2, ok,
                 f"Er staat geldige medische schema-markup ({t}).",
                 "Er ontbreekt medische schema-markup (MedicalClinic/MedicalBusiness).",
                 bewijs=t or None)

def c_behandelpaginas_3plus(ctx):
    if not ctx.html:
        return _f("behandelpaginas_3plus", "seo_visibility", "not_measurable", 0, 3)
    links = set(re.findall(r'href=["\']([^"\']*(?:behandeling|behandelingen|treatment)[^"\']*)', ctx.html, re.I))
    ok = len(links) >= 3
    return _bool("behandelpaginas_3plus", "seo_visibility", 3, ok,
                 f"Er zijn losse behandelpagina's ({len(links)}).",
                 "Er zijn minder dan 3 losse behandelpagina's.",
                 bewijs=f"{len(links)} behandel-links")

def c_klachtenpaginas_3plus(ctx):
    if not ctx.html:
        return _f("klachtenpaginas_3plus", "seo_visibility", "not_measurable", 0, 3)
    links = set(re.findall(r'href=["\']([^"\']*(?:nekklachten|rugpijn|hoofdpijn|lage-rug|klachten)[^"\']*)', ctx.html, re.I))
    ok = len(links) >= 3
    return _bool("klachtenpaginas_3plus", "seo_visibility", 3, ok,
                 f"Er zijn losse klachtenpagina's ({len(links)}).",
                 "Er zijn minder dan 3 losse klachtenpagina's (nekklachten, rugpijn, hoofdpijn).",
                 bewijs=f"{len(links)} klachten-links")

def c_interne_links_key(ctx):
    if not ctx.html:
        return _f("interne_links_key", "seo_visibility", "not_measurable", 0, 1)
    ok = bool(re.search(r'href=["\'][^"\']*(contact|over-ons|team|afspraak)', ctx.html, re.I))
    return _bool("interne_links_key", "seo_visibility", 1, ok,
                 "Er zijn interne links naar de kernpagina's.",
                 "Er ontbreken interne links naar contact/over-ons/team.")

def c_sitemap_robots(ctx):
    ok = bool(ctx.tech.get("has_sitemap"))
    return _bool("sitemap_robots", "seo_visibility", 1, ok,
                 "Er is een sitemap.",
                 "Er is geen sitemap.xml gevonden.")


# ── Technical ───────────────────────────────────────────────────────────────
def c_psi_mobile_50(ctx):
    ps = ctx.tech.get("pagespeed_mobile")
    if ps is None:
        return _f("psi_mobile_50", "technical", "not_measurable", 0, 3)
    ok = ps >= 50
    return _bool("psi_mobile_50", "technical", 3, ok,
                 f"De mobiele snelheid is op orde (PSI {ps}).",
                 f"De mobiele site is traag (PSI {ps}, onder 50).", bewijs=f"psi_mobile={ps}")

def c_lcp_onder_2_5(ctx):
    # Nu not_measurable (LCP wordt niet apart opgeslagen). EERSTE INCREMENTELE
    # VERBETERING (beslissing 2026-07): LCP zit al in de PageSpeed-respons — uit te
    # lezen zonder re-crawl (we hebben PAGESPEED_API_KEY). Buiten deze run, laaghangend.
    return _f("lcp_onder_2_5", "technical", "not_measurable", 0, 2)

def c_https_geldig_cert(ctx):
    ok = bool(ctx.tech.get("has_ssl"))
    return _bool("https_geldig_cert", "technical", 1, ok,
                 "De site heeft een geldig HTTPS-certificaat.",
                 "De site heeft geen (geldig) HTTPS.", severity="high")

def c_geen_mixed_content(ctx):
    if not ctx.html:
        return _f("geen_mixed_content", "technical", "not_measurable", 0, 1)
    mixed = re.search(r'(src|href)=["\']http://', ctx.html, re.I)
    ok = mixed is None
    return _bool("geen_mixed_content", "technical", 1, ok,
                 "Geen onveilige (http) resources op een https-pagina.",
                 "Er worden onveilige http-resources geladen op een https-pagina.",
                 bewijs=(mixed.group(0) if mixed else None))

def c_security_headers(ctx):
    if not ctx.response_headers:
        return _f("security_headers", "technical", "not_measurable", 0, 1)
    present = [h for h in ("strict-transport-security", "content-security-policy", "x-frame-options")
               if h in ctx.response_headers]
    ok = len(present) >= 1
    return _bool("security_headers", "technical", 1, ok,
                 "Er zijn security-headers ingesteld.",
                 "Er ontbreken security-headers (HSTS/CSP/X-Frame-Options).",
                 bewijs=", ".join(present) or None)


# ── Visual (één getal) ──────────────────────────────────────────────────────
def c_visuele_indruk(ctx):
    vs = ctx.wi.get("visual_score")
    if vs is None:
        return _f("visuele_indruk", "visual", "not_measurable", 0, 8)
    behaald = round(vs / 25 * 8)
    status = "pass" if behaald >= 6 else ("warn" if behaald >= 3 else "fail")
    return _f("visuele_indruk", "visual", status, behaald, 8,
              bewijs=f"visual_score {vs}/25",
              mail_zin="De site oogt verzorgd en professioneel." if behaald >= 6
                       else "De site oogt gedateerd t.o.v. wat bezoekers in deze sector verwachten.")


# Registry: check_id -> functie. De scorer draait alleen de checks die voor de
# sector meetellen (config/audit_weights.checks_for_sector).
CHECK_FUNCS: dict[str, Callable[[ScoreContext], dict]] = {
    "online_afspraak_widget": c_online_afspraak_widget, "whatsapp_klikbaar": c_whatsapp_klikbaar,
    "tel_above_fold": c_tel_above_fold, "contactform_max5": c_contactform_max5,
    "cta_above_fold": c_cta_above_fold, "consult_intake_aanbod": c_consult_intake_aanbod,
    "vergoeding_aanvullend": c_vergoeding_aanvullend, "responstijd_belofte": c_responstijd_belofte,
    "live_chat": c_live_chat,
    "reviews_zichtbaar": c_reviews_zichtbaar, "google_rating_min": c_google_rating_min,
    "reviews_via_places": c_reviews_via_places,
    "voor_na_galerij": c_voor_na_galerij, "patientverhalen": c_patientverhalen,
    "behandelaars_naam_foto_kwal": c_behandelaars_naam_foto_kwal, "echte_praktijkfotos": c_echte_praktijkfotos,
    "maps_embed_contact": c_maps_embed_contact, "adres_footer_volledig": c_adres_footer_volledig,
    "gbp_link": c_gbp_link, "openingstijden_structured": c_openingstijden_structured,
    "big_nummer": c_big_nummer, "keurmerk_cosmetiek": c_keurmerk_cosmetiek,
    "wkkgz_klachten": c_wkkgz_klachten, "voor_na_toestemming": c_voor_na_toestemming,
    "scn_nca_registratie": c_scn_nca_registratie, "erkende_opleiding_chiro": c_erkende_opleiding_chiro,
    "geschilleninstantie": c_geschilleninstantie,
    "privacyverklaring_bereikbaar": c_privacyverklaring_bereikbaar, "cookiebanner_weigeren": c_cookiebanner_weigeren,
    "geen_tracking_pre_consent": c_geen_tracking_pre_consent, "verwerkersregister_dpo": c_verwerkersregister_dpo,
    "unieke_title_meta": c_unieke_title_meta, "een_h1_per_pagina": c_een_h1_per_pagina,
    "schema_medical_valide": c_schema_medical_valide, "behandelpaginas_3plus": c_behandelpaginas_3plus,
    "klachtenpaginas_3plus": c_klachtenpaginas_3plus, "interne_links_key": c_interne_links_key,
    "sitemap_robots": c_sitemap_robots,
    "psi_mobile_50": c_psi_mobile_50, "lcp_onder_2_5": c_lcp_onder_2_5,
    "https_geldig_cert": c_https_geldig_cert, "geen_mixed_content": c_geen_mixed_content,
    "security_headers": c_security_headers,
    "visuele_indruk": c_visuele_indruk,
}
