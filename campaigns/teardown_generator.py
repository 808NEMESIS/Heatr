"""
campaigns/teardown_generator.py — de teardown-pagina (fase A PR A1).

Rendert per lead een deelbare, statische HTML-website-analyse uit de ECHTE
runtime-kolommen van heatr_website_intelligence (technical_details /
conversion_details / sector_details / opportunity_reasons + total_score).

Bewust GEEN screenshots of Claude-vision-proza: die bestaan niet in
productie (kolommen ontbreken, visual_score=0, PAGESPEED_API_KEY ongezet).
Wat er wél is — objectieve pass/fail-checks + kant-en-klare NL-redenen —
maakt een feitelijke audit-checklist. Dat is geloofwaardiger dan
AI-meningen én draagt geen reputatierisico van een botte gegenereerde zin
met de bedrijfsnaam erop.

Pure functies: extract_findings() en render_teardown_html() doen geen I/O,
zodat ze zonder DB testbaar zijn. De DB/Storage-orkestratie zit in de
endpoints (PR A2/A4).
"""
from __future__ import annotations

import hashlib
import html
import json
import secrets
from typing import Any

# ── Check-key → respectvol NL-label. Alleen checks die de PROSPECT raken;
#    checks die falen door ONZE config (pagespeed zonder key) worden
#    gefilterd in _usable_checks — nooit een prospect afrekenen op ons gat.
_TECH_LABELS = {
    "ssl": "Beveiligde verbinding (SSL)",
    "cms": "Modern content-systeem",
    "schema_markup": "Vindbaarheid-markup (schema)",
    "sitemap": "Sitemap voor zoekmachines",
    "server_location": "Server in NL/BE",
    "mobile_friendly": "Mobielvriendelijk",
}
_CONV_LABELS = {
    "cta_above_fold": "Duidelijke actieknop bovenaan",
    "phone_clickable": "Klikbaar telefoonnummer",
    "whatsapp": "WhatsApp-contact",
    "online_booking": "Online afspraken maken",
    "chatbot": "Live chat of chatbot",
    "contact_form": "Kort contactformulier",
}
# checks die we NIET tonen als 'gefaald' omdat het onze eigen config-gap is
_SKIP_NOTE_MARKERS = ("api_key", "not set", "not configured")


def new_token() -> str:
    """Niet-raadbare publieke URL-sleutel (128 bit)."""
    return secrets.token_urlsafe(24)


def compute_content_hash(lead: dict, wi: dict) -> str:
    """Deterministische hash over de bron-analyse → idempotente re-generatie
    (alleen opnieuw bouwen als de analyse écht wijzigde)."""
    basis = {
        "company": lead.get("company_name"),
        "city": lead.get("city"),
        "first": lead.get("contact_first_name"),
        "total": wi.get("total_score"),
        "tech": wi.get("technical_details"),
        "conv": wi.get("conversion_details"),
        "sector": wi.get("sector_details"),
        "reasons": wi.get("opportunity_reasons"),
    }
    blob = json.dumps(basis, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _usable_checks(details: dict, labels: dict) -> list[dict]:
    """Zet de `details`-lijst om naar [{label, passed}], met alleen checks
    die een label hebben en niet door onze eigen config falen."""
    out: list[dict] = []
    for c in (details or {}).get("details", []):
        key = c.get("check")
        if key not in labels:
            continue
        note = str(c.get("note") or "").lower()
        if any(m in note for m in _SKIP_NOTE_MARKERS):
            continue  # ons gat (bv. ontbrekende PAGESPEED_API_KEY), niet die van de prospect
        out.append({"label": labels[key], "passed": bool(c.get("passed"))})
    return out


def _sector_checks(wi: dict) -> list[dict]:
    """Sector-certificering/vertrouwenssignalen (RBCZ, AVG, privacyverklaring…)."""
    out: list[dict] = []
    for c in (wi.get("sector_details") or {}).get("checks", []):
        label = c.get("label") or c.get("key")
        if label:
            out.append({"label": str(label), "passed": bool(c.get("passed"))})
    return out


def _score_band(score: int | None) -> tuple[str, str]:
    """(kleur, woord) voor de score-hero."""
    s = score or 0
    if s < 40:
        return "#B23A2E", "veel ruimte om te groeien"
    if s < 70:
        return "#B26F22", "een solide basis met kansen"
    return "#3F7B54", "sterk"


def extract_findings(lead: dict, wi: dict) -> dict:
    """Gestructureerde samenvatting — voor de pagina, de mail (PR A3) en de
    CRM-taak (PR A2). Geen HTML, puur data."""
    tech = _usable_checks(wi.get("technical_details") or {}, _TECH_LABELS)
    conv = _usable_checks(wi.get("conversion_details") or {}, _CONV_LABELS)
    sector = _sector_checks(wi)

    reasons = wi.get("opportunity_reasons") or {}
    # kant-en-klare NL-redenen, ontdubbeld, in vaste volgorde
    top_findings = []
    for key in ("website_rebuild", "conversie_optimalisatie", "conversion_optimization",
                "chatbot", "ai_audit"):
        r = reasons.get(key)
        if r and r not in top_findings:
            top_findings.append(r)

    missing = [c["label"] for c in (conv + tech) if not c["passed"]]

    comp = wi.get("competitor_data") or {}
    svm = wi.get("score_vs_market")
    # Concurrentie is alleen bruikbaar als er >1 vergeleken is EN de delta
    # betekenisvol; de sector-match is in de ruwe data onbetrouwbaar (bekend),
    # dus dit blijft standaard UIT tot operator-review (B2/B4).
    competitor_usable = bool(
        isinstance(svm, int) and comp.get("total_analyzed", 0) >= 2
    )

    return {
        "score": wi.get("total_score") or 0,
        "tech_checks": tech,
        "conv_checks": conv,
        "sector_checks": sector,
        "top_findings": top_findings,
        "missing_labels": missing,
        "competitor_usable": competitor_usable,
        "score_vs_market": svm if competitor_usable else None,
        "market_avg": comp.get("market_avg_score") if competitor_usable else None,
    }


def _e(v: Any) -> str:
    return html.escape(str(v if v is not None else ""))


def _check_row(c: dict) -> str:
    icon = "✓" if c["passed"] else "○"
    cls = "ok" if c["passed"] else "no"
    return (f'<li class="{cls}"><span class="mark">{icon}</span>'
            f'<span>{_e(c["label"])}</span></li>')


def render_teardown_html(
    lead: dict,
    wi: dict,
    *,
    cta_url: str,
    unsubscribe_url: str | None = None,
    include_competitor: bool = False,
    sender_name: str = "Aerys",
) -> str:
    """Volledige, zelfstandige HTML-pagina (mobiel-eerst, huisstijl). Alle
    lead-afgeleide tekst is ge-escaped. `include_competitor` staat standaard
    UIT (concurrentie-data is onbetrouwbaar tot review — B2)."""
    f = extract_findings(lead, wi)
    company = _e(lead.get("company_name") or "jullie praktijk")
    city = _e(lead.get("city") or "")
    first = _e(lead.get("contact_first_name") or "")
    domain = _e(lead.get("domain") or "")
    score = f["score"]
    color, band = _score_band(score)

    greet = f"Hoi {first}," if first else "Hoi,"
    plaats = f" in {city}" if city else ""

    findings_html = "".join(
        f'<li class="finding">{_e(t)}</li>' for t in f["top_findings"][:3]
    ) or '<li class="finding">De basis staat, maar er is ruimte om meer bezoekers klant te maken.</li>'

    tech_html = "".join(_check_row(c) for c in f["tech_checks"])
    conv_html = "".join(_check_row(c) for c in f["conv_checks"])
    sector_html = "".join(_check_row(c) for c in f["sector_checks"][:8])

    competitor_block = ""
    if include_competitor and f["competitor_usable"] and f["score_vs_market"] is not None:
        svm = f["score_vs_market"]
        richting = "onder" if svm < 0 else "boven"
        competitor_block = f'''
      <section class="card">
        <h2>Hoe jullie het doen t.o.v. de buurt</h2>
        <p>Vergeleken met vergelijkbare praktijken{_e(plaats)} scoort jullie site
        <strong>{abs(svm)} punten {richting}</strong> het gemiddelde. Dat is precies
        het soort verschil dat bezoekers onbewust meewegen.</p>
      </section>'''

    unsub = (f'<a href="{_e(unsubscribe_url)}">geen analyses meer ontvangen</a>'
             if unsubscribe_url else "")

    return f'''<!doctype html>
<html lang="nl"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Website-analyse — {company}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600&family=Plus+Jakarta+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
  :root{{--ink:#1E1B17;--ink2:#4A443B;--ink3:#7C7566;--paper:#F7F4F0;--card:#fff;
    --line:#E8E2D8;--accent:#6D5AE6;--accent-soft:#EEEAFB;--ok:#3F7B54;--no:#B26F22;}}
  *{{box-sizing:border-box}}
  body{{margin:0;background:var(--paper);color:var(--ink);
    font-family:'Plus Jakarta Sans',system-ui,sans-serif;line-height:1.6;
    -webkit-font-smoothing:antialiased}}
  .wrap{{max-width:640px;margin:0 auto;padding:24px 20px 64px}}
  .eyebrow{{font-size:12px;letter-spacing:.14em;text-transform:uppercase;
    color:var(--accent);font-weight:600}}
  h1{{font-family:'Fraunces',Georgia,serif;font-weight:600;font-size:clamp(26px,7vw,38px);
    line-height:1.1;margin:8px 0 4px}}
  h2{{font-family:'Fraunces',Georgia,serif;font-weight:600;font-size:21px;margin:0 0 12px}}
  .sub{{color:var(--ink2);margin:0 0 24px}}
  .hero{{background:var(--card);border:1px solid var(--line);border-radius:18px;
    padding:26px;text-align:center;box-shadow:0 8px 30px rgba(30,27,23,.05);margin-bottom:18px}}
  .ring{{font-family:'Fraunces',serif;font-size:64px;font-weight:600;line-height:1;color:{color}}}
  .ring small{{font-size:22px;color:var(--ink3)}}
  .band{{display:inline-block;margin-top:6px;font-size:14px;color:var(--ink2)}}
  .card{{background:var(--card);border:1px solid var(--line);border-radius:16px;
    padding:22px;margin:14px 0;box-shadow:0 4px 16px rgba(30,27,23,.04)}}
  ul{{list-style:none;padding:0;margin:0}}
  ul.findings li{{padding:10px 0;border-bottom:1px solid var(--line);font-size:16px}}
  ul.findings li:last-child{{border:0}}
  ul.checks li{{display:flex;align-items:center;gap:10px;padding:7px 0;font-size:15px;color:var(--ink2)}}
  .mark{{width:22px;height:22px;border-radius:50%;display:grid;place-items:center;
    font-size:13px;font-weight:700;flex:0 0 auto}}
  li.ok .mark{{background:#E3F0E8;color:var(--ok)}}
  li.no .mark{{background:#F6EAD7;color:var(--no)}}
  li.no span:last-child{{color:var(--ink)}}
  .cta{{display:block;text-align:center;background:var(--accent);color:#fff;
    text-decoration:none;font-weight:600;padding:16px;border-radius:14px;margin:24px 0 8px;
    font-size:16px}}
  .cta:hover{{background:#5a48d6}}
  .foot{{margin-top:32px;font-size:12.5px;color:var(--ink3);text-align:center;line-height:1.7}}
  .foot a{{color:var(--ink3)}}
  .grid2{{display:grid;gap:14px}}
  @media(min-width:560px){{.grid2{{grid-template-columns:1fr 1fr}}}}
</style></head>
<body><div class="wrap">
  <p class="eyebrow">Gratis website-analyse</p>
  <h1>{company}</h1>
  <p class="sub">{greet} we hebben <strong>{domain}</strong> objectief doorgelicht op
  techniek, vindbaarheid en hoe makkelijk een bezoeker klant wordt. Hier is wat we zagen.</p>

  <div class="hero">
    <div class="ring">{score}<small>/100</small></div>
    <div class="band">Totaalscore — {band}</div>
  </div>

  <section class="card">
    <h2>Wat het meest opvalt</h2>
    <ul class="findings">{findings_html}</ul>
  </section>
{competitor_block}
  <div class="grid2">
    <section class="card"><h2>Techniek & vindbaarheid</h2><ul class="checks">{tech_html}</ul></section>
    <section class="card"><h2>Bezoeker → klant</h2><ul class="checks">{conv_html}</ul></section>
  </div>

  <section class="card"><h2>Vertrouwen & sector</h2><ul class="checks">{sector_html}</ul></section>

  <a class="cta" href="{_e(cta_url)}">Bespreek de analyse — 15 min, vrijblijvend</a>
  <p style="text-align:center;color:var(--ink3);font-size:13px;margin:0">
    We lopen 'm samen door en je krijgt de drie snelste verbeteringen mee — of je nu met ons werkt of niet.</p>

  <div class="foot">
    Deze analyse is gemaakt door {_e(sender_name)} op basis van je openbaar bereikbare website.
    Geen verplichtingen. {unsub}
  </div>
</div></body></html>'''
