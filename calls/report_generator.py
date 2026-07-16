"""
calls/report_generator.py — het check-up rapport (gate tussen reply en deal).

Model: Sonnet (dit gaat naar een arts met hun praktijknaam erop; Haiku is voor
bulk-openers). Voorwaarde: `checkup_data` niet leeg — anders geen rapport
(report_status='skipped'), want website intelligence alleen is een echo van de
opener, geen geschenk.

Drie regels waar het rapport aan staat of valt (in de prompt afgedwongen):
  1. Symptomen, geen oplossingen. Feit + getal + gevolg. Nooit een oplossing —
     diagnose is gratis, behandelplan is de deal.
  2. Hun getallen, onze rekensom. Citeer uit het transcript, reken conservatief
     en zeg dat hardop. Zij noemden het getal, wij rekenen het door.
  3. Geen pitch. Geen dienst, product of call to action.

`validate_report_sendable` is de fail-closed QA-gate, analoog aan
`utils.text_normalizer.validate_opener_sendable`: faalt de gate → niet opslaan,
report_status blijft 'pending'. Het menselijke vrijgeef-gate (gate 2) blijft
daarnaast altijd bestaan; deze QA is het vangnet, niet de enige bewaker.

Herbruikt het gedrag van campaigns/review_email_generator.py: echte data in,
strakke regels, geen verkooppraat, never-raise.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)

_SONNET_MODEL = "claude-sonnet-4-6"
from config.pricing import get_legacy_per_m_eur
_PRICING = get_legacy_per_m_eur(_SONNET_MODEL)
_COST_PER_M_INPUT = _PRICING["input_per_m_eur"]
_COST_PER_M_OUTPUT = _PRICING["output_per_m_eur"]

_MAX_WORDS = 400
_REQUIRED_FINDINGS = 3

# Woorden die een dienst/pitch aanprijzen — verboden in het rapport.
_PITCH_RE = re.compile(
    r"\b(wij kunnen|we kunnen|onze oplossing|onze dienst|neem contact|"
    r"laat (het )?weten|plan een|boek een|schedule|bel ons|mail ons|"
    r"wij bieden|wij helpen|wij lossen|kunnen wij)\b",
    re.IGNORECASE,
)
# Liggend streepje / gedachtestreepje (en-dash, em-dash, of spatie-omgeven
# koppelteken als gedachtestreepje). Een gewoon koppelteken in 'check-up' mag.
_DASH_RE = re.compile(r"[–—]|\s-\s|\s--\s")


# =============================================================================
# QA-gate (fail-closed)
# =============================================================================

def _collect_source_numbers(checkup_data: dict | None, website_intelligence: dict | None) -> set[float]:
    """Alle getallen die als BRON gelden: checkup_data-waarden + website-score +
    score_vs_market. Recursief door checkup_data."""
    nums: set[float] = set()

    def _walk(v: Any) -> None:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            nums.add(float(v))
        elif isinstance(v, dict):
            for x in v.values():
                _walk(x)
        elif isinstance(v, list):
            for x in v:
                _walk(x)

    _walk(checkup_data or {})
    wi = website_intelligence or {}
    for k in ("total_score", "website_score", "technical_score", "conversion_score"):
        val = wi.get(k)
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            nums.add(float(val))
    comp = wi.get("competitor_data") or {}
    svm = comp.get("score_vs_market")
    if isinstance(svm, (int, float)) and not isinstance(svm, bool):
        nums.add(abs(float(svm)))
    return nums


def _derivable_numbers(source: set[float]) -> set[int]:
    """Getallen die uit de bron af te leiden zijn via een conservatieve rekensom:
    bronwaarden zelf, hun %-vorm (×100), week→maand→jaar-multiples (×4/12/52), en
    producten/sommen van 2-3 bronwaarden (evt. × één multiplier). Zo blijft een
    échte 'onze rekensom' toegestaan, terwijl een verzonnen statistiek opvalt.

    Bewust ruim (het menselijke vrijgeef-gate is de echte bewaker); dit vangt de
    grove verzinsels ('gemiddeld 30% van klinieken…') die niet uit de bron komen.
    """
    base = {n for n in source if n}
    mults = {1, 4, 12, 52, 100}
    vals: set[float] = set()
    b = list(base)
    # enkelvoudig + multiplier
    for x in b:
        for m in mults:
            vals.add(x * m)
    # paren en triples (evt. × multiplier)
    for i in range(len(b)):
        for j in range(len(b)):
            p = b[i] * b[j]
            vals.add(p)
            for m in mults:
                vals.add(p * m)
            for k in range(len(b)):
                vals.add(p * b[k])
    return {int(round(v)) for v in vals if v}


def validate_report_sendable(
    report_html: str | None,
    checkup_data: dict | None,
    website_intelligence: dict | None = None,
    findings: list | None = None,
) -> tuple[bool, str]:
    """Fail-closed QA op een gegenereerd rapport. Returns (ok, reason).

    Weigert bij: getal dat niet uit checkup_data/website-intelligence af te leiden
    is, pitch-woorden, liggend/gedachtestreepje, ≠3 bevindingen, of >400 woorden.
    ok=False → NIET opslaan, report_status blijft 'pending'.
    reason ∈ ok | empty | dash | pitch | too_long | finding_count | ungrounded_number:N.
    """
    html = (report_html or "").strip()
    if not html:
        return False, "empty"

    text = re.sub(r"<[^>]+>", " ", html)  # tags weg voor tekst-checks

    if _DASH_RE.search(text):
        return False, "dash"
    if _PITCH_RE.search(text):
        return False, "pitch"
    if len(text.split()) > _MAX_WORDS:
        return False, "too_long"

    n_findings = len(findings) if findings is not None else len(re.findall(r'class="finding"', html))
    if n_findings != _REQUIRED_FINDINGS:
        return False, "finding_count"

    # Getal-grounding — elk 'betekenisvol' getal (>=10, geen jaartal) moet uit de
    # bron af te leiden zijn. Kleine getallen (ordinalen 1-9) en jaartallen slaan
    # we over.
    allowed = _derivable_numbers(_collect_source_numbers(checkup_data, website_intelligence))
    for raw in re.findall(r"\d[\d.\s]*\d|\d", text):
        digits = re.sub(r"[.\s]", "", raw)
        if not digits.isdigit():
            continue
        n = int(digits)
        if n < 10 or 2000 <= n <= 2035:
            continue
        if not any(abs(n - a) <= max(1, a * 0.02) for a in allowed):
            return False, f"ungrounded_number:{n}"

    return True, "ok"


# =============================================================================
# Rapport-generatie (Sonnet)
# =============================================================================

def _render_report_html(company_name: str, city: str, findings: list[dict]) -> str:
    """Render de 3 bevindingen tot een schoon HTML-document (bron voor de PDF).

    Deterministisch — géén em-dashes, controleerbare structuur (elke bevinding
    heeft class='finding' zodat de QA-gate ze telt).
    """
    blocks = []
    for i, f in enumerate(findings, 1):
        fact = (f.get("fact") or "").strip()
        cost = (f.get("cost") or "").strip()
        blocks.append(
            f'    <section class="finding">\n'
            f'      <h2>{i}. {_esc(f.get("title") or "Bevinding")}</h2>\n'
            f'      <p class="fact">{_esc(fact)}</p>\n'
            f'      <p class="cost">{_esc(cost)}</p>\n'
            f'    </section>'
        )
    body = "\n".join(blocks)
    return (
        f'<article class="checkup-report">\n'
        f'  <header>\n'
        f'    <p class="eyebrow">Check-up</p>\n'
        f'    <h1>{_esc(company_name)}</h1>\n'
        f'    <p class="loc">{_esc(city)}</p>\n'
        f'  </header>\n'
        f'  <div class="findings">\n{body}\n  </div>\n'
        f'</article>'
    )


def _esc(s: str) -> str:
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            .replace("–", ", ").replace("—", ", "))  # dashes preventief weg


async def generate_checkup_report(
    lead: dict,
    checkup_data: dict,
    transcript: str,
    website_intelligence: dict | None = None,
    anthropic_client: Any | None = None,
    supabase_client: Any | None = None,
) -> dict:
    """Genereer een check-up rapport (3 bevindingen: feit+getal, rekensom, geen
    oplossing). Never-raise — retourneert altijd een dict, met 'error' bij falen.

    Voorwaarde: `checkup_data` niet leeg (anders geen rapport → caller zet
    report_status='skipped').

    Args:
        lead: heatr_leads-dict (company_name, city, sector).
        checkup_data: cijfers uit het gesprek (verplicht, niet leeg).
        transcript: het gesprekstranscript.
        website_intelligence: optionele WI-dict (score, top issues, score_vs_market).
        anthropic_client: optioneel geïnjecteerde AsyncAnthropic.
        supabase_client: optioneel, voor api_cost_log.

    Returns:
        {report_findings: list[dict] (3), report_html: str, tokens_used, cost_eur}
        of {..., error: str} bij falen / lege checkup_data / gefaalde QA.
    """
    empty = {"report_findings": None, "report_html": None, "tokens_used": 0, "cost_eur": 0.0}
    try:
        if not checkup_data:
            return {**empty, "error": "no_checkup_data"}
        company_name = (lead.get("company_name") or "").strip()
        city = (lead.get("city") or "").strip()
        sector = lead.get("sector") or ""
        if not company_name:
            return {**empty, "error": "incomplete_lead_data: missing company_name"}

        wi = website_intelligence or {}
        website_score = wi.get("total_score") or wi.get("website_score") or 0
        comp = wi.get("competitor_data") or {}
        score_vs_market = comp.get("score_vs_market") or 0
        top_issues = wi.get("top_issues") or wi.get("top_3_issues") or []
        top_issues_txt = ", ".join(str(x) for x in top_issues) if top_issues else "(geen)"

        if anthropic_client is None:
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                return {**empty, "error": "ANTHROPIC_API_KEY missing"}
            import anthropic
            anthropic_client = anthropic.AsyncAnthropic(api_key=api_key)

        prompt = (
            f"Schrijf een check-up rapport voor {company_name} in {city}, sector {sector}.\n\n"
            f"Bronnen:\n"
            f"- Transcript van het gesprek (hieronder)\n"
            f"- Cijfers die zij zelf noemden: {json.dumps(checkup_data, ensure_ascii=False)}\n"
            f"- Website-analyse: score {website_score}/100, {score_vs_market} punten ten opzichte "
            f"van concurrenten in {city}, top issues: {top_issues_txt}\n\n"
            f"Lever precies 3 bevindingen. Per bevinding:\n"
            f"- Wat er is: feit, met getal\n"
            f"- Wat het kost: rekensom, conservatief, hardop gerekend\n"
            f"- Geen oplossing\n\n"
            f"Regels:\n"
            f"- Gebruik hun eigen woorden waar je ze hebt, citeer uit het transcript\n"
            f"- Reken conservatief en benoem dat je dat doet\n"
            f"- Noem geen enkele oplossing, dienst of product\n"
            f"- Geen call to action\n"
            f"- Geen superlatieven\n"
            f"- Gebruik geen liggende streepjes of gedachtestreepjes, in geen enkele vorm\n"
            f"- Nederlands, zakelijk, kort, maximaal 400 woorden totaal\n\n"
            f"Return ALLEEN JSON (geen andere tekst):\n"
            f'{{"findings": [{{"title": "korte kop", "fact": "feit met getal", '
            f'"cost": "rekensom, conservatief, hardop"}}, ... precies 3]}}\n\n'
            f"TRANSCRIPT:\n{transcript}"
        )

        try:
            response = await anthropic_client.messages.create(
                model=_SONNET_MODEL,
                max_tokens=1500,
                temperature=0.4,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as e:
            logger.warning("checkup report: Sonnet-call faalde voor %s: %s", company_name, e)
            return {**empty, "error": f"claude_failed: {str(e)[:100]}"}

        text = response.content[0].text.strip() if response.content else ""
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        try:
            parsed = json.loads(text)
            findings = parsed.get("findings") or []
        except (json.JSONDecodeError, ValueError, AttributeError) as e:
            logger.warning("checkup report: JSON-parse faalde voor %s: %s", company_name, e)
            return {**empty, "error": f"parse_failed: {str(e)[:100]}"}

        findings = [f for f in findings if isinstance(f, dict)][:_REQUIRED_FINDINGS]
        report_html = _render_report_html(company_name, city, findings)

        input_tokens = response.usage.input_tokens or 0
        output_tokens = response.usage.output_tokens or 0
        cost_eur = round((input_tokens * _COST_PER_M_INPUT + output_tokens * _COST_PER_M_OUTPUT) / 1_000_000, 6)

        if supabase_client is not None:
            try:
                supabase_client.table("api_cost_log").insert({
                    "workspace_id": lead.get("workspace_id") or "aerys",
                    "model": _SONNET_MODEL, "prompt_tokens": input_tokens,
                    "response_tokens": output_tokens, "cost_eur": cost_eur,
                    "context": "checkup_report", "lead_id": lead.get("id"),
                }).execute()
            except Exception as e:
                logger.debug("checkup report cost log faalde: %s", e)

        logger.info("checkup report: %s — %d bevindingen, cost=EUR %.6f",
                    company_name, len(findings), cost_eur)
        return {
            "report_findings": findings,
            "report_html": report_html,
            "tokens_used": input_tokens + output_tokens,
            "cost_eur": cost_eur,
        }

    except Exception as e:
        logger.error("checkup report: onverwachte fout voor %s: %s",
                     lead.get("company_name") if isinstance(lead, dict) else "?", e)
        return {**empty, "error": f"unexpected: {str(e)[:100]}"}
