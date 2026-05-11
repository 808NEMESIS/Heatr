"""
enrichment/archetype_classifier.py — Claude Haiku classifies een lead in een
koop-archetype. Sector + subcategorie zeggen WAT ze doen, archetype zegt HOE
ze beslissen — en dat bepaalt de Mail 1 tone.

8 archetypes, 4 per sector:

  cosmetische_behandelaars:
    - medisch_cosmetisch       (plastisch chirurgen, haartransplant, cosm. tand)
    - esthetisch_medisch       (botox/filler/laser/huidtherapie BIG)
    - premium_beauty           (PMU, hoogwaardige schoonheids­specialisten)
    - volume_beauty            (schoonheids­salons, nagels, body-contour)

  alternatieve_geneeskunde:
    - lichaamswerk_pragmatisch (osteo/chiro/manueel/cranio)
    - holistisch_spiritueel    (reiki/energetisch/ayurveda/shiatsu/reflex)
    - therapeutisch_mentaal    (mindfulness/hypno/NLP/hapto/vaktherapie)
    - welzijn_praktisch        (diëtist/orthomoleculair/darm/voedingscoach)

Output:
  lead.archetype:        kebab-key string
  lead.archetype_reason: 1 zin uitleg (voor audit + UI tooltip)
  lead.archetype_confidence: 0-1 float

Cost: 1 Haiku call per lead (~€0.0003). Gecached op cache_key van
sector + treatment_focus + page_text-hash.
Guarded via utils.cost_guard.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from config.pricing import get_legacy_per_m_eur
from utils.claude_cache import log_api_cost
from utils.cost_guard import LeadCostAccumulator, guarded_call

logger = logging.getLogger(__name__)

_HAIKU_MODEL = "claude-haiku-4-5-20251001"
# Pricing centraal in config/pricing.py.
_PRICING = get_legacy_per_m_eur(_HAIKU_MODEL)
_COST_PER_M_INPUT = _PRICING["input_per_m_eur"]
_COST_PER_M_OUTPUT = _PRICING["output_per_m_eur"]
_ESTIMATED_COST_EUR = 0.0004

VALID_ARCHETYPES: dict[str, set[str]] = {
    "cosmetische_behandelaars": {
        "medisch_cosmetisch",
        "esthetisch_medisch",
        "premium_beauty",
        "volume_beauty",
    },
    "alternatieve_geneeskunde": {
        "lichaamswerk_pragmatisch",
        "holistisch_spiritueel",
        "therapeutisch_mentaal",
        "welzijn_praktisch",
    },
}

_ARCHETYPE_DESCRIPTIONS_NL: dict[str, str] = {
    # cosmetisch
    "medisch_cosmetisch": (
        "Klinisch-medisch: plastisch chirurg, cosmetisch arts, haartransplantatie, "
        "cosmetische tandheelkunde. BIG-trots, certificering belangrijk, hoge prijs "
        "per behandeling, ziet zichzelf als arts NIET als ondernemer. Geen marketing-jargon."
    ),
    "esthetisch_medisch": (
        "Hybride medisch+commercieel: botox/filler-klinieken, laser, huidtherapie. "
        "BIG-geregistreerd MAAR Instagram-actief, conversie-bewust, prijsdruk uit "
        "beauty-segment. Combineert medische geloofwaardigheid met commerciële drive."
    ),
    "premium_beauty": (
        "Persoonlijke high-end behandelaar: PMU-studio, exclusieve schoonheids­specialiste, "
        "wimper-master. Eigenaar = behandelaar = brand. Klantrelatie centraal, "
        "esthetische identiteit cruciaal, geen volume-praktijk."
    ),
    "volume_beauty": (
        "Volume-gedreven beauty: reguliere schoonheidssalon, nagelstudio, body-contouring. "
        "Bezetting-gericht, no-show-angst, beperkt marketing-budget. Nuchter-praktisch, "
        "getallen tellen."
    ),
    # alternatief
    "lichaamswerk_pragmatisch": (
        "Geleerd vak met evidence-bewustzijn: osteopaat, chiropractor, manueel therapeut, "
        "craniosacraal. Vaak ex-fysio. Sceptisch over marketing-fluff. Wil vakinhoudelijk "
        "aangesproken worden, niet met sales-jargon."
    ),
    "holistisch_spiritueel": (
        "Anti-systeem holistisch: reiki, energetisch, shiatsu, ayurveda, reflexzone. "
        "Persoonlijke roeping, allergisch voor 'conversie'/'ROI'/'leadgen'. Tone moet "
        "zacht zijn, ruimte laten, geen verkoop-druk."
    ),
    "therapeutisch_mentaal": (
        "Klant-relatie-gedreven mentale therapie: mindfulness, hypnotherapie, NLP, "
        "haptotherapie, vaktherapie. Vertrouwen voor alles. Niet-haastig, reflectief, "
        "korter ipv langer, geen meteen-oplossings-mode."
    ),
    "welzijn_praktisch": (
        "Resultaat-gerichte natuurgeneeskunde: diëtist, orthomoleculair therapeut, "
        "darmtherapie, voedingscoach. Klant zoekt verandering, vaak pakket-business. "
        "Concreet-praktisch, getal-vriendelijk, korter."
    ),
}


async def classify_archetype(
    lead: dict[str, Any],
    workspace_id: str,
    supabase_client: Any,
    anthropic_client: Any,
    page_text: str | None = None,
    accumulator: LeadCostAccumulator | None = None,
) -> dict[str, Any]:
    """Return {archetype, archetype_reason, archetype_confidence}.

    Never raises. On failure returns {archetype: None, reason: "...", confidence: 0}.
    """
    sector = (lead.get("sector") or "").strip()
    if sector not in VALID_ARCHETYPES:
        return {
            "archetype": None,
            "archetype_reason": f"sector '{sector}' heeft geen archetype-mapping",
            "archetype_confidence": 0.0,
        }

    company_name = (lead.get("company_name") or lead.get("domain") or "").strip()
    if not company_name:
        return {"archetype": None, "archetype_reason": "geen company_name", "archetype_confidence": 0.0}

    lead_id = lead.get("id")
    treatment_focus = lead.get("treatment_focus") or []
    google_category = (lead.get("google_category") or "").strip()
    has_instagram = bool(lead.get("has_instagram"))
    has_online_booking = bool(lead.get("has_online_booking"))
    company_size = (lead.get("company_size_estimate") or "").strip()
    text_capped = (page_text or "")[:3500]

    # Cost gate
    allowed, reason = await guarded_call(
        workspace_id=workspace_id, lead_id=lead_id,
        context="archetype_classify", estimated_cost_eur=_ESTIMATED_COST_EUR,
        supabase_client=supabase_client, accumulator=accumulator,
    )
    if not allowed:
        logger.info("archetype_classify gated for %s — %s", company_name, reason)
        return {"archetype": None, "archetype_reason": f"cost-gated: {reason}", "archetype_confidence": 0.0}

    # Cache key
    cache_key = hashlib.sha256(
        f"archetype::{sector}::{company_name}::{','.join(treatment_focus)}::{text_capped[:1000]}".encode("utf-8")
    ).hexdigest()
    try:
        cached = (
            supabase_client.table("claude_cache")
            .select("response_text")
            .eq("cache_key", cache_key)
            .maybe_single()
            .execute()
        )
        if cached and cached.data and cached.data.get("response_text"):
            try:
                parsed = json.loads(cached.data["response_text"])
                if isinstance(parsed, dict) and parsed.get("archetype"):
                    return parsed
            except (json.JSONDecodeError, ValueError):
                pass
    except Exception as e:
        logger.debug("archetype_classify: cache lookup failed: %s", e)

    valid_keys = sorted(VALID_ARCHETYPES[sector])
    archetype_explainer = "\n".join(
        f"- `{key}`: {_ARCHETYPE_DESCRIPTIONS_NL[key]}" for key in valid_keys
    )

    prompt = (
        f"Classificeer onderstaand bedrijf in één van de volgende koop-archetypes. "
        f"De archetype bepaalt HOE de eigenaar beslist (niet wat ze doen) — kies op "
        f"basis van toon van de website, type behandelingen, doelgroep, en commerciële stijl.\n\n"
        f"Mogelijke archetypes voor sector '{sector}':\n{archetype_explainer}\n\n"
        f"Bedrijf: {company_name}\n"
        f"Sector: {sector}\n"
        f"Treatment focus: {', '.join(treatment_focus) if treatment_focus else '(onbekend)'}\n"
        f"Google categorie: {google_category or '(onbekend)'}\n"
        f"Instagram aanwezig: {'ja' if has_instagram else 'nee'}\n"
        f"Online booking: {'ja' if has_online_booking else 'nee'}\n"
        f"Geschatte teamgrootte: {company_size or '(onbekend)'}\n\n"
        f'Website tekst (eerste 3500 tekens):\n"""\n{text_capped or "(geen tekst beschikbaar)"}\n"""\n\n'
        f"Return ALLEEN JSON (geen markdown, geen uitleg eromheen):\n"
        f'{{"archetype": "<key uit de lijst>", "reason": "<1 korte zin in NL>", "confidence": <0.0-1.0>}}\n\n'
        f"Regels:\n"
        f"- archetype MOET exact één van: {', '.join(valid_keys)}\n"
        f"- reason: max 20 woorden, concrete reden\n"
        f"- confidence: 0.9+ alleen als signalen sterk eenduidig zijn"
    )

    try:
        response = await anthropic_client.messages.create(
            model=_HAIKU_MODEL,
            max_tokens=200,
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        logger.warning("archetype_classify Claude call failed for %s: %s", company_name, e)
        return {"archetype": None, "archetype_reason": f"claude error: {e}", "archetype_confidence": 0.0}

    raw = response.content[0].text.strip() if response.content else ""
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
    first_brace = raw.find("{")
    last_brace = raw.rfind("}")
    if first_brace >= 0 and last_brace > first_brace:
        raw = raw[first_brace:last_brace + 1]

    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("archetype_classify JSON parse failed for %s: %s (raw=%r)", company_name, e, raw[:200])
        return {"archetype": None, "archetype_reason": "json parse failed", "archetype_confidence": 0.0}

    archetype_key = (parsed.get("archetype") or "").strip()
    if archetype_key not in VALID_ARCHETYPES[sector]:
        logger.warning("archetype_classify returned invalid key %r for sector %s", archetype_key, sector)
        return {
            "archetype": None,
            "archetype_reason": f"invalid archetype '{archetype_key}'",
            "archetype_confidence": 0.0,
        }

    result = {
        "archetype": archetype_key,
        "archetype_reason": (parsed.get("reason") or "").strip()[:200],
        "archetype_confidence": max(0.0, min(1.0, float(parsed.get("confidence") or 0.5))),
    }

    # Cost accounting
    input_tokens = getattr(response.usage, "input_tokens", 0) or 0
    output_tokens = getattr(response.usage, "output_tokens", 0) or 0
    cost_eur = round(
        (input_tokens * _COST_PER_M_INPUT + output_tokens * _COST_PER_M_OUTPUT) / 1_000_000, 6,
    )
    try:
        await log_api_cost(
            model=_HAIKU_MODEL, input_tokens=input_tokens, output_tokens=output_tokens,
            cost_eur=cost_eur, workspace_id=workspace_id, supabase_client=supabase_client,
            context="archetype_classify", lead_id=lead_id,
        )
    except Exception as e:
        logger.debug("archetype_classify cost log failed: %s", e)
    if accumulator is not None:
        accumulator.charge(cost_eur, "archetype_classify")

    # Cache
    try:
        supabase_client.table("claude_cache").upsert({
            "cache_key": cache_key,
            "model": _HAIKU_MODEL,
            "prompt_hash": cache_key,
            "response_text": json.dumps(result),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        }, on_conflict="cache_key").execute()
    except Exception as e:
        logger.debug("archetype_classify cache store failed: %s", e)

    logger.info(
        "archetype_classify: %s → %s (conf=%.2f) cost=EUR %.6f",
        company_name, result["archetype"], result["archetype_confidence"], cost_eur,
    )
    return result
