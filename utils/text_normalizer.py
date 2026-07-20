"""
utils/text_normalizer.py — normalisatie + validatie van vrije Claude-tekst
(remediation P1/C2).

De productie sloeg ruwe Claude-output ongeschoond op → 88% van de openers
bevatte markdown-headers, bold-markup, meta-labels ('# Openingszin:',
'**Beste X,**') of code-fences → niet plak-klaar in de mail. Deze laag
schoont die op en KEURT corrupte output af (refusal/leeg/te lang) i.p.v.
'm stil op te slaan.

Gebruikt door personalized_opener, company_summary en andere vrije
tekstvelden die direct in UI/outbound belanden.
"""
from __future__ import annotations

import re

# Refusal-signaturen — Claude weigerde; NIET als geldig veld opslaan.
_REFUSAL_MARKERS = (
    "ik kan hierbij niet helpen", "ik kan je hier niet mee helpen",
    "i can't help", "i cannot help", "i'm unable to", "as an ai",
    "als ai-", "als een ai", "ik kan geen", "sorry, ik kan",
)
# Meta-label-preambles die een eerste regel kunnen vormen.
_LABEL_RE = re.compile(
    r"^\s*(#{1,6}\s*)?(\*+\s*)?(openingszin|opener|openingregel|onderwerp|"
    r"subject|antwoord|email|e-mail|mail)\s*[:\-–]?\s*\**\s*$",
    re.IGNORECASE,
)
_AANHEF_RE = re.compile(
    r"^\s*(\*+\s*)?(beste|hoi|hallo|geachte|dag)\b.*$", re.IGNORECASE,
)
# Conversationele preamble: korte kop-regel die eindigt op ':' en een generator-
# woord bevat ('Hier is een openingszin voor je email:', '**Mogelijke openingszin:**').
# Leidende/trailing emphasis (*/_/#) en quotes worden getolereerd.
_PREAMBLE_RE = re.compile(
    r"^\s*[*_#]*\s*.{0,60}\b(openingszin|opener|voorstel|e-?mail|mail)\b.{0,30}:"
    r"[*_\"''\s]*$",
    re.IGNORECASE,
)
# Horizontale regel (--- *** ___) die Claude als scheider tussen preamble en tekst zet.
_HR_RE = re.compile(r"^\s*([-*_]\s*){3,}$")


# --- Opener-QA (fail-closed) -------------------------------------------------
# Titels die geen aanhef/naam zijn.
_OPENER_TITLES = {"dr", "dr.", "drs", "drs.", "ir", "ir.", "prof", "prof.", "mr", "mr."}
# Kliniek-stem: de opener is geschreven vanuit Aerys náár de prospect; als hij de
# stem van de prospect aanneemt ('onze cliënten', 'hebben we', 'in gesprek over
# een samenwerking') is dat een fout.
_CLINIC_VOICE_RE = re.compile(
    r"\bonze (cli[eë]nt|pati[eë]nt|klant)|\bhebben we\b|\bwij bieden\b|"
    r"\bwaarom we\b|\bwe graag\b|\bons bedrijf\b|in gesprek (gaan|over een samenwerking)",
    re.IGNORECASE,
)


# Ongetoetste observatie over de website/online-aanwezigheid van de prospect.
# 'jullie/uw site' etc. — NIET 'hun online aanwezigheid' (over concurrenten = ok).
_WEBSITE_CLAIM_RE = re.compile(
    r"(zag ik|viel me op|jullie (site|website|online presence|online aanwezigheid|pagina)|"
    r"uw (site|website|online)|geen (chatbot|whatsapp|online.?boeking)|verouderd|"
    r"niet mobiel|laadt traag)",
    re.IGNORECASE,
)


def strip_unverified_claims(text: str | None) -> str:
    """Verwijder zinnen met een ONGETOETSTE claim over de website/online-
    aanwezigheid van de prospect. Claude kan die verzinnen; ze horen uitsluitend
    uit de geverifieerde pick_safe_observation-route. De review-gegronde zinnen
    (feiten) blijven staan.
    """
    t = (text or "").strip()
    if not t:
        return t
    sents = re.split(r"(?<=[.!?])\s+", t)
    kept = [s for s in sents if not _WEBSITE_CLAIM_RE.search(s)]
    return " ".join(kept).strip()


def validate_opener_sendable(text: str | None) -> tuple[bool, str]:
    """Fail-closed QA op een REEDS GENORMALISEERDE opener.

    Voorkomt dat structureel-kapotte output stil als verzendbaar veld wordt
    opgeslagen (root cause 2026-07-15: 88% afgekapte openers gingen ongemerkt
    door). Returns (ok, reason); ok=False → NIET opslaan als productie-opener.
    reason ∈ ok | too_short | truncated | title_as_greeting | clinic_voice | em_dash.
    """
    t = (text or "").strip()
    if len(t) < 15 or len(t.split()) < 4:
        return False, "too_short"
    if t[-1] not in ".!?\"”’)":
        return False, "truncated"
    firstword = t.split()[0].strip().rstrip(",").lower()
    if firstword in _OPENER_TITLES:
        return False, "title_as_greeting"
    if _CLINIC_VOICE_RE.search(t):
        return False, "clinic_voice"
    # Em-/en-dash verboden (outreach spec 1, 2026-07-20): de opener-prompt verbiedt
    # ze al, maar 9% (86/954) glipte er doorheen omdat de gate ze niet toetste.
    # Afgekeurde openers → scripts/regenerate_openers.py.
    if "—" in t or "–" in t:
        return False, "em_dash"
    return True, "ok"


def _strip_inline_markup(line: str) -> str:
    line = re.sub(r"^#{1,6}\s*", "", line)          # headers
    line = re.sub(r"\*\*(.+?)\*\*", r"\1", line)      # **bold**
    line = re.sub(r"__(.+?)__", r"\1", line)          # __bold__
    line = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"\1", line)  # *italic*
    line = re.sub(r"`+", "", line)                    # inline code
    # Onafgesloten/over-regels-lopende bold-markers (bv. Claude opent '**' maar
    # sluit pas veel later of niet) laat de gepaarde regex staan → hard weg.
    line = line.replace("**", "").replace("__", "")
    # Losse emphasis-tekens aan het regelbegin (orphan '*'/'_').
    line = re.sub(r"^\s*[*_]+\s*", "", line)
    return line.strip()


def normalize_generated_text(
    raw: str | None, *, max_sentences: int | None = None, max_chars: int = 1200,
) -> tuple[str, bool, str]:
    """Schoon Claude-vrijetekst en valideer.

    Returns:
        (cleaned, ok, reason). ok=False → NIET opslaan als productieveld
        (bewaar raw hooguit apart voor debug). reason beschrijft de uitkomst
        ('ok' | 'empty' | 'refusal' | 'empty_after_clean').
    """
    if not raw or not raw.strip():
        return "", False, "empty"

    low = raw.strip().lower()
    if any(m in low for m in _REFUSAL_MARKERS):
        return "", False, "refusal"

    text = raw.strip()
    # code-fences (```lang ... ```) verwijderen
    text = re.sub(r"```[a-zA-Z]*\n?", "", text)
    text = text.replace("```", "")

    # regel-voor-regel: strip meta-label/aanhef-preamble-regels aan de KOP
    lines = [ln.rstrip() for ln in text.split("\n")]
    while lines:
        first = lines[0].strip()
        # Een markdown-header (# ...) aan de kop is nooit geldige prozatekst
        # voor een opener/summary → droppen. Idem lege regels, meta-labels
        # ('Openingszin:'), aanhef-preambles ('Beste X,'), conversationele
        # preambles ('Hier is een openingszin:') en horizontale regels.
        if (not first or first.startswith("#")
                or _LABEL_RE.match(first) or _AANHEF_RE.match(first)
                or _PREAMBLE_RE.match(first) or _HR_RE.match(first)):
            lines.pop(0)
            continue
        break
    # inline-markup per resterende regel strippen
    lines = [_strip_inline_markup(ln) for ln in lines]
    cleaned = "\n".join(ln for ln in lines).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)  # collapse lege regels
    # Wrap-quotes rond de hele opener ("...") — strip openings-/sluitquote.
    cleaned = cleaned.strip().strip('"').strip("'").strip('""').strip("''").strip()

    if not cleaned:
        return "", False, "empty_after_clean"

    if max_sentences:
        parts = re.split(r"(?<=[.!?])\s+", cleaned)
        if len(parts) > max_sentences:
            cleaned = " ".join(parts[:max_sentences]).strip()

    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars].rsplit(" ", 1)[0].strip()

    return cleaned, True, "ok"
