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


def _strip_inline_markup(line: str) -> str:
    line = re.sub(r"^#{1,6}\s*", "", line)          # headers
    line = re.sub(r"\*\*(.+?)\*\*", r"\1", line)      # **bold**
    line = re.sub(r"__(.+?)__", r"\1", line)          # __bold__
    line = re.sub(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", r"\1", line)  # *italic*
    line = re.sub(r"`+", "", line)                    # inline code
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
        # ('Openingszin:') en aanhef-preambles ('Beste X,').
        if (not first or first.startswith("#")
                or _LABEL_RE.match(first) or _AANHEF_RE.match(first)):
            lines.pop(0)
            continue
        break
    # inline-markup per resterende regel strippen
    lines = [_strip_inline_markup(ln) for ln in lines]
    cleaned = "\n".join(ln for ln in lines).strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)  # collapse lege regels

    if not cleaned:
        return "", False, "empty_after_clean"

    if max_sentences:
        parts = re.split(r"(?<=[.!?])\s+", cleaned)
        if len(parts) > max_sentences:
            cleaned = " ".join(parts[:max_sentences]).strip()

    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars].rsplit(" ", 1)[0].strip()

    return cleaned, True, "ok"
