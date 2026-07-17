"""
audit/nl_trust.py — Nederlandse trust-signalen uit de sitetekst.

Pure detectoren over page_text (enrichment_data source contact_crawl_v2) +
schema_org. Elke detector geeft bewijs terug (de gematchte snippet), want een
finding zonder bewijs is aanvechtbaar.

Conservatief waar het moet: een BIG-nummer is 11 cijfers MÉT context
("BIG-nummer"/"BIG-register") — een los 11-cijferig getal (KvK, telefoon,
IBAN-deel) is geen BIG-nummer.
"""
from __future__ import annotations

import re

# BIG: het woord BIG in registratie-context, met een 11-cijferig nummer dichtbij.
_BIG_CONTEXT = re.compile(
    r"BIG[\s\-]?(?:nummer|register|registratie|geregistreerd|reg\.?)", re.I)
_11_DIGITS = re.compile(r"\b(\d{11})\b")

# Keurmerken per sector (acroniemen als hele woorden).
_KEURMERKEN_COS = ["ZKN", "NVCG", "NVEPC"]
_KEURMERKEN_CHI = ["SCN", "NCA"]

_WKKGZ = re.compile(r"\b(wkkgz|klachtenregeling|klachtenfunctionaris|klachtenreglement)\b", re.I)
_GESCHIL = re.compile(r"\b(geschilleninstantie|geschillencommissie|geschillenregeling)\b", re.I)
# Erkende chiro-opleidingen (internationale, want NL heeft er geen).
_OPLEIDING_CHI = re.compile(r"\b(AECC|Anglo[\-\s]?European|Barcelona College|BCC Chiropractic|Odense|Syddansk)\b", re.I)


def _snippet(text: str, at: int, width: int = 60) -> str:
    lo = max(0, at - width // 2)
    return " ".join(text[lo:lo + width].split())


def find_big_numbers(text: str) -> list[dict]:
    """11-cijferige nummers die in BIG-context staan. Bewijs = de snippet."""
    if not text:
        return []
    out, seen = [], set()
    for m in _BIG_CONTEXT.finditer(text):
        # zoek een 11-cijferig nummer binnen 80 tekens na de BIG-context
        window = text[m.start():m.end() + 80]
        num = _11_DIGITS.search(window)
        if num and num.group(1) not in seen:
            seen.add(num.group(1))
            out.append({"number": num.group(1), "bewijs": _snippet(text, m.start())})
    return out


def _find_acronyms(text: str, acronyms: list[str]) -> list[dict]:
    out = []
    for a in acronyms:
        m = re.search(rf"\b{re.escape(a)}\b", text or "")
        if m:
            out.append({"keurmerk": a, "bewijs": _snippet(text, m.start())})
    return out


def find_keurmerken(text: str, sector: str) -> list[dict]:
    """Keurmerken per sector (ZKN/NVCG/NVEPC voor cosmetiek, SCN/NCA voor chiro)."""
    acr = _KEURMERKEN_CHI if sector == "chiropractoren" else _KEURMERKEN_COS
    return _find_acronyms(text, acr)


def _match(rx: re.Pattern, text: str) -> dict | None:
    m = rx.search(text or "")
    return {"bewijs": _snippet(text, m.start())} if m else None


def has_wkkgz(text: str) -> dict | None:
    return _match(_WKKGZ, text)


def has_geschilleninstantie(text: str) -> dict | None:
    return _match(_GESCHIL, text)


def has_erkende_opleiding_chiro(text: str) -> dict | None:
    return _match(_OPLEIDING_CHI, text)
