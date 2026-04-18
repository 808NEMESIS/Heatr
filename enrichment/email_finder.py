"""
enrichment/email_finder.py — Pattern-based email candidate generator.

Step 2 of the 4-step email waterfall. Runs when the website scraper (step 1)
found no deliverable emails. Generates all plausible role and name-based email
patterns for a domain, ordered by likelihood. Verification happens downstream
in email_verifier.py — this file only generates candidates.

Order contract (maintained throughout):
  1. Role emails (highest deliverability, GDPR-safe)
  2. First-name-only patterns
  3. First+last combinations
  4. Last-name-only (lowest confidence)
"""

from __future__ import annotations

import re
from typing import Sequence

# ---------------------------------------------------------------------------
# Role email prefixes — ordered by likelihood for Dutch SMB
# (info@ is the single most common business email in NL)
# ---------------------------------------------------------------------------

_ROLE_PREFIXES_ORDERED: list[str] = [
    "info",
    "contact",
    "hallo",
    "hello",
    "praktijk",
    "kliniek",
    "receptie",
    "administratie",
    "afspraken",
    "afspraak",
    "service",
    "mail",
    "post",
    "team",
    "bureau",
    "office",
    "algemeen",
    "secretariaat",
    "intake",
    "behandeling",
    "zorg",
    "boekhouding",
    "planning",
]

# Characters that are not allowed in the local part of an email (strict)
_INVALID_LOCAL_CHARS = re.compile(r"[^a-z0-9._%+\-]")


# =============================================================================
# Public API
# =============================================================================

async def generate_email_candidates(
    domain: str,
    first_name: str | None,
    last_name: str | None,
    tussenvoegsel: str | None,
) -> list[str]:
    """Generate all plausible email address candidates for a domain.

    Generates role patterns first, then name-based patterns if name data is
    available. All candidates are lowercased, normalised, and deduplicated.
    Ordering: role emails → first-name patterns → first+last → last only.

    Args:
        domain: Clean domain string, e.g. 'example.nl'. No protocol.
        first_name: Contact first name or None.
        last_name: Contact last name or None.
        tussenvoegsel: Dutch name particle (e.g. 'van den') or None.

    Returns:
        Deduplicated list of candidate email strings, ordered by likelihood.
        All candidates pass basic format validation.
    """
    if not domain:
        return []

    domain = domain.lower().strip().lstrip("www.").rstrip("/")
    candidates: list[str] = []

    # 1. Role patterns — always generated
    candidates.extend(get_role_patterns(domain))

    # 2. Name patterns — only if at least first_name is known
    fn = _clean_name_part(first_name)
    ln = _clean_name_part(last_name)
    tv = _clean_tussenvoegsel(tussenvoegsel)

    if fn:
        # First-name only
        candidates.append(f"{fn}@{domain}")

    if fn and ln:
        candidates.extend(get_name_patterns(domain, fn, ln, tv or None))

    # 3. Deduplicate preserving order, validate format
    seen: set[str] = set()
    result: list[str] = []
    for candidate in candidates:
        candidate = candidate.lower().strip()
        if candidate not in seen and is_valid_email_format(candidate):
            seen.add(candidate)
            result.append(candidate)

    return result


def get_role_patterns(domain: str) -> list[str]:
    """Return the standard role email patterns for a domain.

    Generates one email per role prefix. These are GDPR-safe and have the
    highest deliverability because they are generic business addresses.

    Args:
        domain: Clean domain string, e.g. 'example.nl'.

    Returns:
        List of role email strings in priority order.
    """
    return [f"{prefix}@{domain}" for prefix in _ROLE_PREFIXES_ORDERED]


def get_name_patterns(
    domain: str,
    first_name: str,
    last_name: str,
    tussenvoegsel: str | None,
) -> list[str]:
    """Generate name-based email patterns for a known contact.

    Includes tussenvoegsel in patterns where it makes the address more likely,
    e.g. ``jan.van.den.berg@domain.nl``.

    Args:
        domain: Clean domain string, e.g. 'example.nl'.
        first_name: Cleaned first name (already lowercased, ASCII).
        last_name: Cleaned last name (already lowercased, ASCII).
        tussenvoegsel: Cleaned tussenvoegsel (e.g. 'vanden') or None.

    Returns:
        List of name-based email candidate strings, ordered by likelihood.
    """
    fn = first_name.lower()
    ln = last_name.lower()
    fi = fn[0] if fn else ""  # first initial

    # Tussenvoegsel without spaces (e.g. "van den" → "vanden")
    tv_compact = tussenvoegsel.replace(" ", "").replace("'", "") if tussenvoegsel else ""
    # Tussenvoegsel with dots (e.g. "van den" → "van.den")
    tv_dotted = tussenvoegsel.replace(" ", ".").replace("'", "") if tussenvoegsel else ""

    patterns: list[str] = []

    # --- First + last combinations (most common Dutch business patterns) ---
    patterns.append(f"{fn}.{ln}@{domain}")             # jan.smit@
    patterns.append(f"{fi}{ln}@{domain}")               # jsmit@
    patterns.append(f"{fi}.{ln}@{domain}")              # j.smit@
    patterns.append(f"{fn}{ln}@{domain}")               # jansmit@
    patterns.append(f"{ln}@{domain}")                   # smit@ (last only)
    patterns.append(f"{ln}.{fn}@{domain}")              # smit.jan@

    # --- Tussenvoegsel variants (only if tv known and non-empty) ---
    if tv_compact:
        patterns.append(f"{fn}.{tv_dotted}.{ln}@{domain}")    # jan.van.den.berg@
        patterns.append(f"{fn}{tv_compact}{ln}@{domain}")     # janvandenberg@
        patterns.append(f"{fi}.{tv_dotted}.{ln}@{domain}")    # j.van.den.berg@
        patterns.append(f"{fn}.{tv_compact}{ln}@{domain}")    # jan.vandenberg@

    return patterns


def is_valid_email_format(email: str) -> bool:
    """Validate an email address format without SMTP or DNS checks.

    Checks: contains exactly one @, local part is 1-64 chars, domain has at
    least one dot, no obvious invalid characters.

    Args:
        email: Email address string to validate.

    Returns:
        True if the format is plausibly valid.
    """
    if not email or "@" not in email:
        return False
    parts = email.split("@")
    if len(parts) != 2:
        return False
    local, domain = parts
    if not local or not domain:
        return False
    if len(local) > 64 or len(domain) > 253:
        return False
    if "." not in domain:
        return False
    # Domain must not start/end with a hyphen or dot
    if domain.startswith(("-", ".")) or domain.endswith(("-", ".")):
        return False
    # Local part: allow alphanumeric + . _ % + -
    if re.search(r"[^a-zA-Z0-9._%+\-]", local):
        return False
    # No consecutive dots
    if ".." in email:
        return False
    return True


# =============================================================================
# Internal helpers
# =============================================================================

def _clean_name_part(name: str | None) -> str:
    """Normalise a name string for use in email local parts.

    Lowercases, strips, converts accented characters to ASCII equivalents,
    removes characters not allowed in email local parts.

    Args:
        name: Raw name string or None.

    Returns:
        Cleaned ASCII string safe for use in email local part, or "".
    """
    if not name:
        return ""
    name = name.lower().strip()
    # Replace common Dutch/Belgian accented chars with ASCII equivalents
    replacements = {
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "ï": "i", "î": "i", "í": "i",
        "ü": "u", "ú": "u", "û": "u",
        "ö": "o", "ó": "o", "ô": "o",
        "ä": "a", "á": "a", "â": "a", "à": "a",
        "ç": "c", "ñ": "n",
    }
    for accented, plain in replacements.items():
        name = name.replace(accented, plain)
    # Remove anything still not alphanumeric (hyphens → remove, apostrophes → remove)
    name = re.sub(r"[^a-z0-9]", "", name)
    return name


def _clean_tussenvoegsel(tv: str | None) -> str:
    """Normalise a Dutch tussenvoegsel for use in email patterns.

    Lowercases, strips, collapses internal whitespace.

    Args:
        tv: Raw tussenvoegsel string or None.

    Returns:
        Normalised string (e.g. "van den") or "".
    """
    if not tv:
        return ""
    return " ".join(tv.lower().strip().split())
