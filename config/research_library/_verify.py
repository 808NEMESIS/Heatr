#!/usr/bin/env python3
"""
_verify.py — Research library validatie.

Loopt door alle markdown-files in config/research_library/, parst YAML-frontmatter,
en rapporteert stale claims, verbroken bronnen, schema-fouten en library-statistieken.

Gebruik: python3 _verify.py
"""
import os
import re
import sys
import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

LIBRARY_ROOT = Path(__file__).parent
STALE_THRESHOLD_DAYS = 180  # 6 maanden
REQUIRED_FIELDS_LIVE = ['id', 'status', 'stat_kort', 'attributie_kort', 'sample',
                        'bron_url', 'last_verified', 'ai_round_1', 'ai_round_2']
REQUIRED_FIELDS_AFGEWEZEN = ['id', 'status', 'afgewezen_reden', 'afgewezen_op']

def parse_yaml_blocks(content: str) -> list[dict]:
    """Extract YAML-frontmatter blocks from markdown content.

    Onze claims zitten als ```yaml...``` fenced code blocks met daarbinnen
    `---`-delimited frontmatter. We pakken eerst de ```yaml blocks en parsen
    de YAML-content binnenin. Markdown horizontal-rules (`---` op zichzelf)
    buiten yaml-fences worden zo niet per ongeluk gematcht.
    """
    blocks = []
    yaml_fence_re = re.compile(r"```yaml\s*\n(.*?)```", re.DOTALL)
    for fenced in yaml_fence_re.findall(content):
        # Strip surrounding --- delimiters als die er staan
        body = fenced.strip()
        if body.startswith("---"):
            body = body[3:].lstrip("\n")
        if body.endswith("---"):
            body = body[:-3].rstrip()
        block = _parse_yaml_simple(body)
        if block:
            blocks.append(block)
    return blocks

def _parse_yaml_simple(text: str) -> dict:
    """Minimal YAML parser: key: value of key: |\nmulti-line.
    Genoeg voor onze claim-frontmatter (geen geneste maps, geen lists die we hoeven te parsen).
    """
    block: dict = {}
    current_key = None
    current_value_lines: list[str] = []
    in_multiline = False

    for line in text.split("\n"):
        # Top-level key: detect via no-leading-whitespace + ":" present
        if not in_multiline and re.match(r"^[a-z_][a-z0-9_]*:", line):
            # Save previous multiline-key if any
            if current_key is not None:
                block[current_key] = "\n".join(current_value_lines).strip()
                current_value_lines = []
            key, _, value = line.partition(":")
            key = key.strip()
            value = value.strip()
            if value in ("|", ">"):
                in_multiline = True
                current_key = key
                current_value_lines = []
            else:
                block[key] = value
                current_key = None
        elif in_multiline:
            # Continuation of multiline if line starts with whitespace OR is empty
            if line.startswith(" ") or line.startswith("\t") or line.strip() == "":
                current_value_lines.append(line.strip())
            else:
                # End of multiline; save & re-process as new key
                block[current_key] = "\n".join(current_value_lines).strip()
                in_multiline = False
                current_key = None
                current_value_lines = []
                if re.match(r"^[a-z_][a-z0-9_]*:", line):
                    key, _, value = line.partition(":")
                    block[key.strip()] = value.strip()

    # Tail-flush
    if current_key is not None:
        block[current_key] = "\n".join(current_value_lines).strip()

    return block

def check_stale(claim: dict) -> bool:
    """True als claim stale is."""
    last_verified = claim.get('last_verified', '')
    if not last_verified:
        return True
    try:
        verified_date = datetime.date.fromisoformat(last_verified)
        age = (datetime.date.today() - verified_date).days
        return age > STALE_THRESHOLD_DAYS
    except ValueError:
        return True

def check_url(url: str, timeout: int = 5) -> tuple[bool, str]:
    """True als URL bereikbaar (2xx response)."""
    if not url or not url.startswith('http'):
        return False, 'invalid_url'
    try:
        req = Request(url, method='HEAD', headers={'User-Agent': 'HeatrLibraryVerify/1.0'})
        with urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300, str(resp.status)
    except HTTPError as e:
        return False, f'http_{e.code}'
    except URLError as e:
        return False, f'url_error_{e.reason}'
    except Exception as e:
        return False, f'error_{type(e).__name__}'

def main():
    issues: list[str] = []
    stats = {'LIVE': 0, 'KANDIDAAT': 0, 'AFGEWEZEN': 0}
    per_file: dict[str, dict[str, int]] = {}

    for md_file in sorted(LIBRARY_ROOT.rglob('*.md')):
        if md_file.name.startswith('_README'):
            continue
        with open(md_file) as f:
            content = f.read()
        blocks = parse_yaml_blocks(content)
        file_stats = {'LIVE': 0, 'KANDIDAAT': 0, 'AFGEWEZEN': 0}
        for block in blocks:
            status = (block.get('status') or '').upper()
            if status in stats:
                stats[status] += 1
                file_stats[status] += 1
            claim_id = block.get('id', '<unknown>')

            if status == 'LIVE':
                missing = [f for f in REQUIRED_FIELDS_LIVE if f not in block or not block.get(f)]
                if missing:
                    issues.append(f"[SCHEMA] {md_file.name}: {claim_id} mist velden: {missing}")
                if block.get('ai_round_1') != 'PASS' or block.get('ai_round_2') != 'PASS':
                    issues.append(f"[CRITICAL] {md_file.name}: {claim_id} is LIVE maar ai_round_1/2 niet PASS")
                if check_stale(block):
                    issues.append(f"[STALE] {md_file.name}: {claim_id} last_verified > 6 maanden of leeg")
                if '--check-urls' in sys.argv:
                    url = block.get('bron_url', '')
                    ok, status_code = check_url(url)
                    if not ok:
                        issues.append(f"[URL] {md_file.name}: {claim_id} bron_url issue ({status_code}): {url}")
            elif status == 'AFGEWEZEN':
                missing = [f for f in REQUIRED_FIELDS_AFGEWEZEN if f not in block or not block.get(f)]
                if missing:
                    issues.append(f"[SCHEMA] {md_file.name}: {claim_id} mist velden: {missing}")

        rel = md_file.relative_to(LIBRARY_ROOT)
        per_file[str(rel)] = file_stats

    # Rapport
    print("\n=== Heatr Research Library Verify ===\n")
    print("Totaal claims:")
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print("\nPer file:")
    for path, fs in per_file.items():
        if sum(fs.values()) == 0:
            continue
        print(f"  {path}: LIVE={fs['LIVE']} KANDIDAAT={fs['KANDIDAAT']} AFGEWEZEN={fs['AFGEWEZEN']}")
    print()

    if issues:
        print(f"Issues gevonden ({len(issues)}):")
        for issue in issues:
            print(f"  {issue}")
        sys.exit(1)
    else:
        print("Geen issues. Library is gezond.")
        sys.exit(0)

if __name__ == '__main__':
    main()
