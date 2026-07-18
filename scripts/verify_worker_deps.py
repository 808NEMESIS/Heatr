"""
scripts/verify_worker_deps.py — draaien de workers met complete dependencies?

Aanleiding (schuld, Notion-bouwlog): pillow stond in requirements.txt (10.3.0)
maar was NIET geïnstalleerd in de productie-interpreter (de CommandLineTools
Python 3.9 die de launchd-plists direct aanroepen, zonder venv). De fix was een
handmatige --user-install — buiten de requirements-flow. Dat breekt stil bij een
herinstall of een andere host: screenshot-capture faalt dan fail-soft en
niemand merkt het.

Dit script importeert de runtime-kritische packages met de INTERPRETER WAARMEE
HET DRAAIT en faalt luid (exit 1) bij elk gat. Draai het dus met dezelfde
python3 als de plists:

    /Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/bin/python3 \
        scripts/verify_worker_deps.py

Bij een verse host/herinstall: eerst
    <die python3> -m pip install --user -r requirements.txt
    <die python3> -m playwright install chromium
en dan dit script als bewijs dat alles er staat.
"""
from __future__ import annotations

import importlib
import sys

# (import-naam, pip-naam, waarom kritisch)
CRITICAL = [
    ("supabase", "supabase", "alle DB-toegang"),
    ("httpx", "httpx", "alle HTTP"),
    ("anthropic", "anthropic", "Claude-calls (openers, sector, Vision)"),
    ("playwright", "playwright", "browser: scraping + screenshots"),
    ("PIL", "pillow", "WebP-encoding van screenshots — faalt fail-soft indien afwezig!"),
    ("dotenv", "python-dotenv", ".env-loading in elke worker"),
    ("fastapi", "fastapi", "API"),
    ("jwt", "pyjwt", "browser-auth"),
    ("dns", "dnspython", "MX-checks"),
]


def main() -> int:
    print(f"interpreter: {sys.executable}")
    missing = []
    for mod, pip_name, why in CRITICAL:
        try:
            importlib.import_module(mod)
            print(f"  OK       {pip_name}")
        except ImportError:
            missing.append((pip_name, why))
            print(f"  ONTBREEKT {pip_name}  <- {why}")
    if missing:
        print(f"\n{len(missing)} package(s) missen in deze interpreter. Installeer met:")
        print(f"  {sys.executable} -m pip install --user " + " ".join(p for p, _ in missing))
        return 1
    print("\nalle runtime-kritische dependencies aanwezig.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
