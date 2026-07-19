"""
scripts/verify_migrations.py — welke migraties staan ECHT in de prod-DB?

Migraties worden handmatig in de Supabase SQL-editor gedraaid (MCP heeft geen
prod-toegang), dus "gedraaid" is een aanname tot je het checkt. Deze tool
parseert elke migrations/*.sql op de tabellen (CREATE TABLE) en kolommen
(ALTER TABLE ADD COLUMN) die 'ie aanmaakt, en verifieert per stuk tegen de live DB.

    python3 scripts/verify_migrations.py            # alle migraties (uitgebreid)
    python3 scripts/verify_migrations.py 033 034    # alleen deze
    python3 scripts/verify_migrations.py --check    # compact + exit 1 bij DEELS/AFWEZIG
                                                    #   (voor ops/cron; pipeline_health draait dit mee)

READ-ONLY. Beperking: checkt tabel- en kolom-BESTAAN (de drift die we zagen),
niet elke DDL-vorm (indexes, constraints, functies, policies). Een tabel die
bestaat maar kolommen mist wordt als 'tabel OK' geteld — kolom-ALTERs worden wel
los gecheckt.

2026-07-19: dekt ook het BASIS-schema (supabase_schema.sql). Dat was een blinde
vlek: heatr_gdpr_log stond alleen in het basis-schema, was nooit toegepast, en
ontsnapte aan deze check — waardoor de GDPR-audittrail stil wegviel. Basis-
schema-tabellen zijn ongeprefixt gedefinieerd maar leven in prod als heatr_<naam>
(config/database.py-conventie); de check test de geprefixte naam.
"""
from __future__ import annotations

import glob
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

MIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "migrations")

# De optionele (?:schema\.) vangt `public.heatr_x` — anders pakt de regex 'public'.
RE_CREATE = re.compile(
    r"create\s+table\s+(?:if\s+not\s+exists\s+)?(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)", re.I)
RE_ALTER = re.compile(
    r"alter\s+table\s+(?:if\s+exists\s+)?(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)"
    r"\s+add\s+column\s+(?:if\s+not\s+exists\s+)?([a-z_][a-z0-9_]*)",
    re.I,
)


def _strip_comments(sql: str) -> str:
    # verwijder -- regelcommentaar zodat uitgecommentarieerde DDL niet meetelt
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _raw_client():
    from supabase import create_client
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)


def main() -> int:
    args = sys.argv[1:]
    check_mode = "--check" in args
    args = [a for a in args if a != "--check"]
    files = sorted(glob.glob(os.path.join(MIG_DIR, "*.sql")))
    if args:
        files = [f for f in files if any(a in os.path.basename(f) for a in args)]

    sb = _raw_client()

    def table_ok(t: str) -> bool:
        try:
            sb.table(t).select("*").limit(1).execute(); return True
        except Exception:
            return False

    def col_ok(t: str, c: str) -> bool:
        try:
            sb.table(t).select(c).limit(1).execute(); return True
        except Exception:
            return False

    # Basis-schema meenemen (2026-07-19): supabase_schema.sql definieert tabellen
    # ongeprefixt; prod draagt de heatr_-prefix. We checken de geprefixte naam.
    base_schema = os.path.join(os.path.dirname(MIG_DIR), "supabase_schema.sql")
    if os.path.exists(base_schema) and (not args or any(a in "supabase_schema.sql" for a in args)):
        files = [base_schema] + files

    fully, partial, empty = [], [], []
    for f in files:
        name = os.path.basename(f)
        is_base = name == "supabase_schema.sql"
        sql = _strip_comments(open(f, encoding="utf-8").read())
        tables = sorted(set(RE_CREATE.findall(sql)))
        if is_base:
            tables = [t if t.startswith("heatr_") else f"heatr_{t}" for t in tables]
        cols = sorted(set(RE_ALTER.findall(sql))) if not is_base else []
        if not tables and not cols:
            continue  # geen tabel/kolom-DDL om te checken (bv. pure data/index-migratie)

        checks = []
        for t in tables:
            checks.append((f"table {t}", table_ok(t)))
        for t, c in cols:
            checks.append((f"{t}.{c}", col_ok(t, c)))
        ok = sum(1 for _, v in checks if v)
        total = len(checks)
        status = "VOLLEDIG" if ok == total else ("DEELS" if ok else "AFWEZIG")
        (fully if ok == total else (empty if ok == 0 else partial)).append(name)
        if not check_mode:
            print(f"\n[{status}] {name}  ({ok}/{total})")
            for label, v in checks:
                if not v:
                    print(f"    ONTBREEKT: {label}")
        elif ok != total:
            missing = [label for label, v in checks if not v]
            print(f"[{status}] {name}: {', '.join(missing)}")

    if not check_mode:
        print("\n" + "=" * 50)
    print(f"VOLLEDIG: {len(fully)}   DEELS: {len(partial)}   AFWEZIG: {len(empty)}")
    if partial and not check_mode:
        print("DEELS toegepast:", ", ".join(partial))
    if empty and not check_mode:
        print("NIET toegepast:", ", ".join(empty))
    # --check: exit 1 zodra er iets DEELS of AFWEZIG is — voor cron/ops-gebruik,
    # zodat "selectief toegepaste migraties" (de 033-les) niet stil blijft.
    if check_mode and (partial or empty):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
