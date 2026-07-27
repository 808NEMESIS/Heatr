"""
scripts/set_legal_form.py — handmatige rechtsvorm-invoer per lead (AVG-02).

Zet kvk_legal_form op leads na handmatig opzoeken in het openbare KvK-zoekregister
(gratis). De gate (utils/legal_form) mapt BV/NV/stichting → door, eenmanszaak/zzp/
vof → geblokkeerd. Vereist migratie 043 (kvk_legal_form-kolom).

Invoer: een CSV `domain,rechtsvorm` (of TSV), bv:
    joostkroon.com,BV
    kliniekvrijdag.nl,eenmanszaak
Gebruik:
    python3 scripts/set_legal_form.py rechtsvorm_32.csv            # dry-run
    python3 scripts/set_legal_form.py rechtsvorm_32.csv --apply
"""
from __future__ import annotations
import argparse, csv, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv; load_dotenv(str(Path(__file__).resolve().parent.parent / ".env"))

WORKSPACE = "aerys"


def main(path: str, apply: bool = False) -> int:
    from config.database import get_heatr_supabase
    from utils.legal_form import classify_legal_form
    sb = get_heatr_supabase()
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.reader(f):
            if len(r) >= 2 and r[0].strip() and not r[0].strip().startswith("#"):
                rows.append((r[0].strip().lower(), r[1].strip()))
    print(f"{len(rows)} rechtsvorm-invoeren — {'APPLY' if apply else 'DRY-RUN'}\n")
    done = 0
    for domain, vorm in rows:
        cls = classify_legal_form({"kvk_legal_form": vorm})
        gate = {"rechtspersoon": "DOOR", "natuurlijk_persoon": "GEBLOKKEERD", "onbepaald": "GEBLOKKEERD(onbepaald)"}[cls]
        lead = (sb.table("leads").select("id,company_name").eq("workspace_id", WORKSPACE)
                .eq("domain", domain).limit(1).execute()).data
        if not lead:
            print(f"  {domain:34} '{vorm}' → {cls} ({gate})  ⚠ geen lead met dit domein"); continue
        print(f"  {domain:34} '{vorm}' → {cls} ({gate})  {lead[0].get('company_name','')[:22]}")
        if apply:
            sb.table("leads").update({"kvk_legal_form": vorm}).eq("id", lead[0]["id"]).execute()
            done += 1
    print(f"\n{'Geschreven: '+str(done) if apply else 'Dry-run — niets geschreven'}")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("csv"); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    raise SystemExit(main(a.csv, a.apply))
