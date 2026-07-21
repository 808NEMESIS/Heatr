"""
scripts/prepare_test_launch.py — de kleinst mogelijke testlancering KLAARZETTEN.

Maakt idempotent één TESTLEAD (is_test_lead=true, Sami's eigen adres) met een
realistisch conceptsite-profiel, en bewijst via een DRY-RENDER dat de Fase A-
sequence end-to-end rendert. VERSTUURT NIETS en zet GEEN kill-switch om.

De testlead passeert de kliniek-gates bewust (is_test_lead-bypass) maar kan nooit
een echte lead raken. Draai:  python3 scripts/prepare_test_launch.py
"""
from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from config.database import get_heatr_supabase
from config.sequence_templates import faseA_brug_for, pick_brug
from campaigns.sequence_engine import render_faseA_marker

WORKSPACE = "aerys"
TEST_EMAIL = os.getenv("HEATR_TEST_EMAIL", "info@aeryssolution.nl")  # Sami's eigen adres
TEST_DOMAIN = "aeryssolution.nl"

# Realistisch conceptsite-profiel zodat mail 1 een echte gepersonaliseerde mail
# rendert (opener + naam + brug). is_test_lead=true → passeert de kliniek-gates.
TEST_LEAD = {
    "workspace_id": WORKSPACE,
    "is_test_lead": True,
    "gdpr_safe": True,
    "status": "enriched",
    "email": TEST_EMAIL,
    "email_status": "valid",
    "email_verification_method": "bouncer_api",
    "sector": "cosmetische_behandelaars",
    "company_name": "Testkliniek (interne test)",
    "domain": TEST_DOMAIN,
    "city": "Groningen",
    "contact_first_name": "Sami",
    "personalized_opener": "Jullie staan met 63 reviews op een 4,8 in Groningen. "
                           "Dat soort consistentie bouw je niet op met standaardwerk.",
    "google_review_count": 63,
    "google_rating": 4.8,
    "website_score": 34,   # < 49 → conceptsite-brug
    "score": 60,
    "has_online_booking": False,
    "has_whatsapp": False,
}


async def main() -> int:
    sb = get_heatr_supabase()

    # idempotent: bestaat de testlead al (op email + is_test_lead)?
    ex = (sb.table("leads").select("id").eq("workspace_id", WORKSPACE)
          .eq("email", TEST_EMAIL).eq("is_test_lead", True).limit(1).execute()).data
    if ex:
        lead_id = ex[0]["id"]
        sb.table("leads").update(TEST_LEAD).eq("id", lead_id).execute()
        print(f"Testlead bijgewerkt: {lead_id}")
    else:
        lead_id = (sb.table("leads").insert(TEST_LEAD).execute()).data[0]["id"]
        print(f"Testlead aangemaakt: {lead_id}")

    lead = (sb.table("leads").select("*").eq("id", lead_id).limit(1).execute()).data[0]
    brug = faseA_brug_for(pick_brug(lead))
    print(f"brug: {brug}  |  e-mail: {lead['email']}  |  is_test_lead: {lead['is_test_lead']}")

    print("\n=== DRY-RENDER (exact wat een launch zou versturen — NIETS verzonden) ===")
    for i in range(3):
        marker = {"faseA_brug": brug, "faseA_step": i}
        out = await render_faseA_marker(marker, lead, sb, WORKSPACE)
        print(f"\n----- MAIL {i+1} | {out['subject']} -----\n{out['body']}")

    print("\n" + "=" * 66)
    print("KLAAR. Niets verzonden, kill-switches ongemoeid.")
    print(f"Testlead-id voor de launch: {lead_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.get_event_loop().run_until_complete(main()))
