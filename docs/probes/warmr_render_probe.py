"""
Empirische probe van Warmr's ECHTE render-pad (spintax_engine.process_content),
de exacte functie die campaign_scheduler.send_campaign_email aanroept om de
uitgaande subject+body samen te stellen (campaign_scheduler.py:860-861).

We reproduceren de sprint-4 A/B-test op codeniveau: wat komt er feitelijk uit
de render als Heatr custom_subject/custom_body meestuurt (belanden in
lead.custom_fields), afhankelijk van of de frozen template het token bevat.

Geen SMTP, geen scheduler — puur de body-compositie die bepaalt wat aankomt.
"""
import sys
sys.path.insert(0, "/Users/nemesis/warmr")

from spintax_engine import process_content

HEATR_CUSTOM_BODY = "DIT-IS-DE-HEATR-GERENDERDE-BODY"
HEATR_CUSTOM_SUBJECT = "DIT-IS-HET-HEATR-SUBJECT"

# Simuleer een Warmr-lead zoals die na een Heatr-push in de DB staat:
# Heatr's push_lead zet custom_subject/custom_body IN custom_fields.
lead = {
    "id": "studio-lumen-test",
    "email": "test@studiolumen.example",
    "first_name": "Lumen",
    "company": "Studio Lumen",
    "custom_fields": {
        "custom_subject": HEATR_CUSTOM_SUBJECT,
        "custom_body": HEATR_CUSTOM_BODY,
        "opener": "Heatr-opener-token",
    },
}

scenarios = [
    # naam, frozen template-body (uit sequence_steps)
    ("A1  template MET {{custom_body}}  (Heatr's gedocumenteerde syntax)",
     "Hallo {{first_name}},\n\n{{custom_body}}\n\nGroet"),
    ("A2  template MET {{custom:custom_body}}  (Warmr's echte syntax)",
     "Hallo {{first_name}},\n\n{{custom:custom_body}}\n\nGroet"),
    ("B   template ZONDER token, custom_body wel meegestuurd",
     "Hallo {{first_name}},\n\nVaste template-tekst, geen custom token.\n\nGroet"),
]

print("=" * 78)
print("WARMR RENDER-CONTRACT PROBE — process_content (het echte verzendpad)")
print("=" * 78)
print(f"Heatr custom_body meegestuurd: {HEATR_CUSTOM_BODY!r}\n")

for name, template in scenarios:
    # spintax_enabled=True, step_number=1 — zoals send_campaign_email
    out = process_content(template, lead, step_number=1, spintax_enabled=True)
    verbatim = HEATR_CUSTOM_BODY in out
    literal_token = "{{custom_body}}" in out
    print("-" * 78)
    print(name)
    print(f"  template : {template!r}")
    print(f"  RENDERED : {out!r}")
    print(f"  → Heatr-body aanwezig?      {verbatim}")
    print(f"  → letterlijk {{{{custom_body}}}} lek? {literal_token}")

print("-" * 78)
