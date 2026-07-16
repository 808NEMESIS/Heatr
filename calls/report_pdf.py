"""Check-up rapport: HTML -> PDF -> Supabase Storage (signed URL).

Warmr kan geen bijlage sturen (client is application/json-only, geen attachment-
parameter). Het rapport gaat daarom als link in de mail-body: we renderen het
reeds goedgekeurde `report_html` naar een PDF met de al aanwezige Chromium
(Playwright is hier een kern-dependency, geen nieuwe systeem-libs zoals
weasyprint die zou vergen), uploaden die naar Supabase Storage en geven een
SIGNED URL met verloop terug. Signed (niet publiek) want een check-up over een
specifieke kliniek is semi-gevoelig en hoort niet permanent open op een raadbare
URL.

De PDF is altijd reproduceerbaar uit `report_html` (dat is persistent op de
call-record) -> we bewaren de vervallende URL niet; de retarget re-rendert.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# Droge, print-gerichte CSS. Geen CDN-fonts (server-side niet beschikbaar en de
# schrijfregels willen onderdrijving, geen designdeck): veilige systeem-stack,
# A4, ruime marges, zwart-op-wit met een enkele stille accentlijn.
_PDF_CSS = """
  @page { size: A4; margin: 22mm 20mm; }
  * { box-sizing: border-box; }
  body { font-family: Georgia, 'Times New Roman', serif; color: #1a1a1a;
         font-size: 11.5pt; line-height: 1.5; }
  .checkup-report > header { border-bottom: 1px solid #cfcfcf; padding-bottom: 10px;
         margin-bottom: 22px; }
  .eyebrow { font-family: 'Helvetica Neue', Arial, sans-serif; font-size: 8.5pt;
         letter-spacing: 0.14em; text-transform: uppercase; color: #7a7a7a;
         margin: 0 0 4px; }
  h1 { font-size: 18pt; font-weight: 600; margin: 0; }
  .loc { color: #6a6a6a; font-size: 10.5pt; margin: 2px 0 0; }
  .findings { display: block; }
  .finding { padding: 14px 0; border-bottom: 1px solid #ececec; }
  .finding:last-child { border-bottom: none; }
  .finding h2 { font-size: 12.5pt; font-weight: 600; margin: 0 0 6px; }
  .finding .fact { margin: 0 0 4px; }
  .finding .cost { margin: 0; color: #444; }
"""


def wrap_report_document(report_html: str, company_name: str) -> str:
    """Wikkel het `report_html`-fragment tot een volledig, self-contained HTML-doc.

    Args:
        report_html: het <article class="checkup-report">-fragment uit
            report_generator._render_report_html.
        company_name: voor de <title> (niet zichtbaar in de PDF-body).

    Returns:
        Complete HTML-string, klaar voor Chromium set_content.
    """
    title = (company_name or "Check-up").replace("<", "").replace(">", "")
    return (
        "<!doctype html>\n<html lang=\"nl\">\n<head>\n"
        "<meta charset=\"utf-8\">\n"
        f"<title>Check-up {title}</title>\n"
        f"<style>{_PDF_CSS}</style>\n"
        "</head>\n<body>\n"
        f"{report_html}\n"
        "</body>\n</html>"
    )


async def render_report_pdf(report_html: str, company_name: str) -> bytes:
    """Render `report_html` naar PDF-bytes met headless Chromium.

    Eigen minimale launch (altijd headless: page.pdf() werkt niet headful) i.p.v.
    de anti-detectie-context uit playwright_helpers, want we renderen lokale HTML,
    geen externe site.

    Raises:
        RuntimeError bij een lege render of een Playwright-fout (caller vangt af
        en laat report_status ongewijzigd -> fail-closed).
    """
    from playwright.async_api import async_playwright

    full_html = wrap_report_document(report_html, company_name)
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            try:
                page = await browser.new_page()
                await page.set_content(full_html, wait_until="load")
                pdf_bytes = await page.pdf(
                    format="A4",
                    print_background=True,
                    margin={"top": "0", "bottom": "0", "left": "0", "right": "0"},
                )
            finally:
                await browser.close()
    except Exception as e:  # noqa: BLE001 - alles omzetten naar één faaltype
        raise RuntimeError(f"PDF-render faalde: {e}") from e

    if not pdf_bytes:
        raise RuntimeError("PDF-render gaf lege bytes terug")
    return pdf_bytes


async def upload_report_pdf(
    pdf_bytes: bytes,
    call_id: str,
    supabase_client,
    *,
    expires_in_days: int | None = None,
) -> str:
    """Upload de PDF naar Supabase Storage en geef een SIGNED URL met verloop terug.

    Object-key: `checkup-reports/{call_id}.pdf` (upsert -> re-render overschrijft).
    Bucket: SUPABASE_STORAGE_BUCKET (hergebruikt de screenshots-bucket).

    Args:
        pdf_bytes: de gerenderde PDF.
        call_id: call-record-id (bepaalt het pad, idempotent).
        supabase_client: supabase-py client.
        expires_in_days: verloop; default env CHECKUP_REPORT_URL_TTL_DAYS of 30.

    Returns:
        Signed URL-string (volledige URL).

    Raises:
        RuntimeError bij upload- of signing-fout.
    """
    if expires_in_days is None:
        expires_in_days = int(os.getenv("CHECKUP_REPORT_URL_TTL_DAYS", "30"))
    expires_in = max(1, expires_in_days) * 86400

    bucket = os.getenv("SUPABASE_STORAGE_BUCKET", "screenshots")
    object_key = f"checkup-reports/{call_id}.pdf"

    up = supabase_client.storage.from_(bucket).upload(
        path=object_key,
        file=pdf_bytes,
        file_options={"content-type": "application/pdf", "upsert": "true"},
    )
    if hasattr(up, "error") and up.error:
        raise RuntimeError(f"PDF-upload faalde ({object_key}): {up.error}")

    signed = supabase_client.storage.from_(bucket).create_signed_url(object_key, expires_in)
    url = None
    if isinstance(signed, dict):
        url = signed.get("signedURL") or signed.get("signedUrl") or signed.get("signed_url")
    elif signed is not None:
        url = getattr(signed, "signedURL", None) or getattr(signed, "signed_url", None)
    if not url:
        raise RuntimeError(f"Signed-URL genereren faalde ({object_key}): {signed!r}")

    # Supabase geeft soms een pad (/storage/v1/object/sign/...) i.p.v. volledige URL.
    if url.startswith("/"):
        base = os.getenv("SUPABASE_URL", "").rstrip("/")
        url = f"{base}{url}"
    return url
