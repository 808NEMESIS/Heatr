"""utils/warmr_unsubscribe.py — POST-send-verificatie van de Warmr-afmeldlink.

Keuze B (Sami 2026-07-27): Warmr bezit de afmeldlink. Zijn campaign_scheduler roept
op het verzendmoment `generate_unsubscribe_link()` aan, die een rij in de tabel
`unsubscribe_tokens` schrijft (random token + lead_id + campaign_id), en plakt de
footer met die link onder de mail.

Belangrijk: die footer-stap is BEST-EFFORT — bij een fout logt Warmr alleen een
warning en verstuurt de mail ZONDER footer. En Heatr kan de uitgaande mail niet
pré-send inzien (Warmr stelt 'm ná de push samen). Daarom mag de gate de afmeld-eis
niet "aannemen": deze module VERIFIEERT ná de send dat de token-rij bestaat. Geen
rij → de best-effort-tak faalde → de mail ging mogelijk zonder afmeldlink → COMPLIANCE-
VLAG, niet stil doorlaten.

NB: `unsubscribe_tokens` is Warmr's tabel (GEEN heatr_-prefix). Geef daarom een
ONgeprefixte supabase-client mee (config.database.get_raw_supabase / create_client),
niet de heatr_-wrapper.
"""
from __future__ import annotations

from typing import Any


def unsubscribe_token_present(
    raw_supabase: Any, lead_id: str, campaign_id: str | None = None,
) -> tuple[bool, str]:
    """Bestaat er een unsubscribe_tokens-rij voor deze lead (+ campagne)?

    Returns (aanwezig, detail). Fail-CLOSED: een leesfout → (False, reason), zodat
    een onbereikbare tabel nooit als "footer aanwezig" telt.

    Aanwezig  → Warmr's generate_unsubscribe_link is gedraaid; de werkende afmeldlink
                is aangemaakt en (behoudens verzendfout) in de mail geplakt.
    Afwezig   → de best-effort-footerstap faalde of draaide niet → de uitgaande mail
                had mogelijk GEEN afmeldlink → compliance-vlag.
    """
    if not lead_id:
        return False, "geen lead_id meegegeven"
    try:
        q = (raw_supabase.table("unsubscribe_tokens")
             .select("id, token, used, campaign_id, created_at")
             .eq("lead_id", str(lead_id)))
        if campaign_id:
            q = q.eq("campaign_id", str(campaign_id))
        rows = q.order("created_at", desc=True).limit(1).execute().data or []
    except Exception as e:  # fail-closed: leesfout telt niet als "aanwezig"
        return False, f"unsubscribe_tokens niet leesbaar: {str(e)[:80]}"
    if not rows:
        scope = f" voor campagne {campaign_id}" if campaign_id else ""
        return False, f"geen unsubscribe_tokens-rij{scope} — Warmr-footer mogelijk niet aangemaakt"
    row = rows[0]
    tok = str(row.get("token") or "")
    return True, f"token aanwezig ({tok[:10]}…, used={row.get('used')})"
