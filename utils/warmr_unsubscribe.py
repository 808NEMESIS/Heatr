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


def verify_and_flag(
    raw_supabase: Any, heatr_supabase: Any, *,
    workspace_id: str, lead_id: str, campaign_id: str | None = None,
) -> tuple[bool, str]:
    """Post-send-verificatie MET harde vlag. Ontbreekt de unsubscribe_tokens-rij, dan
    zet dit een persistente compliance-vlag (heatr_compliance_flags, migratie 044) —
    geen logregel — die de drip-pre-gate (assert_no_open_flags) laat blokkeren.

    raw_supabase = ONgeprefixt (unsubscribe_tokens = Warmr's tabel);
    heatr_supabase = de heatr_-wrapper (schrijft de vlag).
    Returns (ok, detail); ok=False betekent: vlag gezet, drip moet stoppen."""
    from utils.compliance_flags import FLAG_MISSING_UNSUBSCRIBE, raise_flag
    ok, detail = unsubscribe_token_present(raw_supabase, lead_id, campaign_id)
    if not ok:
        raise_flag(
            heatr_supabase, workspace_id=workspace_id,
            flag_type=FLAG_MISSING_UNSUBSCRIBE, lead_id=lead_id,
            campaign_id=campaign_id, detail=detail,
        )
    return ok, detail
