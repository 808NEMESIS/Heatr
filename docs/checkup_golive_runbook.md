# Go-live runbook — check-up follow-up

Alles is gebouwd en achter kill-switches. Dit is de exacte volgorde om te gaan
sturen, met de waarden die ik ken al ingevuld. Twee sporen: **A. rapport-sends**
(kan vandaag) en **B. Zoom-ingest** (vereist een publieke URL).

---

## ⚠️ Lees dit eerst — de gate staat half open

In je echte `.env` staat `ENABLE_CAMPAIGN_SENDS=true` en `ENABLE_PROSPECT_SENDS`
ontbreekt. De dispatcher valt dan terug op de legacy-switch, dus
`_prospect_sends_enabled()` is **nu al `True`**. De "dubbele" kill-switch is
daardoor feitelijk **enkel**: alleen `CHECKUP_REPORT_ENABLED` (leeg = uit) houdt
check-up-sends tegen. Zodra je die op `true` zet, kan een **non-dry-run** send
echt versturen. Daarom: altijd eerst `--dry-run`.

(Wil je de master-switch expliciet i.p.v. via de legacy-fallback? Zet dan
`ENABLE_PROSPECT_SENDS=true` of `=false` bewust; leeglaten = "true" hier.)

---

## Spoor A — rapport-sends live zetten

### A1. Maak de Warmr cover-campagne
Warmr kan geen bijlage; de rapport-link zit in de cover-body die Heatr per lead
meestuurt (`{{custom_subject}}` / `{{custom_body}}`). Er zijn nu **2 ready
inboxes** (`6daad497-…` en `a1c3b714-…`).

```
python3 scripts/create_checkup_campaign.py --dry-run   # check inboxes
python3 scripts/create_checkup_campaign.py             # maakt de campagne, print de id
```

Het script print `CHECKUP_WARMR_CAMPAIGN_ID=<uuid>`.

### A2. Vul `.env` aan
Voeg toe (of pas aan). Ingevuld waar ik de waarde ken:

```env
AERYS_OWN_EMAILS=sami@aeryssolution.nl
CHECKUP_REPORT_URL_TTL_DAYS=30
CHECKUP_WARMR_CAMPAIGN_ID=<uit stap A1>
CHECKUP_REPORT_ENABLED=true
```

`SUPABASE_STORAGE_BUCKET=screenshots`, `DEFAULT_WORKSPACE_ID=aerys`,
`WARMR_API_URL`, `WARMR_API_KEY`, `HEATR_API_KEY`, `ANTHROPIC_API_KEY` staan al
goed — niets aan doen.

### A3. Herstart de API
```
launchctl kickstart -k gui/$(id -u)/nl.aerys.heatr.api
```

### A4. Zorg voor één vrijgegeven rapport (gates 1 + 2)
Via de UI (lead-detail → tab **Gesprek**) of via de API. Het pad naar
`report_status='approved'`:
1. Gesprek aanmaken (transcript) → 2. koppelen aan de juiste lead →
3. uitkomst kiezen **met** cijfers (gate 1) → 4. rapport genereren →
5. **Vrijgeven** (gate 2).

### A5. Dry-run (verstuurt NIETS)
```
python3 scripts/send_checkup_report.py <call_id> --dry-run
```
Verwacht: `report_url` (signed, verloopt na 30 dagen), de 3-regel cover-mail, en
`pdf_bytes`. Controleer de PDF-link in je browser en de covertekst.

### A6. Echte send
```
python3 scripts/send_checkup_report.py <call_id>
```
Verwacht: `report_status='sent'` + retarget ingepland (`scheduled`). Vanaf hier
mag de retarget naar het rapport verwijzen (hard rule 1).

### A7. Retargets (cron/n8n)
```
python3 scripts/run_retarget_cron.py --dry-run   # toon due retargets + concept
python3 scripts/run_retarget_cron.py             # verstuur due retargets
```
Zet dit onder cron/launchd zoals de reply-classifier. Cadans-GOK staat in
`config/retarget_cadence.py`; `GET /analytics/calls` voedt straks de bijstelling.

---

## Spoor B — Zoom-ingest aanzetten (apart, later)

Zoom moet de webhook publiek kunnen bereiken. `WARMR_API_URL` is nu
`localhost:8000`, dus deze machine draait lokaal — Zoom heeft een **publieke URL**
nodig (`HEATR_BASE_URL`, bv. de Railway-deploy of een ngrok-tunnel voor een test).
Zolang `ZOOM_WEBHOOK_SECRET` leeg is, geeft `POST /webhooks/zoom` een 404 (UIT).

Volledige checklist: `docs/zoom_plan_checkup.md`. Kort:
1. Zoom Server-to-Server OAuth-app → `ZOOM_ACCOUNT_ID/CLIENT_ID/CLIENT_SECRET`.
2. Cloud recording + **audio transcript** aan.
3. Event-subscription op `https://<HEATR_BASE_URL>/webhooks/zoom`, event
   `recording.transcript_completed`; secret → `ZOOM_WEBHOOK_SECRET`.
4. `.env` vullen + API herstarten → in Zoom "Validate" klikken (handshake).
5. Testgesprek opnemen → binnen enkele minuten een gesprekrecord (matched, of in
   de unmatched-banner op de Gesprekken-pagina).

Grens: Zoom vult transcript + deelnemers + datum + duur. De check-up-cijfers en
de uitkomst blijven mensenwerk (gate 1).

---

## Terugdraaien
`CHECKUP_REPORT_ENABLED` uit `.env` halen (of `=false`) + API herstarten → alle
check-up-sends staan weer stil. Reeds verstuurde rapporten blijven op
`report_status='sent'`.
```
