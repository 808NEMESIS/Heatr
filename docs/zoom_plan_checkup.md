# Zoom-plan — check-up follow-up ingest (Fase 8)

De webhook-code staat er (`calls/zoom_webhook.py` + `POST /webhooks/zoom`), maar
is UIT: zonder `ZOOM_WEBHOOK_SECRET` geeft de endpoint 404. Dit is de lijst van
wat er aan de Zoom-kant en in de env moet gebeuren om 'm aan te zetten. Niets
hiervan is code die ik kan draaien — het is registratie + config.

## 1. Zoom-app aanmaken
- Type: **Server-to-Server OAuth**-app (account-level, geen user-OAuth) via het
  Zoom App Marketplace / Developer-portaal.
- Noteer **Account ID**, **Client ID**, **Client Secret** -> `.env`:
  `ZOOM_ACCOUNT_ID`, `ZOOM_CLIENT_ID`, `ZOOM_CLIENT_SECRET`.
- Scopes (minimaal): `cloud_recording:read:list_recording_files` (of de klassieke
  `recording:read`) en `meeting:read`. De read-scope op recordings is nodig om
  het VTT-transcript te downloaden.

## 2. Cloud recording + audiotranscript aanzetten
- Zoom-account-instelling: **Cloud recording** aan.
- Aan: **Audio transcript** (de automatische transcriptie). Zonder deze setting
  komt er geen `TRANSCRIPT`/VTT-bestand en valt elke ingest op
  `skipped: no_transcript_file`.

## 3. Event-subscription (de webhook)
- Voeg in de app een **Event Subscription** toe met endpoint-URL:
  `https://<heatr-host>/webhooks/zoom`.
- Events: **`recording.transcript_completed`** (primair) en eventueel
  `recording.completed`. De transcript-variant vuurt pas als de VTT klaar is —
  dat is wat we willen.
- Zoom genereert een **Secret Token** voor de subscription -> `.env`:
  `ZOOM_WEBHOOK_SECRET`. Dít is de aan/uit-schakelaar: zolang leeg -> endpoint 404.

## 4. URL-validatie-handshake
- Bij het opslaan van de endpoint stuurt Zoom een `endpoint.url_validation`-event.
- De endpoint beantwoordt die automatisch met
  `HMAC-SHA256(secret, plainToken)` (zie `url_validation_response`). Zet het
  secret dus in de env vóór je in Zoom op "Validate" klikt.

## 5. HMAC-verificatie (al afgedwongen)
- Elk echt event wordt geverifieerd:
  `x-zm-signature == "v0=" + HMAC-SHA256(secret, "v0:" + x-zm-request-timestamp + ":" + body)`.
- Mismatch -> 401. Fail-closed, geen tolerantie.

## 6. Transcript ophalen
- Het event bevat `object.recording_files[]`; het VTT-bestand heeft
  `file_type=TRANSCRIPT` (of extensie `VTT`) met een `download_url`.
- Zoom stuurt een `download_token` mee in het event; de download gebruikt
  `Authorization: Bearer <download_token>`. De VTT wordt geparsed naar platte
  tekst (`parse_vtt`).

## 7. Deelnemer-matching (fail-closed)
- E-mails worden uit het event-object gehaald en `AERYS_OWN_EMAILS` eruit
  gefilterd. **Exact één** lead-match in workspace `aerys` -> `matched`; anders
  -> `unmatched` (verschijnt in de Gesprekken-banner voor handmatige koppeling).
- Let op / bekende grens: Zoom's recording-payload bevat niet altijd álle
  deelnemer-e-mails (vaak alleen `host_email`). Voor betrouwbare matching kan een
  extra call naar `GET /report/meetings/{meetingId}/participants` (scope
  `report:read:admin`) nodig zijn. Nu: best-effort uit de payload; de rest wordt
  bewust `unmatched` (fail-closed, geen gok).

## 8. Idempotentie + async
- `zoom_meeting_id` is UNIQUE (migratie 032) + er is een pre-check: een
  her-verzonden event maakt geen dubbel record.
- De zware verwerking draait als FastAPI-background-task; de webhook geeft binnen
  Zoom's 3s-window 200 terug (anders retryt Zoom).

## 9. Grens van de automatisering
- Zoom vult alleen **transcript + deelnemers + datum + duur**. De check-up-cijfers
  (`checkup_data`) en de uitkomst blijven **mensenwerk** (gate 1): de operator
  opent het uitkomst-formulier op de lead. Zoom automatiseert de uitkomst niet.

## Aanzetten — checklist
1. `.env`: `ZOOM_ACCOUNT_ID`, `ZOOM_CLIENT_ID`, `ZOOM_CLIENT_SECRET`,
   `ZOOM_WEBHOOK_SECRET`, (`AERYS_OWN_EMAILS` staat al goed).
2. API herstarten.
3. In Zoom op "Validate" klikken bij de event-subscription (handshake).
4. Testgesprek opnemen -> binnen enkele minuten verschijnt een gesprekrecord
   (matched of in de unmatched-banner).
