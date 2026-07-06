# Sprint 3 — Render Ownership: feitelijke render-trace (Stap 1)

**Read-only, aanname-vrij.** Getraceerd op 2026-07-06 tegen de huidige
`main`. Doel: per send-pad vaststellen wáár subject + body feitelijk worden
samengesteld, en waar twee bronnen uit elkaar kunnen lopen.

---

## De twee render-mechanismen

**Warmr-render (mail-merge).** `create_campaign(sequence_steps=…)`
([warmr_client.py:381](../../integrations/warmr_client.py#L381)) bevriest de
volledige multi-step sequence als template in Warmr. Bij elke push levert
`_build_lead_payload` ([warmr_client.py:446](../../integrations/warmr_client.py#L446))
per lead een `custom_fields`-blok met opgeloste **token-waarden**: `opener`,
`company`, `city`, `bedrijfsnaam`, `stad`, `signaal_blok`, `sector_noemer`,
`primaire_dienstverlening`, enz. Warmr substitueert `{{token}}` in de frozen
bodies op verzendmoment. **Heatr levert de ingrediënten, Warmr assembleert de
zin.**

**Heatr-render (volledige body).** `render_step(step, lead)`
([sequence_engine.py:309](../../campaigns/sequence_engine.py#L309)) rendert
een stap volledig: `inject_variables` (token → waarde, mét logica:
signaal_blok-capitalisatie aan zinbegin, `display_first_name`-artefact-strip,
`stad_of_sector`-fallback bij generieke stad) gevolgd door `resolve_spintax`
(`{a|b}` → **random** keuze, `random.choice`, geen seed). Levert een compleet
`{subject, body}`.

Deze twee zijn **niet equivalent**: Warmr's dumme `{{token}}`-substitutie
repliceert de capitalisatie-, fallback- en artefact-logica van
`inject_variables` niet, en `resolve_spintax` is niet-deterministisch.

---

## Matrix — per send-pad, wie rendert wat

| Send-pad | Trigger → call | Heatr rendert | Warmr rendert | Gaat body de deur uit? | Divergentie |
|---|---|---|---|---|---|
| **campaign-launch** | `/campaigns/launch` → `create_campaign(steps)` + `push_leads_bulk` ([main.py:1535](../../api/main.py#L1535)) | token-**waarden** (via `_build_lead_payload`) | **de body** (frozen sequence + tokens) | nee — alleen tokens | Split-render: Heatr's `inject_variables`-logica draait niet bij Warmr → subtiele tekstverschillen als template daarop rekent |
| **bulk send-to-warmr** | `/leads/send-to-warmr` → `push_leads_bulk` ([main.py:616](../../api/main.py#L616)) | token-waarden | de body (frozen sequence v/d inbox-campagne) | nee — alleen tokens | idem launch |
| **single review-email** | `/leads/{id}/send-review-email` → `generate_review_email` + `push_lead` **zonder** `custom_*` ([main.py:720](../../api/main.py#L720)) | **volledige subject+body** via Claude Haiku (`email_data`) | de body (frozen sequence) | **nee — `email_data` wordt weggegooid** | **HOOG — bug: preview toont de Claude-review-email, de send verstuurt de Warmr-sequence. Twee verschillende mails.** |
| **n8n sequence-dispatch** | `/sequences/process-send` → `process_due_send` → `render_step` + `push_lead(custom_subject, custom_body)` ([sequence_engine.py:409](../../campaigns/sequence_engine.py#L409)) | **volledige subject+body** via `render_step` | de body (frozen sequence); `custom_*` alleen bruikbaar als template `{{custom_subject}}` bevat | **ja — volledige body in `custom_fields`** | **HOOG — dubbele render: Heatr rendert volledig ÉN Warmr rendert frozen. `resolve_spintax` is random → zelfs identieke template geeft per bron een andere body.** |

---

## Kernvondst: production = Model A; Heatr-render is dood/kapot

De matrix suggereert twee actieve modellen. De feitelijke run-state is scherper:

**Model A (Warmr-native, autonoom) is de enige live verzendweg.**
`/campaigns/launch` bevriest de hele sequence in Warmr; Warmr stuurt mail
1/2/3 zelf op eigen schema en rendert elke stap uit frozen template +
per-lead tokens. Dit is wat er vandaag verstuurt.

**Model B (Heatr-driven per-step via `sequence_engine`) is dormant.** Bewijs:
- `get_due_sends` ([sequence_engine.py:332](../../campaigns/sequence_engine.py#L332))
  selecteert `lead_campaign_history` op `status='pending'` + `is_active=true`
  + `next_send_at <= now` mét `sequence_steps`.
- **Geen enkel codepad schrijft ooit `sequence_steps` + `pending` +
  `next_send_at` naar `lead_campaign_history`.** De kolommen bestaan
  (`supabase_schema.sql:704` "sequence engine columns"), maar de enige
  `sequence_steps=` in de hele repo is [main.py:1537](../../api/main.py#L1537)
  — dat gaat naar Wármr, niet naar de tabel. De webhook-upsert
  ([main.py:3148](../../api/main.py#L3148)) zet enkel `status`.
- Gevolg: `get_due_sends` levert nooit een render-bare due-send; `render_step`
  in `process_due_send` vuurt in de praktijk niet.

**De spanning die dit blootlegt:** de Control Plane (Sprint 2) is gebouwd
rónd Model B — `restart` bumpt `restart_epoch` op `lead_campaign_history`,
`force-next` zet `next_send_at`, de Run-tab toont per-stap `sequence_steps`.
Die operator-controls hebben vandaag niets om te besturen, én op het moment
dat iemand `sequence_steps` wél vult (waar de Control Plane vanuit gaat)
start Heatr-render naast Warmr-render voor dezelfde logische sequence → de
dubbele bron van waarheid wordt van latent naar actief.

Daarom is dit exact het juiste moment om render-eigenaarschap te beslissen:
vóórdat Model B activeert.

---

## Drie concrete divergentie-bronnen (samengevat)

1. **Random spintax** — `resolve_spintax` kiest per aanroep opnieuw. Zelfs
   Heatr-alleen levert call-vs-call een andere body. "Eén logische send =
   dezelfde body" vereist deterministisch renderen óf één keer bevriezen.
2. **Split render op launch/bulk** — Heatr's `inject_variables`-logica
   (capitalisatie, fallbacks, naam-artefact-strip) draait niet bij Warmr's
   `{{token}}`-substitutie.
3. **Review-email gooit zijn render weg** — `generate_review_email` produceert
   `{subject, body}`, maar de push geeft ze niet mee; Warmr verstuurt de
   inbox-sequence i.p.v. de gegenereerde review-mail.

---

## Stap 2 — Beslissing: Heatr rendert (Warmr verstuurt letterlijk)

**Gekozen: Optie 1 — Heatr is de render-eigenaar van sequence-content.**

Motivatie, gewogen tegen de vondst dat productie vandaag op Model A draait:

1. **Content bij context.** Heatr heeft `why_chosen`, personalisatie-signalen,
   spam-woord-check en readiness. Die horen bij het renderpunt — een
   spam-/kwaliteitscheck op de finale tekst kan alleen waar de tekst ontstaat.
2. **Control Plane-consistentie.** Sprint 2 is al rond Heatr-side per-step state
   gebouwd (`restart_epoch`, `step_index`, `sequence_steps`, de Run-tab). Bij
   Warmr-rendert ziet de Control Plane de finale body nooit — de investering zou
   half hangen.
3. **Render-sofisticatie.** `inject_variables` doet capitalisatie-, fallback- en
   naam-artefact-logica die Warmr's `{{token}}`-substitutie niet repliceert. Die
   logica bij Warmr dupliceren = drift; in Heatr houden = één plek.
4. **Reproduceerbaarheid (I7).** Rendert Heatr en bevriest het de body in het
   ledger, dan is exact wat verstuurd is achteraf reproduceerbaar — onafhankelijk
   van latere template-edits.

## Stap 3 — Handhaving: wat nu afgedwongen is (geen live-send-wijziging)

Bewust gescopet (keuze Sami): de veilige, verifieerbare kern nu; de live-cutover
apart. Deze wijzigingen veranderen **niets** aan wat Warmr vandaag verstuurt.

- **Eén renderfunctie.** `render_step` ([sequence_engine.py:309](../../campaigns/sequence_engine.py#L309))
  is expliciet de enige sequence-renderer (invariant I8).
- **Deterministisch.** `resolve_spintax` accepteert een geseede `random.Random`;
  `render_step(step, lead, seed=…)` seedt op `{lead_id}:{step_index}`.
  `process_due_send` levert die seed. Gevolg: één logische send levert
  byte-identieke body op, herhaalbaar (ook bij restart) — terwijl cross-lead de
  tekst blijft variëren voor deliverability. `restart_epoch` zit bewust NIET in
  de seed: een restart herzendt dezelfde content.
- **Body bevroren in het ledger.** De dispatch-metadata draagt nu
  `rendered.{subject,body}` + `render_owner: "heatr"` → `heatr_outbound_log`.
  Wat de deur uitging is reproduceerbaar (I7).
- **Regressietest.** `tests/test_sequence_gate.py` bewijst determinisme per seed,
  single-source-gelijkheid over paden, en cross-lead volledig-geresolveerde body.

## Openstaand — de live-cutover (vereist Sami + Warmr-coördinatie)

Deze twee maken Warmr's render écht inert op de live paden, maar raken live
e-mail en hangen op een **onbevestigd Warmr-contract**:

1. **Warmr-contract (BLOKKEREND, open vraag).** Onbekend of Warmr
   `custom_subject`/`custom_body` **letterlijk** verstuurt, of ze alleen gebruikt
   als de template `{{custom_subject}}`/`{{custom_body}}` bevat. De payload-
   docstring ([warmr_client.py:193](../../integrations/warmr_client.py#L193))
   suggereert het laatste. Zolang dit niet bevestigd is, garandeert "Heatr
   rendert" nog niet "Warmr verstuurt letterlijk" — de ledger legt Heatr's
   *intentie* vast, niet wat Warmr feitelijk assembleert.
2. **Model A → B cutover.** Het launch-pad (`create_campaign` + autonome
   Warmr-multi-step-send) blijft Warmr-rendered tot óf Model B geactiveerd wordt
   (Heatr drijft elke stap; Warmr-autonoom uit — vereist een writer die
   `sequence_steps`+`pending`+`next_send_at` vult) óf de Warmr-campagnes
   passthrough-templates (`{{custom_body}}`) krijgen. Plus: de
   **review-email-discard-bug** ([main.py:720](../../api/main.py#L720)) fixen
   (rendered subject/body meesturen) hangt op datzelfde contract.

**Aanbevolen volgorde:** (a) Warmr-contract verifiëren; (b) als verbatim →
review-email-fix + full-body op de push-paden; als token-only → passthrough-
templates in Warmr; (c) daarna pas Model B activeren met een `sequence_steps`-
writer, zodat de Control Plane iets te besturen krijgt.
