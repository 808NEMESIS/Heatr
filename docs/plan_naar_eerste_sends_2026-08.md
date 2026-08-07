# Plan — naar de eerste veilige sends (2026-08)

**Doel:** één canary-batch veilig de deur uit → valideren → gecontroleerd opschalen,
zonder compliance of deliverability te schaden.

**Kern-inzicht (volgorde-logica):** discovery loopt vér voor op verzendcapaciteit
— 1061 leads vs. 24 mails/dag (~44 dagen voorraad). De bottleneck is dus niet
leadvolume maar **validatie van de send-keten**. Daarom: eerst smal + diep (canary),
niet breed (sweep opschalen / KvK najagen). Die komen ná de canary-validatie.

Kill-switch blijft dicht tot Fase 2. Elke fix klein/getest/apart gecommit.

---

## Fase 0 — Afmaken wat loopt  (ik; klein)
- [ ] **`results_found`-teller fixen** — scraping-jobs tonen 0 terwijl er leads uitkomen; observability-gaatje dichten.
- [ ] **Website-analyse-backlog** leeg laten lopen (auto; monitoren dat 't zakt).
- [ ] **Slack-keten afronden** — na Sami's Active-toggle in n8n: end-to-end test, dan live.
- [ ] **Warmr-API-daemon** — plist opstellen zodat `localhost:8000` up is + reboot-proof (nu geen daemon → "API down op send-moment"-gat). Ik schrijf 'm in de Warmr-repo; Sami laadt 'm.

## Fase 1 — Canary klaarzetten  (ik)
- [ ] **Canary-batch** — 5-10 leads uit de 217 AVG-veilige, mét mail 1/3-preview + AVG-grond per lead → Sami keurt de exacte lijst.
- [ ] **Arming-runbook** — exacte env-vars in volgorde, met de allowlist-veiligheid (eerst naar jezelf, dan pas echte leads).

## Fase 2 — Eerste sends  (Sami's hand; gated)
- [ ] `ENABLE_PROSPECT_SENDS=true` + `HEATR_SEND_ALLOWLIST=<eigen adres>` → **canary naar jezelf** (valideert de echte send-keten: launch → activate → Warmr → inbox).
- [ ] Allowlist uitbreiden naar een handvol echte leads → **echte canary**.
- [ ] **Monitoren** — Slack-meldingen (reactie/afmelding) + drempels (bounce >3% blokkeert, unsub >2% waarschuwt).

## Fase 3 — Opschalen  (na canary-validatie)
- [ ] `daily_campaign_target` rampen (12 → 20 → …) naarmate reputatie goed blijft.
- [ ] Allowlist eraf; **receptie-sequence volledig aan**.
- [ ] **KvK-key binnen** → rechtsvorm-backfill → +88 `onbepaald`-leads ontgrendeld.
- [ ] **Sweep opschalen** (dagelijkse cron `--spread`) nu de mail-kant bewezen is.
      LET OP (bevinding 08-07): sweep van 8 jobs → alleen de eerste 3 steden
      scrapeten (49-59 resultaten), de laatste 5 kregen 0 — waarschijnlijk de
      google_maps-rate-limiter na ~3 scrapes. **Dose ≤3 jobs/run.** Bijkomend:
      een 0-resultaat-job telt als 'gescrapet' → de sweep-generator-dedup slaat
      die (stad×keyword) daarna over. Fix: 0-resultaat-jobs uitsluiten van de
      dedup óf de 5 lege combo's her-queuen. Niet urgent (voorraad genoeg).
- [ ] **Feedback-lus** — replies → CRM → ICP-scoring bijstellen.

---

## Wat NIET nu (bewust)
- Sweep opschalen / KvK najagen — voorraad is er al; eerst de send-keten bewijzen.
- Kill-switch openen zonder canary-naar-jezelf-validatie.
- daily_campaign_target verder ophogen vóór de eerste echte sends reputatie-data geven.

## Afhankelijkheden (Sami-gated)
1. Slack Active-toggle (Fase 0 afronden).
2. Warmr-API-plist laden (Fase 0).
3. Arming-beslissing + allowlist (Fase 2) — de enige echte "go".
