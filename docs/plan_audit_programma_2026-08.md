# Audit-programma — vijf audits, gekoppeld aan de go-live-route (2026-08-03)

Principe: elke audit valt precies vóór de milestone waar z'n vondsten actionable zijn.
Aanleiding: de drie grootste vondsten van deze week (402-datavernietiging, dood
KvK-v1-endpoint, 4 kapotte UI-endpoints) kwamen uit audits of toeval — nooit uit alarmen.

```
NU ────────────► A1 Integraties ─► A2 Voorraad ─► [F3 curatie] ─► A3 Copy ─► [F2 seed]
                                                                    ─► A4 Deliverability ─► [F5 arming]
                                                                    ─► A5 Security ─► [24/7-hosting]
```

---

## A1 — Integratie-gezondheidsaudit  ⭐ eerst
**Bewaakt:** sweep-opschaling + dagelijkse ops. **Eigenaar:** Claude, autonoom. **Effort:** ~1 uur.

**Waarom:** Bouncer-402 en KvK-v1-dood waren toevalstreffers; externe contracten rotten stil.

**Scope (elke integratie live, read-only):**
| Integratie | Probe | Rood als |
|---|---|---|
| Bouncer | credits/status-endpoint (géén verify-call) | 401/402 of tegoed 0 |
| KvK Zoeken + Basisprofiel | v2-zoek + profiel-call op bekende KvK | ≠200 |
| Google PageSpeed | 1 API-call op bekende URL | key invalid/quota |
| Anthropic | modellen-lijst (goedkoopste call) | auth-fout |
| Supabase REST + Storage | select 1 + bucket-list | fout |
| Warmr API (:8000) | /inboxes | onbereikbaar / 0 ready |
| Afmeld-URL (Vercel) | GET bevestigingspagina | ≠200 |
| Google Maps scrape | Playwright-smoke: 1 query, ≥1 resultaat | selectors kapot |
| n8n (:5678) | HTTP-reachability | down |

**Deliverables:** `scripts/integration_health.py` (exit-code per status: OK/DEGRADED/DOWN,
JSON + leesbare tabel) + cron-regel-voorstel (dagelijks, via cron_logged.sh) + eerste rapport.
**Verdict-drempel:** elke DOWN blokkeert sweep-opschaling tot verklaard.

## A2 — Voorraad-kwaliteitsaudit
**Bewaakt:** cohort-kwaliteit + sweep-richting. **Eigenaar:** Claude, autonoom. **Effort:** ~2 uur.

**Scope (de 986+ leads als dataset):**
1. **Velden-dekking** per kolom × sector (naam, telefoon, rating, domain, opener, haakje) — waar zit de 43%-naamloos-massa precies?
2. **Score/icp-sanity**: verdelingen, outliers (score>55 zonder e-mail? icp=0 met perfecte fit?), gate-gevoeligheid rond 55/0.50.
3. **Versheid**: enrichment-leeftijd per lead (updated_at-verdeling); >90d = stale-vlag.
4. **Duplicaat-rest** ná 021: fuzzy op domein/naam (de unique-index vangt alleen exact e-mail/domein).
5. **Sector-consistentie**: leads in inactieve sectoren, ACTIVE_SECTORS vs CLAUDE.md vs live schedules (de alt-geneeskunde-verwarring definitief in kaart).
6. **Cohort-25 onder het vergrootglas**: velden compleet, haakje-claim nog geldig (site-steekproef), e-mailstatus, rechtsvorm-status.

**Deliverables:** `docs/audit_voorraad_2026-08.md` met reparatie-lijst (gescheiden: script-baar vs. mensenwerk) + evt. kleine fix-scripts.
**Verdict-drempel:** cohort-leads met ongeldige haakjes of ontbrekende kernvelden → uit cohort.

## A3 — Pre-send copy-audit
**Bewaakt:** seed-test (F2). **Eigenaar:** Claude rendert + checkt, Sami leest de mails zelf. **Effort:** ~2 uur + jouw leesronde. **Trigger:** ná F3-curatie (namen!), direct vóór de seed-test.

**Scope:** alle cohort-previews renderen via `build_receptie_preview` (dry, verstuurt niets):
compliance-tokens aanwezig · haakje-claim vs. live site (Playwright-steekproef op elke Q4/Q7-claim) ·
aanhef (echte naam vs 'Hallo,') · lengte/afkapping · **verboden inhoud (prijzen! — hard rule)** ·
subject-lines · spintax-fouten · mail-2-skip-logica per lead.

**Deliverables:** `docs/audit_copy_cohort.md` — per lead go/no-go met reden; de no-go's terug naar curatie.
**Verdict-drempel:** één onware site-claim = no-go voor die lead, geen uitzonderingen.

## A4 — Deliverability-audit
**Bewaakt:** arming (F5). **Eigenaar:** Claude (DNS-deel kan NU), rest samen met de Warmr-W-punten. **Effort:** ~half dagdeel.

**Scope:**
1. **NU al mogelijk:** SPF/DKIM/DMARC/MX van meet-aerys.nl via DNS (dig), DMARC-policy-advies, DNSBL-blacklist-check van domein + verzend-IP's.
2. **Warmr-cijfers kritisch:** warmup-stats lezen mét de bekende "meet zichzelf"-doodzonde in gedachten (spam_rescued > placement); wat is er ONafhankelijk bewezen?
3. **Afmeld-keten E2E:** GET-bevestiging → POST-suppress met een testadres → suppressie-rij verschijnt in heatr_suppressions.
4. Advies verzendritme eerste 2 weken (5/dag-realiteit + ramp-schema zodra targets omhoog gaan).

**Deliverables:** DNS-rapport + go/no-go-lijst vóór activate; hangt aan F4 (Warmr-sessie) voor de fixes.

## A5 — Security-audit
**Bewaakt:** 24/7-hosting-migratie. **Eigenaar:** Claude. **Effort:** ~half dagdeel. **Trigger:** vóórdat iets publiek bereikbaar wordt (nu localhost = laag risico).

**Scope:** auth-paden (X-API-Key/JWT/legacy-flag) · CORS-wildcard dicht · secrets-scan in logs/commits ·
.env-permissies · webhook-HMAC-verificatie (Warmr/Zoom) · rate-limiting op publieke endpoints ·
service-key-oppervlak (n8n, cron — crontab-key is al gefixt) · RLS-restcheck · frontend-token-flow.

**Deliverables:** `docs/audit_security_2026-08.md` met fix-lijst gerangschikt op "moet vóór hosting" vs "nice".

---

## Uitvoeringsvolgorde & koppeling aan het bestaande F-plan

| Wanneer | Wat | Gate |
|---|---|---|
| **Vandaag** | A1 bouwen + draaien → daarna A2 | sweep-opschaling wacht op A1-groen |
| Vandaag (klein) | A4-DNS-deel meepakken (dig-checks zijn gratis) | — |
| Ná F3-curatie (Sami) | A3 op het definitieve cohort | seed-test wacht op A3-groen |
| Vóór F5-arming | A4 compleet (afmeld-E2E + Warmr-cijfers) | activate wacht op A4-groen |
| Vóór hosting-besluit | A5 | publieke URL wacht op A5-groen |

**Herhaalbaarheid:** A1 wordt een dagelijkse cron (integration_health), A2 een her-runbaar script
(voor elke nieuwe sweep-tranche), A3 een vast ritueel vóór élke nieuwe campagne.
