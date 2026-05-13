# BLOCKERS SCOPE — Runtime-blockers fix

**Doel:** De 2 runtime-blockers uit de audit oplossen zodat endpoints `/leads/{id}/send-review-email` en `/website-intelligence/process-next` niet meer crashen.

**Niet in scope:** refactors, schema-consolidatie, docs-sync, JWT auth, scoring-weights wire-up. Pure scope: 2 missing modules + tests + smoke check + changelog.

**Observatie tijdens audit van blockers:** ook `process_next_enrichment` in `job_queue/enrichment_queue` wordt geïmporteerd maar **bestaat niet** (`api/main.py` regel 1516). Gespiegelde blocker aan de enrichment queue kant. Ik voeg `process_next_enrichment` toe in Taak 2 als minimal fix (zelfde pattern als `process_next_website_analysis`).

---

## Taak 1 — `campaigns/review_email_generator.py`

### Where imported
`api/main.py` regel 434:
```python
from campaigns.review_email_generator import generate_review_email
email_data = await generate_review_email(lead=lead, website_intelligence=wi)
```

Return value wordt direct als JSON teruggegeven (als `preview_only=true`) of gebruikt in Warmr push.

### Signature (vastgelegd door bestaande caller)
```python
async def generate_review_email(
    lead: dict,
    website_intelligence: dict,
    anthropic_client: Any | None = None,
    supabase_client: Any | None = None,
) -> dict:
    """Return {subject, body, tokens_used, cost_eur, top_issue, specific_observation}"""
```

`anthropic_client` en `supabase_client` zijn optioneel (default = None → interne instantiatie via `os.environ["ANTHROPIC_API_KEY"]` + `get_heatr_supabase()`). Dit houdt de API-call signature (`generate_review_email(lead=lead, website_intelligence=wi)`) werkend zonder `api/main.py` te wijzigen.

### What it should do
Per CLAUDE.md regel 268-292: Claude Haiku genereert een email van max 90 woorden in het Nederlands, gebaseerd op:
- `lead.company_name`, `lead.city`, `lead.contact_first_name` (of "daar" fallback)
- `website_intelligence.total_score` (grootste probleem)
- `website_intelligence.conversion_details` / `sector_details` (top_issue derivatie)
- `website_intelligence.competitor_data.score_vs_market`

**Regels (CLAUDE.md 283-289):**
- Begin NIET met 'Ik'
- Stel ÉÉN concrete vraag
- Geen verkooppraatje
- Verwijs naar één specifiek probleem
- Eindig open
- Max 90 woorden

### Style conventions to copy
- Copy pattern van `enrichment/opener_generator.py` — zelfde Claude Haiku call via `cached_claude_call` of directe `anthropic.AsyncAnthropic`
- Log via `logger = logging.getLogger(__name__)`
- `from __future__ import annotations`
- Type hints + docstring op elke functie
- Exceptions catchen per lead — functie mag nooit raisen, altijd dict retourneren (eventueel met `"error"` key)

### Output schema
```python
{
    "subject": str,           # Max 60 chars, niet generic
    "body": str,              # Max 90 woorden, Nederlands
    "top_issue": str,         # Specifieke observatie uit WI
    "specific_observation": str,  # Concrete verwijzing (CTA ontbreekt, etc.)
    "tokens_used": int,
    "cost_eur": float,
}
```

### Cost tracking
Log elke call naar `heatr_api_cost_log` via `utils.claude_cache.log_api_cost` (bestaande functie). Context = `"review_email"`.

### Top issue derivatie (rule-based, geen Claude)
Voor `top_issue`, leidt af uit `website_intelligence`:
1. Als `technical_score < 10` → "verouderde techniek"
2. Als `conversion_score < 10` → "ontbrekende CTA/booking/WhatsApp"
3. Als `sector_score < 7` → sector-specifiek (uit `sector_details.checks` pak eerste `.passed=false`)
4. Default → "website uit de tijd"

`specific_observation` wordt gegenereerd door Claude op basis van `conversion_details` + `sector_details`.

### Error handling
- Geen API key → return `{"subject": "", "body": "", "error": "ANTHROPIC_API_KEY missing"}` zonder te raisen
- Claude call faalt → return `{"subject": "", "body": "", "error": "claude_failed: <msg>"}`
- Lead ontbreekt cruciale velden → return met `"error": "incomplete_lead_data"`

### Model
Claude Haiku (latest). Ik gebruik product-self-knowledge skill voor exacte ID als nodig.

### Workspace_id isolation
Niet van toepassing — deze functie krijgt `lead` dict binnen die al workspace-scoped geselecteerd is door de caller in `api/main.py` regel 426. Functie zelf hoeft geen workspace_id filter te doen (geen DB reads).

### Estimated LoC
~180 regels inclusief docstring + helpers + derivation logic.

---

## Taak 2 — `job_queue/website_analysis_queue.py`

### Where imported
`api/main.py` regel 1530:
```python
from job_queue.website_analysis_queue import process_next_website_analysis
result = await process_next_website_analysis(workspace_id, db)
```

Return value wordt direct als JSON teruggegeven. Als `None`, caller vertaalt naar `{"processed": False, "reason": "queue_empty"}`.

### Signature (vastgelegd door caller)
```python
async def process_next_website_analysis(
    workspace_id: str,
    supabase_client: Any,
) -> dict | None:
    """Claim one pending website_analysis job, run analyze_website, return result.
    Returns None if queue is empty (caller handles this as 'queue_empty')."""
```

### Why a separate queue (not enrichment_queue)
CLAUDE.md regel 115 noemt dit expliciet als aparte queue. Reden: website analysis is **duurder en trager** (Claude Sonnet Vision + PageSpeed + Playwright) dan andere enrichment steps. Eigen queue = eigen rate limit + eigen retry gedrag + eigen worker pool.

### What it should do
1. Claim next lead met:
   - `workspace_id = :workspace_id`
   - `domain IS NOT NULL`
   - `email_status IN ('valid','risky','catch_all')` (gate: geen Claude credits op onbruikbare leads)
   - Geen bestaande rij in `heatr_website_intelligence` voor deze lead (dedup)
   - `status = 'discovered'` of `status = 'enriched'` (niet disqualified/unsubscribed/forgotten)

2. Run `is_real_website(domain)` pre-screen (copy pattern van `enrichment_queue.py` regel 365-369)

3. Run `analyze_website()` uit `website_intelligence.analyzer` (zelfde call pattern als enrichment_queue regel 371-380)

4. Return summary dict:
```python
{
    "processed": True,
    "lead_id": str,
    "domain": str,
    "total_score": int,
    "duration_seconds": float,
}
```

Als geen eligible lead:
```python
None  # caller → "queue_empty"
```

Bij Claude/Playwright error: catch exception, log, markeer lead.website_analysis_failed_reason, return `{"processed": False, "error": "..."}`. Nooit raisen — caller is een worker die doorgaat.

### Do we need a new DB table?
**Nee.** In tegenstelling tot `heatr_scraping_jobs` of `heatr_enrichment_jobs` heeft website analysis geen expliciete queue tabel nodig. Eligibility is afleidbaar uit de `heatr_leads` + `heatr_website_intelligence` tabellen zelf:

```
leads WHERE domain IS NOT NULL
  AND email_status IN ('valid','risky','catch_all')
  AND id NOT IN (SELECT lead_id FROM website_intelligence WHERE analyzed_at > now() - interval '30 days')
```

Dit is hoe de huidige enrichment_queue step ook werkt (geen aparte queue row voor WI, alleen aanwezigheid in `website_intelligence` = "done"). Deze aanpak blijft consistent met de bestaande code.

**Migration SQL:** nul nieuwe tabellen of kolommen nodig. Ik lever wel `migrations/005_website_analysis_failed_reason.sql` met één optionele kolom toevoeging:

```sql
ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS website_analysis_failed_reason text;
ALTER TABLE heatr_leads ADD COLUMN IF NOT EXISTS website_analysis_failed_at timestamptz;
```

Zodat we bij Playwright/Claude failures de reden kunnen loggen voor debugging. Niet-auto-runnen per regels.

### Ook: `process_next_enrichment` minimal stub
Tijdens scope-analyse ontdekt: `api/main.py` regel 1516 importeert `process_next_enrichment` uit `enrichment_queue` — functie bestaat niet. Zelfde crash-pattern als website_analysis. Ik voeg deze toe als 10-regel wrapper rond bestaande `claim_next_enrichment_job` + `run_enrichment_for_lead` in **dezelfde PR** (zelfde blocker-categorie, zelfde fix-pattern, 1-file-change in `enrichment_queue.py`).

Signature: `async def process_next_enrichment(workspace_id: str, supabase_client: Any) -> dict | None`.

### Style conventions to copy
- Copy pattern van `job_queue/scraping_queue.py` voor claim/execute/complete flow
- Errors per lead catchen
- Altijd `.eq("workspace_id", workspace_id)` op DB queries
- Return `None` bij lege queue (caller handelt dat af)

### Workspace_id isolation
Cruciaal. Elke query in deze module filtert op `workspace_id`. Anders zou worker van workspace A leads van workspace B kunnen pakken.

### Estimated LoC
~160 regels voor `website_analysis_queue.py` + 15 regels voor `process_next_enrichment` wrapper.

---

## Taak 3 — Tests

### `tests/test_review_email_generator.py`
- Mock Claude response (geen echte API calls in tests)
- Assert: subject length ≤ 60, body word count ≤ 90
- Assert: body start niet met "Ik"
- Assert: minstens één "?" in body (de ene concrete vraag)
- Assert: `top_issue` niet leeg
- Test: lead met minimale data (geen WI) → returned dict met `error` key, geen crash
- Test: lead met vol WI → alle velden gevuld

### `tests/test_website_analysis_queue.py`
- Mock supabase client met chained `.select().eq().execute()`
- Test: lege queue → return `None`
- Test: 1 eligible lead → `analyze_website` aangeroepen met juiste args
- Test: workspace_id isolation — lead van andere workspace wordt NIET gepakt
- Test: lead zonder domain → overgeslagen
- Test: lead met email_status='not_found' → overgeslagen (gate)
- Test: lead met bestaande recent website_intelligence → overgeslagen (dedup)
- Test: `analyze_website` raises → functie raiset niet, returnt dict met `error`

### `tests/test_process_next_enrichment.py`
Minimale smoketest — de wrapper roept `claim_next_enrichment_job` + `run_enrichment_for_lead` correct aan.

### Test stack
Copy conventies van bestaande `tests/test_outreach_rules.py`:
- `pytest.mark.asyncio` decorators
- `unittest.mock.MagicMock` / `AsyncMock` voor supabase
- Geen echte HTTP/DB calls

### Run command
```bash
python3 -m pytest tests/test_review_email_generator.py tests/test_website_analysis_queue.py tests/test_process_next_enrichment.py -v
```

Moet 0 errors geven.

---

## Taak 4 — Import smoke test

Na implementatie:
```bash
python3 -c "
from campaigns.review_email_generator import generate_review_email
from job_queue.website_analysis_queue import process_next_website_analysis
from job_queue.enrichment_queue import process_next_enrichment
import inspect
assert inspect.iscoroutinefunction(generate_review_email)
assert inspect.iscoroutinefunction(process_next_website_analysis)
assert inspect.iscoroutinefunction(process_next_enrichment)
print('All 3 blocker modules importable + async')
"
```

Moet zonder ImportError en zonder AssertionError afronden.

Daarnaast: FastAPI app moet opnieuw kunnen starten:
```bash
python3 -c "from api.main import app; print('FastAPI loads:', len(app.routes))"
```

Moet printen zonder exception.

---

## Taak 5 — Documentatie (CHANGES.md)

Nieuw bestand `CHANGES.md` in root met:
- Datum + scope (2 blockers + 1 gerelateerde)
- Lijst toegevoegde bestanden: `campaigns/review_email_generator.py`, `job_queue/website_analysis_queue.py`, `migrations/005_*.sql`, 3 test files
- Gewijzigd bestand: `job_queue/enrichment_queue.py` (1 functie toegevoegd: `process_next_enrichment`)
- Hoe te testen: pytest commando
- Te draaien migration: `migrations/005_website_analysis_failed_reason.sql` (optioneel, alleen voor failure logging)
- Verschil met CLAUDE.md: geen inhoudelijke afwijking, alleen resolution van import-gap
- **NIET** CLAUDE.md zelf updaten (scope: P3 uit audit, aparte sessie)

---

## Niet-risico's / bewuste keuzes

1. **Geen nieuwe API endpoints.** Alleen ontbrekende module-implementaties die bestaande endpoint-routes werkend maken.
2. **Geen schema consolidatie** — dat is P2 uit audit. Ik gebruik bestaande `heatr_leads` + `heatr_website_intelligence` tabellen en voeg alleen 2 optionele error-log kolommen toe.
3. **Geen scoring-weights wire-up.** Zelfde reden (P4).
4. **Review email prompt** gebruik ik letterlijk CLAUDE.md regel 273-290 om consistent te blijven met de documented intent, ook al is CLAUDE.md op andere plekken achterhaald.
5. **`process_next_enrichment` scope creep?** Nee — het is dezelfde soort import-blocker (ImportError crash bij endpoint aanroep), zelfde fix-pattern, 10 regels extra. Als we het uitstellen crasht een productie endpoint.

---

## Impact na merge

- `/leads/{id}/send-review-email` werkt — Aerys kan eerste review emails sturen
- `/website-intelligence/process-next` werkt — n8n worker loop werkt
- `/enrichment/process-next` werkt — n8n enrichment worker loop werkt
- Geen schema changes (alleen 2 optionele kolommen die alleen geschreven worden bij failure)
- Geen breaking changes voor bestaande callers

---

## Estimated tijd

- Taak 1: 30 min
- Taak 2: 30 min
- Taak 3: 40 min (3 test files)
- Taak 4: 5 min
- Taak 5: 10 min
- **Totaal:** ~2 uur

---

**STOP-GATE:** wacht op akkoord vóór ik Taak 1 start.
