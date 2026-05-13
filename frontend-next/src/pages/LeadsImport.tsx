import { useState, useCallback } from 'react';
import { useMutation } from '@tanstack/react-query';
import { Upload, FileText, ArrowLeft, AlertTriangle, CheckCircle2, Copy } from 'lucide-react';
import { Link } from 'react-router-dom';
import { api } from '@/lib/api';
import { fmtInt } from '@/lib/format';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';

const ALLOWED_HEADERS = [
  'company_name', 'domain', 'email', 'city', 'phone',
  'sector', 'contact_first_name', 'contact_last_name',
  'kvk_number', 'google_maps_url', 'google_rating',
  'google_review_count', 'google_category',
];

// Friendly NL/EN labels voor de mapping-UI
const FIELD_LABELS: Record<string, string> = {
  company_name:        'Bedrijfsnaam',
  domain:              'Website / domein',
  email:               'Email',
  city:                'Stad / plaats',
  phone:               'Telefoon',
  sector:              'Sector',
  contact_first_name:  'Voornaam contact',
  contact_last_name:   'Achternaam contact',
  kvk_number:          'KvK-nummer',
  google_maps_url:     'Google Maps URL',
  google_rating:       'Google rating',
  google_review_count: 'Aantal reviews',
  google_category:     'Google categorie',
};

// Synoniemen voor auto-mapping (lowercase). Eerste match wint.
const COLUMN_SYNONYMS: Record<string, string[]> = {
  company_name:        ['bedrijf', 'bedrijfsnaam', 'naam', 'company', 'firma', 'organisatie', 'praktijk', 'kliniek'],
  domain:              ['website', 'site', 'url', 'web', 'webadres', 'domain'],
  email:               ['email', 'e-mail', 'mail', 'emailadres', 'mailadres'],
  city:                ['stad', 'plaats', 'gemeente', 'city', 'town', 'vestiging'],
  phone:               ['telefoon', 'tel', 'telefoonnummer', 'phone', 'mobiel', 'gsm'],
  sector:              ['sector', 'branche', 'vakgebied', 'industry'],
  contact_first_name:  ['voornaam', 'first name', 'firstname', 'first_name', 'contact', 'contactnaam'],
  contact_last_name:   ['achternaam', 'last name', 'lastname', 'last_name', 'familienaam'],
  kvk_number:          ['kvk', 'kvk-nummer', 'kvknummer', 'coc', 'chamber'],
  google_maps_url:     ['google maps', 'maps url', 'maps_url', 'gmaps'],
  google_rating:       ['rating', 'beoordeling', 'sterren', 'stars', 'google rating'],
  google_review_count: ['reviews', 'review count', 'aantal reviews', 'beoordelingen'],
  google_category:     ['categorie', 'category', 'type'],
};


function autoMapColumn(header: string): string | null {
  const norm = header.toLowerCase().trim().replace(/[\s_-]+/g, ' ');
  // Exact allowed-name match
  const direct = norm.replace(/\s+/g, '_');
  if (ALLOWED_HEADERS.includes(direct)) return direct;
  // Synoniem-match
  for (const [target, synonyms] of Object.entries(COLUMN_SYNONYMS)) {
    if (synonyms.some((s) => norm === s || norm.includes(s))) return target;
  }
  return null;
}

interface ImportResult {
  imported: {
    row_index: number;
    lead_id?: string;
    row?: Record<string, unknown>;
    would_insert?: boolean;
    enrichment_queued?: boolean;
    enrichment_job_id?: string;
  }[];
  duplicates: {
    row_index: number;
    matched_lead_id: string;
    matched_company: string | null;
    match_type: string;
    row: Record<string, unknown>;
    merge_action?: string;
    merged_fields?: string[];
  }[];
  errors: { row_index: number; error: string; row: Record<string, unknown> }[];
  fatal: string | null;
  summary?: {
    total_rows: number;
    imported_count: number;
    duplicate_count: number;
    error_count: number;
    enrichment_queued_count?: number;
    enrichment_queue_failures?: number;
    auto_enrich?: boolean;
    merge_strategy?: string;
  };
  cost_estimate?: {
    row_count: number;
    cost_per_lead_eur: number;
    estimated_total_eur: number;
  };
  idempotent_replay?: boolean;
  queue_failure_samples?: { lead_id: string; error: string }[];
}

// Generate UUID v4 zonder externe lib (crypto.randomUUID is ES2022, modern browsers).
function newImportRunId(): string {
  if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) return crypto.randomUUID();
  // Fallback voor oudere browsers
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

function parseCsv(text: string): { headers: string[]; rows: Record<string, string>[]; warnings: string[] } {
  const warnings: string[] = [];
  const lines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length === 0) return { headers: [], rows: [], warnings: ['Lege CSV.'] };

  const splitLine = (line: string): string[] => {
    const out: string[] = [];
    let cur = '';
    let inQuote = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') {
        if (inQuote && line[i + 1] === '"') { cur += '"'; i++; continue; }
        inQuote = !inQuote;
        continue;
      }
      if ((c === ',' || c === ';') && !inQuote) { out.push(cur.trim()); cur = ''; continue; }
      cur += c;
    }
    out.push(cur.trim());
    return out;
  };

  const headers = splitLine(lines[0]).map((h) => h.toLowerCase().replace(/\s+/g, '_'));
  const unknown = headers.filter((h) => !ALLOWED_HEADERS.includes(h));
  if (unknown.length > 0) warnings.push(`Onbekende kolommen worden genegeerd: ${unknown.join(', ')}`);

  const rows: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = splitLine(lines[i]);
    const obj: Record<string, string> = {};
    headers.forEach((h, j) => { if (cells[j] !== undefined) obj[h] = cells[j]; });
    rows.push(obj);
  }
  return { headers, rows, warnings };
}

export function LeadsImportPage() {
  const [rawText, setRawText] = useState<string>('');
  const [parsed, setParsed] = useState<{ headers: string[]; rows: Record<string, string>[]; warnings: string[] } | null>(null);
  const [mapping, setMapping] = useState<Record<string, string>>({});  // csv-header → heatr-field
  // Snapshot van mapping/strategy ten tijde van laatste dry-run.
  // Als ze daarna wijzigen, toon "preview is verouderd"-banner.
  const [dryRunSnapshot, setDryRunSnapshot] = useState<{ mapping: string; mergeStrategy: string; autoEnrich: boolean } | null>(null);
  const [autoEnrich, setAutoEnrich] = useState(true);
  const [mergeStrategy, setMergeStrategy] = useState<'skip' | 'fill_blanks' | 'overwrite'>('skip');
  // import_run_id wordt PER UPLOAD opnieuw gegenereerd. Idempotency-key voor één set.
  const [importRunId, setImportRunId] = useState<string>(() => newImportRunId());
  const [dragOver, setDragOver] = useState(false);

  // Apply mapping voor de payload — rename keys voordat we naar API sturen
  const remappedRows = (): Record<string, string>[] => {
    if (!parsed) return [];
    return parsed.rows.map((row) => {
      const out: Record<string, string> = {};
      for (const [csvHeader, value] of Object.entries(row)) {
        const target = mapping[csvHeader];
        if (target && value) out[target] = value;
      }
      return out;
    });
  };

  const dryRunMut = useMutation({
    mutationFn: () => api.post<ImportResult>('/leads/import', {
      rows: remappedRows(),
      dry_run: true,
      source: 'csv',
      auto_enrich: autoEnrich,
      merge_strategy: mergeStrategy,
      // Geen import_run_id voor dry-run — dat zou cache vergiftigen
    }),
    onSuccess: () => {
      // Snapshot voor staleness-detection
      setDryRunSnapshot({
        mapping: JSON.stringify(mapping),
        mergeStrategy,
        autoEnrich,
      });
    },
  });
  const commitMut = useMutation({
    mutationFn: () => api.post<ImportResult>('/leads/import', {
      rows: remappedRows(),
      dry_run: false,
      source: 'csv',
      auto_enrich: autoEnrich,
      merge_strategy: mergeStrategy,
      import_run_id: importRunId,
    }),
    onSuccess: () => {
      // Genereer nieuwe id voor volgende batch — voorkomt accidenteel replay
      setImportRunId(newImportRunId());
    },
  });

  const onText = (text: string) => {
    setRawText(text);
    const result = parseCsv(text);
    setParsed(result);
    // Auto-mapping: voor elke detected header probeer een match
    const initialMapping: Record<string, string> = {};
    for (const h of result.headers) {
      const guess = autoMapColumn(h);
      if (guess) initialMapping[h] = guess;
    }
    setMapping(initialMapping);
    dryRunMut.reset();
    commitMut.reset();
  };

  const setMappingFor = (csvHeader: string, target: string) => {
    setMapping((prev) => {
      const next = { ...prev };
      if (target) next[csvHeader] = target;
      else delete next[csvHeader];
      return next;
    });
  };

  const mappedTargets = Object.values(mapping);
  const requiredFieldsMet = mappedTargets.some((t) =>
    ['email', 'domain', 'kvk_number', 'company_name'].includes(t)
  );

  // Detect of de huidige instellingen afwijken van de laatste dry-run.
  // Zo ja: blokkeer commit + toon "voer dry-run opnieuw uit" banner.
  const dryRunStale = (() => {
    if (!dryRunSnapshot || !dryRunMut.data) return false;
    return (
      dryRunSnapshot.mapping !== JSON.stringify(mapping) ||
      dryRunSnapshot.mergeStrategy !== mergeStrategy ||
      dryRunSnapshot.autoEnrich !== autoEnrich
    );
  })();

  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => onText(String(reader.result || ''));
    reader.readAsText(file);
  }, []);

  const onFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => onText(String(reader.result || ''));
    reader.readAsText(file);
  };

  const exampleCsv =
    'company_name,domain,email,city,phone,sector\n' +
    'Praktijk Mark,praktijkmark.nl,info@praktijkmark.nl,Utrecht,030-1234567,alternatieve_geneeskunde\n' +
    'Skin Studio Aestiva,aestiva.nl,info@aestiva.nl,Amsterdam,020-7654321,cosmetische_behandelaars\n';

  const result = commitMut.data || dryRunMut.data;

  return (
    <div className="max-w-6xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="CRM"
        title="Leads importeren"
        subtitle="Plak CSV of sleep een bestand. Heatr controleert op dubbelen via email → domain → kvk → bedrijfsnaam+stad."
        actions={
          <Link
            to="/leads"
            className="inline-flex items-center gap-2 h-10 px-4 rounded-md text-sm font-medium bg-white border border-[var(--color-border)] hover:bg-[var(--color-ivory-100)]"
          >
            <ArrowLeft className="h-4 w-4" /> Terug
          </Link>
        }
      />

      <div className="grid lg:grid-cols-[1fr_320px] gap-6">
        {/* Input */}
        <div>
          <Card
            className={`p-6 mb-4 border-2 border-dashed transition-colors ${
              dragOver ? 'border-[var(--color-blush-500)] bg-[var(--color-ivory-100)]' : 'border-[var(--color-border)]'
            }`}
            onDrop={onDrop}
            onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
            onDragLeave={() => setDragOver(false)}
          >
            <div className="flex flex-col items-center text-center gap-3">
              <Upload className="h-10 w-10 text-[var(--color-stone-400)]" />
              <div className="text-sm">
                <strong>Sleep CSV hier</strong> of
                <label className="ml-1 text-[var(--color-blush-500)] cursor-pointer hover:underline">
                  kies een bestand
                  <input type="file" accept=".csv,text/csv" className="hidden" onChange={onFileInput} />
                </label>
              </div>
              <div className="text-xs text-[var(--color-stone-500)]">
                Comma- of semicolon-gescheiden, eerste regel = headers.
              </div>
            </div>
          </Card>

          <details className="mb-4">
            <summary className="text-xs cursor-pointer text-[var(--color-stone-600)] hover:text-[var(--color-stone-900)]">
              Of plak hier direct CSV-tekst
            </summary>
            <textarea
              value={rawText}
              onChange={(e) => onText(e.target.value)}
              placeholder="company_name,domain,email,city,phone,sector&#10;..."
              className="mt-2 w-full min-h-[140px] p-3 rounded-md border border-[var(--color-border)] bg-white font-mono text-xs"
            />
          </details>

          {parsed && parsed.warnings.length > 0 && (
            <Card className="p-3 mb-4 border border-[var(--color-warning)] bg-amber-50">
              <div className="flex items-start gap-2 text-xs">
                <AlertTriangle className="h-4 w-4 text-[var(--color-warning)] shrink-0 mt-0.5" />
                <div>{parsed.warnings.join(' · ')}</div>
              </div>
            </Card>
          )}

          {/* Column-mapping UI — verschijnt zodra CSV geparsed is */}
          {parsed && parsed.headers.length > 0 && (
            <Card className="p-4 mb-4">
              <h3 className="font-semibold text-sm mb-1">Kolom-mapping</h3>
              <p className="text-xs text-[var(--color-stone-500)] mb-3">
                Heatr probeert je CSV-kolommen automatisch te matchen met haar lead-velden.
                Pas aan waar nodig. Velden die je niet maps worden genegeerd.
              </p>
              <div className="space-y-1.5 max-h-[260px] overflow-y-auto">
                {parsed.headers.map((csvHeader) => {
                  const sample = parsed.rows[0]?.[csvHeader] || '';
                  return (
                    <div key={csvHeader} className="grid grid-cols-[200px_1fr_180px] gap-2 items-center text-xs">
                      <div className="truncate">
                        <code className="text-[var(--color-stone-700)]">{csvHeader}</code>
                        {sample && (
                          <div className="text-[10px] text-[var(--color-stone-400)] truncate">
                            bv. "{sample.slice(0, 40)}"
                          </div>
                        )}
                      </div>
                      <div className="text-[var(--color-stone-400)]">→</div>
                      <select
                        value={mapping[csvHeader] || ''}
                        onChange={(e) => setMappingFor(csvHeader, e.target.value)}
                        className="h-8 px-2 rounded-md border border-[var(--color-border)] bg-white"
                      >
                        <option value="">(negeer)</option>
                        {ALLOWED_HEADERS.map((h) => (
                          <option key={h} value={h}>{FIELD_LABELS[h]}</option>
                        ))}
                      </select>
                    </div>
                  );
                })}
              </div>
              {!requiredFieldsMet && (
                <div className="mt-3 text-xs text-[var(--color-warning)]">
                  ⚠ Map minstens één van: Email / Website / KvK / Bedrijfsnaam (anders kan dedup niet werken)
                </div>
              )}
            </Card>
          )}

          {parsed && parsed.rows.length > 0 && (
            <Card className="overflow-hidden mb-4">
              <div className="px-4 py-3 border-b border-[var(--color-ivory-200)] flex items-center justify-between flex-wrap gap-3">
                <div>
                  <h3 className="font-semibold text-sm">
                    {fmtInt(parsed.rows.length)} rows gedetecteerd
                  </h3>
                  <p className="text-xs text-[var(--color-stone-500)]">
                    {Object.keys(mapping).length} van {parsed.headers.length} kolommen gemapt
                  </p>
                </div>
                <label className="inline-flex items-center gap-2 text-xs text-[var(--color-stone-700)]">
                  <input
                    type="checkbox"
                    checked={autoEnrich}
                    onChange={(e) => setAutoEnrich(e.target.checked)}
                    className="accent-[var(--color-blush-500)]"
                  />
                  Direct enrichment-pijplijn starten
                  <span className="text-[var(--color-stone-400)]" title="KvK lookup, website intelligence, owner extraction, archetype, scoring">
                    (kvk + website + owner + archetype + scoring)
                  </span>
                </label>
                <label className="inline-flex items-center gap-2 text-xs">
                  Bij dubbel:
                  <select
                    value={mergeStrategy}
                    onChange={(e) => setMergeStrategy(e.target.value as typeof mergeStrategy)}
                    className="h-8 px-2 rounded-md border border-[var(--color-border)] bg-white"
                  >
                    <option value="skip">Skip (negeer)</option>
                    <option value="fill_blanks">Vul lege velden bij</option>
                    <option value="overwrite">Overschrijf alles</option>
                  </select>
                </label>
                <div className="flex gap-2">
                  <button
                    onClick={() => dryRunMut.mutate()}
                    disabled={dryRunMut.isPending || !requiredFieldsMet}
                    className={`text-xs px-3 h-9 rounded-md bg-white border hover:border-[var(--color-blush-400)] disabled:opacity-50 ${
                      dryRunStale
                        ? 'border-[var(--color-warning)] ring-1 ring-amber-300'
                        : 'border-[var(--color-border)]'
                    }`}
                  >
                    {dryRunMut.isPending ? 'Checken…' : dryRunStale ? '⚠ Opnieuw checken' : 'Check dubbelen (dry run)'}
                  </button>
                  <button
                    onClick={() => commitMut.mutate()}
                    disabled={commitMut.isPending || !dryRunMut.data || !requiredFieldsMet || dryRunStale}
                    title={
                      !dryRunMut.data ? 'Eerst dry-run uitvoeren'
                        : dryRunStale ? 'Mapping is gewijzigd na de dry-run — voer opnieuw uit'
                        : ''
                    }
                    className="text-xs px-3 h-9 rounded-md bg-[var(--color-blush-500)] text-white font-medium hover:bg-[var(--color-blush-600)] disabled:opacity-50"
                  >
                    {commitMut.isPending ? 'Importeren…' : 'Importeer leads'}
                  </button>
                </div>
              </div>
              {dryRunStale && (
                <div className="px-4 py-2 bg-amber-50 border-t border-amber-200 text-xs text-[var(--color-stone-700)]">
                  ⚠ Mapping of strategy is gewijzigd sinds de dry-run.
                  De preview hieronder klopt niet meer. Voer dry-run opnieuw uit voordat je commit.
                </div>
              )}
              <div className="max-h-[260px] overflow-auto">
                <table className="w-full text-xs">
                  <thead className="bg-[var(--color-ivory-100)] sticky top-0">
                    <tr>
                      {parsed.headers.map((h) => (
                        <th key={h} className="text-left px-3 py-2 font-semibold text-[var(--color-stone-600)]">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {parsed.rows.slice(0, 20).map((row, i) => (
                      <tr key={i} className="border-b border-[var(--color-ivory-200)]">
                        {parsed.headers.map((h) => (
                          <td key={h} className="px-3 py-1.5 truncate max-w-[200px]">{row[h] || '—'}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
                {parsed.rows.length > 20 && (
                  <div className="text-xs text-[var(--color-stone-500)] px-3 py-2 border-t border-[var(--color-ivory-200)]">
                    +{parsed.rows.length - 20} meer rows…
                  </div>
                )}
              </div>
            </Card>
          )}

          {/* Result */}
          {result && (
            <Card className="p-5">
              <div className="flex items-center gap-2 mb-3">
                {result.fatal ? (
                  <AlertTriangle className="h-5 w-5 text-[var(--color-danger)]" />
                ) : (
                  <CheckCircle2 className="h-5 w-5 text-[var(--color-success)]" />
                )}
                <h3 className="font-semibold">
                  {result.fatal ? 'Import mislukt' : commitMut.data ? 'Geïmporteerd' : 'Dry-run resultaat'}
                </h3>
              </div>
              {result.fatal && (
                <div className="text-sm text-[var(--color-danger)] mb-3">
                  <strong>Fout:</strong> {result.fatal}
                  {result.queue_failure_samples && result.queue_failure_samples.length > 0 && (
                    <details className="mt-2 text-xs">
                      <summary className="cursor-pointer">Eerste {result.queue_failure_samples.length} foutmeldingen</summary>
                      <ul className="mt-1 space-y-1">
                        {result.queue_failure_samples.map((s, i) => (
                          <li key={i} className="font-mono">{s.lead_id}: {s.error}</li>
                        ))}
                      </ul>
                    </details>
                  )}
                </div>
              )}
              {result.idempotent_replay && (
                <div className="text-xs text-[var(--color-stone-600)] bg-blue-50 border border-blue-200 rounded p-2 mb-3">
                  ℹ Replay van een eerdere succesvolle import (zelfde id binnen 24u). Geen nieuwe inserts gedaan.
                </div>
              )}
              {!result.fatal && (
                <>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
                    <Stat label="Geïmporteerd" value={result.imported.length} accent="success" />
                    <Stat label="Dubbelen" value={result.duplicates.length} accent="warning" />
                    <Stat label="Fouten" value={result.errors.length} accent="danger" />
                    <Stat
                      label="In enrichment-queue"
                      value={result.summary?.enrichment_queued_count ?? 0}
                      accent={(result.summary?.enrichment_queued_count ?? 0) > 0 ? 'success' : undefined}
                    />
                  </div>
                  {commitMut.data && (result.summary?.enrichment_queued_count ?? 0) > 0 && (
                    <div className="text-xs text-[var(--color-stone-700)] bg-emerald-50 border border-emerald-200 rounded-md p-3 mb-3">
                      ✨ <strong>{result.summary?.enrichment_queued_count}</strong> leads gaan nu door
                      Heatr's enrichment-pijplijn — dezelfde stappen als bij gescrapte leads:
                      website crawl, KvK, owner-extract, archetype, scoring. Volg in
                      <Link to="/leads" className="ml-1 underline text-[var(--color-blush-500)]">
                        Leads
                      </Link>.
                    </div>
                  )}
                  {/* Cost-preview — alleen tonen bij dry-run en als auto_enrich aan staat */}
                  {dryRunMut.data && !commitMut.data && autoEnrich && result.cost_estimate && result.cost_estimate.row_count > 0 && (
                    <div className="text-xs text-[var(--color-stone-700)] bg-blue-50 border border-blue-200 rounded-md p-3 mb-3 flex items-start gap-2">
                      <span className="shrink-0">💰</span>
                      <div>
                        Verwachte enrichment-cost na commit: <strong>~€{result.cost_estimate.estimated_total_eur.toFixed(3)}</strong>
                        {' '}({result.cost_estimate.row_count} leads × ~€{result.cost_estimate.cost_per_lead_eur}/lead).
                        Loopt door cost-guard — wordt geblokkeerd bij €20/maand cap.
                      </div>
                    </div>
                  )}
                  {/* Merge-summary — als duplicates gemerged zijn */}
                  {commitMut.data && result.duplicates.some((d) => d.merge_action && d.merge_action !== 'skipped') && (
                    <div className="text-xs text-[var(--color-stone-700)] bg-amber-50 border border-amber-200 rounded-md p-3 mb-3">
                      🔀 <strong>{result.duplicates.filter((d) => d.merge_action === result.summary?.merge_strategy).length}</strong> bestaande
                      leads bijgewerkt via <code>{result.summary?.merge_strategy}</code> strategie.
                    </div>
                  )}
                  {result.duplicates.length > 0 && (
                    <details className="text-xs mb-3">
                      <summary className="cursor-pointer">Dubbelen ({result.duplicates.length})</summary>
                      <ul className="mt-2 space-y-1">
                        {result.duplicates.slice(0, 50).map((d, i) => (
                          <li key={i} className="border-b border-[var(--color-ivory-200)] py-1">
                            row {d.row_index} → match op <strong>{d.match_type}</strong> met {d.matched_company || d.matched_lead_id}
                          </li>
                        ))}
                      </ul>
                    </details>
                  )}
                  {result.errors.length > 0 && (
                    <details className="text-xs">
                      <summary className="cursor-pointer text-[var(--color-danger)]">Fouten ({result.errors.length})</summary>
                      <ul className="mt-2 space-y-1">
                        {result.errors.map((e, i) => (
                          <li key={i} className="border-b border-[var(--color-ivory-200)] py-1">
                            row {e.row_index}: {e.error}
                          </li>
                        ))}
                      </ul>
                    </details>
                  )}
                </>
              )}
            </Card>
          )}
        </div>

        {/* Side: example + tips */}
        <div className="space-y-4">
          <Card className="p-5">
            <h3 className="text-xs uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-2 flex items-center gap-1">
              <FileText className="h-3 w-3" /> Voorbeeld-CSV
            </h3>
            <pre className="text-[10px] bg-[var(--color-ivory-100)] p-2 rounded overflow-auto leading-relaxed">{exampleCsv}</pre>
            <button
              onClick={() => navigator.clipboard?.writeText(exampleCsv)}
              className="mt-2 inline-flex items-center gap-1 text-xs text-[var(--color-blush-500)] hover:text-[var(--color-blush-600)]"
            >
              <Copy className="h-3 w-3" /> Kopieer voorbeeld
            </button>
          </Card>
          <Card className="p-5 text-xs text-[var(--color-stone-700)] leading-relaxed">
            <h3 className="text-xs uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-2">
              Dedup-volgorde
            </h3>
            <ol className="list-decimal pl-4 space-y-1">
              <li>Email exact match</li>
              <li>Domain (genormaliseerd)</li>
              <li>KvK-nummer</li>
              <li>Bedrijfsnaam + stad fuzzy ≥92%</li>
            </ol>
            <p className="mt-2 text-[var(--color-stone-500)]">
              Eerst dry-run, dan committen. Cap: 500 rows per import.
            </p>
          </Card>
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value, accent }: { label: string; value: number; accent?: 'success' | 'warning' | 'danger' | undefined }) {
  const color =
    accent === 'success' ? 'text-[var(--color-success)]' :
    accent === 'warning' ? 'text-[var(--color-warning)]' :
    accent === 'danger' ? 'text-[var(--color-danger)]' : 'text-[var(--color-stone-800)]';
  return (
    <div className="rounded-md border border-[var(--color-ivory-200)] p-3 bg-white">
      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">{label}</div>
      <div className={`font-display text-2xl font-semibold ${color}`}>{value}</div>
    </div>
  );
}
