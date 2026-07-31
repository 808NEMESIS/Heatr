import { useMemo, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { ArrowRight, RefreshCw, Sparkles } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtInt, fmtRelative } from '@/lib/format';
import type { JobsResponse, ScrapingJob } from '@/lib/types';
import { SECTOR_LABEL } from '@/lib/types';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/cn';

interface SubcategoryEntry {
  key: string;
  label: string;
  primary_keyword: string;
  all_keywords: string[];
}
interface SectorEntry {
  key: string;
  label: string;
  subcategories: SubcategoryEntry[];
}

interface GroupedJob {
  sector: string | null;
  city: string | null;
  jobs: ScrapingJob[];
  latest: string;
  total_found: number;
  total_new: number;
  completed: number;
  failed: number;
  active: number;
}

interface ScrapingLive {
  counters: { pending: number; running: number; completed_24h: number; failed_24h: number; total_companies_24h: number };
  recent_companies: { id: string; company_name: string; city?: string | null; domain?: string | null; google_rating?: number | null; created_at: string }[];
  by_sector_city: { sector: string; city: string; count: number }[];
}

interface Schedule {
  id: string; sector: string; city: string; frequency_days: number;
  next_run_at: string | null; last_run_at: string | null; active: boolean;
}

export function ZoekenPage() {
  const [sector, setSector] = useState<'cosmetische_behandelaars' | 'alternatieve_geneeskunde'>(
    'cosmetische_behandelaars'
  );
  const [city, setCity] = useState('');
  const [selectedSubKeys, setSelectedSubKeys] = useState<Set<string>>(new Set());
  const [customQuery, setCustomQuery] = useState('');
  const [showCustom, setShowCustom] = useState(false);
  const [maxResults, setMaxResults] = useState(60);
  const [srcMaps, setSrcMaps] = useState(true);
  const [srcDirs, setSrcDirs] = useState(true);
  const [err, setErr] = useState<string | null>(null);
  const [schedCity, setSchedCity] = useState('');
  const [schedFreq, setSchedFreq] = useState(14);
  const qc = useQueryClient();

  const { data: sectorsFull } = useQuery({
    queryKey: ['sectors-full'],
    queryFn: () => api.get<{ sectors: SectorEntry[] }>('/sectors/full'),
    staleTime: 60_000,
  });

  const { data: jobsData, isLoading, refetch } = useQuery({
    queryKey: ['scraping-jobs'],
    queryFn: () => api.get<JobsResponse>('/jobs?limit=200&type=scraping'),
    refetchInterval: 6000,
  });
  const { data: live } = useQuery({
    queryKey: ['scraping-live'],
    queryFn: () => api.get<ScrapingLive>('/analytics/scraping-live').catch(() => null),
    refetchInterval: 4000,
  });
  const { data: schedules } = useQuery({
    queryKey: ['discovery-schedules'],
    queryFn: () => api.get<{ schedules: Schedule[] }>('/discovery-schedules').catch(() => null),
  });
  const invalidateSched = () => qc.invalidateQueries({ queryKey: ['discovery-schedules'] });
  const createSched = useMutation({
    mutationFn: () => api.post('/discovery-schedules', { sector, city: schedCity.trim(), frequency_days: schedFreq }),
    onSuccess: () => { invalidateSched(); setSchedCity(''); },
  });
  const pauseSched = useMutation({ mutationFn: (id: string) => api.post(`/discovery-schedules/${id}/pause`), onSuccess: invalidateSched });
  const resumeSched = useMutation({ mutationFn: (id: string) => api.post(`/discovery-schedules/${id}/resume`), onSuccess: invalidateSched });
  const delSched = useMutation({ mutationFn: (id: string) => api.del(`/discovery-schedules/${id}`), onSuccess: invalidateSched });

  const startMutation = useMutation({
    mutationFn: (payload: unknown) => api.post<{ jobs: { query: string; source: string }[]; total_jobs: number }>('/search', payload),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['scraping-jobs'] });
      setErr(null);
      setSelectedSubKeys(new Set());
      setCustomQuery('');
    },
    onError: (e: Error) => setErr(e.message),
  });

  // Active sector's subcategorieën
  const activeSubs = useMemo(() => {
    return sectorsFull?.sectors.find((s) => s.key === sector)?.subcategories || [];
  }, [sectorsFull, sector]);

  const toggleSub = (key: string) => {
    setSelectedSubKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  // Live job-preview: welke queries gaan we starten?
  const plannedQueries = useMemo<string[]>(() => {
    if (customQuery.trim()) return [customQuery.trim()];
    return activeSubs
      .filter((s) => selectedSubKeys.has(s.key))
      .map((s) => s.primary_keyword);
  }, [customQuery, activeSubs, selectedSubKeys]);

  const sourceCount = (srcMaps ? 1 : 0) + (srcDirs ? 1 : 0);
  const plannedJobs = plannedQueries.length * Math.max(sourceCount, 1);
  const plannedVolume = plannedJobs * maxResults;

  const jobs = jobsData?.jobs || [];
  const totalDone = jobs.filter((j) => j.status === 'completed').length;
  const totalFound = jobs.reduce((s, j) => s + (j.total_found || 0), 0);
  const totalNew = jobs.reduce((s, j) => s + (j.total_new || 0), 0);
  const active = jobs.filter((j) => j.status === 'running' || j.status === 'pending').length;

  const groups: GroupedJob[] = (() => {
    const map = new Map<string, GroupedJob>();
    for (const j of jobs) {
      const key = `${j.sector || '?'}|${j.city || '?'}`;
      if (!map.has(key)) {
        map.set(key, {
          sector: j.sector, city: j.city, jobs: [],
          latest: j.created_at, total_found: 0, total_new: 0,
          completed: 0, failed: 0, active: 0,
        });
      }
      const g = map.get(key)!;
      g.jobs.push(j);
      if (j.created_at > g.latest) g.latest = j.created_at;
      g.total_found += j.total_found || 0;
      g.total_new += j.total_new || 0;
      if (j.status === 'completed') g.completed++;
      else if (j.status === 'failed') g.failed++;
      else g.active++;
    }
    return Array.from(map.values()).sort((a, b) => {
      if (b.active !== a.active) return b.active - a.active;
      return b.latest.localeCompare(a.latest);
    });
  })();

  const submit = () => {
    if (!city.trim()) { setErr('Vul een stad of regio in.'); return; }
    if (!customQuery.trim() && selectedSubKeys.size === 0) {
      setErr('Kies minstens één specialisme of vul een eigen zoekterm in.');
      return;
    }
    if (!srcMaps && !srcDirs) {
      setErr('Selecteer minstens één bron (Google Maps of Branchegidsen).');
      return;
    }
    setErr(null);
    startMutation.mutate({
      sector,
      city: city.trim(),
      subcategory_keys: customQuery.trim() ? [] : Array.from(selectedSubKeys),
      custom_query: customQuery.trim() || null,
      max_results: maxResults,
      sources: { google_maps: srcMaps, directories: srcDirs },
    });
  };

  return (
    <div className="max-w-6xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Lead discovery"
        title="Ontdek je volgende"
        titleAccent="gouden leads."
        subtitle="Scrape Google Maps + branchegidsen voor cosmetische en alternatieve behandelaars. Start een zoekopdracht links, zie resultaten rechts binnendruppelen."
      />

      {/* Stats strip */}
      <Card className="grid grid-cols-2 md:grid-cols-5 divide-x divide-[var(--color-border)] overflow-hidden mb-8">
        <Stat label="Jobs totaal" value={fmtInt(jobs.length)} sub="alle zoekopdrachten" />
        <Stat label="Completed" value={fmtInt(totalDone)} sub="afgerond" accent="success" />
        <Stat label="Bedrijven" value={fmtInt(totalFound)} sub="in database" accent="primary" />
        <Stat label="Unieke leads" value={fmtInt(totalNew)} sub="na dedup" />
        <Stat label="Actief" value={fmtInt(active)} sub="running / pending" accent="warning" />
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-[440px_1fr] gap-7">
        {/* Form */}
        <div>
          <Card className="p-7 sticky top-6">
            <h2 className="font-display text-xl font-semibold mb-1">Nieuwe zoekopdracht</h2>
            <p className="text-sm text-[var(--color-stone-500)] mb-5">
              Kies sector → specialismen → stad. Per specialisme starten we een aparte
              Google-query — dat geeft scherpere lead-fits dan één vage sector-zoekterm.
            </p>

            {err && (
              <div className="mb-4 rounded-md bg-[var(--color-danger-bg)] text-[var(--color-danger)] px-3 py-2 text-sm">
                {err}
              </div>
            )}

            <Field label="Sector">
              <div className="flex gap-2 flex-wrap">
                {(['cosmetische_behandelaars', 'alternatieve_geneeskunde'] as const).map((s) => (
                  <button
                    key={s}
                    onClick={() => { setSector(s); setSelectedSubKeys(new Set()); }}
                    className={cn(
                      'px-3 py-2 rounded-md text-sm font-medium border transition-colors',
                      sector === s
                        ? 'bg-[var(--color-blush-100)] border-[var(--color-blush-500)] text-[var(--color-blush-700)]'
                        : 'bg-white border-[var(--color-border)] text-[var(--color-stone-600)] hover:border-[var(--color-blush-400)]'
                    )}
                  >
                    {SECTOR_LABEL[s]}
                  </button>
                ))}
              </div>
            </Field>

            <Field label={`Specialisme (${selectedSubKeys.size}/${activeSubs.length})`}>
              <div className="flex flex-wrap gap-1.5">
                {activeSubs.map((s) => {
                  const on = selectedSubKeys.has(s.key);
                  return (
                    <button
                      key={s.key}
                      onClick={() => toggleSub(s.key)}
                      title={`Query: "${s.primary_keyword}"`}
                      className={cn(
                        'text-xs px-2.5 py-1.5 rounded-md border transition-colors',
                        on
                          ? 'bg-[var(--color-blush-500)] border-[var(--color-blush-500)] text-white'
                          : 'bg-white border-[var(--color-border)] text-[var(--color-stone-700)] hover:border-[var(--color-blush-400)]',
                        customQuery.trim() && 'opacity-40 pointer-events-none'
                      )}
                    >
                      {s.label}
                    </button>
                  );
                })}
              </div>
              <div className="text-[10px] text-[var(--color-stone-500)] mt-2">
                Hover over een chip om de exact Google-query te zien.
                Selecteer 1-3 voor scherpe resultaten.
              </div>
            </Field>

            <Field label="Stad of regio">
              <Input
                placeholder="Amsterdam, Utrecht, Groningen…"
                value={city}
                onChange={(e) => setCity(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
              />
            </Field>

            <div className="mb-4">
              <button
                type="button"
                onClick={() => setShowCustom((v) => !v)}
                className="text-xs text-[var(--color-stone-600)] hover:text-[var(--color-blush-500)]"
              >
                {showCustom ? '× Verberg' : '+ Eigen zoekterm (override)'}
              </button>
              {showCustom && (
                <div className="mt-2">
                  <Input
                    placeholder='bv. "botox kliniek centrum" of "huidtherapeut wellness"'
                    value={customQuery}
                    onChange={(e) => setCustomQuery(e.target.value)}
                  />
                  <div className="text-[10px] text-[var(--color-stone-500)] mt-1">
                    Vervangt specialisme-keuze. Voor experimenten met scherpere termen.
                  </div>
                </div>
              )}
            </div>

            <Field label="Max. resultaten">
              <select
                value={maxResults}
                onChange={(e) => setMaxResults(Number(e.target.value))}
                className="h-10 w-full rounded-md border border-[var(--color-border)] bg-white px-3 text-sm"
              >
                <option value={20}>20 leads</option>
                <option value={40}>40 leads</option>
                <option value={60}>60 leads (Google Maps cap)</option>
                <option value={100}>100 leads</option>
              </select>
            </Field>

            <Field label="Bronnen">
              <div className="flex gap-4">
                <label className="inline-flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={srcMaps} onChange={(e) => setSrcMaps(e.target.checked)} className="accent-[var(--color-blush-500)]" />
                  Google Maps
                </label>
                <label className="inline-flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={srcDirs} onChange={(e) => setSrcDirs(e.target.checked)} className="accent-[var(--color-blush-500)]" />
                  Branchegidsen
                </label>
              </div>
            </Field>

            {plannedQueries.length > 0 && city.trim() && (
              <div className="mb-4 rounded-md border border-[var(--color-ivory-200)] bg-[var(--color-ivory-100)] p-3">
                <div className="flex items-center gap-1.5 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-700)] mb-2">
                  <Sparkles className="h-3 w-3" />
                  We starten {plannedJobs} job{plannedJobs === 1 ? '' : 's'}
                </div>
                <ul className="space-y-1 text-xs text-[var(--color-stone-700)]">
                  {plannedQueries.map((q) => (
                    <li key={q} className="font-mono">
                      → "{q} {city.trim()}"
                    </li>
                  ))}
                </ul>
                <div className="text-[10px] text-[var(--color-stone-500)] mt-2 pt-2 border-t border-[var(--color-ivory-200)]">
                  Bronnen: {[srcMaps && 'Google Maps', srcDirs && 'Branchegidsen'].filter(Boolean).join(' + ') || '—'} ·
                  Verwacht ~{fmtInt(plannedVolume)} leads totaal
                </div>
              </div>
            )}

            {startMutation.isSuccess && startMutation.data && (
              <div className="mb-4 rounded-md bg-green-50 border border-[var(--color-success)] p-3 text-xs text-[var(--color-stone-700)]">
                ✓ {startMutation.data.total_jobs} job(s) gestart. Eerste resultaten verschijnen rechts binnen 1-2 minuten.
              </div>
            )}

            <button
              onClick={submit}
              disabled={startMutation.isPending}
              className="w-full h-12 rounded-md bg-[var(--color-blush-500)] text-white font-semibold flex items-center justify-center gap-2 hover:bg-[var(--color-blush-600)] transition-colors disabled:opacity-60 mt-2"
            >
              {startMutation.isPending ? 'Starten…' : `Zoekopdracht starten${plannedJobs > 0 ? ` (${plannedJobs})` : ''}`}
              <ArrowRight className="h-4 w-4" />
            </button>
          </Card>

          {/* Herhaal-scrapes (discovery-schedules) */}
          <Card className="p-5 mt-4">
            <h3 className="font-display text-base font-semibold mb-1">Herhaal-scrapes</h3>
            <p className="text-xs text-[var(--color-stone-500)] mb-3">
              Periodiek automatisch scrapen. Sector = de bovenaan gekozen sector ({SECTOR_LABEL[sector]}).
            </p>
            <div className="flex gap-2 mb-3">
              <Input placeholder="Stad" value={schedCity} onChange={(e) => setSchedCity(e.target.value)} className="flex-1" />
              <input
                type="number" min={1} value={schedFreq}
                onChange={(e) => setSchedFreq(Math.max(1, Number(e.target.value)))}
                title="Elke X dagen" className="w-16 h-10 rounded-md border border-[var(--color-border)] px-2 text-sm text-center"
              />
              <button
                onClick={() => createSched.mutate()}
                disabled={!schedCity.trim() || createSched.isPending}
                title={`Elke ${schedFreq} dagen`}
                className="h-10 px-4 rounded-md bg-[var(--color-blush-500)] text-white text-sm font-medium disabled:opacity-50"
              >+</button>
            </div>
            {(schedules?.schedules || []).length === 0 ? (
              <p className="text-xs text-[var(--color-stone-400)]">Nog geen herhaal-scrapes.</p>
            ) : (
              <div className="space-y-1.5">
                {schedules!.schedules.map((s) => (
                  <div key={s.id} className="flex items-center justify-between gap-2 text-xs border-b border-[var(--color-ivory-200)] pb-1.5 last:border-b-0">
                    <div className="min-w-0">
                      <div className="font-medium truncate">{SECTOR_LABEL[s.sector] || s.sector} · {s.city}</div>
                      <div className="text-[var(--color-stone-500)]">
                        elke {s.frequency_days}d · volgende {s.next_run_at ? fmtRelative(s.next_run_at) : '—'}
                      </div>
                    </div>
                    <div className="flex items-center gap-2 shrink-0">
                      <Badge variant={s.active ? 'success' : 'neutral'}>{s.active ? 'actief' : 'pauze'}</Badge>
                      <button
                        onClick={() => (s.active ? pauseSched : resumeSched).mutate(s.id)}
                        className="text-[var(--color-blush-500)] hover:underline"
                      >{s.active ? 'pauze' : 'hervat'}</button>
                      <button onClick={() => delSched.mutate(s.id)} className="text-[var(--color-danger)] hover:underline" title="Verwijderen">×</button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        </div>

        {/* History */}
        <div>
          {live && live.recent_companies.length > 0 && (
            <Card className="p-4 mb-4">
              <div className="flex items-center justify-between mb-2">
                <h3 className="font-display text-sm font-semibold">Live binnenkomend (24u)</h3>
                <span className="text-xs text-[var(--color-stone-500)] tabular-nums">
                  {fmtInt(live.counters.total_companies_24h)} bedrijven · {live.counters.running} running
                </span>
              </div>
              <div className="divide-y divide-[var(--color-ivory-200)]">
                {live.recent_companies.slice(0, 8).map((c) => (
                  <div key={c.id} className="py-1.5 flex items-center justify-between gap-3 text-sm">
                    <div className="min-w-0 truncate">
                      <span className="font-medium">{c.company_name}</span>
                      {c.city && <span className="text-xs text-[var(--color-stone-500)]"> · {c.city}</span>}
                    </div>
                    <div className="text-xs text-[var(--color-stone-500)] shrink-0 tabular-nums">
                      {c.google_rating ? `${c.google_rating}★ · ` : ''}{fmtRelative(c.created_at)}
                    </div>
                  </div>
                ))}
              </div>
            </Card>
          )}
          <div className="flex items-baseline justify-between mb-3">
            <h2 className="font-display text-xl font-semibold">Gedane zoekopdrachten</h2>
            <button
              onClick={() => refetch()}
              className="inline-flex items-center gap-1.5 text-xs text-[var(--color-stone-500)] hover:text-[var(--color-blush-500)]"
            >
              <RefreshCw className="h-3 w-3" /> Vernieuwen
            </button>
          </div>
          <p className="text-sm text-[var(--color-stone-500)] mb-4">
            Gegroepeerd per sector × stad — zie waar je al hebt gescrapet.
          </p>

          {isLoading && (
            <div className="space-y-2">
              {Array.from({ length: 5 }).map((_, i) => <div key={i} className="skeleton h-14" />)}
            </div>
          )}

          {!isLoading && groups.length === 0 && (
            <Card className="p-12 text-center text-[var(--color-stone-500)]">
              <div className="font-display text-4xl text-[var(--color-stone-200)] mb-2">∅</div>
              Nog geen zoekopdrachten. Start er een links.
            </Card>
          )}

          {!isLoading && groups.length > 0 && (
            <Card className="overflow-hidden">
              {groups.map((g, i) => (
                <div
                  key={`${g.sector}-${g.city}-${i}`}
                  className={cn(
                    'grid grid-cols-[1fr_110px_120px_110px] gap-5 items-center px-5 py-3.5 border-b border-[var(--color-ivory-200)] last:border-b-0 hover:bg-[var(--color-ivory-50)] transition-colors',
                    g.active > 0 && 'bg-[#f0f7ff] border-l-[3px] border-l-[var(--color-info)] pl-[calc(1.25rem-3px)]'
                  )}
                >
                  <div>
                    <div className="font-medium text-sm">
                      {SECTOR_LABEL[g.sector || ''] || g.sector} · {g.city}
                    </div>
                    <div className="text-xs mono text-[var(--color-stone-500)] mt-0.5">
                      {g.jobs.length} run{g.jobs.length === 1 ? '' : 's'} · laatst {fmtRelative(g.latest)}
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="font-display text-[22px] font-semibold leading-none">{fmtInt(g.total_new)}</div>
                    <div className="text-[10px] uppercase tracking-wider text-[var(--color-stone-500)] mt-1">nieuw</div>
                  </div>
                  <div className="text-right text-xs text-[var(--color-stone-500)] tabular-nums">
                    {fmtInt(g.total_found)} gevonden
                  </div>
                  <div className="text-right">
                    {g.active > 0 ? (
                      <Badge variant="info">
                        <span className="w-1.5 h-1.5 rounded-full bg-current pulse-dot" />
                        {g.active} actief
                      </Badge>
                    ) : g.failed > 0 ? (
                      <Badge variant="danger">{g.failed} failed</Badge>
                    ) : (
                      <Badge variant="success">✓ {g.completed}×</Badge>
                    )}
                  </div>
                </div>
              ))}
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value, sub, accent }: { label: string; value: string; sub?: string; accent?: 'primary' | 'success' | 'warning' }) {
  const color =
    accent === 'primary' ? 'text-[var(--color-blush-500)]' :
    accent === 'success' ? 'text-[var(--color-success)]' :
    accent === 'warning' ? 'text-[var(--color-warning)]' :
    'text-[var(--color-stone-800)]';
  return (
    <div className="p-5">
      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1.5">{label}</div>
      <div className={cn('font-display text-3xl font-semibold leading-none tracking-tight', color)}>{value}</div>
      {sub && <div className="text-[11px] text-[var(--color-stone-500)] mt-1.5">{sub}</div>}
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="mb-4">
      <label className="block text-[11px] font-semibold uppercase tracking-wider text-[var(--color-stone-800)] mb-2">{label}</label>
      {children}
    </div>
  );
}
