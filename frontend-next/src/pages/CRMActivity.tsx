import { useMemo, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { Calendar, Mail, MessageSquare, Lock, RefreshCw, Info, Filter, Wrench, AlertTriangle, FlaskConical } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtInt, fmtRelative } from '@/lib/format';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { ARCHETYPE_LABELS, SECTOR_LABEL } from '@/lib/types';

interface BoardItem {
  lead_id: string;
  company_name: string | null;
  domain: string | null;
  city: string | null;
  sector: string | null;
  archetype: string | null;
  score: number | null;
  status: string;
  status_reason: string | null;
  status_changed_at: string | null;
  is_manual_override: boolean;
  is_test_lead?: boolean;
  last_outbound_at: string | null;
  last_inbound_at: string | null;
  last_inbound_category: string | null;
}

interface BoardResponse {
  leads: BoardItem[];
  buckets: Record<string, BoardItem[]>;
  total: number;
}

// Volgorde + visual kleuren per status. Naam moet matchen met utils/lead_activity.py CRM_STATUSES.
const STATUS_COLUMNS: { key: string; label: string; color: string; subtitle: string }[] = [
  { key: 'niet_aangeschreven',    label: 'Niet aangeschreven',  color: 'border-stone-300 bg-stone-50',  subtitle: 'klaar voor sequence' },
  { key: 'in_sequence',           label: 'In sequence',         color: 'border-blue-300 bg-blue-50',    subtitle: 'mails lopen' },
  { key: 'cooldown',              label: 'Cooldown',            color: 'border-stone-300 bg-stone-50',  subtitle: '90d na sequence' },
  { key: 'klaar_voor_recontact',  label: 'Klaar voor recontact', color: 'border-amber-300 bg-amber-50', subtitle: 'cooldown voorbij' },
  { key: 'actief_gesprek',        label: 'Actief gesprek',      color: 'border-emerald-300 bg-emerald-50', subtitle: 'positief gereageerd' },
  { key: 'recontact_later',       label: 'Later contacten',     color: 'border-purple-300 bg-purple-50', subtitle: '"niet nu"' },
  { key: 'verkeerde_contact',     label: 'Verkeerde contact',   color: 'border-orange-300 bg-orange-50', subtitle: 'doorverwijzen' },
  { key: 'geen_interesse',        label: 'Geen interesse',      color: 'border-rose-300 bg-rose-50',    subtitle: 'afgesloten' },
  { key: 'afgemeld',              label: 'Afgemeld',            color: 'border-red-400 bg-red-50',      subtitle: 'unsubscribe' },
];

const MANUAL_STATUS_OPTIONS = [
  { value: '', label: '— Geen override —' },
  { value: 'afgemeld', label: 'Afgemeld' },
  { value: 'geen_interesse', label: 'Geen interesse' },
  { value: 'recontact_later', label: 'Later contacten' },
  { value: 'actief_gesprek', label: 'Actief gesprek' },
  { value: 'verkeerde_contact', label: 'Verkeerde contact' },
];

// Hydrate filters uit URL/localStorage zodat ze persisteren over reloads
function loadFilter(key: string): string {
  if (typeof window === 'undefined') return '';
  const url = new URLSearchParams(window.location.search);
  return url.get(key) || window.localStorage.getItem(`crm-filter-${key}`) || '';
}
function saveFilter(key: string, value: string) {
  if (typeof window === 'undefined') return;
  if (value) window.localStorage.setItem(`crm-filter-${key}`, value);
  else window.localStorage.removeItem(`crm-filter-${key}`);
  const url = new URL(window.location.href);
  if (value) url.searchParams.set(key, value);
  else url.searchParams.delete(key);
  window.history.replaceState({}, '', url.toString());
}

export function CRMActivityPage() {
  const qc = useQueryClient();
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [statusForBulk, setStatusForBulk] = useState('');
  const [reason, setReason] = useState('');
  const [recontactDate, setRecontactDate] = useState('');
  const [dragOverCol, setDragOverCol] = useState<string | null>(null);
  const [dragLeadId, setDragLeadId] = useState<string | null>(null);
  // Filters — persisted in URL params + localStorage
  const [filterArchetype, setFilterArchetype] = useState<string>(() => loadFilter('archetype'));
  const [filterSector, setFilterSector] = useState<string>(() => loadFilter('sector'));
  const [filterMinScore, setFilterMinScore] = useState<string>(() => loadFilter('min_score'));

  const updateFilter = (key: string, value: string, setter: (v: string) => void) => {
    setter(value);
    saveFilter(key, value);
    setSelected(new Set());  // Clear selectie bij filter-wijziging om verwarring te voorkomen
  };

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['leads-activity-board'],
    queryFn: () => api.get<BoardResponse>('/crm/activity-board?limit=500'),
    refetchInterval: 60_000,
  });

  const bulkMut = useMutation({
    mutationFn: () =>
      api.post('/leads/bulk-status', {
        lead_ids: Array.from(selected),
        status: statusForBulk,
        reason: reason || null,
        recontact_after: statusForBulk === 'recontact_later' && recontactDate ? recontactDate : undefined,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['leads-activity-board'] });
      setSelected(new Set());
      setReason('');
      setRecontactDate('');
    },
  });

  // Drag-drop status-override: één lead direct verplaatsen naar andere kolom
  const dropMut = useMutation({
    mutationFn: ({ leadId, status, dropReason }: { leadId: string; status: string; dropReason: string }) =>
      api.post('/leads/bulk-status', {
        lead_ids: [leadId],
        status,
        reason: dropReason || null,
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['leads-activity-board'] }),
  });

  // Welke statussen mogen via drag-drop overrided worden? Niet de "auto"-statussen
  // die ALLEEN derived horen te zijn (in_sequence, cooldown, klaar_voor_recontact,
  // niet_aangeschreven). Sleep daarheen heeft geen logische betekenis.
  const DROP_TARGETS = new Set([
    'afgemeld', 'geen_interesse', 'recontact_later',
    'actief_gesprek', 'verkeerde_contact',
  ]);

  const handleDrop = (targetStatus: string, leadId: string | null) => {
    setDragOverCol(null);
    setDragLeadId(null);
    if (!leadId || !DROP_TARGETS.has(targetStatus)) return;
    const dropReason = window.prompt(
      `Override status naar "${targetStatus}". Reden? (optioneel)`,
      '',
    );
    if (dropReason === null) return;  // cancel
    dropMut.mutate({ leadId, status: targetStatus, dropReason });
  };

  const toggle = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  // Apply filters op buckets — server returnt alles, we filteren client-side voor responsiveness
  const buckets = useMemo<Record<string, BoardItem[]>>(() => {
    const raw = data?.buckets || {};
    const minScore = parseInt(filterMinScore || '', 10);
    if (!filterArchetype && !filterSector && !Number.isFinite(minScore)) return raw;
    const filtered: Record<string, BoardItem[]> = {};
    for (const [status, items] of Object.entries(raw)) {
      filtered[status] = items.filter((it) => {
        if (filterArchetype && it.archetype !== filterArchetype) return false;
        if (filterSector && it.sector !== filterSector) return false;
        if (Number.isFinite(minScore) && (it.score ?? 0) < minScore) return false;
        return true;
      });
    }
    return filtered;
  }, [data, filterArchetype, filterSector, filterMinScore]);

  const filteredTotal = useMemo(
    () => Object.values(buckets).reduce((sum, arr) => sum + arr.length, 0),
    [buckets],
  );
  const hasActiveFilter = !!(filterArchetype || filterSector || filterMinScore);

  return (
    <div className="max-w-[1800px] mx-auto px-8 py-8">
      <PageHeader
        eyebrow="CRM"
        title="Activity board"
        subtitle="Status afgeleid uit timeline + reply-classifier. Override handmatig waar nodig."
        actions={
          <div className="flex items-center gap-2">
            <Link
              to="/leads/import"
              className="inline-flex items-center gap-2 h-10 px-4 rounded-md text-sm font-medium bg-white border border-[var(--color-border)] hover:bg-[var(--color-ivory-100)]"
            >
              + Importeer leads
            </Link>
            <button
              onClick={() => refetch()}
              className="inline-flex items-center gap-1 text-xs text-[var(--color-stone-500)] hover:text-[var(--color-blush-500)] h-10 px-3"
            >
              <RefreshCw className="h-3 w-3" /> Vernieuwen
            </button>
          </div>
        }
      />

      {/* Filters */}
      <Card className="p-3 mb-4">
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex items-center gap-1.5 text-xs uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
            <Filter className="h-3 w-3" /> Filter
          </div>
          <select
            value={filterSector}
            onChange={(e) => updateFilter('sector', e.target.value, setFilterSector)}
            className="h-8 px-2 rounded-md border border-[var(--color-border)] bg-white text-xs"
          >
            <option value="">Alle sectoren</option>
            {Object.entries(SECTOR_LABEL).filter(([k]) => !k.includes('archived')).map(([k, label]) => (
              <option key={k} value={k}>{label}</option>
            ))}
          </select>
          <select
            value={filterArchetype}
            onChange={(e) => updateFilter('archetype', e.target.value, setFilterArchetype)}
            className="h-8 px-2 rounded-md border border-[var(--color-border)] bg-white text-xs"
          >
            <option value="">Alle archetypes</option>
            {Object.entries(ARCHETYPE_LABELS).map(([k, m]) => (
              <option key={k} value={k}>{m.label} ({m.sector})</option>
            ))}
          </select>
          <label className="flex items-center gap-1.5 text-xs">
            Score ≥
            <input
              type="number"
              min="0"
              max="100"
              value={filterMinScore}
              onChange={(e) => updateFilter('min_score', e.target.value, setFilterMinScore)}
              placeholder="0"
              className="h-8 w-20 px-2 rounded-md border border-[var(--color-border)] bg-white text-xs"
            />
          </label>
          {hasActiveFilter && (
            <button
              onClick={() => {
                updateFilter('archetype', '', setFilterArchetype);
                updateFilter('sector', '', setFilterSector);
                updateFilter('min_score', '', setFilterMinScore);
              }}
              className="text-xs text-[var(--color-stone-600)] hover:text-[var(--color-blush-500)]"
            >
              Wis filters
            </button>
          )}
          {hasActiveFilter && data && (
            <span className="text-[10px] text-[var(--color-stone-500)] ml-auto tabular-nums">
              {fmtInt(filteredTotal)} van {fmtInt(data.total)} leads zichtbaar
            </span>
          )}
        </div>
      </Card>

      {/* Bulk action bar */}
      {selected.size > 0 && (
        <Card className="p-3 mb-4 border-2 border-[var(--color-blush-400)] bg-[var(--color-ivory-100)] sticky top-2 z-10">
          <div className="flex items-center gap-3 flex-wrap">
            <div className="text-sm font-semibold">
              {selected.size} {selected.size === 1 ? 'lead' : 'leads'} geselecteerd
            </div>
            <select
              value={statusForBulk}
              onChange={(e) => setStatusForBulk(e.target.value)}
              className="h-9 px-2 rounded-md border border-[var(--color-border)] bg-white text-sm"
            >
              {MANUAL_STATUS_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
            {statusForBulk === 'recontact_later' && (
              <input
                type="date"
                value={recontactDate}
                onChange={(e) => setRecontactDate(e.target.value)}
                title="Recontact-datum (leeg = +90 dagen)"
                className="h-9 px-2 rounded-md border border-[var(--color-border)] bg-white text-sm"
              />
            )}
            <input
              type="text"
              value={reason}
              onChange={(e) => setReason(e.target.value)}
              placeholder="Reden (optioneel)"
              className="h-9 px-2 rounded-md border border-[var(--color-border)] bg-white text-sm flex-1 min-w-[200px]"
            />
            <button
              onClick={() => bulkMut.mutate()}
              disabled={bulkMut.isPending}
              className="h-9 px-4 rounded-md bg-[var(--color-blush-500)] text-white text-sm font-medium hover:bg-[var(--color-blush-600)] disabled:opacity-50"
            >
              {bulkMut.isPending ? 'Bezig…' : statusForBulk ? 'Override toepassen' : 'Wis override'}
            </button>
            <button
              onClick={() => setSelected(new Set())}
              className="text-xs text-[var(--color-stone-600)] hover:text-[var(--color-stone-900)]"
            >
              Deselecteer
            </button>
          </div>
        </Card>
      )}

      {isLoading && <div className="skeleton h-96" />}

      {!isLoading && data && (
        <>
          <div className="text-xs text-[var(--color-stone-500)] mb-3">
            {hasActiveFilter ? (
              <>Toont <strong>{fmtInt(filteredTotal)}</strong> van {fmtInt(data.total)} leads (gefilterd)</>
            ) : (
              <>Totaal: <strong>{fmtInt(data.total)}</strong> leads</>
            )}
          </div>
          <div className="text-[10px] text-[var(--color-stone-500)] mb-2">
            💡 Sleep een kaart naar een andere kolom om handmatig te overrulen.
            Sleep-targets: afgemeld, geen interesse, later contacten, actief gesprek,
            verkeerde contact. Auto-statussen (in_sequence, cooldown) zijn niet
            sleep-baar — die volgen uit de timeline.
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
            {STATUS_COLUMNS.map((col) => {
              const items = buckets[col.key] || [];
              const isDropTarget = DROP_TARGETS.has(col.key);
              const isDraggedOver = dragOverCol === col.key && isDropTarget;
              return (
                <div
                  key={col.key}
                  onDragOver={(e) => {
                    if (!isDropTarget) return;
                    e.preventDefault();
                    setDragOverCol(col.key);
                  }}
                  onDragLeave={(e) => {
                    if ((e.relatedTarget as HTMLElement | null)?.closest?.(`[data-col="${col.key}"]`)) return;
                    setDragOverCol((cur) => (cur === col.key ? null : cur));
                  }}
                  onDrop={(e) => {
                    e.preventDefault();
                    handleDrop(col.key, dragLeadId);
                  }}
                  data-col={col.key}
                  className={`rounded-md border-t-2 ${col.color} flex flex-col min-h-[200px] transition-shadow ${
                    isDraggedOver ? 'ring-2 ring-[var(--color-blush-500)] ring-offset-2' : ''
                  } ${dragLeadId && !isDropTarget ? 'opacity-50' : ''}`}
                >
                  <div className="px-3 py-2 border-b border-[var(--color-ivory-200)] bg-white/60">
                    <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-700)]">
                      {col.label}
                    </div>
                    <div className="flex items-center justify-between">
                      <div className="font-display text-2xl font-semibold">{items.length}</div>
                      <div className="text-[10px] text-[var(--color-stone-500)]">{col.subtitle}</div>
                    </div>
                  </div>
                  <div className="flex-1 px-2 py-2 space-y-1.5 max-h-[600px] overflow-y-auto">
                    {items.length === 0 && (
                      <div className="text-[10px] text-[var(--color-stone-400)] italic px-1 py-3 text-center">
                        {isDraggedOver ? '↓ drop hier' : 'leeg'}
                      </div>
                    )}
                    {items.map((it) => (
                      <BoardCard
                        key={it.lead_id}
                        item={it}
                        selected={selected.has(it.lead_id)}
                        onToggle={() => toggle(it.lead_id)}
                        onDragStart={() => setDragLeadId(it.lead_id)}
                        onDragEnd={() => { setDragLeadId(null); setDragOverCol(null); }}
                      />
                    ))}
                  </div>
                </div>
              );
            })}
          </div>
        </>
      )}

      {/* Admin: re-enqueue stale leads */}
      <ReEnqueueAdmin />
    </div>
  );
}

interface ReEnqueueResult {
  matched_count: number;
  fresh_count?: number;
  enqueued_count?: number;
  skipped_already_in_queue?: number;
  failed_count?: number;
  dry_run: boolean;
  message: string;
  sample: { id: string; company_name: string | null; sector: string | null }[];
}

function ReEnqueueAdmin() {
  const qc = useQueryClient();
  const [open, setOpen] = useState(false);
  const [missingField, setMissingField] = useState('archetype');
  const [confirmPhrase, setConfirmPhrase] = useState('');

  // Pre-fetch counts per veld zodat dropdown direct toont waar de meeste pijn zit
  const { data: counts } = useQuery({
    queryKey: ['missing-field-counts'],
    queryFn: () => api.get<{ missing_field_counts: Record<string, number | null> }>('/admin/missing-field-counts'),
    enabled: open,  // alleen ophalen als admin-card geopend is
    staleTime: 60_000,
  });
  const counts_for = (field: string): string => {
    if (!counts || !counts.missing_field_counts) return '';
    const n = counts.missing_field_counts[field];
    if (n === null || n === undefined) return '';
    return ` (${n} missing)`;
  };

  const dryRun = useMutation({
    mutationFn: () =>
      api.post<ReEnqueueResult>('/admin/re-enqueue-stale-leads', {
        dry_run: true,
        missing_field: missingField || null,
        limit: 2000,
      }),
  });
  const execute = useMutation({
    mutationFn: () =>
      api.post<ReEnqueueResult>('/admin/re-enqueue-stale-leads', {
        dry_run: false,
        missing_field: missingField || null,
        limit: 2000,
        confirm_phrase: confirmPhrase,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['leads-activity-board'] });
      setConfirmPhrase('');
    },
  });

  const result = execute.data || dryRun.data;
  const canExecute = confirmPhrase === 'ENQUEUE' && (dryRun.data?.fresh_count ?? 0) > 0;

  return (
    <Card className="mt-8 p-5 border-amber-200 bg-amber-50/30">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between text-left"
      >
        <div className="flex items-center gap-2">
          <Wrench className="h-4 w-4 text-[var(--color-warning)]" />
          <span className="text-sm font-semibold">Admin · Re-enqueue stale leads</span>
        </div>
        <span className="text-xs text-[var(--color-stone-500)]">{open ? 'Verberg' : 'Toon'}</span>
      </button>

      {open && (
        <div className="mt-4 space-y-3">
          <div className="text-xs text-[var(--color-stone-700)] flex items-start gap-2">
            <AlertTriangle className="h-3.5 w-3.5 text-[var(--color-warning)] shrink-0 mt-0.5" />
            <div>
              Zet leads zonder een specifiek veld terug op de enrichment-queue.
              Worker draait dan dezelfde 16 stappen opnieuw. Cost-guard (€20/maand)
              vangt overschrijdingen, maar gebruik dit bewust — kan honderden
              Claude-calls triggeren.
            </div>
          </div>

          <div className="flex items-center gap-3 flex-wrap">
            <label className="text-xs flex items-center gap-2">
              Mist het veld:
              <select
                value={missingField}
                onChange={(e) => { setMissingField(e.target.value); dryRun.reset(); execute.reset(); }}
                className="h-8 px-2 rounded-md border border-[var(--color-border)] bg-white text-xs"
              >
                <option value="archetype">archetype (8 koop-DNA buckets){counts_for('archetype')}</option>
                <option value="treatment_focus">treatment_focus{counts_for('treatment_focus')}</option>
                <option value="company_summary">company_summary{counts_for('company_summary')}</option>
                <option value="kvk_number">kvk_number{counts_for('kvk_number')}</option>
                <option value="latest_review_date">latest_review_date{counts_for('latest_review_date')}</option>
                <option value="domain_age_years">domain_age_years{counts_for('domain_age_years')}</option>
                <option value="contact_first_name">contact_first_name{counts_for('contact_first_name')}</option>
                <option value="personalized_opener">personalized_opener{counts_for('personalized_opener')}</option>
                <option value="">geen filter (ALLE leads)</option>
              </select>
            </label>
            <button
              onClick={() => dryRun.mutate()}
              disabled={dryRun.isPending}
              className="text-xs px-3 h-8 rounded-md bg-white border border-[var(--color-border)] hover:border-[var(--color-blush-400)]"
            >
              {dryRun.isPending ? 'Checken…' : '1. Dry-run (preview)'}
            </button>
          </div>

          {result && (
            <div className="rounded-md border border-[var(--color-ivory-200)] bg-white p-3 text-xs">
              <div className="font-semibold mb-2">{result.message}</div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-[10px]">
                <div><span className="text-[var(--color-stone-500)]">Matched: </span><strong>{result.matched_count}</strong></div>
                {result.fresh_count !== undefined && (
                  <div><span className="text-[var(--color-stone-500)]">Fresh: </span><strong>{result.fresh_count}</strong></div>
                )}
                {result.skipped_already_in_queue !== undefined && (
                  <div><span className="text-[var(--color-stone-500)]">Al in queue: </span><strong>{result.skipped_already_in_queue}</strong></div>
                )}
                {result.enqueued_count !== undefined && (
                  <div><span className="text-emerald-700">Enqueued: </span><strong>{result.enqueued_count}</strong></div>
                )}
              </div>
              {result.sample.length > 0 && (
                <details className="mt-2">
                  <summary className="cursor-pointer text-[var(--color-stone-600)]">Sample ({result.sample.length})</summary>
                  <ul className="mt-1 space-y-0.5">
                    {result.sample.map((s) => (
                      <li key={s.id} className="text-[var(--color-stone-600)]">
                        {s.company_name || s.id} <span className="text-[var(--color-stone-400)]">· {s.sector || '—'}</span>
                      </li>
                    ))}
                  </ul>
                </details>
              )}
            </div>
          )}

          {dryRun.data && (dryRun.data.fresh_count ?? 0) > 0 && (
            <div className="flex items-center gap-2 flex-wrap">
              <label className="text-xs flex items-center gap-2">
                Type <code className="px-1 bg-amber-100 rounded">ENQUEUE</code> om te bevestigen:
                <input
                  type="text"
                  value={confirmPhrase}
                  onChange={(e) => setConfirmPhrase(e.target.value)}
                  placeholder="ENQUEUE"
                  className="h-8 w-32 px-2 rounded-md border border-[var(--color-warning)] bg-white text-xs font-mono"
                />
              </label>
              <button
                onClick={() => execute.mutate()}
                disabled={!canExecute || execute.isPending}
                className="text-xs px-3 h-8 rounded-md bg-[var(--color-warning)] text-white font-medium hover:bg-amber-700 disabled:opacity-40"
              >
                {execute.isPending ? 'Bezig…' : `2. Execute (${dryRun.data.fresh_count} leads)`}
              </button>
            </div>
          )}

          {execute.isError && (
            <div className="text-xs text-[var(--color-danger)]">
              Mislukt — check API log of verwijder type-to-confirm fout.
            </div>
          )}
        </div>
      )}
    </Card>
  );
}

function BoardCard({
  item, selected, onToggle, onDragStart, onDragEnd,
}: {
  item: BoardItem;
  selected: boolean;
  onToggle: () => void;
  onDragStart?: () => void;
  onDragEnd?: () => void;
}) {
  const [showWhy, setShowWhy] = useState(false);
  return (
    <div
      onClick={onToggle}
      draggable
      onDragStart={(e) => { e.dataTransfer.effectAllowed = 'move'; onDragStart?.(); }}
      onDragEnd={onDragEnd}
      className={`rounded-md border bg-white p-2.5 cursor-grab active:cursor-grabbing transition-colors ${
        selected ? 'border-[var(--color-blush-500)] ring-1 ring-[var(--color-blush-300)]' : 'border-[var(--color-ivory-200)] hover:border-[var(--color-blush-400)]'
      }`}
    >
      <div className="flex items-start justify-between gap-2 mb-1">
        <Link
          to={`/leads/${item.lead_id}`}
          onClick={(e) => e.stopPropagation()}
          className="text-xs font-semibold truncate flex-1 hover:text-[var(--color-blush-500)]"
        >
          {item.company_name || item.domain || '—'}
        </Link>
        <div className="flex items-center gap-1 shrink-0">
          {item.is_test_lead && (
            <span
              title="Test-mode actief"
              className="inline-flex items-center text-[9px] font-semibold px-1 py-0.5 rounded bg-amber-100 text-amber-800"
            >
              <FlaskConical className="h-2.5 w-2.5 mr-0.5" /> TEST
            </span>
          )}
          {item.is_manual_override && (
            <Lock className="h-3 w-3 text-[var(--color-warning)]" aria-label="manual override" />
          )}
          <button
            onClick={(e) => { e.stopPropagation(); setShowWhy((v) => !v); }}
            className="text-[var(--color-stone-400)] hover:text-[var(--color-stone-700)]"
            title="Waarom staat deze hier?"
          >
            <Info className="h-3 w-3" />
          </button>
        </div>
      </div>
      <div className="text-[10px] text-[var(--color-stone-500)] mb-2 truncate">
        {item.city || '—'} · score {item.score ?? '—'}
      </div>
      <div className="flex items-center gap-2 text-[10px] text-[var(--color-stone-500)]">
        {item.last_outbound_at && (
          <span className="inline-flex items-center gap-0.5" title={`Laatst verstuurd: ${item.last_outbound_at}`}>
            <Mail className="h-3 w-3" /> {fmtRelative(item.last_outbound_at)}
          </span>
        )}
        {item.last_inbound_at && (
          <span className="inline-flex items-center gap-0.5" title={`Laatste reply: ${item.last_inbound_at}`}>
            <MessageSquare className="h-3 w-3" /> {fmtRelative(item.last_inbound_at)}
          </span>
        )}
        {item.status_changed_at && !item.last_inbound_at && !item.last_outbound_at && (
          <span className="inline-flex items-center gap-0.5">
            <Calendar className="h-3 w-3" /> {fmtRelative(item.status_changed_at)}
          </span>
        )}
      </div>

      {showWhy && (
        <div
          onClick={(e) => e.stopPropagation()}
          className="mt-2 pt-2 border-t border-[var(--color-ivory-200)] text-[10px] text-[var(--color-stone-700)] space-y-1"
        >
          <div>
            <span className="text-[var(--color-stone-500)]">Status:</span>{' '}
            <strong>{item.status}</strong>
            {item.is_manual_override && <span className="ml-1 text-[var(--color-warning)]">(handmatig)</span>}
          </div>
          {item.status_reason && (
            <div>
              <span className="text-[var(--color-stone-500)]">Reden:</span> {item.status_reason}
            </div>
          )}
          {item.status_changed_at && (
            <div>
              <span className="text-[var(--color-stone-500)]">Sinds:</span>{' '}
              {new Date(item.status_changed_at).toLocaleDateString('nl-NL')}{' '}
              <span className="text-[var(--color-stone-400)]">({fmtRelative(item.status_changed_at)})</span>
            </div>
          )}
          {item.last_inbound_category && (
            <div>
              <span className="text-[var(--color-stone-500)]">Laatste reply-classificatie:</span>{' '}
              <code>{item.last_inbound_category}</code>
            </div>
          )}
          {item.archetype && (
            <div>
              <span className="text-[var(--color-stone-500)]">Archetype:</span>{' '}
              <code>{item.archetype}</code>
            </div>
          )}

          {/* Recent activity feed — laatste 5 events, lazy-loaded zodra panel open is */}
          <RecentActivity leadId={item.lead_id} />
        </div>
      )}
    </div>
  );
}

interface CompactEvent {
  id: string;
  event_type: string;
  title: string;
  created_at: string;
}

function RecentActivity({ leadId }: { leadId: string }) {
  const { data, isLoading } = useQuery({
    queryKey: ['lead-activity-compact', leadId],
    queryFn: () => api.get<{ events: CompactEvent[]; count: number }>(`/timeline/${leadId}?compact=true&limit=5`)
      .catch(() => ({ events: [], count: 0 })),
    staleTime: 30_000,  // panel kan vaak open/dicht — niet steeds refetchen
  });

  if (isLoading) {
    return (
      <div className="mt-2 pt-2 border-t border-[var(--color-ivory-200)] text-[var(--color-stone-400)]">
        Laden…
      </div>
    );
  }
  const events = data?.events || [];
  if (events.length === 0) {
    return (
      <div className="mt-2 pt-2 border-t border-[var(--color-ivory-200)]">
        <div className="text-[var(--color-stone-500)] mb-1">Recent activity:</div>
        <div className="text-[var(--color-stone-400)] italic">Geen events</div>
      </div>
    );
  }
  return (
    <div className="mt-2 pt-2 border-t border-[var(--color-ivory-200)]">
      <div className="text-[var(--color-stone-500)] mb-1">Recent activity:</div>
      <ul className="space-y-0.5">
        {events.map((e) => (
          <li key={e.id} className="flex items-baseline gap-1.5">
            <EventIcon type={e.event_type} />
            <span className="truncate flex-1" title={e.title}>{e.title}</span>
            <span className="text-[var(--color-stone-400)] tabular-nums shrink-0">
              {fmtRelative(e.created_at)}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function EventIcon({ type }: { type: string }) {
  // Compact 1-char icon per event-type. Geen Lucide-import voor performance bij N kaarten.
  const map: Record<string, { char: string; cls: string }> = {
    email_sent:              { char: '↗', cls: 'text-[var(--color-stone-600)]' },
    review_email_sent:       { char: '↗', cls: 'text-[var(--color-stone-600)]' },
    reply_received:          { char: '↙', cls: 'text-blue-600' },
    interested:              { char: '✦', cls: 'text-emerald-600' },
    sequence_completed:      { char: '✓', cls: 'text-[var(--color-stone-500)]' },
    manual_status_override:  { char: '⚙', cls: 'text-amber-600' },
    bounced:                 { char: '✗', cls: 'text-[var(--color-danger)]' },
    unsubscribed:            { char: '⊘', cls: 'text-[var(--color-danger)]' },
    stage_changed:           { char: '→', cls: 'text-[var(--color-stone-500)]' },
  };
  const m = map[type] || { char: '·', cls: 'text-[var(--color-stone-400)]' };
  return <span className={`${m.cls} shrink-0 w-2.5 inline-block text-center`}>{m.char}</span>;
}
