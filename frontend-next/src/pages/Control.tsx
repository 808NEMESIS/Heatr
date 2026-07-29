import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { api } from '@/lib/api';
import { fmtRelative } from '@/lib/format';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { QueryStateGate } from '@/components/ui/query-state';

interface OutboundRecord {
  id: string;
  idempotency_key: string;
  kind: string;
  status: 'completed' | 'failed' | 'blocked_compliance' | 'skipped_duplicate';
  actor: string;
  lead_id: string | null;
  error: string | null;
  created_at: string;
  metadata?: Record<string, unknown>;
}

interface LedgerResponse {
  records: OutboundRecord[];
  count: number;
  warning?: string;
}

interface QueueHealth {
  total_jobs: number;
  recent_24h: number;
  completion_rate_pct: number;
  fail_rate_pct: number;
  status_counts: Record<string, number>;
  retry_distribution: Record<string, number>;
  top_failing_steps: { step: string; fail_count: number }[];
  diagnostic_hints: string[];
}

interface ComplianceFlag {
  id: string;
  flag_type: string;
  lead_id: string | null;
  campaign_id: string | null;
  detail: string | null;
  created_at: string;
}

interface DueSend {
  id: string;
  lead_id: string | null;
  step_index?: number | null;
  next_send_at?: string | null;
  leads?: { company_name?: string | null; email?: string | null } | null;
}

const STATUS_BADGE: Record<OutboundRecord['status'], 'success' | 'danger' | 'warning' | 'neutral'> = {
  completed: 'success',
  failed: 'danger',
  blocked_compliance: 'danger',
  skipped_duplicate: 'warning',
};

const STATUS_FILTERS = [
  { value: '', label: 'Alles' },
  { value: 'completed', label: 'Completed' },
  { value: 'failed', label: 'Failed' },
  { value: 'blocked_compliance', label: 'Blocked' },
  { value: 'skipped_duplicate', label: 'Duplicates' },
];

/**
 * Control Plane — overzichtslaag. Het append-only outbound-ledger (elke
 * side-effect via de dispatcher, ook geblokkeerde en geskipte pogingen)
 * + queue-health. Per-lead detail: LeadDetail → Run-tab.
 */
export function ControlPage() {
  const [statusFilter, setStatusFilter] = useState('');

  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['control-outbound', statusFilter],
    queryFn: () =>
      api.get<LedgerResponse>(
        `/control/outbound?limit=100${statusFilter ? `&status=${statusFilter}` : ''}`
      ),
    refetchInterval: 15_000,
  });

  const { data: queueHealth } = useQuery({
    queryKey: ['queue-health'],
    queryFn: () => api.get<QueueHealth>('/analytics/queue-health'),
    refetchInterval: 30_000,
  });

  const qc = useQueryClient();
  const { data: flagsData } = useQuery({
    queryKey: ['compliance-flags'],
    queryFn: () => api.get<{ flags: ComplianceFlag[] }>('/compliance/flags'),
    refetchInterval: 30_000,
  });
  const ackMut = useMutation({
    mutationFn: (flag_ids: string[]) => api.post('/compliance/flags/acknowledge', { flag_ids }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['compliance-flags'] }),
  });
  const flags = flagsData?.flags;

  const { data: dueSends } = useQuery({
    queryKey: ['due-sends'],
    queryFn: () => api.get<{ due_sends: DueSend[]; count: number }>('/sequences/due-sends'),
    refetchInterval: 30_000,
  });

  const records = data?.records || [];

  return (
    <div className="max-w-7xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Control Plane"
        title="Control"
        subtitle="Elke outbound side-effect via de dispatcher — ook geblokkeerd, geskipt en gefaald. Append-only."
      />

      {/* Compliance-vlaggen — fail-closed drip-blokkade. Bovenaan: één open vlag legt de hele drip stil. */}
      {flags && flags.length > 0 ? (
        <Card className="p-4 mb-6 border-2 border-[var(--color-danger)] bg-[var(--color-danger-bg)]">
          <h3 className="font-display text-sm font-semibold text-[var(--color-danger)] mb-3">
            ⚠ {flags.length} open compliance-vlag{flags.length > 1 ? 'gen' : ''} — drip geblokkeerd tot afgehandeld
          </h3>
          <div className="space-y-2">
            {flags.map((f) => (
              <div key={f.id} className="flex items-center justify-between gap-3 text-sm">
                <div className="min-w-0 flex items-center gap-2 flex-wrap">
                  <Badge variant="danger">{f.flag_type}</Badge>
                  {f.lead_id && (
                    <Link to={`/leads/${f.lead_id}`} className="text-[var(--color-blush-500)] hover:underline font-mono text-xs">
                      {f.lead_id.slice(0, 8)}…
                    </Link>
                  )}
                  {f.detail && <span className="text-[var(--color-stone-600)] text-xs truncate">{f.detail}</span>}
                </div>
                <button
                  onClick={() => ackMut.mutate([f.id])}
                  disabled={ackMut.isPending}
                  className="shrink-0 rounded-md border border-[var(--color-border)] bg-white px-3 py-1 text-xs hover:bg-[var(--color-ivory-100)] disabled:opacity-50"
                >
                  Afhandelen
                </button>
              </div>
            ))}
          </div>
        </Card>
      ) : flags ? (
        <Card className="p-3 mb-6 text-xs text-[var(--color-success)] border-[var(--color-success)]">
          ✓ Geen open compliance-vlaggen — de drip is niet geblokkeerd.
        </Card>
      ) : null}

      {data?.warning && (
        <Card className="p-4 mb-5 border-[var(--color-warning)] text-sm text-[var(--color-warning)]">
          ⚠ {data.warning}
        </Card>
      )}

      {queueHealth && (
        <section className="mb-6 space-y-3">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            {Object.entries(queueHealth.status_counts || {}).map(([status, n]) => (
              <Card key={status} className="p-4">
                <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">{status}</div>
                <div className="mt-1 text-2xl font-display font-semibold tabular-nums">{n}</div>
              </Card>
            ))}
          </div>
          <div className="flex flex-wrap items-center gap-2 text-xs">
            <Badge variant="neutral">{queueHealth.total_jobs} jobs</Badge>
            <Badge variant="neutral">{queueHealth.recent_24h} in 24u</Badge>
            <Badge variant={queueHealth.completion_rate_pct >= 80 ? 'success' : 'neutral'}>
              {queueHealth.completion_rate_pct}% completed
            </Badge>
            <Badge variant={queueHealth.fail_rate_pct > 20 ? 'danger' : queueHealth.fail_rate_pct > 0 ? 'warning' : 'success'}>
              {queueHealth.fail_rate_pct}% failed
            </Badge>
          </div>
          {queueHealth.top_failing_steps?.length > 0 && (
            <Card className="p-4">
              <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-2">Top failing steps</div>
              <div className="flex flex-wrap gap-3 text-xs tabular-nums">
                {queueHealth.top_failing_steps.map((s) => (
                  <span key={s.step}>
                    <span className="text-[var(--color-stone-700)]">{s.step}</span>
                    <span className="text-[var(--color-danger)]"> ×{s.fail_count}</span>
                  </span>
                ))}
              </div>
            </Card>
          )}
          {queueHealth.diagnostic_hints?.length > 0 && (
            <Card className="p-4 border-[var(--color-warning)]">
              <div className="text-xs text-[var(--color-warning)] space-y-1">
                {queueHealth.diagnostic_hints.map((h, i) => <div key={i}>⚠ {h}</div>)}
              </div>
            </Card>
          )}
        </section>
      )}

      {/* Due-sends outbox — wat de sequence-engine ná arming zou versturen (read-only). */}
      <section className="mb-6">
        <Card className="overflow-hidden">
          <div className="px-4 py-3 border-b border-[var(--color-border)] flex items-center justify-between">
            <h3 className="font-display text-sm font-semibold">Klaar om te versturen (outbox)</h3>
            <span className="text-xs text-[var(--color-stone-500)] tabular-nums">{dueSends?.count ?? 0} due</span>
          </div>
          {(dueSends?.due_sends?.length ?? 0) === 0 ? (
            <div className="p-6 text-center text-xs text-[var(--color-stone-500)]">
              Niets due — geen wachtende sequence-sends (onder de kill-switch verwacht).
            </div>
          ) : (
            <div className="divide-y divide-[var(--color-ivory-200)]">
              {dueSends!.due_sends.map((d) => (
                <div key={d.id} className="px-4 py-2.5 flex items-center justify-between gap-3 text-sm">
                  <div className="min-w-0 truncate">
                    {d.lead_id ? (
                      <Link to={`/leads/${d.lead_id}`} className="text-[var(--color-blush-500)] hover:underline">
                        {d.leads?.company_name || `${d.lead_id.slice(0, 8)}…`}
                      </Link>
                    ) : '—'}
                    {d.leads?.email && <span className="text-xs text-[var(--color-stone-500)]"> · {d.leads.email}</span>}
                  </div>
                  <div className="shrink-0 flex items-center gap-2 text-xs text-[var(--color-stone-500)]">
                    {d.step_index != null && <Badge variant="neutral">stap {d.step_index}</Badge>}
                    {d.next_send_at && <span>{fmtRelative(d.next_send_at)}</span>}
                  </div>
                </div>
              ))}
            </div>
          )}
        </Card>
      </section>

      <Card className="overflow-hidden">
        <div className="flex items-center gap-1.5 p-4 border-b border-[var(--color-border)]">
          {STATUS_FILTERS.map((f) => (
            <button
              key={f.value}
              onClick={() => setStatusFilter(f.value)}
              className={
                statusFilter === f.value
                  ? 'px-3 py-1.5 text-xs rounded-md border font-medium bg-[var(--color-blush-100)] border-[var(--color-blush-500)] text-[var(--color-blush-700)]'
                  : 'px-3 py-1.5 text-xs rounded-md border font-medium bg-white border-[var(--color-border)] text-[var(--color-stone-600)] hover:border-[var(--color-blush-400)]'
              }
            >
              {f.label}
            </button>
          ))}
        </div>

        <QueryStateGate
          isLoading={isLoading}
          isError={isError}
          error={error}
          onRetry={() => refetch()}
          isEmpty={records.length === 0}
          emptyLabel="Nog geen outbound-records"
          emptyHint="Records verschijnen zodra een side-effect via de dispatcher loopt (en migratie 020 gedraaid is)."
        >
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[var(--color-border)] bg-[var(--color-ivory-100)]">
                  {['Tijd', 'Kind', 'Status', 'Actor', 'Lead', 'Key / fout'].map((h) => (
                    <th key={h} className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {records.map((r) => (
                  <tr key={r.id} className="border-b border-[var(--color-ivory-200)] hover:bg-[var(--color-ivory-50)]">
                    <td className="px-4 py-2.5 whitespace-nowrap text-xs text-[var(--color-stone-500)]">{fmtRelative(r.created_at)}</td>
                    <td className="px-4 py-2.5 text-xs">{r.kind}</td>
                    <td className="px-4 py-2.5"><Badge variant={STATUS_BADGE[r.status] || 'neutral'}>{r.status}</Badge></td>
                    <td className="px-4 py-2.5 text-xs text-[var(--color-stone-600)]">{r.actor}</td>
                    <td className="px-4 py-2.5 text-xs">
                      {r.lead_id ? (
                        <Link to={`/leads/${r.lead_id}`} className="text-[var(--color-blush-500)] hover:underline">
                          {r.lead_id.slice(0, 8)}…
                        </Link>
                      ) : '—'}
                    </td>
                    <td className="px-4 py-2.5 text-xs font-mono max-w-md truncate" title={r.error || r.idempotency_key}>
                      {r.error ? (
                        <span className="text-[var(--color-danger)]">{r.error}</span>
                      ) : r.idempotency_key}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </QueryStateGate>
      </Card>
    </div>
  );
}
