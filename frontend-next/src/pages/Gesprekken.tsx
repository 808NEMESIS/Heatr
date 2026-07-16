import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { AlertTriangle } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtRelative } from '@/lib/format';
import { toast } from '@/lib/toast';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { QueryStateGate } from '@/components/ui/query-state';
import type { CallRecord, CallOutcome, ReportStatus } from '@/lib/types';

const OUTCOME_LABEL: Record<CallOutcome, { label: string; variant: 'success' | 'warning' | 'neutral' | 'danger' }> = {
  won: { label: 'Gewonnen', variant: 'success' },
  timing: { label: 'Timing', variant: 'warning' },
  no_value: { label: 'Geen waarde', variant: 'neutral' },
  stalled: { label: 'Vastgelopen', variant: 'warning' },
  hard_no: { label: 'Harde nee', variant: 'danger' },
};
const REPORT_LABEL: Record<ReportStatus, { label: string; variant: 'neutral' | 'accent' | 'success' | 'warning' }> = {
  pending: { label: 'Geen rapport', variant: 'neutral' },
  generated: { label: 'Concept', variant: 'accent' },
  approved: { label: 'Vrijgegeven', variant: 'success' },
  sent: { label: 'Verzonden', variant: 'success' },
  skipped: { label: 'Overgeslagen', variant: 'warning' },
};

const FILTERS = [
  { key: 'alle', label: 'Alle' },
  { key: 'generated', label: 'Concept' },
  { key: 'approved', label: 'Vrijgegeven' },
  { key: 'sent', label: 'Verzonden' },
];

export function GesprekkenPage() {
  const [filter, setFilter] = useState('alle');
  const list = useQuery({
    queryKey: ['calls', filter],
    queryFn: () => api.get<{ calls: CallRecord[] }>(
      '/calls?limit=200' + (filter !== 'alle' ? `&report_status=${filter}` : ''),
    ),
  });
  const unmatched = useQuery({
    queryKey: ['calls-unmatched'],
    queryFn: () => api.get<{ calls: CallRecord[] }>('/calls/unmatched'),
  });

  const calls = list.data?.calls || [];
  const unm = unmatched.data?.calls || [];

  return (
    <div className="max-w-5xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Check-up follow-up"
        title="Gesprekken"
        subtitle="Elk gesprek dat niet op een ja eindigt: kies de uitkomst, vul de cijfers, en het rapport wordt de aanleiding om terug te komen."
        actions={
          <Tabs value={filter} onValueChange={setFilter}>
            <TabsList>
              {FILTERS.map((f) => <TabsTrigger key={f.key} value={f.key}>{f.label}</TabsTrigger>)}
            </TabsList>
          </Tabs>
        }
      />

      {unm.length > 0 && <UnmatchedBanner calls={unm} />}

      <QueryStateGate isLoading={list.isLoading} isError={list.isError} error={list.error}
        onRetry={() => list.refetch()} isEmpty={calls.length === 0}
        emptyLabel="Nog geen gesprekken" emptyHint="Gesprekken verschijnen hier zodra je een transcript toevoegt of Zoom een opname doorstuurt.">
        <Card className="overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-[var(--color-ivory-200)]">
                  {['Bedrijf', 'Datum', 'Uitkomst', 'Rapport', 'Retarget'].map((h) => (
                    <th key={h} className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--color-ivory-200)]">
                {calls.map((c) => (
                  <tr key={c.id} className="hover:bg-[var(--color-ivory-50)]">
                    <td className="px-4 py-3 font-medium">
                      {c.lead_id
                        ? <Link to={`/leads/${c.lead_id}`} className="text-[var(--color-blush-700)] hover:underline">{c.company_name || 'lead'}</Link>
                        : <span className="text-[var(--color-stone-400)]">niet gekoppeld</span>}
                    </td>
                    <td className="px-4 py-3 text-[var(--color-stone-500)] text-xs">{fmtRelative(c.call_date)}</td>
                    <td className="px-4 py-3">{c.outcome ? <Badge variant={OUTCOME_LABEL[c.outcome].variant}>{OUTCOME_LABEL[c.outcome].label}</Badge> : <span className="text-xs text-[var(--color-stone-400)]">nog kiezen</span>}</td>
                    <td className="px-4 py-3"><Badge variant={REPORT_LABEL[c.report_status].variant}>{REPORT_LABEL[c.report_status].label}</Badge></td>
                    <td className="px-4 py-3 text-xs text-[var(--color-stone-500)]">
                      {c.retarget_status !== 'none' ? c.retarget_status : '—'}
                      {c.retarget_due_at && <span className="ml-1">· {fmtRelative(c.retarget_due_at)}</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card>
      </QueryStateGate>
    </div>
  );
}

// --- Banner voor gesprekken zonder gekoppelde lead (fail-closed matching) ----
function UnmatchedBanner({ calls }: { calls: CallRecord[] }) {
  return (
    <Card className="mb-5 border-[var(--color-warning)] p-4">
      <div className="flex items-center gap-2 mb-3">
        <AlertTriangle className="h-4 w-4 text-[var(--color-warning)]" />
        <span className="font-medium">{calls.length} gesprek{calls.length > 1 ? 'ken' : ''} zonder lead</span>
        <span className="text-xs text-[var(--color-stone-500)]">— koppel handmatig (geen automatische naam-match)</span>
      </div>
      <div className="flex flex-col gap-2">
        {calls.map((c) => <UnmatchedRow key={c.id} call={c} />)}
      </div>
    </Card>
  );
}

function UnmatchedRow({ call }: { call: CallRecord }) {
  const qc = useQueryClient();
  const [leadId, setLeadId] = useState('');
  const [open, setOpen] = useState(false);
  const match = useMutation({
    mutationFn: () => api.patch(`/calls/${call.id}/match`, { lead_id: leadId.trim() }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['calls-unmatched'] });
      qc.invalidateQueries({ queryKey: ['calls'] });
      toast('Gesprek gekoppeld', 'success');
    },
    onError: (e: Error) => toast(e.message, 'error'),
  });
  const emails = (call.participants || []).map((p) => (typeof p === 'string' ? p : (p as { email?: string })?.email)).filter(Boolean);
  return (
    <div className="rounded-md border border-[var(--color-ivory-200)] bg-white px-3 py-2 text-sm">
      <div className="flex items-center justify-between gap-2">
        <span className="text-[var(--color-stone-600)]">
          {fmtRelative(call.call_date)}
          {emails.length > 0 && <span className="text-xs text-[var(--color-stone-400)] ml-2">{emails.join(', ')}</span>}
        </span>
        <Button variant="outline" size="xs" onClick={() => setOpen((o) => !o)}>Koppel</Button>
      </div>
      {open && (
        <div className="mt-2 flex items-center gap-2">
          <Input value={leadId} onChange={(e) => setLeadId(e.target.value)} placeholder="lead-id" className="h-8" />
          <Button size="sm" disabled={!leadId.trim() || match.isPending} onClick={() => match.mutate()}>
            {match.isPending ? '…' : 'Koppel'}
          </Button>
        </div>
      )}
    </div>
  );
}
