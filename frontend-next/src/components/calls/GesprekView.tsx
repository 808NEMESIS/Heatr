import { useMemo, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { ChevronDown, ChevronUp } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtRelative } from '@/lib/format';
import { toast } from '@/lib/toast';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { QueryStateGate } from '@/components/ui/query-state';
import type { CallRecord, CallOutcome } from '@/lib/types';

const OUTCOMES: { key: CallOutcome; label: string; variant: 'success' | 'warning' | 'neutral' | 'danger' }[] = [
  { key: 'won', label: 'Gewonnen', variant: 'success' },
  { key: 'timing', label: 'Timing', variant: 'warning' },
  { key: 'no_value', label: 'Geen waarde (nu)', variant: 'neutral' },
  { key: 'stalled', label: 'Vastgelopen', variant: 'warning' },
  { key: 'hard_no', label: 'Harde nee', variant: 'danger' },
];

const CHECKUP_FIELDS: { key: string; label: string; step?: string }[] = [
  { key: 'unanswered_inbound_per_week', label: 'Gemiste inbound / week' },
  { key: 'response_time_hours', label: 'Reactietijd (uur)' },
  { key: 'value_per_new_patient', label: 'Waarde nieuwe patiënt (€)' },
  { key: 'conversion_estimate', label: 'Conversie (0–1)', step: '0.01' },
  { key: 'no_shows_per_month', label: 'No-shows / maand' },
  { key: 'hours_reception_per_week', label: 'Receptie-uren / week' },
  { key: 'reviews_per_month', label: 'Reviews / maand' },
  { key: 'treatments_per_month', label: 'Behandelingen / maand' },
];

const _label = 'text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]';
const _input = 'h-9 w-full rounded-md border border-[var(--color-border)] bg-white px-3 text-sm';

export function GesprekView({ leadId }: { leadId: string }) {
  const q = useQuery({
    queryKey: ['lead-calls', leadId],
    queryFn: () => api.get<{ calls: CallRecord[] }>(`/calls?lead_id=${leadId}&limit=5`),
  });
  const call = q.data?.calls?.[0];

  return (
    <QueryStateGate isLoading={q.isLoading} isError={q.isError} error={q.error} onRetry={() => q.refetch()}>
      {call ? <CallDetail call={call} leadId={leadId} /> : <CreateCall leadId={leadId} />}
    </QueryStateGate>
  );
}

// --- Nog geen gesprek: plak een transcript ----------------------------------
function CreateCall({ leadId }: { leadId: string }) {
  const qc = useQueryClient();
  const [transcript, setTranscript] = useState('');
  const [date, setDate] = useState(() => new Date().toISOString().slice(0, 10));
  const mut = useMutation({
    mutationFn: () => api.post<CallRecord>('/calls', {
      transcript, call_date: new Date(date).toISOString(), lead_id: leadId,
    }),
    onSuccess: () => { qc.invalidateQueries({ queryKey: ['lead-calls', leadId] }); toast('Gesprek aangemaakt', 'success'); },
  });
  return (
    <Card className="p-5">
      <h3 className="font-display text-lg font-semibold mb-1">Gespreksverslag</h3>
      <p className="text-sm text-[var(--color-stone-500)] mb-4">Plak het transcript van het gesprek. Daarna kies je de uitkomst en vul je de cijfers in.</p>
      <div className="flex flex-col gap-3">
        <div>
          <label className={_label}>Datum</label>
          <Input type="date" value={date} onChange={(e) => setDate(e.target.value)} className="mt-1" />
        </div>
        <div>
          <label className={_label}>Transcript</label>
          <textarea value={transcript} onChange={(e) => setTranscript(e.target.value)}
            rows={10} className={_input + ' mt-1 h-auto py-2 font-mono text-xs leading-relaxed'}
            placeholder="Plak hier het gesprekstranscript…" />
        </div>
        <div>
          <Button disabled={!transcript.trim() || mut.isPending} onClick={() => mut.mutate()}>
            {mut.isPending ? 'Aanmaken…' : 'Gesprek aanmaken'}
          </Button>
        </div>
      </div>
    </Card>
  );
}

// --- Bestaand gesprek: transcript + uitkomst + rapport + cadans --------------
function CallDetail({ call, leadId }: { call: CallRecord; leadId: string }) {
  const [openTranscript, setOpenTranscript] = useState(false);
  return (
    <div className="flex flex-col gap-4">
      {/* transcript, inklapbaar */}
      <Card className="p-4">
        <button onClick={() => setOpenTranscript((o) => !o)}
          className="flex w-full items-center justify-between text-sm font-medium">
          <span>Transcript · {fmtRelative(call.call_date)}</span>
          {openTranscript ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
        </button>
        {openTranscript && (
          <pre className="mt-3 whitespace-pre-wrap font-mono text-xs leading-relaxed text-[var(--color-stone-700)]">{call.transcript}</pre>
        )}
      </Card>

      {call.outcome ? <OutcomeSummary call={call} /> : <OutcomeForm call={call} leadId={leadId} />}

      {call.outcome && call.outcome !== 'won' && call.outcome !== 'hard_no' && (
        <ReportSection call={call} leadId={leadId} />
      )}
      {call.outcome && <CadenceStatus call={call} />}
    </div>
  );
}

function OutcomeSummary({ call }: { call: CallRecord }) {
  const o = OUTCOMES.find((x) => x.key === call.outcome);
  const cd = call.report_status; // (om lint tevreden te houden — niet gebruikt hier)
  void cd;
  return (
    <Card className="p-4">
      <div className="flex items-center gap-2">
        <span className={_label}>Uitkomst</span>
        <Badge variant={o?.variant ?? 'neutral'}>{o?.label ?? call.outcome}</Badge>
        {call.timing_target_date && (
          <span className="text-xs text-[var(--color-stone-500)]">richtdatum {call.timing_target_date}</span>
        )}
      </div>
      {call.outcome_note && <p className="mt-2 text-sm text-[var(--color-stone-600)]">{call.outcome_note}</p>}
    </Card>
  );
}

// --- Gate 1: uitkomst kiezen + cijfers invullen -----------------------------
function OutcomeForm({ call, leadId }: { call: CallRecord; leadId: string }) {
  const qc = useQueryClient();
  const [outcome, setOutcome] = useState<CallOutcome | null>(null);
  const [note, setNote] = useState('');
  const [target, setTarget] = useState('');
  const [form, setForm] = useState<Record<string, string>>({});

  const checkup = useMemo(() => {
    const entries = Object.entries(form).filter(([, v]) => v.trim() !== '').map(([k, v]) => [k, Number(v)]);
    return Object.fromEntries(entries);
  }, [form]);
  const hasCheckup = Object.keys(checkup).length > 0;

  const mut = useMutation({
    mutationFn: () => api.patch<CallRecord>(`/calls/${call.id}/outcome`, {
      outcome, outcome_note: note || undefined,
      timing_target_date: outcome === 'timing' && target ? target : undefined,
      checkup_data: hasCheckup ? { ...checkup, source: 'call', captured_at: new Date().toISOString().slice(0, 10) } : undefined,
    }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['lead-calls', leadId] });
      qc.invalidateQueries({ queryKey: ['lead', leadId] });
      qc.invalidateQueries({ queryKey: ['lead-timeline', leadId] });
      toast('Uitkomst opgeslagen', 'success');
    },
  });

  return (
    <Card className="p-5">
      <h3 className="font-display text-lg font-semibold mb-1">Uitkomst</h3>
      <p className="text-sm text-[var(--color-stone-500)] mb-4">Kies de uitkomst van dit gesprek. Dit bepaalt of er een rapport en cadans volgen.</p>

      <div className="flex flex-wrap gap-2 mb-5">
        {OUTCOMES.map((o) => (
          <button key={o.key} onClick={() => setOutcome(o.key)}
            className={
              'px-3 py-1.5 text-sm rounded-md border transition-colors font-medium ' +
              (outcome === o.key
                ? 'bg-[var(--color-blush-100)] border-[var(--color-blush-500)] text-[var(--color-blush-700)]'
                : 'bg-white border-[var(--color-border)] text-[var(--color-stone-600)] hover:border-[var(--color-blush-400)]')
            }>
            {o.label}
          </button>
        ))}
      </div>

      {outcome === 'timing' && (
        <div className="mb-4 max-w-xs">
          <label className={_label}>Richtdatum (prospect noemde zelf een moment)</label>
          <Input type="date" value={target} onChange={(e) => setTarget(e.target.value)} className="mt-1" />
        </div>
      )}

      {outcome && outcome !== 'won' && outcome !== 'hard_no' && (
        <div className="mb-4">
          <div className="flex items-baseline justify-between mb-2">
            <label className={_label}>Cijfers uit het gesprek</label>
            {!hasCheckup && <span className="text-xs text-[var(--color-warning)]">Leeg = geen rapport, alleen de cadans loopt.</span>}
          </div>
          <div className="grid grid-cols-2 gap-3">
            {CHECKUP_FIELDS.map((f) => (
              <div key={f.key}>
                <label className="text-xs text-[var(--color-stone-500)]">{f.label}</label>
                <Input type="number" step={f.step} inputMode="decimal"
                  value={form[f.key] ?? ''} onChange={(e) => setForm((s) => ({ ...s, [f.key]: e.target.value }))}
                  className="mt-1 h-9" />
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="mb-4">
        <label className={_label}>Notitie (optioneel)</label>
        <textarea value={note} onChange={(e) => setNote(e.target.value)} rows={2}
          className={_input + ' mt-1 h-auto py-2'} placeholder="Wat je hoorde tussen de regels door…" />
      </div>

      <Button disabled={!outcome || mut.isPending} onClick={() => mut.mutate()}>
        {mut.isPending ? 'Opslaan…' : 'Uitkomst opslaan'}
      </Button>
    </Card>
  );
}

// --- Rapport: genereren + gate 2 (vrijgeven/weggooien) ----------------------
function ReportSection({ call, leadId }: { call: CallRecord; leadId: string }) {
  const qc = useQueryClient();
  const [editHtml, setEditHtml] = useState<string | null>(null);
  const invalidate = () => qc.invalidateQueries({ queryKey: ['lead-calls', leadId] });

  const gen = useMutation({
    mutationFn: () => api.post<{ report_status: string; qa_failed?: string }>(`/calls/${call.id}/generate-report`),
    onSuccess: (r) => {
      invalidate();
      if (r.qa_failed) toast(`QA-gate afgekeurd: ${r.qa_failed}`, 'error');
      else if (r.report_status === 'skipped') toast('Geen cijfers ingevuld — geen rapport (skipped)', 'info');
      else toast('Rapport gegenereerd', 'success');
    },
  });
  const patch = useMutation({
    mutationFn: (body: { action: 'approve' | 'discard'; report_html?: string }) => api.patch(`/calls/${call.id}/report`, body),
    onSuccess: () => { invalidate(); setEditHtml(null); },
  });

  if (call.report_status === 'skipped') {
    return <Card className="p-4 text-sm text-[var(--color-stone-500)]">Geen cijfers ingevuld, dus geen rapport. Alleen de cadans loopt.</Card>;
  }
  if (call.report_status === 'sent') {
    return <Card className="p-4 text-sm"><Badge variant="success">Rapport verzonden</Badge> {call.report_sent_at && <span className="text-[var(--color-stone-500)] ml-2">{fmtRelative(call.report_sent_at)}</span>}</Card>;
  }
  if (call.report_status === 'pending') {
    return (
      <Card className="p-4">
        <p className="text-sm text-[var(--color-stone-500)] mb-3">Nog geen rapport. Genereer er een op basis van transcript, cijfers en website-analyse.</p>
        <Button disabled={gen.isPending} onClick={() => gen.mutate()}>{gen.isPending ? 'Genereren…' : 'Rapport genereren'}</Button>
      </Card>
    );
  }

  // generated | approved → preview + gate 2
  const html = editHtml ?? call.report_html ?? '';
  return (
    <Card className="p-5">
      <div className="flex items-center justify-between mb-3">
        <h3 className="font-display text-lg font-semibold">Rapport</h3>
        <Badge variant={call.report_status === 'approved' ? 'success' : 'accent'}>
          {call.report_status === 'approved' ? 'Vrijgegeven' : 'Concept'}
        </Badge>
      </div>

      <div className="rounded-lg border border-[var(--color-ivory-200)] bg-[var(--color-panel-2,#fbfafe)] p-4 mb-3"
        dangerouslySetInnerHTML={{ __html: html }} />

      <details className="mb-3">
        <summary className="cursor-pointer text-xs text-[var(--color-blush-600)]">HTML bewerken</summary>
        <textarea value={html} onChange={(e) => setEditHtml(e.target.value)} rows={8}
          className={_input + ' mt-2 h-auto py-2 font-mono text-xs'} />
      </details>

      <div className="flex gap-2">
        <Button disabled={patch.isPending} onClick={() => patch.mutate({ action: 'approve', report_html: editHtml ?? undefined })}>
          Vrijgeven
        </Button>
        <Button variant="outline" disabled={patch.isPending} onClick={() => patch.mutate({ action: 'discard' })}>
          Weggooien
        </Button>
      </div>
      {patch.isError && <p className="mt-2 text-xs text-[var(--color-danger)]">{(patch.error as Error)?.message}</p>}
    </Card>
  );
}

function CadenceStatus({ call }: { call: CallRecord }) {
  const label: Record<string, string> = {
    none: 'geen cadans', scheduled: 'gepland', sent: 'verstuurd', replied: 'gereageerd', exhausted: 'afgerond',
  };
  return (
    <Card className="p-4 text-sm">
      <span className={_label}>Retarget</span>
      <div className="mt-1 flex items-center gap-3 text-[var(--color-stone-600)]">
        <Badge variant={call.retarget_status === 'replied' ? 'success' : call.retarget_status === 'exhausted' ? 'neutral' : 'info'}>
          {label[call.retarget_status] ?? call.retarget_status}
        </Badge>
        {call.retarget_due_at && <span>gepland {fmtRelative(call.retarget_due_at)}</span>}
        <span className="text-[var(--color-stone-400)]">poging {call.retarget_attempt}</span>
      </div>
    </Card>
  );
}
