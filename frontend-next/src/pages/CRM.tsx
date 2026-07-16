import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import {
  Target,
  Workflow,
  CheckCircle2,
  Clock,
  ArrowUpRight,
  CircleDot,
  AlertCircle,
} from 'lucide-react';
import { api } from '@/lib/api';
import { fmtEuro, fmtInt, fmtRelative } from '@/lib/format';
import type { Lead } from '@/lib/types';
import { SECTOR_LABEL } from '@/lib/types';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { QueryStateGate } from '@/components/ui/query-state';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { cn } from '@/lib/cn';

interface CrmStats {
  open_tasks_today?: number;
  pipeline_count?: number;
  won_this_month?: number;
}

interface Task {
  id: string;
  title: string;
  task_type: string;
  priority: 'low' | 'medium' | 'high' | 'urgent';
  due_date: string | null;
  lead_id: string;
  leads?: { company_name?: string; city?: string };
}

// Gesprek-uitkomst → badge (check-up follow-up, migratie 032)
const CALL_OUTCOME_META: Record<string, { label: string; variant: 'success' | 'warning' | 'neutral' | 'danger' }> = {
  won: { label: 'Gewonnen', variant: 'success' },
  timing: { label: 'Timing', variant: 'warning' },
  no_value: { label: 'Geen waarde', variant: 'neutral' },
  stalled: { label: 'Vastgelopen', variant: 'warning' },
  hard_no: { label: 'Harde nee', variant: 'danger' },
};

interface Stage {
  key: string;
  label: string;
  accent?: 'success' | 'danger' | 'neutral' | 'warning';
}
const STAGES: Stage[] = [
  { key: 'ontdekt', label: 'Ontdekt' },
  { key: 'review_verstuurd', label: 'Review verstuurd' },
  { key: 'gereageerd', label: 'Gereageerd', accent: 'warning' },
  { key: 'gesprek_gepland', label: 'Gesprek gepland', accent: 'warning' },
  { key: 'offerte_verstuurd', label: 'Offerte verstuurd', accent: 'warning' },
  { key: 'gewonnen', label: 'Gewonnen', accent: 'success' },
  { key: 'verloren', label: 'Verloren', accent: 'danger' },
  { key: 'later', label: 'Later', accent: 'neutral' },
];

export function CRMPage() {
  const [view, setView] = useState<'focus' | 'kanban' | 'lijst'>('focus');

  const { data: stats } = useQuery({
    queryKey: ['crm-stats'],
    queryFn: () => api.get<CrmStats>('/crm/stats'),
    refetchInterval: 30_000,
  });

  const openTasks = stats?.open_tasks_today ?? 0;
  const pipeline = stats?.pipeline_count ?? 0;
  const won = stats?.won_this_month ?? 0;
  const hasData = openTasks > 0 || pipeline > 0;

  return (
    <div className="max-w-7xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Pipeline"
        title="CRM"
        subtitle="Je taken, pipeline en gewonnen deals op één plek."
        actions={
          <Tabs value={view} onValueChange={(v) => setView(v as typeof view)}>
            <TabsList>
              <TabsTrigger value="focus">Focus</TabsTrigger>
              <TabsTrigger value="kanban">Kanban</TabsTrigger>
              <TabsTrigger value="lijst">Lijst</TabsTrigger>
            </TabsList>
          </Tabs>
        }
      />

      {/* Top stats */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        <StatCard
          icon={CheckCircle2}
          label="Open taken vandaag"
          value={fmtInt(openTasks)}
          accent={openTasks > 0 ? 'danger' : 'neutral'}
        />
        <StatCard icon={Workflow} label="In pipeline" value={fmtInt(pipeline)} accent="primary" />
        <StatCard icon={Target} label="Gewonnen deze maand" value={fmtEuro(won)} accent="success" />
      </div>

      {!hasData ? (
        <EmptyState />
      ) : (
        <Tabs value={view} onValueChange={(v) => setView(v as typeof view)}>
          <TabsContent value="focus"><FocusView /></TabsContent>
          <TabsContent value="kanban"><KanbanView /></TabsContent>
          <TabsContent value="lijst"><LijstView /></TabsContent>
        </Tabs>
      )}
    </div>
  );
}

function EmptyState() {
  return (
    <Card className="p-12 text-center">
      <div className="font-display text-5xl text-[var(--color-stone-200)] mb-4">⬡</div>
      <h2 className="font-display text-2xl font-semibold mb-2">Nog geen leads in je pipeline</h2>
      <p className="text-sm text-[var(--color-stone-500)] max-w-md mx-auto mb-8">
        Het CRM toont je taken, deals en pipeline-voortgang. Pak je eerste leads op en zet ze in beweging.
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 max-w-2xl mx-auto text-left">
        <CtaCard to="/leads" icon="◉" title="Leads bekijken" subtitle="Scan je database" />
        <CtaCard to="/website-kansen" icon="◎" title="Website kansen" subtitle="Grootste pijnpunten" />
        <CtaCard to="/zoeken" icon="⊕" title="Nieuwe zoekopdracht" subtitle="Meer leads scrapen" />
      </div>
    </Card>
  );
}

function CtaCard({ to, icon, title, subtitle }: { to: string; icon: string; title: string; subtitle: string }) {
  return (
    <Link
      to={to}
      className="block rounded-lg border border-[var(--color-border)] bg-[var(--color-ivory-50)] p-4 hover:border-[var(--color-blush-400)] transition-colors"
    >
      <div className="font-medium text-sm mb-1">{icon} {title}</div>
      <div className="text-xs text-[var(--color-stone-500)]">{subtitle}</div>
    </Link>
  );
}

function StatCard({
  icon: Icon, label, value, accent,
}: {
  icon: typeof Target;
  label: string;
  value: string;
  accent?: 'primary' | 'success' | 'danger' | 'warning' | 'neutral';
}) {
  const colorMap: Record<string, string> = {
    primary: 'text-[var(--color-blush-500)]',
    success: 'text-[var(--color-success)]',
    danger: 'text-[var(--color-danger)]',
    warning: 'text-[var(--color-warning)]',
    neutral: 'text-[var(--color-stone-400)]',
  };
  return (
    <Card className="p-5">
      <div className="flex items-start justify-between mb-3">
        <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">{label}</div>
        <Icon className={cn('h-4 w-4', colorMap[accent || 'neutral'])} />
      </div>
      <div className={cn('font-display text-3xl font-semibold leading-none', colorMap[accent || ''])}>
        {value}
      </div>
    </Card>
  );
}

function FocusView() {
  const { data: tasks, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['tasks-open'],
    queryFn: () => api.get<{ tasks: Task[] }>('/tasks?status=open'),
  });
  const { data: activity } = useQuery({
    queryKey: ['crm-timeline'],
    queryFn: () =>
      api.get<{ events: { id: string; event_type: string; title: string; body?: string; created_at: string; company_name?: string; lead_id?: string }[] }>('/crm/timeline/recent'),
    refetchInterval: 60_000,
  });

  if (isError) {
    return (
      <QueryStateGate isLoading={false} isError error={error} onRetry={() => refetch()}>
        {null}
      </QueryStateGate>
    );
  }

  const list = tasks?.tasks || [];
  const now = new Date();
  const todayEnd = new Date(now); todayEnd.setHours(23, 59, 59, 999);
  const weekEnd = new Date(now); weekEnd.setDate(now.getDate() + 7);

  const today = list.filter((t) => t.due_date && new Date(t.due_date) <= todayEnd);
  const week = list.filter((t) => t.due_date && new Date(t.due_date) > todayEnd && new Date(t.due_date) <= weekEnd);
  const later = list.filter((t) => !t.due_date || new Date(t.due_date) > weekEnd);

  return (
    <div className="grid grid-cols-1 lg:grid-cols-[1fr_380px] gap-6">
      <div className="space-y-5">
        <TaskSection title="Vandaag & achterstallig" tasks={today} loading={isLoading} urgent showEmptyOK />
        <TaskSection title="Deze week" tasks={week} loading={isLoading} />
        <TaskSection title="Later" tasks={later} loading={isLoading} collapsed />
      </div>

      <div>
        <h3 className="font-display text-lg font-semibold mb-3">Recente activiteit</h3>
        <Card className="overflow-hidden">
          {(activity?.events || []).length === 0 ? (
            <div className="p-8 text-center text-sm text-[var(--color-stone-500)]">Nog geen activiteit.</div>
          ) : (
            <div className="divide-y divide-[var(--color-ivory-200)]">
              {(activity?.events || []).slice(0, 12).map((e) => (
                <div key={e.id} className="p-4 hover:bg-[var(--color-ivory-50)]">
                  <div className="text-sm font-medium">{e.title}</div>
                  {e.company_name && (
                    <div className="text-xs text-[var(--color-blush-500)] mt-0.5">{e.company_name}</div>
                  )}
                  {e.body && <div className="text-xs text-[var(--color-stone-500)] mt-1">{e.body}</div>}
                  <div className="text-xs text-[var(--color-stone-400)] mt-1.5">{fmtRelative(e.created_at)}</div>
                </div>
              ))}
            </div>
          )}
        </Card>
      </div>
    </div>
  );
}

function TaskSection({
  title, tasks, loading, urgent, collapsed, showEmptyOK,
}: {
  title: string;
  tasks: Task[];
  loading: boolean;
  urgent?: boolean;
  collapsed?: boolean;
  showEmptyOK?: boolean;
}) {
  const [open, setOpen] = useState(!collapsed);
  return (
    <div>
      <div
        className="flex items-center justify-between mb-2 cursor-pointer select-none"
        onClick={() => setOpen(!open)}
      >
        <div className="flex items-center gap-2">
          <h3 className={cn('font-display text-lg font-semibold', urgent && tasks.length > 0 && 'text-[var(--color-danger)]')}>
            {title}
          </h3>
          <Badge variant={urgent && tasks.length > 0 ? 'danger' : 'neutral'}>{tasks.length}</Badge>
        </div>
        <span className="text-xs text-[var(--color-stone-400)]">{open ? '▾' : '▸'}</span>
      </div>
      {open && (
        <Card className="overflow-hidden">
          {loading ? (
            <div className="p-4 space-y-2">
              <div className="skeleton h-10" />
              <div className="skeleton h-10" />
            </div>
          ) : tasks.length === 0 ? (
            <div className="p-6 text-center text-sm text-[var(--color-stone-500)]">
              {showEmptyOK ? 'Niets te doen vandaag 🎉' : 'Geen taken.'}
            </div>
          ) : (
            <div className="divide-y divide-[var(--color-ivory-200)]">
              {tasks.map((t) => <TaskRow key={t.id} task={t} />)}
            </div>
          )}
        </Card>
      )}
    </div>
  );
}

function TaskRow({ task }: { task: Task }) {
  const due = task.due_date ? new Date(task.due_date) : null;
  const overdue = due && due < new Date();
  const dueStr = due ? due.toLocaleDateString('nl-NL', { day: 'numeric', month: 'short' }) : null;
  const prioColor: Record<string, string> = {
    urgent: 'bg-[var(--color-danger)]',
    high: 'bg-[var(--color-warning)]',
    medium: 'bg-[var(--color-info)]',
    low: 'bg-[var(--color-stone-300)]',
  };
  return (
    <div className="flex items-center gap-3 px-4 py-3 hover:bg-[var(--color-ivory-50)]">
      <span className={cn('w-1.5 h-1.5 rounded-full shrink-0', prioColor[task.priority] || prioColor.medium)} />
      <div className="flex-1 min-w-0">
        <div className="text-sm font-medium truncate">{task.title}</div>
        {task.leads?.company_name && (
          <div className="text-xs text-[var(--color-blush-500)] mt-0.5">
            {task.leads.company_name}{task.leads.city && ` · ${task.leads.city}`}
          </div>
        )}
      </div>
      {dueStr && (
        <span className={cn('text-xs tabular-nums shrink-0', overdue ? 'text-[var(--color-danger)]' : 'text-[var(--color-stone-500)]')}>
          {overdue && '⚠ '}{dueStr}
        </span>
      )}
    </div>
  );
}

function KanbanView() {
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['crm-pipeline'],
    queryFn: () => api.get<{ stages: Record<string, Lead[]> }>('/crm/pipeline'),
  });

  const stagesData: Record<string, Lead[]> = data?.stages || {};
  const total = Object.values(stagesData).reduce((s: number, arr) => s + (Array.isArray(arr) ? arr.length : 0), 0);

  if (isLoading) {
    return (
      <div className="grid grid-cols-4 gap-3">
        {Array.from({ length: 4 }).map((_, i) => <div key={i} className="skeleton h-64" />)}
      </div>
    );
  }

  if (isError) {
    return (
      <QueryStateGate isLoading={false} isError error={error} onRetry={() => refetch()}>
        {null}
      </QueryStateGate>
    );
  }

  if (total === 0) {
    return (
      <Card className="p-12 text-center">
        <CircleDot className="h-10 w-10 mx-auto mb-3 text-[var(--color-stone-300)]" />
        <h3 className="font-display text-xl font-semibold mb-2">Kanban is leeg</h3>
        <p className="text-sm text-[var(--color-stone-500)] mb-5">
          Je hebt nog geen leads in een pipeline-stage.
        </p>
        <Link
          to="/leads"
          className="inline-flex h-10 items-center gap-2 px-4 rounded-md bg-[var(--color-blush-500)] text-white text-sm font-medium hover:bg-[var(--color-blush-600)]"
        >
          Naar leads
          <ArrowUpRight className="h-4 w-4" />
        </Link>
      </Card>
    );
  }

  return (
    <div className="overflow-x-auto pb-4">
      <div className="flex gap-3 min-w-max">
        {STAGES.map((s) => {
          const leads = stagesData[s.key] || [];
          return (
            <div key={s.key} className="w-64 shrink-0">
              <div className="flex items-center justify-between mb-2 px-1">
                <h4 className="text-sm font-semibold">{s.label}</h4>
                <Badge variant={s.accent || 'neutral'}>{leads.length}</Badge>
              </div>
              <div className="space-y-2">
                {leads.length === 0 ? (
                  <div className="rounded-lg border border-dashed border-[var(--color-border)] p-4 text-center text-xs text-[var(--color-stone-400)]">
                    Geen leads
                  </div>
                ) : (
                  leads.map((l) => <KanbanCard key={l.id} lead={l} />)
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function KanbanCard({ lead }: { lead: Lead }) {
  return (
    <Link
      to={`/leads/${lead.id}`}
      className="block rounded-lg border border-[var(--color-border)] bg-white p-3 hover:border-[var(--color-blush-400)] transition-colors"
    >
      <div className="text-sm font-medium truncate">{lead.company_name || '—'}</div>
      <div className="text-xs text-[var(--color-stone-500)] mt-0.5">{lead.city || '—'}</div>
      <div className="flex items-center gap-2 mt-2">
        <Badge variant="neutral">{SECTOR_LABEL[lead.sector || ''] || lead.sector || '—'}</Badge>
        {lead.score != null && (
          <span className="text-xs tabular-nums text-[var(--color-stone-500)]">S:{lead.score}</span>
        )}
      </div>
    </Link>
  );
}

function LijstView() {
  const { data, isLoading } = useQuery({
    queryKey: ['leads-in-pipeline'],
    queryFn: () => api.get<{ leads: Lead[] }>('/leads?limit=200'),
  });
  const leads = (data?.leads || []).filter((l) => l.crm_stage && l.status !== 'archived');

  if (isLoading) return <div className="skeleton h-64" />;

  if (leads.length === 0) {
    return (
      <Card className="p-10 text-center text-sm text-[var(--color-stone-500)]">
        <AlertCircle className="h-6 w-6 mx-auto mb-2 text-[var(--color-stone-300)]" />
        Geen leads met een CRM-stage. Zet leads in een stage via de Kanban.
      </Card>
    );
  }

  return (
    <Card className="overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-[var(--color-border)] bg-[var(--color-ivory-100)]">
            <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Bedrijf</th>
            <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Stage</th>
            <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Score</th>
            <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Gesprek</th>
            <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Laatste activiteit</th>
          </tr>
        </thead>
        <tbody>
          {leads.map((l) => (
            <tr key={l.id} className="border-b border-[var(--color-ivory-200)] hover:bg-[var(--color-ivory-50)]">
              <td className="px-4 py-3">
                <Link to={`/leads/${l.id}`} className="font-medium hover:text-[var(--color-blush-500)]">
                  {l.company_name || '—'}
                </Link>
                <div className="text-xs text-[var(--color-stone-500)]">{l.city || '—'} · {SECTOR_LABEL[l.sector || ''] || '—'}</div>
              </td>
              <td className="px-4 py-3">
                <Badge variant="neutral">{STAGES.find((s) => s.key === l.crm_stage)?.label || l.crm_stage}</Badge>
              </td>
              <td className="px-4 py-3 tabular-nums">{l.score ?? '—'}</td>
              <td className="px-4 py-3">
                {l.last_call_outcome ? (
                  <span className="text-xs text-[var(--color-stone-500)]">
                    <Badge variant={CALL_OUTCOME_META[l.last_call_outcome]?.variant || 'neutral'}>
                      {CALL_OUTCOME_META[l.last_call_outcome]?.label || l.last_call_outcome}
                    </Badge>
                    {l.last_call_at && <span className="ml-1.5">{fmtRelative(l.last_call_at)}</span>}
                  </span>
                ) : (
                  <span className="text-xs text-[var(--color-stone-300)]">—</span>
                )}
              </td>
              <td className="px-4 py-3 text-xs text-[var(--color-stone-500)]">
                <Clock className="inline h-3 w-3 mr-1" />
                {fmtRelative(l.updated_at || l.created_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}
