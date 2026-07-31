import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { TrendingUp, Zap, Rocket, MessageSquare, Target, Users, AlertTriangle, Pause, Play, Lock, Plus } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtInt, fmtEuro } from '@/lib/format';
import type { PipelineStats, EnrichmentCost } from '@/lib/types';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Link } from 'react-router-dom';

function greeting() {
  const h = new Date().getHours();
  if (h < 12) return 'Goedemorgen';
  if (h < 18) return 'Goedemiddag';
  return 'Goedenavond';
}

interface StatTile {
  label: string;
  value: string;
  sub?: string;
  icon: typeof Users;
  accent?: 'primary' | 'success' | 'warning';
}

export function DashboardPage() {
  const { data: stats, isLoading } = useQuery({
    queryKey: ['analytics-pipeline'],
    queryFn: () => api.get<PipelineStats>('/analytics/pipeline'),
    refetchInterval: 30_000,
  });

  const { data: cost } = useQuery({
    queryKey: ['enrichment-cost'],
    queryFn: () => api.get<EnrichmentCost>('/analytics/enrichment-cost'),
    refetchInterval: 10_000,  // near-live kostenbewaking
  });

  const tiles: StatTile[] = [
    { label: 'Totale leads', value: fmtInt(stats?.total_leads), sub: 'GDPR-safe', icon: Users },
    { label: 'Verrijkte leads', value: fmtInt(stats?.enriched_leads), sub: 'score ≥ 1', icon: Zap, accent: 'primary' },
    { label: 'Email coverage', value: stats ? `${stats.email_coverage_pct}%` : '—', sub: `${fmtInt(stats?.verified_emails)} verified`, icon: MessageSquare },
    { label: 'In pipeline', value: fmtInt(stats?.sent_to_warmr), sub: `voorbij 'ontdekt' · ${fmtInt(stats?.total_replies)} replies`, icon: Rocket, accent: 'success' },
    { label: 'Website kansen', value: fmtInt(stats?.website_opportunities), sub: 'score < 50', icon: Target, accent: 'warning' },
    { label: 'Gewonnen deze maand', value: fmtEuro(stats?.won_this_month), sub: 'closed deals', icon: TrendingUp, accent: 'success' },
  ];

  return (
    <div className="max-w-7xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Vandaag"
        title={greeting()}
        subtitle="Realtime overzicht van je outbound pipeline."
        actions={
          <>
            <Link
              to="/zoeken"
              className="inline-flex items-center gap-2 h-10 px-4 rounded-md text-sm font-medium bg-white border border-[var(--color-border)] text-[var(--color-foreground)] hover:border-[var(--color-blush-400)] hover:bg-[var(--color-ivory-100)] transition-colors"
            >
              + Nieuwe zoekopdracht
            </Link>
            <Link
              to="/leads"
              className="inline-flex items-center gap-2 h-10 px-4 rounded-md text-sm font-medium bg-[var(--color-blush-500)] text-white hover:bg-[var(--color-blush-600)] transition-colors"
            >
              Bekijk leads
            </Link>
          </>
        }
      />

      {/* Stats grid */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-8">
        {tiles.map((t) => (
          <Card key={t.label} className="p-5">
            <div className="flex items-start justify-between mb-4">
              <div className="text-[10px] uppercase tracking-[0.08em] font-semibold text-[var(--color-stone-500)]">
                {t.label}
              </div>
              <t.icon
                className={
                  t.accent === 'primary'
                    ? 'h-4 w-4 text-[var(--color-blush-500)]'
                    : t.accent === 'success'
                    ? 'h-4 w-4 text-[var(--color-success)]'
                    : t.accent === 'warning'
                    ? 'h-4 w-4 text-[var(--color-warning)]'
                    : 'h-4 w-4 text-[var(--color-stone-400)]'
                }
              />
            </div>
            <div className="font-display text-4xl font-semibold leading-none tracking-tight">
              {isLoading ? <span className="skeleton inline-block h-9 w-20" /> : t.value}
            </div>
            {t.sub && (
              <div className="mt-2 text-xs text-[var(--color-stone-500)]">{t.sub}</div>
            )}
          </Card>
        ))}
      </div>

      {/* Cost monitor — 2 progress bars: today + month */}
      {cost && <CostMonitor cost={cost} />}
    </div>
  );
}

function CostMonitor({ cost }: { cost: EnrichmentCost }) {
  const qc = useQueryClient();
  const [raiseAmount, setRaiseAmount] = useState<string>('20');
  const approve = useMutation({
    mutationFn: () => api.post('/analytics/enrichment-approve-continue'),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['enrichment-cost'] }),
  });
  const pause = useMutation({
    mutationFn: () => api.post('/analytics/enrichment-pause'),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['enrichment-cost'] }),
  });
  const raiseCap = useMutation({
    mutationFn: (extra: number) =>
      api.post('/analytics/enrichment-raise-monthly-cap', { extra_eur: extra }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['enrichment-cost'] }),
  });

  const overrideActive = (cost.monthly_override_eur ?? 0) > 0;

  const monthColor =
    cost.tier_100_hit ? 'var(--color-danger)' :
    cost.tier_50_hit && !cost.approved_over_50 ? 'var(--color-warning)' :
    cost.month_pct_used >= 80 ? 'var(--color-warning)' :
    'var(--color-blush-500)';

  const dayColor =
    cost.budget_pct_used >= 100 ? 'var(--color-danger)' :
    cost.budget_pct_used >= 80 ? 'var(--color-warning)' :
    'var(--color-blush-500)';

  return (
    <div className="mb-8 space-y-3">
      {/* Blocking alert: 100% hit or approval required */}
      {cost.tier_100_hit && (
        <Card className="p-5 border-2 border-[var(--color-danger)] bg-red-50">
          <div className="flex items-start gap-3">
            <Lock className="h-5 w-5 text-[var(--color-danger)] shrink-0 mt-0.5" />
            <div className="flex-1">
              <div className="font-display text-lg font-semibold text-[var(--color-danger)] mb-1">
                Maand-budget bereikt — enrichment volledig gepauzeerd
              </div>
              <p className="text-sm text-[var(--color-stone-700)] mb-3">
                Je hebt <strong>{fmtEuro(cost.month_eur)}</strong> uitgegeven van{' '}
                <strong>{fmtEuro(cost.monthly_budget_eur)}</strong>
                {overrideActive && (
                  <> (basis {fmtEuro(cost.monthly_base_eur ?? 0)} + verhoging {fmtEuro(cost.monthly_override_eur ?? 0)})</>
                )}
                . Alle Claude-calls zijn geblokkeerd tot begin volgende maand. Verhoog het budget hieronder
                om nu door te gaan — geldt tot einde van deze maand.
              </p>
              <div className="flex items-end gap-2 flex-wrap">
                <label className="flex flex-col gap-1">
                  <span className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                    Extra budget (€)
                  </span>
                  <input
                    type="number"
                    min="1"
                    max="500"
                    step="5"
                    value={raiseAmount}
                    onChange={(e) => setRaiseAmount(e.target.value)}
                    className="h-10 w-32 px-3 rounded-md border border-[var(--color-border)] bg-white text-sm focus:outline-none focus:border-[var(--color-danger)]"
                    placeholder="20"
                  />
                </label>
                <button
                  onClick={() => {
                    const v = parseFloat(raiseAmount);
                    if (!Number.isFinite(v) || v <= 0) return;
                    raiseCap.mutate(v);
                  }}
                  disabled={raiseCap.isPending || !(parseFloat(raiseAmount) > 0)}
                  className="inline-flex items-center gap-2 h-10 px-4 rounded-md bg-[var(--color-danger)] text-white text-sm font-medium hover:bg-red-700 disabled:opacity-60"
                >
                  <Plus className="h-3.5 w-3.5" />
                  {raiseCap.isPending ? 'Bezig…' : `Verhoog met €${raiseAmount || '0'}`}
                </button>
              </div>
              {raiseCap.isSuccess && (
                <div className="mt-3 text-xs text-[var(--color-success)]">
                  ✓ Budget verhoogd tot einde maand. Enrichment kan weer draaien.
                </div>
              )}
              {raiseCap.isError && (
                <div className="mt-3 text-xs text-[var(--color-danger)]">
                  Verhogen mislukt — check API log.
                </div>
              )}
            </div>
          </div>
        </Card>
      )}

      {cost.approval_required && !cost.tier_100_hit && (
        <Card className="p-5 border-2 border-[var(--color-warning)] bg-amber-50">
          <div className="flex items-start gap-3">
            <AlertTriangle className="h-5 w-5 text-[var(--color-warning)] shrink-0 mt-0.5" />
            <div className="flex-1">
              <div className="font-display text-lg font-semibold text-[var(--color-warning)] mb-1">
                50% van maand-budget bereikt — bevestiging nodig
              </div>
              <p className="text-sm text-[var(--color-stone-700)] mb-4">
                Je hebt <strong>{fmtEuro(cost.month_eur)}</strong> van <strong>{fmtEuro(cost.monthly_budget_eur)}</strong> ({cost.month_pct_used}%) uitgegeven deze maand.
                Enrichment is gepauzeerd. Wil je doorgaan tot 100% of nu stoppen?
              </p>
              <div className="flex gap-2">
                <button
                  onClick={() => approve.mutate()}
                  disabled={approve.isPending}
                  className="inline-flex items-center gap-2 h-10 px-4 rounded-md bg-[var(--color-warning)] text-white text-sm font-medium hover:bg-amber-700 disabled:opacity-60"
                >
                  <Play className="h-3.5 w-3.5" />
                  {approve.isPending ? 'Bezig…' : `Doorgaan tot ${fmtEuro(cost.monthly_budget_eur)}`}
                </button>
                <Link
                  to="/analytics"
                  className="inline-flex items-center gap-2 h-10 px-4 rounded-md bg-white border border-[var(--color-border)] text-sm font-medium text-[var(--color-stone-700)] hover:bg-[var(--color-ivory-100)]"
                >
                  Bekijk details
                </Link>
              </div>
            </div>
          </div>
        </Card>
      )}

      {overrideActive && !cost.tier_100_hit && (
        <Card className="p-3 bg-amber-50 border-[var(--color-warning)]">
          <div className="flex items-center gap-2 text-xs text-[var(--color-stone-700)]">
            <Plus className="h-3.5 w-3.5 text-[var(--color-warning)]" />
            <span>
              Maand-budget tijdelijk verhoogd met <strong>{fmtEuro(cost.monthly_override_eur ?? 0)}</strong>{' '}
              (totaal nu <strong>{fmtEuro(cost.monthly_budget_eur)}</strong>, geldig tot einde maand).
            </span>
          </div>
        </Card>
      )}

      {cost.approved_over_50 && !cost.tier_100_hit && (
        <Card className="p-3 bg-[var(--color-ivory-100)] border-[var(--color-blush-400)]">
          <div className="flex items-center gap-2 text-xs text-[var(--color-stone-700)]">
            <Play className="h-3.5 w-3.5 text-[var(--color-success)]" />
            <span>Enrichment goedgekeurd om door te gaan tot {fmtEuro(cost.monthly_budget_eur)}.</span>
            <button
              onClick={() => pause.mutate()}
              disabled={pause.isPending}
              className="ml-auto inline-flex items-center gap-1 text-[var(--color-stone-600)] hover:text-[var(--color-danger)]"
              title="Pauzeer enrichment nu"
            >
              <Pause className="h-3 w-3" /> Pauzeer
            </button>
          </div>
        </Card>
      )}

      {/* Dual progress card */}
      <Card className="p-5">
        <div className="grid md:grid-cols-2 gap-6">
          {/* TODAY */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                Vandaag
              </div>
              {cost.budget_warning && (
                <span className="text-[10px] font-semibold text-[var(--color-warning)]">⚠ {cost.budget_pct_used}%</span>
              )}
            </div>
            <div className="flex items-baseline gap-2 mb-2">
              <span className="font-display text-3xl font-semibold">{fmtEuro(cost.today_eur)}</span>
              <span className="text-sm text-[var(--color-stone-500)]">/ {fmtEuro(cost.daily_budget_eur)}</span>
            </div>
            <div className="h-2 rounded-full bg-[var(--color-ivory-200)] overflow-hidden">
              <div
                className="h-full rounded-full transition-all"
                style={{ width: `${Math.min(cost.budget_pct_used, 100)}%`, background: dayColor }}
              />
            </div>
            <div className="mt-2 text-xs text-[var(--color-stone-500)]">
              Nog <strong>{fmtEuro(cost.budget_remaining_eur)}</strong> tot de dagelijkse cap
            </div>
          </div>

          {/* THIS MONTH */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                Deze maand
              </div>
              {cost.tier_100_hit ? (
                <span className="text-[10px] font-semibold text-[var(--color-danger)]">🔒 100%</span>
              ) : cost.tier_50_hit ? (
                <span className="text-[10px] font-semibold text-[var(--color-warning)]">⚠ {cost.month_pct_used}%</span>
              ) : null}
            </div>
            <div className="flex items-baseline gap-2 mb-2">
              <span className="font-display text-3xl font-semibold">{fmtEuro(cost.month_eur)}</span>
              <span className="text-sm text-[var(--color-stone-500)]">/ {fmtEuro(cost.monthly_budget_eur)}</span>
            </div>
            <div className="relative h-2 rounded-full bg-[var(--color-ivory-200)] overflow-hidden">
              {/* 50% marker line */}
              <div className="absolute top-0 bottom-0 w-px bg-[var(--color-stone-400)] z-10" style={{ left: '50%' }} />
              <div
                className="h-full rounded-full transition-all"
                style={{ width: `${Math.min(cost.month_pct_used, 100)}%`, background: monthColor }}
              />
            </div>
            <div className="mt-2 text-xs text-[var(--color-stone-500)]">
              Nog <strong>{fmtEuro(cost.month_remaining_eur)}</strong> · 50% check bij {fmtEuro(cost.monthly_budget_eur / 2)}
              {overrideActive && (
                <> · <span className="text-[var(--color-warning)]">+{fmtEuro(cost.monthly_override_eur ?? 0)} verhoging actief</span></>
              )}
            </div>
          </div>
        </div>

        <div className="mt-4 pt-4 border-t border-[var(--color-ivory-200)] flex items-center justify-between text-xs text-[var(--color-stone-500)]">
          <span>
            {cost.window_days}-daags totaal: <strong className="text-[var(--color-stone-800)]">{fmtEuro(cost.window_eur)}</strong>
          </span>
          <span>
            {cost.block_events_in_window > 0
              ? `${cost.block_events_in_window} geblokkeerde calls`
              : 'Geen budget-blocks'}
          </span>
          <Link to="/analytics" className="text-[var(--color-blush-500)] hover:underline">
            Kosten details →
          </Link>
        </div>
      </Card>
    </div>
  );
}
