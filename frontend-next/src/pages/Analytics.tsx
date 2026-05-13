import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { fmtInt, fmtEuro } from '@/lib/format';
import type { PipelineStats, EnrichmentCost } from '@/lib/types';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { cn } from '@/lib/cn';

interface WebsiteAgg {
  total_analysed: number;
  avg_website_score: number | null;
  score_distribution: Record<string, number>;
  website_rebuild_count: number;
  conversion_count: number;
}

export function AnalyticsPage() {
  const { data: pipe } = useQuery({
    queryKey: ['analytics-pipeline'],
    queryFn: () => api.get<PipelineStats>('/analytics/pipeline'),
  });
  const { data: web } = useQuery({
    queryKey: ['analytics-website'],
    queryFn: () => api.get<WebsiteAgg>('/analytics/website').catch(() => null),
  });
  const { data: cost } = useQuery({
    queryKey: ['enrichment-cost-7'],
    queryFn: () => api.get<EnrichmentCost>('/analytics/enrichment-cost?days=30').catch(() => null),
  });

  return (
    <div className="max-w-7xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Insights"
        title="Analytics"
        subtitle="Funnel, email coverage, en website-intelligence aggregaten."
      />

      {/* Funnel */}
      <section className="mb-8">
        <h3 className="font-display text-lg font-semibold mb-3">Pipeline funnel</h3>
        <Card className="p-6">
          {!pipe ? (
            <div className="skeleton h-32" />
          ) : (
            <div className="space-y-3">
              <FunnelRow label="Totaal leads" value={pipe.total_leads} max={pipe.total_leads} color="var(--color-stone-300)" />
              <FunnelRow label="Verrijkt" value={pipe.enriched_leads} max={pipe.total_leads} color="var(--color-blush-400)" />
              <FunnelRow label="Email verified" value={pipe.verified_emails} max={pipe.total_leads} color="var(--color-blush-500)" />
              <FunnelRow label="Naar Warmr" value={pipe.sent_to_warmr} max={pipe.total_leads} color="var(--color-blush-600)" />
              <FunnelRow label="Replies" value={pipe.total_replies} max={pipe.total_leads} color="var(--color-success)" />
            </div>
          )}
        </Card>
      </section>

      {/* Email coverage */}
      <section className="mb-8">
        <h3 className="font-display text-lg font-semibold mb-3">Email coverage</h3>
        {pipe && pipe.total_leads > 0 && (
          <Card className="p-6">
            <div className="flex items-baseline gap-3 mb-4">
              <span className="font-display text-4xl font-semibold">{pipe.email_coverage_pct}%</span>
              <span className="text-sm text-[var(--color-stone-500)]">verified + catchall</span>
            </div>
            <div className="flex h-3 rounded-full overflow-hidden">
              {Object.entries(pipe.email_breakdown).map(([status, count]) => {
                const colorMap: Record<string, string> = {
                  verified: 'var(--color-success)',
                  valid: 'var(--color-success)',
                  catch_all: 'var(--color-warning)',
                  catchall_risky: 'var(--color-warning)',
                  risky: 'var(--color-info)',
                  not_found: 'var(--color-stone-300)',
                  pending: 'var(--color-ivory-200)',
                };
                return (
                  <div
                    key={status}
                    style={{ width: `${(count / pipe.total_leads) * 100}%`, background: colorMap[status] || 'var(--color-stone-200)' }}
                    title={`${status}: ${count}`}
                  />
                );
              })}
            </div>
            <div className="mt-3 flex flex-wrap gap-4 text-xs text-[var(--color-stone-500)]">
              {Object.entries(pipe.email_breakdown).map(([k, v]) => (
                <span key={k}>
                  <strong className="text-[var(--color-stone-800)]">{fmtInt(v)}</strong> {k}
                </span>
              ))}
            </div>
          </Card>
        )}
      </section>

      {/* Website score distribution */}
      <section className="mb-8">
        <h3 className="font-display text-lg font-semibold mb-3">Website-score verdeling</h3>
        <Card className="p-6">
          {!web ? (
            <div className="skeleton h-32" />
          ) : web.total_analysed === 0 ? (
            <div className="text-sm text-[var(--color-stone-500)]">Nog geen websites geanalyseerd.</div>
          ) : (
            <div>
              <div className="flex items-baseline gap-3 mb-5">
                <span className="font-display text-3xl font-semibold">{web.avg_website_score?.toFixed(1) ?? '—'}</span>
                <span className="text-sm text-[var(--color-stone-500)]">gemiddelde over {fmtInt(web.total_analysed)} sites</span>
              </div>
              <div className="grid grid-cols-5 gap-2">
                {Object.entries(web.score_distribution).map(([bucket, count]) => {
                  const total = Object.values(web.score_distribution).reduce((a, b) => a + b, 0) || 1;
                  const pct = (count / total) * 100;
                  const color =
                    bucket === '0–20' ? 'var(--color-danger)' :
                    bucket === '20–40' ? '#ea580c' :
                    bucket === '40–60' ? 'var(--color-warning)' :
                    bucket === '60–80' ? '#65a30d' :
                    'var(--color-success)';
                  return (
                    <div key={bucket} className="flex flex-col items-center">
                      <div className="w-full rounded-t relative" style={{ height: '120px', background: 'var(--color-ivory-200)' }}>
                        <div
                          className="absolute bottom-0 left-0 right-0 rounded-t transition-all"
                          style={{ height: `${pct}%`, background: color }}
                        />
                      </div>
                      <div className="text-xs font-medium mt-2">{count}</div>
                      <div className="text-[10px] text-[var(--color-stone-500)]">{bucket}</div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </Card>
      </section>

      {/* Enrichment cost */}
      <section>
        <h3 className="font-display text-lg font-semibold mb-3">Enrichment spend (30 dagen)</h3>
        <Card className="p-6">
          {!cost ? (
            <div className="skeleton h-32" />
          ) : (
            <div>
              <div className="grid grid-cols-4 gap-4 mb-5">
                <Metric label="Vandaag" value={fmtEuro(cost.today_eur)} />
                <Metric label="Deze maand" value={fmtEuro(cost.month_eur)} />
                <Metric label="Budget/maand" value={fmtEuro(cost.monthly_budget_eur)} />
                <Metric label="Budget/dag" value={fmtEuro(cost.daily_budget_eur)} />
              </div>
              <div className="space-y-2">
                <div className="text-xs uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Per context</div>
                {Object.entries(cost.by_context).slice(0, 8).map(([ctx, eur]) => {
                  const max = Math.max(...Object.values(cost.by_context));
                  return (
                    <div key={ctx} className="flex items-center gap-3">
                      <div className="w-44 text-sm truncate">{ctx}</div>
                      <div className="flex-1 h-2 rounded-full bg-[var(--color-ivory-200)] overflow-hidden">
                        <div className="h-full bg-[var(--color-blush-500)] rounded-full" style={{ width: `${(eur / max) * 100}%` }} />
                      </div>
                      <div className="w-16 text-xs tabular-nums text-right">{fmtEuro(eur)}</div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </Card>
      </section>
    </div>
  );
}

function FunnelRow({ label, value, max, color }: { label: string; value: number; max: number; color: string }) {
  const pct = max > 0 ? (value / max) * 100 : 0;
  return (
    <div className="flex items-center gap-4">
      <div className="w-32 text-sm">{label}</div>
      <div className="flex-1 h-8 bg-[var(--color-ivory-100)] rounded-md overflow-hidden relative">
        <div
          className="h-full transition-all flex items-center justify-end pr-3 text-xs font-medium text-white"
          style={{ width: `${Math.max(pct, 3)}%`, background: color }}
        >
          {pct > 10 && fmtInt(value)}
        </div>
      </div>
      <div className={cn('w-20 text-sm tabular-nums text-right', pct < 10 && 'text-[var(--color-stone-500)]')}>
        {pct < 10 && fmtInt(value)} <span className="text-xs text-[var(--color-stone-400)]">{pct.toFixed(1)}%</span>
      </div>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">{label}</div>
      <div className="font-display text-2xl font-semibold">{value}</div>
    </div>
  );
}
