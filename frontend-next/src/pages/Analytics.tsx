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

interface CallsAgg {
  total_calls: number;
  funnel: {
    by_outcome: Record<string, number>;
    unmatched: number;
    reports: Record<string, number>;
    retargets: Record<string, number>;
  };
  learning: {
    engaged_total: number;
    reply_rate_per_outcome: Record<string, { engaged: number; replied: number; reply_rate_pct: number }>;
    reply_per_attempt: Record<string, number>;
    days_to_reply_per_outcome: Record<string, number>;
    finding_types_in_replies: Record<string, number>;
  };
}

const OUTCOME_NL: Record<string, string> = {
  won: 'Gewonnen', timing: 'Timing', no_value: 'Geen waarde', stalled: 'Vastgelopen',
  hard_no: 'Harde nee', onbekend: 'Onbekend',
};

interface FunnelData {
  conversions: { email_pct: number; verified_pct_of_email: number; sent_pct_of_verified: number; reply_rate_pct: number; interested_pct_of_replies: number };
  cohorts: { group: string; week: string; imported: number; has_email: number; has_verified_email: number; sent: number; replied: number; interested: number }[];
}
interface CostAttr {
  groups: { group: string; cost_eur: number; cost_per_lead_eur: number | null; cost_per_reply_eur: number | null; cost_per_interested_eur: number | null }[];
}
interface EmailBreakdown {
  total: number;
  buckets: { bucket: string; count: number; pct: number }[];
  diagnostic_hints: string[];
}
interface CoverageData {
  steps: { step: string; completed_count: number; missing_count: number; coverage_pct: number }[];
  diagnostic_hints: string[];
}
interface CostsData {
  total_eur: number; cache_hits: number;
  rows: { model: string; cost_eur: number }[];
}
interface MetricsData {
  metrics: { date: string; emails_sent?: number; open_rate?: number; reply_rate?: number }[];
}

function aggByModel(rows: { model: string; cost_eur: number }[]): [string, number][] {
  const out: Record<string, number> = {};
  for (const r of rows) out[r.model] = (out[r.model] || 0) + (r.cost_eur || 0);
  return Object.entries(out).sort((a, b) => b[1] - a[1]);
}
const covColor = (p: number) => (p < 50 ? 'var(--color-danger)' : p < 80 ? 'var(--color-warning)' : 'var(--color-success)');

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
  const { data: calls } = useQuery({
    queryKey: ['analytics-calls'],
    queryFn: () => api.get<CallsAgg>('/analytics/calls').catch(() => null),
  });
  const { data: coverage } = useQuery({ queryKey: ['analytics-coverage'], queryFn: () => api.get<CoverageData>('/analytics/enrichment-coverage').catch(() => null) });
  const { data: emailBd } = useQuery({ queryKey: ['analytics-email-bd'], queryFn: () => api.get<EmailBreakdown>('/analytics/email-status-breakdown').catch(() => null) });
  const { data: funnel } = useQuery({ queryKey: ['analytics-funnel'], queryFn: () => api.get<FunnelData>('/analytics/funnel?weeks=8&group_by=archetype').catch(() => null) });
  const { data: costAttr } = useQuery({ queryKey: ['analytics-cost-attr'], queryFn: () => api.get<CostAttr>('/analytics/cost-attribution?days=30&group_by=archetype').catch(() => null) });
  const { data: costs } = useQuery({ queryKey: ['analytics-costs'], queryFn: () => api.get<CostsData>('/analytics/costs?days=30').catch(() => null) });
  const { data: metrics } = useQuery({ queryKey: ['analytics-metrics'], queryFn: () => api.get<MetricsData>('/analytics/metrics?days=30').catch(() => null) });

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

      {/* Check-up follow-up */}
      <section className="mt-8">
        <h3 className="font-display text-lg font-semibold mb-3">Check-up follow-up</h3>
        <Card className="p-6">
          {!calls || calls.total_calls === 0 ? (
            <p className="text-sm text-[var(--color-stone-500)]">
              Nog geen gesprekken. De leerlus (reply-rate per uitkomst, per poging, en welke
              bevindingen replies opleveren) vult zich zodra check-ups verstuurd worden.
            </p>
          ) : (
            <div className="grid md:grid-cols-2 gap-6">
              <div>
                <div className="text-xs uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-2">Funnel</div>
                <div className="space-y-1.5 text-sm">
                  <TallyRow label="Gesprekken" value={calls.total_calls} />
                  <TallyRow label="Niet gekoppeld" value={calls.funnel.unmatched} />
                  <TallyRow label="Rapport verstuurd" value={calls.funnel.reports.sent || 0} />
                  <TallyRow label="Rapport overgeslagen" value={calls.funnel.reports.skipped || 0} />
                  <TallyRow label="Retarget gepland" value={calls.funnel.retargets.scheduled || 0} />
                  <TallyRow label="Retarget beantwoord" value={calls.funnel.retargets.replied || 0} />
                  <TallyRow label="Retarget afgerond" value={calls.funnel.retargets.exhausted || 0} />
                </div>
              </div>
              <div>
                <div className="text-xs uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-2">Leerlus · reply-rate per uitkomst</div>
                {Object.keys(calls.learning.reply_rate_per_outcome).length === 0 ? (
                  <p className="text-sm text-[var(--color-stone-400)]">Nog geen verstuurde follow-ups.</p>
                ) : (
                  <div className="space-y-1.5 text-sm">
                    {Object.entries(calls.learning.reply_rate_per_outcome).map(([o, s]) => (
                      <div key={o} className="flex items-center justify-between gap-3">
                        <span>{OUTCOME_NL[o] || o}</span>
                        <span className="tabular-nums text-[var(--color-stone-500)]">
                          {s.replied}/{s.engaged} · {s.reply_rate_pct}%
                        </span>
                      </div>
                    ))}
                  </div>
                )}
                {Object.keys(calls.learning.finding_types_in_replies).length > 0 && (
                  <div className="mt-4">
                    <div className="text-xs uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-2">Bevindingen die replies opleveren</div>
                    <div className="flex flex-wrap gap-1.5">
                      {Object.entries(calls.learning.finding_types_in_replies).map(([t, n]) => (
                        <span key={t} className="rounded-full bg-[var(--color-blush-100)] text-[var(--color-blush-700)] px-2.5 py-0.5 text-xs">
                          {t} · {n}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </Card>
      </section>

      {/* Enrichment-coverage per stap */}
      <section className="mt-8">
        <h3 className="font-display text-lg font-semibold mb-3">Enrichment-coverage per stap</h3>
        <Card className="p-6">
          {!coverage ? <div className="skeleton h-32" /> : coverage.steps.length === 0 ? (
            <p className="text-sm text-[var(--color-stone-500)]">Geen data.</p>
          ) : (
            <div className="space-y-2.5">
              {coverage.diagnostic_hints.map((h, i) => <div key={i} className="text-xs text-[var(--color-warning)]">⚠ {h}</div>)}
              {coverage.steps.map((s) => (
                <div key={s.step}>
                  <div className="flex justify-between text-xs mb-0.5">
                    <span>{s.step}</span>
                    <span className="tabular-nums text-[var(--color-stone-500)]">{s.coverage_pct}% · {fmtInt(s.missing_count)} mist</span>
                  </div>
                  <div className="h-1.5 rounded-full bg-[var(--color-ivory-200)] overflow-hidden">
                    <div className="h-full rounded-full" style={{ width: `${s.coverage_pct}%`, background: covColor(s.coverage_pct) }} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </Card>
      </section>

      {/* E-mail-status diagnose */}
      <section className="mt-8">
        <h3 className="font-display text-lg font-semibold mb-3">E-mail-status diagnose</h3>
        <Card className="p-6">
          {!emailBd ? <div className="skeleton h-32" /> : emailBd.total === 0 ? (
            <p className="text-sm text-[var(--color-stone-500)]">Geen e-mails.</p>
          ) : (
            <div>
              {emailBd.diagnostic_hints.map((h, i) => <div key={i} className="text-xs text-[var(--color-warning)] mb-1">⚠ {h}</div>)}
              <div className="space-y-1.5 mt-2">
                {emailBd.buckets.map((b) => (
                  <div key={b.bucket} className="flex items-center gap-3 text-sm">
                    <div className="w-44 truncate">{b.bucket}</div>
                    <div className="flex-1 h-2 rounded-full bg-[var(--color-ivory-200)] overflow-hidden">
                      <div className="h-full bg-[var(--color-blush-500)] rounded-full" style={{ width: `${b.pct}%` }} />
                    </div>
                    <div className="w-24 text-xs tabular-nums text-right">{fmtInt(b.count)} · {b.pct}%</div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </Card>
      </section>

      {/* Funnel-cohort */}
      <section className="mt-8">
        <h3 className="font-display text-lg font-semibold mb-3">Funnel-cohort (week × archetype)</h3>
        <Card className="p-6 overflow-x-auto">
          {!funnel ? <div className="skeleton h-32" /> : funnel.cohorts.length === 0 ? (
            <p className="text-sm text-[var(--color-stone-500)]">Geen cohortdata.</p>
          ) : (
            <>
              <div className="flex flex-wrap gap-6 mb-4">
                <Metric label="Email%" value={`${funnel.conversions.email_pct}%`} />
                <Metric label="Verified%" value={`${funnel.conversions.verified_pct_of_email}%`} />
                <Metric label="Sent%" value={`${funnel.conversions.sent_pct_of_verified}%`} />
                <Metric label="Reply-rate" value={`${funnel.conversions.reply_rate_pct}%`} />
                <Metric label="Interested%" value={`${funnel.conversions.interested_pct_of_replies}%`} />
              </div>
              <table className="w-full text-xs">
                <thead><tr className="text-left text-[10px] uppercase tracking-wider text-[var(--color-stone-500)]">
                  <th className="py-1 pr-3">Week</th><th className="pr-3">Groep</th><th className="pr-3">Imp</th><th className="pr-3">Email</th><th className="pr-3">Verif</th><th className="pr-3">Sent</th><th className="pr-3">Reply</th><th>Interested</th>
                </tr></thead>
                <tbody>
                  {funnel.cohorts.slice(0, 24).map((c, i) => (
                    <tr key={i} className="border-t border-[var(--color-ivory-200)] tabular-nums">
                      <td className="py-1 pr-3">{c.week}</td><td className="pr-3 text-[var(--color-stone-500)]">{c.group}</td>
                      <td className="pr-3">{c.imported}</td><td className="pr-3">{c.has_email}</td><td className="pr-3">{c.has_verified_email}</td><td className="pr-3">{c.sent}</td><td className="pr-3">{c.replied}</td><td>{c.interested}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </Card>
      </section>

      {/* Kosten-attributie */}
      <section className="mt-8">
        <h3 className="font-display text-lg font-semibold mb-3">Kosten-attributie per archetype (30d)</h3>
        <Card className="p-6 overflow-x-auto">
          {!costAttr ? <div className="skeleton h-32" /> : costAttr.groups.length === 0 ? (
            <p className="text-sm text-[var(--color-stone-500)]">Geen kostendata.</p>
          ) : (
            <table className="w-full text-sm">
              <thead><tr className="text-left text-[10px] uppercase tracking-wider text-[var(--color-stone-500)]">
                <th className="py-1 pr-3">Groep</th><th className="pr-3">Kosten</th><th className="pr-3">/lead</th><th className="pr-3">/reply</th><th>/interested</th>
              </tr></thead>
              <tbody>
                {costAttr.groups.map((g) => (
                  <tr key={g.group} className="border-t border-[var(--color-ivory-200)]">
                    <td className="py-1.5 pr-3">{g.group}</td>
                    <td className="pr-3 tabular-nums">{fmtEuro(g.cost_eur)}</td>
                    <td className="pr-3 tabular-nums text-[var(--color-stone-500)]">{g.cost_per_lead_eur != null ? fmtEuro(g.cost_per_lead_eur) : '—'}</td>
                    <td className="pr-3 tabular-nums text-[var(--color-stone-500)]">{g.cost_per_reply_eur != null ? fmtEuro(g.cost_per_reply_eur) : '—'}</td>
                    <td className="tabular-nums text-[var(--color-stone-500)]">{g.cost_per_interested_eur != null ? fmtEuro(g.cost_per_interested_eur) : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>
      </section>

      {/* Claude-kosten per model */}
      <section className="mt-8">
        <h3 className="font-display text-lg font-semibold mb-3">Claude-kosten per model (30d)</h3>
        <Card className="p-6">
          {!costs ? <div className="skeleton h-32" /> : (
            <div>
              <div className="flex gap-6 mb-4">
                <Metric label="Totaal" value={fmtEuro(costs.total_eur)} />
                <Metric label="Cache-hits" value={fmtInt(costs.cache_hits)} />
              </div>
              <div className="space-y-1.5">
                {aggByModel(costs.rows).map(([model, eur]) => (
                  <div key={model} className="flex items-center justify-between text-sm">
                    <span className="truncate">{model}</span>
                    <span className="tabular-nums">{fmtEuro(eur)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </Card>
      </section>

      {/* Dagelijkse metrics */}
      <section className="mt-8">
        <h3 className="font-display text-lg font-semibold mb-3">Dagelijkse metrics (30d)</h3>
        <Card className="p-6 overflow-x-auto">
          {!metrics ? <div className="skeleton h-32" /> : metrics.metrics.length === 0 ? (
            <p className="text-sm text-[var(--color-stone-500)]">Nog geen historie — de collect-metrics-cron vult daily_metrics.</p>
          ) : (
            <table className="w-full text-xs">
              <thead><tr className="text-left text-[10px] uppercase tracking-wider text-[var(--color-stone-500)]">
                <th className="py-1 pr-3">Datum</th><th className="pr-3">Verstuurd</th><th className="pr-3">Open-rate</th><th>Reply-rate</th>
              </tr></thead>
              <tbody>
                {metrics.metrics.slice(0, 30).map((m) => (
                  <tr key={m.date} className="border-t border-[var(--color-ivory-200)] tabular-nums">
                    <td className="py-1 pr-3">{m.date}</td>
                    <td className="pr-3">{m.emails_sent ?? 0}</td>
                    <td className="pr-3">{m.open_rate != null ? `${Math.round(m.open_rate * 100)}%` : '—'}</td>
                    <td>{m.reply_rate != null ? `${Math.round(m.reply_rate * 100)}%` : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </Card>
      </section>
    </div>
  );
}

function TallyRow({ label, value }: { label: string; value: number }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-[var(--color-stone-600)]">{label}</span>
      <span className="tabular-nums font-medium">{fmtInt(value)}</span>
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
