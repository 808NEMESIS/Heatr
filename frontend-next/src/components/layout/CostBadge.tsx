import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import { Wallet, AlertTriangle, Lock } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtEuro } from '@/lib/format';
import type { EnrichmentCost } from '@/lib/types';

/**
 * Sticky cost-badge in de top header. Polling 30s. Toont:
 *   - €X / €Y deze maand (kleur op basis van tier)
 *   - Lock-icon bij 100% hit
 *   - Warning-icon bij 50%+ zonder approval
 */
export function CostBadge() {
  const { data } = useQuery({
    queryKey: ['enrichment-cost-badge'],
    queryFn: () => api.get<EnrichmentCost>('/analytics/enrichment-cost').catch(() => null),
    refetchInterval: 30_000,
    staleTime: 20_000,
  });

  if (!data) {
    return (
      <div className="text-xs text-[var(--color-stone-400)] flex items-center gap-1.5 px-3">
        <Wallet className="h-3.5 w-3.5" />
        <span>—</span>
      </div>
    );
  }

  const tier100 = data.tier_100_hit;
  const tier50AwaitingApproval = data.tier_50_hit && !data.approved_over_50 && !tier100;
  const dayOver = data.budget_pct_used >= 100;

  let bg = 'bg-[var(--color-ivory-100)]';
  let text = 'text-[var(--color-stone-700)]';
  let border = 'border-[var(--color-border)]';
  if (tier100) {
    bg = 'bg-red-50';
    text = 'text-[var(--color-danger)]';
    border = 'border-[var(--color-danger)]';
  } else if (tier50AwaitingApproval || dayOver) {
    bg = 'bg-amber-50';
    text = 'text-[var(--color-warning)]';
    border = 'border-[var(--color-warning)]';
  } else if (data.budget_pct_used >= 80 || data.month_pct_used >= 80) {
    bg = 'bg-amber-50';
    text = 'text-[var(--color-warning)]';
    border = 'border-amber-300';
  }

  return (
    <Link
      to="/analytics"
      title={`Vandaag: ${fmtEuro(data.today_eur)} / ${fmtEuro(data.daily_budget_eur)} (${data.budget_pct_used}%)\nMaand: ${fmtEuro(data.month_eur)} / ${fmtEuro(data.monthly_budget_eur)} (${data.month_pct_used}%)\nKlik voor details`}
      className={`inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-md border ${border} ${bg} ${text} hover:opacity-80 transition-opacity`}
    >
      {tier100 ? <Lock className="h-3 w-3" /> :
       (tier50AwaitingApproval || dayOver) ? <AlertTriangle className="h-3 w-3" /> :
       <Wallet className="h-3 w-3" />}
      <span className="tabular-nums">
        {fmtEuro(data.month_eur)} <span className="opacity-60">/ {fmtEuro(data.monthly_budget_eur)}</span>
      </span>
      <span className="text-[10px] opacity-60">·</span>
      <span className="text-[10px] tabular-nums opacity-70">
        {data.month_pct_used}%
      </span>
    </Link>
  );
}
