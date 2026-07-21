import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ExternalLink, AlertTriangle } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtInt, priorityFromScore, scoreColor } from '@/lib/format';
import type { Lead, LeadsResponse } from '@/lib/types';
import { SECTOR_LABEL } from '@/lib/types';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/cn';

function topIssueFromScore(s: number): string {
  if (s < 20) return 'Website technisch en visueel sterk verouderd';
  if (s < 35) return 'Voldoet niet meer aan moderne conversie-standaarden';
  if (s < 50) return 'Ontbrekende conversie-elementen (booking / chat / WhatsApp)';
  return 'Kleinere verbeterpunten aan de flow';
}

export function WebsiteKansenPage() {
  const [sector, setSector] = useState('');
  const [priority, setPriority] = useState('');

  const { data, isLoading } = useQuery({
    queryKey: ['leads-for-kansen', 500],
    queryFn: () => api.get<LeadsResponse>('/leads?limit=500'),
  });

  const filtered = useMemo(() => {
    return (data?.leads || [])
      .filter((l) => l.status !== 'archived')
      .filter((l) => l.website_score != null && l.website_score > 0)
      .filter((l) => !sector || l.sector === sector)
      .filter((l) => {
        // Drempels: config/scoring_thresholds.py (herijking 2026-07-21).
        const s = l.website_score || 0;
        if (priority === 'urgent') return s < 41;
        if (priority === 'hoog') return s >= 41 && s < 49;
        if (priority === 'medium') return s >= 49 && s < 56;
        return true;
      })
      .sort((a, b) => (a.website_score || 0) - (b.website_score || 0));
  }, [data, sector, priority]);

  const counts = useMemo(() => {
    const urgent = filtered.filter((l) => (l.website_score || 0) < 41).length;
    const hoog = filtered.filter((l) => {
      const s = l.website_score || 0;
      return s >= 41 && s < 49;
    }).length;
    const medium = filtered.filter((l) => {
      const s = l.website_score || 0;
      return s >= 49 && s < 56;
    }).length;
    return { urgent, hoog, medium };
  }, [filtered]);

  return (
    <div className="max-w-7xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Website intelligence"
        title="Website kansen"
        subtitle="Leads met de grootste website-pijnpunten — eerste prioriteit voor outreach."
      />

      <div className="flex flex-wrap gap-3 items-center mb-6">
        <div className="flex gap-1.5">
          {[
            { v: '', l: 'Alle sectoren' },
            { v: 'cosmetische_behandelaars', l: 'Cosmetisch' },
            { v: 'alternatieve_geneeskunde', l: 'Alt. geneeskunde' },
          ].map((f) => (
            <button
              key={f.v}
              onClick={() => setSector(f.v)}
              className={cn(
                'px-3 py-1.5 text-xs font-medium rounded-md border transition-colors',
                sector === f.v
                  ? 'bg-[var(--color-blush-100)] border-[var(--color-blush-500)] text-[var(--color-blush-700)]'
                  : 'bg-white border-[var(--color-border)] text-[var(--color-stone-600)]'
              )}
            >
              {f.l}
            </button>
          ))}
        </div>
        <select
          value={priority}
          onChange={(e) => setPriority(e.target.value)}
          className="ml-auto h-10 px-3 rounded-md border border-[var(--color-border)] bg-white text-sm"
        >
          <option value="">Alle prioriteiten</option>
          <option value="urgent">Urgent (&lt;30)</option>
          <option value="hoog">Hoog (30-44)</option>
          <option value="medium">Medium (45-69)</option>
        </select>
      </div>

      <div className="flex gap-5 text-sm text-[var(--color-stone-500)] mb-4 px-1">
        <span><strong className="text-[var(--color-stone-800)]">{fmtInt(filtered.length)}</strong> kansen</span>
        <span>·</span>
        <span>🔴 <strong className="text-[var(--color-stone-800)]">{counts.urgent}</strong> urgent</span>
        <span>🟠 <strong className="text-[var(--color-stone-800)]">{counts.hoog}</strong> hoog</span>
        <span>🟡 <strong className="text-[var(--color-stone-800)]">{counts.medium}</strong> medium</span>
      </div>

      <div className="flex flex-col gap-3">
        {isLoading && Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="skeleton h-36 rounded-xl" />
        ))}
        {!isLoading && filtered.length === 0 && (
          <Card className="p-12 text-center text-[var(--color-stone-500)]">
            <AlertTriangle className="h-6 w-6 mx-auto mb-3 text-[var(--color-stone-300)]" />
            Geen kansen met deze filters. Draai enrichment om website-scores te genereren.
          </Card>
        )}
        {filtered.map((l) => <KansenCard key={l.id} lead={l} />)}
      </div>
    </div>
  );
}

function KansenCard({ lead }: { lead: Lead }) {
  const score = lead.website_score || 0;
  const prio = priorityFromScore(score);
  const color = scoreColor(score);
  const topIssue = topIssueFromScore(score);
  const vsMkt = (lead as unknown as { score_vs_market?: number }).score_vs_market;

  return (
    <Card
      className={cn('grid grid-cols-[96px_1fr_220px] gap-5 p-5 transition-colors hover:border-[var(--color-stone-200)]')}
      style={{ borderLeft: `4px solid ${prio.border}` }}
    >
      {/* Score */}
      <div className="flex flex-col items-center justify-center">
        <div className="font-display text-4xl font-semibold leading-none tracking-tight" style={{ color }}>
          {score}
        </div>
        <div className="text-[10px] mt-1 text-[var(--color-stone-500)]">/ 100</div>
        <div className="w-full h-1.5 rounded-full bg-[var(--color-ivory-200)] mt-3 overflow-hidden">
          <div className="h-full rounded-full" style={{ width: `${score}%`, background: color }} />
        </div>
      </div>

      {/* Content */}
      <div className="min-w-0">
        <div className="flex items-baseline gap-2 mb-1 flex-wrap">
          <span className="font-display text-xl font-semibold text-[var(--color-stone-800)]">
            {lead.company_name || '?'}
          </span>
          <Badge variant="accent">{SECTOR_LABEL[lead.sector || ''] || lead.sector || '—'}</Badge>
        </div>
        <div className="text-xs text-[var(--color-stone-500)] mb-3">
          {lead.domain && (
            <a
              href={`https://${lead.domain}`}
              target="_blank"
              rel="noopener"
              className="text-[var(--color-blush-500)] hover:underline inline-flex items-center gap-0.5"
            >
              {lead.domain} <ExternalLink className="h-2.5 w-2.5" />
            </a>
          )}
          {lead.city && <> · {lead.city}</>}
          {lead.google_rating && <> · {lead.google_rating}★ ({lead.google_review_count || 0})</>}
        </div>
        <div className="rounded-md bg-[var(--color-blush-50)] border-l-[3px] border-[var(--color-blush-500)] px-3 py-2 mb-2">
          <div className="text-[10px] font-semibold uppercase tracking-wider text-[var(--color-blush-500)] mb-0.5">
            Grootste probleem
          </div>
          <div className="text-sm text-[var(--color-stone-800)]">{topIssue}</div>
        </div>
        {lead.personalized_opener && (
          <div className="text-sm text-[var(--color-stone-600)] italic leading-relaxed mt-2">
            "{lead.personalized_opener}"
          </div>
        )}
      </div>

      {/* Right */}
      <div className="flex flex-col gap-2 items-end">
        <span
          className="inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wider"
          style={{ background: prio.bg, color: prio.color }}
        >
          {prio.label}
        </span>
        <div className="w-full rounded-md bg-[var(--color-ivory-50)] px-3 py-2 text-right">
          <div className="text-[10px] uppercase tracking-wider text-[var(--color-stone-500)]">
            vs markt {lead.city || ''}
          </div>
          {vsMkt == null ? (
            <div className="text-xs italic text-[var(--color-stone-400)] mt-0.5">niet berekend</div>
          ) : (
            <div
              className="font-semibold tabular-nums"
              style={{ color: vsMkt < 0 ? 'var(--color-danger)' : vsMkt > 0 ? 'var(--color-success)' : 'inherit' }}
            >
              {vsMkt < 0 ? '↓' : vsMkt > 0 ? '↑ +' : ''} {vsMkt} pts
            </div>
          )}
        </div>
      </div>
    </Card>
  );
}
