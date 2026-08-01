import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { ExternalLink, AlertTriangle, ImageOff } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtInt, scoreColor } from '@/lib/format';
import { SECTOR_LABEL } from '@/lib/types';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/cn';

/** Eén website-opportunity uit GET /website-opportunities (WI-rij + lead-join). */
interface Opportunity {
  lead_id: string;
  company_name: string | null;
  city: string | null;
  sector: string | null;
  domain: string | null;
  total_score: number | null;
  priority: string | null;                       // urgent | high | medium | low
  opportunity_types: string[] | null;
  opportunity_reasons: Record<string, string> | null;
  screenshot_desktop_url: string | null;
  review_status?: string | null;
}

const REVIEW_BADGE: Record<string, { label: string; variant: 'success' | 'accent' | 'danger' }> = {
  ok: { label: 'OK', variant: 'success' },
  opportunity: { label: 'Kans', variant: 'accent' },
  urgent: { label: 'Urgent', variant: 'danger' },
};

const OPP_LABEL: Record<string, string> = {
  website_rebuild: 'Website',
  conversie_optimalisatie: 'Conversie',
  chatbot: 'Chatbot',
  ai_audit: 'AI Audit',
};

const PRIO: Record<string, { label: string; variant: 'danger' | 'warning' | 'accent' | 'neutral' }> = {
  urgent: { label: 'Urgent', variant: 'danger' },
  high: { label: 'Hoog', variant: 'warning' },
  medium: { label: 'Medium', variant: 'accent' },
  low: { label: 'Laag', variant: 'neutral' },
};

export function WebsiteKansenPage() {
  const [sector, setSector] = useState('');
  const [priority, setPriority] = useState('');

  const { data, isLoading } = useQuery({
    queryKey: ['website-opportunities', 2000],
    queryFn: () => api.get<{ opportunities: Opportunity[]; total: number }>('/website-opportunities?limit=2000'),
  });

  const filtered = useMemo(() => {
    return (data?.opportunities || [])
      // total_score 0 = niet (volledig) geanalyseerd → geen echte kans
      .filter((o) => (o.total_score ?? 0) > 0)
      .filter((o) => !sector || o.sector === sector)
      .filter((o) => !priority || o.priority === priority);
    // endpoint levert al worst-first (total_score ASC)
  }, [data, sector, priority]);

  const counts = useMemo(() => {
    const c = { urgent: 0, high: 0, medium: 0, low: 0 } as Record<string, number>;
    for (const o of filtered) if (o.priority && o.priority in c) c[o.priority]++;
    return c;
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
          <option value="urgent">Urgent</option>
          <option value="high">Hoog</option>
          <option value="medium">Medium</option>
          <option value="low">Laag</option>
        </select>
      </div>

      <div className="flex gap-5 text-sm text-[var(--color-stone-500)] mb-4 px-1">
        <span><strong className="text-[var(--color-stone-800)]">{fmtInt(filtered.length)}</strong> kansen</span>
        {data && data.opportunities.length < data.total && (
          <>
            <span>·</span>
            <span className="text-[var(--color-warning)]">⚠ {fmtInt(data.opportunities.length)}/{fmtInt(data.total)} opgehaald</span>
          </>
        )}
        <span>·</span>
        <span>🔴 <strong className="text-[var(--color-stone-800)]">{counts.urgent}</strong> urgent</span>
        <span>🟠 <strong className="text-[var(--color-stone-800)]">{counts.high}</strong> hoog</span>
        <span>🟡 <strong className="text-[var(--color-stone-800)]">{counts.medium}</strong> medium</span>
      </div>

      <div className="flex flex-col gap-3">
        {isLoading && Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="skeleton h-36 rounded-xl" />
        ))}
        {!isLoading && filtered.length === 0 && (
          <Card className="p-12 text-center text-[var(--color-stone-500)]">
            <AlertTriangle className="h-6 w-6 mx-auto mb-3 text-[var(--color-stone-300)]" />
            Geen kansen met deze filters. Draai de website-analyse om scores te genereren.
          </Card>
        )}
        {filtered.map((o) => <KansenCard key={o.lead_id} o={o} />)}
      </div>
    </div>
  );
}

function KansenCard({ o }: { o: Opportunity }) {
  const navigate = useNavigate();
  const score = o.total_score || 0;
  const color = scoreColor(score);
  const prio = o.priority ? PRIO[o.priority] : undefined;
  const reasons = Object.entries(o.opportunity_reasons || {});

  return (
    // div+navigate i.p.v. <Link>: de kaart bevat een echte domein-<a>, en een
    // <a> in een <a> is invalide HTML (browser splitst de DOM op).
    <div onClick={() => navigate(`/leads/${o.lead_id}`)} className="block cursor-pointer">
      <Card className="grid grid-cols-[132px_1fr_180px] gap-5 p-4 transition-colors hover:border-[var(--color-stone-200)]">
        {/* Screenshot */}
        <div className="aspect-[4/3] rounded-md overflow-hidden bg-[var(--color-ivory-100)] border border-[var(--color-border)] flex items-center justify-center">
          {o.screenshot_desktop_url ? (
            <img src={o.screenshot_desktop_url} alt={o.company_name || ''} loading="lazy" className="w-full h-full object-cover object-top" />
          ) : (
            <ImageOff className="h-6 w-6 text-[var(--color-stone-300)]" />
          )}
        </div>

        {/* Content */}
        <div className="min-w-0">
          <div className="flex items-baseline gap-2 mb-1 flex-wrap">
            <span className="font-display text-lg font-semibold text-[var(--color-stone-800)]">{o.company_name || '?'}</span>
            <Badge variant="accent">{SECTOR_LABEL[o.sector || ''] || o.sector || '—'}</Badge>
          </div>
          <div className="text-xs text-[var(--color-stone-500)] mb-2">
            {o.domain && (
              <a
                href={`https://${o.domain}`}
                target="_blank"
                rel="noopener"
                onClick={(e) => e.stopPropagation()}
                className="text-[var(--color-blush-500)] hover:underline inline-flex items-center gap-0.5"
              >
                {o.domain} <ExternalLink className="h-2.5 w-2.5" />
              </a>
            )}
            {o.city && <> · {o.city}</>}
          </div>
          {/* Dienst-tags uit opportunity_types */}
          <div className="flex flex-wrap gap-1.5 mb-2">
            {(o.opportunity_types || []).map((t) => (
              <Badge key={t} variant="neutral">{OPP_LABEL[t] || t}</Badge>
            ))}
          </div>
          {/* Echte pijnpunten uit opportunity_reasons */}
          {reasons.length > 0 && (
            <ul className="text-xs text-[var(--color-stone-600)] space-y-0.5">
              {reasons.slice(0, 3).map(([t, reason]) => (
                <li key={t}><span className="text-[var(--color-stone-400)]">{OPP_LABEL[t] || t}:</span> {reason}</li>
              ))}
            </ul>
          )}
        </div>

        {/* Score + priority */}
        <div className="flex flex-col items-end justify-between">
          <div className="flex flex-col items-end gap-1">
            {prio && <Badge variant={prio.variant}>{prio.label}</Badge>}
            {o.review_status && REVIEW_BADGE[o.review_status] && (
              <Badge variant={REVIEW_BADGE[o.review_status].variant}>{REVIEW_BADGE[o.review_status].label}</Badge>
            )}
          </div>
          <div className="text-right">
            <div className="font-display text-4xl font-semibold leading-none tracking-tight" style={{ color }}>{score}</div>
            <div className="text-[10px] mt-1 text-[var(--color-stone-500)]">/ 100 website-score</div>
          </div>
        </div>
      </Card>
    </div>
  );
}
