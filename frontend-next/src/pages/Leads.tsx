import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link, useNavigate } from 'react-router-dom';
import { Star, Search, ExternalLink, Phone, Mail, Users as UsersIcon, Camera, Calendar } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtInt } from '@/lib/format';
import type { Lead, LeadsResponse } from '@/lib/types';
import { SECTOR_LABEL } from '@/lib/types';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/cn';

const SECTOR_FILTERS: { value: string; label: string }[] = [
  { value: '', label: 'Alle actieve sectoren' },
  { value: 'cosmetische_behandelaars', label: 'Cosmetisch' },
  { value: 'alternatieve_geneeskunde', label: 'Alt. geneeskunde' },
];

const SCORE_FILTERS = [
  { value: 0, label: 'Alle scores' },
  { value: 70, label: 'Hot (70+)' },
  { value: 45, label: 'Warm (45+)' },
  { value: 30, label: 'Koud (30-44)' },
];

const SORTS = [
  { value: 'score', label: 'Score (hoog → laag)' },
  { value: 'company', label: 'Naam A-Z' },
  { value: 'rating', label: 'Google rating' },
  { value: 'reviews', label: 'Meeste reviews' },
];

export function LeadsPage() {
  const [search, setSearch] = useState('');
  const [sectorFilter, setSectorFilter] = useState('');
  const [minScore, setMinScore] = useState(0);
  const [sortBy, setSortBy] = useState('score');
  const [receptieOnly, setReceptieOnly] = useState(false);

  const { data, isLoading, isFetching, isError, error, refetch } = useQuery({
    queryKey: ['leads', 500],
    queryFn: () => api.get<LeadsResponse>('/leads?limit=500'),
    refetchInterval: 8000,
  });

  const filtered = useMemo(() => {
    const leads = (data?.leads || []).filter((l) => l.status !== 'archived');
    const q = search.trim().toLowerCase();
    return leads
      .filter((l) => {
        if (sectorFilter && l.sector !== sectorFilter) return false;
        if ((l.score || 0) < minScore) return false;
        if (receptieOnly && !l.receptie_hook_code) return false;
        if (!q) return true;
        return (
          (l.company_name || '').toLowerCase().includes(q) ||
          (l.domain || '').toLowerCase().includes(q) ||
          (l.city || '').toLowerCase().includes(q)
        );
      })
      .sort((a, b) => {
        switch (sortBy) {
          case 'company':
            return (a.company_name || '').localeCompare(b.company_name || '');
          case 'rating':
            return (b.google_rating || 0) - (a.google_rating || 0);
          case 'reviews':
            return (b.google_review_count || 0) - (a.google_review_count || 0);
          default:
            return (b.score || 0) - (a.score || 0);
        }
      });
  }, [data, search, sectorFilter, minScore, sortBy, receptieOnly]);

  return (
    <div className="max-w-7xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Lead database"
        title="Leads"
        subtitle={isLoading ? 'Laden…' : `${fmtInt(filtered.length)} leads ${isFetching ? '· ververst…' : ''}`}
        actions={
          <Link
            to="/zoeken"
            className="inline-flex items-center gap-2 h-10 px-4 rounded-md text-sm font-medium bg-[var(--color-blush-500)] text-white hover:bg-[var(--color-blush-600)] transition-colors"
          >
            + Nieuwe zoekopdracht
          </Link>
        }
      />

      <Card className="overflow-hidden">
        <div className="flex items-center gap-3 p-4 border-b border-[var(--color-border)] flex-wrap">
          <div className="flex gap-1.5">
            {SECTOR_FILTERS.map((f) => (
              <button
                key={f.value}
                onClick={() => setSectorFilter(f.value)}
                className={cn(
                  'px-3 py-1.5 text-xs rounded-md border transition-colors font-medium',
                  sectorFilter === f.value
                    ? 'bg-[var(--color-blush-100)] border-[var(--color-blush-500)] text-[var(--color-blush-700)]'
                    : 'bg-white border-[var(--color-border)] text-[var(--color-stone-600)] hover:border-[var(--color-blush-400)]'
                )}
              >
                {f.label}
              </button>
            ))}
          </div>

          <button
            onClick={() => setReceptieOnly((v) => !v)}
            className={cn(
              'px-3 py-1.5 text-xs rounded-md border transition-colors font-medium',
              receptieOnly
                ? 'bg-[var(--color-blush-100)] border-[var(--color-blush-500)] text-[var(--color-blush-700)]'
                : 'bg-white border-[var(--color-border)] text-[var(--color-stone-600)] hover:border-[var(--color-blush-400)]'
            )}
            title="Toon alleen leads met een receptie-haak (mail-1)"
          >
            Receptie-haak
          </button>

          <div className="relative flex-1 min-w-[220px]">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-[var(--color-stone-400)]" />
            <Input
              placeholder="Zoek bedrijf, domein, stad…"
              className="pl-9"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>

          <select
            value={minScore}
            onChange={(e) => setMinScore(Number(e.target.value))}
            className="h-10 px-3 rounded-md border border-[var(--color-border)] bg-white text-sm"
          >
            {SCORE_FILTERS.map((f) => (
              <option key={f.value} value={f.value}>{f.label}</option>
            ))}
          </select>

          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            className="h-10 px-3 rounded-md border border-[var(--color-border)] bg-white text-sm"
          >
            {SORTS.map((s) => (
              <option key={s.value} value={s.value}>{s.label}</option>
            ))}
          </select>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--color-border)] bg-[var(--color-ivory-100)]">
                <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Bedrijf</th>
                <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Contact</th>
                <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Sector · Grootte</th>
                <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Rating</th>
                <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Website</th>
                <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Score</th>
                <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Signalen</th>
              </tr>
            </thead>
            <tbody>
              {isLoading && Array.from({ length: 8 }).map((_, i) => (
                <tr key={i} className="border-b border-[var(--color-ivory-200)]">
                  {Array.from({ length: 7 }).map((_, j) => (
                    <td key={j} className="px-4 py-3"><div className="skeleton h-4" /></td>
                  ))}
                </tr>
              ))}
              {isError && (
                <tr>
                  <td colSpan={7} className="px-4 py-16 text-center">
                    <p className="text-[var(--color-danger)] font-medium mb-1">Kon leads niet laden</p>
                    <p className="text-xs text-[var(--color-stone-500)] mb-3">{error instanceof Error ? error.message : 'Onbekende fout'}</p>
                    <button onClick={() => refetch()} className="rounded bg-[var(--color-blush-500)] px-3 py-1.5 text-sm text-white hover:opacity-90">Opnieuw proberen</button>
                  </td>
                </tr>
              )}
              {!isLoading && !isError && filtered.length === 0 && (
                <tr><td colSpan={7} className="px-4 py-16 text-center text-[var(--color-stone-500)]">Geen leads met deze filters.</td></tr>
              )}
              {filtered.map((l) => (
                <LeadRow key={l.id} lead={l} />
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

function LeadRow({ lead }: { lead: Lead }) {
  const navigate = useNavigate();
  const sectorLabel = SECTOR_LABEL[lead.sector || ''] || lead.sector || '—';
  const score = lead.score || 0;
  const scoreColor = score >= 70 ? 'success' : score >= 45 ? 'warning' : score >= 30 ? 'accent' : 'neutral';
  return (
    <tr
      className="border-b border-[var(--color-ivory-200)] hover:bg-[var(--color-ivory-50)] transition-colors cursor-pointer"
      onClick={() => navigate(`/leads/${lead.id}`)}
    >
      <td className="px-4 py-3">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-[var(--color-blush-100)] text-[var(--color-blush-700)] text-xs font-semibold">
            {(lead.company_name || '?').slice(0, 2).toUpperCase()}
          </div>
          <div className="min-w-0">
            <div className="font-medium text-[var(--color-stone-800)] flex items-center gap-1.5 min-w-0">
              <span className="truncate">{lead.company_name || '—'}</span>
              {lead.receptie_hook_code && (
                <Badge variant="info" className="shrink-0" title="Receptie-haak (mail-1)">{lead.receptie_hook_code}</Badge>
              )}
            </div>
            <div className="text-xs text-[var(--color-stone-500)] truncate flex items-center gap-1">
              {lead.domain && (
                <a
                  href={`https://${lead.domain}`}
                  target="_blank"
                  rel="noopener"
                  onClick={(e) => e.stopPropagation()}
                  className="hover:text-[var(--color-blush-500)] inline-flex items-center gap-0.5"
                >
                  {lead.domain} <ExternalLink className="h-2.5 w-2.5" />
                </a>
              )}
              {lead.city && <span>· {lead.city}</span>}
            </div>
          </div>
        </div>
      </td>
      {/* Contact column */}
      <td className="px-4 py-3 text-xs">
        <div className="flex flex-col gap-0.5">
          {lead.phone ? (
            <a
              href={`tel:${lead.phone.replace(/\s/g, '')}`}
              onClick={(e) => e.stopPropagation()}
              className="inline-flex items-center gap-1 text-[var(--color-stone-700)] hover:text-[var(--color-blush-500)] tabular-nums"
            >
              <Phone className="h-3 w-3" /> {lead.phone}
            </a>
          ) : (
            <span className="text-[var(--color-stone-400)] inline-flex items-center gap-1">
              <Phone className="h-3 w-3" /> —
            </span>
          )}
          {lead.email ? (
            <a
              href={`mailto:${lead.email}`}
              onClick={(e) => e.stopPropagation()}
              className="inline-flex items-center gap-1 text-[var(--color-blush-500)] hover:underline truncate max-w-[180px]"
            >
              <Mail className="h-3 w-3" /> {lead.email}
            </a>
          ) : (
            <span className="text-[var(--color-stone-400)] inline-flex items-center gap-1">
              <Mail className="h-3 w-3" /> —
            </span>
          )}
          {lead.contact_first_name && (
            <span className="text-[var(--color-stone-600)]">{lead.contact_first_name}</span>
          )}
        </div>
      </td>
      {/* Sector + size column */}
      <td className="px-4 py-3">
        <div className="flex flex-col gap-1 items-start">
          <Badge variant="accent">{sectorLabel}</Badge>
          {lead.company_size_estimate && (
            <span className="text-[10px] text-[var(--color-stone-500)] inline-flex items-center gap-1">
              <UsersIcon className="h-2.5 w-2.5" /> {lead.company_size_estimate}
            </span>
          )}
        </div>
      </td>
      <td className="px-4 py-3 text-sm">
        {lead.google_rating ? (
          <span className="inline-flex items-center gap-1 tabular-nums">
            <Star className="h-3 w-3 fill-[var(--color-warning)] text-[var(--color-warning)]" />
            {lead.google_rating}
            <span className="text-xs text-[var(--color-stone-400)]">
              ({lead.google_review_count || 0})
            </span>
          </span>
        ) : (
          <span className="text-[var(--color-stone-400)]">—</span>
        )}
      </td>
      <td className="px-4 py-3">
        {lead.website_score != null ? (
          <div className="flex items-center gap-2">
            <span className="tabular-nums text-sm font-medium min-w-[28px]">{lead.website_score}</span>
            <div className="h-1.5 w-16 rounded-full bg-[var(--color-ivory-200)] overflow-hidden">
              <div
                className="h-full rounded-full"
                style={{
                  width: `${lead.website_score}%`,
                  background:
                    lead.website_score < 30 ? 'var(--color-danger)' :
                    lead.website_score < 45 ? '#ea580c' :
                    lead.website_score < 70 ? 'var(--color-warning)' :
                    'var(--color-success)',
                }}
              />
            </div>
          </div>
        ) : (
          <span className="text-[var(--color-stone-400)]">—</span>
        )}
      </td>
      <td className="px-4 py-3">
        <Badge variant={scoreColor as 'success' | 'warning' | 'accent' | 'neutral'}>{score}</Badge>
      </td>
      {/* Signalen column: booking / IG / ads / reviews-cadans */}
      <td className="px-4 py-3">
        <div className="flex flex-wrap gap-1">
          {lead.has_online_booking && (
            <span title="Online booking aanwezig" className="inline-flex items-center gap-0.5 rounded px-1.5 py-0.5 bg-[var(--color-success-bg)] text-[var(--color-success)] text-[10px]"><Calendar className="h-2.5 w-2.5" /></span>
          )}
          {lead.has_instagram && (
            <span title="Instagram aanwezig" className="inline-flex items-center gap-0.5 rounded px-1.5 py-0.5 bg-pink-100 text-pink-700 text-[10px]"><Camera className="h-2.5 w-2.5" /></span>
          )}
          {lead.meta_ads_active && (
            <span title="Meta ads actief" className="inline-flex items-center gap-0.5 rounded px-1.5 py-0.5 bg-[var(--color-info-bg)] text-[var(--color-info)] text-[10px]">Ads</span>
          )}
          {!lead.has_online_booking && !lead.has_instagram && !lead.meta_ads_active && (
            <span className="text-[var(--color-stone-400)] text-[10px]">—</span>
          )}
        </div>
      </td>
    </tr>
  );
}
