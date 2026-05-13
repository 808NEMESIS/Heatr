import { useQuery } from '@tanstack/react-query';
import { Send, Play, Pause, CheckCircle2, Plus } from 'lucide-react';
import { Link } from 'react-router-dom';
import { api } from '@/lib/api';
import { fmtInt, fmtRelative } from '@/lib/format';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

interface Campaign {
  id: string;
  name: string;
  status: 'draft' | 'active' | 'paused' | 'completed' | string;
  inbox_count?: number;
  leads_count?: number;
  sent_count?: number;
  reply_count?: number;
  created_at: string;
}

interface Inbox {
  id: string;
  email: string;
  status: string;
  daily_cap?: number;
  warmup_score?: number;
}

export function CampagnesPage() {
  const { data: camps, isLoading: lc } = useQuery({
    queryKey: ['campaigns'],
    queryFn: () => api.get<{ campaigns: Campaign[] }>('/campaigns').catch(() => ({ campaigns: [] })),
  });

  const { data: inboxes, isLoading: li } = useQuery({
    queryKey: ['warmr-inboxes'],
    queryFn: () => api.get<{ inboxes: Inbox[] }>('/warmr/inboxes').catch(() => ({ inboxes: [] })),
  });

  const campList = camps?.campaigns || [];
  const inboxList = inboxes?.inboxes || [];

  return (
    <div className="max-w-6xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Outbound"
        title="Campagnes"
        subtitle="Stuur leads naar Warmr — track sends, replies, en inbox warmup."
        actions={
          <Link
            to="/campagnes/nieuw"
            className="inline-flex items-center gap-2 h-10 px-4 rounded-md text-sm font-medium bg-[var(--color-blush-500)] text-white hover:bg-[var(--color-blush-600)]"
          >
            <Plus className="h-4 w-4" /> Nieuwe campagne
          </Link>
        }
      />

      {/* Warmr inboxes */}
      <section className="mb-8">
        <h3 className="font-display text-lg font-semibold mb-3">Warmr inboxes</h3>
        {li ? (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {Array.from({ length: 3 }).map((_, i) => <div key={i} className="skeleton h-24" />)}
          </div>
        ) : inboxList.length === 0 ? (
          <Card className="p-6 text-sm text-[var(--color-stone-500)]">
            Geen Warmr inboxes geconfigureerd. Voeg er één toe in Warmr om campagnes te kunnen draaien.
          </Card>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {inboxList.map((i) => (
              <Card key={i.id} className="p-4">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm font-medium truncate">{i.email}</div>
                  <Badge variant={i.status === 'ready' ? 'success' : 'warning'}>{i.status}</Badge>
                </div>
                <div className="flex items-center gap-3 text-xs text-[var(--color-stone-500)]">
                  {i.daily_cap != null && <span>cap: {i.daily_cap}/dag</span>}
                  {i.warmup_score != null && <span>warmup: {i.warmup_score}</span>}
                </div>
              </Card>
            ))}
          </div>
        )}
      </section>

      {/* Campaigns */}
      <section>
        <h3 className="font-display text-lg font-semibold mb-3">Campagnes</h3>
        {lc ? (
          <div className="skeleton h-40" />
        ) : campList.length === 0 ? (
          <Card className="p-12 text-center">
            <Send className="h-8 w-8 mx-auto mb-3 text-[var(--color-stone-300)]" />
            <h4 className="font-display text-lg font-semibold mb-2">Geen campagnes</h4>
            <p className="text-sm text-[var(--color-stone-500)] max-w-md mx-auto mb-4">
              Maak je eerste campagne: selecteer leads, kies een sequence, assign inboxes, launch via Warmr.
            </p>
          </Card>
        ) : (
          <Card className="overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-[var(--color-ivory-100)]">
                  <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Naam</th>
                  <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Status</th>
                  <th className="text-right px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Leads</th>
                  <th className="text-right px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Verstuurd</th>
                  <th className="text-right px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Replies</th>
                  <th className="text-left px-4 py-3 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Aangemaakt</th>
                </tr>
              </thead>
              <tbody>
                {campList.map((c) => (
                  <tr key={c.id} className="border-b border-[var(--color-ivory-200)] hover:bg-[var(--color-ivory-50)]">
                    <td className="px-4 py-3 font-medium">{c.name}</td>
                    <td className="px-4 py-3">
                      <Badge variant={c.status === 'active' ? 'success' : c.status === 'paused' ? 'warning' : 'neutral'}>
                        {c.status === 'active' ? <Play className="h-3 w-3" /> : c.status === 'paused' ? <Pause className="h-3 w-3" /> : <CheckCircle2 className="h-3 w-3" />}
                        {c.status}
                      </Badge>
                    </td>
                    <td className="px-4 py-3 text-right tabular-nums">{fmtInt(c.leads_count)}</td>
                    <td className="px-4 py-3 text-right tabular-nums">{fmtInt(c.sent_count)}</td>
                    <td className="px-4 py-3 text-right tabular-nums">{fmtInt(c.reply_count)}</td>
                    <td className="px-4 py-3 text-xs text-[var(--color-stone-500)]">{fmtRelative(c.created_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        )}
      </section>
    </div>
  );
}
