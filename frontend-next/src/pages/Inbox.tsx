import { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { Mail, Reply, Sparkles, Copy, Check, ChevronDown, ChevronUp } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtRelative } from '@/lib/format';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

interface Reply {
  id: string;
  lead_id: string;
  subject: string | null;
  body_preview: string | null;
  sender_email: string | null;
  classification: 'interested' | 'not_interested' | 'oooo' | 'question' | string | null;
  received_at: string;
  company_name?: string;
}

interface DraftResponse {
  draft: string | null;
  category: string;
  skip_reason: string | null;
  cached: boolean;
  cost_eur: number;
}

export function InboxPage() {
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['reply-inbox'],
    queryFn: () => api.get<{ replies: Reply[] }>('/reply-inbox?limit=100'),
    refetchInterval: 30_000,
  });

  const replies = data?.replies || [];

  return (
    <div className="max-w-6xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Unified inbox"
        title="Inbox"
        subtitle="Alle Warmr replies — Claude stelt een draft-antwoord voor, jij stuurt hem zelf."
      />

      {isLoading && <div className="skeleton h-64" />}

      {/* Fout ≠ lege inbox: een API-fout als "Geen replies nog" presenteren
          verbergt config-issues. Aparte error-state met retry. */}
      {!isLoading && isError && (
        <Card className="p-10 text-center border-[var(--color-danger)]">
          <p className="text-[var(--color-danger)] font-medium mb-1">Kon inbox niet laden</p>
          <p className="text-xs text-[var(--color-stone-500)] mb-3">{error instanceof Error ? error.message : 'Onbekende fout'}</p>
          <button onClick={() => refetch()} className="rounded bg-[var(--color-blush-500)] px-3 py-1.5 text-sm text-white hover:opacity-90">Opnieuw proberen</button>
        </Card>
      )}

      {!isLoading && !isError && replies.length === 0 && (
        <Card className="p-16 text-center">
          <Mail className="h-8 w-8 mx-auto mb-3 text-[var(--color-stone-300)]" />
          <h3 className="font-display text-xl font-semibold mb-2">Geen replies nog</h3>
          <p className="text-sm text-[var(--color-stone-500)] max-w-md mx-auto">
            Zodra prospects reageren op je Warmr-campagnes zie je ze hier — met AI-classificatie op intent.
          </p>
        </Card>
      )}

      {!isLoading && !isError && replies.length > 0 && (
        <Card className="overflow-hidden">
          <div className="divide-y divide-[var(--color-ivory-200)]">
            {replies.map((r) => <ReplyRow key={r.id} reply={r} />)}
          </div>
        </Card>
      )}
    </div>
  );
}

function ReplyRow({ reply }: { reply: Reply }) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);

  const classMap: Record<string, { label: string; variant: 'success' | 'danger' | 'warning' | 'neutral' }> = {
    interested: { label: 'Geïnteresseerd', variant: 'success' },
    not_interested: { label: 'Niet geïnteresseerd', variant: 'danger' },
    not_now: { label: 'Niet nu', variant: 'warning' },
    wrong_person: { label: 'Verkeerde persoon', variant: 'warning' },
    unsubscribe_request: { label: 'Uitschrijven', variant: 'danger' },
    auto_reply: { label: 'OOO', variant: 'neutral' },
    oooo: { label: 'OOO', variant: 'neutral' },
    question: { label: 'Vraag', variant: 'warning' },
    other: { label: 'Anders', variant: 'neutral' },
  };
  const cls = classMap[reply.classification || ''] || { label: reply.classification || 'ongeclassificeerd', variant: 'neutral' as const };

  const draftMut = useMutation({
    mutationFn: () => api.post<DraftResponse>(`/replies/${reply.id}/draft`, {}),
  });

  const onToggle = () => {
    const next = !open;
    setOpen(next);
    if (next && !draftMut.data && !draftMut.isPending) {
      draftMut.mutate();
    }
  };

  const onCopy = async () => {
    if (!draftMut.data?.draft) return;
    try {
      await navigator.clipboard.writeText(draftMut.data.draft);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      /* clipboard onbeschikbaar — silent */
    }
  };

  return (
    <div className="hover:bg-[var(--color-ivory-50)]">
      <div className="p-4 flex items-start gap-4">
        <Reply className="h-4 w-4 mt-1 text-[var(--color-stone-400)] shrink-0" />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-sm font-semibold">{reply.company_name || reply.sender_email || '—'}</span>
            <Badge variant={cls.variant}>{cls.label}</Badge>
            <span className="text-xs text-[var(--color-stone-400)] ml-auto">{fmtRelative(reply.received_at)}</span>
          </div>
          {reply.subject && (
            <div className="text-sm text-[var(--color-stone-700)] truncate">{reply.subject}</div>
          )}
          {reply.body_preview && (
            <div className="text-xs text-[var(--color-stone-500)] line-clamp-2 mt-1">{reply.body_preview}</div>
          )}
          <button
            onClick={onToggle}
            className="mt-2 inline-flex items-center gap-1.5 text-xs text-[var(--color-blush-600)] hover:text-[var(--color-blush-700)]"
          >
            <Sparkles className="h-3 w-3" />
            {open ? 'Verberg suggestie' : 'Toon Claude-suggestie'}
            {open ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
          </button>
        </div>
      </div>

      {open && (
        <div className="px-4 pb-4 pl-12">
          <div className="rounded-md border border-[var(--color-ivory-200)] bg-[var(--color-ivory-100)] p-4">
            {draftMut.isPending && (
              <div className="text-xs text-[var(--color-stone-500)]">Claude denkt na…</div>
            )}
            {draftMut.isError && (
              <div className="text-xs text-[var(--color-danger)]">
                Suggestie ophalen mislukt. Check API-log.
              </div>
            )}
            {draftMut.data?.skip_reason && (
              <div className="text-xs text-[var(--color-stone-600)] italic">
                {draftMut.data.skip_reason}
              </div>
            )}
            {draftMut.data?.draft && (
              <>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2 text-[10px] text-[var(--color-stone-500)] uppercase tracking-wider font-semibold">
                    <Sparkles className="h-3 w-3" />
                    Suggestie
                    {draftMut.data.cached && <span className="text-[var(--color-stone-400)] normal-case font-normal lowercase">(cached)</span>}
                  </div>
                  <button
                    onClick={onCopy}
                    className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded bg-white border border-[var(--color-border)] hover:border-[var(--color-blush-400)]"
                  >
                    {copied ? (
                      <><Check className="h-3 w-3 text-[var(--color-success)]" /> Gekopieerd</>
                    ) : (
                      <><Copy className="h-3 w-3" /> Kopieer</>
                    )}
                  </button>
                </div>
                <pre className="whitespace-pre-wrap text-sm leading-relaxed text-[var(--color-stone-800)] font-sans">
{draftMut.data.draft}
                </pre>
                <div className="mt-3 pt-3 border-t border-[var(--color-ivory-200)] text-[10px] text-[var(--color-stone-500)]">
                  Heatr verstuurt niets — kopieer naar je mailclient en check vóór sturen.
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
