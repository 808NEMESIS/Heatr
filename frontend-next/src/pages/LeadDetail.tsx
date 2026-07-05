import { useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { ArrowLeft, ExternalLink, Mail, Phone, Building2, MapPin, Calendar, Globe, Camera, Hash, Store, Megaphone, Clock, FlaskConical } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtInt, fmtRelative, priorityFromScore, scoreColor } from '@/lib/format';
import type { Lead } from '@/lib/types';
import { SECTOR_LABEL } from '@/lib/types';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';

interface Contact { id: string; name?: string; email?: string; role?: string; source?: string; is_primary?: boolean; confidence?: number; why_chosen?: string; }
interface TimelineEvent { id: string; event_type: string; title: string; body?: string; created_at: string; }

export function LeadDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [tab, setTab] = useState('overview');
  const qc = useQueryClient();

  const { data: lead, isLoading } = useQuery({
    queryKey: ['lead', id],
    queryFn: () => api.get<Lead>(`/leads/${id}`),
    enabled: !!id,
  });

  const toggleTestMode = useMutation({
    mutationFn: (next: boolean) =>
      api.post(`/leads/${id}/test-mode`, { is_test_lead: next }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['lead', id] }),
  });

  const { data: contacts } = useQuery({
    queryKey: ['lead-contacts', id],
    queryFn: () => api.get<{ contacts: Contact[] }>(`/leads/${id}/contacts`).catch(() => ({ contacts: [] })),
    enabled: !!id,
  });

  const { data: tl } = useQuery({
    queryKey: ['lead-timeline', id],
    queryFn: () => api.get<{ events: TimelineEvent[] }>(`/timeline/${id}`).catch(() => ({ events: [] })),
    enabled: !!id,
  });

  if (isLoading || !lead) {
    return (
      <div className="max-w-6xl mx-auto px-10 py-8">
        <div className="skeleton h-12 w-64 mb-4" />
        <div className="skeleton h-48" />
      </div>
    );
  }

  const prio = priorityFromScore(lead.website_score);

  return (
    <div className="max-w-6xl mx-auto px-10 py-8">
      <Link to="/leads" className="inline-flex items-center gap-1.5 text-sm text-[var(--color-stone-500)] hover:text-[var(--color-blush-500)] mb-4">
        <ArrowLeft className="h-3.5 w-3.5" /> Terug naar leads
      </Link>

      {/* Hero */}
      <div className="mb-6 flex items-start justify-between gap-6 flex-wrap">
        <div>
          <div className="flex items-center gap-3 mb-2">
            <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-[var(--color-blush-100)] text-[var(--color-blush-700)] font-semibold">
              {(lead.company_name || '?').slice(0, 2).toUpperCase()}
            </div>
            <div>
              <h1 className="font-display text-3xl font-semibold leading-tight flex items-center gap-2">
                {lead.company_name || '—'}
                {lead.is_test_lead && (
                  <span
                    className="inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded bg-amber-100 text-amber-800"
                    title="Test-mode actief: gate-bypass + BCC + [TEST]-prefix"
                  >
                    <FlaskConical className="h-3 w-3" /> TEST
                  </span>
                )}
              </h1>
              <div className="flex items-center gap-2 text-sm text-[var(--color-stone-500)] mt-0.5">
                {lead.city && <><MapPin className="h-3 w-3" /> {lead.city}</>}
                {lead.sector && (
                  <>
                    <span>·</span>
                    <Badge variant="accent">{SECTOR_LABEL[lead.sector] || lead.sector}</Badge>
                  </>
                )}
                {lead.status && <Badge variant="neutral">{lead.status}</Badge>}
              </div>
            </div>
          </div>
          {lead.domain && (
            <a
              href={`https://${lead.domain}`}
              target="_blank"
              rel="noopener"
              className="inline-flex items-center gap-1.5 text-sm text-[var(--color-blush-500)] hover:underline"
            >
              <Globe className="h-3.5 w-3.5" />
              {lead.domain}
              <ExternalLink className="h-3 w-3" />
            </a>
          )}
        </div>

        <div className="flex gap-2">
          <a
            href={lead.google_maps_url || '#'}
            target="_blank"
            rel="noopener"
            className="inline-flex items-center gap-2 h-10 px-3 rounded-md border border-[var(--color-border)] text-sm hover:bg-[var(--color-ivory-100)]"
          >
            Google Maps <ExternalLink className="h-3 w-3" />
          </a>
          <button
            onClick={() => toggleTestMode.mutate(!lead.is_test_lead)}
            disabled={toggleTestMode.isPending}
            title={lead.is_test_lead ? 'Test-mode uitschakelen' : 'Lead markeren als testlead — bypass email-gate, BCC + [TEST]-prefix bij send'}
            className={`inline-flex items-center gap-2 h-10 px-3 rounded-md text-sm font-medium border transition-colors ${
              lead.is_test_lead
                ? 'bg-amber-100 border-amber-300 text-amber-800 hover:bg-amber-200'
                : 'border-[var(--color-border)] hover:bg-[var(--color-ivory-100)]'
            }`}
          >
            <FlaskConical className="h-3.5 w-3.5" />
            {lead.is_test_lead ? 'Test-mode aan' : 'Mark as test lead'}
          </button>
          <button className="inline-flex items-center gap-2 h-10 px-4 rounded-md bg-[var(--color-blush-500)] text-white text-sm font-medium hover:bg-[var(--color-blush-600)]">
            Review email sturen
          </button>
        </div>
      </div>

      {/* Quick stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <QuickStat label="Lead-score" value={lead.score ?? '—'} accent="primary" />
        <QuickStat
          label="Website-score"
          value={lead.website_score ?? '—'}
          sub={lead.website_score != null ? prio.label : undefined}
          color={lead.website_score != null ? scoreColor(lead.website_score) : undefined}
        />
        <QuickStat
          label="ICP match"
          value={lead.icp_match != null ? `${Math.round(lead.icp_match * 100)}%` : '—'}
        />
        <QuickStat
          label="Google rating"
          value={lead.google_rating ? `${lead.google_rating}★` : '—'}
          sub={lead.google_review_count != null ? `${fmtInt(lead.google_review_count)} reviews` : undefined}
        />
      </div>

      {/* Tabs */}
      <Tabs value={tab} onValueChange={setTab} className="mb-6">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="website">Website</TabsTrigger>
          <TabsTrigger value="contacts">Contacts ({contacts?.contacts?.length || 0})</TabsTrigger>
          <TabsTrigger value="thread">Thread</TabsTrigger>
          <TabsTrigger value="timeline">Timeline</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-5">
          <WhyThisLead lead={lead} />
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
            <Card className="p-5">
              <h3 className="font-display text-base font-semibold mb-4 flex items-center gap-2">
                <Phone className="h-4 w-4 text-[var(--color-blush-500)]" /> Contact
              </h3>
              <div className="space-y-3 text-sm">
                {/* Phone — primary action */}
                {lead.phone ? (
                  <a
                    href={`tel:${lead.phone.replace(/\s/g, '')}`}
                    className="flex items-center gap-3 rounded-md bg-[var(--color-ivory-100)] px-3 py-2.5 hover:bg-[var(--color-blush-100)] transition-colors"
                  >
                    <Phone className="h-4 w-4 text-[var(--color-blush-500)]" />
                    <div className="flex-1">
                      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Telefoon</div>
                      <div className="font-medium tabular-nums">{lead.phone}</div>
                    </div>
                    <span className="text-xs text-[var(--color-blush-500)]">Bel →</span>
                  </a>
                ) : (
                  <div className="flex items-center gap-3 rounded-md bg-[var(--color-ivory-50)] px-3 py-2.5">
                    <Phone className="h-4 w-4 text-[var(--color-stone-300)]" />
                    <div className="text-[var(--color-stone-400)] text-sm italic">Telefoon nog niet gevonden</div>
                  </div>
                )}

                {/* Email */}
                {lead.email ? (
                  <a
                    href={`mailto:${lead.email}`}
                    className="flex items-center gap-3 rounded-md bg-[var(--color-ivory-100)] px-3 py-2.5 hover:bg-[var(--color-blush-100)] transition-colors"
                  >
                    <Mail className="h-4 w-4 text-[var(--color-blush-500)]" />
                    <div className="flex-1 min-w-0">
                      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Email</div>
                      <div className="font-medium truncate">{lead.email}</div>
                    </div>
                    {lead.email_status && <Badge variant={lead.email_status === 'valid' ? 'success' : lead.email_status === 'risky' ? 'warning' : 'neutral'}>{lead.email_status}</Badge>}
                  </a>
                ) : (
                  <div className="flex items-center gap-3 rounded-md bg-[var(--color-ivory-50)] px-3 py-2.5">
                    <Mail className="h-4 w-4 text-[var(--color-stone-300)]" />
                    <div className="text-[var(--color-stone-400)] text-sm italic">Email nog niet gevonden — valt terug op info@ pattern</div>
                  </div>
                )}

                {/* Contact person */}
                {lead.contact_first_name && (
                  <Row icon={Building2} label="Contactpersoon" value={lead.contact_first_name} />
                )}

                {/* Address via Google Maps */}
                {lead.google_maps_url && (
                  <a
                    href={lead.google_maps_url}
                    target="_blank"
                    rel="noopener"
                    className="flex items-center gap-3 rounded-md bg-[var(--color-ivory-100)] px-3 py-2.5 hover:bg-[var(--color-blush-100)] transition-colors"
                  >
                    <MapPin className="h-4 w-4 text-[var(--color-blush-500)]" />
                    <div className="flex-1">
                      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Locatie</div>
                      <div className="font-medium">{lead.city || '—'}</div>
                    </div>
                    <ExternalLink className="h-3.5 w-3.5 text-[var(--color-stone-400)]" />
                  </a>
                )}

                {/* Website */}
                {lead.domain && (
                  <a
                    href={`https://${lead.domain}`}
                    target="_blank"
                    rel="noopener"
                    className="flex items-center gap-3 rounded-md bg-[var(--color-ivory-100)] px-3 py-2.5 hover:bg-[var(--color-blush-100)] transition-colors"
                  >
                    <Globe className="h-4 w-4 text-[var(--color-blush-500)]" />
                    <div className="flex-1">
                      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Website</div>
                      <div className="font-medium">{lead.domain}</div>
                    </div>
                    <ExternalLink className="h-3.5 w-3.5 text-[var(--color-stone-400)]" />
                  </a>
                )}

                {lead.created_at && (
                  <Row icon={Clock} label="Ontdekt" value={fmtRelative(lead.created_at)} />
                )}
              </div>
            </Card>

            {/* NEW: Business profile card */}
            <Card className="p-5">
              <h3 className="font-display text-base font-semibold mb-4 flex items-center gap-2">
                <Store className="h-4 w-4 text-[var(--color-blush-500)]" /> Bedrijfsprofiel
              </h3>
              <div className="grid grid-cols-2 gap-3 text-sm">
                <DP label="Google categorie" value={lead.google_category || '—'} />
                <DP label="Bedrijfsgrootte" value={lead.company_size_estimate || '—'} />
                <DP label="KvK nummer" value={lead.kvk_number || '—'} />
                <DP label="SBI code" value={lead.kvk_sbi_code || '—'} />
                <DP label="CMS" value={lead.cms_detected || '—'} />
                <DP label="Enrichment versie" value={lead.enrichment_version != null ? `v${lead.enrichment_version}` : '—'} />
              </div>

              {lead.company_summary && (
                <div className="mt-4 pt-4 border-t border-[var(--color-border)]">
                  <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1.5">Samenvatting</div>
                  <p className="text-sm text-[var(--color-stone-700)] leading-relaxed">
                    {lead.company_summary}
                  </p>
                </div>
              )}

              <div className="mt-4 pt-4 border-t border-[var(--color-border)]">
                <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-2">Signalen</div>
                <div className="flex flex-wrap gap-1.5">
                  {lead.has_online_booking && <Chip icon={Calendar} label="Online booking" tone="success" />}
                  {lead.has_whatsapp && <Chip icon={Hash} label="WhatsApp" tone="success" />}
                  {lead.has_instagram && <Chip icon={Camera} label="Instagram" tone="accent" />}
                  {lead.meta_ads_active && <Chip icon={Megaphone} label={`Meta Ads ${lead.ad_focus ? `· ${lead.ad_focus}` : ''}`} tone="info" />}
                  {lead.booking_system && <Chip icon={Calendar} label={`Booking: ${lead.booking_system}`} tone="neutral" />}
                  {!lead.has_online_booking && !lead.has_whatsapp && !lead.has_instagram && !lead.meta_ads_active && !lead.booking_system && (
                    <span className="text-xs text-[var(--color-stone-400)] italic">Geen signalen gedetecteerd — of niet gedraaid</span>
                  )}
                </div>
              </div>
            </Card>

            <Card className="p-5">
              <h3 className="font-display text-base font-semibold mb-3">Warmr v1.0 datapunten</h3>
              <div className="grid grid-cols-2 gap-3 text-sm">
                <DP label="Website leeftijd" value={lead.website_age_years != null ? `${lead.website_age_years} jaar` : '—'} />
                <DP label="Booking systeem" value={lead.booking_system || '—'} />
                <DP label="Meta Ads actief" value={lead.meta_ads_active == null ? '—' : lead.meta_ads_active ? 'Ja' : 'Nee'} />
                <DP label="Ad focus" value={lead.ad_focus || '—'} />
                <DP
                  label="Treatment focus"
                  value={lead.treatment_focus && lead.treatment_focus.length > 0 ? lead.treatment_focus.join(', ') : '—'}
                />
                <DP label="Laatste review" value={lead.latest_review_date ? fmtRelative(lead.latest_review_date) : '—'} />
              </div>
            </Card>

            {(lead.personalization_hooks?.length || lead.personalization_observations?.length || lead.review_best_quote) && (
              <Card className="p-5 lg:col-span-2">
                <h3 className="font-display text-base font-semibold mb-3">Personalisatie</h3>
                {lead.personalized_opener && (
                  <div className="italic text-sm text-[var(--color-stone-700)] border-l-2 border-[var(--color-blush-500)] pl-3 mb-3">
                    "{lead.personalized_opener}"
                  </div>
                )}
                {lead.personalization_hooks?.length && (
                  <div className="mb-2">
                    <div className="text-xs uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">Hooks</div>
                    <ul className="text-sm list-disc list-inside space-y-0.5">
                      {lead.personalization_hooks.map((h, i) => <li key={i}>{h}</li>)}
                    </ul>
                  </div>
                )}
                {lead.review_best_quote && (
                  <div>
                    <div className="text-xs uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">Klantquote</div>
                    <div className="text-sm italic text-[var(--color-stone-600)]">"{lead.review_best_quote}"</div>
                  </div>
                )}
              </Card>
            )}
          </div>
        </TabsContent>

        <TabsContent value="website" className="mt-5">
          <Card className="p-5">
            <h3 className="font-display text-base font-semibold mb-3">Website intelligence</h3>
            {lead.website_score == null ? (
              <p className="text-sm text-[var(--color-stone-500)]">
                Deze lead is nog niet geanalyseerd. Draai enrichment om website-intelligence te genereren.
              </p>
            ) : (
              <div className="flex items-center gap-5">
                <div className="flex flex-col items-center">
                  <div className="font-display text-5xl font-semibold" style={{ color: scoreColor(lead.website_score) }}>
                    {lead.website_score}
                  </div>
                  <div className="text-xs text-[var(--color-stone-500)]">/ 100</div>
                </div>
                <div className="flex-1">
                  <div className="mb-2 flex items-center gap-2">
                    <Badge variant="accent">{prio.label}</Badge>
                  </div>
                  <div className="h-2 rounded-full bg-[var(--color-ivory-200)] overflow-hidden max-w-sm">
                    <div
                      className="h-full rounded-full"
                      style={{ width: `${lead.website_score}%`, background: scoreColor(lead.website_score) }}
                    />
                  </div>
                </div>
              </div>
            )}
          </Card>
        </TabsContent>

        <TabsContent value="contacts" className="mt-5">
          <Card>
            {(contacts?.contacts || []).length === 0 ? (
              <div className="p-8 text-center text-sm text-[var(--color-stone-500)]">
                Nog geen contactpersonen gevonden. Contact discovery wordt tijdens enrichment gedraaid.
              </div>
            ) : (
              <div className="divide-y divide-[var(--color-ivory-200)]">
                {(contacts?.contacts || []).map((c) => (
                  <div key={c.id} className="p-4">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium">{c.name || '—'}</span>
                      {c.is_primary && <Badge variant="accent">primary</Badge>}
                      {c.confidence != null && (
                        <Badge variant="neutral">{Math.round(c.confidence * 100)}% confidence</Badge>
                      )}
                    </div>
                    <div className="text-xs text-[var(--color-stone-500)] mt-1">
                      {c.role && <span>{c.role} · </span>}
                      {c.email && <span>{c.email} · </span>}
                      {c.source && <span>via {c.source}</span>}
                    </div>
                    {c.why_chosen && <div className="text-xs text-[var(--color-stone-600)] italic mt-1">{c.why_chosen}</div>}
                  </div>
                ))}
              </div>
            )}
          </Card>
        </TabsContent>

        <TabsContent value="thread" className="mt-5">
          <ThreadView leadId={id!} />
        </TabsContent>

        <TabsContent value="timeline" className="mt-5">
          <Card>
            {(tl?.events || []).length === 0 ? (
              <div className="p-8 text-center text-sm text-[var(--color-stone-500)]">
                Geen tijdlijn events.
              </div>
            ) : (
              <div className="divide-y divide-[var(--color-ivory-200)]">
                {(tl?.events || []).map((e) => (
                  <div key={e.id} className="p-4">
                    <div className="flex items-center gap-2">
                      <span className="w-2 h-2 rounded-full bg-[var(--color-blush-500)]" />
                      <span className="text-sm font-medium">{e.title}</span>
                      <span className="text-xs text-[var(--color-stone-400)] ml-auto">{fmtRelative(e.created_at)}</span>
                    </div>
                    {e.body && <div className="text-sm text-[var(--color-stone-500)] mt-1 pl-4">{e.body}</div>}
                  </div>
                ))}
              </div>
            )}
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}

function QuickStat({ label, value, sub, accent, color }: { label: string; value: string | number; sub?: string; accent?: 'primary'; color?: string }) {
  return (
    <Card className="p-4">
      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">{label}</div>
      <div
        className="font-display text-2xl font-semibold"
        style={color ? { color } : accent === 'primary' ? { color: 'var(--color-blush-500)' } : undefined}
      >
        {value}
      </div>
      {sub && <div className="text-xs text-[var(--color-stone-500)] mt-0.5">{sub}</div>}
    </Card>
  );
}

function Row({ icon: Icon, label, value, sub }: { icon: typeof Mail; label: string; value: string; sub?: string }) {
  return (
    <div className="flex items-center gap-2.5">
      <Icon className="h-3.5 w-3.5 text-[var(--color-stone-400)] shrink-0" />
      <span className="text-[var(--color-stone-500)] w-28 shrink-0 text-xs uppercase tracking-wider font-semibold">{label}</span>
      <span className="truncate">{value}</span>
      {sub && <Badge variant="neutral">{sub}</Badge>}
    </div>
  );
}

function DP({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">{label}</div>
      <div className="text-sm mt-0.5">{value}</div>
    </div>
  );
}

function Chip({ icon: Icon, label, tone }: { icon: typeof Phone; label: string; tone: 'success' | 'accent' | 'info' | 'neutral' }) {
  const toneCls: Record<string, string> = {
    success: 'bg-[var(--color-success-bg)] text-[var(--color-success)]',
    accent: 'bg-pink-100 text-pink-700',
    info: 'bg-[var(--color-info-bg)] text-[var(--color-info)]',
    neutral: 'bg-[var(--color-ivory-200)] text-[var(--color-stone-600)]',
  };
  return (
    <span className={`inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-medium ${toneCls[tone]}`}>
      <Icon className="h-3 w-3" /> {label}
    </span>
  );
}

interface ThreadItem {
  direction: 'sent' | 'received';
  timestamp: string | null;
  subject: string;
  body: string;
  step_index: number | null;
  campaign_id: string | null;
  classification: string | null;
  classifier_summary: string | null;
  from_email: string | null;
}

interface ThreadResponse {
  lead_id: string;
  thread: ThreadItem[];
  counts: { total: number; sent: number; received: number };
}

function ThreadView({ leadId }: { leadId: string }) {
  const { data, isLoading } = useQuery({
    queryKey: ['lead-thread', leadId],
    queryFn: () => api.get<ThreadResponse>(`/leads/${leadId}/thread`).catch(() => ({ lead_id: leadId, thread: [], counts: { total: 0, sent: 0, received: 0 } })),
    enabled: !!leadId,
  });

  if (isLoading) {
    return <Card className="p-6"><div className="skeleton h-32" /></Card>;
  }

  const items = data?.thread || [];

  if (items.length === 0) {
    return (
      <Card className="p-12 text-center">
        <Mail className="h-8 w-8 mx-auto mb-3 text-[var(--color-stone-300)]" />
        <h3 className="font-display text-lg font-semibold mb-1">Geen email-thread</h3>
        <p className="text-sm text-[var(--color-stone-500)] max-w-md mx-auto">
          Zodra er mails verstuurd zijn (via campagne) of replies ontvangen,
          verschijnen ze hier chronologisch.
        </p>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card className="p-3 flex items-center justify-between">
        <div className="text-xs text-[var(--color-stone-600)]">
          <strong>{data?.counts.total}</strong> berichten —{' '}
          <span className="text-[var(--color-stone-500)]">
            {data?.counts.sent} verstuurd · {data?.counts.received} ontvangen
          </span>
        </div>
        <button
          disabled
          title="Antwoord versturen vanuit Heatr is uitgeschakeld. Gebruik je Warmr-inbox of mailclient."
          className="inline-flex items-center gap-1.5 text-xs px-3 h-8 rounded-md bg-[var(--color-ivory-100)] text-[var(--color-stone-400)] border border-[var(--color-border)] cursor-not-allowed"
        >
          <Mail className="h-3 w-3" /> Antwoord (use Warmr UI)
        </button>
      </Card>

      {items.map((item, idx) => (
        <ThreadMessage key={idx} item={item} />
      ))}
    </div>
  );
}

function ThreadMessage({ item }: { item: ThreadItem }) {
  const isSent = item.direction === 'sent';
  return (
    <div className={`flex ${isSent ? 'justify-start' : 'justify-end'}`}>
      <Card
        className={`p-4 max-w-[85%] ${
          isSent
            ? 'bg-[var(--color-ivory-100)] border-[var(--color-ivory-200)]'
            : 'bg-blue-50 border-blue-200'
        }`}
      >
        <div className="flex items-center justify-between gap-3 mb-2 flex-wrap">
          <div className="flex items-center gap-2 text-[10px] uppercase tracking-wider font-semibold">
            {isSent ? (
              <span className="text-[var(--color-stone-600)]">
                ↗ Verstuurd
                {item.step_index != null && <span className="ml-1.5 text-[var(--color-stone-400)] normal-case">stap {item.step_index + 1}</span>}
              </span>
            ) : (
              <span className="text-blue-700">
                ↙ Ontvangen
                {item.classification && (
                  <span className="ml-1.5 px-1.5 py-0.5 rounded bg-blue-100 text-blue-800 normal-case">
                    {item.classification}
                  </span>
                )}
              </span>
            )}
          </div>
          <div className="text-[10px] text-[var(--color-stone-500)] font-mono">
            {item.timestamp ? new Date(item.timestamp).toLocaleString('nl-NL', { dateStyle: 'short', timeStyle: 'short' }) : '?'}
          </div>
        </div>

        {item.from_email && (
          <div className="text-[10px] text-[var(--color-stone-500)] mb-1 font-mono truncate">
            van: {item.from_email}
          </div>
        )}

        <div className="text-sm font-semibold mb-2 text-[var(--color-stone-800)]">
          {item.subject}
        </div>

        {item.body ? (
          <pre className="whitespace-pre-wrap text-sm leading-relaxed text-[var(--color-stone-800)] font-sans">
{item.body}
          </pre>
        ) : (
          <div className="text-xs text-[var(--color-stone-400)] italic">(geen body opgeslagen)</div>
        )}

        {item.classifier_summary && (
          <div className="mt-3 pt-3 border-t border-blue-200 text-xs text-[var(--color-stone-600)]">
            <span className="font-semibold">Claude:</span> {item.classifier_summary}
          </div>
        )}
      </Card>
    </div>
  );
}

// ---------------------------------------------------------------------------
// "Waarom deze lead?" — Capability 2: binnen 10 seconden beoordelen of een
// lead klopt. Combineert bestaande lead-data (icp, archetype, hooks,
// why_chosen, review-signalen) met de launch-readiness uit Capability 1.
// ---------------------------------------------------------------------------

interface ReadinessCheck { key: string; ok: boolean; severity: string; detail: string; }
interface Readiness { verdict: 'ready' | 'blocked' | 'needs_review'; checks: ReadinessCheck[]; blockers: string[]; reviews: string[]; }

const VERDICT_STYLE: Record<Readiness['verdict'], { label: string; cls: string }> = {
  ready: { label: 'Klaar voor launch', cls: 'bg-[var(--color-success-bg)] text-[var(--color-success)] border-[var(--color-success)]' },
  needs_review: { label: 'Review nodig', cls: 'bg-[var(--color-warning-bg)] text-[var(--color-warning)] border-[var(--color-warning)]' },
  blocked: { label: 'Geblokkeerd', cls: 'bg-[var(--color-danger-bg)] text-[var(--color-danger)] border-[var(--color-danger)]' },
};

function WhyThisLead({ lead }: { lead: Lead }) {
  const { data: readiness } = useQuery({
    queryKey: ['lead-readiness', lead.id],
    queryFn: () => api.get<Readiness>(`/leads/${lead.id}/launch-readiness`),
  });

  const icpPct = lead.icp_match != null ? Math.round(lead.icp_match * 100) : null;
  const hooks = lead.personalization_hooks || [];
  const pains = lead.review_pain_points || [];
  const verdict = readiness ? VERDICT_STYLE[readiness.verdict] : null;

  return (
    <Card className="p-5 mb-5">
      <div className="flex items-start justify-between gap-4 mb-4">
        <h3 className="font-display text-base font-semibold">Waarom deze lead?</h3>
        {verdict && (
          <span className={`rounded-full border px-3 py-1 text-xs font-semibold ${verdict.cls}`}>
            {verdict.label}
          </span>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5 text-sm">
        {/* Links: waarom gekozen */}
        <div className="space-y-3">
          {icpPct != null && (
            <div>
              <span className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">ICP-match</span>
              <div className="flex items-center gap-2 mt-1">
                <div className="h-2 flex-1 rounded-full bg-[var(--color-ivory-200)] overflow-hidden">
                  <div className="h-full rounded-full bg-[var(--color-blush-500)]" style={{ width: `${icpPct}%` }} />
                </div>
                <span className="tabular-nums font-medium">{icpPct}%</span>
              </div>
            </div>
          )}
          {lead.archetype && (
            <div>
              <span className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Archetype</span>
              <div className="mt-0.5 font-medium">{lead.archetype}</div>
              {lead.archetype_reason && <div className="text-xs text-[var(--color-stone-500)] mt-0.5">{lead.archetype_reason}</div>}
            </div>
          )}
          {lead.contact_why_chosen && (
            <div>
              <span className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Contactpersoon-keuze</span>
              <div className="text-xs text-[var(--color-stone-600)] mt-0.5">{lead.contact_why_chosen}</div>
            </div>
          )}
          {hooks.length > 0 && (
            <div>
              <span className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Personalisatie-signalen in de mail</span>
              <ul className="mt-1 space-y-1">
                {hooks.slice(0, 3).map((h, i) => (
                  <li key={i} className="text-xs text-[var(--color-stone-600)] flex gap-1.5"><span className="text-[var(--color-blush-500)]">•</span>{h}</li>
                ))}
              </ul>
            </div>
          )}
          {pains.length > 0 && (
            <div>
              <span className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Pijnpunten uit reviews</span>
              <div className="mt-1 flex flex-wrap gap-1.5">
                {pains.map((p, i) => (
                  <span key={i} className="rounded bg-[var(--color-warning-bg)] px-1.5 py-0.5 text-[10px] text-[var(--color-warning)]">{p}</span>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Rechts: launch-readiness checks */}
        <div>
          <span className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">Launch-readiness</span>
          {!readiness ? (
            <div className="skeleton h-24 mt-2 rounded" />
          ) : (
            <ul className="mt-2 space-y-1.5">
              {readiness.checks.map((c) => (
                <li key={c.key} className="flex items-start gap-2 text-xs">
                  <span className={
                    c.ok
                      ? 'text-[var(--color-success)]'
                      : c.severity === 'block'
                        ? 'text-[var(--color-danger)]'
                        : 'text-[var(--color-warning)]'
                  }>
                    {c.ok ? '✓' : c.severity === 'block' ? '✕' : '⚠'}
                  </span>
                  <span className={c.ok ? 'text-[var(--color-stone-500)]' : 'text-[var(--color-stone-700)] font-medium'}>
                    {c.detail}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </Card>
  );
}
