import { useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { ArrowLeft, ExternalLink, Mail, Phone, Building2, MapPin, Calendar, Globe, Camera, Hash, Store, Megaphone, Clock, FlaskConical, Download, Trash2 } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtInt, fmtRelative, priorityFromScore, scoreColor } from '@/lib/format';
import type { Lead } from '@/lib/types';
import { SECTOR_LABEL } from '@/lib/types';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { GesprekView } from '@/components/calls/GesprekView';

interface Contact { id: string; name?: string; email?: string; role?: string; source?: string; is_primary?: boolean; confidence?: number; why_chosen?: string; }
interface TimelineEvent { id: string; event_type: string; title: string; body?: string; created_at: string; }
interface ReceptieMail { step: number; subject: string | null; body: string | null; sendable: boolean; block_reason: string | null; skipped: boolean; }
interface ReceptiePreview {
  hook_code: string | null; second_hook: string | null;
  axes: Record<string, string | null>;
  q4_gated: boolean | null; form_present: boolean | null;
  detected_at: string | null; evidence: string[] | null;
  mails: ReceptieMail[];
}
const OPP_LABEL: Record<string, string> = {
  website_rebuild: 'Website', conversie_optimalisatie: 'Conversie', chatbot: 'Chatbot', ai_audit: 'AI Audit',
};
interface WebsiteIntel {
  total_score?: number | null; technical_score?: number | null; visual_score?: number | null;
  conversion_score?: number | null; sector_score?: number | null;
  score_denominator?: number | null; visual_included?: boolean | null; priority?: string | null;
  opportunity_types?: string[] | null; opportunity_reasons?: Record<string, string> | null;
  screenshot_desktop_url?: string | null; screenshot_mobile_url?: string | null;
  review_status?: string | null;
  score_vs_market?: number | null;
  market_benchmark?: { scope: string; city?: string | null; sector: string; peer_count: number; market_avg: number; score_vs_market: number } | null;
}
interface AuditCategory { behaald: number; max: number; label: string; }
interface AuditFinding {
  check_id: string; categorie: string; status: string;
  bewijs?: string | null; severity?: string | null; uitleg?: string | null;
}
interface AuditReport {
  version?: number; tier?: number;
  score_total?: number | null; score_normalized?: number | null; score_capped_by?: string | null;
  categories?: Record<string, AuditCategory> | null;
  findings?: AuditFinding[] | null;
  benchmark?: Record<string, unknown> | null;
  created_at?: string | null;
}
const _findingRank = (s: string) => (s === 'fail' ? 0 : s === 'not_measurable' ? 1 : 2);

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

  // AVG art.17 — recht op vergetelheid (anonimiseert PII + stopt sequences; onomkeerbaar).
  const forgetMut = useMutation({
    mutationFn: () => api.post(`/gdpr/forget/${id}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['lead', id] }),
  });
  const onForget = (companyName: string | null) => {
    if (!window.confirm(
      `AVG art.17 — "${companyName || 'deze lead'}" vergeten? Dit anonimiseert alle PII en stopt ` +
      `alle sequences. Onomkeerbaar.`
    )) return;
    forgetMut.mutate();
  };
  // AVG art.15/20 — inzage / dataportabiliteit: alle opgeslagen data als JSON-download.
  const exportGdpr = async () => {
    const data = await api.get<Record<string, unknown>>(`/gdpr/export/${id}`);
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `gdpr-export-${id}.json`; a.click();
    URL.revokeObjectURL(url);
  };

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
          <button
            onClick={exportGdpr}
            title="AVG art.15/20 — exporteer alle opgeslagen data van deze lead (JSON)"
            className="inline-flex items-center gap-2 h-10 px-3 rounded-md border border-[var(--color-border)] text-sm hover:bg-[var(--color-ivory-100)]"
          >
            <Download className="h-3.5 w-3.5" /> Exporteer (AVG)
          </button>
          <button
            onClick={() => onForget(lead.company_name)}
            disabled={forgetMut.isPending}
            title="AVG art.17 — recht op vergetelheid; anonimiseert PII + stopt sequences (onomkeerbaar)"
            className="inline-flex items-center gap-2 h-10 px-3 rounded-md border border-[var(--color-danger)] text-[var(--color-danger)] text-sm hover:bg-[var(--color-danger-bg)] disabled:opacity-50"
          >
            <Trash2 className="h-3.5 w-3.5" /> {forgetMut.isPending ? 'Vergeten…' : 'Vergeten (AVG)'}
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
          <TabsTrigger value="audit">Audit</TabsTrigger>
          <TabsTrigger value="receptie">Receptie</TabsTrigger>
          <TabsTrigger value="contacts">Contacts ({contacts?.contacts?.length || 0})</TabsTrigger>
          <TabsTrigger value="thread">Thread</TabsTrigger>
          <TabsTrigger value="gesprek">Gesprek</TabsTrigger>
          <TabsTrigger value="run">Run</TabsTrigger>
          <TabsTrigger value="timeline">Timeline</TabsTrigger>
        </TabsList>

        <TabsContent value="overview" className="mt-5">
          <RecontactBlock leadId={id!} />
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
          <WebsiteView leadId={id!} />
        </TabsContent>

        <TabsContent value="audit" className="mt-5">
          <AuditView leadId={id!} />
        </TabsContent>

        <TabsContent value="receptie" className="mt-5">
          <ReceptieView leadId={id!} />
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

        <TabsContent value="gesprek" className="mt-5">
          <GesprekView leadId={id!} />
        </TabsContent>

        <TabsContent value="run" className="mt-5">
          <RunView leadId={id!} />
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

function RecontactBlock({ leadId }: { leadId: string }) {
  const { data } = useQuery({
    queryKey: ['lead-recontact', leadId],
    queryFn: () => api.get<{ has_signal: boolean; signals: { type: string; detail: string; weight: number }[]; score: number; suggested_opener_angle: string }>(`/leads/${leadId}/recontact-signals`).catch(() => null),
  });
  if (!data || !data.has_signal) return null;
  return (
    <Card className="p-4 mb-5 border-[var(--color-warning)]">
      <div className="flex items-center gap-2 mb-2">
        <h3 className="font-display text-sm font-semibold">Recontact-signaal</h3>
        <Badge variant="warning">score {Math.round(data.score * 100)}</Badge>
      </div>
      <ul className="text-sm text-[var(--color-stone-700)] space-y-0.5">
        {data.signals.map((s, i) => <li key={i}>· {s.detail}</li>)}
      </ul>
      {data.suggested_opener_angle && (
        <div className="text-xs text-[var(--color-blush-500)] italic mt-2">Suggestie: "{data.suggested_opener_angle}"</div>
      )}
    </Card>
  );
}

function AuditView({ leadId }: { leadId: string }) {
  const qc = useQueryClient();
  const { data: report, isLoading } = useQuery({
    queryKey: ['lead-audit', leadId],
    queryFn: () => api.get<AuditReport>(`/leads/${leadId}/audit`).catch(() => null),
  });
  const runMut = useMutation({
    mutationFn: () => api.post(`/leads/${leadId}/audit`, {}),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['lead-audit', leadId] }),
  });

  if (isLoading) return <div className="skeleton h-64" />;
  const hasReport = report && report.score_normalized != null;

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h3 className="font-display text-base font-semibold">Audit (Stap 2)</h3>
        <button
          onClick={() => runMut.mutate()}
          disabled={runMut.isPending}
          className="inline-flex items-center gap-2 h-9 px-3 rounded-md border border-[var(--color-border)] text-sm hover:bg-[var(--color-ivory-100)] disabled:opacity-50"
        >
          {runMut.isPending ? 'Draait…' : 'Draai tier-1 audit'}
        </button>
      </div>

      {!hasReport ? (
        <Card className="p-8 text-center text-sm text-[var(--color-stone-500)]">
          Nog geen audit-rapport voor deze lead. Klik "Draai tier-1 audit".
        </Card>
      ) : (
        <>
          <Card className="p-5">
            <div className="flex items-center gap-6 flex-wrap">
              <div className="flex flex-col items-center">
                <div className="font-display text-5xl font-semibold" style={{ color: scoreColor(report.score_normalized!) }}>{report.score_normalized}</div>
                <div className="text-xs text-[var(--color-stone-500)]">/ 100</div>
              </div>
              <div className="flex-1 min-w-[200px] text-xs text-[var(--color-stone-500)] space-y-1">
                <div>versie {report.version} · tier {report.tier}{report.created_at ? ` · ${fmtRelative(report.created_at)}` : ''}</div>
                {report.score_capped_by && <div className="text-[var(--color-danger)]">Knock-out cap: {report.score_capped_by}</div>}
              </div>
            </div>
          </Card>

          {report.categories && Object.keys(report.categories).length > 0 && (
            <Card className="p-5">
              <h4 className="font-display text-sm font-semibold mb-3">Categorieën</h4>
              <div className="space-y-2.5">
                {Object.entries(report.categories).map(([key, c]) => {
                  const pct = c.max > 0 ? Math.round((c.behaald / c.max) * 100) : 0;
                  return (
                    <div key={key}>
                      <div className="flex justify-between text-xs mb-0.5">
                        <span className="text-[var(--color-stone-700)]">{c.label}</span>
                        <span className="tabular-nums text-[var(--color-stone-500)]">{c.behaald}/{c.max}</span>
                      </div>
                      <div className="h-1.5 rounded-full bg-[var(--color-ivory-200)] overflow-hidden">
                        <div className="h-full rounded-full" style={{ width: `${pct}%`, background: scoreColor(pct) }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            </Card>
          )}

          {report.findings && report.findings.length > 0 && (
            <Card className="p-5">
              <h4 className="font-display text-sm font-semibold mb-3">Bevindingen</h4>
              <div className="space-y-2">
                {[...report.findings]
                  .sort((a, b) => _findingRank(a.status) - _findingRank(b.status))
                  .map((f) => (
                    <div key={f.check_id} className="flex items-start gap-2 text-sm">
                      <Badge variant={f.status === 'pass' ? 'success' : f.status === 'fail' ? 'danger' : 'neutral'}>{f.status}</Badge>
                      <div className="min-w-0">
                        <div className="text-[var(--color-stone-700)]">{f.uitleg || f.check_id}</div>
                        {f.bewijs && <div className="text-xs text-[var(--color-stone-500)]">{f.bewijs}</div>}
                      </div>
                    </div>
                  ))}
              </div>
            </Card>
          )}

          {report.benchmark && (
            <Card className="p-4 text-xs text-[var(--color-stone-600)]">
              <span className="font-semibold">Benchmark: </span>
              {String(report.benchmark.rank_sentence || report.benchmark.niche_label || 'beschikbaar')}
            </Card>
          )}
        </>
      )}
    </div>
  );
}

function WebsiteView({ leadId }: { leadId: string }) {
  const qc = useQueryClient();
  const { data: wi, isLoading } = useQuery({
    queryKey: ['lead-website', leadId],
    queryFn: () => api.get<WebsiteIntel>(`/leads/${leadId}/website`).catch(() => null),
  });
  const reviewMut = useMutation({
    mutationFn: (status: string) => api.patch(`/leads/${leadId}/website-review`, { status }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['lead-website', leadId] }),
  });

  if (isLoading) return <div className="skeleton h-64" />;
  if (!wi || wi.total_score == null) {
    return (
      <Card className="p-8 text-center text-sm text-[var(--color-stone-500)]">
        Deze lead is nog niet geanalyseerd. Draai de website-analyse om website-intelligence te genereren.
      </Card>
    );
  }

  const layers = [
    { label: 'Technisch', v: wi.technical_score },
    { label: 'Visueel', v: wi.visual_score },
    { label: 'Conversie', v: wi.conversion_score },
    { label: 'Sector', v: wi.sector_score },
  ];
  const reasons = Object.entries(wi.opportunity_reasons || {});

  return (
    <div className="space-y-5">
      {/* Review-triage — schrijft review_status (PATCH). Vereist migratie 045. */}
      <Card className="p-4">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mr-1">Review-status</span>
          {([['ok', 'OK'], ['opportunity', 'Kans'], ['urgent', 'Urgent']] as const).map(([val, label]) => (
            <button
              key={val}
              onClick={() => reviewMut.mutate(val)}
              disabled={reviewMut.isPending}
              className={`px-3 py-1 text-xs rounded-md border font-medium transition-colors disabled:opacity-50 ${
                wi.review_status === val
                  ? 'bg-[var(--color-blush-100)] border-[var(--color-blush-500)] text-[var(--color-blush-700)]'
                  : 'bg-white border-[var(--color-border)] text-[var(--color-stone-600)] hover:bg-[var(--color-ivory-100)]'
              }`}
            >
              {label}
            </button>
          ))}
          {!wi.review_status && <span className="text-xs text-[var(--color-stone-400)]">nog niet getrieerd</span>}
        </div>
      </Card>

      <Card className="p-5">
        <div className="flex items-start gap-6 flex-wrap">
          <div className="flex flex-col items-center">
            <div className="font-display text-5xl font-semibold" style={{ color: scoreColor(wi.total_score) }}>{wi.total_score}</div>
            <div className="text-xs text-[var(--color-stone-500)]">/ {wi.score_denominator ?? 100}</div>
            {wi.priority && <Badge variant="accent" className="mt-2">{wi.priority}</Badge>}
            {wi.market_benchmark && (
              <div
                className="mt-2 text-xs text-center"
                title={`Gemiddelde van ${wi.market_benchmark.peer_count} geanalyseerde ${wi.market_benchmark.scope === 'stad' ? `sites in ${wi.market_benchmark.city}` : 'sites sector-breed'}: ${wi.market_benchmark.market_avg}`}
              >
                <span className={wi.market_benchmark.score_vs_market < 0 ? 'text-[var(--color-danger)] font-semibold' : 'text-[var(--color-success)] font-semibold'}>
                  {wi.market_benchmark.score_vs_market > 0 ? '+' : ''}{wi.market_benchmark.score_vs_market}
                </span>{' '}
                <span className="text-[var(--color-stone-500)]">
                  vs {wi.market_benchmark.scope === 'stad' ? wi.market_benchmark.city : 'sector'}-gemiddelde ({wi.market_benchmark.market_avg}, n={wi.market_benchmark.peer_count})
                </span>
              </div>
            )}
          </div>
          <div className="flex-1 min-w-[240px] grid grid-cols-2 sm:grid-cols-4 gap-3">
            {layers.map((l) => (
              <div key={l.label} className="rounded-md border border-[var(--color-border)] p-3 text-center">
                <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">{l.label}</div>
                <div className="font-display text-2xl font-semibold tabular-nums mt-1">{l.v ?? '—'}</div>
              </div>
            ))}
          </div>
        </div>
        {wi.visual_included === false && (
          <div className="text-xs text-[var(--color-stone-400)] mt-3">
            Visuele laag niet meegewogen (geen Vision-analyse) — score over {wi.score_denominator ?? 70}.
          </div>
        )}
      </Card>

      {(wi.opportunity_types?.length || reasons.length) ? (
        <Card className="p-5">
          <h4 className="font-display text-sm font-semibold mb-2">Kansen</h4>
          <div className="flex flex-wrap gap-1.5 mb-3">
            {(wi.opportunity_types || []).map((t) => <Badge key={t} variant="neutral">{OPP_LABEL[t] || t}</Badge>)}
          </div>
          {reasons.length > 0 && (
            <ul className="text-sm text-[var(--color-stone-700)] space-y-1">
              {reasons.map(([t, r]) => (
                <li key={t}><span className="text-[var(--color-stone-400)] text-[10px] uppercase tracking-wider mr-1.5">{OPP_LABEL[t] || t}</span>{r}</li>
              ))}
            </ul>
          )}
        </Card>
      ) : null}

      {(wi.screenshot_desktop_url || wi.screenshot_mobile_url) && (
        <Card className="p-5">
          <h4 className="font-display text-sm font-semibold mb-3">Screenshots</h4>
          <div className="flex gap-4 flex-wrap items-start">
            {wi.screenshot_desktop_url && (
              <a href={wi.screenshot_desktop_url} target="_blank" rel="noopener">
                <img src={wi.screenshot_desktop_url} alt="desktop" loading="lazy" className="w-80 rounded-md border border-[var(--color-border)]" />
                <div className="text-[10px] text-[var(--color-stone-500)] mt-1 text-center">Desktop</div>
              </a>
            )}
            {wi.screenshot_mobile_url && (
              <a href={wi.screenshot_mobile_url} target="_blank" rel="noopener">
                <img src={wi.screenshot_mobile_url} alt="mobiel" loading="lazy" className="w-40 rounded-md border border-[var(--color-border)]" />
                <div className="text-[10px] text-[var(--color-stone-500)] mt-1 text-center">Mobiel</div>
              </a>
            )}
          </div>
        </Card>
      )}

      <div className="text-xs text-[var(--color-stone-400)] px-1">
        Vision-analyse (typografie/kleur) wordt in productie niet berekend en is daarom niet zichtbaar.
        De marktvergelijking hierboven komt uit onze eigen geanalyseerde voorraad (zelfde sector × stad).
      </div>
    </div>
  );
}

function ReceptieView({ leadId }: { leadId: string }) {
  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['lead-receptie', leadId],
    queryFn: () => api.get<ReceptiePreview>(`/leads/${leadId}/receptie`),
  });

  if (isLoading) return <div className="skeleton h-64" />;
  if (isError || !data) {
    return (
      <Card className="p-8 text-center border-[var(--color-danger)]">
        <p className="text-[var(--color-danger)] font-medium mb-2">Kon receptie-preview niet laden</p>
        <button onClick={() => refetch()} className="rounded bg-[var(--color-blush-500)] px-3 py-1.5 text-sm text-white">Opnieuw</button>
      </Card>
    );
  }
  if (!data.hook_code) {
    return (
      <Card className="p-8 text-center text-sm text-[var(--color-stone-500)]">
        Nog geen receptie-haak gedetecteerd voor deze lead. Draai de receptie-backfill
        (run_receptie_backfill.py) om de haak + mail-preview te genereren.
      </Card>
    );
  }

  const axisVariant = (s: string | null): 'success' | 'warning' | 'neutral' =>
    s === 'hit' ? 'success' : s === 'onbepaald' ? 'warning' : 'neutral';

  return (
    <div className="space-y-5">
      {/* Haak + axis-states */}
      <Card className="p-5">
        <div className="flex items-center gap-2 mb-3 flex-wrap">
          <h3 className="font-display text-base font-semibold">Receptie-haak</h3>
          <Badge variant="accent">{data.hook_code}</Badge>
          {data.second_hook && <Badge variant="neutral">mail 2: {data.second_hook}</Badge>}
        </div>
        <div className="flex items-center gap-1.5 flex-wrap text-xs">
          {(['q4', 'q7', 'q2', 'p1'] as const).map((ax) => (
            <Badge key={ax} variant={axisVariant(data.axes?.[ax] ?? null)}>
              {ax.toUpperCase()}: {data.axes?.[ax] ?? '—'}
            </Badge>
          ))}
          {data.q4_gated && <Badge variant="warning">Q4 doorgevallen</Badge>}
          <Badge variant="neutral">formulier: {data.form_present ? 'ja' : 'nee'}</Badge>
        </div>
        <div className="text-xs text-[var(--color-stone-500)] mt-2">
          {data.detected_at && <span>Gedetecteerd {fmtRelative(data.detected_at)}</span>}
          {data.evidence && data.evidence.length > 0 && <span> · {data.evidence.join(', ')}</span>}
        </div>
      </Card>

      {/* Gerenderde mails (dry, verstuurt niets) */}
      {data.mails.map((m) => (
        <Card key={m.step} className="p-5">
          <div className="flex items-center gap-2 mb-2 flex-wrap">
            <h4 className="font-display text-sm font-semibold">Mail {m.step}</h4>
            <Badge variant={m.sendable ? 'success' : 'danger'}>
              {m.sendable ? 'verzendbaar' : `geblokkeerd: ${m.block_reason}`}
            </Badge>
          </div>
          {m.subject && <div className="text-sm font-medium mb-2">{m.subject}</div>}
          <pre className="text-sm whitespace-pre-wrap font-sans text-[var(--color-stone-700)] leading-relaxed">{m.body}</pre>
        </Card>
      ))}
    </div>
  );
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

// ---------------------------------------------------------------------------
// Run-tab — Control Plane per lead (Sprint 2). Inspect: pipeline, jobs,
// campagne-records, side-effects (outbound-ledger), cost, gaps. Control:
// retry (idempotent), pause/resume (laag risico), force-next + restart
// (unsafe → expliciete bevestiging; veilig door de dispatcher-epoch-key).
// ---------------------------------------------------------------------------

interface RunPipelineStep { step: string; reached: boolean; at: string | null; detail: string; }
interface RunJob { id: string; status: string; current_step: number; retry_count: number; error_message: string | null; created_at: string; }
interface RunCampaignRecord {
  id: string; campaign_id: string; step_index: number; status: string;
  is_active: boolean; next_send_at: string | null; sent_at: string | null;
  block_reason: string | null; restart_epoch?: number; sequence_step_count: number;
}
interface RunSideEffect { id: string; idempotency_key: string; kind: string; status: string; actor: string; error: string | null; created_at: string; }
interface RunState {
  pipeline: RunPipelineStep[];
  current_step: string;
  jobs: RunJob[];
  campaign_history: RunCampaignRecord[];
  side_effects: RunSideEffect[];
  blocked_sends: { reason: string; created_at?: string; blocked_at?: string }[];
  cost: { total_eur: number; call_count: number; cache_hits: number; by_context: Record<string, number> };
  gaps: string[];
}

function RunView({ leadId }: { leadId: string }) {
  const qc = useQueryClient();
  const { data: run, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['lead-run-state', leadId],
    queryFn: () => api.get<RunState>(`/leads/${leadId}/run-state`),
  });

  const invalidate = () => qc.invalidateQueries({ queryKey: ['lead-run-state', leadId] });

  const retryEnrichment = useMutation({
    mutationFn: () => api.post(`/control/leads/${leadId}/retry-enrichment`),
    onSuccess: invalidate,
  });
  const recordAction = useMutation({
    mutationFn: ({ recordId, action, body }: { recordId: string; action: string; body?: unknown }) =>
      api.post(`/control/campaign-records/${recordId}/${action}`, body),
    onSuccess: invalidate,
  });

  if (isLoading) return <div className="skeleton h-64" />;
  if (isError || !run) {
    return (
      <Card className="p-8 text-center border-[var(--color-danger)]">
        <p className="text-[var(--color-danger)] font-medium mb-2">Kon run-state niet laden</p>
        <p className="text-xs text-[var(--color-stone-500)] mb-3">{error instanceof Error ? error.message : ''}</p>
        <button onClick={() => refetch()} className="rounded bg-[var(--color-blush-500)] px-3 py-1.5 text-sm text-white">Opnieuw</button>
      </Card>
    );
  }

  const forceNext = (recordId: string, stepIndex: number) => {
    if (!window.confirm(`Stap ${stepIndex} NU forceren? Dit overschrijft de wachttijd. De dispatcher voorkomt dubbele sends, maar de timing wijkt af van de sequence-planning.`)) return;
    recordAction.mutate({ recordId, action: 'force-next', body: { confirm: true } });
  };
  const restartFrom = (recordId: string, maxStep: number) => {
    const raw = window.prompt(`Herstart vanaf stap (0-${maxStep - 1})? LET OP: dit kan een al-verzonden stap OPNIEUW versturen naar de prospect.`);
    if (raw == null) return;
    const step = Number(raw);
    if (!Number.isInteger(step) || step < 0 || step >= maxStep) { window.alert('Ongeldige stap.'); return; }
    if (!window.confirm(`Bevestig: sequence herstarten vanaf stap ${step}. Een tweede mail voor deze stap is mogelijk.`)) return;
    recordAction.mutate({ recordId, action: 'restart', body: { confirm: true, step_index: step } });
  };

  return (
    <div className="space-y-5">
      {/* Pipeline */}
      <Card className="p-5">
        <h3 className="font-display text-base font-semibold mb-4">Pipeline</h3>
        <div className="flex flex-wrap items-center gap-2">
          {run.pipeline.map((p, i) => (
            <div key={p.step} className="flex items-center gap-2">
              {i > 0 && <span className="text-[var(--color-stone-300)]">→</span>}
              <div
                className={`rounded-md border px-3 py-1.5 text-xs ${
                  p.reached
                    ? 'border-[var(--color-success)] bg-[var(--color-success-bg)] text-[var(--color-success)]'
                    : 'border-[var(--color-border)] text-[var(--color-stone-400)]'
                }`}
                title={p.detail}
              >
                <div className="font-semibold">{p.step}</div>
                <div className="text-[10px]">{p.at ? fmtRelative(p.at) : p.detail}</div>
              </div>
            </div>
          ))}
        </div>
      </Card>

      {/* Campagne-records + control-acties */}
      <Card className="p-5">
        <h3 className="font-display text-base font-semibold mb-3">Sequences</h3>
        {run.campaign_history.length === 0 ? (
          <p className="text-sm text-[var(--color-stone-500)]">Geen campagne-koppelingen.</p>
        ) : (
          <div className="space-y-3">
            {run.campaign_history.map((r) => (
              <div key={r.id} className="rounded-md border border-[var(--color-border)] p-3 text-sm">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <Badge variant={r.status === 'pending' ? 'success' : r.status === 'paused' ? 'warning' : 'neutral'}>{r.status}</Badge>
                    <span className="ml-2 text-xs text-[var(--color-stone-600)]">
                      stap {r.step_index}/{r.sequence_step_count}
                      {(r.restart_epoch ?? 0) > 0 && <span className="ml-1 text-[var(--color-warning)]">· epoch {r.restart_epoch}</span>}
                      {r.next_send_at && <span className="ml-1">· volgende: {fmtRelative(r.next_send_at)}</span>}
                    </span>
                    {r.block_reason && <div className="text-xs text-[var(--color-danger)] mt-1">{r.block_reason}</div>}
                  </div>
                  <div className="flex gap-1.5">
                    {r.is_active && r.status !== 'paused' && (
                      <button onClick={() => recordAction.mutate({ recordId: r.id, action: 'pause' })} className="rounded border border-[var(--color-border)] px-2.5 py-1 text-xs hover:bg-[var(--color-ivory-100)]">Pauzeer</button>
                    )}
                    {r.status === 'paused' && (
                      <button onClick={() => recordAction.mutate({ recordId: r.id, action: 'resume' })} className="rounded border border-[var(--color-border)] px-2.5 py-1 text-xs hover:bg-[var(--color-ivory-100)]">Hervat</button>
                    )}
                    {r.is_active && (
                      <button onClick={() => forceNext(r.id, r.step_index)} className="rounded border border-[var(--color-warning)] px-2.5 py-1 text-xs text-[var(--color-warning)] hover:bg-[var(--color-warning-bg)]">Force next ⚠</button>
                    )}
                    <button onClick={() => restartFrom(r.id, r.sequence_step_count)} className="rounded border border-[var(--color-danger)] px-2.5 py-1 text-xs text-[var(--color-danger)] hover:bg-[var(--color-danger-bg)]">Restart ⚠</button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </Card>

      {/* Jobs + retry */}
      <Card className="p-5">
        <div className="flex items-center justify-between mb-3">
          <h3 className="font-display text-base font-semibold">Enrichment-jobs</h3>
          <button
            onClick={() => retryEnrichment.mutate()}
            disabled={retryEnrichment.isPending}
            className="rounded bg-[var(--color-blush-500)] px-3 py-1.5 text-xs text-white hover:opacity-90 disabled:opacity-50"
          >
            {retryEnrichment.isPending ? 'Bezig…' : 'Retry enrichment'}
          </button>
        </div>
        {run.jobs.length === 0 ? (
          <p className="text-sm text-[var(--color-stone-500)]">Geen jobs bekend.</p>
        ) : (
          <div className="space-y-1.5 text-xs">
            {run.jobs.map((j) => (
              <div key={j.id} className="flex flex-wrap items-center gap-2">
                <Badge variant={j.status === 'completed' ? 'success' : j.status === 'failed' ? 'danger' : 'warning'}>{j.status}</Badge>
                <span className="text-[var(--color-stone-500)]">{fmtRelative(j.created_at)} · retries: {j.retry_count}</span>
                {j.error_message && <span className="text-[var(--color-danger)] truncate max-w-md" title={j.error_message}>{j.error_message}</span>}
              </div>
            ))}
          </div>
        )}
      </Card>

      {/* Side-effects (outbound-ledger) */}
      <Card className="p-5">
        <h3 className="font-display text-base font-semibold mb-3">Side-effects (outbound-ledger)</h3>
        {run.side_effects.length === 0 ? (
          <p className="text-sm text-[var(--color-stone-500)]">Geen dispatcher-records voor deze lead (of migratie 020 nog niet gedraaid).</p>
        ) : (
          <div className="space-y-1.5 text-xs">
            {run.side_effects.map((s) => (
              <div key={s.id} className="flex flex-wrap items-center gap-2">
                <Badge variant={s.status === 'completed' ? 'success' : s.status === 'skipped_duplicate' ? 'warning' : 'danger'}>{s.status}</Badge>
                <span>{s.kind}</span>
                <span className="text-[var(--color-stone-500)]">{fmtRelative(s.created_at)} · {s.actor}</span>
                <span className="font-mono text-[var(--color-stone-400)] truncate max-w-xs" title={s.idempotency_key}>{s.idempotency_key}</span>
                {s.error && <span className="text-[var(--color-danger)]">{s.error}</span>}
              </div>
            ))}
          </div>
        )}
        {run.blocked_sends.length > 0 && (
          <div className="mt-3 pt-3 border-t border-[var(--color-border)]">
            <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1.5">Geblokkeerde sends (SendingGuard)</div>
            {run.blocked_sends.map((b, i) => (
              <div key={i} className="text-xs text-[var(--color-danger)]">{b.reason} <span className="text-[var(--color-stone-400)]">{fmtRelative(b.blocked_at || b.created_at || '')}</span></div>
            ))}
          </div>
        )}
      </Card>

      {/* Cost + gaps */}
      <Card className="p-5">
        <h3 className="font-display text-base font-semibold mb-3">
          Cost — €{run.cost.total_eur.toFixed(4)} <span className="text-xs font-normal text-[var(--color-stone-500)]">({run.cost.call_count} calls, {run.cost.cache_hits} cache-hits)</span>
        </h3>
        <div className="flex flex-wrap gap-1.5">
          {Object.entries(run.cost.by_context).map(([ctx, eur]) => (
            <span key={ctx} className="rounded bg-[var(--color-ivory-100)] px-2 py-1 text-xs tabular-nums">{ctx}: €{eur.toFixed(4)}</span>
          ))}
        </div>
        <div className="mt-4 pt-3 border-t border-[var(--color-border)]">
          {run.gaps.map((g, i) => (
            <p key={i} className="text-[11px] text-[var(--color-stone-400)] italic">◌ {g}</p>
          ))}
        </div>
      </Card>
    </div>
  );
}
