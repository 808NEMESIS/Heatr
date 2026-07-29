import { useMemo, useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { ArrowLeft, Eye, ShieldOff, CheckCircle2, AlertTriangle } from 'lucide-react';
import { Link } from 'react-router-dom';
import { api } from '@/lib/api';
import { fmtInt } from '@/lib/format';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import type { Lead, LeadsResponse } from '@/lib/types';
import { ARCHETYPE_LABELS } from '@/lib/types';

interface Inbox {
  id: string;
  email: string;
  status: string;
  daily_cap?: number | null;
  warmup_score?: number | null;
}

interface SequenceTemplate {
  id: string;
  name: string;
  version: string;
  segment: string;
  sector?: string;
  language: string;
  cadence_days: number[];
  primary_cta: string;
  constraints: string[];
  observation_blocks: string[];
  min_personalization_score?: number;
  step_count: number;
  preview_steps: { subject: string; delay_days: number; body_preview: string }[];
}

interface RenderedStep {
  subject: string;
  body: string;
  delay_days: number;
}

interface LeadPreview {
  lead_id: string;
  company_name: string | null;
  first_name: string | null;
  domain: string | null;
  observation_block: string | null;
  opener_source?: string;
  opener_fallback_reason?: string | null;
  personalization_score?: number;
  steps: RenderedStep[];
}

interface GateBucketEntry {
  lead_id: string;
  company_name: string | null;
  personalization_score: number;
}

interface PersonalizationGate {
  threshold: number | null;
  auto_count: number;
  review_count: number;
  skip_count: number;
  auto_sample: GateBucketEntry[];
  review_sample: GateBucketEntry[];
  skip_sample: GateBucketEntry[];
}

interface CompletenessBlocked {
  lead_id: string;
  company_name: string | null;
  missing_required: string[];
}

interface CompletenessCheck {
  launchable_count: number;
  blocked_count: number;
  warning_count: number;
  blocked_sample: CompletenessBlocked[];
  warning_sample: { lead_id: string; company_name: string | null; missing_recommended: string[] }[];
}

// Sendability-definitie komt van de backend (GET /config/sendability) —
// geen eigen kopie meer (Sprint 1-audit §8: de UI-lijst week af van de
// server-gate en gaf de operator een ander risicobeeld). I5.
interface SendabilityConfig {
  always_sendable: string[];
  never_sendable: string[];
  risky_statuses: string[];
  allow_risky: boolean;
}

function isRiskyEmail(status: string | null | undefined, cfg?: SendabilityConfig): boolean {
  if (!status) return true; // missing = unknown = risky
  if (!cfg) return false; // config nog niet geladen → geen vals alarm
  const s = status.toLowerCase();
  return cfg.never_sendable.includes(s) || cfg.risky_statuses.includes(s);
}

function ArchetypeBadge({ archetype, reason }: { archetype: string | null | undefined; reason?: string | null }) {
  if (!archetype) {
    return <span className="text-[10px] text-[var(--color-stone-400)]">—</span>;
  }
  const meta = ARCHETYPE_LABELS[archetype];
  if (!meta) {
    return <span className="text-[10px] text-[var(--color-stone-500)]">{archetype}</span>;
  }
  return (
    <span
      className={`inline-block text-[10px] px-1.5 py-0.5 rounded ${meta.color} truncate max-w-[140px]`}
      title={reason || archetype}
    >
      {meta.label}
    </span>
  );
}

function EmailStatusBadge({ status }: { status: string | null | undefined }) {
  const s = (status || '').toLowerCase();
  let cls = 'bg-[var(--color-ivory-200)] text-[var(--color-stone-600)]';
  let label = status || 'unknown';
  if (s === 'verified' || s === 'valid') {
    cls = 'bg-green-100 text-[var(--color-success)]';
    label = 'verified';
  } else if (s === 'catchall') {
    cls = 'bg-amber-100 text-[var(--color-warning)]';
    label = 'catchall';
  } else if (s === 'bounced' || s === 'invalid' || s === 'not_found' || s === 'unsubscribed') {
    cls = 'bg-red-100 text-[var(--color-danger)]';
    label = s;
  } else if (s === 'pending') {
    cls = 'bg-[var(--color-ivory-200)] text-[var(--color-stone-500)]';
    label = 'pending';
  }
  return (
    <span className={`inline-block text-[10px] px-1.5 py-0.5 rounded ${cls}`}>
      {label}
    </span>
  );
}

interface PreviewResponse {
  template_id: string | null;
  valid: boolean;
  errors: string[];
  lead_count: number;
  preview_count: number;
  previews: LeadPreview[];
  personalization_gate: PersonalizationGate;
  completeness_check?: CompletenessCheck;
}

export function CampagneLaunchPage() {
  // DEPRECATED default: 'v1_cosmetisch_audit' — server-side reroute /campaigns/preview
  // + /campaigns/launch detect dit en mappen naar v3.1 auto-mode (per-lead pick_brug).
  // Frontend-default-flip naar 'v3_1_ai_audit' kan later, geen functioneel verschil nu.
  // Kept until 2026-06-06.
  const [selectedTemplate, setSelectedTemplate] = useState<string>('v1_cosmetisch_audit');
  const [campaignName, setCampaignName] = useState<string>(`Campagne ${new Date().toISOString().slice(0, 10)}`);
  const [selectedLeadIds, setSelectedLeadIds] = useState<Set<string>>(new Set());
  const [activePreviewIdx, setActivePreviewIdx] = useState(0);

  const { data: templates } = useQuery({
    queryKey: ['sequence-templates'],
    queryFn: () => api.get<{ templates: SequenceTemplate[] }>('/sequences/templates'),
  });

  const { data: leadsData, isLoading: leadsLoading } = useQuery({
    queryKey: ['leads', 'campaign-eligible'],
    queryFn: () =>
      api.get<LeadsResponse>(
        '/leads?status=enriched&has_email=true&limit=200&order=score_desc'
      ),
  });

  const { data: inboxData } = useQuery({
    queryKey: ['warmr-inboxes'],
    queryFn: () =>
      api.get<{ inboxes: Inbox[] }>('/warmr/inboxes').catch(() => ({ inboxes: [] })),
  });

  // Server-side sendability-definitie — één bron van waarheid voor
  // risico-badges (I5, geen eigen statuslijst-kopie in de UI).
  const { data: sendability } = useQuery({
    queryKey: ['config-sendability'],
    queryFn: () => api.get<SendabilityConfig>('/config/sendability'),
    staleTime: 5 * 60_000,
  });

  const eligibleLeads = leadsData?.leads || [];
  const availableInboxes = inboxData?.inboxes || [];
  const [selectedInboxIds, setSelectedInboxIds] = useState<Set<string>>(new Set());

  const toggleInbox = (id: string) => {
    setSelectedInboxIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  // Sum daily-caps across selected inboxes for capacity check
  const totalDailyCap = useMemo(() => {
    return availableInboxes
      .filter((i) => selectedInboxIds.has(i.id))
      .reduce((sum, i) => sum + (i.daily_cap || 0), 0);
  }, [availableInboxes, selectedInboxIds]);

  const FIRST_MONTH_LIMIT = 40;  // v1.0 spec: max 40 sends/dag eerste maand
  const effectiveCap = Math.min(totalDailyCap || Infinity, FIRST_MONTH_LIMIT);
  const overCapacity = selectedLeadIds.size > effectiveCap;

  const previewMut = useMutation({
    mutationFn: () =>
      api.post<PreviewResponse>('/campaigns/preview', {
        name: campaignName,
        lead_ids: Array.from(selectedLeadIds),
        template_id: selectedTemplate,
        inbox_ids: Array.from(selectedInboxIds),
      }),
  });

  const tmpl = templates?.templates.find((t) => t.id === selectedTemplate);

  const toggleLead = (id: string) => {
    setSelectedLeadIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const selectAllVisible = () => {
    setSelectedLeadIds(new Set(eligibleLeads.map((l) => l.id)));
  };
  const clearSelection = () => setSelectedLeadIds(new Set());

  const previewLeads = useMemo(
    () => previewMut.data?.previews || [],
    [previewMut.data]
  );
  const activePreview = previewLeads[activePreviewIdx];

  // Count risky emails in current selection (server-definitie, zie
  // /config/sendability — high bounce risk)
  const riskyInSelection = useMemo(() => {
    const selectedSet = selectedLeadIds;
    return eligibleLeads.filter((l) => selectedSet.has(l.id) && isRiskyEmail(l.email_status, sendability));
  }, [eligibleLeads, selectedLeadIds, sendability]);

  // Tone-mismatch: template heeft een `sector` veld; leads waar lead.sector
  // afwijkt van template.sector krijgen verkeerde toon. Voorbeeld:
  // template=v1_cosmetisch_audit (sector=cosmetische_behandelaars) + lead met
  // sector=alternatieve_geneeskunde → mismatch.
  const toneMismatchLeads = useMemo(() => {
    const templateSector = tmpl?.sector;
    if (!templateSector) return [];
    return eligibleLeads.filter((l) => {
      if (!selectedLeadIds.has(l.id)) return false;
      return l.sector && l.sector !== templateSector;
    });
  }, [eligibleLeads, selectedLeadIds, tmpl]);

  return (
    <div className="max-w-7xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="Outbound"
        title="Nieuwe campagne"
        subtitle="Kies template, selecteer leads, preview de mails per lead. Versturen staat momenteel uit."
        actions={
          <Link
            to="/campagnes"
            className="inline-flex items-center gap-2 h-10 px-4 rounded-md text-sm font-medium bg-white border border-[var(--color-border)] hover:bg-[var(--color-ivory-100)]"
          >
            <ArrowLeft className="h-4 w-4" /> Terug naar campagnes
          </Link>
        }
      />

      {/* Send-model banner: samenstellen + preview kan in de browser; launch (draft) en
          activate (verzendmoment) zijn service-key-acties buiten de browser. */}
      <Card className="p-4 mb-6 border-2 border-[var(--color-warning)] bg-amber-50">
        <div className="flex items-start gap-3">
          <ShieldOff className="h-5 w-5 text-[var(--color-warning)] shrink-0 mt-0.5" />
          <div className="text-sm text-[var(--color-stone-700)]">
            <strong>Hier stel je samen en preview je — versturen gebeurt buiten de browser.</strong> Het
            send-model heeft drie fasen: <em>samenstellen/preview</em> (hier), <em>launch</em> = draft
            aanmaken en <em>activate</em> = het verzendmoment. Launch én activate zijn service-key-acties
            (CLI) zodat een browser nooit per ongeluk verstuurt. De kill-switch op activate is{' '}
            <code>ENABLE_PROSPECT_SENDS</code> (legacy-fallback <code>ENABLE_CAMPAIGN_SENDS</code>).
          </div>
        </div>
      </Card>

      <div className="grid lg:grid-cols-[320px_1fr] gap-6">
        {/* LEFT: Template picker + name + actions */}
        <div className="space-y-4">
          <Card className="p-5">
            <h3 className="font-display text-base font-semibold mb-3">Campagne</h3>
            <label className="block">
              <span className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                Naam
              </span>
              <Input
                className="mt-1"
                value={campaignName}
                onChange={(e) => setCampaignName(e.target.value)}
              />
            </label>
          </Card>

          <Card className="p-5">
            <h3 className="font-display text-base font-semibold mb-3">Template</h3>
            <div className="space-y-2">
              {(templates?.templates || []).map((t) => (
                <button
                  key={t.id}
                  onClick={() => setSelectedTemplate(t.id)}
                  className={`w-full text-left rounded-md border p-3 transition-colors ${
                    selectedTemplate === t.id
                      ? 'border-[var(--color-blush-500)] bg-[var(--color-ivory-100)]'
                      : 'border-[var(--color-border)] hover:border-[var(--color-blush-400)]'
                  }`}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-sm font-medium">{t.name}</span>
                    <Badge variant="neutral">v{t.version}</Badge>
                  </div>
                  <div className="text-xs text-[var(--color-stone-500)] mb-2">{t.segment}</div>
                  <div className="flex flex-wrap items-center gap-1.5 text-[10px] text-[var(--color-stone-600)]">
                    <span className="px-1.5 py-0.5 rounded bg-[var(--color-ivory-200)]">
                      {t.step_count} mails
                    </span>
                    <span className="px-1.5 py-0.5 rounded bg-[var(--color-ivory-200)]">
                      cadans {t.cadence_days.join('/')}
                    </span>
                    <span className="px-1.5 py-0.5 rounded bg-[var(--color-ivory-200)]">
                      {t.observation_blocks.length} openers
                    </span>
                  </div>
                </button>
              ))}
            </div>
          </Card>

          <Card className="p-5">
            <h3 className="font-display text-base font-semibold mb-3">Verzendinboxes</h3>
            {availableInboxes.length === 0 ? (
              <p className="text-xs text-[var(--color-stone-500)]">
                Geen ready-inboxes in Warmr. Voeg toe + warm-up voor je kan launchen.
              </p>
            ) : (
              <div className="space-y-1.5 mb-3">
                {availableInboxes.map((i) => (
                  <label
                    key={i.id}
                    className={`flex items-center gap-2 rounded-md border p-2 cursor-pointer ${
                      selectedInboxIds.has(i.id)
                        ? 'border-[var(--color-blush-500)] bg-[var(--color-ivory-100)]'
                        : 'border-[var(--color-border)] hover:border-[var(--color-blush-400)]'
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={selectedInboxIds.has(i.id)}
                      onChange={() => toggleInbox(i.id)}
                    />
                    <div className="flex-1 min-w-0">
                      <div className="text-xs font-medium truncate">{i.email}</div>
                      <div className="text-[10px] text-[var(--color-stone-500)]">
                        cap {i.daily_cap ?? '?'}/dag
                        {i.warmup_score != null && ` · warmup ${i.warmup_score}`}
                      </div>
                    </div>
                  </label>
                ))}
              </div>
            )}
            {selectedInboxIds.size > 0 && (
              <div className="text-[10px] text-[var(--color-stone-600)] pt-2 border-t border-[var(--color-ivory-200)]">
                Totale cap: <strong>{totalDailyCap}/dag</strong>
                {totalDailyCap > FIRST_MONTH_LIMIT && (
                  <> · effectief {FIRST_MONTH_LIMIT}/dag (v1.0 limiet eerste maand)</>
                )}
              </div>
            )}
          </Card>

          {overCapacity && selectedLeadIds.size > 0 && (
            <Card className="p-3 border border-[var(--color-warning)] bg-amber-50">
              <div className="flex items-start gap-2">
                <AlertTriangle className="h-4 w-4 text-[var(--color-warning)] shrink-0 mt-0.5" />
                <div className="text-xs text-[var(--color-stone-700)]">
                  <strong>{selectedLeadIds.size}</strong> leads geselecteerd, maar effectieve
                  capaciteit is <strong>{effectiveCap}/dag</strong>.
                  {totalDailyCap === 0
                    ? ' Selecteer minstens één inbox.'
                    : ' Warmr verspreidt over meerdere dagen óf weigert het overschot.'}
                </div>
              </div>
            </Card>
          )}

          {tmpl && (
            <Card className="p-5">
              <h3 className="font-display text-base font-semibold mb-2">Spelregels</h3>
              <ul className="space-y-1.5 text-xs text-[var(--color-stone-700)]">
                {tmpl.constraints.map((c) => (
                  <li key={c} className="flex items-start gap-1.5">
                    <CheckCircle2 className="h-3.5 w-3.5 text-[var(--color-success)] shrink-0 mt-0.5" />
                    <span>{c}</span>
                  </li>
                ))}
              </ul>
              <div className="mt-3 pt-3 border-t border-[var(--color-ivory-200)] text-xs text-[var(--color-stone-600)]">
                <strong>CTA:</strong> {tmpl.primary_cta}
              </div>
            </Card>
          )}

          {toneMismatchLeads.length > 0 && (
            <Card className="p-3 border border-[var(--color-danger)] bg-red-50">
              <div className="flex items-start gap-2">
                <AlertTriangle className="h-4 w-4 text-[var(--color-danger)] shrink-0 mt-0.5" />
                <div className="text-xs text-[var(--color-stone-700)]">
                  <strong>{toneMismatchLeads.length} tone-mismatch.</strong>
                  Het gekozen template ({tmpl?.name}) is geschreven voor sector
                  <code className="mx-1">{tmpl?.sector}</code>, maar deze leads
                  zitten in een andere sector. Kies een passend template of
                  deselecteer de leads.
                  <button
                    onClick={() => {
                      setSelectedLeadIds((prev) => {
                        const next = new Set(prev);
                        toneMismatchLeads.forEach((l) => next.delete(l.id));
                        return next;
                      });
                    }}
                    className="block mt-1.5 text-[var(--color-danger)] underline hover:text-red-700"
                  >
                    Deselecteer mismatched leads
                  </button>
                </div>
              </div>
            </Card>
          )}

          {riskyInSelection.length > 0 && (
            <Card className="p-3 border border-[var(--color-warning)] bg-amber-50">
              <div className="flex items-start gap-2">
                <AlertTriangle className="h-4 w-4 text-[var(--color-warning)] shrink-0 mt-0.5" />
                <div className="text-xs text-[var(--color-stone-700)]">
                  <strong>{riskyInSelection.length}</strong> van {selectedLeadIds.size} geselecteerde
                  leads heeft een risky email-status (catchall / bounced / unknown). Versturen geeft
                  hoge bounce-rate en schaadt inbox-warmup.
                  <button
                    onClick={() => {
                      setSelectedLeadIds((prev) => {
                        const next = new Set(prev);
                        riskyInSelection.forEach((l) => next.delete(l.id));
                        return next;
                      });
                    }}
                    className="block mt-1.5 text-[var(--color-warning)] underline hover:text-amber-700"
                  >
                    Deselecteer risky leads
                  </button>
                </div>
              </div>
            </Card>
          )}

          <div className="space-y-2">
            <button
              onClick={() => previewMut.mutate()}
              disabled={selectedLeadIds.size === 0 || previewMut.isPending}
              className="w-full inline-flex items-center justify-center gap-2 h-11 rounded-md bg-[var(--color-blush-500)] text-white font-medium hover:bg-[var(--color-blush-600)] disabled:opacity-50"
            >
              <Eye className="h-4 w-4" />
              {previewMut.isPending ? 'Renderen…' : `Preview (${selectedLeadIds.size} leads)`}
            </button>
            <div className="rounded-md border border-[var(--color-border)] bg-[var(--color-ivory-100)] p-3 text-xs text-[var(--color-stone-600)]">
              <div className="font-medium text-[var(--color-stone-700)] mb-1">Volgende stap: handoff (buiten de browser)</div>
              Draft aanmaken (<code>launch</code>) en activeren (<code>activate</code>, het verzendmoment)
              lopen via de service-key/CLI — de browser kan ze niet aanroepen. Stel hier samen en preview;
              de arming zelf gebeurt buiten de browser (kill-switch <code>ENABLE_PROSPECT_SENDS</code>).
            </div>
          </div>
        </div>

        {/* RIGHT: Leads + Preview tabs */}
        <div className="space-y-6">
          {/* Lead selector */}
          <Card className="overflow-hidden">
            <div className="px-5 py-4 border-b border-[var(--color-ivory-200)] flex items-center justify-between">
              <div>
                <h3 className="font-display text-base font-semibold">Leads</h3>
                <p className="text-xs text-[var(--color-stone-500)]">
                  {leadsLoading
                    ? 'Laden…'
                    : `${fmtInt(eligibleLeads.length)} verrijkte leads met email · ${fmtInt(selectedLeadIds.size)} geselecteerd`}
                </p>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={selectAllVisible}
                  className="text-xs px-2 py-1 rounded text-[var(--color-blush-600)] hover:bg-[var(--color-ivory-100)]"
                >
                  Alles selecteren
                </button>
                <button
                  onClick={clearSelection}
                  className="text-xs px-2 py-1 rounded text-[var(--color-stone-600)] hover:bg-[var(--color-ivory-100)]"
                >
                  Wissen
                </button>
              </div>
            </div>
            <div className="max-h-[340px] overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="bg-[var(--color-ivory-100)] sticky top-0">
                  <tr>
                    <th className="w-10"></th>
                    <th className="text-left px-3 py-2 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                      Bedrijf
                    </th>
                    <th className="text-left px-3 py-2 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                      Stad
                    </th>
                    <th className="text-right px-3 py-2 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                      Score
                    </th>
                    <th className="text-left px-3 py-2 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                      Archetype
                    </th>
                    <th className="text-left px-3 py-2 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                      Email
                    </th>
                    <th className="text-left px-3 py-2 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">
                      Status
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {eligibleLeads.map((l: Lead) => {
                    return (
                    <tr
                      key={l.id}
                      onClick={() => toggleLead(l.id)}
                      className={`border-b border-[var(--color-ivory-200)] cursor-pointer ${
                        selectedLeadIds.has(l.id) ? 'bg-[var(--color-ivory-100)]' : 'hover:bg-[var(--color-ivory-50)]'
                      }`}
                    >
                      <td className="px-3 py-2">
                        <input
                          type="checkbox"
                          checked={selectedLeadIds.has(l.id)}
                          onChange={() => toggleLead(l.id)}
                          onClick={(e) => e.stopPropagation()}
                        />
                      </td>
                      <td className="px-3 py-2 font-medium truncate max-w-[260px]">
                        {l.company_name || l.domain || '—'}
                      </td>
                      <td className="px-3 py-2 text-[var(--color-stone-600)]">{l.city || '—'}</td>
                      <td className="px-3 py-2 text-right tabular-nums">{l.score ?? '—'}</td>
                      <td className="px-3 py-2">
                        <ArchetypeBadge archetype={l.archetype} reason={l.archetype_reason} />
                      </td>
                      <td className="px-3 py-2 text-xs text-[var(--color-stone-500)] truncate max-w-[200px]">
                        {l.email || '—'}
                      </td>
                      <td className="px-3 py-2">
                        <EmailStatusBadge status={l.email_status} />
                      </td>
                    </tr>
                    );
                  })}
                  {!leadsLoading && eligibleLeads.length === 0 && (
                    <tr>
                      <td colSpan={7} className="px-3 py-12 text-center text-sm text-[var(--color-stone-500)]">
                        Geen verrijkte leads met email beschikbaar.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </Card>

          {/* Completeness check — vertelt welke leads bij echte launch worden geweigerd */}
          {previewMut.data?.completeness_check && previewMut.data.completeness_check.blocked_count > 0 && (
            <Card className="p-4 border-2 border-[var(--color-danger)] bg-red-50">
              <div className="flex items-start gap-2 mb-3">
                <AlertTriangle className="h-5 w-5 text-[var(--color-danger)] shrink-0 mt-0.5" />
                <div className="flex-1">
                  <h3 className="font-semibold text-sm text-[var(--color-danger)]">
                    {previewMut.data.completeness_check.blocked_count} leads worden geweigerd bij launch
                  </h3>
                  <p className="text-xs text-[var(--color-stone-700)] mt-1">
                    Deze leads missen verplichte enrichment-velden (archetype, score, sector).
                    Re-enqueue ze via <Link to="/crm/activity" className="underline">CRM-board admin</Link>,
                    of markeer ze als test-lead.
                  </p>
                </div>
              </div>
              <details className="text-xs text-[var(--color-stone-700)]">
                <summary className="cursor-pointer">Toon eerste {previewMut.data.completeness_check.blocked_sample.length} leads</summary>
                <ul className="mt-2 space-y-1">
                  {previewMut.data.completeness_check.blocked_sample.map((b) => (
                    <li key={b.lead_id} className="border-b border-[var(--color-ivory-200)] py-1">
                      <Link to={`/leads/${b.lead_id}`} className="font-medium hover:underline">
                        {b.company_name || b.lead_id}
                      </Link>
                      <span className="ml-2 text-[var(--color-stone-500)]">
                        mist: {b.missing_required.join(', ')}
                      </span>
                    </li>
                  ))}
                </ul>
              </details>
            </Card>
          )}

          {/* Soft warnings — launchable maar incomplete in recommended velden */}
          {previewMut.data?.completeness_check && previewMut.data.completeness_check.warning_count > 0 && (
            <Card className="p-3 border border-[var(--color-warning)] bg-amber-50">
              <div className="flex items-start gap-2 text-xs">
                <AlertTriangle className="h-4 w-4 text-[var(--color-warning)] shrink-0 mt-0.5" />
                <div>
                  <strong>{previewMut.data.completeness_check.warning_count} leads</strong> hebben optionele
                  velden ontbreken (contact_first_name, treatment_focus, opener). Werkt nog wel — Mail 1
                  valt terug op static block.
                </div>
              </div>
            </Card>
          )}

          {/* Personalization gate buckets */}
          {previewMut.data?.personalization_gate?.threshold != null && (
            <Card className="p-5">
              <div className="flex items-center justify-between mb-3">
                <div>
                  <h3 className="font-display text-base font-semibold">Personalisatie-gate</h3>
                  <p className="text-xs text-[var(--color-stone-500)]">
                    Drempel: <strong>{previewMut.data.personalization_gate.threshold}</strong> /
                    100 — leads onder de drempel worden geblokkeerd bij launch
                  </p>
                </div>
              </div>
              <div className="grid grid-cols-3 gap-3 mb-4">
                <div className="rounded-md border-2 border-[var(--color-success)] bg-green-50 p-3">
                  <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-success)] mb-1">
                    Auto-send
                  </div>
                  <div className="font-display text-2xl font-semibold">
                    {previewMut.data.personalization_gate.auto_count}
                  </div>
                  <div className="text-xs text-[var(--color-stone-600)]">≥ drempel — gaan mee</div>
                </div>
                <div className="rounded-md border-2 border-[var(--color-warning)] bg-amber-50 p-3">
                  <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-warning)] mb-1">
                    Handmatige review
                  </div>
                  <div className="font-display text-2xl font-semibold">
                    {previewMut.data.personalization_gate.review_count}
                  </div>
                  <div className="text-xs text-[var(--color-stone-600)]">
                    {Math.max((previewMut.data.personalization_gate.threshold ?? 0) - 20, 0)}–
                    {(previewMut.data.personalization_gate.threshold ?? 0) - 1}
                  </div>
                </div>
                <div className="rounded-md border-2 border-[var(--color-danger)] bg-red-50 p-3">
                  <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-danger)] mb-1">
                    Skip
                  </div>
                  <div className="font-display text-2xl font-semibold">
                    {previewMut.data.personalization_gate.skip_count}
                  </div>
                  <div className="text-xs text-[var(--color-stone-600)]">te weinig data</div>
                </div>
              </div>

              {previewMut.data.personalization_gate.review_sample.length > 0 && (
                <details className="text-xs">
                  <summary className="cursor-pointer text-[var(--color-stone-600)] hover:text-[var(--color-stone-900)]">
                    Toon eerste {previewMut.data.personalization_gate.review_sample.length} review-bucket leads
                  </summary>
                  <ul className="mt-2 space-y-1">
                    {previewMut.data.personalization_gate.review_sample.map((l) => (
                      <li key={l.lead_id} className="flex items-center justify-between border-b border-[var(--color-ivory-200)] py-1">
                        <span>{l.company_name || l.lead_id}</span>
                        <span className="tabular-nums text-[var(--color-stone-500)]">
                          {l.personalization_score}/100
                        </span>
                      </li>
                    ))}
                  </ul>
                </details>
              )}
            </Card>
          )}

          {/* Preview */}
          {previewMut.data && (
            <Card className="p-0 overflow-hidden">
              <div className="px-5 py-4 border-b border-[var(--color-ivory-200)] flex items-center justify-between">
                <div>
                  <h3 className="font-display text-base font-semibold">Preview</h3>
                  <p className="text-xs text-[var(--color-stone-500)]">
                    Eerste {previewLeads.length} van {previewMut.data.lead_count} leads — exact wat de lead krijgt
                  </p>
                </div>
                {!previewMut.data.valid && (
                  <Badge variant="warning">{previewMut.data.errors.length} validatiefouten</Badge>
                )}
              </div>

              {previewLeads.length > 0 && (
                <>
                  <div className="px-5 pt-3 flex flex-wrap gap-1 border-b border-[var(--color-ivory-200)]">
                    {previewLeads.map((p, i) => (
                      <button
                        key={p.lead_id}
                        onClick={() => setActivePreviewIdx(i)}
                        className={`text-xs px-2.5 py-1.5 rounded-t-md border-b-2 transition-colors ${
                          i === activePreviewIdx
                            ? 'border-[var(--color-blush-500)] text-[var(--color-stone-900)] font-medium'
                            : 'border-transparent text-[var(--color-stone-500)] hover:text-[var(--color-stone-800)]'
                        }`}
                      >
                        {p.company_name || p.domain || '—'}
                      </button>
                    ))}
                  </div>

                  {activePreview && (
                    <div className="p-5 space-y-4">
                      {activePreview.observation_block && (
                        <div className="text-xs flex flex-wrap items-center gap-2">
                          <span className="text-[var(--color-stone-500)]">Mail-1 blok:</span>
                          <Badge variant="neutral">{activePreview.observation_block}</Badge>
                          <span className="text-[var(--color-stone-500)] ml-2">Bron:</span>
                          {activePreview.opener_source === 'claude_personalized' ? (
                            <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-700">
                              ✨ Claude per-lead
                            </span>
                          ) : (
                            <span
                              className="text-[10px] px-1.5 py-0.5 rounded bg-[var(--color-ivory-200)] text-[var(--color-stone-600)]"
                              title={activePreview.opener_fallback_reason || ''}
                            >
                              📋 Statisch fallback
                              {activePreview.opener_fallback_reason && (
                                <> · {activePreview.opener_fallback_reason}</>
                              )}
                            </span>
                          )}
                        </div>
                      )}
                      {activePreview.steps.map((s, i) => (
                        <div key={i} className="border border-[var(--color-ivory-200)] rounded-md overflow-hidden">
                          <div className="px-4 py-2 bg-[var(--color-ivory-100)] flex items-center justify-between">
                            <div className="text-xs font-semibold text-[var(--color-stone-700)]">
                              Mail {i + 1} {i > 0 ? `· dag +${s.delay_days}` : '· dag 0'}
                            </div>
                          </div>
                          <div className="px-4 py-3 border-b border-[var(--color-ivory-200)]">
                            <div className="text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">
                              Onderwerp
                            </div>
                            <div className="text-sm font-medium">{s.subject}</div>
                          </div>
                          <div className="px-4 py-3 whitespace-pre-wrap text-sm leading-relaxed text-[var(--color-stone-800)]">
                            {s.body}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </>
              )}

              {!previewMut.data.valid && (
                <div className="px-5 pb-5">
                  <div className="rounded-md bg-amber-50 border border-[var(--color-warning)] p-3 text-xs text-[var(--color-stone-700)]">
                    {previewMut.data.errors.map((e, i) => (
                      <div key={i}>• {e}</div>
                    ))}
                  </div>
                </div>
              )}
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}
