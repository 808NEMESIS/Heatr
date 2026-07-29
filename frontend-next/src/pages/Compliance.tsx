import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { fmtRelative } from '@/lib/format';
import { PageHeader } from '@/components/layout/PageHeader';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

interface Recipient { name: string; role: string; country: string; purpose: string; }
interface Activity {
  name: string; purpose: string; legal_basis: string;
  data_categories: string[]; data_subjects: string; recipients: Recipient[];
  retention: string; security_measures: string[];
}
interface Register {
  organisation: string; tool: string; dpo_contact: string; generated_at: string;
  processing_activities: Activity[];
}
interface GdprLogRow {
  id?: string; action: string; performed_by?: string | null; lead_id?: string | null; completed_at?: string | null;
}

export function CompliancePage() {
  const { data: reg } = useQuery({
    queryKey: ['gdpr-register'],
    queryFn: () => api.get<Register>('/gdpr/register'),
  });
  const { data: logData } = useQuery({
    queryKey: ['gdpr-log'],
    queryFn: () => api.get<{ log: GdprLogRow[] }>('/gdpr/log?limit=50'),
  });
  const log = logData?.log || [];

  return (
    <div className="max-w-5xl mx-auto px-10 py-8">
      <PageHeader
        eyebrow="AVG / GDPR"
        title="Compliance"
        subtitle="Artikel 30-verwerkingsregister + AVG-auditlog. Vergeten/exporteren per lead staat op de leadpagina."
      />

      {reg && (
        <section className="mb-8 space-y-4">
          <Card className="p-5">
            <h3 className="font-display text-base font-semibold mb-2">Verwerkingsregister (Art. 30)</h3>
            <div className="text-xs text-[var(--color-stone-500)] flex flex-wrap gap-x-4 gap-y-1">
              <span>{reg.organisation} · {reg.tool}</span>
              <span>DPO: {reg.dpo_contact}</span>
              <span>Gegenereerd {fmtRelative(reg.generated_at)}</span>
            </div>
          </Card>
          {reg.processing_activities.map((a) => (
            <Card key={a.name} className="p-5">
              <h4 className="font-display text-sm font-semibold mb-1">{a.name}</h4>
              <p className="text-sm text-[var(--color-stone-600)] mb-3">{a.purpose}</p>
              <dl className="grid sm:grid-cols-2 gap-x-6 gap-y-3 text-xs">
                <div>
                  <dt className="uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">Rechtsgrond</dt>
                  <dd className="text-[var(--color-stone-700)]">{a.legal_basis}</dd>
                </div>
                <div>
                  <dt className="uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">Betrokkenen</dt>
                  <dd className="text-[var(--color-stone-700)]">{a.data_subjects}</dd>
                </div>
                <div>
                  <dt className="uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">Gegevenscategorieën</dt>
                  <dd><ul className="list-disc pl-4 text-[var(--color-stone-700)] space-y-0.5">{a.data_categories.map((c) => <li key={c}>{c}</li>)}</ul></dd>
                </div>
                <div>
                  <dt className="uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">Beveiliging</dt>
                  <dd><ul className="list-disc pl-4 text-[var(--color-stone-700)] space-y-0.5">{a.security_measures.map((s) => <li key={s}>{s}</li>)}</ul></dd>
                </div>
                <div className="sm:col-span-2">
                  <dt className="uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">Ontvangers (verwerkers)</dt>
                  <dd className="flex flex-wrap gap-1.5">
                    {a.recipients.map((r) => (
                      <Badge key={r.name} variant="neutral" title={`${r.role} · ${r.purpose}`}>{r.name} ({r.country})</Badge>
                    ))}
                  </dd>
                </div>
                <div className="sm:col-span-2">
                  <dt className="uppercase tracking-wider font-semibold text-[var(--color-stone-500)] mb-1">Bewaartermijn</dt>
                  <dd className="text-[var(--color-stone-700)]">{a.retention}</dd>
                </div>
              </dl>
            </Card>
          ))}
        </section>
      )}

      <section>
        <Card className="overflow-hidden">
          <div className="px-4 py-3 border-b border-[var(--color-border)]">
            <h3 className="font-display text-sm font-semibold">AVG-auditlog</h3>
          </div>
          {log.length === 0 ? (
            <div className="p-6 text-center text-xs text-[var(--color-stone-500)]">Nog geen AVG-acties gelogd.</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[var(--color-border)] bg-[var(--color-ivory-100)]">
                    {['Actie', 'Lead', 'Door', 'Tijd'].map((h) => (
                      <th key={h} className="text-left px-4 py-2.5 text-[10px] uppercase tracking-wider font-semibold text-[var(--color-stone-500)]">{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {log.map((r, i) => (
                    <tr key={r.id || i} className="border-b border-[var(--color-ivory-200)]">
                      <td className="px-4 py-2 text-xs"><Badge variant="neutral">{r.action}</Badge></td>
                      <td className="px-4 py-2 text-xs font-mono">{r.lead_id ? `${r.lead_id.slice(0, 8)}…` : '—'}</td>
                      <td className="px-4 py-2 text-xs text-[var(--color-stone-600)]">{r.performed_by || '—'}</td>
                      <td className="px-4 py-2 text-xs text-[var(--color-stone-500)]">{r.completed_at ? fmtRelative(r.completed_at) : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Card>
      </section>
    </div>
  );
}
