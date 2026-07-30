import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Bell } from 'lucide-react';
import { api } from '@/lib/api';
import { fmtRelative } from '@/lib/format';

interface Alert {
  id: string;
  alert_type: string;
  message: string;
  severity: string;
  is_read: boolean;
  created_at: string;
}

const SEV_DOT: Record<string, string> = {
  critical: 'bg-[var(--color-danger)]',
  warning: 'bg-[var(--color-warning)]',
  info: 'bg-[var(--color-stone-400)]',
};

/** Notificatie-bel in de header: ongelezen system_alerts (budget-cap, stall, send-guard, …). */
export function AlertsBell() {
  const [open, setOpen] = useState(false);
  const qc = useQueryClient();

  const { data } = useQuery({
    queryKey: ['alerts'],
    queryFn: () => api.get<{ alerts: Alert[] }>('/alerts').catch(() => null),
    refetchInterval: 60_000,
    staleTime: 30_000,
  });
  const markRead = useMutation({
    mutationFn: (id: string) => api.patch(`/alerts/${id}/read`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['alerts'] }),
  });

  const alerts = data?.alerts || [];
  const count = alerts.length;
  const hasCritical = alerts.some((a) => a.severity === 'critical');

  return (
    <div className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        title={`${count} ongelezen alert${count === 1 ? '' : 's'}`}
        className="relative inline-flex items-center justify-center h-8 w-8 rounded-md border border-[var(--color-border)] text-[var(--color-stone-500)] hover:bg-[var(--color-ivory-100)]"
      >
        <Bell className="h-3.5 w-3.5" />
        {count > 0 && (
          <span
            className={`absolute -top-1 -right-1 min-w-[16px] h-4 px-1 rounded-full text-[9px] font-semibold text-white flex items-center justify-center ${
              hasCritical ? 'bg-[var(--color-danger)]' : 'bg-[var(--color-warning)]'
            }`}
          >
            {count > 9 ? '9+' : count}
          </span>
        )}
      </button>

      {open && (
        <>
          <div className="fixed inset-0 z-30" onClick={() => setOpen(false)} />
          <div className="absolute right-0 top-full mt-1 z-40 w-80 max-h-96 overflow-y-auto rounded-md border border-[var(--color-border)] bg-white shadow-lg">
            <div className="px-3 py-2 border-b border-[var(--color-border)] text-xs font-semibold text-[var(--color-stone-600)]">
              Alerts ({count})
            </div>
            {alerts.length === 0 ? (
              <div className="p-4 text-center text-xs text-[var(--color-stone-400)]">Geen ongelezen alerts.</div>
            ) : (
              alerts.map((a) => (
                <div key={a.id} className="px-3 py-2 border-b border-[var(--color-ivory-200)] flex items-start gap-2">
                  <span className={`mt-1 h-2 w-2 rounded-full shrink-0 ${SEV_DOT[a.severity] || SEV_DOT.info}`} />
                  <div className="min-w-0 flex-1">
                    <div className="text-xs text-[var(--color-stone-700)]">{a.message}</div>
                    <div className="text-[10px] text-[var(--color-stone-400)] mt-0.5">{a.alert_type} · {fmtRelative(a.created_at)}</div>
                  </div>
                  <button
                    onClick={() => markRead.mutate(a.id)}
                    disabled={markRead.isPending}
                    className="text-[10px] text-[var(--color-blush-500)] hover:underline shrink-0 disabled:opacity-50"
                  >
                    gelezen
                  </button>
                </div>
              ))
            )}
          </div>
        </>
      )}
    </div>
  );
}
