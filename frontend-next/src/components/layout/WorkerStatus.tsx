import { useQuery } from '@tanstack/react-query';
import { Activity, AlertCircle } from 'lucide-react';
import { api } from '@/lib/api';

interface SidebarCounts {
  worker_healthy: boolean | null;
  worker_pending_jobs: number;
  worker_recent_completed: number;
}

/**
 * Mini worker-health pill. Groen = recent activiteit (<5min) of geen pending werk.
 * Amber = pending jobs zonder recent processed (worker mogelijk vast).
 * Grijs = kan status niet ophalen.
 */
export function WorkerStatus() {
  const { data } = useQuery({
    queryKey: ['sidebar-counts'],  // gedeeld met Sidebar — 1 fetch totaal
    queryFn: () => api.get<SidebarCounts>('/system/sidebar-counts').catch(() => null),
    refetchInterval: 60_000,
    staleTime: 30_000,
  });

  if (!data || data.worker_healthy === null) {
    return (
      <div className="text-xs text-[var(--color-stone-400)] flex items-center gap-1.5 px-3">
        <Activity className="h-3.5 w-3.5" />
        <span>Worker —</span>
      </div>
    );
  }

  const healthy = data.worker_healthy;
  const tooltip = healthy
    ? `Worker actief — ${data.worker_recent_completed} jobs in laatste 5 min, ${data.worker_pending_jobs} in queue`
    : `Worker mogelijk vast — ${data.worker_pending_jobs} pending jobs zonder recente activiteit`;

  return (
    <div
      title={tooltip}
      className={`inline-flex items-center gap-1.5 text-xs px-2.5 py-1 rounded-md border ${
        healthy
          ? 'border-emerald-300 bg-emerald-50 text-emerald-700'
          : 'border-amber-300 bg-amber-50 text-amber-700'
      }`}
    >
      {healthy ? (
        <>
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
            <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
          </span>
          <span>Worker</span>
        </>
      ) : (
        <>
          <AlertCircle className="h-3 w-3" />
          <span>{data.worker_pending_jobs} stuck</span>
        </>
      )}
    </div>
  );
}
