import { NavLink } from 'react-router-dom';
import {
  LayoutDashboard,
  Search,
  Users,
  Target,
  Send,
  Inbox,
  Workflow,
  Kanban,
  Upload,
  BarChart3,
  Activity,
  Phone,
} from 'lucide-react';
import { useQuery } from '@tanstack/react-query';
import { api } from '@/lib/api';
import { cn } from '@/lib/cn';

interface SidebarCounts {
  recontact_due: number | null;
  unread_replies: number | null;
  worker_healthy: boolean | null;
  worker_pending_jobs: number;
  worker_recent_completed: number;
}

const NAV = [
  { to: '/', icon: LayoutDashboard, label: 'Dashboard', end: true, badge: undefined as 'recontact' | 'inbox' | undefined },
  { to: '/zoeken', icon: Search, label: 'Zoeken' },
  { to: '/leads', icon: Users, label: 'Leads' },
  { to: '/leads/import', icon: Upload, label: 'Import' },
  { to: '/website-kansen', icon: Target, label: 'Website kansen' },
  { to: '/campagnes', icon: Send, label: 'Campagnes' },
  { to: '/inbox', icon: Inbox, label: 'Inbox', badge: 'inbox' as const },
  { to: '/crm', icon: Workflow, label: 'CRM' },
  { to: '/crm/activity', icon: Kanban, label: 'CRM activity', badge: 'recontact' as const },
  { to: '/gesprekken', icon: Phone, label: 'Gesprekken' },
  { to: '/analytics', icon: BarChart3, label: 'Analytics' },
  { to: '/control', icon: Activity, label: 'Control' },
];

export function Sidebar() {
  const { data: counts } = useQuery({
    queryKey: ['sidebar-counts'],
    queryFn: () => api.get<SidebarCounts>('/system/sidebar-counts').catch(() => null),
    refetchInterval: 60_000,
    staleTime: 30_000,
  });

  const badgeFor = (kind: 'recontact' | 'inbox' | undefined): number | null => {
    if (!counts || !kind) return null;
    if (kind === 'recontact') return counts.recontact_due;
    if (kind === 'inbox') return counts.unread_replies;
    return null;
  };

  return (
    <aside className="hidden lg:flex w-56 shrink-0 flex-col border-r border-[var(--color-border)] bg-white">
      <div className="flex items-center gap-2 px-5 py-5 border-b border-[var(--color-border)]">
        <img src={`${import.meta.env.BASE_URL}heatr-logo.png`} alt="Heatr" className="h-8 w-auto" />
      </div>

      <nav className="flex-1 p-3 space-y-0.5">
        <div className="px-2 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-[var(--color-stone-400)]">
          Navigatie
        </div>
        {NAV.map((item) => {
          const count = badgeFor(item.badge);
          return (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                cn(
                  'flex items-center gap-2.5 rounded-md px-2.5 py-2 text-sm font-medium transition-colors',
                  isActive
                    ? 'bg-[var(--color-blush-100)] text-[var(--color-blush-700)]'
                    : 'text-[var(--color-stone-600)] hover:bg-[var(--color-ivory-100)] hover:text-[var(--color-stone-800)]'
                )
              }
            >
              <item.icon className="h-4 w-4" />
              <span className="flex-1">{item.label}</span>
              {count !== null && count !== undefined && count > 0 && (
                <span className="text-[10px] font-semibold tabular-nums px-1.5 py-0.5 rounded-full bg-[var(--color-blush-500)] text-white min-w-[18px] text-center">
                  {count > 99 ? '99+' : count}
                </span>
              )}
            </NavLink>
          );
        })}
      </nav>

      <div className="p-3 border-t border-[var(--color-border)]">
        <div className="flex items-center gap-2.5 px-2 py-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-full bg-[var(--color-blush-200)] text-[var(--color-blush-700)] text-xs font-semibold">
            AE
          </div>
          <div className="flex-1 min-w-0">
            <div className="text-sm font-medium leading-tight">Aerys</div>
            <div className="text-xs text-[var(--color-stone-500)] truncate">info@aeryssolution.nl</div>
          </div>
        </div>
      </div>
    </aside>
  );
}
