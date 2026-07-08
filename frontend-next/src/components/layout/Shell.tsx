import { Outlet } from 'react-router-dom';
import { LogOut } from 'lucide-react';
import { Sidebar } from './Sidebar';
import { CostBadge } from './CostBadge';
import { WorkerStatus } from './WorkerStatus';
import { AuthErrorBanner } from './AuthErrorBanner';
import { SystemToggle } from './SystemToggle';
import { signOut } from '@/lib/auth';

export function Shell() {
  return (
    <div className="flex min-h-screen bg-[var(--color-background)]">
      <Sidebar />
      <main className="flex-1 overflow-x-hidden">
        <AuthErrorBanner />
        <header className="sticky top-0 z-20 flex items-center justify-end gap-3 px-6 py-2 bg-white/80 backdrop-blur border-b border-[var(--color-border)]">
          <SystemToggle />
          <div className="mx-1 h-5 w-px bg-[var(--color-border)]" />
          <WorkerStatus />
          <CostBadge />
          <button
            onClick={() => signOut()}
            title="Uitloggen (beëindigt de gedeelde sessie voor Heatr én Warmr)"
            className="inline-flex items-center gap-1.5 rounded-md border border-[var(--color-border)] px-2 py-1 text-xs text-[var(--color-stone-500)] hover:text-[var(--color-stone-800)] hover:bg-[var(--color-ivory-100)]"
          >
            <LogOut className="h-3.5 w-3.5" /> Uitloggen
          </button>
        </header>
        <Outlet />
      </main>
    </div>
  );
}
