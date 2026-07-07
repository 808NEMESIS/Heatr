import { Outlet } from 'react-router-dom';
import { Sidebar } from './Sidebar';
import { CostBadge } from './CostBadge';
import { WorkerStatus } from './WorkerStatus';
import { AuthErrorBanner } from './AuthErrorBanner';
import { SystemToggle } from './SystemToggle';

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
        </header>
        <Outlet />
      </main>
    </div>
  );
}
