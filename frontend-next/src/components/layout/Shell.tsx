import { useState } from 'react';
import { Outlet } from 'react-router-dom';
import { LogOut, Menu } from 'lucide-react';
import { Sidebar, SidebarNav } from './Sidebar';
import { CostBadge } from './CostBadge';
import { WorkerStatus } from './WorkerStatus';
import { AlertsBell } from './AlertsBell';
import { AuthErrorBanner } from './AuthErrorBanner';
import { SystemToggle } from './SystemToggle';
import { signOut } from '@/lib/auth';

export function Shell() {
  const [drawerOpen, setDrawerOpen] = useState(false);

  return (
    <div className="flex min-h-screen bg-[var(--color-background)]">
      <Sidebar />

      {/* Mobiele nav-drawer (<lg): slide-in met backdrop */}
      {drawerOpen && (
        <div className="fixed inset-0 z-40 lg:hidden">
          <div className="absolute inset-0 bg-black/30" onClick={() => setDrawerOpen(false)} />
          <aside className="absolute left-0 top-0 h-full w-56 flex flex-col border-r border-[var(--color-border)] bg-white shadow-xl">
            <SidebarNav onNavigate={() => setDrawerOpen(false)} />
          </aside>
        </div>
      )}

      <main className="flex-1 overflow-x-hidden">
        <AuthErrorBanner />
        <header className="sticky top-0 z-20 flex items-center justify-between gap-3 px-6 py-2 bg-white/80 backdrop-blur border-b border-[var(--color-border)]">
          <button
            onClick={() => setDrawerOpen(true)}
            className="lg:hidden inline-flex items-center justify-center h-8 w-8 rounded-md text-[var(--color-stone-600)] hover:bg-[var(--color-ivory-100)]"
            title="Menu"
            aria-label="Navigatie openen"
          >
            <Menu className="h-5 w-5" />
          </button>
          <div className="flex items-center gap-3 ml-auto">
            <SystemToggle />
            <div className="mx-1 h-5 w-px bg-[var(--color-border)]" />
            <WorkerStatus />
            <AlertsBell />
            <CostBadge />
            <button
              onClick={() => signOut()}
              title="Uitloggen (beëindigt de gedeelde sessie voor Heatr én Warmr)"
              className="inline-flex items-center gap-1.5 rounded-md border border-[var(--color-border)] px-2 py-1 text-xs text-[var(--color-stone-500)] hover:text-[var(--color-stone-800)] hover:bg-[var(--color-ivory-100)]"
            >
              <LogOut className="h-3.5 w-3.5" /> Uitloggen
            </button>
          </div>
        </header>
        <Outlet />
      </main>
    </div>
  );
}
