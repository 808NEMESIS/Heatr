import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { MutationCache, QueryCache, QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ApiError } from '@/lib/api';
import { ErrorBoundary } from '@/components/ErrorBoundary';
import { Toaster } from '@/components/ui/toast';
import { toast } from '@/lib/toast';
import { Shell } from '@/components/layout/Shell';
import { DashboardPage } from '@/pages/Dashboard';
import { LeadsPage } from '@/pages/Leads';
import { LeadDetailPage } from '@/pages/LeadDetail';
import { WebsiteKansenPage } from '@/pages/WebsiteKansen';
import { ZoekenPage } from '@/pages/Zoeken';
import { CRMPage } from '@/pages/CRM';
import { InboxPage } from '@/pages/Inbox';
import { CampagnesPage } from '@/pages/Campagnes';
import { CampagneLaunchPage } from '@/pages/CampagneLaunch';
import { LeadsImportPage } from '@/pages/LeadsImport';
import { CRMActivityPage } from '@/pages/CRMActivity';
import { GesprekkenPage } from '@/pages/Gesprekken';
import { AnalyticsPage } from '@/pages/Analytics';
import { ControlPage } from '@/pages/Control';
import { CompliancePage } from '@/pages/Compliance';
import { AuthGate } from '@/components/AuthGate';

/** 401/403 worden al gemeld via de AuthErrorBanner (heatr:auth-error event)
 *  — die hier ook toasten zou dubbel alarm geven. */
function notifyQueryError(error: unknown) {
  if (error instanceof ApiError && (error.status === 401 || error.status === 403)) return;
  const msg = error instanceof Error ? error.message : 'Onbekende fout';
  toast(`API-fout: ${msg}`, 'error');
}

const queryClient = new QueryClient({
  queryCache: new QueryCache({ onError: notifyQueryError }),
  mutationCache: new MutationCache({ onError: notifyQueryError }),
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

export default function App() {
  return (
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        {/* AuthGate: geen render zonder geldige Supabase-sessie (login-anywhere).
            basename volgt Vite's base (/heatr/) — Heatr leeft same-origin onder
            /heatr/*, Warmr op de origin-root. */}
        <AuthGate>
        <BrowserRouter basename={import.meta.env.BASE_URL.replace(/\/$/, '')}>
          <Routes>
            <Route element={<Shell />}>
              <Route index element={<DashboardPage />} />
              <Route path="/zoeken" element={<ZoekenPage />} />
              <Route path="/leads" element={<LeadsPage />} />
              <Route path="/leads/import" element={<LeadsImportPage />} />
              <Route path="/leads/:id" element={<LeadDetailPage />} />
              <Route path="/website-kansen" element={<WebsiteKansenPage />} />
              <Route path="/campagnes" element={<CampagnesPage />} />
              <Route path="/campagnes/nieuw" element={<CampagneLaunchPage />} />
              <Route path="/inbox" element={<InboxPage />} />
              <Route path="/crm" element={<CRMPage />} />
              <Route path="/crm/activity" element={<CRMActivityPage />} />
              <Route path="/gesprekken" element={<GesprekkenPage />} />
              <Route path="/analytics" element={<AnalyticsPage />} />
              <Route path="/control" element={<ControlPage />} />
              <Route path="/compliance" element={<CompliancePage />} />
            </Route>
          </Routes>
        </BrowserRouter>
        </AuthGate>
        <Toaster />
      </QueryClientProvider>
    </ErrorBoundary>
  );
}
