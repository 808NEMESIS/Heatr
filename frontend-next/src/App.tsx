import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
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
import { AnalyticsPage } from '@/pages/Analytics';

const queryClient = new QueryClient({
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
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
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
            <Route path="/analytics" element={<AnalyticsPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
