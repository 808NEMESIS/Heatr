import { useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { supabase } from '@/lib/supabase';
import { LoginPage } from '@/pages/Login';

/**
 * AuthGate — vereist een geldige Supabase-sessie voordat de app rendert
 * (A0 verbetering #5). Zonder sessie → loginpagina. Reageert live op
 * inloggen/uitloggen (ook wanneer de sessie op de Warmr-kant verandert, want
 * die deelt dezelfde storage) via onAuthStateChange.
 */
export function AuthGate({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<'loading' | 'in' | 'out'>('loading');

  useEffect(() => {
    let mounted = true;
    supabase.auth.getSession().then(({ data }) => {
      if (mounted) setStatus(data.session ? 'in' : 'out');
    });
    const { data: sub } = supabase.auth.onAuthStateChange((_event, session) => {
      setStatus(session ? 'in' : 'out');
    });
    return () => {
      mounted = false;
      sub.subscription.unsubscribe();
    };
  }, []);

  if (status === 'loading') {
    return (
      <div className="grid min-h-screen place-items-center text-sm text-[var(--color-stone-500)]">
        Laden…
      </div>
    );
  }
  if (status === 'out') return <LoginPage />;
  return <>{children}</>;
}
