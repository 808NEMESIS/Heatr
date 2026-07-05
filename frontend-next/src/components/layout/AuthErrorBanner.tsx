import { useEffect, useState } from 'react';

interface AuthErrorDetail {
  status: number;
  path: string;
  message: string;
}

/**
 * Rode banner bovenin zodra een API-call 401/403 teruggeeft (event vanuit
 * lib/api.ts). Zonder deze banner lekten auth-fouten weg als stille lege
 * lijsten — de operator zag "geen leads" terwijl het een config-issue was.
 */
export function AuthErrorBanner() {
  const [error, setError] = useState<AuthErrorDetail | null>(null);

  useEffect(() => {
    const handler = (e: Event) => {
      setError((e as CustomEvent<AuthErrorDetail>).detail);
    };
    window.addEventListener('heatr:auth-error', handler);
    return () => window.removeEventListener('heatr:auth-error', handler);
  }, []);

  if (!error) return null;

  return (
    <div className="bg-[var(--color-danger-bg,#fde8e8)] border-b border-[var(--color-danger,#c0392b)] px-6 py-3 text-sm text-[var(--color-danger,#c0392b)]">
      <strong>Niet geautoriseerd ({error.status})</strong> — de API weigert
      requests ({error.message}). Controleer de auth-configuratie: is{' '}
      <code className="font-mono">LEGACY_DEV_TOKEN_ALLOWED=true</code> gezet
      tijdens de frontend-cutover, of is er een geldige Supabase-sessie? Zie{' '}
      <code className="font-mono">GET /healthz</code> → <code className="font-mono">auth</code>{' '}
      voor de actieve auth-modes.
      <button
        onClick={() => setError(null)}
        className="ml-3 underline hover:no-underline"
      >
        sluiten
      </button>
    </div>
  );
}
