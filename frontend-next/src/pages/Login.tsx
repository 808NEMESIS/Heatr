import { useState } from 'react';
import { signInWithPassword } from '@/lib/auth';

/**
 * Heatr-loginpagina (A0 verbetering #5 — login-anywhere).
 *
 * Zelfde Supabase-project als Warmr; inloggen hier schrijft de gedeelde,
 * same-origin sessie, dus daarna is óók de Warmr-kant ingelogd. Wie liever op
 * de Warmr-kant inlogt, klikt de link onderaan — het resultaat is identiek.
 */
export function LoginPage() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    const { error } = await signInWithPassword(email.trim(), password);
    setBusy(false);
    if (error) setError(error.message);
    // Succes → AuthGate's onAuthStateChange rendert de app.
  }

  return (
    <div className="grid min-h-screen place-items-center bg-[var(--color-background)] px-4">
      <div className="w-full max-w-sm">
        <div className="mb-6 flex items-center gap-2.5">
          <img
            src={`${import.meta.env.BASE_URL}heatr-logo.png`}
            alt="Heatr"
            className="h-9 w-auto"
          />
        </div>
        <div className="rounded-xl border border-[var(--color-border)] bg-white p-6 shadow-sm">
          <h1 className="font-display text-xl font-semibold">Inloggen</h1>
          <p className="mt-1 mb-5 text-sm text-[var(--color-stone-500)]">
            Eén login voor Heatr én Warmr.
          </p>
          <form onSubmit={onSubmit} className="space-y-3">
            <label className="block">
              <span className="mb-1 block text-xs font-medium text-[var(--color-stone-600)]">
                E-mail
              </span>
              <input
                type="email"
                autoComplete="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className="w-full rounded-md border border-[var(--color-border)] px-3 py-2 text-sm outline-none focus:border-[var(--color-blush-400)]"
              />
            </label>
            <label className="block">
              <span className="mb-1 block text-xs font-medium text-[var(--color-stone-600)]">
                Wachtwoord
              </span>
              <input
                type="password"
                autoComplete="current-password"
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full rounded-md border border-[var(--color-border)] px-3 py-2 text-sm outline-none focus:border-[var(--color-blush-400)]"
              />
            </label>
            {error && (
              <p className="text-xs text-[var(--color-danger)]">{error}</p>
            )}
            <button
              type="submit"
              disabled={busy}
              className="w-full rounded-md bg-[var(--color-blush-500)] px-3 py-2 text-sm font-medium text-white hover:bg-[var(--color-blush-600)] disabled:opacity-50"
            >
              {busy ? 'Bezig…' : 'Inloggen'}
            </button>
          </form>
          <p className="mt-4 text-center text-xs text-[var(--color-stone-500)]">
            Liever via Warmr?{' '}
            <a href="/" className="text-[var(--color-blush-600)] hover:underline">
              Ga naar Warmr-login
            </a>
          </p>
        </div>
      </div>
    </div>
  );
}
