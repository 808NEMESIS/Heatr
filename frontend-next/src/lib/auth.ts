/**
 * Auth-helpers — A0 operator-shell (Sprint 5, verbetering #1 + #5).
 *
 * Draait op de gedeelde Supabase-client (./supabase). Eén login via Warmr óf
 * de Heatr-loginpagina; de sessie leeft same-origin in localStorage en wordt
 * automatisch ververst. Heatr's backend accepteert de resulterende JWT.
 */
import { supabase } from './supabase';

/**
 * Huidige access_token voor de Authorization-header.
 *   1. expliciete `heatr_token` in sessionStorage (handmatige/legacy override)
 *   2. de gedeelde Supabase-sessie (ververst automatisch bij verloop)
 *   3. null → geen sessie (caller/AuthGate stuurt naar login)
 *
 * `getSession()` geeft de in-memory sessie terug en triggert refresh wanneer
 * de token bijna verloopt (autoRefreshToken), dus we sturen nooit stil een
 * verlopen token.
 */
export async function getAuthToken(): Promise<string | null> {
  if (typeof window !== 'undefined') {
    const manual = sessionStorage.getItem('heatr_token');
    if (manual) return manual;
  }
  const { data } = await supabase.auth.getSession();
  return data.session?.access_token ?? null;
}

/** Is er een geldige sessie? (voor de AuthGate) */
export async function hasSession(): Promise<boolean> {
  const { data } = await supabase.auth.getSession();
  return !!data.session;
}

export async function signInWithPassword(email: string, password: string) {
  return supabase.auth.signInWithPassword({ email, password });
}

export async function signOut() {
  return supabase.auth.signOut();
}
