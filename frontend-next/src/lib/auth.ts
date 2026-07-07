/**
 * Gedeelde Supabase-sessie lezen — A0 same-origin operator-shell (Sprint 5).
 *
 * De operator logt één keer in via Warmr's Supabase-login. supabase-js bewaart
 * die sessie in localStorage onder `sb-<project-ref>-auth-token`. Omdat de
 * Warmr-view same-origin onder Heatr draait (Vite `/warmr/*` in dev,
 * reverse-proxy in prod), deelt Heatr diezelfde localStorage en kan het de
 * ECHTE Supabase-JWT meesturen i.p.v. `dev-token`.
 *
 * Heatr's backend accepteert die JWT al (`get_workspace` decodet tegen
 * `SUPABASE_JWT_SECRET`). Geen backend-wijziging, geen wijziging aan Warmr.
 */

const PROJECT_REF =
  import.meta.env.VITE_SUPABASE_PROJECT_REF || 'zomdrygdcaenjnrrpcpw';

const SB_KEY_RE = /^sb-.*-auth-token$/;

/** Haal het access_token uit een supabase-js localStorage-blob (v2-varianten). */
function extractAccessToken(raw: string | null): string | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw);
    const token =
      parsed?.access_token ??
      parsed?.currentSession?.access_token ??
      parsed?.session?.access_token ??
      null;
    return typeof token === 'string' && token.length > 0 ? token : null;
  } catch {
    return null;
  }
}

/** De gedeelde Supabase-JWT, of null als er geen sessie is. */
export function getSupabaseAccessToken(): string | null {
  if (typeof window === 'undefined') return null;
  // 1) directe key op de bekende project-ref
  const direct = extractAccessToken(
    localStorage.getItem(`sb-${PROJECT_REF}-auth-token`)
  );
  if (direct) return direct;
  // 2) fallback: scan elke sb-*-auth-token (ref gewijzigd/onbekend)
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (key && SB_KEY_RE.test(key)) {
      const t = extractAccessToken(localStorage.getItem(key));
      if (t) return t;
    }
  }
  return null;
}

/**
 * Token voor de Authorization-header. Volgorde:
 *   1. expliciet gezette `heatr_token` (handmatige/legacy override)
 *   2. gedeelde Supabase-sessie (A0 — één login via Warmr)
 *   3. `dev-token` (legacy cutover-fallback; alleen geldig als de backend
 *      LEGACY_DEV_TOKEN_ALLOWED=true heeft)
 */
export function resolveAuthToken(): string {
  if (typeof window === 'undefined') return 'dev-token';
  return (
    sessionStorage.getItem('heatr_token') ||
    getSupabaseAccessToken() ||
    'dev-token'
  );
}
