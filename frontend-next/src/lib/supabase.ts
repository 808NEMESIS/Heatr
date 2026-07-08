/**
 * Gedeelde Supabase-client — A0 operator-shell (Sprint 5, verbetering #1).
 *
 * Zelfde project + default storageKey (`sb-<ref>-auth-token`) als Warmr, en
 * beide draaien same-origin, dus deze client leest exact dezelfde sessie die
 * Warmr's login schrijft. Anders dan de eerdere hand-gerolde localStorage-
 * reader ververst deze client de access_token automatisch (autoRefreshToken),
 * zodat Heatr niet meer stilvalt zodra de token na ~1u verloopt.
 */
import { createClient } from '@supabase/supabase-js';

// Publieke build-config uit frontend-next/.env (VITE_-vars; anon-key is bedoeld
// voor de browser — Warmr ship't dezelfde in config.js). De .env is
// meegecommit (geen secrets) zodat de build overal werkt.
const url = import.meta.env.VITE_SUPABASE_URL as string;
const anonKey = import.meta.env.VITE_SUPABASE_ANON_KEY as string;

if (!url || !anonKey) {
  console.warn(
    '[auth] VITE_SUPABASE_URL / VITE_SUPABASE_ANON_KEY ontbreken — ' +
      'gedeelde sessie kan niet gelezen/ververst worden. Zie frontend-next/.env.'
  );
}

export const supabase = createClient(url ?? '', anonKey ?? '', {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    // Heatr is niet het OAuth-redirect-doel (login = e-mail/wachtwoord op de
    // Warmr- of Heatr-loginpagina); URL-detectie uit voorkomt verrassingen.
    detectSessionInUrl: false,
    // storageKey blijft de default (sb-<ref>-auth-token) — identiek aan Warmr.
  },
});
