/**
 * A0 sessie-continuïteit — probe voor de lezer-kant (Sprint 5).
 *
 * Bewijst schakel 3 van de keten: Heatr's frontend haalt de ECHTE Supabase-JWT
 * uit de gedeelde localStorage (dezelfde die Warmr's login vult, same-origin).
 * Reproduceert exact de extractie-logica uit src/lib/auth.ts tegen het
 * werkelijke supabase-js v2 opslagformaat.
 *
 * Draai:  node docs/probes/a0_session_continuity_probe.mjs
 */

// ── extractie-logica, identiek aan src/lib/auth.ts ─────────────────────────
const SB_KEY_RE = /^sb-.*-auth-token$/;

function extractAccessToken(raw) {
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

function getSupabaseAccessToken(store, projectRef) {
  const direct = extractAccessToken(store.getItem(`sb-${projectRef}-auth-token`));
  if (direct) return direct;
  for (const key of store.keys()) {
    if (SB_KEY_RE.test(key)) {
      const t = extractAccessToken(store.getItem(key));
      if (t) return t;
    }
  }
  return null;
}

// ── mini localStorage-mock ─────────────────────────────────────────────────
function makeStore(obj) {
  const m = new Map(Object.entries(obj));
  return { getItem: (k) => (m.has(k) ? m.get(k) : null), keys: () => m.keys() };
}

// ── testgevallen: echte supabase-js v2 opslagvormen ────────────────────────
const REF = 'zomdrygdcaenjnrrpcpw';
const FAKE_JWT = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1c2VyLTEyMyIsImF1ZCI6ImF1dGhlbnRpY2F0ZWQifQ.sig';

const cases = [
  {
    name: 'v2 flat session-object (huidige supabase-js)',
    store: makeStore({
      [`sb-${REF}-auth-token`]: JSON.stringify({
        access_token: FAKE_JWT, token_type: 'bearer', expires_at: 9999999999,
        refresh_token: 'r', user: { id: 'user-123' },
      }),
    }),
    expect: FAKE_JWT,
  },
  {
    name: 'legacy { currentSession }',
    store: makeStore({
      [`sb-${REF}-auth-token`]: JSON.stringify({ currentSession: { access_token: FAKE_JWT } }),
    }),
    expect: FAKE_JWT,
  },
  {
    name: 'ref onbekend → scan-fallback vindt sb-*-auth-token',
    store: makeStore({
      'sb-someotherref-auth-token': JSON.stringify({ access_token: FAKE_JWT }),
    }),
    expect: FAKE_JWT,
  },
  {
    name: 'geen sessie → null (valt terug op dev-token in de app)',
    store: makeStore({ 'unrelated': 'x' }),
    expect: null,
  },
];

let ok = 0;
for (const c of cases) {
  const got = getSupabaseAccessToken(c.store, REF);
  const pass = got === c.expect;
  console.log(`${pass ? 'PASS' : 'FAIL'}  ${c.name}`);
  if (!pass) console.log(`      verwacht ${c.expect}, kreeg ${got}`);
  if (pass) ok++;
}
console.log(`\n${ok}/${cases.length} geslaagd`);
process.exit(ok === cases.length ? 0 : 1);
